"""Capture, freeze and check the bronze inventory that the AWS catalog cutover registers.

The owner CLI (``scripts/lakehouse/bronze_cutover.py``) calls ``capture`` against the frozen Mac catalog and the project bucket, then
``write_inventory`` once per cutover. The document has the same five keys as ``register_aws_catalog.Inventory.document`` with no
publications, so the read-run parser refuses it and only ``inventory_from_document`` here accepts it. Nothing here writes to a catalog
or to S3. Each table pin comes from ``register_aws_catalog.capture_table``: the catalog response must equal the versioned metadata file.
"""

from __future__ import annotations

import contextlib
import hashlib
import http.client
import json
import os
import socket
import tempfile
import threading
import time
import urllib.parse
from collections.abc import Mapping, Set
from pathlib import Path
from typing import Any, Protocol

from scripts.acquisition.s3_store import AwsCli, StorageError
from scripts.lakehouse import register_aws_catalog as reg
from scripts.lakehouse.catalog_journal import durable_directory, sync_directory

NAMESPACES = ("bronze", "bronze_dictionary")
# Test namespaces of the bronze layer: they must be empty at capture and are never registered.
SCRATCH_NAMESPACES = ("smoke_check", "duckdb_view_check")
# Bronze tables outside the table map: the file register and the side tables the bronze loader keeps.
AUXILIARY = ("column_map", "file_metadata", "file_preambles", "stored_copies")
INVENTORY_NAME = "bronze_inventory.json"
DIGEST_NAME = "bronze_inventory.sha256"
MAX_PAGES = 1000


class Source(reg.Catalog, Protocol):
    def list_tables(self, namespace: str, *, deadline: float) -> list[str]:
        """Return every table name in the namespace, or no name when the namespace is absent."""
        ...


def required_bronze(root: Path) -> frozenset[reg.TableId]:
    """Every map table in ``bronze`` and ``bronze_dictionary`` plus the auxiliary bronze tables, from committed contracts only."""
    rows = reg.read_document((root / "config/lakehouse/bronze_tables.json").read_bytes(), "coverage")["tables"]
    names = {str(row["table"]) for row in rows}
    result = {("bronze", name) for name in names | set(AUXILIARY)} | {("bronze_dictionary", name) for name in names}
    for identifier in result:
        reg.plain_identifier(identifier)
    return frozenset(result)


class SourceCatalog(reg.PolarisCatalog):
    """The registration client plus a paged table listing, used read-only against the frozen Mac catalog."""

    def list_tables(self, namespace: str, *, deadline: float) -> list[str]:
        """List one namespace through every page; an absent namespace has no table."""
        reg.plain_identifier((namespace, "namespace"))
        names: list[str] = []
        token: str | None = None
        for _ in range(MAX_PAGES):
            query = f"?pageToken={urllib.parse.quote(token, safe='')}" if token else ""
            status, document = self.send("GET", f"{self.base}/{namespace}/tables{query}", deadline=deadline)
            if status == 404 and token is None:
                return []
            if status != 200 or not isinstance(document, dict) or not isinstance(document.get("identifiers"), list):
                raise reg.RegistrationError("catalog_http", f"listing namespace {namespace} failed (HTTP {status})")
            for item in document["identifiers"]:
                if not isinstance(item, dict) or item.get("namespace") != [namespace] or not isinstance(item.get("name"), str):
                    raise reg.RegistrationError("catalog_http", f"listing namespace {namespace} returned a malformed identifier")
                names.append(item["name"])
            token = document.get("next-page-token")
            if not token:
                return sorted(names)
            if not isinstance(token, str):
                raise reg.RegistrationError("catalog_http", f"listing namespace {namespace} returned a malformed page token")
        raise reg.RegistrationError("catalog_http", f"listing namespace {namespace} exceeded {MAX_PAGES} pages")


class PolarisHttp:
    """A bounded request to one Polaris endpoint with a client-credentials token held only in memory (R10).

    The secret and token never reach an argument, a log or a file; a refusal carries only the HTTP status. A 401 exchanges the token once more:
    Polaris refused the request before acting on it, so the repeat is not a second effect.
    """

    def __init__(self, host: str, port: int, *, client_id: str, secret: str, realm: str = "POLARIS", clock: reg.Clock = time.monotonic) -> None:
        self.host, self.port, self.realm, self.clock = host, port, realm, clock
        self._credentials = (client_id, secret)
        self._token: str | None = None

    def __repr__(self) -> str:
        return f"PolarisHttp({self.host}:{self.port})"

    def send(self, method: str, path: str, data: bytes | None, headers: dict[str, str], deadline: float) -> tuple[int, object]:
        """One request within the absolute deadline: a watchdog shuts the socket at expiry, so neither a silent peer nor one that keeps
        sending a few bytes holds the header or body read past it."""
        timeout = deadline - self.clock()
        if timeout <= 0:
            raise reg.RegistrationError("deadline", "catalog registration exceeded its fixed deadline")
        connection = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
        expired = threading.Event()

        def expire() -> None:
            expired.set()
            if connection.sock is not None:
                with contextlib.suppress(OSError):
                    connection.sock.shutdown(socket.SHUT_RDWR)

        watchdog = threading.Timer(timeout, expire)
        watchdog.daemon = True
        watchdog.start()
        try:
            try:
                connection.connect()
                if expired.is_set():
                    raise TimeoutError
                connection.request(method, path, body=data, headers={"Accept": "application/json", "Polaris-Realm": self.realm, **headers})
                response = connection.getresponse()
                raw = response.read(reg.MAX_RECORD + 1)
            except (OSError, http.client.HTTPException):
                if expired.is_set() or self.clock() >= deadline:
                    raise reg.RegistrationError("deadline", "catalog registration exceeded its fixed deadline") from None
                raise
            if expired.is_set():
                raise reg.RegistrationError("deadline", "catalog registration exceeded its fixed deadline")
        finally:
            watchdog.cancel()
            connection.close()
        if response.status >= 400:
            return response.status, None
        return response.status, reg.read_document(raw, "catalog_http") if raw else None

    def exchange(self, deadline: float) -> str:
        client_id, secret = self._credentials
        form = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": secret, "scope": "PRINCIPAL_ROLE:ALL"}
        data = urllib.parse.urlencode(form).encode()
        status, body = self.send("POST", "/api/catalog/v1/oauth/tokens", data, {"Content-Type": "application/x-www-form-urlencoded"}, deadline)
        if status != 200 or not isinstance(body, dict) or not isinstance(body.get("access_token"), str):
            raise reg.RegistrationError("authentication", f"Polaris refused the client credentials (HTTP {status})")
        return str(body["access_token"])

    def __call__(self, method: str, path: str, *, body: Mapping[str, Any] | None = None, deadline: float) -> tuple[int, object]:
        data = json.dumps(body).encode() if body is not None else None
        for _ in range(2):
            if self._token is None:
                self._token = self.exchange(deadline)
            headers = {"Authorization": f"Bearer {self._token}", **({"Content-Type": "application/json"} if data is not None else {})}
            status, document = self.send(method, path, data, headers, deadline)
            if status != 401:
                return status, document
            self._token = None
        return status, document


class S3Metadata:
    """Read one metadata object version, or the current version, from the project bucket through the bounded AWS CLI."""

    def __init__(self, client: AwsCli, bucket: str, *, clock: reg.Clock = time.monotonic) -> None:
        self.client, self.bucket, self.clock = client, bucket, clock

    def read(self, location: str, version_id: str | None, *, deadline: float) -> reg.ObjectValue:
        key = urllib.parse.urlsplit(reg.bucket_location(location, self.bucket)).path.removeprefix("/")
        reg.check_deadline(deadline, self.clock)
        with tempfile.TemporaryDirectory(prefix="bronze_metadata_") as scratch:
            local = Path(scratch) / "object"
            arguments = ["--key", key, *(["--version-id", version_id] if version_id is not None else []), str(local)]
            try:
                result = self.client.call_bounded("s3api", "get-object", arguments, timeout=max(0.001, deadline - self.clock()))
            except StorageError as error:
                raise reg.RegistrationError("transport", f"reading catalog metadata from S3 failed ({error.code})") from None
            size = local.stat().st_size if local.exists() else 0
            if not 0 < size <= reg.MAX_RECORD:
                raise reg.RegistrationError("metadata_identity", "a metadata object is empty or exceeds 32 MiB")
            body = local.read_bytes()
        version = result.get("VersionId")
        return reg.ObjectValue(version if isinstance(version, str) else "", body)


class Unused:
    """Stands for the destination, reader, publication and reservation services that a capture never calls."""

    def refuse(self) -> None:
        raise reg.RegistrationError("inventory", "a bronze capture touched a service it must not use")

    def ensure_namespace(self, namespace: str, *, deadline: float) -> None:
        self.refuse()

    def load(self, identifier: reg.TableId, *, deadline: float) -> Mapping[str, Any] | None:
        self.refuse()
        return None

    def register(self, identifier: reg.TableId, metadata_location: str, *, deadline: float) -> None:
        self.refuse()

    def read(self, namespace: str, run_id: str, *, deadline: float) -> Mapping[str, Any] | None:
        self.refuse()
        return None

    def ensure(self, reader: str, publish_run: str, *, deadline: float) -> None:
        self.refuse()

    def verify(self, reader: str, publish_run: str, *, deadline: float) -> None:
        self.refuse()


def named(identifier: reg.TableId, error: reg.RegistrationError) -> reg.RegistrationError:
    """The same cause code with the table named, so the owner can act on one table."""
    return reg.RegistrationError(error.code, f"{identifier[0]}.{identifier[1]}: {error}")


def check_listing(source: Source, required: Set[reg.TableId], deadline: float) -> None:
    """Refuse a non-empty scratch namespace and any listed table outside the set or set table not listed (I1, F7)."""
    for namespace in SCRATCH_NAMESPACES:
        found = source.list_tables(namespace, deadline=deadline)
        if found:
            raise reg.RegistrationError("coverage", f"scratch namespace {namespace} holds {len(found)} tables, first {namespace}.{found[0]}")
    listed = {(namespace, name) for namespace in NAMESPACES for name in source.list_tables(namespace, deadline=deadline)}
    extra, missing = sorted(listed - set(required)), sorted(set(required) - listed)
    if extra or missing or not required:
        shown = ", ".join([*(f"extra {a}.{b}" for a, b in extra[:5]), *(f"missing {a}.{b}" for a, b in missing[:5])]) or "no required table"
        raise reg.RegistrationError("coverage", f"the frozen Mac catalog differs from the bronze set ({len(extra)} extra, {len(missing)} missing): {shown}")


def capture(
    source: Source,
    objects: reg.MetadataReader,
    required: Set[reg.TableId],
    *,
    bucket: str,
    deadline: float,
    per_table: float,
    clock: reg.Clock = time.monotonic,
) -> reg.Inventory:
    """Pin every required table from the frozen Mac catalog and its versioned metadata object; each table gets its own deadline.

    Read-only: nothing is written anywhere. The first failure stops the capture with its cause code and the table named.
    """
    reg.check_deadline(deadline, clock)
    check_listing(source, required, deadline)
    unused = Unused()
    tables = []
    for identifier in sorted(required):
        table_deadline = min(deadline, clock() + per_table)
        io = reg.RegistrationIO(source, unused, unused, objects, unused, unused, clock)
        try:
            tables.append(reg.capture_table(io, identifier, None, bucket, table_deadline))
        except reg.RegistrationError as error:
            raise named(identifier, error) from None
    inventory = reg.Inventory((), tuple(tables))
    reg.check_coverage(inventory, required)
    return inventory


def inventory_from_document(document: Mapping[str, Any], required: Set[reg.TableId]) -> reg.Inventory:
    """Parse a bronze inventory: the read-run keys, no publication, a valid pin per row and exactly the bronze set."""
    if (
        set(document) != {"schema", "source_catalog", "destination_catalog", "publications", "tables"}
        or type(document.get("schema")) is not int
        or document["schema"] != 1
        or document.get("source_catalog") != "mac"
        or document.get("destination_catalog") != "aws"
        or document.get("publications") != []
        or not isinstance(document.get("tables"), list)
    ):
        raise reg.RegistrationError("inventory", "the bronze inventory version, origin, publications or fields are invalid")
    try:
        tables = tuple(reg.TablePin(**item) for item in document["tables"])
    except TypeError:
        raise reg.RegistrationError("inventory", "a bronze inventory entry has missing or unexpected fields") from None
    for table in tables:
        reg.validate_pin(table)
    inventory = reg.Inventory((), tuple(sorted(tables, key=lambda pin: pin.identifier)))
    reg.check_coverage(inventory, required)
    return inventory


def write_new(path: Path, content: bytes) -> None:
    """Publish complete bytes once (mode 0600, through a linked temporary file); an identical retry passes and a different one is refused."""
    descriptor, temporary = tempfile.mkstemp(prefix=".partial_", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != content:
                raise reg.RegistrationError("inventory", f"a different {path.name} is already frozen for this cutover") from None
    finally:
        os.unlink(temporary)
    sync_directory(path.parent)


def write_inventory(directory: Path, inventory: reg.Inventory) -> str:
    """Freeze the inventory and its SHA-256 write-once in the cutover folder and return the digest (I7, I8)."""
    durable_directory(directory)
    os.chmod(directory, 0o700)
    content = reg.canonical(inventory.document())
    digest = hashlib.sha256(content).hexdigest()
    write_new(directory / INVENTORY_NAME, content)
    write_new(directory / DIGEST_NAME, (digest + "\n").encode())
    return digest


def read_inventory(path: Path, sha256: str, required: Set[reg.TableId]) -> reg.Inventory:
    """Read a frozen bronze inventory and refuse it unless its bytes and parsed content match the owner-supplied digest."""
    raw = path.read_bytes()
    if not reg.SHA256.fullmatch(sha256) or hashlib.sha256(raw).hexdigest() != sha256:
        raise reg.RegistrationError("inventory", "the bronze inventory differs from its frozen digest")
    inventory = inventory_from_document(reg.read_document(raw, "inventory"), required)
    if inventory.sha256 != sha256:
        raise reg.RegistrationError("inventory", "the bronze inventory is not in canonical form")
    return inventory
