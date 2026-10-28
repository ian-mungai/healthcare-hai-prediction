"""Inventory, refresh, compare and rebuild catalogs through caller-owned bounded services.

The owner supplies authenticated source and destination requests, versioned object
readers, the storage scope and a durable record directory. Source requests are GET
only. Table registration always uses ``register_exact_metadata``. Authentication,
writer drain, authority transitions and effective fences belong to the owner.
The optional artifact writer must implement write-once remote storage. Without it,
evidence is local only and does not establish native migration acceptance.
"""

from __future__ import annotations

import copy
import hashlib
import math
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote, urlencode, urlsplit

from scripts.lakehouse import catalog_journal as journal
from scripts.lakehouse import register_aws_catalog as registration

Document = dict[str, Any]
Namespace = tuple[str, ...]
Identifier = tuple[Namespace, str]
Clock = Callable[[], float]


class MigrationError(RuntimeError):
    """A migration refusal whose cause and message contain no remote response body."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def bounded[T](operation: Callable[[], T], deadline: float, clock: Clock) -> T:
    if not math.isfinite(deadline) or clock() >= deadline:
        raise MigrationError("deadline", "catalog migration exceeded its fixed deadline")
    try:
        result = operation()
    except registration.RegistrationError as error:
        raise MigrationError(error.code, "the shared catalog registration refused the operation") from None
    except OSError:
        raise MigrationError("transport", "catalog migration transport failed") from None
    if clock() >= deadline:
        raise MigrationError("deadline", "catalog migration exceeded its fixed deadline")
    return result


def digest(value: object) -> str:
    return hashlib.sha256(registration.canonical(value)).hexdigest()


def secret_scan(value: object, secret_values: Sequence[str]) -> None:
    raw = registration.canonical(value)
    text = raw.decode()
    for marker in secret_values:
        if not marker or marker in text or marker.encode() in raw or registration.canonical(marker).strip()[1:-1] in raw:
            raise MigrationError("evidence_secret", "migration evidence contains a supplied secret value")


def namespace_value(value: object) -> Namespace:
    if not isinstance(value, (list, tuple)) or not value or any(not isinstance(part, str) or not registration.IDENTIFIER.fullmatch(part) for part in value):
        raise MigrationError("inventory", "a namespace identifier is invalid")
    return tuple(value)


def properties(value: object) -> dict[str, str]:
    if not isinstance(value, dict) or not all(isinstance(name, str) and isinstance(item, str) for name, item in value.items()):
        raise MigrationError("inventory", "namespace and table properties must contain strings")
    return dict(sorted(value.items()))


def table_identifier(row: Mapping[str, Any]) -> Identifier:
    namespace = namespace_value(row.get("namespace"))
    name = row.get("name")
    if not isinstance(name, str) or not registration.IDENTIFIER.fullmatch(name):
        raise MigrationError("inventory", "a table identifier is invalid")
    return namespace, name


def namespace_path(namespace: Namespace) -> str:
    return quote("\x1f".join(namespace), safe="")


class GetOnlyCatalog:
    """Read one named catalog with the caller's authenticated, deadline-bounded request."""

    def __init__(self, request: registration.AuthenticatedRequest, catalog_name: str, *, clock: Clock = time.monotonic) -> None:
        if not registration.IDENTIFIER.fullmatch(catalog_name):
            raise MigrationError("inventory", "the catalog name is invalid")

        def get_only(method: str, path: str, *, body: Mapping[str, Any] | None = None, deadline: float) -> tuple[int, object]:
            if method != "GET":
                raise MigrationError("source_read_only", "the source transport permits GET requests only")
            return request(method, path, body=body, deadline=deadline)

        self._request = get_only if type(self) is GetOnlyCatalog else request
        self.name, self.clock = catalog_name, clock
        self.base = f"/api/catalog/v1/{catalog_name}/namespaces"
        self.management = f"/api/management/v1/catalogs/{catalog_name}"

    def get(self, path: str, *, deadline: float, absent: bool = False) -> Document | None:
        status, value = bounded(lambda: self._request("GET", path, deadline=deadline), deadline, self.clock)
        if absent and status == 404:
            return None
        if status != 200 or not isinstance(value, dict):
            raise MigrationError("catalog_http", f"reading catalog state failed (HTTP {status})")
        return value

    def pages(self, path: str, field: str, *, query: Mapping[str, str] | None = None, deadline: float, counts: dict[str, int], label: str) -> list[Any]:
        values, tokens = [], set()
        token = None
        counts[label] = 0
        while True:
            parameters = dict(query or {})
            parameters["pageToken"] = "" if token is None else token
            page = self.get(path + ("?" + urlencode(parameters) if parameters else ""), deadline=deadline)
            if page is None or set(page) - {field, "next-page-token"} or not isinstance(page.get(field), list):
                raise MigrationError("paging", "a catalog list response has an unknown object kind or invalid page")
            values.extend(page[field])
            counts[label] += 1
            token = page.get("next-page-token")
            if token in (None, ""):
                return values
            if not isinstance(token, str) or token in tokens:
                raise MigrationError("paging", "a catalog list has an invalid or repeated continuation token")
            tokens.add(token)

    def namespace(self, namespace: Namespace, *, deadline: float) -> Document | None:
        value = self.get(f"{self.base}/{namespace_path(namespace)}", deadline=deadline, absent=True)
        if value is not None and namespace_value(value.get("namespace")) != namespace:
            raise MigrationError("inventory", "the catalog returned another namespace")
        return value

    def load(self, identifier: Identifier, *, deadline: float) -> Document | None:
        namespace, name = identifier
        return self.get(f"{self.base}/{namespace_path(namespace)}/tables/{quote(name, safe='')}", deadline=deadline, absent=True)

    def catalog(self, *, deadline: float) -> Document:
        value = self.get(self.management, deadline=deadline)
        if value is None:
            raise MigrationError("catalog_properties", "the catalog definition is absent")
        storage, options = value.get("storageConfigInfo"), value.get("properties")
        if (
            not isinstance(storage, dict)
            or not isinstance(options, dict)
            or not isinstance(storage.get("allowedLocations"), list)
            or type(storage.get("stsUnavailable")) is not bool
        ):
            raise MigrationError("catalog_properties", "the catalog definition is incomplete")
        return {
            "default-base-location": options.get("default-base-location"),
            "allowedLocations": sorted(storage["allowedLocations"]),
            "stsUnavailable": storage["stsUnavailable"],
        }

    def grants(self, *, deadline: float, counts: dict[str, int]) -> Document:
        roles = self.pages(self.management + "/catalog-roles", "roles", deadline=deadline, counts=counts, label="catalog_roles")
        result: Document = {}
        for role in roles:
            name = role.get("name") if isinstance(role, dict) else None
            if not isinstance(name, str) or not registration.IDENTIFIER.fullmatch(name) or name in result:
                raise MigrationError("grant_parity", "the catalog role inventory is invalid")
            entries = self.pages(self.management + f"/catalog-roles/{name}/grants", "grants", deadline=deadline, counts=counts, label="grants:" + name)
            if any(not isinstance(entry, dict) for entry in entries):
                raise MigrationError("grant_parity", "the catalog grant inventory is invalid")
            result[name] = sorted(entries, key=registration.canonical)
        return dict(sorted(result.items()))


class DestinationCatalog(GetOnlyCatalog):
    """Create namespaces and remove registrations without purge, reusing the exact registration client."""

    def mutate(self, method: str, path: str, *, body: Mapping[str, Any] | None = None, deadline: float) -> None:
        status, _ = bounded(lambda: self._request(method, path, body=body, deadline=deadline), deadline, self.clock)
        if status not in (200, 201, 204):
            raise MigrationError("catalog_http", f"changing destination catalog state failed (HTTP {status})")

    def create_namespace(self, namespace: Namespace, value: Mapping[str, str], *, deadline: float) -> None:
        self.mutate("POST", self.base, body={"namespace": list(namespace), "properties": dict(value)}, deadline=deadline)
        actual = self.namespace(namespace, deadline=deadline)
        if actual is None or properties(actual.get("properties")) != value:
            raise MigrationError("namespace_properties", "the destination namespace does not have the required properties")

    def drop_table(self, identifier: Identifier, *, deadline: float) -> None:
        namespace, name = identifier
        self.mutate("DELETE", f"{self.base}/{namespace_path(namespace)}/tables/{name}?purgeRequested=false", deadline=deadline)
        if self.load(identifier, deadline=deadline) is not None:
            raise MigrationError("refresh_conflict", "the non-purging drop left a registration present")

    def drop_namespace(self, namespace: Namespace, *, deadline: float) -> None:
        self.mutate("DELETE", f"{self.base}/{namespace_path(namespace)}", deadline=deadline)
        if self.namespace(namespace, deadline=deadline) is not None:
            raise MigrationError("reverse_conflict", "the confirmed namespace removal did not finish")

    def update_properties(self, namespace: Namespace, actual: dict[str, str], expected: dict[str, str], *, deadline: float) -> None:
        body = {"updates": expected, "removals": sorted(set(actual) - set(expected))}
        self.mutate("POST", f"{self.base}/{namespace_path(namespace)}/properties", body=body, deadline=deadline)
        observed = self.namespace(namespace, deadline=deadline)
        if observed is None or properties(observed.get("properties")) != expected:
            raise MigrationError("namespace_properties", "the confirmed namespace property change did not finish")

    def scoped(self, namespace: Namespace) -> registration.PolarisCatalog:
        """Translate a nested namespace to the shared client's flat registration identifier."""
        flat = namespace[-1]
        prefix = f"{self.base}/{flat}"

        def request(method: str, path: str, *, body: Mapping[str, Any] | None = None, deadline: float) -> tuple[int, object]:
            if not path.startswith(prefix + "/"):
                raise MigrationError("registration_scope", "the registration request escaped its namespace")
            translated = f"{self.base}/{namespace_path(namespace)}" + path[len(prefix) :]
            return self._request(method, translated, body=body, deadline=deadline)

        return registration.PolarisCatalog(request, self.name, clock=self.clock)


@dataclass(frozen=True)
class ObjectStat:
    """Identity returned for the current object or an explicitly requested immutable version."""

    version_id: str
    size: int
    etag: str
    modified: float


class VersionedObjects(registration.MetadataReader, Protocol):
    def stat(self, location: str, version_id: str | None, *, deadline: float) -> ObjectStat: ...

    def list_metadata(self, prefix: str, token: str | None, *, deadline: float) -> Document: ...


class ArtifactWriter(Protocol):
    def put(self, name: str, payload: bytes, *, sha256: str, if_none_match: str, deadline: float) -> None:
        """Create a remote migration artifact once, verifying an identical object on an exact retry."""
        ...


@dataclass(frozen=True)
class StorageScope:
    """Independent deployment scope and required membership, supplied before catalog enumeration."""

    table_root: str
    control_prefixes: tuple[str, ...]
    scratch_locations: tuple[str, ...]
    required_namespaces: tuple[Namespace, ...]
    required_tables: tuple[Identifier, ...]
    publish_branch_prefixes: tuple[str, ...]

    def path(self, value: object, *, table_path: bool = True) -> str:
        def location_value(value: object) -> str:
            if not isinstance(value, str):
                raise MigrationError("coverage", "a storage path is not a string")
            uri = urlsplit(value)
            if (
                uri.scheme != "s3"
                or not re.fullmatch(r"[a-z0-9][a-z0-9.-]+", uri.netloc)
                or uri.query
                or uri.fragment
                or not uri.path.startswith("/")
                or any(part in ("", ".", "..") for part in uri.path[1:].rstrip("/").split("/"))
                or re.search(r"[\s\x00-\x1f\x7f]", value)
            ):
                raise MigrationError("coverage", "a storage path is not a valid covered S3 location")
            return value

        location = location_value(value)
        root = location_value(self.table_root).rstrip("/") + "/"
        controls = [location_value(prefix).rstrip("/") + "/" for prefix in self.control_prefixes]
        prefix = location.rstrip("/") + "/"
        if not prefix.startswith(root) or any(not control.startswith(root) for control in controls):
            raise MigrationError("coverage", "a storage path is outside the declared table root")
        if any(prefix.startswith(control) or (table_path and control.startswith(prefix)) for control in controls):
            raise MigrationError("coverage", "a table path overlaps a control-object exception")
        return location

    def catalog(self, value: Document) -> list[str]:
        paths = [self.path(value.get("default-base-location"), table_path=False)]
        locations = value.get("allowedLocations")
        if not isinstance(locations, list) or not locations:
            raise MigrationError("coverage", "the catalog has no covered allowed location")
        paths.extend(self.path(location, table_path=False) for location in locations)
        paths.extend(self.path(location) for location in self.scratch_locations)
        return sorted(set(paths))


class MigrationRecords:
    """Keep immutable migration artifacts, owner intents and refresh boundaries under one owner.

    ``observed_at`` anchors the supplied monotonic clock to UTC. ``database_id`` identifies the
    live destination database, never the source of a pending recovery intent.
    A native caller supplies remote write-once storage and the effective deadline.
    """

    def __init__(
        self,
        root: Path,
        cutover_id: str,
        epoch: str,
        database_id: str,
        *,
        observed_at: str,
        remote: ArtifactWriter | None = None,
        clock: Clock = time.monotonic,
    ) -> None:
        self.root, self.cutover_id, self.database_id = root, journal.identifier(cutover_id), database_id
        self.anchor = datetime.fromisoformat(observed_at)
        if self.anchor.tzinfo is None or self.anchor.utcoffset() != timedelta(0):
            raise MigrationError("record_time", "the record clock anchor must be an explicit UTC timestamp")
        self.anchor = self.anchor.astimezone(UTC)
        self.clock, self.anchor_clock, self.remote = clock, clock(), remote
        self.journal = journal.LocalJournal(root / "owner_journal", epoch)
        for folder in (root, root / "refresh", self.journal.intents_path, self.journal.completions_path):
            folder.mkdir(parents=True, exist_ok=True)
        journal.write_once(root / "owner.json", {"cutover_id": cutover_id, "epoch": epoch, "database_id": database_id})

    def timestamp(self) -> str:
        return (self.anchor + timedelta(seconds=self.clock() - self.anchor_clock)).isoformat().replace("+00:00", "Z")

    def save(self, name: str, value: Document, *, secret_values: Sequence[str], deadline: float, clock: Clock) -> str:
        secret_scan(value, secret_values)
        filename = f"{journal.identifier(name)}_{self.cutover_id}.json"
        bounded(lambda: journal.write_once(self.root / filename, value), deadline, clock)
        checksum = digest(value)
        journal.write_once(self.root / (filename + ".sha256.json"), {"sha256": checksum})
        remote = self.remote
        if remote is not None:
            bounded(lambda: remote.put(filename, registration.canonical(value), sha256=checksum, if_none_match="*", deadline=deadline), deadline, clock)
        return checksum

    def command_id(self, action: str, namespace: Namespace, table: str = "") -> str:
        return action + "_" + digest([self.cutover_id, list(namespace), table])[:24]

    def start(self, action: str, namespace: Namespace, *, table: str = "", old: str | None = None, new: str | None = None, secret_values: Sequence[str]) -> str:
        command = self.command_id(action, namespace, table)
        path = self.root / "refresh" / f"{command}_start.json"
        record = {"action": action, "namespace": list(namespace), "table": table, "old_location": old, "new_location": new, "start": self.timestamp()}
        if path.exists():
            prior = journal.read_document(path)
            if any(prior[key] != record[key] for key in ("action", "namespace", "table", "new_location")):
                raise MigrationError("refresh_conflict", "a refresh retry changes its recorded target")
            record = prior
        secret_scan(record, secret_values)
        journal.write_once(path, record)
        self.journal.intent(command, namespace[0], [".".join(namespace)], self.database_id)
        return command

    def finish(self, command: str, observation: Document, *, secret_values: Sequence[str]) -> None:
        secret_scan(observation, secret_values)
        confirmed = any(record["intent_id"] == command and record["status"] == "confirmed" for record in self.journal.completions())
        if not confirmed:
            self.journal.complete(
                command, "owner", {"kind": "live", "database_id": self.database_id, "available": True, "namespaces": observation}, terminal=True, quiesced=True
            )
        path = self.root / "refresh" / f"{command}_end.json"
        if not path.exists():
            journal.write_once(path, {"end": self.timestamp()})

    def entries(self) -> list[Document]:
        result = []
        for path in sorted((self.root / "refresh").glob("*_start.json")):
            end = path.with_name(path.name.replace("_start.json", "_end.json"))
            result.append({**journal.read_document(path), **(journal.read_document(end) if end.exists() else {"end": None})})
        return result

    def pending_entries(self) -> list[Document]:
        confirmed = {record["intent_id"] for record in self.journal.completions() if record["status"] == "confirmed"}
        pending = set(self.journal.intents()) - confirmed
        return [entry for entry in self.entries() if self.command_id(entry["action"], namespace_value(entry["namespace"]), entry["table"]) in pending]


@dataclass(frozen=True)
class MigrationIO:
    """The owner-selected catalog, versioned object, scope and durable record services."""

    source: GetOnlyCatalog
    destination: DestinationCatalog
    source_objects: VersionedObjects
    destination_objects: VersionedObjects
    scope: StorageScope
    records: MigrationRecords
    clock: Clock = time.monotonic
    secret_values: tuple[str, ...] = ()
    residues: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if type(self.source) is not GetOnlyCatalog:
            raise MigrationError("source_read_only", "the migration source must be an exact GET-only catalog client")


def object_metadata(
    objects: VersionedObjects, location: str, version_id: str | None, *, deadline: float, clock: Clock
) -> tuple[Document, Document, ObjectStat]:
    stat = bounded(lambda: objects.stat(location, version_id, deadline=deadline), deadline, clock)
    if (
        not isinstance(stat, ObjectStat)
        or not stat.version_id
        or stat.version_id == "null"
        or not stat.etag
        or type(stat.size) is not int
        or stat.size <= 0
        or not math.isfinite(stat.modified)
    ):
        raise MigrationError("object_version", "the metadata object has no complete immutable identity")
    if version_id is not None and stat.version_id != version_id:
        raise MigrationError("object_version", "the metadata object returned another version")
    value = bounded(lambda: objects.read(location, stat.version_id, deadline=deadline), deadline, clock)
    if value.version_id != stat.version_id:
        raise MigrationError("object_version", "the metadata read did not return its requested immutable version")
    if len(value.body) != stat.size:
        raise MigrationError("four_readings", "metadata object bytes differ from their recorded size")
    try:
        metadata = registration.read_document(value.body, "four_readings")
    except registration.RegistrationError:
        raise MigrationError("four_readings", "the metadata object is not valid JSON") from None
    identity = {"version_id": stat.version_id, "size": stat.size, "etag": stat.etag, "sha256": hashlib.sha256(value.body).hexdigest()}
    return metadata, identity, stat


def reading(namespace: Namespace, name: str, location: str, metadata: object) -> Document:
    if not isinstance(metadata, dict):
        raise MigrationError("four_readings", "a catalog reading has no Iceberg metadata")
    keys = (
        "table-uuid",
        "current-snapshot-id",
        "refs",
        "snapshots",
        "last-sequence-number",
        "format-version",
        "current-schema-id",
        "default-spec-id",
        "default-sort-order-id",
        "location",
        "properties",
    )
    if any(key not in metadata for key in keys) or not isinstance(metadata["refs"], dict) or not isinstance(metadata["snapshots"], list):
        raise MigrationError("four_readings", "a table reading omits a required parity field")
    snapshots = metadata["snapshots"]
    if any(not isinstance(item, dict) or type(item.get("snapshot-id")) is not int for item in snapshots):
        raise MigrationError("four_readings", "snapshot history is invalid")
    if len({item["snapshot-id"] for item in snapshots}) != len(snapshots):
        raise MigrationError("four_readings", "snapshot history contains duplicate identifiers")
    result = {key: copy.deepcopy(metadata[key]) for key in keys if key not in {"snapshots", "properties"}}
    result.update(
        {
            "namespace": list(namespace),
            "name": name,
            "metadata-location": location,
            "snapshots": sorted(
                [{"snapshot-id": item["snapshot-id"], "parent-snapshot-id": item.get("parent-snapshot-id")} for item in snapshots],
                key=lambda item: item["snapshot-id"],
            ),
            "properties_sha256": digest(properties(metadata["properties"])),
        }
    )
    return result


def metadata_pages(objects: VersionedObjects, prefix: str, modified: float, *, deadline: float, clock: Clock, counts: dict[str, int], label: str) -> None:
    token = None
    tokens: set[str] = set()
    counts[label] = 0
    while True:
        page = bounded(partial(objects.list_metadata, prefix, token, deadline=deadline), deadline, clock)
        if not isinstance(page, dict) or set(page) - {"objects", "next-page-token"} or not isinstance(page.get("objects"), list):
            raise MigrationError("paging", "the metadata listing has an invalid page")
        counts[label] += 1
        for item in page["objects"]:
            if (
                not isinstance(item, dict)
                or not isinstance(item.get("location"), str)
                or not item["location"].startswith(prefix)
                or type(item.get("modified")) not in (int, float)
                or not math.isfinite(item["modified"])
            ):
                raise MigrationError("paging", "a metadata listing entry is invalid")
            if item["location"].endswith(".metadata.json") and item["modified"] > modified:
                raise MigrationError("newer_metadata", "a newer metadata object requires the owner's decision")
        token = page.get("next-page-token")
        if token in (None, ""):
            return
        if not isinstance(token, str) or token in tokens:
            raise MigrationError("paging", "the metadata listing repeats a continuation token")
        tokens.add(token)


def capture_table(
    catalog: GetOnlyCatalog,
    objects: VersionedObjects,
    scope: StorageScope,
    identifier: Identifier,
    *,
    deadline: float,
    clock: Clock,
    counts: dict[str, int],
    check_newer: bool,
) -> Document:
    namespace, name = identifier
    loaded = catalog.load(identifier, deadline=deadline)
    if loaded is None:
        raise MigrationError("membership", "a listed table disappeared during inventory")
    try:
        location = registration.s3_location(loaded.get("metadata-location"))
    except registration.RegistrationError:
        raise MigrationError("four_readings", "the catalog returned an invalid metadata location") from None
    metadata, identity, stat = object_metadata(objects, location, None, deadline=deadline, clock=clock)
    catalog_reading = reading(namespace, name, location, loaded.get("metadata"))
    object_reading = reading(namespace, name, location, metadata)
    if catalog_reading != object_reading:
        raise MigrationError("four_readings", "catalog metadata differs from the pinned object reading")
    refs = metadata["refs"]
    if any(branch.startswith(scope.publish_branch_prefixes) and value.get("type") == "branch" for branch, value in refs.items()):
        raise MigrationError("open_publish_branch", "an open publication branch prevents inventory acceptance")
    covered = [scope.path(metadata["location"]), scope.path(location.rsplit("/", 1)[0] + "/")]
    covered.extend(
        scope.path(metadata["properties"][key])
        for key in ("write.data.path", "write.metadata.path", "write.object-storage.path")
        if key in metadata["properties"]
    )
    if not any(
        (metadata["location"].rstrip("/") + "/").startswith(allowed.rstrip("/") + "/") for allowed in catalog.catalog(deadline=deadline)["allowedLocations"]
    ):
        raise MigrationError("coverage", "a table lies outside the catalog's allowed locations")
    if check_newer:
        metadata_pages(
            objects,
            location.rsplit("/", 1)[0] + "/",
            stat.modified,
            deadline=deadline,
            clock=clock,
            counts=counts,
            label="metadata:" + ".".join((*namespace, name)),
        )
    row = {
        "namespace": list(namespace),
        "name": name,
        "metadata_location": location,
        "object": identity,
        "readings": {"catalog": catalog_reading, "object": object_reading},
        "covered_paths": sorted(set(covered)),
    }
    pin_for(row)
    return row


def inventory(
    catalog: GetOnlyCatalog,
    objects: VersionedObjects,
    scope: StorageScope,
    *,
    deadline: float,
    clock: Clock,
    required: bool = True,
    check_newer: bool = False,
    secret_values: Sequence[str] = (),
) -> Document:
    """Recursively read every namespace and list page, pinning metadata versions and independent readings."""
    counts: dict[str, int] = {}
    definition = catalog.catalog(deadline=deadline)
    covered = scope.catalog(definition)
    namespaces: list[Document] = []
    tables: list[Document] = []
    seen: set[Namespace] = set()
    pending: list[Namespace] = [()]
    while pending:
        parent = pending.pop(0)
        query = {"parent": "\x1f".join(parent)} if parent else {}
        listed = catalog.pages(
            catalog.base, "namespaces", query=query, deadline=deadline, counts=counts, label="namespaces:" + (".".join(parent) if parent else "root")
        )
        for raw in listed:
            namespace = namespace_value(raw)
            if namespace[:-1] != parent or namespace in seen:
                raise MigrationError("paging", "the namespace listing is duplicated or is not recursive")
            seen.add(namespace)
            pending.append(namespace)
            entry = catalog.namespace(namespace, deadline=deadline)
            if entry is None:
                raise MigrationError("membership", "a listed namespace disappeared during inventory")
            namespaces.append({"namespace": list(namespace), "properties": properties(entry.get("properties"))})
            for kind in ("tables", "views"):
                identifiers = catalog.pages(
                    f"{catalog.base}/{namespace_path(namespace)}/{kind}",
                    "identifiers",
                    deadline=deadline,
                    counts=counts,
                    label=kind + ":" + ".".join(namespace),
                )
                unique: set[Identifier] = set()
                for identifier in identifiers:
                    if not isinstance(identifier, dict) or table_identifier(identifier)[0] != namespace or table_identifier(identifier) in unique:
                        raise MigrationError("paging", "a table or view listing is invalid or duplicated")
                    unique.add(table_identifier(identifier))
                if kind == "views" and identifiers:
                    raise MigrationError("unsupported_view", "a view requires the owner's readiness decision")
                if kind == "tables":
                    tables.extend(
                        capture_table(catalog, objects, scope, identifier, deadline=deadline, clock=clock, counts=counts, check_newer=check_newer)
                        for identifier in sorted(unique)
                    )
    if required and (not set(scope.required_namespaces).issubset(seen) or not set(scope.required_tables).issubset({table_identifier(row) for row in tables})):
        raise MigrationError("coverage", "the inventory omits independently required namespaces or tables")
    result = {
        "schema": 1,
        "catalog_name": catalog.name,
        "catalog": definition,
        "namespaces": sorted(namespaces, key=lambda row: row["namespace"]),
        "tables": sorted(tables, key=table_identifier),
        "grants": catalog.grants(deadline=deadline, counts=counts),
        "pages": dict(sorted(counts.items())),
        "counts": {"namespaces": len(namespaces), "tables": len(tables), "views": 0},
        "covered_paths": sorted(set(covered + [path for row in tables for path in row["covered_paths"]])),
    }
    secret_scan(result, secret_values)
    return result


def source_inventory(io: MigrationIO, *, deadline: float) -> Document:
    if io.residues:
        raise MigrationError("rehearsal_residue", "rehearsal roles or objects remain in the supplied residue inventory")
    result = inventory(io.source, io.source_objects, io.scope, deadline=deadline, clock=io.clock, check_newer=True, secret_values=io.secret_values)
    if any(any(part.startswith("migration_rehearsal_") for part in row["namespace"]) for row in result["namespaces"]):
        raise MigrationError("rehearsal_residue", "a rehearsal namespace prevents inventory acceptance")
    return result


def freeze(io: MigrationIO, *, deadline: float, wait: Callable[[float], None] = time.sleep) -> Document:
    """Capture equal source inventories at least 120 seconds apart under the caller's freeze deadline."""
    first = source_inventory(io, deadline=deadline)
    ended = io.clock()
    if deadline - ended <= 120:
        raise MigrationError("deadline", "the freeze deadline leaves no time for the second inventory")
    bounded(lambda: wait(120.0), deadline, io.clock)
    separation = io.clock() - ended
    if separation < 120:
        raise MigrationError("freeze_interval", "the frozen inventory reads were less than two minutes apart")
    second = source_inventory(io, deadline=deadline)
    if first != second:
        raise MigrationError("source_changed", "the two frozen source inventories differ")
    frozen = {
        "schema": 1,
        "source_catalog": io.source.name,
        "destination_catalog": io.destination.name,
        "inventory": first,
        "inventory_sha256": digest(first),
        "read_sha256": [digest(first), digest(second)],
        "separation_seconds": separation,
    }
    io.records.save("inventory", frozen, secret_values=io.secret_values, deadline=deadline, clock=io.clock)
    return frozen


def frozen_inventory(io: MigrationIO, frozen: Document) -> Document:
    value = frozen.get("inventory")
    separation = frozen.get("separation_seconds")
    if (
        not isinstance(value, dict)
        or frozen.get("inventory_sha256") != digest(value)
        or frozen.get("read_sha256") != [digest(value), digest(value)]
        or not isinstance(separation, (int, float))
        or isinstance(separation, bool)
        or not math.isfinite(separation)
        or separation < 120
        or frozen.get("source_catalog") != io.source.name
        or frozen.get("destination_catalog") != io.destination.name
    ):
        raise MigrationError("inventory_digest", "the frozen inventory identity or two-read evidence is invalid")
    secret_scan(frozen, io.secret_values)
    return value


def pin_for(row: Document) -> registration.TablePin:
    namespace, name = table_identifier(row)
    reading_value, identity = row["readings"]["object"], row["object"]
    snapshot = reading_value["current-snapshot-id"]
    if type(snapshot) is not int or snapshot <= 0:
        raise MigrationError("registration_contract", "the shared registration interface requires a positive retained snapshot")
    pin = registration.TablePin(
        namespace[-1],
        name,
        reading_value["table-uuid"],
        reading_value["location"],
        row["metadata_location"],
        identity["version_id"],
        identity["sha256"],
        identity["size"],
        snapshot,
        snapshot,
    )
    try:
        registration.validate_pin(pin)
    except registration.RegistrationError:
        raise MigrationError("four_readings", "the metadata pin is invalid for exact registration") from None
    return pin


def namespace_map(value: Document) -> dict[Namespace, dict[str, str]]:
    return {namespace_value(row["namespace"]): row["properties"] for row in value["namespaces"]}


def table_map(value: Document) -> dict[Identifier, Document]:
    return {table_identifier(row): row for row in value["tables"]}


def membership(actual: Document, expected: Document, *, complete: bool) -> None:
    left, right = namespace_map(actual), namespace_map(expected)
    tables, required_tables = table_map(actual), table_map(expected)
    if set(left) - set(right) or set(tables) - set(required_tables) or (complete and (set(left) != set(right) or set(tables) != set(required_tables))):
        raise MigrationError("membership", "catalog namespace or table membership differs")
    if any(left[name] != right[name] for name in left.keys() & right.keys()):
        raise MigrationError("namespace_properties", "catalog namespace properties differ")
    if actual["catalog"] != expected["catalog"]:
        raise MigrationError("catalog_properties", "catalog storage properties differ")


def metadata_at(io: MigrationIO, row: Document, *, deadline: float) -> Document:
    metadata, identity, _ = object_metadata(io.source_objects, row["metadata_location"], row["object"]["version_id"], deadline=deadline, clock=io.clock)
    if identity != row["object"] or reading(*table_identifier(row), row["metadata_location"], metadata) != row["readings"]["object"]:
        raise MigrationError("four_readings", "the frozen metadata object identity or reading changed")
    return metadata


def observe_namespace(io: MigrationIO, namespace: Namespace, *, deadline: float) -> Document:
    entry = io.destination.namespace(namespace, deadline=deadline)
    if entry is None:
        return {".".join(namespace): {"state": "absent", "properties": {}, "tables": {}}}
    counts: dict[str, int] = {}
    listed = io.destination.pages(
        f"{io.destination.base}/{namespace_path(namespace)}/tables", "identifiers", deadline=deadline, counts=counts, label="completion"
    )
    heads = {}
    for item in listed:
        identifier = table_identifier(item)
        if identifier[0] != namespace or identifier[1] in heads:
            raise MigrationError("membership", "the owner completion has invalid table membership")
        row = capture_table(io.destination, io.destination_objects, io.scope, identifier, deadline=deadline, clock=io.clock, counts=counts, check_newer=False)
        heads[identifier[1]] = journal_head(row)
    return {".".join(namespace): {"state": "present", "properties": properties(entry["properties"]), "tables": heads}}


def ensure_namespace(io: MigrationIO, namespace: Namespace, value: dict[str, str], *, deadline: float) -> None:
    actual = io.destination.namespace(namespace, deadline=deadline)
    command = io.records.command_id("namespace", namespace)
    pending = command in io.records.journal.intents() and not any(
        record["intent_id"] == command and record["status"] == "confirmed" for record in io.records.journal.completions()
    )
    if actual is not None:
        if properties(actual.get("properties")) != value:
            raise MigrationError("namespace_properties", "the existing namespace has different properties")
        if pending:
            io.records.finish(command, observe_namespace(io, namespace, deadline=deadline), secret_values=io.secret_values)
        return
    command = io.records.start("namespace", namespace, secret_values=io.secret_values)
    io.destination.create_namespace(namespace, value, deadline=deadline)
    io.records.finish(command, observe_namespace(io, namespace, deadline=deadline), secret_values=io.secret_values)


def register_row(io: MigrationIO, row: Document, old: str | None, *, deadline: float) -> str:
    namespace, name = table_identifier(row)
    command = io.records.start("table", namespace, table=name, old=old, new=row["metadata_location"], secret_values=io.secret_values)
    loaded = io.destination.load((namespace, name), deadline=deadline)
    confirmed = any(record["intent_id"] == command and record["status"] == "confirmed" for record in io.records.journal.completions())
    if confirmed and (loaded is None or loaded["metadata-location"] != row["metadata_location"]):
        raise MigrationError("refresh_conflict", "a completed refresh target changed before its retry")
    if loaded is not None and loaded["metadata-location"] != row["metadata_location"]:
        if loaded["metadata-location"] != old:
            raise MigrationError("refresh_conflict", "a refresh target changed after preflight")
        io.destination.drop_table((namespace, name), deadline=deadline)
    pin = pin_for(row)
    bucket = urlsplit(io.scope.table_root).netloc
    outcome = bounded(
        lambda: registration.register_exact_metadata(
            io.destination.scoped(namespace), io.source_objects, pin, bucket=bucket, deadline=deadline, clock=io.clock
        ),
        deadline,
        io.clock,
    )
    io.records.finish(command, observe_namespace(io, namespace, deadline=deadline), secret_values=io.secret_values)
    return outcome


def registration_receipt(io: MigrationIO, rows: list[Document], registered: list[Identifier], unchanged: list[Identifier]) -> Document:
    pins = tuple(replace(pin_for(row), namespace=".".join(table_identifier(row)[0])) for row in rows)
    result = registration.RegistrationResult(
        registration.Inventory((), pins),
        tuple((".".join(namespace), name) for namespace, name in registered),
        tuple((".".join(namespace), name) for namespace, name in unchanged),
    ).document()
    result["inventory"]["source_catalog"], result["inventory"]["destination_catalog"] = io.source.name, io.destination.name
    result["inventory_sha256"] = digest(result["inventory"])
    return result


def refresh_plan(io: MigrationIO, expected: Document, actual: Document, *, deadline: float) -> list[tuple[Document, Document | None]]:
    plan: list[tuple[Document, Document | None]] = []
    existing = table_map(actual)
    for row in expected["tables"]:
        identifier = table_identifier(row)
        old = existing.get(identifier)
        metadata = metadata_at(io, row, deadline=deadline)
        if old is not None and old["metadata_location"] != row["metadata_location"]:
            ancestors = {entry.get("metadata-file") for entry in metadata.get("metadata-log", []) if isinstance(entry, dict)}
            if old["readings"]["object"]["table-uuid"] != row["readings"]["object"]["table-uuid"] or old["metadata_location"] not in ancestors:
                raise MigrationError("refresh_conflict", "the destination registration is not a same-UUID ancestor")
        elif old is not None and (old["object"] != row["object"] or old["readings"] != row["readings"]):
            raise MigrationError("refresh_conflict", "an identical destination location has different metadata")
        plan.append((row, old))
    return plan


def refresh(io: MigrationIO, frozen: Document, *, deadline: float) -> Document:
    """Refresh only missing or same-UUID ancestor registrations, after complete conflict preflight."""
    expected = frozen_inventory(io, frozen)
    actual = inventory(io.destination, io.destination_objects, io.scope, deadline=deadline, clock=io.clock, required=False, secret_values=io.secret_values)
    membership(actual, expected, complete=False)
    plan = refresh_plan(io, expected, actual, deadline=deadline)
    for namespace, value in sorted(namespace_map(expected).items(), key=lambda item: (len(item[0]), item[0])):
        ensure_namespace(io, namespace, value, deadline=deadline)
    registered, unchanged = [], []
    for row, old in plan:
        namespace, name = identifier = table_identifier(row)
        command = io.records.command_id("table", namespace, name)
        started = (io.records.root / "refresh" / f"{command}_start.json").exists()
        if old is None or old["metadata_location"] != row["metadata_location"] or started:
            register_row(io, row, None if old is None else old["metadata_location"], deadline=deadline)
            registered.append(identifier)
        else:
            bounded(
                partial(
                    registration.check_loaded,
                    io.destination.load(identifier, deadline=deadline),
                    pin_for(row),
                    metadata_at(io, row, deadline=deadline),
                    "refresh_conflict",
                ),
                deadline,
                io.clock,
            )
            unchanged.append(identifier)
    states = journal_inventory(frozen)
    replayed = journal.replay(states, io.records.journal.intents(), io.records.journal.completions())
    result = {
        "schema": 1,
        "state": "refreshed",
        "registration_receipt": registration_receipt(io, expected["tables"], registered, unchanged),
        "refresh_journal": io.records.entries(),
        "pending_intents": replayed["pending_intents"],
    }
    io.records.save("refresh_journal", result, secret_values=io.secret_values, deadline=deadline, clock=io.clock)
    return result


def parity(io: MigrationIO, frozen: Document, *, expected_grants: Document, deadline: float) -> Document:
    """Compare four independent readings, complete membership, namespace properties and the phase grants."""
    expected = frozen_inventory(io, frozen)
    actual = inventory(io.destination, io.destination_objects, io.scope, deadline=deadline, clock=io.clock, required=False, secret_values=io.secret_values)
    membership(actual, expected, complete=True)
    if actual["grants"] != {role: sorted(entries, key=registration.canonical) for role, entries in sorted(expected_grants.items())}:
        raise MigrationError("grant_parity", "destination grants differ from the current phase contract")
    tables = table_map(actual)
    compared = []
    for row in expected["tables"]:
        target = tables[table_identifier(row)]
        source_catalog = row["readings"]["catalog"]
        source_object = row["readings"]["object"]
        destination_catalog = target["readings"]["catalog"]
        destination_object = target["readings"]["object"]
        if not (source_catalog == source_object == destination_catalog == destination_object) or row["object"] != target["object"]:
            raise MigrationError("four_readings", "the four table readings or exact object identities differ")
        compared.append({"namespace": row["namespace"], "name": row["name"], "readings": 4, "object": row["object"]})
    io.records.save("aws_after", actual, secret_values=io.secret_values, deadline=deadline, clock=io.clock)
    result = {
        "schema": 1,
        "equal": True,
        "inventory_sha256": frozen["inventory_sha256"],
        "destination_sha256": digest(actual),
        "counts": actual["counts"],
        "tables": compared,
        "grant_contract_sha256": digest(expected_grants),
    }
    io.records.save("parity", result, secret_values=io.secret_values, deadline=deadline, clock=io.clock)
    return result


def final_read(io: MigrationIO, frozen: Document, *, deadline: float) -> Document:
    """Require a third source inventory equal to both frozen readings before the owner stops the source."""
    expected = frozen_inventory(io, frozen)
    try:
        actual = source_inventory(io, deadline=deadline)
    except MigrationError as error:
        if error.code in {"coverage", "membership"}:
            raise MigrationError("source_changed", "source membership changed after the frozen inventory") from None
        raise
    if actual != expected:
        raise MigrationError("source_changed", "the final source inventory differs from the frozen inventory")
    io.records.save("final_source", actual, secret_values=io.secret_values, deadline=deadline, clock=io.clock)
    return {"equal": True, "inventory_sha256": digest(actual), "read_sha256": [*frozen["read_sha256"], digest(actual)]}


def journal_head(row: Document) -> Document:
    value = row["readings"]["object"]
    return {
        "uuid": value["table-uuid"],
        "metadata_location": row["metadata_location"],
        "version_id": row["object"]["version_id"],
        "snapshot_id": value["current-snapshot-id"],
        "refs": value["refs"],
    }


def journal_inventory(frozen: Document) -> Document:
    """Translate the frozen complete inventory into the existing namespace replacement journal format."""
    inventory_value = frozen["inventory"]
    states = {".".join(row["namespace"]): {"state": "present", "properties": row["properties"], "tables": {}} for row in inventory_value["namespaces"]}
    for row in inventory_value["tables"]:
        states[".".join(row["namespace"])]["tables"][row["name"]] = journal_head(row)
    return journal.namespace_states(states)


def validate_replay_inputs(intents: dict[str, Document], completions: list[Document]) -> None:
    if not isinstance(intents, dict) or not isinstance(completions, list):
        raise MigrationError("journal_replay", "the recovery journal must contain an intent map and a completion list")
    for name, intent in intents.items():
        if not isinstance(name, str) or not isinstance(intent, dict) or set(intent) != {"intent_id", "epoch", "layer", "database_id", "namespaces"}:
            raise MigrationError("journal_replay", "a recovery intent has invalid fields")
        if any(not isinstance(intent[field], str) or not intent[field] for field in ("intent_id", "epoch", "layer", "database_id")):
            raise MigrationError("journal_replay", "a recovery intent has an invalid identity")
        names = intent["namespaces"]
        if (
            not isinstance(names, list)
            or not names
            or any(not isinstance(value, str) or not all(registration.IDENTIFIER.fullmatch(part) for part in value.split(".")) for value in names)
        ):
            raise MigrationError("journal_replay", "a recovery intent has invalid namespace identifiers")
        if len(names) != len(set(names)):
            raise MigrationError("journal_replay", "a recovery intent repeats a namespace")
    for record in completions:
        if not isinstance(record, dict) or set(record) != {"sequence", "epoch", "intent_id", "step", "status", "source", "terminal", "quiesced", "namespaces"}:
            raise MigrationError("journal_replay", "a recovery completion has invalid fields")
        if any(not isinstance(record[field], str) or not record[field] for field in ("epoch", "intent_id", "step", "status")):
            raise MigrationError("journal_replay", "a recovery completion has an invalid identity")
        source = record["source"]
        if not isinstance(source, dict) or set(source) != {"kind", "database_id", "available"}:
            raise MigrationError("journal_replay", "a recovery completion has no valid source observation")
        if any(not isinstance(source[field], str) or not source[field] for field in ("kind", "database_id")) or type(source["available"]) is not bool:
            raise MigrationError("journal_replay", "a recovery completion has an invalid source identity or availability flag")
        if any(type(record[field]) is not bool for field in ("terminal", "quiesced")):
            raise MigrationError("journal_replay", "a recovery completion has invalid terminal or quiescence flags")
        states = record["namespaces"]
        if not isinstance(states, dict) or any(isinstance(state, dict) and not isinstance(state.get("state"), str) for state in states.values()):
            raise MigrationError("journal_replay", "a recovery completion has invalid namespace observations")
        try:
            journal.namespace_states(states)
        except ValueError:
            raise MigrationError("journal_replay", "a recovery completion has invalid namespace state") from None


def confirmed_replay(frozen: Document, intents: dict[str, Document], completions: list[Document], *, epoch: str, quiesced: bool) -> Document:
    if quiesced is not True:
        raise MigrationError("not_quiesced", "writers and owner commands must be quiesced before recovery")
    validate_replay_inputs(intents, completions)
    if [record.get("sequence") for record in completions] != list(range(1, len(completions) + 1)) or any(
        type(record.get("sequence")) is not int for record in completions
    ):
        raise MigrationError("journal_sequence", "the commit journal sequence is incomplete or out of order")
    if any(intent.get("epoch") != epoch or intent.get("intent_id") != name for name, intent in intents.items()) or any(
        record.get("epoch") != epoch for record in completions
    ):
        raise MigrationError("journal_epoch", "the recovery journal contains another epoch or intent identity")
    try:
        return journal.replay(journal_inventory(frozen), intents, completions)
    except ValueError:
        raise MigrationError("journal_replay", "the commit journal cannot establish confirmed namespace states") from None


def reverse_refresh(
    io: MigrationIO,
    frozen: Document,
    cutover: Document,
    intents: dict[str, Document],
    completions: list[Document],
    *,
    epoch: str,
    quiesced: bool,
    expected_grants: Document,
    deadline: float,
) -> Document:
    """Refresh a fenced former source using live heads and confirmed deletion and property evidence.

    The caller drains and fences the live source first. ``cutover`` is the
    original frozen inventory. This function never changes fences or grants.
    """
    expected = frozen_inventory(io, frozen)
    original = cutover.get("inventory")
    if not isinstance(original, dict) or cutover.get("inventory_sha256") != digest(original):
        raise MigrationError("inventory_digest", "the original cutover inventory digest is invalid")
    replayed = confirmed_replay(cutover, intents, completions, epoch=epoch, quiesced=quiesced)
    if replayed["unresolved"] or replayed["pending_intents"]:
        raise MigrationError("pending_intent", "a reverse migration has an unresolved source intent")
    confirmed = {name: state for name, state in replayed["namespace_states"].items() if state["state"] == "present"}
    if confirmed != journal_inventory(frozen):
        raise MigrationError("journal_parity", "the live reverse inventory differs from confirmed journal state")
    latest = {name: state for record in completions if record["status"] == "confirmed" for name, state in record["namespaces"].items()}
    actual = inventory(io.destination, io.destination_objects, io.scope, deadline=deadline, clock=io.clock, required=False, secret_values=io.secret_values)
    if actual["catalog"] != expected["catalog"]:
        raise MigrationError("catalog_properties", "reverse catalog storage properties differ")
    before, after = table_map(actual), table_map(expected)
    old_namespaces, new_namespaces = namespace_map(actual), namespace_map(expected)
    pending = io.records.pending_entries()
    pending_tables = {(namespace_value(entry["namespace"]), entry["table"]) for entry in pending if entry["action"] == "drop_table"}
    pending_namespaces = {namespace_value(entry["namespace"]) for entry in pending if entry["action"] == "drop_namespace"}
    pending_properties = {namespace_value(entry["namespace"]) for entry in pending if entry["action"] == "namespace_properties"}
    removed_tables = sorted((set(before) - set(after)) | pending_tables, reverse=True)
    removed_namespaces = sorted((set(old_namespaces) - set(new_namespaces)) | pending_namespaces, key=lambda value: (len(value), value), reverse=True)
    changed = {name for name in old_namespaces.keys() & new_namespaces.keys() if old_namespaces[name] != new_namespaces[name]}
    changed.update(pending_properties)
    for namespace, table in removed_tables:
        state = latest.get(".".join(namespace))
        if (namespace, table) not in table_map(original) or (namespace, table) in after or state is None or table in state["tables"]:
            raise MigrationError("reverse_conflict", "a reverse table removal lacks a confirmed deletion")
    for namespace in removed_namespaces:
        state = latest.get(".".join(namespace))
        if namespace not in namespace_map(original) or namespace in new_namespaces or state is None or state["state"] != "absent":
            raise MigrationError("reverse_conflict", "a reverse namespace removal lacks confirmed absence")
    for namespace in changed:
        state = latest.get(".".join(namespace))
        if namespace not in new_namespaces or state is None or state["state"] != "present" or state["properties"] != new_namespaces[namespace]:
            raise MigrationError("reverse_conflict", "a reverse namespace property change lacks confirmation")
    refresh_plan(io, expected, actual, deadline=deadline)
    for namespace, table in removed_tables:
        prior = before.get((namespace, table))
        command = io.records.start("drop_table", namespace, table=table, old=prior["metadata_location"] if prior else None, secret_values=io.secret_values)
        if io.destination.load((namespace, table), deadline=deadline) is not None:
            io.destination.drop_table((namespace, table), deadline=deadline)
        io.records.finish(command, observe_namespace(io, namespace, deadline=deadline), secret_values=io.secret_values)
    for namespace in removed_namespaces:
        command = io.records.start("drop_namespace", namespace, secret_values=io.secret_values)
        if io.destination.namespace(namespace, deadline=deadline) is not None:
            io.destination.drop_namespace(namespace, deadline=deadline)
        io.records.finish(command, observe_namespace(io, namespace, deadline=deadline), secret_values=io.secret_values)
    for namespace in sorted(changed):
        command = io.records.start("namespace_properties", namespace, secret_values=io.secret_values)
        actual_properties = io.destination.namespace(namespace, deadline=deadline)
        if actual_properties is None:
            raise MigrationError("reverse_conflict", "the property-update namespace disappeared after preflight")
        current_properties = properties(actual_properties["properties"])
        if current_properties != new_namespaces[namespace]:
            io.destination.update_properties(namespace, current_properties, new_namespaces[namespace], deadline=deadline)
        io.records.finish(command, observe_namespace(io, namespace, deadline=deadline), secret_values=io.secret_values)
    refreshed = refresh(io, frozen, deadline=deadline)
    return {
        "refresh": refreshed,
        "parity": parity(io, frozen, expected_grants=expected_grants, deadline=deadline),
        "final_source": final_read(io, frozen, deadline=deadline),
    }


def replay_inventory(io: MigrationIO, frozen: Document, states: Document, *, deadline: float) -> Document:
    result = copy.deepcopy(frozen_inventory(io, frozen))
    result["namespaces"], result["tables"] = [], []
    for name, state in sorted(states.items()):
        if state["state"] == "absent":
            continue
        namespace = namespace_value(name.split("."))
        result["namespaces"].append({"namespace": list(namespace), "properties": state["properties"]})
        for table, head in sorted(state["tables"].items()):
            metadata, identity, _ = object_metadata(io.source_objects, head["metadata_location"], head["version_id"], deadline=deadline, clock=io.clock)
            projected = reading(namespace, table, head["metadata_location"], metadata)
            if any(projected[key] != head[field] for key, field in (("table-uuid", "uuid"), ("current-snapshot-id", "snapshot_id"), ("refs", "refs"))):
                raise MigrationError("journal_head", "the confirmed journal head differs from its pinned metadata")
            row = {
                "namespace": list(namespace),
                "name": table,
                "metadata_location": head["metadata_location"],
                "object": identity,
                "readings": {"catalog": projected, "object": copy.deepcopy(projected)},
                "covered_paths": [io.scope.path(metadata["location"]), io.scope.path(head["metadata_location"].rsplit("/", 1)[0] + "/")],
            }
            pin_for(row)
            result["tables"].append(row)
    result["counts"] = {"namespaces": len(result["namespaces"]), "tables": len(result["tables"]), "views": 0}
    return result


def rebuild(
    io: MigrationIO,
    frozen: Document,
    intents: dict[str, Document],
    completions: list[Document],
    *,
    epoch: str,
    quiesced: bool,
    expected_grants: Document,
    deadline: float,
) -> Document:
    """Rebuild confirmed namespace replacements and leave pending namespaces uncreated.

    The caller must first stop or reconcile every writer and owner command. The
    original live database alone may confirm pending intents before this call.
    The destination is isolated. Existing content must already match the replay.
    """
    frozen_inventory(io, frozen)
    replayed = confirmed_replay(frozen, intents, completions, epoch=epoch, quiesced=quiesced)
    unresolved = set(replayed["unresolved"])
    descendants = {name for name in replayed["namespace_states"] if any(name.startswith(parent + ".") for parent in unresolved)}
    unresolved.update(descendants)
    states = {name: state for name, state in replayed["namespace_states"].items() if name not in unresolved}
    expected = replay_inventory(io, frozen, states, deadline=deadline)
    actual = inventory(io.destination, io.destination_objects, io.scope, deadline=deadline, clock=io.clock, required=False, secret_values=io.secret_values)
    membership(actual, expected, complete=False)
    current = table_map(actual)
    if any(row != current[identifier] for identifier, row in table_map(expected).items() if identifier in current):
        for identifier in current:
            target = table_map(expected)[identifier]
            if current[identifier]["object"] != target["object"] or current[identifier]["readings"] != target["readings"]:
                raise MigrationError("rebuild_conflict", "the isolated rebuild destination contains a different table head")
    for namespace, value in sorted(namespace_map(expected).items(), key=lambda item: (len(item[0]), item[0])):
        ensure_namespace(io, namespace, value, deadline=deadline)
    for row in expected["tables"]:
        register_row(io, row, None, deadline=deadline)
    actual = inventory(io.destination, io.destination_objects, io.scope, deadline=deadline, clock=io.clock, required=False, secret_values=io.secret_values)
    membership(actual, expected, complete=True)
    for identifier, row in table_map(expected).items():
        found = table_map(actual)[identifier]
        if found["object"] != row["object"] or found["readings"] != row["readings"]:
            raise MigrationError("four_readings", "the rebuilt table differs from the confirmed journal head")
    if actual["grants"] != {role: sorted(entries, key=registration.canonical) for role, entries in sorted(expected_grants.items())}:
        raise MigrationError("grant_parity", "rebuilt grants differ from the supplied phase contract")
    result = {
        "schema": 1,
        "state": "recovering" if unresolved else "rebuilt",
        "epoch": epoch,
        "pending_intents": replayed["pending_intents"],
        "unresolved": sorted(unresolved),
        "blocked_layers": replayed["blocked_layers"],
        "namespace_states": states,
        "parity": {"equal": True, "counts": actual["counts"]},
        "registration_receipt": registration_receipt(io, expected["tables"], [table_identifier(row) for row in expected["tables"]], []),
    }
    io.records.save("rebuild", result, secret_values=io.secret_values, deadline=deadline, clock=io.clock)
    return result
