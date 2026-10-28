"""Register a retained read inventory from catalog-confirmed, version-pinned Iceberg metadata.

Call ``register_read_catalog`` with authenticated, deadline-bounded I/O. The source
publication reader uses the existing authoritative ``publish_runs`` reader. The
metadata reader returns the requested S3 version, or the current version when its
version argument is None. Neither interface may substitute cached data.

``register_exact_metadata`` is also available to catalog migration callers. It
registers an absent table and adopts an identical table. Replacement policy stays
with the migration caller. This module does not fetch credentials, create data,
release snapshot protections or start a transport.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
import time
import urllib.parse
import uuid
from collections.abc import Callable, Mapping, Set
from dataclasses import asdict, dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, Protocol

from scripts.lakehouse.gold_publish_record import READER_NAME, RUN_ID, SHA256

MAX_RECORD = 32 * 1024 * 1024
MAX_SNAPSHOT = (1 << 63) - 1
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,127}")
TableId = tuple[str, str]
Clock = Callable[[], float]


class RegistrationError(RuntimeError):
    """A sanitized refusal with a stable cause code and no remote response body."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ObjectValue:
    """One metadata object's returned version and bytes, held only in memory."""

    version_id: str
    body: bytes = field(repr=False)


class MetadataReader(Protocol):
    def read(self, location: str, version_id: str | None, *, deadline: float) -> ObjectValue:
        """Read the exact version, or the current version, before the fixed deadline."""
        ...


class AuthenticatedRequest(Protocol):
    def __call__(self, method: str, path: str, *, body: Mapping[str, Any] | None = None, deadline: float) -> tuple[int, object]:
        """Use the caller's authenticated management seam and bounded transport."""
        ...


class Catalog(Protocol):
    def ensure_namespace(self, namespace: str, *, deadline: float) -> None:
        """Create a missing read namespace without changing existing properties."""
        ...

    def load(self, identifier: TableId, *, deadline: float) -> Mapping[str, Any] | None:
        """Return the table response, or None only for a table-not-found response."""
        ...

    def register(self, identifier: TableId, metadata_location: str, *, deadline: float) -> None:
        """Register without replacement, then allow the caller to verify the result."""
        ...


class PublicationReader(Protocol):
    def read(self, namespace: str, run_id: str, *, deadline: float) -> Mapping[str, Any] | None:
        """Read the authoritative committed publication row, not a local receipt."""
        ...


class Reservations(Protocol):
    def ensure(self, reader: str, publish_run: str, *, deadline: float) -> None:
        """Acquire source protection or verify the same existing protection."""
        ...

    def verify(self, reader: str, publish_run: str, *, deadline: float) -> None:
        """Refuse when this exact source protection is absent or changed."""
        ...


class LauncherClient(Protocol):
    def call(self, method: str, params: dict[str, Any], *, timeout: float) -> Any:
        """Make a bounded request through an already selected launcher client."""
        ...


@dataclass(frozen=True)
class TablePin:
    """The exact metadata identity and retained snapshot of one required table."""

    namespace: str
    name: str
    table_uuid: str
    location: str
    metadata_location: str
    metadata_version: str
    metadata_sha256: str
    metadata_size: int
    snapshot_id: int
    current_snapshot_id: int

    @property
    def identifier(self) -> TableId:
        """The table's catalog identifier, independent of its storage location."""
        return self.namespace, self.name


@dataclass(frozen=True)
class PublishPin:
    """A committed prerequisite receipt bound to its source snapshot protection."""

    layer: str
    run_id: str
    receipt_sha256: str
    reservation: str


@dataclass(frozen=True)
class Inventory:
    """An immutable read view that can be retained and passed to refresh callers."""

    publications: tuple[PublishPin, ...]
    tables: tuple[TablePin, ...]

    def document(self) -> dict[str, Any]:
        """Return only portable identities and digests, without response bodies."""
        return {
            "schema": 1,
            "source_catalog": "mac",
            "destination_catalog": "aws",
            "publications": [asdict(pin) for pin in self.publications],
            "tables": [asdict(pin) for pin in self.tables],
        }

    @property
    def sha256(self) -> str:
        """The digest of this inventory's canonical JSON bytes."""
        return hashlib.sha256(canonical(self.document())).hexdigest()


@dataclass(frozen=True)
class RegistrationResult:
    """A complete verified inventory and this invocation's registration outcomes."""

    inventory: Inventory
    registered: tuple[TableId, ...]
    unchanged: tuple[TableId, ...]
    state: str = field(default="verified", init=False)

    def document(self) -> dict[str, Any]:
        """Return a sanitized acceptance receipt only after complete verification."""
        return {
            "schema": 1,
            "state": self.state,
            "inventory_sha256": self.inventory.sha256,
            "inventory": self.inventory.document(),
            "registered": self.registered,
            "unchanged": self.unchanged,
        }


@dataclass(frozen=True)
class RegistrationIO:
    """The caller's authenticated, bounded source, destination and reader services."""

    source: Catalog
    destination: Catalog
    reader: Catalog
    objects: MetadataReader
    publications: PublicationReader
    reservations: Reservations
    clock: Clock = time.monotonic


def canonical(value: object) -> bytes:
    """Encode portable records in stable key order with one final newline."""
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def check_deadline(deadline: float, clock: Clock) -> None:
    """Refuse work that starts or returns at or after the caller's deadline."""
    if not math.isfinite(deadline) or clock() >= deadline:
        raise RegistrationError("deadline", "catalog registration exceeded its fixed deadline")


def bounded[T](operation: Callable[[], T], deadline: float, clock: Clock) -> T:
    check_deadline(deadline, clock)
    try:
        result = operation()
    except OSError:
        raise RegistrationError("transport", "catalog registration transport failed") from None
    check_deadline(deadline, clock)
    return result


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def invalid_json_constant(_value: str) -> None:
    raise ValueError("non-finite JSON number")


def read_document(raw: bytes, code: str) -> dict[str, Any]:
    if len(raw) > MAX_RECORD:
        raise RegistrationError(code, "a registration record exceeds the 32 MiB bound")
    try:
        value = json.loads(raw, object_pairs_hook=unique_object, parse_constant=invalid_json_constant)
    except (ValueError, UnicodeError):
        raise RegistrationError(code, "a registration record is not valid JSON") from None
    if not isinstance(value, dict):
        raise RegistrationError(code, "a registration record must be a JSON object")
    return value


def plain_identifier(identifier: TableId) -> None:
    if len(identifier) != 2 or not all(isinstance(value, str) and IDENTIFIER.fullmatch(value) for value in identifier):
        raise RegistrationError("inventory", "a catalog table identifier is invalid")


def s3_location(value: object) -> str:
    if not isinstance(value, str):
        raise RegistrationError("metadata_identity", "a metadata location is not an S3 URI")
    uri = urllib.parse.urlsplit(value)
    if (
        uri.scheme != "s3"
        or not uri.netloc
        or not re.fullmatch(r"[a-z0-9][a-z0-9.-]+", uri.netloc)
        or uri.query
        or uri.fragment
        or not uri.path.startswith("/lakehouse/")
        or any(part in ("", ".", "..") for part in uri.path[1:].split("/"))
        or re.search(r"[\s\x00-\x1f\x7f]", value)
    ):
        raise RegistrationError("metadata_identity", "a metadata location is outside the lakehouse S3 scope")
    return value


def bucket_location(value: object, bucket: str) -> str:
    location = s3_location(value)
    if urllib.parse.urlsplit(location).netloc != bucket:
        raise RegistrationError("metadata_identity", "a metadata location names a different deployment bucket")
    return location


def snapshot_id(value: object, code: str) -> int:
    if type(value) is not int or not 0 < value <= MAX_SNAPSHOT:
        raise RegistrationError(code, "a required snapshot identity is invalid")
    return value


def snapshots(document: Mapping[str, Any], code: str) -> dict[int, Mapping[str, Any]]:
    entries = document.get("snapshots")
    if not isinstance(entries, list):
        raise RegistrationError(code, "table metadata has no retained snapshot list")
    result: dict[int, Mapping[str, Any]] = {}
    for item in entries:
        if not isinstance(item, dict):
            raise RegistrationError(code, "a retained snapshot is invalid")
        identity = snapshot_id(item.get("snapshot-id"), code)
        if identity in result or not isinstance(item.get("manifest-list"), str):
            raise RegistrationError(code, "a retained snapshot is duplicated or has no manifest")
        result[identity] = item
    return result


def validate_pin(pin: TablePin) -> None:
    plain_identifier(pin.identifier)
    try:
        valid_uuid = isinstance(pin.table_uuid, str) and str(uuid.UUID(pin.table_uuid)) == pin.table_uuid
    except ValueError:
        valid_uuid = False
    if not valid_uuid or not isinstance(pin.metadata_sha256, str) or not SHA256.fullmatch(pin.metadata_sha256):
        raise RegistrationError("metadata_identity", "a table pin has no valid UUID or metadata digest")
    if not isinstance(pin.metadata_version, str) or pin.metadata_version in ("", "null") or re.search(r"[\s\x00-\x1f\x7f]", pin.metadata_version):
        raise RegistrationError("metadata_identity", "a table pin has no immutable metadata version")
    if type(pin.metadata_size) is not int or not 0 < pin.metadata_size <= MAX_RECORD:
        raise RegistrationError("metadata_identity", "a metadata size is invalid or exceeds 32 MiB")
    if urllib.parse.urlsplit(s3_location(pin.location)).netloc != urllib.parse.urlsplit(s3_location(pin.metadata_location)).netloc:
        raise RegistrationError("metadata_identity", "metadata and table locations name different buckets")
    snapshot_id(pin.snapshot_id, "metadata_identity")
    snapshot_id(pin.current_snapshot_id, "metadata_identity")


def check_coverage(inventory: Inventory, required: Set[TableId]) -> None:
    """Require exactly one pin for every independently declared source table."""
    for identifier in required:
        plain_identifier(identifier)
    identifiers = [pin.identifier for pin in inventory.tables]
    if not required or len(identifiers) != len(set(identifiers)) or set(identifiers) != set(required):
        raise RegistrationError("coverage", "the inventory does not name every required table exactly once")


def check_object(pin: TablePin, value: ObjectValue) -> None:
    """Compare object version, digest and size with the complete frozen identity."""
    if value.version_id != pin.metadata_version or len(value.body) != pin.metadata_size or hashlib.sha256(value.body).hexdigest() != pin.metadata_sha256:
        raise RegistrationError("metadata_identity", "metadata object bytes or version differ from the frozen inventory")


def pinned_metadata(objects: MetadataReader, pin: TablePin, deadline: float, clock: Clock) -> dict[str, Any]:
    value = bounded(lambda: objects.read(pin.metadata_location, pin.metadata_version, deadline=deadline), deadline, clock)
    check_object(pin, value)
    document = read_document(value.body, "metadata_identity")
    if (
        document.get("table-uuid") != pin.table_uuid
        or document.get("location") != pin.location
        or snapshot_id(document.get("current-snapshot-id"), "metadata_identity") != pin.current_snapshot_id
    ):
        raise RegistrationError("metadata_identity", "metadata identifies another table or current snapshot")
    if pin.snapshot_id not in snapshots(document, "metadata_identity") or pin.current_snapshot_id not in snapshots(document, "metadata_identity"):
        raise RegistrationError("metadata_identity", "the frozen metadata does not retain the required snapshots")
    return document


def current_object(objects: MetadataReader, pin: TablePin, deadline: float, clock: Clock) -> None:
    value = bounded(lambda: objects.read(pin.metadata_location, None, deadline=deadline), deadline, clock)
    check_object(pin, value)


def first_difference(actual: object, expected: object, path: str) -> str | None:
    if type(actual) is not type(expected):
        return path
    if isinstance(actual, dict) and isinstance(expected, dict):
        for key in sorted(actual.keys() | expected.keys(), key=str):
            child = path + "/" + str(key).replace("~", "~0").replace("/", "~1")
            if key not in actual or key not in expected:
                return child
            difference = first_difference(actual[key], expected[key], child)
            if difference is not None:
                return difference
    elif isinstance(actual, list) and isinstance(expected, list):
        for index, (left, right) in enumerate(zip(actual, expected, strict=False)):
            difference = first_difference(left, right, path + "/" + str(index))
            if difference is not None:
                return difference
        if len(actual) != len(expected):
            return path + "/" + str(min(len(actual), len(expected)))
    elif actual != expected:
        return path
    return None


def check_loaded(loaded: Mapping[str, Any] | None, pin: TablePin, metadata: Mapping[str, Any], code: str = "registration_conflict") -> None:
    """Require exact metadata and identify only the first differing JSON path."""
    if loaded is None:
        difference = "/"
    elif loaded.get("metadata-location") != pin.metadata_location:
        difference = "/metadata-location"
    else:
        try:
            matches = canonical(loaded.get("metadata")) == canonical(metadata)
        except (TypeError, ValueError):
            difference = first_difference(loaded.get("metadata"), metadata, "/metadata") or "/metadata"
            raise RegistrationError(code, f"catalog metadata is not valid JSON for {pin.namespace}.{pin.name} at JSON path {json.dumps(difference)}") from None
        if matches:
            return
        difference = first_difference(loaded.get("metadata"), metadata, "/metadata") or "/metadata"
    raise RegistrationError(code, f"the catalog metadata differs for {pin.namespace}.{pin.name} at JSON path {json.dumps(difference)}")


def check_source_snapshot(source: Mapping[str, Any] | None, pin: TablePin, frozen: Mapping[str, Any]) -> None:
    """Allow a newer source head only while its UUID, location and retained snapshot agree."""
    metadata = source.get("metadata") if source is not None else None
    if not isinstance(metadata, dict) or metadata.get("table-uuid") != pin.table_uuid or metadata.get("location") != pin.location:
        raise RegistrationError("source_snapshot", "the source no longer identifies the retained table")
    if snapshots(metadata, "source_snapshot").get(pin.snapshot_id) != snapshots(frozen, "source_snapshot").get(pin.snapshot_id):
        raise RegistrationError("source_snapshot", "the source no longer retains the exact required snapshot")


def checked_receipts(receipts: Mapping[str, bytes], required: Set[TableId]) -> dict[str, dict[str, Any]]:
    if set(receipts) != {"silver", "gold"}:
        raise RegistrationError("publication", "both successful real publish receipts are required")
    result = {}
    for layer in ("silver", "gold"):
        document = read_document(receipts[layer], "publication")
        expected = {name for namespace, name in required if namespace == layer and name != "publish_runs"}
        pins = document.get("snapshot_set")
        if (
            type(document.get("schema")) is not int
            or document["schema"] != 1
            or document.get("state") != "committed"
            or not RUN_ID.fullmatch(str(document.get("run_id", "")))
            or not SHA256.fullmatch(str(document.get("backup_sha256", "")))
            or not isinstance(pins, dict)
            or not expected
            or set(pins) != expected
        ):
            raise RegistrationError("publication", "a prerequisite publish receipt is incomplete or uncommitted")
        for value in pins.values():
            snapshot_id(value, "publication")
        result[layer] = document
    return result


def check_publications(io: RegistrationIO, documents: Mapping[str, Mapping[str, Any]], deadline: float) -> None:
    """Compare both receipts with real authoritative committed publication rows."""
    for layer in ("silver", "gold"):
        receipt = documents[layer]
        row = bounded(partial(io.publications.read, layer, str(receipt["run_id"]), deadline=deadline), deadline, io.clock)
        if row is None or row.get("mode") != "real" or any(row.get(key) != receipt[key] for key in ("run_id", "snapshot_set", "backup_sha256")):
            raise RegistrationError("publication", "a receipt does not match its authoritative real publication")


def publication_pins(documents: Mapping[str, Mapping[str, Any]], receipts: Mapping[str, bytes], prefix: str) -> tuple[PublishPin, ...]:
    result = tuple(
        PublishPin(layer, str(documents[layer]["run_id"]), hashlib.sha256(receipts[layer]).hexdigest(), f"{prefix}:{layer}") for layer in ("silver", "gold")
    )
    if not all(READER_NAME.fullmatch(pin.reservation) for pin in result):
        raise RegistrationError("reservation", "the source reader reservation name is invalid")
    return result


def check_reservations(io: RegistrationIO, inventory: Inventory, deadline: float) -> None:
    """Verify both source protections without removing or changing either one."""
    for pin in inventory.publications:
        bounded(partial(io.reservations.verify, pin.reservation, pin.run_id, deadline=deadline), deadline, io.clock)


def capture_table(io: RegistrationIO, identifier: TableId, retained: int | None, bucket: str, deadline: float) -> TablePin:
    loaded = bounded(lambda: io.source.load(identifier, deadline=deadline), deadline, io.clock)
    if loaded is None:
        raise RegistrationError("source_missing", "a required source table is absent")
    metadata = loaded.get("metadata")
    if not isinstance(metadata, dict):
        raise RegistrationError("metadata_identity", "the source returned no table metadata")
    location = bucket_location(loaded.get("metadata-location"), bucket)
    value = bounded(lambda: io.objects.read(location, None, deadline=deadline), deadline, io.clock)
    document = read_document(value.body, "metadata_identity")
    current = snapshot_id(document.get("current-snapshot-id"), "metadata_identity")
    table_uuid = document.get("table-uuid")
    if not isinstance(table_uuid, str):
        raise RegistrationError("metadata_identity", "table metadata has no UUID")
    table_location = bucket_location(document.get("location"), bucket)
    pin = TablePin(
        identifier[0],
        identifier[1],
        table_uuid,
        table_location,
        location,
        value.version_id,
        hashlib.sha256(value.body).hexdigest(),
        len(value.body),
        current if retained is None else retained,
        current,
    )
    validate_pin(pin)
    pinned_metadata(io.objects, pin, deadline, io.clock)
    check_loaded(loaded, pin, document, "metadata_identity")
    return pin


def prepare_read_inventory(
    io: RegistrationIO, required: Set[TableId], receipts: Mapping[str, bytes], *, bucket: str, reservation_prefix: str, deadline: float
) -> Inventory:
    """Capture exact catalog metadata after both real publishes, retaining their source snapshots.

    Required tables must come from the committed source contracts. The source
    publication service must read authoritative committed rows. Reservations stay
    held after success and failure, until the read view is explicitly retired.
    The bucket comes from the verified deployment. Every supplied I/O service
    must enforce the same absolute deadline.
    """
    check_deadline(deadline, io.clock)
    documents = checked_receipts(receipts, required)
    publications = publication_pins(documents, receipts, reservation_prefix)
    for pin in publications:
        bounded(partial(io.reservations.ensure, pin.reservation, pin.run_id, deadline=deadline), deadline, io.clock)
    check_publications(io, documents, deadline)
    tables = []
    for identifier in sorted(required):
        plain_identifier(identifier)
        retained = documents.get(identifier[0], {}).get("snapshot_set", {}).get(identifier[1])
        tables.append(capture_table(io, identifier, retained, bucket, deadline))
    inventory = Inventory(publications, tuple(tables))
    check_coverage(inventory, required)
    check_reservations(io, inventory, deadline)
    return inventory


def register_exact_metadata(
    destination: Catalog, objects: MetadataReader, pin: TablePin, *, bucket: str, deadline: float, clock: Clock = time.monotonic
) -> str:
    """Register one exact metadata location or adopt the identical existing table.

    The caller supplies the verified deployment bucket and owns namespace
    preparation, source retention and replacement
    policy. A conflicting table is never overwritten. A lost registration reply
    propagates without another POST. An explicit retry first reads the table.
    """
    check_deadline(deadline, clock)
    validate_pin(pin)
    bucket_location(pin.metadata_location, bucket)
    metadata = pinned_metadata(objects, pin, deadline, clock)
    current_object(objects, pin, deadline, clock)
    loaded = bounded(lambda: destination.load(pin.identifier, deadline=deadline), deadline, clock)
    if loaded is not None:
        check_loaded(loaded, pin, metadata)
        outcome = "unchanged"
    else:
        bounded(lambda: destination.register(pin.identifier, pin.metadata_location, deadline=deadline), deadline, clock)
        loaded = bounded(lambda: destination.load(pin.identifier, deadline=deadline), deadline, clock)
        check_loaded(loaded, pin, metadata)
        outcome = "registered"
    current_object(objects, pin, deadline, clock)
    return outcome


def verify_reader(io: RegistrationIO, inventory: Inventory, metadata: Mapping[TableId, Mapping[str, Any]], deadline: float) -> None:
    """Verify every required table through the distinct AWS reader identity."""
    for pin in inventory.tables:
        loaded = bounded(partial(io.reader.load, pin.identifier, deadline=deadline), deadline, io.clock)
        check_loaded(loaded, pin, metadata[pin.identifier], "reader_inventory")


def register_read_catalog(
    io: RegistrationIO,
    required: Set[TableId],
    receipts: Mapping[str, bytes],
    *,
    bucket: str,
    reservation_prefix: str,
    deadline: float,
    inventory: Inventory | None = None,
    inventory_sha256: str | None = None,
) -> RegistrationResult:
    """Preflight, register and accept a complete retained read view.

    Pass the returned immutable inventory on an explicit retry. All source and
    destination conflicts are checked before mutation. Partial failures return no
    acceptance receipt and retain source protections. The caller supplies the
    verified deployment bucket. No writer is admitted.
    """
    check_deadline(deadline, io.clock)
    documents = checked_receipts(receipts, required)
    expected_publications = publication_pins(documents, receipts, reservation_prefix)
    if inventory is None:
        inventory = prepare_read_inventory(io, required, receipts, bucket=bucket, reservation_prefix=reservation_prefix, deadline=deadline)
    elif not isinstance(inventory_sha256, str) or not SHA256.fullmatch(inventory_sha256) or inventory.sha256 != inventory_sha256:
        raise RegistrationError("inventory", "the resumed inventory differs from its independently frozen digest")
    if inventory.publications != expected_publications:
        raise RegistrationError("publication", "the frozen inventory belongs to different publish receipts or reservations")
    check_coverage(inventory, required)
    for pin in inventory.tables:
        if pin.namespace in documents and pin.name != "publish_runs" and pin.snapshot_id != documents[pin.namespace]["snapshot_set"][pin.name]:
            raise RegistrationError("publication", "the frozen table snapshot differs from its committed publication")
    check_reservations(io, inventory, deadline)
    check_publications(io, documents, deadline)
    metadata = {}
    for pin in inventory.tables:
        validate_pin(pin)
        bucket_location(pin.metadata_location, bucket)
        document = pinned_metadata(io.objects, pin, deadline, io.clock)
        current_object(io.objects, pin, deadline, io.clock)
        source = bounded(partial(io.source.load, pin.identifier, deadline=deadline), deadline, io.clock)
        check_source_snapshot(source, pin, document)
        loaded = bounded(partial(io.destination.load, pin.identifier, deadline=deadline), deadline, io.clock)
        if loaded is not None:
            check_loaded(loaded, pin, document)
        metadata[pin.identifier] = document
    for namespace in sorted({pin.namespace for pin in inventory.tables}):
        bounded(partial(io.destination.ensure_namespace, namespace, deadline=deadline), deadline, io.clock)
    outcomes: dict[str, list[TableId]] = {"registered": [], "unchanged": []}
    for pin in inventory.tables:
        check_reservations(io, inventory, deadline)
        outcome = register_exact_metadata(io.destination, io.objects, pin, bucket=bucket, deadline=deadline, clock=io.clock)
        outcomes[outcome].append(pin.identifier)
    verify_reader(io, inventory, metadata, deadline)
    for pin in inventory.tables:
        current_object(io.objects, pin, deadline, io.clock)
        source = bounded(partial(io.source.load, pin.identifier, deadline=deadline), deadline, io.clock)
        check_source_snapshot(source, pin, metadata[pin.identifier])
    check_publications(io, documents, deadline)
    check_reservations(io, inventory, deadline)
    check_deadline(deadline, io.clock)
    return RegistrationResult(inventory, tuple(outcomes["registered"]), tuple(outcomes["unchanged"]))


def required_tables(root: Path) -> frozenset[TableId]:
    """Derive the read increment's required tables from committed contracts only."""
    bronze = read_document((root / "config/lakehouse/bronze_tables.json").read_bytes(), "coverage")
    result = {("bronze", row["table"]) for row in bronze["tables"]} | {("bronze", "file_metadata")}
    for namespace, name in (("silver", "silver_publish.json"), ("gold", "gold_contract.json")):
        contract = read_document((root / "data_contracts/lakehouse" / name).read_bytes(), "coverage")["publish"]
        result.update((namespace, table) for table in contract["tables"])
        result.add((namespace, contract["runs_table"]))
    for identifier in result:
        plain_identifier(identifier)
    return frozenset(result)


def inventory_from_document(document: Mapping[str, Any], required: Set[TableId]) -> Inventory:
    """Parse a portable inventory, refusing unknown fields and malformed identities."""
    if (
        set(document) != {"schema", "source_catalog", "destination_catalog", "publications", "tables"}
        or type(document.get("schema")) is not int
        or document["schema"] != 1
        or document.get("source_catalog") != "mac"
        or document.get("destination_catalog") != "aws"
    ):
        raise RegistrationError("inventory", "the inventory version, origin or fields are invalid")
    if not isinstance(document["tables"], list) or not isinstance(document["publications"], list):
        raise RegistrationError("inventory", "the inventory table and publication fields must be lists")
    try:
        publications = tuple(PublishPin(**item) for item in document["publications"])
        tables = tuple(TablePin(**item) for item in document["tables"])
    except TypeError:
        raise RegistrationError("inventory", "an inventory entry has missing or unexpected fields") from None
    for publication in publications:
        if not all(isinstance(value, str) for value in asdict(publication).values()):
            raise RegistrationError("publication", "an inventory publication identity has an invalid type")
    if len(publications) != 2 or {pin.layer for pin in publications} != {"silver", "gold"} or len({pin.reservation for pin in publications}) != 2:
        raise RegistrationError("publication", "the inventory must retain both prerequisite publications separately")
    for publication in publications:
        if (
            not all(isinstance(value, str) for value in asdict(publication).values())
            or not RUN_ID.fullmatch(publication.run_id)
            or not SHA256.fullmatch(publication.receipt_sha256)
            or not READER_NAME.fullmatch(publication.reservation)
        ):
            raise RegistrationError("publication", "an inventory publication identity is invalid")
    for table in tables:
        validate_pin(table)
    inventory = Inventory(
        tuple(sorted(publications, key=lambda pin: ("silver", "gold").index(pin.layer))), tuple(sorted(tables, key=lambda pin: pin.identifier))
    )
    check_coverage(inventory, required)
    return inventory


class PolarisCatalog:
    """Use an authenticated bounded request without changing credentials or grants."""

    def __init__(self, request: AuthenticatedRequest, catalog_name: str, *, clock: Clock = time.monotonic) -> None:
        if not IDENTIFIER.fullmatch(catalog_name):
            raise RegistrationError("inventory", "the Polaris catalog name is invalid")
        self.request, self.clock = request, clock
        self.base = f"/api/catalog/v1/{catalog_name}/namespaces"

    def send(self, method: str, path: str, *, body: Mapping[str, Any] | None = None, deadline: float) -> tuple[int, object]:
        return bounded(partial(self.request, method, path, body=body, deadline=deadline), deadline, self.clock)

    def ensure_namespace(self, namespace: str, *, deadline: float) -> None:
        """Create an absent namespace and preserve every existing namespace property."""
        plain_identifier((namespace, "namespace"))
        path = f"{self.base}/{namespace}"
        status, document = self.send("GET", path, deadline=deadline)
        if status == 404:
            status, _ = self.send("POST", self.base, body={"namespace": [namespace]}, deadline=deadline)
            if status not in (200, 201, 409):
                raise RegistrationError("catalog_http", f"creating a read namespace failed (HTTP {status})")
            status, document = self.send("GET", path, deadline=deadline)
        if status != 200 or not isinstance(document, dict) or document.get("namespace") != [namespace]:
            raise RegistrationError("catalog_http", f"verifying a read namespace failed (HTTP {status})")

    def load(self, identifier: TableId, *, deadline: float) -> Mapping[str, Any] | None:
        """Load one table, treating only HTTP 404 as absence."""
        plain_identifier(identifier)
        status, document = self.send("GET", f"{self.base}/{identifier[0]}/tables/{identifier[1]}", deadline=deadline)
        if status == 404:
            return None
        if status != 200 or not isinstance(document, dict):
            raise RegistrationError("catalog_http", f"loading catalog metadata failed (HTTP {status})")
        return document

    def register(self, identifier: TableId, metadata_location: str, *, deadline: float) -> None:
        """Register an absent table without overwrite, then let the caller compare it."""
        plain_identifier(identifier)
        status, _ = self.send(
            "POST",
            f"{self.base}/{identifier[0]}/register",
            body={"name": identifier[1], "metadata-location": s3_location(metadata_location)},
            deadline=deadline,
        )
        if status not in (200, 201, 409):
            raise RegistrationError("catalog_http", f"registering exact catalog metadata failed (HTTP {status})")


class LauncherReservations:
    """Keep source snapshot protections in the existing Mac reader protocol."""

    def __init__(self, client: LauncherClient, *, pid: int | None = None, clock: Clock = time.monotonic) -> None:
        self.client, self.pid, self.clock = client, os.getpid() if pid is None else pid, clock

    def call(self, method: str, params: dict[str, Any], deadline: float) -> Any:
        check_deadline(deadline, self.clock)
        return bounded(lambda: self.client.call(method, params, timeout=deadline - self.clock()), deadline, self.clock)

    def entry(self, reader: str, deadline: float) -> Mapping[str, Any] | None:
        view = self.call("status", {"what": "registry"}, deadline)
        if not isinstance(view, dict) or not isinstance(view.get("readers"), dict):
            raise RegistrationError("reservation", "the launcher returned no reader protection inventory")
        entry = view["readers"].get(reader)
        if entry is not None and not isinstance(entry, dict):
            raise RegistrationError("reservation", "the launcher reader protection is malformed")
        return entry

    def ensure(self, reader: str, publish_run: str, *, deadline: float) -> None:
        """Reserve this publication once, accepting only an identical existing entry."""
        if not READER_NAME.fullmatch(reader) or not RUN_ID.fullmatch(publish_run):
            raise RegistrationError("reservation", "the source reservation identity is invalid")
        if self.entry(reader, deadline) is None:
            self.call("reserve", {"reader": reader, "domain": "transform", "publish_run": publish_run, "pid": self.pid}, deadline)
        self.verify(reader, publish_run, deadline=deadline)

    def verify(self, reader: str, publish_run: str, *, deadline: float) -> None:
        """Keep protection until explicit retirement, including after the owner process exits."""
        entry = self.entry(reader, deadline)
        if (
            entry is None
            or entry.get("domain") != "transform"
            or entry.get("publish_run") != publish_run
            or entry.get("state") not in ("reserved", "registered")
            or (entry.get("state") == "registered" and entry.get("snapshot_set") != publish_run)
        ):
            raise RegistrationError("reservation", "the source snapshot protection is absent or belongs to another publication")


def main() -> int:
    """Check a saved inventory's shape and digest without connecting to any service."""
    parser = argparse.ArgumentParser(
        description="Check a frozen catalog inventory. Native registration calls register_read_catalog with bounded authenticated I/O."
    )
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    args = parser.parse_args()
    try:
        inventory = inventory_from_document(read_document(args.inventory.read_bytes(), "inventory"), required_tables(Path(__file__).resolve().parents[2]))
        if not SHA256.fullmatch(args.sha256) or inventory.sha256 != args.sha256:
            raise RegistrationError("inventory", "the saved inventory differs from its frozen digest")
        sys.stdout.write(json.dumps({"tables": len(inventory.tables), "inventory_sha256": inventory.sha256, "native_registration": "unverified"}) + "\n")
        return 0
    except (RegistrationError, OSError, ValueError, KeyError, TypeError) as error:
        code = error.code if isinstance(error, RegistrationError) else "inventory"
        sys.stderr.write(f"catalog registration: {code}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
