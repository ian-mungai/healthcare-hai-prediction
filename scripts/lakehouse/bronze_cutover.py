"""Register the frozen bronze inventory on the AWS catalog through the owned Session Manager route, verify it and write the receipt once.

The AWS catalog is reached only through ``orchestration.catalog_transport.CatalogTransport``: a Session Manager tunnel to the host's private
port 8181 behind an owned loopback relay. Every dependent call runs once through ``Route.execute``; a lost route fails visibly and is never
reopened inside an operation. Registration uses ``register_aws_catalog.register_exact_metadata`` per table with its own deadline, so a rerun
with the same inventory adopts done tables as ``unchanged`` and never overwrites a different table. Secrets are read into memory only.
"""

from __future__ import annotations

import hashlib
import time
import urllib.parse
from collections.abc import Callable, Mapping
from functools import partial
from pathlib import Path
from typing import Any, Protocol

from orchestration.catalog_transport import CatalogTransport, TransportSpec
from scripts.acquisition.s3_store import AwsCli, StorageError
from scripts.lakehouse import bronze_inventory
from scripts.lakehouse import register_aws_catalog as reg
from scripts.lakehouse.catalog_journal import durable_directory

CATALOG = "hai_lakehouse"
MAC_POLARIS_PORT = 8181
RECEIPT_NAME = "bronze_receipt.json"
RUNS_FOLDER = "registration_runs"
PRINCIPAL = "polaris_root"


class Execute(Protocol):
    def __call__[R](self, operation: Callable[[], R], /) -> R:
        """Run one dependent catalog operation exactly once."""
        ...


def direct[R](operation: Callable[[], R], /) -> R:
    """Run an operation with no transport in between (the in-memory stand-ins)."""
    return operation()


class Route:
    """The owned loopback relay in front of the Session Manager tunnel; each operation runs once with the route checked on both sides."""

    def __init__(self, transport: CatalogTransport, endpoint: str, *, deadline: float) -> None:
        parsed = urllib.parse.urlsplit(endpoint)
        if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or not parsed.port:
            raise reg.RegistrationError("network", "the catalog relay endpoint is not a loopback port")
        self.transport, self.port, self.deadline = transport, parsed.port, deadline

    def execute[R](self, operation: Callable[[], R], /) -> R:
        try:
            return self.transport.execute(operation, deadline=self.deadline)
        except reg.RegistrationError:
            raise
        except RuntimeError as error:
            if str(error) == "catalog_transport_unavailable":
                raise reg.RegistrationError("transport", "the catalog route is unavailable; it is never reopened inside an operation") from None
            raise


def check_ports(spec: TransportSpec) -> None:
    """The tunnel and relay listen on local ports other than the Mac Polaris port (R9)."""
    if MAC_POLARIS_PORT in (spec.tunnel_port, spec.relay_port):
        raise reg.RegistrationError("network", f"the tunnel and relay ports must differ from the Mac Polaris port {MAC_POLARIS_PORT}")


def network_acceptance(spec: TransportSpec, aws: AwsCli, *, deadline: float, clock: reg.Clock = time.monotonic) -> None:
    """Accept the route only to one running host with no public address on the instance or any interface; the catalog stays private."""
    check_ports(spec)
    reg.check_deadline(deadline, clock)
    try:
        described = aws.call_bounded("ec2", "describe-instances", ["--instance-ids", spec.instance], timeout=deadline - clock())
    except StorageError as error:
        raise reg.RegistrationError("network", f"describing the catalog host failed ({error.code})") from None
    instances = [item for reservation in described.get("Reservations") or [] for item in reservation.get("Instances") or []]
    if (
        len(instances) != 1
        or (instances[0].get("State") or {}).get("Name") != "running"
        or instances[0].get("PublicIpAddress")
        or any((interface.get("Association") or {}).get("PublicIp") for interface in instances[0].get("NetworkInterfaces") or [])
    ):
        raise reg.RegistrationError("network", "the catalog host is not one running instance without a public address")


def read_parameter(aws: AwsCli, name: str, *, deadline: float, clock: reg.Clock = time.monotonic) -> str:
    """Read one secure parameter into memory; errors name the parameter and a fixed code, never a value."""
    reg.check_deadline(deadline, clock)
    try:
        reply = aws.call_bounded("ssm", "get-parameter", ["--name", name, "--with-decryption"], timeout=deadline - clock())
    except StorageError as error:
        raise reg.RegistrationError("authentication", f"reading parameter {name} failed ({error.code})") from None
    value = (reply.get("Parameter") or {}).get("Value")
    if not isinstance(value, str) or not value:
        raise reg.RegistrationError("authentication", f"parameter {name} is missing or empty")
    return value


def preflight(request: reg.AuthenticatedRequest, bucket: str, *, deadline: float, execute: Execute = direct, clock: reg.Clock = time.monotonic) -> None:
    """Refuse an absent AWS catalog or one whose base location, allowed locations or credential mode differ (R1)."""
    status, body = execute(lambda: reg.bounded(partial(request, "GET", f"/api/management/v1/catalogs/{CATALOG}", deadline=deadline), deadline, clock))
    location = f"s3://{bucket}/lakehouse/"
    storage = body.get("storageConfigInfo") if isinstance(body, dict) else None
    properties = body.get("properties") if isinstance(body, dict) else None
    if (
        status != 200
        or not isinstance(storage, dict)
        or not isinstance(properties, dict)
        or properties.get("default-base-location") != location
        or storage.get("allowedLocations") != [location]
        or storage.get("stsUnavailable") is not True
    ):
        raise reg.RegistrationError("catalog_location", f"the AWS catalog {CATALOG} is absent or its location or credential mode differs (HTTP {status})")


def register_tables(
    destination: reg.Catalog,
    objects: reg.MetadataReader,
    inventory: reg.Inventory,
    *,
    bucket: str,
    deadline: float,
    per_table: float,
    execute: Execute = direct,
    clock: reg.Clock = time.monotonic,
) -> dict[str, list[reg.TableId]]:
    """Create the two namespaces if absent, then register or adopt each table in sorted order; stop at the first failure (R2 to R7)."""
    for namespace in bronze_inventory.NAMESPACES:
        try:
            execute(partial(destination.ensure_namespace, namespace, deadline=deadline))
        except reg.RegistrationError as error:
            raise reg.RegistrationError(error.code, f"namespace {namespace}: {error}") from None
    outcomes: dict[str, list[reg.TableId]] = {"registered": [], "unchanged": []}
    for pin in sorted(inventory.tables, key=lambda item: item.identifier):
        table_deadline = min(deadline, clock() + per_table)
        try:
            outcome = execute(partial(reg.register_exact_metadata, destination, objects, pin, bucket=bucket, deadline=table_deadline, clock=clock))
        except reg.RegistrationError as error:
            raise bronze_inventory.named(pin.identifier, error) from None
        outcomes[outcome].append(pin.identifier)
    return outcomes


def verify_tables(
    reader: reg.Catalog,
    source: reg.Catalog,
    objects: reg.MetadataReader,
    inventory: reg.Inventory,
    *,
    bucket: str,
    deadline: float,
    per_table: float,
    execute: Execute = direct,
    clock: reg.Clock = time.monotonic,
) -> dict[str, Any]:
    """The reader identity loads every table exactly, one manifest list per namespace is read and every Mac table is unchanged (V1 to V3)."""
    metadata: dict[reg.TableId, Mapping[str, Any]] = {}
    for pin in inventory.tables:
        table_deadline = min(deadline, clock() + per_table)
        try:
            document = reg.pinned_metadata(objects, pin, table_deadline, clock)
            reg.current_object(objects, pin, table_deadline, clock)
            loaded = execute(partial(reader.load, pin.identifier, deadline=table_deadline))
            reg.check_loaded(loaded, pin, document, "reader_inventory")
        except reg.RegistrationError as error:
            raise bronze_inventory.named(pin.identifier, error) from None
        metadata[pin.identifier] = document
    data_reads = []
    for namespace in bronze_inventory.NAMESPACES:
        pin = next(item for item in inventory.tables if item.namespace == namespace)
        table_deadline = min(deadline, clock() + per_table)
        try:
            manifest = reg.snapshots(metadata[pin.identifier], "data_read")[pin.current_snapshot_id]["manifest-list"]
            location = reg.bucket_location(manifest, bucket)
            reg.bounded(partial(objects.read, location, None, deadline=table_deadline), table_deadline, clock)
        except reg.RegistrationError as error:
            raise bronze_inventory.named(pin.identifier, error) from None
        data_reads.append(namespace)
    for pin in inventory.tables:
        table_deadline = min(deadline, clock() + per_table)
        try:
            current = reg.bounded(partial(source.load, pin.identifier, deadline=table_deadline), table_deadline, clock)
            reg.check_loaded(current, pin, metadata[pin.identifier], "source_snapshot")
        except reg.RegistrationError as error:
            raise bronze_inventory.named(pin.identifier, error) from None
    return {"reader_tables": len(metadata), "data_reads": data_reads, "mac_recompare": len(inventory.tables)}


def write_run_record(directory: Path, cutover_id: str, inventory_sha256: str, outcomes: Mapping[str, list[reg.TableId]], stamp: str) -> Path:
    """Keep one registration run's outcomes as evidence; a rerun never reads it as a skip list (R5)."""
    folder = directory / RUNS_FOLDER
    durable_directory(folder)
    document = {"cutover_id": cutover_id, "inventory_sha256": inventory_sha256, **{key: [list(item) for item in value] for key, value in outcomes.items()}}
    path = folder / f"{stamp}.json"
    bronze_inventory.write_new(path, reg.canonical(document))
    return path


def write_receipt(directory: Path, cutover_id: str, inventory_sha256: str, facts: Mapping[str, Any]) -> str:
    """Write the receipt once after verification passed and return its SHA-256; the same facts give the same bytes again."""
    folder = directory / RUNS_FOLDER
    runs = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(folder.glob("*.json"))} if folder.exists() else {}
    document = {
        "schema": 1,
        "cutover_id": cutover_id,
        "inventory_sha256": inventory_sha256,
        "principal": PRINCIPAL,
        "namespace_creation_approved_for": cutover_id,
        "registration_runs": runs,
        "verification": dict(facts),
        "mac_reads": "stale after the switch until the reader relay exists",
    }
    content = reg.canonical(document)
    bronze_inventory.write_new(directory / RECEIPT_NAME, content)
    return hashlib.sha256(content).hexdigest()
