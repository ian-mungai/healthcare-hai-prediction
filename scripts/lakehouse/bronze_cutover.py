"""The owner's bronze cutover: freeze the Mac catalog, capture the bronze inventory, register it on the AWS catalog, verify and switch authority.

Run from the repository root, one step at a time, at a terminal (each authority step prints its confirmation text and reads it back):

    .venv/bin/python -m scripts.lakehouse.bronze_cutover status
    .venv/bin/python -m scripts.lakehouse.bronze_cutover dry-run
    .venv/bin/python -m scripts.lakehouse.bronze_cutover freeze --cutover <CUTOVER_ID>
    .venv/bin/python -m scripts.lakehouse.bronze_cutover fence
    .venv/bin/python -m scripts.lakehouse.bronze_cutover capture
    .venv/bin/python -m scripts.lakehouse.bronze_cutover refresh
    .venv/bin/python -m scripts.lakehouse.bronze_cutover register --inventory <FILE> --sha256 <DIGEST> --instance <HOST_INSTANCE_ID>
    .venv/bin/python -m scripts.lakehouse.bronze_cutover verify --instance <HOST_INSTANCE_ID>
    .venv/bin/python -m scripts.lakehouse.bronze_cutover switch
    .venv/bin/python -m scripts.lakehouse.bronze_cutover rollback

Authority changes go through the launcher's ``catalog_authority`` owner method, which applies them in its registry transaction and refuses Mac
catalog writes outside ``mac_writer``. Each command body is stored write-once under ``data/lakehouse/cutover/<CUTOVER_ID>/commands/`` before it
is sent, so repeating a step after a lost reply sends the identical command and gets the stored record back.

The AWS catalog is reached only through ``orchestration.catalog_transport.CatalogTransport``: a Session Manager tunnel to the host's private
port 8181 behind an owned loopback relay, after the network acceptance check. Every dependent call runs once through ``Route.execute``; a lost
route fails visibly and is never reopened inside an operation. Registration uses ``register_aws_catalog.register_exact_metadata`` per table
with its own deadline, so a rerun with the same inventory adopts done tables as ``unchanged`` and never overwrites a different table. The
Polaris root credential registers (owner decision for this one attended run; the receipt records it) and the AWS reader pair verifies. Both
are read from Parameter Store into memory only and never printed or written.
"""

from __future__ import annotations

import argparse
import contextlib
import functools
import hashlib
import json
import re
import sys
import time
import urllib.parse
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from orchestration import catalog_authority, launcher
from orchestration.catalog_transport import CatalogTransport, TransportSpec
from scripts.acquisition.s3_store import AwsCli, StorageError
from scripts.infrastructure import secret_store
from scripts.lakehouse import bronze_inventory, run_lock
from scripts.lakehouse import register_aws_catalog as reg
from scripts.lakehouse.catalog_journal import durable_directory

CATALOG = "hai_lakehouse"
MAC_POLARIS_PORT = 8181
RECEIPT_NAME = "bronze_receipt.json"
RUNS_FOLDER = "registration_runs"
PRINCIPAL = "polaris_root"
REPO_ROOT = Path(__file__).resolve().parents[2]
CUTOVER_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
INITIAL_CUTOVER = "initial"
FENCE_POLL_SECONDS = 10.0
# The test registry a test of this script passes; the real run never sets it.
TEST_REGISTRY: str | None = None


# Runs one dependent catalog operation exactly once and returns its result.
Execute = Callable[[Callable[[], Any]], Any]


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
    status, body = execute(lambda: reg.bounded(functools.partial(request, "GET", f"/api/management/v1/catalogs/{CATALOG}", deadline=deadline), deadline, clock))
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
            execute(functools.partial(destination.ensure_namespace, namespace, deadline=deadline))
        except reg.RegistrationError as error:
            raise reg.RegistrationError(error.code, f"namespace {namespace}: {error}") from None
    outcomes: dict[str, list[reg.TableId]] = {"registered": [], "unchanged": []}
    for pin in sorted(inventory.tables, key=lambda item: item.identifier):
        table_deadline = min(deadline, clock() + per_table)
        try:
            outcome = execute(functools.partial(reg.register_exact_metadata, destination, objects, pin, bucket=bucket, deadline=table_deadline, clock=clock))
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
            loaded = execute(functools.partial(reader.load, pin.identifier, deadline=table_deadline))
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
            reg.bounded(functools.partial(objects.read, location, None, deadline=table_deadline), table_deadline, clock)
        except reg.RegistrationError as error:
            raise bronze_inventory.named(pin.identifier, error) from None
        data_reads.append(namespace)
    for pin in inventory.tables:
        table_deadline = min(deadline, clock() + per_table)
        try:
            current = reg.bounded(functools.partial(source.load, pin.identifier, deadline=table_deadline), table_deadline, clock)
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


class CutoverError(RuntimeError):
    """A step cannot run in the current authority state or its inputs are wrong; nothing was changed."""


@dataclass(frozen=True)
class Terminal:
    """The owner's terminal: whether both streams are a TTY, how a typed line is read and where output goes."""

    tty: bool
    read: Callable[[str], str]
    write: Callable[[str], object]


@dataclass(frozen=True)
class Remote:
    """The AWS catalog through the owned route: the registering client, the reader client and the once-only executor."""

    request: reg.AuthenticatedRequest
    destination: reg.Catalog
    reader: reg.Catalog
    execute: Execute


class Services(Protocol):
    """Everything a step reaches outside the launcher; the E2E supplies stand-ins."""

    state_root: Path
    required: frozenset[reg.TableId]

    @property
    def bucket(self) -> str:
        """The project bucket the inventory pins must name."""
        ...

    def source(self, *, deadline: float) -> bronze_inventory.Source:
        """The frozen Mac catalog, read-only."""
        ...

    def objects(self) -> reg.MetadataReader:
        """The versioned metadata objects in the project bucket."""
        ...

    def remote(self, instance: str, *, deadline: float) -> contextlib.AbstractContextManager[Remote]:
        """Open the accepted route for the duration of one step and close it after."""
        ...


class LiveServices:
    """The project profile, the Mac Polaris on 127.0.0.1 and the Session Manager route to the AWS host."""

    def __init__(self, tunnel_port: int, relay_port: int) -> None:
        self.state_root = run_lock.main_checkout(REPO_ROOT) / "data/lakehouse"
        self.required = bronze_inventory.required_bronze(REPO_ROOT)
        self.ports = (tunnel_port, relay_port)

    @functools.cached_property
    def settings(self) -> dict[str, str]:
        """The project profile, region, bucket and account, checked against the caller identity on first use only."""
        from scripts.infrastructure.render_project_config import load_configuration, verify_project_identity

        settings, _ = load_configuration(REPO_ROOT / ".env")
        verify_project_identity(settings)
        return settings

    @functools.cached_property
    def aws(self) -> AwsCli:
        return AwsCli(self.settings)

    @property
    def bucket(self) -> str:
        return self.settings["data_bucket_name"]

    def secret(self, name: str, deadline: float) -> str:
        return read_parameter(self.aws, name, deadline=deadline)

    def source(self, *, deadline: float) -> bronze_inventory.Source:
        request = bronze_inventory.PolarisHttp(
            "127.0.0.1", MAC_POLARIS_PORT, client_id=launcher.ROOT_CLIENT_ID, secret=self.secret(secret_store.POLARIS_ROOT_SECRET, deadline)
        )
        return bronze_inventory.SourceCatalog(request, CATALOG)

    def objects(self) -> reg.MetadataReader:
        return bronze_inventory.S3Metadata(self.aws, self.bucket)

    @contextlib.contextmanager
    def remote(self, instance: str, *, deadline: float) -> Iterator[Remote]:
        spec = TransportSpec(instance, self.settings["aws_profile"], self.settings["aws_region"], *self.ports)
        network_acceptance(spec, self.aws, deadline=deadline)
        names = secret_store.variable_names(REPO_ROOT / secret_store.VARIABLES)
        root = self.secret(secret_store.POLARIS_ROOT_SECRET, deadline)
        reader_id = self.secret(names["AWS_POLARIS_READER_CLIENT_ID_NAME"], deadline)
        reader_secret = self.secret(names["AWS_POLARIS_READER_CLIENT_SECRET_NAME"], deadline)
        folder = self.state_root / "cutover_transport"
        durable_directory(folder)
        transport = CatalogTransport(folder)
        try:
            endpoint = transport.start(spec, deadline=deadline, ready_deadline=min(deadline, time.monotonic() + 120))
            route = Route(transport, endpoint, deadline=deadline)
            request = bronze_inventory.PolarisHttp("127.0.0.1", route.port, client_id=launcher.ROOT_CLIENT_ID, secret=root)
            reader = bronze_inventory.PolarisHttp("127.0.0.1", route.port, client_id=reader_id, secret=reader_secret)
            yield Remote(request, reg.PolarisCatalog(request, CATALOG), reg.PolarisCatalog(reader, CATALOG), route.execute)
        finally:
            if not transport.close(deadline=time.monotonic() + 30):
                raise CutoverError("the catalog route did not close with every process confirmed gone; check the transport ledger")


def client() -> launcher.LauncherClient:
    return launcher.load_client(TEST_REGISTRY)


def current_authority() -> dict[str, Any] | None:
    record = client().call("status", {"what": "launcher"})["catalog_authority"]
    return dict(record) if record else None


def folder_of(services: Services, cutover_id: str) -> Path:
    if not CUTOVER_ID.fullmatch(cutover_id):
        raise CutoverError("the cutover ID must be letters, digits, underscores and hyphens")
    return services.state_root / "cutover" / cutover_id


def stored_command(folder: Path, command: dict[str, Any]) -> dict[str, Any]:
    """Keep the command body write-once before sending it; a repeat of the step sends the stored body unchanged."""
    commands = folder / "commands"
    durable_directory(commands)
    path = commands / f"{command['command_id']}.json"
    if path.exists():
        return dict(json.loads(path.read_bytes()))
    bronze_inventory.write_new(path, reg.canonical(command))
    return command


def send(terminal: Terminal, command: dict[str, Any]) -> dict[str, Any]:
    """Show the confirmation text, read it back from the terminal and send the owner command once."""
    text = catalog_authority.confirmation_text(command)
    terminal.write(f"Type this line to confirm: {text}\n")
    typed = terminal.read("> ").strip()
    params = {"command": command, "confirmation": typed, "tty": terminal.tty, "owner": True}
    return dict(client().call("catalog_authority", params))


def authority_step(services: Services, terminal: Terminal, step: str, state: str, reason: str, *, cutover: str | None = None) -> dict[str, Any]:
    """Build or reload the command for one authority transition of the current cutover and send it."""
    record = current_authority()
    cutover_id = cutover or (record or {}).get("cutover_id", "")
    folder = folder_of(services, cutover_id)
    command_id = f"{cutover_id}_{step}"
    stored = folder / "commands" / f"{command_id}.json"
    if stored.exists():
        return send(terminal, dict(json.loads(stored.read_bytes())))
    if record is None:
        raise CutoverError("no catalog authority record exists; run freeze first")
    command = {
        "command_id": command_id,
        "expected_epoch": record["epoch"],
        "action": "transition",
        "state": state,
        "cutover_id": cutover_id,
        "reason": reason,
        "time": datetime.now(UTC).isoformat(),
    }
    return send(terminal, stored_command(folder, command))


def require_state(*states: str) -> dict[str, Any]:
    record = current_authority()
    if record is None or record["state"] not in states:
        raise CutoverError(f"this step needs catalog authority in {' or '.join(states)}; it is {(record or {}).get('state', 'not initialized')}")
    return record


def step_freeze(services: Services, terminal: Terminal, cutover: str) -> dict[str, Any]:
    """Initialize Mac authority once if the registry has none, then freeze every layer for a new cutover (F1, F5)."""
    if cutover == INITIAL_CUTOVER:
        raise CutoverError(f"{INITIAL_CUTOVER} is reserved for the first Mac authority record")
    folder = folder_of(services, cutover)
    if current_authority() is None:
        initial = {
            "command_id": f"{cutover}_initialize",
            "expected_epoch": 0,
            "action": "initialize",
            "state": "mac_writer",
            "cutover_id": INITIAL_CUTOVER,
            "reason": "the first catalog authority record: the Mac catalog is the writer",
            "time": datetime.now(UTC).isoformat(),
        }
        send(terminal, stored_command(folder, initial))
    record = authority_step(services, terminal, "freeze", "freezing", f"bronze cutover {cutover}: freeze Mac catalog writes", cutover=cutover)
    if record["cutover_id"] != cutover or record["state"] == "mac_writer":
        raise CutoverError(f"cutover {cutover} was already used and authority is {record['state']}; a new freeze needs a new cutover ID")
    return record


def step_fence(services: Services, terminal: Terminal, seconds: float, poll: float = FENCE_POLL_SECONDS) -> dict[str, Any]:
    """Repeat the fence until no Mac write is open or the deadline passes; a writer is never stopped (F2)."""
    record = require_state("freezing", "source_fenced")
    cutover = record["cutover_id"]
    command = {
        "command_id": f"{cutover}_fence",
        "expected_epoch": record["epoch"],
        "action": "transition",
        "state": "source_fenced",
        "cutover_id": cutover,
        "reason": "no Mac catalog write is queued or running",
        "time": datetime.now(UTC).isoformat(),
    }
    if record["state"] == "source_fenced" and not (folder_of(services, cutover) / "commands" / f"{cutover}_fence.json").exists():
        raise CutoverError("authority is source_fenced without this cutover's fence command")
    command = stored_command(folder_of(services, cutover), command)
    deadline = time.monotonic() + seconds
    terminal.write(f"Type this line to confirm: {catalog_authority.confirmation_text(command)}\n")
    typed = terminal.read("> ").strip()
    while True:
        try:
            return dict(client().call("catalog_authority", {"command": command, "confirmation": typed, "tty": terminal.tty, "owner": True}))
        except launcher.ClientError as error:
            if "Mac catalog writes are still open" not in str(error):
                raise
            if time.monotonic() + poll >= deadline:
                raise CutoverError(f"the fence deadline passed with Mac writes open; authority stays freezing ({error})") from None
            terminal.write(f"waiting: {error}\n")
            time.sleep(poll)


def step_capture(services: Services, terminal: Terminal, seconds: float, per_table: float) -> str:
    """Pin every bronze table from the frozen Mac catalog and freeze the inventory and its digest once (I1 to I8)."""
    record = require_state("source_fenced")
    deadline = time.monotonic() + seconds
    inventory = bronze_inventory.capture(
        services.source(deadline=deadline), services.objects(), services.required, bucket=services.bucket, deadline=deadline, per_table=per_table
    )
    folder = folder_of(services, record["cutover_id"])
    digest = bronze_inventory.write_inventory(folder, inventory)
    terminal.write(f"inventory: {folder / bronze_inventory.INVENTORY_NAME}\nsha256: {digest}\ntables: {len(inventory.tables)}\n")
    return digest


def step_dry_run(services: Services, terminal: Terminal, seconds: float, per_table: float) -> str:
    """Capture every table read-only before the freeze and write nothing (I4, I5)."""
    deadline = time.monotonic() + seconds
    inventory = bronze_inventory.capture(
        services.source(deadline=deadline), services.objects(), services.required, bucket=services.bucket, deadline=deadline, per_table=per_table
    )
    terminal.write(f"dry run: {len(inventory.tables)} tables captured; nothing was written; sha256 {inventory.sha256}\n")
    return inventory.sha256


def step_register(services: Services, terminal: Terminal, inventory_path: Path, sha256: str, instance: str, seconds: float, per_table: float) -> dict[str, Any]:
    """Register every pinned table through the accepted route; a rerun with the same digest adopts done tables (R1 to R10)."""
    record = require_state("refreshing")
    folder = folder_of(services, record["cutover_id"])
    if inventory_path.resolve() != (folder / bronze_inventory.INVENTORY_NAME).resolve():
        raise CutoverError("the inventory must be the one frozen for the current cutover")
    inventory = bronze_inventory.read_inventory(inventory_path, sha256, services.required)
    deadline = time.monotonic() + seconds
    with services.remote(instance, deadline=deadline) as remote:
        preflight(remote.request, services.bucket, deadline=deadline, execute=remote.execute)
        outcomes = register_tables(
            remote.destination, services.objects(), inventory, bucket=services.bucket, deadline=deadline, per_table=per_table, execute=remote.execute
        )
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    path = write_run_record(folder, record["cutover_id"], inventory.sha256, outcomes, stamp)
    terminal.write(f"registered: {len(outcomes['registered'])}; unchanged: {len(outcomes['unchanged'])}; record: {path}\n")
    return {key: len(value) for key, value in outcomes.items()}


def step_verify(services: Services, terminal: Terminal, instance: str, seconds: float, per_table: float) -> str:
    """Verify through the reader identity, recompare the Mac tables and write the receipt once (V1 to V3)."""
    record = require_state("refreshing")
    folder = folder_of(services, record["cutover_id"])
    digest = (folder / bronze_inventory.DIGEST_NAME).read_text().strip()
    inventory = bronze_inventory.read_inventory(folder / bronze_inventory.INVENTORY_NAME, digest, services.required)
    deadline = time.monotonic() + seconds
    with services.remote(instance, deadline=deadline) as remote:
        facts = verify_tables(
            remote.reader,
            services.source(deadline=deadline),
            services.objects(),
            inventory,
            bucket=services.bucket,
            deadline=deadline,
            per_table=per_table,
            execute=remote.execute,
        )
    receipt = write_receipt(folder, record["cutover_id"], inventory.sha256, facts)
    terminal.write(f"verified {facts['reader_tables']} tables; receipt sha256 {receipt}\n")
    return receipt


def step_switch(services: Services, terminal: Terminal) -> dict[str, Any]:
    """Make AWS the writer with every layer still off, naming the receipt digest in the command reason (S1, S4)."""
    record = current_authority()
    folder = folder_of(services, str((record or {}).get("cutover_id", "")))
    receipt = folder / RECEIPT_NAME
    if not (folder / "commands" / f"{(record or {}).get('cutover_id')}_switch.json").exists():
        require_state("refreshing")
        if not receipt.exists():
            raise CutoverError("the switch needs the verification receipt; run verify first")
    digest = hashlib.sha256(receipt.read_bytes()).hexdigest()
    reason = f"bronze registered and verified, receipt sha256 {digest}; Mac reads stay stale until the reader relay exists"
    return authority_step(services, terminal, "switch", "aws_writer", reason)


def step_rollback(services: Services, terminal: Terminal, seconds: float, per_table: float) -> dict[str, Any]:
    """Return to Mac authority before any AWS writer was admitted; from P2 to P4 the Mac source is recompared first (B1 to B4)."""
    record = require_state("freezing", "source_fenced", "refreshing", "aws_writer", "rollback_pending", "source_verify")
    cutover = record["cutover_id"]
    if record["state"] not in ("rollback_pending", "source_verify"):
        record = authority_step(services, terminal, "rollback", "rollback_pending", f"bronze cutover {cutover}: roll back to Mac authority")
    if record["state"] == "rollback_pending" and record["phase"] != "P1":
        record = authority_step(services, terminal, "source_verify", "source_verify", f"bronze cutover {cutover}: verify the frozen Mac source")
    if record["state"] == "source_verify":
        folder = folder_of(services, cutover)
        if (folder / bronze_inventory.DIGEST_NAME).exists():
            digest = (folder / bronze_inventory.DIGEST_NAME).read_text().strip()
            deadline = time.monotonic() + seconds
            again = bronze_inventory.capture(
                services.source(deadline=deadline), services.objects(), services.required, bucket=services.bucket, deadline=deadline, per_table=per_table
            )
            if again.sha256 != digest:
                raise CutoverError("the Mac source differs from the frozen inventory; authority stays source_verify")
    return authority_step(services, terminal, "mac_writer", "mac_writer", f"bronze cutover {cutover}: Mac authority restored, AWS registrations kept")


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    cli.add_argument("--test-registry", help=argparse.SUPPRESS)
    cli.add_argument("--tunnel-port", type=int, default=18181)
    cli.add_argument("--relay-port", type=int, default=18182)
    cli.add_argument("--seconds", type=float, default=7200, help="the deadline of the whole step")
    cli.add_argument("--per-table-seconds", type=float, default=120)
    steps = cli.add_subparsers(dest="step", required=True)
    for name in ("status", "dry-run", "fence", "capture", "refresh", "switch", "rollback"):
        steps.add_parser(name)
    steps.add_parser("freeze").add_argument("--cutover", required=True)
    register = steps.add_parser("register")
    register.add_argument("--inventory", type=Path, required=True)
    register.add_argument("--sha256", required=True)
    register.add_argument("--instance", required=True)
    steps.add_parser("verify").add_argument("--instance", required=True)
    return cli


def run(args: argparse.Namespace, services: Services, terminal: Terminal) -> Any:
    """Run one owner step; every step names what it changed."""
    seconds, per_table = args.seconds, args.per_table_seconds
    if args.step == "status":
        return current_authority()
    if args.step == "dry-run":
        return step_dry_run(services, terminal, seconds, per_table)
    if args.step == "freeze":
        return step_freeze(services, terminal, args.cutover)
    if args.step == "fence":
        return step_fence(services, terminal, seconds)
    if args.step == "capture":
        return step_capture(services, terminal, seconds, per_table)
    if args.step == "refresh":
        record = require_state("source_fenced", "refreshing")
        folder = folder_of(services, record["cutover_id"])
        if not (folder / bronze_inventory.DIGEST_NAME).exists():
            raise CutoverError("refresh needs the frozen inventory; run capture first")
        return authority_step(services, terminal, "refresh", "refreshing", "bronze inventory frozen; AWS registration may start")
    if args.step == "register":
        return step_register(services, terminal, args.inventory, args.sha256, args.instance, seconds, per_table)
    if args.step == "verify":
        return step_verify(services, terminal, args.instance, seconds, per_table)
    if args.step == "switch":
        return step_switch(services, terminal)
    return step_rollback(services, terminal, seconds, per_table)


def main(argv: list[str] | None = None) -> int:
    global TEST_REGISTRY
    args = parser().parse_args(argv)
    TEST_REGISTRY = args.test_registry
    terminal = Terminal(sys.stdin.isatty() and sys.stdout.isatty(), input, sys.stdout.write)
    try:
        services = LiveServices(args.tunnel_port, args.relay_port)
        result = run(args, services, terminal)
    except (CutoverError, reg.RegistrationError, launcher.ClientError, OSError, ValueError) as error:
        code = f" ({error.code})" if isinstance(error, reg.RegistrationError) else ""
        sys.stderr.write(f"bronze cutover {args.step}{code}: {error}\n")
        return 1
    sys.stdout.write(json.dumps(result, sort_keys=True, default=str) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
