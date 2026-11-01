"""Exercise exact catalog registration with Synthetic metadata and in-memory I/O."""

from __future__ import annotations

import argparse
import ast
import copy
import hashlib
import inspect
import json
import sys
import textwrap
import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from functools import partial
from importlib.machinery import SourceFileLoader
from pathlib import Path
from tempfile import TemporaryDirectory
from types import CodeType, FunctionType
from typing import Any
from unittest.mock import patch

from scripts.lakehouse import register_aws_catalog as reg

ROOT = Path(__file__).resolve().parents[2]
REQUIRED = frozenset({("bronze", "source_table"), ("silver", "silver_table"), ("silver", "publish_runs"), ("gold", "gold_table"), ("gold", "publish_runs")})
RUNS = {"silver": "20261024T010000Z-00000001", "gold": "20261024T020000Z-00000002"}


def encoded(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class Objects:
    def __init__(self, events: list[str]) -> None:
        self.versions: dict[str, dict[str, bytes]] = {}
        self.current: dict[str, str] = {}
        self.events = events
        self.hook: Callable[[str, str | None], None] | None = None

    def put(self, location: str, document: Mapping[str, Any], version: str = "version_1") -> None:
        self.versions.setdefault(location, {})[version] = encoded(document)
        self.current[location] = version

    def read(self, location: str, version_id: str | None, *, deadline: float) -> reg.ObjectValue:
        self.events.append("object_read")
        if self.hook:
            self.hook(location, version_id)
        version = version_id if version_id is not None else self.current.get(location, "")
        try:
            body = self.versions[location][version]
        except KeyError:
            raise reg.RegistrationError("metadata_missing", "Synthetic metadata is absent") from None
        return reg.ObjectValue(version, body)


class Catalog:
    def __init__(self, objects: Objects, events: list[str], role: str, tables: dict[tuple[str, str], dict[str, Any]] | None = None) -> None:
        self.objects, self.events, self.role = objects, events, role
        self.tables = {} if tables is None else tables
        self.posts: list[tuple[str, dict[str, Any]]] = []
        self.namespaces = {"bronze", "silver", "gold"}
        self.namespace_posts: list[str] = []
        self.namespace_failure = False
        self.hidden: set[tuple[str, str]] = set()
        self.hook: Callable[[str, tuple[str, str]], None] | None = None
        self.status: int | None = None
        self.lose_reply = False
        self.fail_table: tuple[str, str] | None = None
        self.concurrent: dict[str, Any] | None = None

    def request(self, method: str, path: str, *, body: Mapping[str, Any] | None = None, deadline: float) -> tuple[int, object]:
        self.events.append(self.role + "_" + method)
        pieces = path.split("/")
        if method == "GET" and pieces[-2] == "namespaces":
            return (200, {"namespace": [pieces[-1]], "properties": {}}) if pieces[-1] in self.namespaces else (404, None)
        if method == "POST" and pieces[-1] == "namespaces":
            require(body is not None and set(body) == {"namespace"}, "unexpected namespace mutation")
            if body is None:
                raise AssertionError("missing namespace body")
            name = body["namespace"][0]
            self.namespace_posts.append(name)
            if self.namespace_failure:
                return 503, {"error": "synthetic_private_error_do_not_record"}
            if name in self.namespaces:
                return 409, None
            self.namespaces.add(name)
            return 200, {"namespace": [name], "properties": {}}
        if method == "GET":
            key = (pieces[-3], pieces[-1])
            if self.hook:
                self.hook(method, key)
            if self.status is not None:
                return self.status, {"error": "synthetic_private_error_do_not_record"}
            value = self.tables.get(key)
            return (404, None) if value is None or key in self.hidden else (200, copy.deepcopy(value))
        require(method == "POST" and self.role == "destination" and pieces[-1] == "register", "unexpected catalog mutation")
        if body is None or set(body) != {"name", "metadata-location"}:
            raise AssertionError("registration payload permits replacement")
        key = (pieces[-2], str(body["name"]))
        self.posts.append((path, dict(body)))
        if key[0] not in self.namespaces:
            return 404, None
        if self.hook:
            self.hook(method, key)
        if key == self.fail_table:
            return 503, {"error": "synthetic_private_error_do_not_record"}
        if self.concurrent is not None:
            self.tables[key] = copy.deepcopy(self.concurrent)
            self.concurrent = None
            return 409, None
        if key in self.tables:
            return 409, None
        location = str(body["metadata-location"])
        version = self.objects.current[location]
        self.tables[key] = {
            "metadata-location": location,
            "metadata": json.loads(self.objects.versions[location][version]),
            "config": {"credential": "synthetic_private_config"},
        }
        if self.lose_reply:
            self.lose_reply = False
            raise reg.RegistrationError("transport", "Synthetic registration reply was lost")
        return 200, copy.deepcopy(self.tables[key])


class Publications:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.rows: dict[str, dict[str, Any]] = {
            layer: {"run_id": RUNS[layer], "mode": "real", "backup_sha256": "b" * 64, "snapshot_set": {layer + "_table": 101}} for layer in ("silver", "gold")
        }

    def read(self, namespace: str, run_id: str, *, deadline: float) -> Mapping[str, Any] | None:
        self.events.append("publication_read")
        row = self.rows.get(namespace)
        return copy.deepcopy(row) if row is not None and row["run_id"] == run_id else None


class Reservations:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.entries: dict[str, str] = {}
        self.checks = 0
        self.lose_on_check: int | None = None

    def ensure(self, reader: str, publish_run: str, *, deadline: float) -> None:
        self.events.append("reserve")
        if reader in self.entries and self.entries[reader] != publish_run:
            raise reg.RegistrationError("reservation", "Synthetic reservation belongs to another publication")
        self.entries[reader] = publish_run

    def verify(self, reader: str, publish_run: str, *, deadline: float) -> None:
        self.events.append("reservation_check")
        self.checks += 1
        if self.checks == self.lose_on_check:
            self.entries.pop(reader, None)
        if self.entries.get(reader) != publish_run:
            raise reg.RegistrationError("reservation", "Synthetic source protection is absent")


class Fixture:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.clock = Clock()
        self.objects = Objects(self.events)
        self.source = Catalog(self.objects, self.events, "source")
        self.destination = Catalog(self.objects, self.events, "destination")
        self.reader = Catalog(self.objects, self.events, "reader", self.destination.tables)
        self.publications = Publications(self.events)
        self.reservations = Reservations(self.events)
        self.receipts = {
            layer: encoded({"schema": 1, "run_id": RUNS[layer], "state": "committed", "backup_sha256": "b" * 64, "snapshot_set": {layer + "_table": 101}})
            for layer in ("silver", "gold")
        }
        for number, (namespace, name) in enumerate(sorted(REQUIRED), 1):
            location = f"s3://synthetic-bucket/lakehouse/{namespace}/{name}"
            metadata_location = location + "/metadata/catalog_confirmed.json"
            document = {
                "format-version": 2,
                "table-uuid": f"00000000-0000-4000-8000-{number:012d}",
                "location": location,
                "last-sequence-number": 2,
                "last-updated-ms": 1,
                "last-column-id": 1,
                "schemas": [{"schema-id": 0, "type": "struct", "fields": [{"id": 1, "name": "synthetic_id", "required": False, "type": "long"}]}],
                "current-schema-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "sort-orders": [{"order-id": 0, "fields": []}],
                "default-sort-order-id": 0,
                "current-snapshot-id": 202,
                "snapshots": [
                    {
                        "snapshot-id": snapshot,
                        "sequence-number": sequence,
                        "timestamp-ms": sequence,
                        "manifest-list": location + f"/metadata/synthetic_{snapshot}.avro",
                        "summary": {"operation": "append"},
                    }
                    for sequence, snapshot in enumerate((101, 202), 1)
                ],
                "refs": {"main": {"snapshot-id": 202, "type": "branch"}},
            }
            self.objects.put(metadata_location, document)
            self.source.tables[(namespace, name)] = {"metadata-location": metadata_location, "metadata": document}
        self.io = reg.RegistrationIO(
            source=reg.PolarisCatalog(self.source.request, "hai_lakehouse", clock=self.clock),
            destination=reg.PolarisCatalog(self.destination.request, "hai_lakehouse", clock=self.clock),
            reader=reg.PolarisCatalog(self.reader.request, "hai_lakehouse", clock=self.clock),
            objects=self.objects,
            publications=self.publications,
            reservations=self.reservations,
            clock=self.clock,
        )

    def prepare(self) -> reg.Inventory:
        return reg.prepare_read_inventory(self.io, REQUIRED, self.receipts, bucket="synthetic-bucket", reservation_prefix="synthetic_aws_reader", deadline=100)

    def run(self, inventory: reg.Inventory | None = None) -> reg.RegistrationResult:
        return reg.register_read_catalog(
            self.io,
            REQUIRED,
            self.receipts,
            bucket="synthetic-bucket",
            reservation_prefix="synthetic_aws_reader",
            deadline=100,
            inventory=inventory,
            inventory_sha256=None if inventory is None else inventory.sha256,
        )


def refusal(fixture: Fixture, code: str, name: str, inventory: reg.Inventory | None = None, *, no_posts: bool = True) -> None:
    try:
        fixture.run(inventory)
    except reg.RegistrationError as error:
        require(error.code == code, f"{name}: expected {code}, received {error.code}")
    except (ValueError, TypeError) as error:
        raise AssertionError(f"{name}: unsanitized {type(error).__name__}") from None
    else:
        raise AssertionError(f"{name}: accepted the invalid registration")
    if no_posts:
        require(not fixture.destination.posts and not fixture.destination.namespace_posts, f"{name}: changed destination before complete preflight")


def successful_inventory() -> None:
    fixture = Fixture()
    first = fixture.run()
    require(set(fixture.destination.tables) == REQUIRED, "success: required destination tables are absent")
    require(first.state == "verified" and len(first.registered) == 5 and not first.unchanged, "success: wrong complete result")
    require(first.inventory.tables[0].snapshot_id == 202, "success: bronze snapshot changed")
    published = {pin.identifier: pin.snapshot_id for pin in first.inventory.tables if pin.name.endswith("_table") and pin.namespace != "bronze"}
    require(published == {("silver", "silver_table"): 101, ("gold", "gold_table"): 101}, "success: substituted current heads for published inputs")
    expected_paths = {f"/api/catalog/v1/hai_lakehouse/namespaces/{namespace}/register" for namespace, _ in REQUIRED}
    require({path for path, _ in fixture.destination.posts} == expected_paths, "success: wrong REST registration paths")
    require(fixture.events.index("reserve") < fixture.events.index("source_GET"), "success: source discovery preceded reservation")
    require(not fixture.source.posts and not fixture.reader.posts, "success: a read-only catalog was mutated")
    require("synthetic_private" not in json.dumps(first.document()), "success: raw catalog config reached evidence")
    require(reg.inventory_from_document(first.inventory.document(), REQUIRED) == first.inventory, "success: frozen inventory does not round trip")
    second = fixture.run(first.inventory)
    require(
        second.inventory == first.inventory and len(second.unchanged) == 5 and len(fixture.destination.posts) == 5,
        "repeat: duplicate registration or changed inventory",
    )
    require(second.inventory.sha256 == hashlib.sha256(reg.canonical(second.inventory.document())).hexdigest(), "repeat: inventory digest changed")


def empty_destination() -> None:
    fixture = Fixture()
    fixture.destination.namespaces.clear()
    try:
        result = fixture.run()
    except reg.RegistrationError as error:
        raise AssertionError("empty catalog: namespace preparation failed with " + error.code) from error
    require(result.state == "verified" and set(fixture.destination.namespace_posts) == {"bronze", "silver", "gold"}, "empty catalog: missing namespaces")
    posts = fixture.destination.namespace_posts[:]
    fixture.run(result.inventory)
    require(fixture.destination.namespace_posts == posts, "empty catalog: repeated namespace creation")


def late_namespace_reply() -> None:
    fixture = Fixture()
    fixture.destination.namespaces.clear()
    original = fixture.destination.request

    def late(method: str, path: str, *, body: Mapping[str, Any] | None = None, deadline: float) -> tuple[int, object]:
        response = original(method, path, body=body, deadline=deadline)
        if method == "GET" and path.endswith("/namespaces/bronze"):
            fixture.clock.now = 100.0
        return response

    fixture.io = replace(fixture.io, destination=reg.PolarisCatalog(late, "hai_lakehouse", clock=fixture.clock))
    refusal(fixture, "deadline", "late namespace response")


def namespace_failure() -> None:
    fixture = Fixture()
    fixture.destination.namespaces.clear()
    fixture.destination.namespace_failure = True
    refusal(fixture, "catalog_http", "namespace failure", no_posts=False)
    require(not fixture.destination.posts, "namespace failure: attempted table registration")


def publication_case(kind: str) -> None:
    fixture = Fixture()
    if kind == "missing_receipt":
        fixture.receipts.pop("gold")
    elif kind == "missing_row":
        fixture.publications.rows.pop("gold")
    elif kind == "fixture_mode":
        fixture.publications.rows["gold"]["mode"] = "publish_fixture"
    elif kind == "failed_receipt":
        document = json.loads(fixture.receipts["gold"])
        document["state"] = "failed"
        fixture.receipts["gold"] = encoded(document)
    elif kind == "missing_snapshot":
        document = json.loads(fixture.receipts["gold"])
        document["snapshot_set"] = {}
        fixture.receipts["gold"] = encoded(document)
    elif kind == "other_snapshot":
        fixture.publications.rows["gold"]["snapshot_set"]["gold_table"] = 303
    elif kind == "other_backup":
        fixture.publications.rows["silver"]["backup_sha256"] = "c" * 64
    refusal(fixture, "publication", kind)


def changed_receipt() -> None:
    fixture = Fixture()
    inventory = fixture.prepare()
    fixture.receipts["gold"] += b" "
    refusal(fixture, "publication", "changed receipt", inventory)


def changed_snapshot_pin() -> None:
    fixture = Fixture()
    inventory = fixture.prepare()
    pins = tuple(replace(pin, snapshot_id=202) if pin.identifier == ("silver", "silver_table") else pin for pin in inventory.tables)
    refusal(fixture, "publication", "changed retained input", replace(inventory, tables=pins))


def changed_frozen_inventory() -> None:
    fixture = Fixture()
    inventory = fixture.prepare()
    pin = inventory.tables[0]
    fixture.objects.put(pin.metadata_location, fixture.source.tables[pin.identifier]["metadata"], "version_2")
    edited = replace(inventory, tables=(replace(pin, metadata_version="version_2"), *inventory.tables[1:]))
    try:
        reg.register_read_catalog(
            fixture.io,
            REQUIRED,
            fixture.receipts,
            bucket="synthetic-bucket",
            reservation_prefix="synthetic_aws_reader",
            deadline=100,
            inventory=edited,
            inventory_sha256=inventory.sha256,
        )
    except reg.RegistrationError as error:
        require(error.code == "inventory", "changed frozen inventory: wrong refusal")
    else:
        raise AssertionError("changed frozen inventory: accepted edited bytes")
    require(not fixture.destination.posts, "changed frozen inventory: changed destination")


def malformed_inventory(kind: str) -> None:
    fixture = Fixture()
    inventory = fixture.prepare().document()
    if kind == "unknown_field":
        inventory["unexpected_config"] = "synthetic_private_error_do_not_record"
    elif kind == "boolean_snapshot":
        inventory["tables"][0]["snapshot_id"] = True
    elif kind == "duplicate_table":
        inventory["tables"].append(copy.deepcopy(inventory["tables"][0]))
    else:
        inventory["publications"][0]["layer"] = []
    try:
        reg.inventory_from_document(inventory, REQUIRED)
    except reg.RegistrationError:
        return
    raise AssertionError("malformed inventory: accepted " + kind)


def invalid_json_constant() -> None:
    fixture = Fixture()
    inventory = fixture.prepare()
    pin = inventory.tables[0]
    raw = fixture.objects.versions[pin.metadata_location][pin.metadata_version].replace(b'"format-version":2', b'"format-version":NaN')
    fixture.objects.versions[pin.metadata_location][pin.metadata_version] = raw
    edited = replace(pin, metadata_size=len(raw), metadata_sha256=hashlib.sha256(raw).hexdigest())
    try:
        reg.register_exact_metadata(fixture.io.destination, fixture.objects, edited, bucket="synthetic-bucket", deadline=100, clock=fixture.clock)
    except reg.RegistrationError as error:
        require(error.code == "metadata_identity", "non-finite JSON: wrong refusal")
    else:
        raise AssertionError("non-finite JSON: accepted invalid Iceberg metadata")


def missing_source() -> None:
    fixture = Fixture()
    fixture.source.tables.pop(("gold", "gold_table"))
    refusal(fixture, "source_missing", "missing source")


def missing_inventory(kind: str) -> None:
    fixture = Fixture()
    inventory = fixture.prepare()
    tables = inventory.tables[:-1] if kind == "missing" else inventory.tables + (inventory.tables[0],)
    refusal(fixture, "coverage", "inventory " + kind, replace(inventory, tables=tables))


def object_case(kind: str) -> None:
    fixture = Fixture()
    inventory = fixture.prepare()
    pin = inventory.tables[0]
    if kind == "missing":
        fixture.objects.versions[pin.metadata_location].pop(pin.metadata_version)
        code = "metadata_missing"
    elif kind == "digest":
        inventory = replace(inventory, tables=(replace(pin, metadata_sha256="0" * 64), *inventory.tables[1:]))
        code = "metadata_identity"
    elif kind == "size":
        inventory = replace(inventory, tables=(replace(pin, metadata_size=pin.metadata_size + 1), *inventory.tables[1:]))
        code = "metadata_identity"
    elif kind == "current_version":
        fixture.objects.put(pin.metadata_location, fixture.source.tables[pin.identifier]["metadata"], "version_2")
        code = "metadata_identity"
    else:
        fixture.objects.current[pin.metadata_location] = "null"
        fixture.objects.versions[pin.metadata_location]["null"] = fixture.objects.versions[pin.metadata_location][pin.metadata_version]
        code = "metadata_identity"
    refusal(fixture, code, "metadata " + kind, inventory)


def source_identity(kind: str) -> None:
    fixture = Fixture()
    inventory = fixture.prepare()
    key = ("silver", "silver_table")
    metadata = fixture.source.tables[key]["metadata"]
    if kind == "uuid":
        metadata["table-uuid"] = "ffffffff-ffff-4fff-8fff-ffffffffffff"
    elif kind == "location":
        metadata["location"] += "_changed"
    elif kind == "snapshot":
        metadata["snapshots"] = [snapshot for snapshot in metadata["snapshots"] if snapshot["snapshot-id"] != 101]
    else:
        metadata["snapshots"][0]["manifest-list"] += "_changed"
    refusal(fixture, "source_snapshot", "source " + kind, inventory)


def source_advances() -> None:
    fixture = Fixture()
    inventory = fixture.prepare()
    key = ("silver", "silver_table")
    moved = copy.deepcopy(fixture.source.tables[key])
    moved["metadata-location"] = moved["metadata-location"].replace("catalog_confirmed", "later")
    moved["metadata"]["current-snapshot-id"] = 303
    moved["metadata"]["snapshots"].append({"snapshot-id": 303, "manifest-list": "s3://synthetic-bucket/lakehouse/silver/later.avro"})
    fixture.objects.put(moved["metadata-location"], moved["metadata"])
    fixture.source.tables[key] = moved
    result = fixture.run(inventory)
    require(result.inventory == inventory, "advanced source: changed immutable inventory")
    require(fixture.destination.tables[key]["metadata"]["current-snapshot-id"] == 202, "advanced source: registered a new head")


def conflict_case(kind: str) -> None:
    fixture = Fixture()
    inventory = fixture.prepare()
    key = ("gold", "gold_table")
    table = copy.deepcopy(fixture.source.tables[key])
    if kind == "uuid":
        table["metadata"]["table-uuid"] = "ffffffff-ffff-4fff-8fff-ffffffffffff"
    elif kind == "metadata_location":
        table["metadata-location"] += "_changed"
    elif kind == "snapshot":
        table["metadata"]["current-snapshot-id"] = 999
    elif kind in ("nan", "infinity", "negative_infinity"):
        table["metadata"]["last-updated-ms"] = {"nan": float("nan"), "infinity": float("inf"), "negative_infinity": float("-inf")}[kind]
    else:
        table["metadata"]["schemas"][0]["fields"][0]["name"] = "changed_column"
    fixture.destination.tables[key] = copy.deepcopy(table)
    refusal(fixture, "registration_conflict", "conflicting " + kind, inventory)
    require(fixture.destination.tables[key] == table, "conflict: existing destination changed")


def reader_case(kind: str) -> None:
    fixture = Fixture()
    if kind == "missing":
        fixture.reader.hidden.add(("silver", "silver_table"))
    else:

        def changed(method: str, key: tuple[str, str]) -> None:
            if key == ("silver", "silver_table"):
                fixture.destination.tables[key]["metadata"]["current-snapshot-id"] = 999

        fixture.reader.hook = changed
    refusal(fixture, "reader_inventory", "reader " + kind, no_posts=False)


def partial_registration() -> None:
    fixture = Fixture()
    inventory = fixture.prepare()
    fixture.destination.fail_table = sorted(REQUIRED)[2]
    refusal(fixture, "catalog_http", "partial registration", inventory, no_posts=False)
    require(len(fixture.destination.tables) == 2, "partial: wrong recorded partial state")
    fixture.destination.fail_table = None
    result = fixture.run(inventory)
    require(len(result.unchanged) == 2 and len(result.registered) == 3 and len(fixture.destination.tables) == 5, "partial: retry did not resume exact entries")


def lost_reply() -> None:
    fixture = Fixture()
    inventory = fixture.prepare()
    fixture.destination.lose_reply = True
    refusal(fixture, "transport", "lost reply", inventory, no_posts=False)
    require(len(fixture.destination.posts) == 1 and len(fixture.destination.tables) == 1, "lost reply: blindly retried registration")
    result = fixture.run(inventory)
    require(len(result.unchanged) == 1 and len(fixture.destination.posts) == 5, "lost reply: explicit retry duplicated the table")


def concurrent_registration(conflicting: bool) -> None:
    fixture = Fixture()
    inventory = fixture.prepare()
    first = inventory.tables[0]
    fixture.destination.concurrent = copy.deepcopy(fixture.source.tables[first.identifier])
    if conflicting:
        fixture.destination.concurrent["metadata"]["current-snapshot-id"] = 999
        refusal(fixture, "registration_conflict", "concurrent conflict", inventory, no_posts=False)
    else:
        require(fixture.run(inventory).state == "verified", "concurrent identical: refused exact identity")


def late_object_change() -> None:
    fixture = Fixture()
    inventory = fixture.prepare()
    pin = inventory.tables[-1]

    def changed(method: str, key: tuple[str, str]) -> None:
        if key == pin.identifier:
            fixture.objects.put(pin.metadata_location, fixture.source.tables[key]["metadata"], "version_2")

    fixture.reader.hook = changed
    refusal(fixture, "metadata_identity", "late metadata version", inventory, no_posts=False)


def lost_reservation() -> None:
    fixture = Fixture()
    inventory = fixture.prepare()
    fixture.reservations.lose_on_check = fixture.reservations.checks + 3
    refusal(fixture, "reservation", "lost reservation", inventory, no_posts=False)


def deadline_case(late: bool) -> None:
    fixture = Fixture()
    if late:
        fixture.source.hook = lambda _method, _key: setattr(fixture.clock, "now", 100.0)
    else:
        fixture.clock.now = 100.0
    refusal(fixture, "deadline", "late reply" if late else "expired deadline")


def http_case(status: int) -> None:
    fixture = Fixture()
    fixture.source.status = status
    refusal(fixture, "catalog_http", "HTTP " + str(status))


def reusable_operation() -> None:
    fixture = Fixture()
    pin = fixture.prepare().tables[0]
    outcome = reg.register_exact_metadata(fixture.io.destination, fixture.objects, pin, bucket="synthetic-bucket", deadline=100, clock=fixture.clock)
    require(outcome == "registered" and set(fixture.destination.tables) == {pin.identifier}, "reusable registration: wrong table or result")
    require(
        reg.register_exact_metadata(fixture.io.destination, fixture.objects, pin, bucket="synthetic-bucket", deadline=100, clock=fixture.clock) == "unchanged",
        "reusable registration: repeated mutation",
    )


def required_contracts() -> None:
    actual = reg.required_tables(ROOT)
    counts = {namespace: sum(layer == namespace for layer, _ in actual) for namespace in ("bronze", "silver", "gold")}
    require(counts == {"bronze": 331, "silver": 62, "gold": 72}, "contracts: required namespace counts differ")


def shortened_contract() -> None:
    path = ROOT / "config/lakehouse/bronze_tables.json"
    original_bytes, original_text = Path.read_bytes, Path.read_text
    document = json.loads(path.read_bytes())
    document["tables"].pop()
    shortened = encoded(document)

    def read_bytes(target: Path) -> bytes:
        return shortened if target == path else original_bytes(target)

    def read_text(target: Path, *args: Any, **kwargs: Any) -> str:
        return shortened.decode() if target == path else original_text(target, *args, **kwargs)

    with patch.object(Path, "read_bytes", read_bytes), patch.object(Path, "read_text", read_text):
        required_contracts()


def launcher_reservations() -> None:
    class Client:
        def __init__(self) -> None:
            self.readers: dict[str, Any] = {}
            self.calls: list[str] = []

        def call(self, method: str, params: dict[str, Any], *, timeout: float) -> dict[str, Any]:
            require(0 < timeout <= 100, "launcher reservation: missing common deadline")
            self.calls.append(method)
            if method == "status":
                require(params == {"what": "registry"}, "launcher reservation: wrong status request")
                return {"readers": copy.deepcopy(self.readers)}
            require(method == "reserve", "launcher reservation: changed or released a protection")
            self.readers[params["reader"]] = {"domain": params["domain"], "publish_run": params["publish_run"], "state": "reserved", "snapshot_set": None}
            return {"ok": True}

    client = Client()
    reservations = reg.LauncherReservations(client, pid=123, clock=lambda: 0.0)
    reservations.ensure("synthetic_read", RUNS["gold"], deadline=100)
    reservations.ensure("synthetic_read", RUNS["gold"], deadline=100)
    reservations.verify("synthetic_read", RUNS["gold"], deadline=100)
    require(client.calls.count("reserve") == 1 and len(client.readers) == 1, "launcher reservation: duplicate or released protection")
    valid = copy.deepcopy(client.readers["synthetic_read"])
    for kind, changes in (
        ("publication", {"publish_run": RUNS["silver"]}),
        ("domain", {"domain": "ingestion"}),
        ("state", {"state": "finished"}),
        ("snapshot", {"state": "registered", "snapshot_set": RUNS["silver"]}),
    ):
        client.readers["synthetic_read"] = {**valid, **changes}
        try:
            reservations.verify("synthetic_read", RUNS["gold"], deadline=100)
        except reg.RegistrationError as error:
            require(error.code == "reservation", "launcher reservation: wrong refusal")
        else:
            raise AssertionError("launcher reservation: accepted wrong " + kind)
    client.readers["synthetic_read"] = {**valid, "state": "registered", "snapshot_set": RUNS["gold"]}
    reservations.verify("synthetic_read", RUNS["gold"], deadline=100)


def missing_published_snapshot() -> None:
    fixture = Fixture()
    table = fixture.source.tables[("silver", "silver_table")]
    table["metadata"]["snapshots"] = [item for item in table["metadata"]["snapshots"] if item["snapshot-id"] != 101]
    fixture.objects.put(table["metadata-location"], table["metadata"])
    refusal(fixture, "metadata_identity", "published snapshot absent before capture")


def incomplete_matching_publication() -> None:
    fixture = Fixture()
    document = json.loads(fixture.receipts["gold"])
    document["snapshot_set"] = {}
    fixture.receipts["gold"] = encoded(document)
    fixture.publications.rows["gold"]["snapshot_set"] = {}
    try:
        fixture.prepare()
    except reg.RegistrationError as error:
        require(error.code == "publication", "matching publication omits required table: wrong refusal")
    else:
        raise AssertionError("matching publication omits required table: accepted incomplete inventory")


def unsafe_location(kind: str) -> None:
    fixture = Fixture()
    table = fixture.source.tables[("bronze", "source_table")]

    def changed(value: str) -> str:
        if kind == "scheme":
            return value.replace("s3://", "http://")
        if kind == "path":
            return value.replace("/lakehouse/", "/outside/")
        if kind == "parent":
            return value.replace("/lakehouse/", "/lakehouse/../")
        if kind == "query":
            return value + "?version=changed"
        if kind == "fragment":
            return value + "#changed"
        return value.replace("/lakehouse/", "/lakehouse//")

    table["metadata-location"] = changed(table["metadata-location"])
    table["metadata"]["location"] = changed(table["metadata"]["location"])
    fixture.objects.put(table["metadata-location"], table["metadata"])
    refusal(fixture, "metadata_identity", "unsafe location " + kind)


def capture_without_version(version: str) -> None:
    fixture = Fixture()
    table = fixture.source.tables[("bronze", "source_table")]
    fixture.objects.put(table["metadata-location"], table["metadata"], version)
    refusal(fixture, "metadata_identity", "captured metadata has no immutable version")


def duplicate_json_key() -> None:
    fixture = Fixture()
    location = fixture.source.tables[("bronze", "source_table")]["metadata-location"]
    body = fixture.objects.versions[location]["version_1"]
    fixture.objects.versions[location]["version_1"] = b'{"format-version":2,' + body[1:]
    refusal(fixture, "metadata_identity", "duplicate metadata JSON key")


def record_bound() -> None:
    body = b"{}" + b" " * (reg.MAX_RECORD - 2)
    require(reg.read_document(body, "metadata_identity") == {}, "record bound: refused exactly 32 MiB")
    try:
        reg.read_document(body + b" ", "metadata_identity")
    except reg.RegistrationError as error:
        require(error.code == "metadata_identity", "record bound: wrong refusal")
    else:
        raise AssertionError("record bound: accepted over 32 MiB")


def pin_boundary(kind: str) -> None:
    fixture = Fixture()
    document = fixture.prepare().document()
    if kind == "bucket":
        document["tables"][0]["location"] = document["tables"][0]["location"].replace("synthetic-bucket", "other-bucket")
    else:
        document["tables"][0]["metadata_size"] = reg.MAX_RECORD
        reg.inventory_from_document(document, REQUIRED)
        document["tables"][0]["metadata_size"] += 1
    try:
        reg.inventory_from_document(document, REQUIRED)
    except reg.RegistrationError as error:
        require(error.code == "metadata_identity", "pin boundary: wrong refusal")
    else:
        raise AssertionError("pin boundary: accepted " + kind)


def unexpected_bucket() -> None:
    fixture = Fixture()
    for table in fixture.source.tables.values():
        table["metadata-location"] = table["metadata-location"].replace("synthetic-bucket", "other-bucket")
        table["metadata"]["location"] = table["metadata"]["location"].replace("synthetic-bucket", "other-bucket")
        fixture.objects.put(table["metadata-location"], table["metadata"])
    refusal(fixture, "metadata_identity", "unexpected source bucket")


def unexpected_exact_bucket() -> None:
    fixture = Fixture()
    pin = fixture.prepare().tables[0]
    try:
        reg.register_exact_metadata(fixture.io.destination, fixture.objects, pin, bucket="other-bucket", deadline=100, clock=fixture.clock)
    except reg.RegistrationError as error:
        require(error.code == "metadata_identity", "exact bucket: wrong refusal")
    else:
        raise AssertionError("exact bucket: accepted another deployment bucket")
    require(not fixture.destination.posts, "exact bucket: changed destination")


def conflict_diagnostic(kind: str) -> None:
    fixture = Fixture()
    inventory = fixture.prepare()
    key = ("gold", "gold_table")
    table = copy.deepcopy(fixture.source.tables[key])
    hidden = "synthetic_private_value_do_not_record"
    if kind == "nested":
        table["metadata"]["schemas"][0]["fields"][0]["name"] = hidden
        table["metadata"]["table-uuid"] = hidden
        expected = "/metadata/schemas/0/fields/0/name"
    elif kind == "missing":
        del table["metadata"]["schemas"][0]["fields"][0]["required"]
        expected = "/metadata/schemas/0/fields/0/required"
    elif kind == "list":
        table["metadata"]["schemas"][0]["fields"].clear()
        expected = "/metadata/schemas/0/fields/0"
    elif kind == "type":
        table["metadata"]["last-updated-ms"] = hidden
        expected = "/metadata/last-updated-ms"
    elif kind == "non_finite":
        table["metadata"]["last-updated-ms"] = float("nan")
        expected = "/metadata/last-updated-ms"
    elif kind == "extra":
        table["metadata"]["properties"] = {"default": hidden}
        expected = "/metadata/properties"
    else:
        table["metadata-location"] += hidden
        expected = "/metadata-location"
    fixture.destination.tables[key] = table
    try:
        fixture.run(inventory)
    except reg.RegistrationError as error:
        require(error.code == "registration_conflict", "conflict diagnostic: wrong refusal")
        require(expected in str(error), "conflict diagnostic: missing first differing path")
        require("gold.gold_table" in str(error), "conflict diagnostic: missing table identifier")
        require(hidden not in str(error), "conflict diagnostic: exposed response values")
    else:
        raise AssertionError("conflict diagnostic: accepted different metadata")


def cases() -> list[tuple[str, Callable[[], None]]]:
    result: list[tuple[str, Callable[[], None]]] = [
        ("successful retained inventory and exact repeat", successful_inventory),
        ("empty destination namespaces", empty_destination),
        ("failed namespace preparation", namespace_failure),
        ("late namespace reply stops mutation", late_namespace_reply),
    ]
    result.extend(
        ("publication " + kind, partial(publication_case, kind))
        for kind in ("missing_receipt", "missing_row", "fixture_mode", "failed_receipt", "missing_snapshot", "other_snapshot", "other_backup")
    )
    result.extend(
        [
            ("changed frozen receipt", changed_receipt),
            ("changed retained snapshot pin", changed_snapshot_pin),
            ("changed frozen inventory digest", changed_frozen_inventory),
            ("non-finite JSON metadata", invalid_json_constant),
            ("missing source table", missing_source),
        ]
    )
    result.extend(("inventory " + kind, partial(missing_inventory, kind)) for kind in ("missing", "duplicate"))
    result.extend(
        ("malformed inventory " + kind, partial(malformed_inventory, kind))
        for kind in ("unknown_field", "boolean_snapshot", "duplicate_table", "wrong_layer_type")
    )
    result.extend(("metadata " + kind, partial(object_case, kind)) for kind in ("missing", "digest", "size", "current_version", "unversioned"))
    result.extend(("source " + kind, partial(source_identity, kind)) for kind in ("uuid", "location", "snapshot", "manifest"))
    result.append(("source advance keeps retained input", source_advances))
    result.extend(
        ("destination " + kind, partial(conflict_case, kind))
        for kind in ("uuid", "metadata_location", "snapshot", "schema", "nan", "infinity", "negative_infinity")
    )
    result.extend(("reader " + kind, partial(reader_case, kind)) for kind in ("missing", "changed"))
    result.extend(
        [
            ("partial registration resumes", partial_registration),
            ("lost reply reconciles on explicit retry", lost_reply),
            ("concurrent identical registration", lambda: concurrent_registration(False)),
            ("concurrent conflicting registration", lambda: concurrent_registration(True)),
            ("late metadata change", late_object_change),
            ("lost source protection", lost_reservation),
            ("expired absolute deadline", lambda: deadline_case(False)),
            ("late source reply", lambda: deadline_case(True)),
        ]
    )
    result.extend(("source HTTP " + str(status), partial(http_case, status)) for status in (401, 403, 500))
    result.extend(
        [
            ("reusable exact metadata operation", reusable_operation),
            ("required committed contract coverage", required_contracts),
            ("launcher reader reservation protocol", launcher_reservations),
        ]
    )
    result.extend(
        [
            ("published snapshot absent before capture", missing_published_snapshot),
            ("matching publication omits required table", incomplete_matching_publication),
            ("duplicate metadata JSON key", duplicate_json_key),
            ("record size boundary", record_bound),
            ("unexpected source bucket", unexpected_bucket),
            ("unexpected exact registration bucket", unexpected_exact_bucket),
        ]
    )
    result.extend(
        ("unsafe source location " + kind, partial(unsafe_location, kind)) for kind in ("scheme", "path", "parent", "query", "fragment", "empty_component")
    )
    result.extend(("captured metadata version " + repr(version), partial(capture_without_version, version)) for version in ("", "null"))
    result.extend(("inventory pin boundary " + kind, partial(pin_boundary, kind)) for kind in ("bucket", "size"))
    result.extend(
        ("conflict diagnostic " + kind, partial(conflict_diagnostic, kind))
        for kind in ("nested", "missing", "list", "type", "metadata_location", "non_finite", "extra")
    )
    return result


def without_guard(message: str, function: Callable[..., Any] = reg.register_read_catalog, condition: str | None = None) -> Callable[..., Any]:
    tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
    changed = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.If) or len(node.body) != 1 or not isinstance(node.body[0], ast.Raise):
            continue
        error = node.body[0].exc
        if isinstance(error, ast.Call) and len(error.args) == 2 and isinstance(error.args[1], ast.Constant) and error.args[1].value == message:
            if condition is None:
                node.body = [ast.Pass()]
            else:
                if not isinstance(node.test, ast.BoolOp):
                    raise AssertionError("condition mutation has no combined guard")
                original = len(node.test.values)
                node.test.values = [term for term in node.test.values if ast.unparse(term) != condition]
                require(len(node.test.values) == original - 1, "condition mutation did not target exactly one term")
            changed += 1
    require(changed == 1, "guard mutation did not target exactly one production branch")
    with TemporaryDirectory(prefix="registration_guard_", dir=ROOT) as folder:
        source = Path(folder) / "mutant.py"
        source.write_text(ast.unparse(ast.fix_missing_locations(tree)), encoding="utf-8")
        module = SourceFileLoader("registration_guard_mutation", str(source)).get_code("registration_guard_mutation")
        if module is None:
            raise AssertionError("guard mutation did not load code")
    code = next(value for value in module.co_consts if isinstance(value, CodeType) and value.co_name == function.__name__)
    operation = FunctionType(code, vars(reg), function.__name__, function.__defaults__)
    operation.__kwdefaults__ = function.__kwdefaults__
    return operation


def mutation_result(name: str, operation: Callable[[], None], expected: str) -> dict[str, Any]:
    try:
        operation()
    except AssertionError as error:
        observed = str(error)
        caught = expected in observed
    except (reg.RegistrationError, ValueError, KeyError, TypeError) as error:
        code = error.code if isinstance(error, reg.RegistrationError) else type(error).__name__
        caught, observed = False, "unrelated refusal: " + code
    else:
        caught, observed = False, "defect was not caught"
    return {"name": name, "caught": caught, "expected": expected, "observed": observed}


def raw_catalog_comparison(loaded: Mapping[str, Any] | None, pin: reg.TablePin, metadata: Mapping[str, Any], code: str = "registration_conflict") -> None:
    if loaded is None or loaded.get("metadata-location") != pin.metadata_location or reg.canonical(loaded.get("metadata")) != reg.canonical(metadata):
        raise reg.RegistrationError(code, "the catalog does not expose the exact frozen table metadata")


def wrong_registry_request(self: reg.LauncherReservations, method: str, params: dict[str, Any], deadline: float) -> Any:
    if method == "status":
        params = {"what": "jobs"}
    reg.check_deadline(deadline, self.clock)
    return self.client.call(method, params, timeout=deadline - self.clock())


def mutations() -> list[dict[str, Any]]:
    planned: list[tuple[str, str, Callable[[], None], str]] = [
        ("skip authoritative publication comparison", "check_publications", lambda: publication_case("other_snapshot"), "other_snapshot: accepted"),
        ("skip exact required coverage", "check_coverage", lambda: missing_inventory("missing"), "inventory missing: accepted"),
        ("skip metadata object identity", "check_object", lambda: object_case("digest"), "metadata digest: accepted"),
        ("skip complete table comparison", "check_loaded", lambda: conflict_case("schema"), "conflicting schema: accepted"),
        ("skip retained source snapshot", "check_source_snapshot", lambda: source_identity("manifest"), "source manifest: accepted"),
        ("skip final reader comparison", "verify_reader", lambda: reader_case("missing"), "reader missing: accepted"),
        ("skip absolute deadline", "check_deadline", lambda: deadline_case(True), "late reply: accepted"),
        ("skip protection verification", "check_reservations", lost_reservation, "lost reservation: accepted"),
    ]
    outcomes = []
    for name, function, operation, expected in planned:
        with patch.object(reg, function, lambda *_args, **_kwargs: None):
            outcomes.append(mutation_result(name, operation, expected))
    with patch.object(reg.PolarisCatalog, "ensure_namespace", lambda *_args, **_kwargs: None):
        outcomes.append(
            mutation_result("skip missing namespace preparation", empty_destination, "empty catalog: namespace preparation failed with catalog_http")
        )
    guard_cases: list[tuple[str, str, Callable[[], None], str, Callable[..., Any], str | None]] = [
        (
            "skip snapshot to receipt binding",
            "the frozen table snapshot differs from its committed publication",
            changed_snapshot_pin,
            "changed retained input: accepted",
            reg.register_read_catalog,
            None,
        ),
        (
            "skip frozen inventory digest",
            "the resumed inventory differs from its independently frozen digest",
            changed_frozen_inventory,
            "changed frozen inventory: accepted edited bytes",
            reg.register_read_catalog,
            None,
        ),
        (
            "skip frozen published snapshot retention",
            "the frozen metadata does not retain the required snapshots",
            missing_published_snapshot,
            "published snapshot absent before capture: accepted",
            reg.pinned_metadata,
            None,
        ),
        (
            "skip exact published table membership",
            "a prerequisite publish receipt is incomplete or uncommitted",
            incomplete_matching_publication,
            "matching publication omits required table: accepted",
            reg.checked_receipts,
            "set(pins) != expected",
        ),
        (
            "skip matching table and metadata buckets",
            "metadata and table locations name different buckets",
            partial(pin_boundary, "bucket"),
            "pin boundary: accepted bucket",
            reg.validate_pin,
            None,
        ),
        (
            "skip immutable metadata version",
            "a table pin has no immutable metadata version",
            partial(capture_without_version, "null"),
            "captured metadata has no immutable version: accepted",
            reg.validate_pin,
            None,
        ),
        (
            "skip metadata pin size bound",
            "a metadata size is invalid or exceeds 32 MiB",
            partial(pin_boundary, "size"),
            "pin boundary: accepted size",
            reg.validate_pin,
            None,
        ),
        (
            "skip JSON record size bound",
            "a registration record exceeds the 32 MiB bound",
            record_bound,
            "record bound: accepted over 32 MiB",
            reg.read_document,
            None,
        ),
    ]
    for name, message, operation, expected, guard_function, condition in guard_cases:
        with patch.object(reg, guard_function.__name__, without_guard(message, guard_function, condition)):
            outcomes.append(mutation_result(name, operation, expected))
    extra: list[tuple[str, str, Callable[..., Any], Callable[[], None], str]] = [
        (
            "skip expected deployment bucket",
            "bucket_location",
            lambda value, _bucket: reg.s3_location(value),
            unexpected_bucket,
            "unexpected source bucket: accepted",
        ),
        (
            "skip first differing path",
            "first_difference",
            lambda *_args, **_kwargs: None,
            partial(conflict_diagnostic, "nested"),
            "conflict diagnostic: missing first differing path",
        ),
        (
            "skip non-finite response sanitization",
            "check_loaded",
            raw_catalog_comparison,
            partial(conflict_case, "nan"),
            "conflicting nan: unsanitized ValueError",
        ),
        (
            "skip source location scope",
            "s3_location",
            lambda value: value,
            partial(unsafe_location, "scheme"),
            "unsafe location scheme: accepted",
        ),
        (
            "skip duplicate JSON key refusal",
            "unique_object",
            dict,
            duplicate_json_key,
            "duplicate metadata JSON key: accepted",
        ),
    ]
    for name, target, replacement, operation, expected in extra:
        with patch.object(reg, target, replacement):
            outcomes.append(mutation_result(name, operation, expected))
    message = "the source snapshot protection is absent or belongs to another publication"
    for name, condition in (
        ("domain", "entry.get('domain') != 'transform'"),
        ("state", "entry.get('state') not in ('reserved', 'registered')"),
        ("snapshot", "entry.get('state') == 'registered' and entry.get('snapshot_set') != publish_run"),
    ):
        with patch.object(reg.LauncherReservations, "verify", without_guard(message, reg.LauncherReservations.verify, condition)):
            outcomes.append(mutation_result("skip launcher reservation " + name, launcher_reservations, "launcher reservation: accepted wrong " + name))
    with patch.object(reg.LauncherReservations, "call", wrong_registry_request):
        outcomes.append(mutation_result("ask for wrong launcher status", launcher_reservations, "launcher reservation: wrong status request"))
    outcomes.append(mutation_result("drop required bronze contract table", shortened_contract, "contracts: required namespace counts differ"))
    return outcomes


def main() -> int:
    """Run Synthetic source cases and planned defects, saving a repeatable local record."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--mutations", action="store_true")
    args = parser.parse_args()
    paths = [
        Path(__file__),
        Path(reg.__file__),
        ROOT / "scripts/lakehouse/gold_publish_record.py",
        ROOT / "config/lakehouse/bronze_tables.json",
        ROOT / "data_contracts/lakehouse/silver_publish.json",
        ROOT / "data_contracts/lakehouse/gold_contract.json",
    ]
    before = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    results: list[dict[str, Any]] = []
    for name, operation in cases():
        started = time.monotonic()
        try:
            operation()
        except (AssertionError, reg.RegistrationError, ValueError, KeyError, TypeError) as error:
            message = error.code if isinstance(error, reg.RegistrationError) else str(error)
            results.append({"name": name, "ok": False, "cause": message})
        else:
            results.append({"name": name, "ok": True})
        results[-1]["seconds"] = round(time.monotonic() - started, 6)
    planted: list[dict[str, Any]] = mutations() if args.mutations else []
    unchanged = before == {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    stamp = datetime.now(UTC).isoformat()
    report = {
        "at_utc": stamp,
        "classification": "Synthetic",
        "proof": "source cases with in-memory I/O",
        "source_hashes": before,
        "command": ".venv/bin/python -m scripts.lakehouse.run_catalog_registration_e2e --out " + str(args.out) + (" --mutations" if args.mutations else ""),
        "python": sys.version.split()[0],
        "seed": "fixed fixtures, no random source",
        "cases": results,
        "mutations": planted,
        "passed": sum(row["ok"] for row in results),
        "total": len(results),
        "sources_unchanged": unchanged,
        "limits": [
            "No AWS, native catalog, sockets, SDK workers, secret store or real launcher",
            "Authenticated I/O must enforce the common deadline before native integration",
        ],
        "cleanup": "done",
    }
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "report.json").write_bytes(encoded(report))
    (args.out / "retention.json").write_bytes(
        encoded(
            {
                "class": 4,
                "purpose": "catalog registration source verification",
                "created": stamp,
                "end_condition": "no active registration review cites this record",
            }
        )
    )
    ok = unchanged and all(row["ok"] for row in results) and all(row["caught"] for row in planted)
    sys.stdout.write(
        json.dumps(
            {
                "passed": report["passed"],
                "total": report["total"],
                "mutations_caught": sum(row["caught"] for row in planted),
                "mutations": len(planted),
                "cleanup": "done",
                "report": str(args.out / "report.json"),
            }
        )
        + "\n"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
