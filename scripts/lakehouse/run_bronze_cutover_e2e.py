"""E2E of the bronze cutover: launcher authority and Mac write gate, bronze inventory capture and registration with stand-ins.

Run ``.venv/bin/python -m scripts.lakehouse.run_bronze_cutover_e2e [--case NAME ...]``. A launcher case starts its own launcher on its own test
registry (the stand-ins of ``run_launcher_e2e``). An offline case uses in-memory catalog and object stand-ins (``run_catalog_registration_e2e``)
and a scratch folder. Nothing calls AWS or Docker. The report is written under ``data/e2e/bronze_cutover/<stamp>/``.
Failure IDs (F1 and the rest) refer to the bronze registration failure modes of the cutover design.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import functools
import hashlib
import json
import os
import signal
import socket
import stat
import sys
import tempfile
import threading
import time
import traceback
import urllib.parse
import zlib
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import duckdb

from orchestration import catalog_authority, launcher
from orchestration.catalog_transport import CatalogTransport, TransportSpec
from orchestration.pipeline_session import RoleSession
from scripts.acquisition.s3_store import AwsCli
from scripts.lakehouse import bronze_cutover, bronze_inventory, bronze_scan, cutover_route, mac_route_probe
from scripts.lakehouse import register_aws_catalog as reg
from scripts.lakehouse import run_catalog_registration_e2e as registration_e2e
from scripts.lakehouse import run_gold_publish_launcher_cases as gold_cases
from scripts.lakehouse import run_launcher_e2e as launcher_e2e
from scripts.lakehouse.run_launcher_e2e import (
    Env,
    catalog_script,
    catalog_up,
    expect_error,
    hold_forever,
    lifecycle_host,
    real_run,
    record_file,
    refused,
    run_lock,
)
from scripts.process import CompletedProcess, run_command

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "e2e" / "bronze_cutover"
BUCKET = "synthetic-bucket"
MAP_TABLES = ("alpha", "beta")
REQUIRED = frozenset({("bronze", name) for name in (*MAP_TABLES, *bronze_inventory.AUXILIARY)} | {("bronze_dictionary", name) for name in MAP_TABLES})
Case = Callable[[Env], dict[str, bool]]
Offline = Callable[[Path], dict[str, bool]]
CASES: dict[str, Case] = {}
OFFLINE: dict[str, Offline] = {}


def case(function: Case) -> Case:
    CASES[function.__name__] = function
    return function


def offline(function: Offline) -> Offline:
    OFFLINE[function.__name__] = function
    return function


class Source(registration_e2e.Catalog):
    """The frozen Mac catalog stand-in: the registration stand-in plus a paged table listing per namespace."""

    page = 3

    def __init__(self, objects: registration_e2e.Objects, events: list[str]) -> None:
        super().__init__(objects, events, "source")
        self.namespaces = {"bronze", "bronze_dictionary", "smoke_check", "duckdb_view_check"}
        self.extra: dict[str, list[str]] = {}

    def request(self, method: str, path: str, *, body: Mapping[str, Any] | None = None, deadline: float) -> tuple[int, object]:
        route, _, query = path.partition("?")
        if method == "GET" and route.endswith("/tables"):
            namespace = route.split("/")[-2]
            if namespace not in self.namespaces:
                return 404, None
            names = sorted({name for space, name in self.tables if space == namespace} | set(self.extra.get(namespace, [])))
            start = int(query.removeprefix("pageToken=")) if query else 0
            page = names[start : start + self.page]
            token = str(start + self.page) if start + self.page < len(names) else None
            return 200, {"identifiers": [{"namespace": [namespace], "name": name} for name in page], "next-page-token": token}
        return super().request(method, path, body=body, deadline=deadline)


def zigzag(value: int) -> bytes:
    """An Avro long: zigzag, then base-128 groups with a continuation bit."""
    value = (value << 1) ^ (value >> 63)
    result = bytearray()
    while value > 0x7F:
        result.append(value & 0x7F | 0x80)
        value >>= 7
    return bytes([*result, value])


def avro_text(text: str) -> bytes:
    raw = text.encode()
    return zigzag(len(raw)) + raw


def avro_file(schema: Mapping[str, Any], rows: list[bytes], codec: str = "deflate") -> bytes:
    """A one-block Avro object container file of already encoded rows, as Iceberg writes manifests."""
    sync = hashlib.sha256(json.dumps(schema).encode()).digest()[:16]
    metadata = {"avro.schema": json.dumps(schema).encode(), "avro.codec": codec.encode()}
    header = b"Obj\x01" + zigzag(len(metadata)) + b"".join(avro_text(key) + zigzag(len(value)) + value for key, value in metadata.items()) + zigzag(0)
    payload = b"".join(rows)
    if codec == "deflate":
        packer = zlib.compressobj(wbits=-15)
        payload = packer.compress(payload) + packer.flush()
    return header + sync + zigzag(len(rows)) + zigzag(len(payload)) + payload + sync


MANIFEST_LIST_SCHEMA = {
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path", "type": "string"},
        {"name": "manifest_length", "type": "long"},
        {"name": "content", "type": "int"},
        {"name": "added_snapshot_id", "type": ["null", "long"]},
    ],
}
MANIFEST_SCHEMA = {
    "type": "record",
    "name": "manifest_entry",
    "fields": [
        {"name": "status", "type": "int"},
        {
            "name": "data_file",
            "type": {
                "type": "record",
                "name": "r2",
                "fields": [
                    {"name": "content", "type": "int"},
                    {"name": "file_path", "type": "string"},
                    {"name": "file_format", "type": "string"},
                    {"name": "file_size_in_bytes", "type": "long"},
                    {"name": "value_counts", "type": {"type": "map", "values": "long"}},
                ],
            },
        },
    ],
}


def manifest_entry(status: int, path: str, size: int) -> bytes:
    data_file = zigzag(0) + avro_text(path) + avro_text("PARQUET") + zigzag(size) + zigzag(1) + avro_text("1") + zigzag(3) + zigzag(0)
    return zigzag(status) + data_file


@functools.cache
def parquet_fixture() -> bytes:
    """A synthetic zstd Parquet file of one row group, written by DuckDB as Iceberg's default codec would compress it."""
    with tempfile.TemporaryDirectory(prefix="bronze_parquet_") as scratch:
        path = Path(scratch) / "rows.parquet"
        with duckdb.connect(config={"autoinstall_known_extensions": False, "autoload_known_extensions": False}) as connection:
            rows = connection.sql("SELECT i AS id, 'synthetic row ' || i AS label FROM range(200) t(i)")
            rows.write_parquet(str(path), compression="zstd")
        return path.read_bytes()


def broken_parquet(kind: str) -> bytes:
    """The fixture with its footer zeroed, its row group cut out (the length and magic kept) or bytes of its first compressed page overwritten."""
    valid = parquet_fixture()
    footer = int.from_bytes(valid[-8:-4], "little")
    if kind == "footer":
        return valid[: -8 - footer] + bytes(footer) + valid[-8:]
    if kind == "truncated":
        return valid[:4] + valid[-8 - footer :]
    return valid[:44] + b"\xff" * 32 + valid[76:]


class DataStore:
    """The S3 objects as the lakehouse reader role sees them: manifest lists, manifests and data files, read whole or by byte range."""

    identity = "lakehouse_reader"

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.denied = False
        self.reads: list[str] = []
        self.snapshots: dict[str, int] = {}

    def put_table(self, location: str, snapshot: int, data: bytes | None = None) -> None:
        data = parquet_fixture() if data is None else data
        self.snapshots[location] = snapshot
        manifest = location + "/metadata/m0.avro"
        live, deleted = location + "/data/00001.parquet", location + "/data/00000.parquet"
        self.objects[manifest] = avro_file(MANIFEST_SCHEMA, [manifest_entry(2, deleted, 99), manifest_entry(1, live, len(data))])
        self.objects[location + "/metadata/snap.avro"] = avro_file(
            MANIFEST_LIST_SCHEMA, [avro_text(manifest) + zigzag(len(self.objects[manifest])) + zigzag(0) + zigzag(1) + zigzag(snapshot)], codec="null"
        )
        self.objects[live] = data

    def replace_data(self, location: str, data: bytes) -> None:
        """Replace the table's live data file, with a manifest entry of the new size."""
        self.put_table(location, self.snapshots[location], data)

    def read(self, location: str, byte_range: str | None, *, deadline: float) -> tuple[bytes, int]:
        self.reads.append(location)
        if self.denied:
            raise reg.RegistrationError("transport", "reading a data object failed (AccessDenied)")
        if location not in self.objects:
            raise reg.RegistrationError("transport", "reading a data object failed (NoSuchKey)")
        body = self.objects[location]
        if byte_range is None:
            return body, len(body)
        first, last = (int(part) for part in byte_range.removeprefix("bytes=").split("-"))
        return body[first : last + 1], len(body)


class Lake:
    """A frozen Mac catalog whose tables match their versioned metadata objects in S3."""

    def __init__(self, required: frozenset[reg.TableId] = REQUIRED) -> None:
        self.events: list[str] = []
        self.clock = registration_e2e.Clock()
        self.objects = registration_e2e.Objects(self.events)
        self.storage = DataStore()
        self.source = Source(self.objects, self.events)
        for number, (namespace, name) in enumerate(sorted(required), 1):
            self.put(namespace, name, number)
        self.catalog = bronze_inventory.SourceCatalog(self.source.request, "hai_lakehouse", clock=self.clock)

    def put(self, namespace: str, name: str, number: int, version: str = "version_1", bucket: str = BUCKET) -> dict[str, Any]:
        location = f"s3://{bucket}/lakehouse/{namespace}/{name}"
        metadata_location = location + f"/metadata/{number:05d}.metadata.json"
        document = {
            "format-version": 2,
            "table-uuid": f"00000000-0000-4000-8000-{number:012d}",
            "location": location,
            "last-sequence-number": 1,
            "last-updated-ms": 1,
            "last-column-id": 1,
            "schemas": [{"schema-id": 0, "type": "struct", "fields": [{"id": 1, "name": "synthetic_id", "required": False, "type": "long"}]}],
            "current-schema-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            "last-partition-id": 999,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "current-snapshot-id": 500 + number,
            "snapshots": [
                {"snapshot-id": 500 + number, "sequence-number": 1, "timestamp-ms": 1, "manifest-list": location + "/metadata/snap.avro", "summary": {}}
            ],
            "refs": {"main": {"snapshot-id": 500 + number, "type": "branch"}},
        }
        self.objects.put(metadata_location, document, version)
        self.storage.put_table(location, 500 + number)
        self.source.tables[(namespace, name)] = {"metadata-location": metadata_location, "metadata": copy.deepcopy(document)}
        return document

    def capture(self, required: frozenset[reg.TableId] = REQUIRED, per_table: float = 10) -> reg.Inventory:
        return bronze_inventory.capture(
            self.catalog, self.objects, required, bucket=BUCKET, deadline=self.clock() + 1000, per_table=per_table, clock=self.clock
        )


class HttpStandIn:
    """A loopback Polaris stand-in: a client-credentials token exchange in front of a catalog stand-in's request function."""

    def __init__(self, catalog: registration_e2e.Catalog, client_id: str, secret: str) -> None:
        self.catalog, self.credentials = catalog, (client_id, secret)
        self.tokens: set[str] = set()
        self.exchanges = 0
        self.delay = 0.0
        self.headers: list[str] = []
        stand_in = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                return

            def answer(self, status: int, document: object) -> None:
                raw = json.dumps(document).encode() if document is not None else b""
                self.send_response(status)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def handle_any(self) -> None:
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                stand_in.headers.append(json.dumps(dict(self.headers)))
                time.sleep(stand_in.delay)
                if self.path == "/api/catalog/v1/oauth/tokens":
                    form = dict(urllib.parse.parse_qsl(raw.decode()))
                    if (form.get("client_id"), form.get("client_secret")) != stand_in.credentials:
                        return self.answer(401, {"error": "synthetic_private_error_do_not_record"})
                    stand_in.exchanges += 1
                    token = f"synthetic_token_{stand_in.exchanges}"
                    stand_in.tokens.add(token)
                    return self.answer(200, {"access_token": token, "token_type": "bearer", "expires_in": 3600})
                if self.headers.get("Authorization", "").removeprefix("Bearer ") not in stand_in.tokens:
                    return self.answer(401, None)
                status, document = stand_in.catalog.request(self.command, self.path, body=json.loads(raw) if raw else None, deadline=0)
                return self.answer(status, document)

            do_GET = do_POST = do_DELETE = handle_any

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = int(self.server.server_address[1])
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)


class Destination(registration_e2e.Catalog):
    """The AWS catalog stand-in: the registration stand-in plus the management catalog record the preflight reads."""

    def __init__(self, objects: registration_e2e.Objects, events: list[str], role: str = "destination", tables: dict[reg.TableId, Any] | None = None) -> None:
        super().__init__(objects, events, role, tables)
        location = f"s3://{BUCKET}/lakehouse/"
        self.management: dict[str, Any] | None = {
            "name": "hai_lakehouse",
            "properties": {"default-base-location": location},
            "storageConfigInfo": {"storageType": "S3", "allowedLocations": [location], "stsUnavailable": True},
        }
        self.refuse_reads = False

    def request(self, method: str, path: str, *, body: Mapping[str, Any] | None = None, deadline: float) -> tuple[int, object]:
        if path == "/api/management/v1/catalogs/hai_lakehouse":
            return (200, copy.deepcopy(self.management)) if self.management is not None else (404, None)
        if self.refuse_reads:
            return 403, {"error": "synthetic_private_error_do_not_record"}
        return super().request(method, path, body=body, deadline=deadline)


class Cutover:
    """A captured lake, an empty AWS catalog and its reader view over the same tables."""

    def __init__(self) -> None:
        self.lake = Lake()
        self.inventory = self.lake.capture()
        self.clock = self.lake.clock
        self.destination_stand_in = Destination(self.lake.objects, self.lake.events)
        self.reader_stand_in = Destination(self.lake.objects, self.lake.events, "reader", self.destination_stand_in.tables)
        self.destination = reg.PolarisCatalog(self.destination_stand_in.request, "hai_lakehouse", clock=self.clock)
        self.reader = reg.PolarisCatalog(self.reader_stand_in.request, "hai_lakehouse", clock=self.clock)

    def register(self, per_table: float = 10) -> dict[str, list[reg.TableId]]:
        return bronze_cutover.register_tables(
            self.destination, self.lake.objects, self.inventory, bucket=BUCKET, deadline=self.clock() + 1000, per_table=per_table, clock=self.clock
        )

    def verify(self) -> dict[str, Any]:
        return bronze_cutover.verify_tables(
            self.reader,
            self.lake.catalog,
            self.lake.objects,
            self.lake.storage,
            self.inventory,
            bucket=BUCKET,
            deadline=self.clock() + 1000,
            per_table=10,
            clock=self.clock,
        )

    def preflight(self) -> None:
        bronze_cutover.preflight(self.destination_stand_in.request, BUCKET, deadline=self.clock() + 10, clock=self.clock)


def code_of(function: Callable[[], Any]) -> tuple[str, str]:
    """The cause code and message of a registration refusal, or two empty strings when the call passed."""
    try:
        function()
    except reg.RegistrationError as error:
        return error.code, str(error)
    return "", ""


def command(command_id: str, epoch: int, state: str, cutover: str, action: str = "transition") -> dict[str, Any]:
    return {
        "command_id": command_id,
        "expected_epoch": epoch,
        "action": action,
        "state": state,
        "cutover_id": cutover,
        "reason": f"bronze cutover E2E {command_id}",
        "time": datetime.now(UTC).isoformat(),
    }


def owner_call(env: Env, body: dict[str, Any], *, tty: bool = True, confirmation: str | None = None) -> dict[str, Any]:
    text = catalog_authority.confirmation_text(body) if confirmation is None else confirmation
    return dict(env.client().call("catalog_authority", {"command": body, "confirmation": text, "tty": tty, "owner": True}))


def authority(env: Env) -> dict[str, Any]:
    return dict(env.state()["launcher"].get("catalog_authority") or {})


def writer_submit(env: Env, principal: dict[str, Any], launch: str) -> dict[str, Any]:
    return env.submit(principal, "spark_write", launch, ["--group", "hai"], {"job": "bronze"})


def end_run(env: Env, principal: dict[str, Any], run_id: str) -> None:
    client = env.client()
    for attempt in env.state()["attempts"].values():
        if attempt["owner"] == f"run:{run_id}" and attempt["phase"] != "ended":
            client.call("cancel", {"attempt_id": attempt["id"]}, principal)
            env.wait_ended(attempt["id"])
    client.call("finalize", {}, principal)
    client.call("release", {"record": str(record_file(env, run_id))}, principal, timeout=60)


@case
def freeze_gate_and_fence(env: Env) -> dict[str, bool]:
    """F1, F2, F3, F4: the freeze refuses Mac writes, the fence waits for the running writer, a repeat returns the stored record."""
    catalog_script(env)
    env.start()
    catalog_up(env)
    writer = real_run(env, "writer", "acquisition", write_prefixes=["lakehouse/bronze/"])
    checks: dict[str, bool] = {}
    checks["Mac authority initializes at epoch 1"] = owner_call(env, command("c_init", 0, "mac_writer", "mac_initial", "initialize"))["epoch"] == 1
    running = writer_submit(env, writer, "w1")
    env.wait_phase(running["attempt_id"], ("started",))
    checks["a Mac write starts under mac_writer"] = True
    frozen = owner_call(env, command("c_freeze", 1, "freezing", "cut1"))
    checks["freeze records freezing at P1 with every layer off"] = frozen["state"] == "freezing" and set(frozen["production_layers"].values()) == {"off"}
    checks["F1: a Mac write submit after the freeze is refused"] = refused(
        expect_error(lambda: writer_submit(env, writer, "w2")), "refused", "catalog authority"
    )
    fence = command("c_fence", 2, "source_fenced", "cut1")
    busy = expect_error(lambda: owner_call(env, fence))
    checks["F2: the fence is refused while a Mac write runs and the state stays freezing"] = (
        refused(busy, "refused", "Mac catalog writes are still open") and authority(env)["state"] == "freezing"
    )
    checks["F2: the running writer is not stopped by the freeze"] = env.phase(running["attempt_id"]) == "started"
    env.client().call("cancel", {"attempt_id": running["attempt_id"]}, writer)
    env.wait_ended(running["attempt_id"])
    fenced = owner_call(env, fence)
    checks["the same fence command passes once no Mac write is open"] = fenced["state"] == "source_fenced" and fenced["epoch"] == 3
    checks["F3: a repeated command returns the stored record without a new epoch"] = owner_call(env, fence) == fenced and authority(env)["epoch"] == 3
    checks["F3: a stale epoch is refused"] = refused(expect_error(lambda: owner_call(env, command("c_stale", 1, "refreshing", "cut1"))), "refused", "stale")
    refresh = command("c_refresh", 3, "refreshing", "cut1")
    checks["F4: a command without a terminal is refused"] = refused(expect_error(lambda: owner_call(env, refresh, tty=False)), "refused", "terminal")
    checks["F4: a wrong typed confirmation is refused"] = refused(expect_error(lambda: owner_call(env, refresh, confirmation="yes")), "refused", "typed")
    checks["refused commands left the record unchanged"] = authority(env)["epoch"] == 3
    intents = env.config.registry.path / "catalog_authority_intents"
    checks["every applied command left one intent file"] = sorted(path.name for path in intents.iterdir()) == ["c_fence.json", "c_freeze.json", "c_init.json"]
    end_run(env, writer, "writer")
    return checks


@case
def queued_write_fenced(env: Env) -> dict[str, bool]:
    """F1: a Mac write queued before the freeze ends as fenced instead of starting after it."""
    catalog_script(env)
    instance = lifecycle_host(env, target="q1", held_phase="queued")
    catalog_up(env)
    writer = real_run(env, "writer", "acquisition", write_prefixes=["lakehouse/bronze/"])
    owner_call(env, command("c_init", 0, "mac_writer", "mac_initial", "initialize"))
    queued = writer_submit(env, writer, "q1")
    was_queued = env.phase(queued["attempt_id"]) == "queued"
    owner_call(env, command("c_freeze", 1, "freezing", "cut1"))
    instance.held_phase = ""
    ended = env.wait_ended(queued["attempt_id"])
    checks = {
        "the write was queued before the freeze": was_queued,
        "the queued write ends as fenced": ended["reason"] == "fenced",
        "no container was created for it": not launcher_e2e.creates(env, "-q1-"),
        "the fence then passes": owner_call(env, command("c_fence", 2, "source_fenced", "cut1"))["state"] == "source_fenced",
    }
    end_run(env, writer, "writer")
    return checks


def checksum_submit(env: Env, principal: dict[str, Any], launch: str, args: list[str]) -> dict[str, Any]:
    return env.submit(principal, "spark_write", launch, args, {"job": "checksums"})


@case
def mac_gate_arguments(env: Env) -> dict[str, bool]:
    """F1 by job and arguments: after the freeze, checksum reads pass at submit, queued dispatch and fence inspection; smoke, the checksum
    fixture and every other write stay refused; inert dbt commands pass and test runs stay exempt."""
    catalog_script(env)
    instance = lifecycle_host(env, target="qread", held_phase="queued")
    catalog_up(env)
    reader = real_run(env, "reader", "acquisition", write_prefixes=["lakehouse/"])
    owner_call(env, command("c_init", 0, "mac_writer", "mac_initial", "initialize"))
    queued = checksum_submit(env, reader, "qread", ["--tables", "alpha"])
    running = checksum_submit(env, reader, "r1", ["--tables", "alpha"])
    env.wait_phase(running["attempt_id"], ("started",))
    checks: dict[str, bool] = {"a checksum read is queued before the freeze": env.phase(queued["attempt_id"]) == "queued"}
    owner_call(env, command("c_freeze", 1, "freezing", "cut1"))
    late = expect_error(lambda: checksum_submit(env, reader, "r2", ["--tables", "beta"]))
    checks["a checksum read is accepted at submit after the freeze"] = late is None
    for label, launch, args, params in (
        ("the checksum fixture create", "f1", ["--fixture", "create"], {"job": "checksums"}),
        ("the checksum fixture drop", "f2", ["--tables", "alpha", "--fixture", "drop"], {"job": "checksums"}),
        ("smoke", "s1", [], {"job": "smoke"}),
        ("a bronze load", "b1", ["--group", "hai"], {"job": "bronze"}),
    ):
        refusal = expect_error(functools.partial(env.submit, reader, "spark_write", launch, args, params))
        checks[f"F1: {label} is refused at submit after the freeze"] = refused(refusal, "refused", "catalog authority")
    for attempt in env.state()["attempts"].values():
        if attempt["owner"] == "run:reader" and attempt["phase"] != "ended" and attempt["id"] != queued["attempt_id"]:
            env.client().call("cancel", {"attempt_id": attempt["id"]}, reader)
            env.wait_ended(attempt["id"])
    instance.held_phase = ""
    started = env.wait_phase(queued["attempt_id"], ("started", "ended"))
    checks["the queued checksum read starts after the freeze instead of ending as fenced"] = env.phase(queued["attempt_id"]) == "started" and (
        started.get("reason") != "fenced"
    )
    fenced = owner_call(env, command("c_fence", 2, "source_fenced", "cut1"))
    checks["the fence passes while checksum reads run"] = fenced["state"] == "source_fenced"
    write = launcher.mac_catalog_write
    checks["dbt ls and parse are not Mac writes"] = not write("real_dbt", {}, ["ls"], "real") and not write("real_dbt", {}, ["parse", "--quiet"], "real")
    checks["dbt build, compile and an empty dbt command are Mac writes"] = all(write("real_dbt", {}, args, "real") for args in (["build"], ["compile"], []))
    checks["checksum arguments outside the grammar count as a write"] = write("spark_write", {"job": "checksums"}, ["--bogus"], "real")
    checks["gold publish counts as a write"] = write("gold_publish", {"job": "gold_publish"}, [], "real")
    checks["a test run stays exempt"] = not write("spark_write", {"job": "smoke"}, [], "fixture") and not write("real_dbt", {}, ["build"], "fixture")
    end_run(env, reader, "reader")
    return checks


@case
def gold_reconcile_read(env: Env) -> dict[str, bool]:
    """F1 by gold publish job: the candidate reconcile reads the catalog as the reader principal, so after the freeze it passes at submit,
    dispatch and fence inspection; the other gold publish steps stay writes."""
    hold_forever(env)
    catalog_script(env)
    env.start()
    catalog_up(env)
    principal = gold_cases.publish_run(env, gold_cases.GOLD_RUN, "real", ["lakehouse/gold/"], accepted=True)
    owner_call(env, command("g_init", 0, "mac_writer", "mac_initial", "initialize"))
    owner_call(env, command("g_freeze", 1, "freezing", "cut1"))
    checks: dict[str, bool] = {}
    reply: dict[str, Any] = {}
    error = expect_error(lambda: reply.update(env.submit(principal, "gold_publish", "after", [], {"job": "reconcile_candidates"})))
    checks["the candidate reconcile is accepted at submit after the freeze"] = error is None and "attempt_id" in reply
    if "attempt_id" in reply:
        env.wait_phase(reply["attempt_id"], ("started", "ended"))
        checks["the candidate reconcile is dispatched after the freeze"] = env.phase(reply["attempt_id"]) == "started"
        fenced = owner_call(env, command("g_fence", 2, "source_fenced", "cut1"))
        checks["the fence passes while the candidate reconcile runs"] = fenced["state"] == "source_fenced"
    write = launcher.mac_catalog_write
    checks["only the candidate reconcile among the gold publish steps is a read"] = [
        job for job in launcher.GOLD_JOBS if not write("gold_publish", {"job": job}, [], "real")
    ] == ["reconcile_candidates"]
    end_run(env, principal, gold_cases.GOLD_RUN)
    return checks


@case
def rollback_paths(env: Env) -> dict[str, bool]:
    """F5, B1, B2, B4: rollback from P1 and P3, a reused cutover ID, and the refusal after a writer was admitted (P5)."""
    env.start()
    checks: dict[str, bool] = {}
    owner_call(env, command("c_init", 0, "mac_writer", "mac_initial", "initialize"))
    owner_call(env, command("p1_freeze", 1, "freezing", "cut1"))
    owner_call(env, command("p1_back", 2, "rollback_pending", "cut1"))
    rollback = command("p1_mac", 3, "mac_writer", "cut1")
    back = owner_call(env, rollback)
    checks["B2: rollback at P1 returns to mac_writer"] = back["state"] == "mac_writer" and back["phase"] == "P1"
    checks["B4: a repeated rollback returns the stored record"] = owner_call(env, rollback) == back
    checks["F3: the same command ID with other content is refused"] = refused(
        expect_error(lambda: owner_call(env, {**rollback, "reason": "changed"})), "refused", "identity conflict"
    )
    checks["F5: a new freeze that reuses the cutover ID is refused"] = refused(
        expect_error(lambda: owner_call(env, command("reuse", 4, "freezing", "cut1"))), "refused", "new cutover identity"
    )
    epoch = 4
    for name, state in (("p3_freeze", "freezing"), ("p3_fence", "source_fenced"), ("p3_refresh", "refreshing"), ("p3_back", "rollback_pending")):
        owner_call(env, command(name, epoch, state, "cut2"))
        epoch += 1
    direct = expect_error(lambda: owner_call(env, command("p3_direct", 8, "mac_writer", "cut2")))
    checks["B2: rollback from P3 cannot skip source verification"] = refused(direct, "refused", "source verification")
    owner_call(env, command("p3_verify", 8, "source_verify", "cut2"))
    restored = owner_call(env, command("p3_mac", 9, "mac_writer", "cut2"))
    checks["B2: rollback from P3 returns to mac_writer through source_verify"] = restored["state"] == "mac_writer" and restored["epoch"] == 10
    epoch = 10
    for name, state in (("p5_freeze", "freezing"), ("p5_fence", "source_fenced"), ("p5_refresh", "refreshing"), ("p5_switch", "aws_writer")):
        owner_call(env, command(name, epoch, state, "cut3"))
        epoch += 1
    checks["the switch records aws_writer at P4 with every layer off"] = authority(env)["phase"] == "P4" and set(
        authority(env)["production_layers"].values()
    ) == {"off"}
    with run_lock.transaction(run_lock.Registry(env.registry, "test")) as registry_state:
        registry_state["launcher"]["catalog_authority"]["phase"] = "P5"
    late = expect_error(lambda: owner_call(env, command("p5_back", 14, "rollback_pending", "cut3")))
    checks["B1: rollback after an admitted AWS writer (P5) is refused"] = refused(late, "refused", "admitted writer")
    return checks


@offline
def inventory_capture(folder: Path) -> dict[str, bool]:
    """I1, I7, I8, F7: capture lists both namespaces, writes the inventory once and atomically and refuses a different second capture."""
    lake = Lake()
    lake.source.page = 2
    checks: dict[str, bool] = {}
    map_rows = json.loads((ROOT / "config/lakehouse/bronze_tables.json").read_text())["tables"]
    real = bronze_inventory.required_bronze(ROOT)
    checks["I1: the real required set is every map table twice plus the four auxiliary tables"] = (
        len(real) == 2 * len(map_rows) + 4 and {("bronze", name) for name in bronze_inventory.AUXILIARY} <= real
    )
    inventory = lake.capture()
    checks["a paged listing captures every required table in sorted order"] = [pin.identifier for pin in inventory.tables] == sorted(REQUIRED)
    checks["the bronze inventory has no publications"] = inventory.publications == () and inventory.document()["publications"] == []
    cutover = folder / "cutover" / "cut1"
    digest = bronze_inventory.write_inventory(cutover, inventory)
    path = cutover / bronze_inventory.INVENTORY_NAME
    checks["I7: the inventory file holds the canonical bytes of its digest"] = hashlib.sha256(path.read_bytes()).hexdigest() == digest == inventory.sha256
    checks["I7: the inventory and digest files are mode 0600"] = all(
        stat.S_IMODE(item.stat().st_mode) == 0o600 for item in (path, cutover / bronze_inventory.DIGEST_NAME)
    )
    checks["I7: no temporary file is left"] = sorted(item.name for item in cutover.iterdir()) == sorted(
        (bronze_inventory.INVENTORY_NAME, bronze_inventory.DIGEST_NAME)
    )
    checks["I8: an identical second capture keeps the same digest"] = bronze_inventory.write_inventory(cutover, lake.capture()) == digest
    lake.put("bronze", "alpha", 1, version="version_2")
    changed = lake.capture()
    checks["I8: a different second capture is refused and the file is unchanged"] = (
        code_of(lambda: bronze_inventory.write_inventory(cutover, changed))[0] == "inventory" and hashlib.sha256(path.read_bytes()).hexdigest() == digest
    )
    parsed = bronze_inventory.read_inventory(path, digest, REQUIRED)
    checks["the bronze parser accepts the frozen file and its digest"] = parsed == inventory
    checks["the bronze parser refuses another digest"] = code_of(lambda: bronze_inventory.read_inventory(path, "0" * 64, REQUIRED))[0] == "inventory"
    document = inventory.document()
    checks["the read-run parser refuses a bronze inventory"] = code_of(lambda: reg.inventory_from_document(document, REQUIRED))[0] == "publication"
    with_publication = {**document, "publications": [{"layer": "silver"}]}
    checks["the bronze parser refuses publications"] = code_of(lambda: bronze_inventory.inventory_from_document(with_publication, REQUIRED))[0] == "inventory"
    checks["the bronze parser refuses an extra field"] = (
        code_of(lambda: bronze_inventory.inventory_from_document({**document, "x": 1}, REQUIRED))[0] == "inventory"
    )
    fewer = {**document, "tables": document["tables"][1:]}
    checks["the bronze parser refuses a missing table"] = code_of(lambda: bronze_inventory.inventory_from_document(fewer, REQUIRED))[0] == "coverage"
    extra = Lake()
    extra.source.extra["bronze"] = ["stray"]
    code, message = code_of(extra.capture)
    checks["I1: a listed table outside the set stops with its name"] = code == "coverage" and "bronze.stray" in message
    missing = Lake()
    del missing.source.tables[("bronze_dictionary", "beta")]
    code, message = code_of(missing.capture)
    checks["I1: a set table not listed stops with its name"] = code == "coverage" and "bronze_dictionary.beta" in message
    scratch = Lake()
    scratch.source.extra["smoke_check"] = ["leftover"]
    code, message = code_of(scratch.capture)
    checks["F7: a non-empty scratch namespace stops capture"] = code == "coverage" and "smoke_check" in message
    empty = Lake(frozenset())
    checks["I1: an empty catalog stops capture"] = code_of(empty.capture)[0] == "coverage"
    slow = Lake()
    slow.objects.hook = lambda location, version: setattr(slow.clock, "now", slow.clock.now + 20)
    target = folder / "cutover" / "slow"
    code, message = code_of(lambda: bronze_inventory.write_inventory(target, slow.capture(per_table=10)))
    checks["I7: a per-table deadline stops capture with the table named and writes nothing"] = (
        code == "deadline" and "bronze." in message and not target.exists()
    )
    return checks


@offline
def inventory_identity(folder: Path) -> dict[str, bool]:
    """I2, I3, I5, I6, F6: capture stops with the table named when its snapshot, version, bytes or location do not hold."""
    checks: dict[str, bool] = {}
    no_snapshot = Lake()
    document = no_snapshot.put("bronze", "alpha", 1)
    document["current-snapshot-id"] = -1
    no_snapshot.objects.put(no_snapshot.source.tables[("bronze", "alpha")]["metadata-location"], document)
    no_snapshot.source.tables[("bronze", "alpha")]["metadata"] = copy.deepcopy(document)
    code, message = code_of(no_snapshot.capture)
    checks["I2: a table without a current snapshot stops with its name"] = code == "metadata_identity" and "bronze.alpha" in message
    null_version = Lake()
    null_version.put("bronze", "beta", 2, version="null")
    code, message = code_of(null_version.capture)
    checks["I3: a null object version stops with the table named"] = code == "metadata_identity" and "bronze.beta" in message
    drift = Lake()
    drift.source.tables[("bronze", "alpha")]["metadata"]["last-updated-ms"] = 2
    code, message = code_of(drift.capture)
    checks["I5 and F6: catalog bytes that differ from the file stop with the JSON path"] = (
        code == "metadata_identity" and "/metadata/last-updated-ms" in message
    )
    foreign = Lake()
    foreign.put("bronze", "alpha", 1, bucket="other-bucket")
    code, message = code_of(foreign.capture)
    checks["I6: a metadata location in another bucket stops"] = code == "metadata_identity" and "bronze.alpha" in message
    return checks


@offline
def s3_metadata_reads(folder: Path) -> dict[str, bool]:
    """I3, I4: the S3 adapter reads the exact or current version through AwsCli and maps a denied read to the transport code."""
    calls: list[list[str]] = []
    body = b'{"synthetic": true}\n'

    def runner(command: list[str], **_: Any) -> SimpleNamespace:
        calls.append(command)
        if "--key" in command and command[command.index("--key") + 1].endswith("denied.json"):
            return SimpleNamespace(returncode=254, stdout="", stderr="An error occurred (AccessDenied) when calling the GetObject operation")
        Path(command[-1]).write_bytes(body)
        version = command[command.index("--version-id") + 1] if "--version-id" in command else "current_version"
        return SimpleNamespace(returncode=0, stdout=json.dumps({"VersionId": version, "ContentLength": len(body)}), stderr="")

    settings = {"aws_profile": "synthetic", "aws_region": "us-east-1", "data_bucket_name": BUCKET, "expected_account_id": "synthetic_account"}
    reader = bronze_inventory.S3Metadata(AwsCli(settings, runner=runner), BUCKET)
    exact = reader.read(f"s3://{BUCKET}/lakehouse/bronze/a/metadata/1.json", "version_7", deadline=time.monotonic() + 30)
    current = reader.read(f"s3://{BUCKET}/lakehouse/bronze/a/metadata/1.json", None, deadline=time.monotonic() + 30)
    code, _ = code_of(lambda: reader.read(f"s3://{BUCKET}/lakehouse/bronze/a/metadata/denied.json", None, deadline=time.monotonic() + 30))
    other, _ = code_of(lambda: reader.read("s3://other-bucket/lakehouse/bronze/a/metadata/1.json", None, deadline=time.monotonic() + 30))
    return {
        "an exact version read returns that version and the bytes": exact == reg.ObjectValue("version_7", body),
        "a current read returns the current version": current.version_id == "current_version",
        "every read names the project bucket key, never another bucket": all(command[command.index("--key") + 1].startswith("lakehouse/") for command in calls),
        "I4: AccessDenied maps to the transport cause": code == "transport",
        "a location in another bucket is refused before any call": other == "metadata_identity" and len(calls) == 3,
    }


@offline
def polaris_http(folder: Path) -> dict[str, bool]:
    """R10, R6: the bounded client exchanges the token in memory, repeats once after a 401, keeps the secret out of errors and meets its deadline."""
    lake = Lake()
    secret = "synthetic_secret_" + hashlib.sha256(str(folder).encode()).hexdigest()[:12]
    stand_in = HttpStandIn(lake.source, "synthetic_client", secret)
    try:
        client = bronze_inventory.PolarisHttp("127.0.0.1", stand_in.port, client_id="synthetic_client", secret=secret)
        source = bronze_inventory.SourceCatalog(client, "hai_lakehouse")
        inventory = bronze_inventory.capture(source, lake.objects, REQUIRED, bucket=BUCKET, deadline=time.monotonic() + 60, per_table=10)
        checks = {
            "capture over loopback HTTP equals the in-memory capture": inventory == lake.capture(),
            "one token exchange serves the whole capture": stand_in.exchanges == 1,
            "the secret is not in the client's representation": secret not in repr(client) and secret not in repr(source.__dict__),
        }
        stand_in.tokens.clear()
        checks["a revoked token is exchanged once more and the request passes"] = (
            source.list_tables("bronze", deadline=time.monotonic() + 30)
            == [
                "alpha",
                "beta",
                *sorted(bronze_inventory.AUXILIARY),
            ]
            and stand_in.exchanges == 2
        )
        checks["the secret never travels in a request header"] = all(secret not in header for header in stand_in.headers)
        wrong = bronze_inventory.SourceCatalog(
            bronze_inventory.PolarisHttp("127.0.0.1", stand_in.port, client_id="synthetic_client", secret=secret[::-1]), "hai_lakehouse"
        )
        code, message = code_of(lambda: wrong.list_tables("bronze", deadline=time.monotonic() + 30))
        checks["R10: refused credentials give the authentication cause and no body or secret"] = (
            code == "authentication" and "synthetic_private" not in message and secret[::-1] not in message
        )
        stand_in.delay = 3.0
        started = time.monotonic()
        code, _ = code_of(lambda: source.list_tables("bronze", deadline=time.monotonic() + 0.5))
        checks["R6: a slow catalog fails visibly within the deadline"] = code in ("transport", "deadline") and time.monotonic() - started < 2.5
        stand_in.delay = 0
    finally:
        stand_in.close()
    return checks


class SlowPeer:
    """A loopback peer that accepts one connection at a time and then sends the given chunks with a pause between them, or nothing."""

    def __init__(self, chunks: list[bytes], pause: float) -> None:
        self.chunks, self.pause = chunks, pause
        self.closed_early = threading.Event()
        self.server = socket.create_server(("127.0.0.1", 0))
        self.server.settimeout(0.2)
        self.port = int(self.server.getsockname()[1])
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()

    def serve(self) -> None:
        while not self.stop.is_set():
            try:
                connection, _ = self.server.accept()
            except TimeoutError:
                continue
            with connection:
                connection.settimeout(0.2)
                with contextlib.suppress(OSError):
                    connection.recv(65536)
                for chunk in self.chunks:
                    if self.stop.wait(self.pause):
                        break
                    try:
                        connection.sendall(chunk)
                    except OSError:
                        self.closed_early.set()
                        break
                if not self.chunks:
                    self.stop.wait(5)

    def close(self) -> None:
        self.stop.set()
        self.thread.join(5)
        self.server.close()


def http_outcome(client: bronze_inventory.PolarisHttp, deadline: float) -> str:
    """The cause code of one bounded request, or the exception type when it was not a registration refusal."""
    try:
        client.send("GET", "/api/catalog/v1/config", None, {}, deadline)
    except reg.RegistrationError as error:
        return error.code
    except Exception as error:
        return type(error).__name__
    return ""


@offline
def http_absolute_deadline(folder: Path) -> dict[str, bool]:
    """R6: the bounded client stops at the absolute deadline during the header and body reads and closes the connection, for a silent peer,
    for a peer that keeps sending a few bytes and for a response that owns its socket (``Connection: close`` or HTTP/1.0)."""
    checks: dict[str, bool] = {}
    body = b"x" * 600
    head = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 600\r\n\r\n"
    close_head = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: 600\r\n\r\n"
    old_head = b"HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n"
    for label, chunks in (
        ("a silent peer", []),
        ("a peer that trickles the headers", [head[index : index + 3] for index in range(0, len(head), 3)]),
        ("a peer that trickles the body", [head, *(body[index : index + 3] for index in range(0, len(body), 3))]),
        ("a closing peer that trickles the body", [close_head, *(body[index : index + 3] for index in range(0, len(body), 3))]),
        ("an HTTP/1.0 peer that trickles the body", [old_head, *(body[index : index + 3] for index in range(0, len(body), 3))]),
    ):
        peer = SlowPeer(chunks, 0.025)
        try:
            client = bronze_inventory.PolarisHttp("127.0.0.1", peer.port, client_id="synthetic_client", secret=hashlib.sha256(label.encode()).hexdigest())
            started = time.monotonic()
            code = http_outcome(client, started + 0.3)
            elapsed = time.monotonic() - started
            checks[f"R6: {label} fails with the deadline cause"] = code == "deadline"
            checks[f"R6: {label} is stopped within the absolute deadline"] = elapsed < 0.45
            checks[f"R6: the connection to {label} is closed at expiry"] = not chunks or peer.closed_early.wait(2)
        finally:
            peer.close()
    return checks


@offline
def role_storage_reads(folder: Path) -> dict[str, bool]:
    """V1: the live storage reads through the reader role's session values, never the project profile, and reads the size from the range."""
    calls: list[tuple[list[str], dict[str, str]]] = []
    body = b"PAR1"

    def process(program: str, arguments: list[str], *, env: Mapping[str, str], timeout: float) -> CompletedProcess[str]:
        calls.append(([program, *arguments], dict(env)))
        Path(arguments[-1]).write_bytes(body)
        return CompletedProcess(arguments, 0, json.dumps({"ContentRange": "bytes 0-3/120", "ContentLength": 4}), "")

    token = "synthetic_token_" + hashlib.sha256(str(folder).encode()).hexdigest()[:12]
    session = RoleSession("reader", "synthetic_key", "synthetic_secret_value", token, 4102444800)
    settings = {"aws_profile": "synthetic_profile", "aws_region": "us-east-1", "data_bucket_name": BUCKET, "expected_account_id": "synthetic_account"}
    storage = bronze_scan.RoleStorage(settings, session, process=process)
    read = storage.read(f"s3://{BUCKET}/lakehouse/bronze/a/data/1.parquet", "bytes=0-3", deadline=time.monotonic() + 30)
    command, env = calls[0]
    return {
        "a range read returns the bytes and the whole object size": read == (body, 120),
        "the command names no profile": "--profile" not in command and "synthetic_profile" not in command,
        "the session values select the identity through the environment only": env.get("AWS_SESSION_TOKEN") == token
        and env.get("AWS_ACCESS_KEY_ID") == "synthetic_key"
        and "AWS_PROFILE" not in env
        and not any(token in part or "synthetic_secret_value" in part for part in command),
        "the read names the byte range and the bucket key": command[command.index("--range") + 1] == "bytes=0-3"
        and command[command.index("--key") + 1] == "lakehouse/bronze/a/data/1.parquet",
        "the identity label is the lakehouse reader": storage.identity == "lakehouse_reader",
    }


@offline
def register_and_verify(folder: Path) -> dict[str, bool]:
    """R1, R3, R5, Q3: preflight, namespace creation, registration, verification and a write-once receipt; a rerun gives unchanged."""
    cut = Cutover()
    cut.preflight()
    first = cut.register()
    checks = {
        "every table is registered on the first run": first == {"registered": [pin.identifier for pin in cut.inventory.tables], "unchanged": []},
        "only the absent bronze_dictionary namespace is created": cut.destination_stand_in.namespace_posts == ["bronze_dictionary"],
        "one register POST per table": len(cut.destination_stand_in.posts) == len(cut.inventory.tables),
    }
    facts = cut.verify()
    scanned = {"manifests": 1, "data_file_bytes": len(parquet_fixture()), "rows": 200, "storage_identity": "lakehouse_reader"}
    checks["V1: verification loads every table and scans one data file per namespace through the reader role"] = facts == {
        "reader_tables": len(cut.inventory.tables),
        "data_reads": {"bronze": scanned, "bronze_dictionary": scanned},
        "mac_recompare": len(cut.inventory.tables),
    }
    checks["V1: the scan skips the deleted manifest entry and reads the live data file by range only"] = (
        not any(location.endswith("00000.parquet") for location in cut.lake.storage.reads)
        and sum(location.endswith("00001.parquet") for location in cut.lake.storage.reads) == 8
    )
    directory = folder / "cutover" / "cut1"
    run_record = bronze_cutover.write_run_record(directory, "cut1", cut.inventory.sha256, first, "20261028T000000Z")
    digest = bronze_cutover.write_receipt(directory, "cut1", cut.inventory.sha256, facts)
    receipt = json.loads((directory / bronze_cutover.RECEIPT_NAME).read_text())
    checks["the receipt records the cutover, digest, root principal and the namespace approval"] = (
        receipt["cutover_id"] == "cut1"
        and receipt["inventory_sha256"] == cut.inventory.sha256
        and receipt["principal"] == "polaris_root"
        and receipt["namespace_creation_approved_for"] == "cut1"
        and receipt["registration_runs"] == {run_record.name: hashlib.sha256(run_record.read_bytes()).hexdigest()}
    )
    second = cut.register()
    checks["R5: a rerun with the same inventory gives unchanged for every table and no POST"] = second == {
        "registered": [],
        "unchanged": [pin.identifier for pin in cut.inventory.tables],
    } and len(cut.destination_stand_in.posts) == len(cut.inventory.tables)
    checks["the same verification writes the same receipt again"] = (
        bronze_cutover.write_receipt(directory, "cut1", cut.inventory.sha256, cut.verify()) == digest
    )
    return checks


@offline
def preflight_refusals(folder: Path) -> dict[str, bool]:
    """R1: the AWS catalog must exist with exactly the lakehouse base location, allowed locations and no credential vending."""
    checks: dict[str, bool] = {}
    for label, change in (
        ("an absent catalog", lambda cut: setattr(cut.destination_stand_in, "management", None)),
        ("another base location", lambda cut: cut.destination_stand_in.management["properties"].update({"default-base-location": "s3://other/"})),
        ("extra allowed locations", lambda cut: cut.destination_stand_in.management["storageConfigInfo"]["allowedLocations"].append("s3://other/")),
        ("credential vending", lambda cut: cut.destination_stand_in.management["storageConfigInfo"].update({"stsUnavailable": False})),
    ):
        cut = Cutover()
        change(cut)
        code, _ = code_of(cut.preflight)
        checks[f"R1: {label} is refused before any namespace or table call"] = code == "catalog_location" and not cut.destination_stand_in.namespace_posts
    return checks


@offline
def register_failures(folder: Path) -> dict[str, bool]:
    """R2, R3, R4, R5, R6, R7: conflicts stop without overwrite, a lost reply is never re-posted, partial runs resume as unchanged."""
    checks: dict[str, bool] = {}
    cut = Cutover()
    victim = cut.inventory.tables[2]
    other = copy.deepcopy(cut.lake.source.tables[cut.inventory.tables[0].identifier])
    cut.destination_stand_in.tables[victim.identifier] = other
    code, message = code_of(cut.register)
    checks["R2: a different table under the same name stops with its name and JSON path and is not overwritten"] = (
        code == "registration_conflict" and f"{victim.namespace}.{victim.name}" in message and "JSON path" in message
    ) and cut.destination_stand_in.tables[victim.identifier] == other
    checks["R2: the run stops at the first error"] = len(cut.destination_stand_in.posts) == 2
    lost = Cutover()
    lost.destination_stand_in.lose_reply = True
    code, message = code_of(lost.register)
    first_pin = lost.inventory.tables[0]
    checks["R3: a lost register reply stops with the table named after one POST"] = code == "transport" and first_pin.name in message
    checks["R3: no second POST for the lost reply"] = len(lost.destination_stand_in.posts) == 1
    rerun = lost.register()
    checks["R3: the rerun adopts the identical table as unchanged and registers the rest"] = (
        rerun["unchanged"] == [first_pin.identifier] and len(rerun["registered"]) == len(lost.inventory.tables) - 1
    )
    checks["R3: one POST per table across both runs"] = len(lost.destination_stand_in.posts) == len(lost.inventory.tables)
    race = Cutover()
    race.destination_stand_in.concurrent = copy.deepcopy(race.lake.source.tables[race.inventory.tables[0].identifier])
    checks["R4: a 409 with an identical table is accepted"] = race.register()["registered"][0] == race.inventory.tables[0].identifier
    clash = Cutover()
    clash.destination_stand_in.concurrent = copy.deepcopy(clash.lake.source.tables[clash.inventory.tables[1].identifier])
    checks["R4: a 409 with a different table is a conflict"] = code_of(clash.register)[0] == "registration_conflict"
    partial = Cutover()
    partial.destination_stand_in.fail_table = partial.inventory.tables[3].identifier
    code, message = code_of(partial.register)
    checks["R5: a failed table stops the run with its name"] = code == "catalog_http" and partial.inventory.tables[3].name in message
    partial.destination_stand_in.fail_table = None
    resumed = partial.register()
    checks["R5: the rerun gives unchanged for done tables and registers the rest"] = (
        len(resumed["unchanged"]) == 3 and len(resumed["registered"]) == len(partial.inventory.tables) - 3
    )
    slow = Cutover()
    slow.lake.objects.hook = lambda location, version: setattr(slow.clock, "now", slow.clock.now + 4)
    code, message = code_of(lambda: slow.register(per_table=10))
    checks["R6: a per-table deadline fails visibly with the table named"] = code == "deadline" and slow.inventory.tables[0].name in message
    moved = Cutover()
    pin = moved.inventory.tables[1]
    document = json.loads(moved.lake.objects.versions[pin.metadata_location]["version_1"])
    document["last-updated-ms"] = 9
    moved.lake.objects.put(pin.metadata_location, document, "version_2")
    code, message = code_of(moved.register)
    checks["R7: a metadata object that changed after capture stops with its name"] = code == "metadata_identity" and pin.name in message
    return checks


@offline
def verify_failures(folder: Path) -> dict[str, bool]:
    """V1, V2, V3: a missing data file, an absent reader identity or a changed Mac table stops verification, so no receipt is written."""
    checks: dict[str, bool] = {}
    for label, change, expected in (
        ("V1: an absent manifest list", lambda cut: cut.lake.storage.objects.pop(cut.inventory.tables[0].location + "/metadata/snap.avro"), "data_read"),
        (
            "V1: a manifest list that is not Avro",
            lambda cut: cut.lake.storage.objects.update({cut.inventory.tables[0].location + "/metadata/snap.avro": b'{"synthetic": true}'}),
            "data_read",
        ),
        (
            "V1: a truncated manifest",
            lambda cut: cut.lake.storage.objects.update(
                {cut.inventory.tables[0].location + "/metadata/m0.avro": cut.lake.storage.objects[cut.inventory.tables[0].location + "/metadata/m0.avro"][:-30]}
            ),
            "data_read",
        ),
        ("V1: an absent data file", lambda cut: cut.lake.storage.objects.pop(cut.inventory.tables[0].location + "/data/00001.parquet"), "data_read"),
        (
            "V1: a data file of another size",
            lambda cut: cut.lake.storage.objects.update({cut.inventory.tables[0].location + "/data/00001.parquet": b"PAR1PAR1"}),
            "data_read",
        ),
        (
            "V1: a data file that is not Parquet",
            lambda cut: cut.lake.storage.objects.update({cut.inventory.tables[0].location + "/data/00001.parquet": b"JUNK" + b"synthetic rows" + b"PAR1"}),
            "data_read",
        ),
        ("V1: a storage identity that is refused", lambda cut: setattr(cut.lake.storage, "denied", True), "data_read"),
        (
            "V1: a Parquet file with a broken footer",
            lambda cut: cut.lake.storage.objects.update({cut.inventory.tables[0].location + "/data/00001.parquet": broken_parquet("footer")}),
            "data_read",
        ),
        (
            "V1: a truncated Parquet file that keeps its magic bytes",
            lambda cut: cut.lake.storage.replace_data(cut.inventory.tables[0].location, broken_parquet("truncated")),
            "data_read",
        ),
        (
            "V1: a Parquet file with corrupt compressed pages",
            lambda cut: cut.lake.storage.objects.update({cut.inventory.tables[0].location + "/data/00001.parquet": broken_parquet("pages")}),
            "data_read",
        ),
        (
            "V1: a bronze_dictionary data file that is absent",
            lambda cut: cut.lake.storage.objects.pop(
                next(pin for pin in cut.inventory.tables if pin.namespace == "bronze_dictionary").location + "/data/00001.parquet"
            ),
            "data_read",
        ),
        ("V2: a refused reader identity", lambda cut: setattr(cut.reader_stand_in, "refuse_reads", True), "catalog_http"),
        (
            "V3: a Mac table that changed after registration",
            lambda cut: cut.lake.source.tables[cut.inventory.tables[4].identifier]["metadata"].update({"last-updated-ms": 7}),
            "source_snapshot",
        ),
    ):
        cut = Cutover()
        cut.register()
        change(cut)
        code, _ = code_of(cut.verify)
        checks[f"{label} stops verification with its cause"] = code == expected
    return checks


@offline
def tunnel_route(folder: Path) -> dict[str, bool]:
    """R3, R9, R10: the owned relay carries preflight, registration and verification once each; a closed route never reconnects."""
    checks: dict[str, bool] = {}
    secret = "synthetic_root_" + hashlib.sha256(str(folder).encode()).hexdigest()[:12]
    cut = Cutover()
    stand_in = HttpStandIn(cut.destination_stand_in, "root", secret)
    transport = CatalogTransport(folder / "registry")
    try:
        deadline = time.monotonic() + 60
        endpoint = transport.start_relay(stand_in.port, deadline=deadline)
        route = bronze_cutover.Route(transport, endpoint, deadline=deadline)
        request = bronze_inventory.PolarisHttp("127.0.0.1", route.port, client_id="root", secret=secret)
        destination = reg.PolarisCatalog(request, "hai_lakehouse")
        bronze_cutover.preflight(request, BUCKET, deadline=deadline, execute=route.execute)
        outcomes = bronze_cutover.register_tables(
            destination, cut.lake.objects, cut.inventory, bucket=BUCKET, deadline=deadline, per_table=10, execute=route.execute
        )
        checks["registration through the relay registers every table"] = len(outcomes["registered"]) == len(cut.inventory.tables)
        checks["R10: the root secret never reaches a header the catalog logs"] = all(secret not in header for header in stand_in.headers)
        checks["the relay endpoint is loopback"] = endpoint.startswith("http://127.0.0.1:")
        posts = len(cut.destination_stand_in.posts)
        checks["the route closes with its processes confirmed gone"] = transport.close(deadline=time.monotonic() + 10)
        code, message = code_of(
            lambda: bronze_cutover.register_tables(
                destination, cut.lake.objects, cut.inventory, bucket=BUCKET, deadline=time.monotonic() + 10, per_table=10, execute=route.execute
            )
        )
        checks["R3: a closed route fails visibly and sends nothing"] = code == "transport" and len(cut.destination_stand_in.posts) == posts
    finally:
        transport.close(deadline=time.monotonic() + 10)
        stand_in.close()
    checks["R9: tunnel or relay on the Mac Polaris port is refused"] = (
        code_of(lambda: bronze_cutover.check_ports(TransportSpec("i-0123456789abcdef0", "p", "r", bronze_cutover.MAC_POLARIS_PORT, 18182)))[0] == "network"
    )
    settings = {"aws_profile": "synthetic", "aws_region": "us-east-1", "data_bucket_name": BUCKET, "expected_account_id": "synthetic_account"}
    spec = TransportSpec("i-0123456789abcdef0", "synthetic", "us-east-1", 18181, 18182)
    tags = [{"Key": "Project", "Value": "synthetic_project"}]
    for label, instance, expected in (
        ("a running private host of the project passes", {"State": {"Name": "running"}, "NetworkInterfaces": [{}], "Tags": tags}, ""),
        (
            "a public address is refused",
            {"State": {"Name": "running"}, "PublicIpAddress": "192.0.2.1", "NetworkInterfaces": [{}], "Tags": tags},
            "network",
        ),
        ("a stopped host is refused", {"State": {"Name": "stopped"}, "NetworkInterfaces": [{}], "Tags": tags}, "network"),
        (
            "a public interface association is refused",
            {"State": {"Name": "running"}, "NetworkInterfaces": [{"Association": {"PublicIp": "192.0.2.1"}}], "Tags": tags},
            "network",
        ),
        (
            "a host without the project tag is refused",
            {"State": {"Name": "running"}, "NetworkInterfaces": [{}], "Tags": [{"Key": "Project", "Value": "other"}]},
            "network",
        ),
    ):
        reply = json.dumps({"Reservations": [{"Instances": [instance]}]})

        def runner(command: list[str], reply: str = reply, **_: Any) -> SimpleNamespace:
            return SimpleNamespace(returncode=0, stdout=reply, stderr="")

        code, _ = code_of(lambda: bronze_cutover.network_acceptance(spec, AwsCli(settings, runner=runner), "synthetic_project", deadline=time.monotonic() + 10))
        checks[f"network acceptance: {label}"] = code == expected
    return checks


class RouteProbe:
    """The Mac-side route observations as stand-ins: each observation holds as the attended cutover requires, unless changed."""

    def __init__(self) -> None:
        self.answers: dict[str, bool | None] = dict.fromkeys(bronze_cutover.ROUTE_OBSERVATIONS, True)
        self.calls: list[str] = []

    def observe(self, name: str, *, deadline: float) -> tuple[bool | None, bytes]:
        self.calls.append(name)
        return self.answers.get(name), f"sanitized evidence of {name}".encode()


def group_runner(instances: list[dict[str, Any]], groups: int = 1) -> Callable[..., SimpleNamespace]:
    reply = json.dumps({"AutoScalingGroups": [{"Instances": instances} for _ in range(groups)]})

    def runner(command: list[str], **_: Any) -> SimpleNamespace:
        return SimpleNamespace(returncode=0, stdout=reply, stderr="")

    return runner


@offline
def route_binding_and_evidence(folder: Path) -> dict[str, bool]:
    """R9 and route acceptance: the host comes from the approved host group, and register needs a passing native route check of that host."""
    checks: dict[str, bool] = {}
    settings = {"aws_profile": "synthetic", "aws_region": "us-east-1", "data_bucket_name": BUCKET, "expected_account_id": "synthetic_account"}
    service = {"InstanceId": "i-0123456789abcdef0", "LifecycleState": "InService"}
    bound = bronze_cutover.bind_target(AwsCli(settings, runner=group_runner([service])), "synthetic_group", deadline=time.monotonic() + 10)
    checks["the host is the one in-service instance of the approved host group"] = bound == "i-0123456789abcdef0"
    for label, runner in (
        ("an empty host group", group_runner([])),
        ("two in-service hosts", group_runner([service, {**service, "InstanceId": "i-0fedcba9876543210"}])),
        ("a host that is not in service", group_runner([{**service, "LifecycleState": "Pending"}])),
        ("two matching groups", group_runner([service], groups=2)),
    ):
        code, _ = code_of(
            functools.partial(
                lambda bound_runner: bronze_cutover.bind_target(AwsCli(settings, runner=bound_runner), "synthetic_group", deadline=time.monotonic() + 10),
                runner,
            )
        )
        checks[f"binding refuses {label}"] = code == "network"
    cutover = folder / "cutover" / "cut1"
    refusal = code_of_cutover(lambda: bronze_cutover.require_route(cutover, "cut1", bound))
    checks["register is refused before any route check"] = "route-check" in refusal
    probe = RouteProbe()
    clock = registration_e2e.Clock()
    passed = bronze_cutover.route_check(probe, "cut1", bound, cutover, deadline=clock() + 100, clock=clock, stamp="20261028T000000000000Z")
    checks["the route check makes each Mac-side observation once"] = sorted(probe.calls) == sorted(bronze_cutover.ROUTE_OBSERVATIONS)
    checks["the observations are the catalog port only, no public address and Mac containers refused"] = set(bronze_cutover.ROUTE_OBSERVATIONS) == {
        "catalog_port_only",
        "host_without_public_address",
        "mac_containers_refused",
    }
    evidence = cutover / bronze_cutover.ROUTE_CHECKS / "20261028T000000000000Z.json"
    recorded = json.loads(evidence.read_text())
    checks["a passing check is recorded write-once with the cutover ID and the bound host"] = (
        passed["passed"] is True and recorded["instance"] == bound and recorded["cutover_id"] == "cut1" and stat.S_IMODE(evidence.stat().st_mode) == 0o600
    )
    checks["each observation names its result and the hash of its sanitized evidence"] = all(
        item["passed"] is True and item["evidence_sha256"] == hashlib.sha256(f"sanitized evidence of {item['name']}".encode()).hexdigest()
        for item in recorded["observations"]
    )
    checks["the record states the owner decision and that the ECS class-network matrix stays required"] = (
        "2026-10-28" in recorded["scope"] and "ECS class-network matrix" in recorded["scope"]
    )
    checks["register is allowed with the passing check of the same host"] = code_of_cutover(lambda: bronze_cutover.require_route(cutover, "cut1", bound)) == ""
    checks["a check of another host does not count"] = "another" in code_of_cutover(
        lambda: bronze_cutover.require_route(cutover, "cut1", "i-0fedcba9876543210")
    )
    checks["a check of another cutover does not count"] = "cutover" in code_of_cutover(lambda: bronze_cutover.require_route(cutover, "cut2", bound))
    for number, (label, name, answer) in enumerate(
        (
            ("a tunnel that reaches another port", "catalog_port_only", False),
            ("a host with a public address", "host_without_public_address", False),
            ("a Mac container that reaches the tunnel or relay", "mac_containers_refused", False),
            ("a container probe that did not run", "mac_containers_refused", None),
        ),
        1,
    ):
        probe = RouteProbe()
        probe.answers[name] = answer
        failed = bronze_cutover.route_check(probe, "cut1", bound, cutover, deadline=clock() + 100, clock=clock, stamp=f"20261028T00000{number}000000Z")
        refusal = code_of_cutover(lambda: bronze_cutover.require_route(cutover, "cut1", bound))
        checks[f"{label} fails the check, is recorded and register is refused"] = failed["passed"] is False and name in refusal
    tampered = cutover / bronze_cutover.ROUTE_CHECKS / "20261028T000009000000Z.json"
    document = json.loads(evidence.read_text())
    document["observations"] = document["observations"][1:]
    tampered.write_bytes(reg.canonical(document))
    checks["a recorded check that misses an observation is refused"] = "route" in code_of_cutover(lambda: bronze_cutover.require_route(cutover, "cut1", bound))
    checks.update(route_attempts(folder / "attempts", bound))
    return checks


class InterruptedProbe(RouteProbe):
    """Observations whose step deadline passes between the second and third observation, after checking the attempt was persisted first."""

    def __init__(self, clock: registration_e2e.Clock, attempt: Path) -> None:
        super().__init__()
        self.clock, self.attempt = clock, attempt
        self.attempt_first = False

    def observe(self, name: str, *, deadline: float) -> tuple[bool | None, bytes]:
        if not self.calls:
            self.attempt_first = self.attempt.is_file()
        if name == "catalog_port_only":
            self.clock.now = deadline + 1
            self.calls.append(name)
            return None, b"the route did not become ready before its deadline"
        return super().observe(name, deadline=deadline)


def route_attempts(folder: Path, bound: str) -> dict[str, bool]:
    """Route proof: an incomplete newest attempt hides an older pass, and each evidence file must still match its recorded hash."""
    checks: dict[str, bool] = {}
    clock = registration_e2e.Clock()
    checks_folder = folder / bronze_cutover.ROUTE_CHECKS
    bronze_cutover.route_check(RouteProbe(), "cut1", bound, folder, deadline=clock() + 100, clock=clock, stamp="20261028T000000000000Z")
    interrupted = InterruptedProbe(clock, checks_folder / "20261028T000001000000Z.attempt")
    code, _ = code_of(
        lambda: bronze_cutover.route_check(interrupted, "cut1", bound, folder, deadline=clock() + 10, clock=clock, stamp="20261028T000001000000Z")
    )
    checks["the route attempt is persisted before the first observation"] = interrupted.attempt_first
    checks["an attempt that expires midway fails with the deadline cause"] = code == "deadline"
    refusal = code_of_cutover(lambda: bronze_cutover.require_route(folder, "cut1", bound))
    checks["an incomplete newest attempt refuses register even when an older check passed"] = "did not complete" in refusal
    for label, change in (
        ("a deleted evidence file", lambda path: path.unlink()),
        ("a changed evidence file", lambda path: path.write_bytes(b"other evidence")),
    ):
        fresh = folder / label.replace(" ", "_")
        bronze_cutover.route_check(RouteProbe(), "cut1", bound, fresh, deadline=clock() + 100, clock=clock, stamp="20261028T000002000000Z")
        change(fresh / bronze_cutover.ROUTE_CHECKS / "20261028T000002000000Z.mac_containers_refused.evidence")
        refusal = code_of_cutover(functools.partial(bronze_cutover.require_route, fresh, "cut1", bound))
        checks[f"{label} refuses register"] = "evidence" in refusal
    return checks


def describe_runner(instance: dict[str, Any] | None) -> Callable[..., SimpleNamespace]:
    """A stand-in `describe-instances` reply, or a failed call when the instance is None."""

    def runner(command: list[str], **_: Any) -> SimpleNamespace:
        if instance is None:
            return SimpleNamespace(returncode=255, stdout="", stderr="An error occurred (UnauthorizedOperation)")
        return SimpleNamespace(returncode=0, stdout=json.dumps({"Reservations": [{"Instances": [instance]}]}), stderr="")

    return runner


class Answer(BaseHTTPRequestHandler):
    """The catalog behind the relay as a stand-in: every request is refused for want of a token, as Polaris answers without one."""

    def do_GET(self) -> None:
        self.send_response(401)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        """Keep the case output quiet."""


def probe_folder(folder: Path, nonce: str, ports: list[int], results: Mapping[str, Mapping[str, Any] | None], listed: list[str] | None = None) -> Path:
    """A container probe folder as the owner's documented command leaves it: one result per Docker network and the network list last."""
    folder.mkdir(parents=True)
    for network, result in results.items():
        if result is not None:
            document = {"nonce": nonce, "ports": ports, "network": network, "targets": ["172.17.0.1"], "reached": [], **result}
            (folder / f"{network}.json").write_text(json.dumps(document) + "\n")
    (folder / "networks.txt").write_text("".join(f"{name}\n" for name in (listed if listed is not None else list(results))))
    return folder


@offline
def mac_route_observations(folder: Path) -> dict[str, bool]:
    """Route acceptance, Mac side (owner decision Oct 28 2026): the host check, the catalog answer through the relay and the container probe."""
    checks: dict[str, bool] = {}
    settings = {"aws_profile": "synthetic", "aws_region": "us-east-1", "data_bucket_name": BUCKET, "expected_account_id": "synthetic_account"}
    host = {
        "InstanceId": "i-0123456789abcdef0",
        "State": {"Name": "running"},
        "PrivateIpAddress": "10.23.0.11",
        "NetworkInterfaces": [{"PrivateIpAddress": "10.23.0.11"}],
        "Tags": [{"Key": "Project", "Value": "hai"}],
    }

    def host_result(instance: dict[str, Any] | None) -> tuple[bool | None, bytes]:
        aws = AwsCli(settings, runner=describe_runner(instance))
        return cutover_route.observe_host(aws, "i-0123456789abcdef0", "hai", deadline=time.monotonic() + 10)

    passed, evidence = host_result(host)
    checks["a running project host without a public address passes"] = passed is True
    checks["the host evidence holds no address"] = b"10.23" not in evidence and b"i-0123456789abcdef0" in evidence
    for label, changed in (
        ("a public address on the instance", {**host, "PublicIpAddress": "203.0.113.9"}),
        ("a public address on an interface", {**host, "NetworkInterfaces": [{"Association": {"PublicIp": "203.0.113.9"}}]}),
        ("a host without the project tag", {**host, "Tags": []}),
        ("a stopped host", {**host, "State": {"Name": "stopped"}}),
    ):
        result, evidence = host_result(changed)
        checks[f"{label} fails and is recorded without the address"] = result is False and b"203.0.113" not in evidence
    checks["a failed describe call is recorded as not observed"] = host_result(None)[0] is None

    server = ThreadingHTTPServer(("127.0.0.1", 0), Answer)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        status = cutover_route.catalog_status(server.server_address[1], deadline=time.monotonic() + 5)
    finally:
        server.shutdown()
        server.server_close()
    checks["the catalog answers through the relay"] = status == 401
    with socket.socket() as unused:
        unused.bind(("127.0.0.1", 0))
        closed = unused.getsockname()[1]
    checks["a relay without the catalog behind it gives no answer"] = cutover_route.catalog_status(closed, deadline=time.monotonic() + 5) == 0
    spec = TransportSpec("i-0123456789abcdef0", "synthetic", "us-east-1", 18181, 18182)
    result, evidence = cutover_route.port_observation(spec, 401, 2)
    checks["the tunnel forwards only the catalog port and the relay only the tunnel"] = (
        result is True and json.loads(evidence)["remote_port"] == 8181 and json.loads(evidence)["relay_target_port"] == 18181
    )
    checks["no catalog answer fails the port observation"] = cutover_route.port_observation(spec, 0, 2)[0] is False
    checks["an extra owned process fails the port observation"] = cutover_route.port_observation(spec, 401, 3)[0] is False

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        open_port = listener.getsockname()[1]
        report = mac_route_probe.report("nonce1", [open_port, closed], ["127.0.0.1"], "bridge")
    reached = report["reached"]
    checks["the container probe reports a port it reached"] = reached == [["127.0.0.1", open_port]] and report["nonce"] == "nonce1"
    checks["the container probe names the network it ran on"] = report.get("network") == "bridge"
    checks["the container probe reports refused ports as not reached"] = isinstance(reached, list) and ["127.0.0.1", closed] not in reached
    checks["outside a container the probe finds no host route to try"] = set(mac_route_probe.host_targets(folder / "absent_route_table")) <= {
        "127.0.0.1",
        mac_route_probe.HOST_NAME,
    }

    ports = [18181, 18182]
    refused_all: dict[str, Mapping[str, Any] | None] = {"bridge": {}, "hai_lakehouse": {}, "hai_offline": {}}
    for label, results, listed, nonce, expected in (
        ("every network refused", refused_all, None, "nonce1", True),
        ("one network that reached the relay", {**refused_all, "hai_lakehouse": {"reached": [["host.docker.internal", 18182]]}}, None, "nonce1", False),
        ("a listed network without a result", {**refused_all, "hai_offline": None}, None, "nonce1", None),
        ("a result of another run", refused_all, None, "nonce2", None),
        ("no network listed", refused_all, [], "nonce1", None),
        ("a network name that is not a plain name", refused_all, ["../bridge"], "nonce1", None),
        ("a result that tried no target", {**refused_all, "hai_offline": {"targets": []}}, None, "nonce1", None),
        ("the bridge result copied as another network's result", {**refused_all, "hai_lakehouse": {"network": "bridge"}}, None, "nonce1", None),
    ):
        probe = probe_folder(folder / "probes" / label.replace(" ", "_"), "nonce1", ports, results, listed)
        result, evidence = cutover_route.container_observation(probe, nonce, ports)
        checks[f"container probe: {label} gives {expected}"] = result is expected and b"172.17" not in evidence
    started = time.monotonic()
    waited = cutover_route.wait_for_probe(folder / "probes" / "never", deadline=time.monotonic() + 0.3, poll=0.05)
    checks["the wait for the container probe ends at its deadline"] = waited is False and time.monotonic() - started < 2
    command = cutover_route.probe_command(Path("data/lakehouse/cutover/cut1/route_checks/s1.probe"), "nonce1", ports, "hai/analytics-dbt:pinned", 4102444800)
    checks["the documented probe command covers every Docker network, never pulls and runs the probe from standard input"] = all(
        part in command
        for part in (
            "docker network ls",
            '--network "$network"',
            "--pull=never",
            "-I -",
            'nonce1 18181 18182 "$network"',
            "hai/analytics-dbt:pinned",
            "networks.txt",
        )
    )
    return checks


# The owner's tool as a shell stand-in: the command text names the tool once per call, so only the tool name is replaced and no Docker
# binary can run. Two networks are listed. A probe container is a background sleep whose process ID `rm -f` stops, as Docker stops a
# named container; each call records the bytes of probe script it received. TRAP makes a call ignore SIGTERM and RM_SECONDS makes the
# removal hang. Every sleep records its process ID so the case stops what cleanup left behind.
STAND_IN_TOOL = """stand_in_tool() {
  case "$1" in
    network) printf '%s\\n' bridge hai_net ;;
    run) TRAP
         while [ "$1" != --network ]; do shift; done; net=$2
         wc -c | tr -d ' ' > "$probe/received.$net"
         /bin/sleep "$RUN_SECONDS" </dev/null 2>/dev/null & echo $! > "$probe/container.pid"; echo $! >> "$probe/sleeps"; wait $!
         printf '{"nonce":"nonce1","ports":[18181,18182],"network":"%s","targets":["127.0.0.1"],"reached":[]}\\n' "$net" ;;
    rm) echo "$*" >> "$probe/stopped"; kill "$(cat "$probe/container.pid")" 2>/dev/null
        /bin/sleep "$RM_SECONDS" </dev/null 2>/dev/null & echo $! >> "$probe/sleeps"; wait $! ;;
  esac
}
"""


def run_owner_probe(probe: Path, seconds: int, *, run: int, remove: int = 0, trap: bool = False) -> tuple[Any, float]:
    """Run the owner's command under /bin/sh with the stand-in tool; returns the result and the seconds it ran past the step deadline."""
    probe.mkdir(parents=True)
    expires = int(time.time()) + seconds
    command = cutover_route.probe_command(probe, "nonce1", [18181, 18182], "synthetic_image", expires)
    tool = STAND_IN_TOOL.replace("TRAP", "trap '' TERM" if trap else ":").replace("$RUN_SECONDS", str(run)).replace("$RM_SECONDS", str(remove))
    script = tool + command.replace("docker ", "stand_in_tool ")
    try:
        result = run_command("/bin/sh", ["-c", script], cwd=ROOT, env={"PATH": "/usr/bin:/bin"}, timeout=20)
    finally:
        with contextlib.suppress(OSError):
            for pid in (probe / "sleeps").read_text().split():
                with contextlib.suppress(OSError):
                    os.kill(int(pid), signal.SIGKILL)
    return result, time.time() - expires


@offline
def owner_probe_deadline(folder: Path) -> dict[str, bool]:
    """Route acceptance, owner command: it ends at the step's deadline and stops its own container then; in time it writes the network list."""
    checks: dict[str, bool] = {}
    late = folder / "late"
    result, overrun = run_owner_probe(late, 1, run=30)
    stopped = (late / "stopped").read_text() if (late / "stopped").exists() else ""
    checks["a probe that outlasts the deadline exits nonzero by the deadline"] = result.returncode != 0 and overrun < 1.5
    checks["a probe that outlasts the deadline stops its own container"] = "-f hai_route_probe_nonce1_bridge" in stopped
    checks["a probe that outlasts the deadline writes no network list"] = not (late / cutover_route.NETWORK_LIST).exists()
    ontime = folder / "on_time"
    result, _ = run_owner_probe(ontime, 30, run=0)
    script = (ROOT / "scripts/lakehouse/mac_route_probe.py").stat().st_size
    checks["a probe within the deadline writes the network list and exits 0"] = result.returncode == 0 and (ontime / cutover_route.NETWORK_LIST).is_file()
    checks["under /bin/sh every network receives the whole probe script"] = all(
        (ontime / f"received.{name}").is_file() and (ontime / f"received.{name}").read_text().strip() == str(script) for name in ("bridge", "hai_net")
    )
    checks["the results of a probe within the deadline are accepted"] = cutover_route.container_observation(ontime, "nonce1", [18181, 18182])[0] is True
    return checks


@offline
def owner_probe_timeout_names(folder: Path) -> dict[str, bool]:
    """Route acceptance, live wait: when the owner's command runs out of time, the recorded evidence still names every container its cleanup
    could not confirm stopped, whether the cleanup file is there at the deadline or arrives within the cleanup ceiling after it."""
    checks: dict[str, bool] = {}
    for label, delay in (("already written at the deadline", 0.0), ("written just after the deadline", 1.0)):
        probe = folder / label.replace(" ", "_")
        probe.mkdir(parents=True)
        live: Any = object.__new__(cutover_route.LiveRouteProbe)
        live.probe, live.nonce, live.image, live.write = probe, "nonce1", "synthetic_image", lambda text: None
        live.spec = SimpleNamespace(tunnel_port=18181, relay_port=18182)
        live.transport = SimpleNamespace(check=lambda *, deadline: None)

        def leave(target: Path = probe / cutover_route.CLEANUP_UNCONFIRMED, wait: float = delay) -> None:
            time.sleep(wait)
            target.write_text("hai_route_probe_nonce1_bridge\n")

        if delay == 0.0:
            leave()
            writer = None
        else:
            writer = threading.Thread(target=functools.partial(leave), daemon=True)
            writer.start()
        observed, record = live.observe_containers(time.monotonic() + 0.3)
        if writer is not None:
            writer.join(5)
        checks[f"a cleanup file {label} is not observed"] = observed is None
        checks[f"a cleanup file {label} names the container in the evidence"] = b"hai_route_probe_nonce1_bridge" in record
    stale = folder / "stale_report_from_another_run"
    stale.mkdir(parents=True)
    (stale / cutover_route.CLEANUP_UNCONFIRMED).write_text("hai_route_probe_oldnonce_bridge\n")
    other_run: Any = object.__new__(cutover_route.LiveRouteProbe)
    other_run.probe, other_run.nonce, other_run.image, other_run.write = stale, "nonce1", "synthetic_image", lambda text: None
    other_run.spec = SimpleNamespace(tunnel_port=18181, relay_port=18182)
    other_run.transport = SimpleNamespace(check=lambda *, deadline: None)
    observed, record = other_run.observe_containers(time.monotonic() + 0.1)
    checks["a cleanup report from another run's nonce is not observed"] = observed is None
    checks["a cleanup report from another run's nonce is not taken as this run's evidence"] = b"oldnonce" not in record
    return checks


@offline
def owner_probe_cleanup(folder: Path) -> dict[str, bool]:
    """Route acceptance, owner command: cleanup after the deadline has a fixed ceiling when removal hangs or the tool ignores SIGTERM."""
    checks: dict[str, bool] = {}
    ceiling = 2 * cutover_route.CLEANUP_SECONDS + 1.5
    hung = folder / "hung_removal"
    result, overrun = run_owner_probe(hung, 1, run=30, remove=30)
    named = "hai_route_probe_nonce1_bridge"
    listed = (hung / cutover_route.CLEANUP_UNCONFIRMED).read_text().split() if (hung / cutover_route.CLEANUP_UNCONFIRMED).exists() else []
    checks["a hanging removal still ends the command within the deadline plus the cleanup ceiling"] = result.returncode != 0 and overrun < ceiling
    checks["a container whose removal hangs is named in the output and the probe folder"] = named in result.stderr and listed == [named]
    observed, record = cutover_route.container_observation(hung, "nonce1", [18181, 18182])
    checks["the route check records the unconfirmed container as not observed"] = observed is None and named in record.decode()
    ignoring = folder / "ignores_term"
    result, overrun = run_owner_probe(ignoring, 1, run=30, trap=True)
    checks["a tool that ignores SIGTERM is force-stopped within the deadline plus the cleanup ceiling"] = result.returncode != 0 and overrun < ceiling
    checks["a container removed in time is not reported unconfirmed"] = not (ignoring / cutover_route.CLEANUP_UNCONFIRMED).exists()
    return checks


class StandInServices:
    """The owner CLI's outside services as stand-ins: the captured lake, the AWS catalog and its reader, all in memory."""

    def __init__(self, state_root: Path) -> None:
        self.cut = Cutover()
        self.state_root = state_root
        self.required = REQUIRED
        self.bucket = BUCKET
        self.routes = 0
        self.probe = RouteProbe()
        self.instance = "i-0123456789abcdef0"

    def target(self, *, deadline: float) -> str:
        return self.instance

    @contextlib.contextmanager
    def route_probe(self, instance: str, folder: Path, stamp: str, terminal: bronze_cutover.Terminal, *, deadline: float) -> Iterator[RouteProbe]:
        yield self.probe

    def source(self, *, deadline: float) -> bronze_inventory.Source:
        return bronze_inventory.SourceCatalog(self.cut.lake.source.request, "hai_lakehouse")

    def objects(self) -> reg.MetadataReader:
        return self.cut.lake.objects

    def storage(self, *, deadline: float) -> bronze_scan.Storage:
        return self.cut.lake.storage

    @contextlib.contextmanager
    def remote(self, instance: str, *, deadline: float) -> Iterator[bronze_cutover.Remote]:
        self.routes += 1
        request = self.cut.destination_stand_in.request
        yield bronze_cutover.Remote(
            request, reg.PolarisCatalog(request, "hai_lakehouse"), reg.PolarisCatalog(self.cut.reader_stand_in.request, "hai_lakehouse"), bronze_cutover.direct
        )


class Owner:
    """The owner at a terminal: types back the confirmation the CLI printed, or a chosen wrong line."""

    def __init__(self, env: Env, services: StandInServices) -> None:
        self.env, self.services, self.lines = env, services, [""]
        self.typed: str | None = None
        self.tty = True
        bronze_cutover.TEST_REGISTRY = str(env.registry)

    def read(self, prompt: str) -> str:
        self.lines.append(prompt)
        shown = next(line for line in reversed(self.lines) if line.startswith("Type this line to confirm: "))
        return self.typed if self.typed is not None else shown.removeprefix("Type this line to confirm: ").strip()

    def run(self, *argv: str) -> Any:
        terminal = bronze_cutover.Terminal(self.tty, self.read, self.lines.append)
        return bronze_cutover.run(bronze_cutover.parser().parse_args(list(argv)), self.services, terminal)


@case
def owner_cli_sequence(env: Env) -> dict[str, bool]:
    """Steps 0 to 7 through the owner CLI: dry run, freeze, fence with its deadline, capture, refresh, register, verify, switch and a replay."""
    catalog_script(env)
    env.start()
    catalog_up(env)
    services = StandInServices(env.temp / "state")
    owner = Owner(env, services)
    checks: dict[str, bool] = {}
    owner.run("dry-run")
    checks["step 0: the dry run writes nothing"] = not (services.state_root / "cutover").exists()
    writer = real_run(env, "writer", "acquisition", write_prefixes=["lakehouse/bronze/"])
    running = writer_submit(env, writer, "w1")
    env.wait_phase(running["attempt_id"], ("started",))
    frozen = owner.run("freeze", "--cutover", "cut1")
    checks["step 1: freeze initializes Mac authority and freezes at epoch 2"] = frozen["state"] == "freezing" and frozen["epoch"] == 2
    checks["F1: a Mac write after the freeze is refused"] = refused(expect_error(lambda: writer_submit(env, writer, "w2")), "refused", "catalog authority")
    started = time.monotonic()
    late = code_of_cutover(lambda: bronze_cutover.step_fence(services, bronze_cutover.Terminal(True, owner.read, owner.lines.append), 1.0, poll=0.2))
    checks["F2: the fence deadline passes visibly while a writer runs and authority stays freezing"] = (
        "stays freezing" in late and time.monotonic() - started < 5 and authority(env)["state"] == "freezing"
    )
    checks["F2: the writer was not stopped"] = env.phase(running["attempt_id"]) == "started"
    env.client().call("cancel", {"attempt_id": running["attempt_id"]}, writer)
    env.wait_ended(running["attempt_id"])
    checks["step 2: the fence passes once the writer ended"] = owner.run("fence")["state"] == "source_fenced"
    digest = owner.run("capture")
    folder = services.state_root / "cutover" / "cut1"
    checks["step 3: capture freezes the inventory and its digest"] = (folder / bronze_inventory.DIGEST_NAME).read_text().strip() == digest
    owner.tty = False
    checks["F4: a step without a terminal is refused"] = (
        "terminal" in code_of_cutover(lambda: owner.run("refresh")) and authority(env)["state"] == "source_fenced"
    )
    owner.tty, owner.typed = True, "yes"
    checks["F4: a wrong typed line is refused"] = "typed" in code_of_cutover(lambda: owner.run("refresh")) and authority(env)["state"] == "source_fenced"
    owner.typed = None
    checks["step 4: refresh"] = owner.run("refresh")["state"] == "refreshing"
    inventory = str(folder / bronze_inventory.INVENTORY_NAME)
    checks["register is refused before a route check and opens no route"] = (
        "route-check" in code_of_cutover(lambda: owner.run("register", "--inventory", inventory, "--sha256", digest)) and services.routes == 0
    )
    services.probe.answers["mac_containers_refused"] = False
    checks["a failing route check is refused"] = "mac_containers_refused" in code_of_cutover(lambda: owner.run("route-check", "--probe-image", "synthetic"))
    services.probe.answers["mac_containers_refused"] = True
    checks["step 5a: the route check passes and is recorded"] = owner.run("route-check", "--probe-image", "synthetic")["passed"] is True
    counts = owner.run("register", "--inventory", inventory, "--sha256", digest)
    checks["step 5: every table is registered"] = counts == {"registered": len(REQUIRED), "unchanged": 0}
    again = owner.run("register", "--inventory", inventory, "--sha256", digest)
    checks["R5: a second register run gives unchanged"] = again == {"registered": 0, "unchanged": len(REQUIRED)}
    checks["I8: register refuses another digest"] = "inventory" in code_of_cutover(
        lambda: owner.run("register", "--inventory", inventory, "--sha256", "0" * 64)
    )
    services.instance = "i-0fedcba9876543210"
    checks["verify is refused when the approved host changed after the route check"] = "another" in code_of_cutover(lambda: owner.run("verify"))
    services.instance = "i-0123456789abcdef0"
    receipt = owner.run("verify")
    switched = owner.run("switch")
    checks["step 7: switch records aws_writer at P4 with layers off and the receipt digest in the reason"] = (
        switched["state"] == "aws_writer"
        and switched["phase"] == "P4"
        and receipt in switched["reason"]
        and set(switched["production_layers"].values()) == {"off"}
    )
    checks["S5: repeating the switch returns the stored record"] = owner.run("switch") == switched and authority(env)["epoch"] == switched["epoch"]
    checks["S1: a Mac write after the switch is refused"] = refused(expect_error(lambda: writer_submit(env, writer, "w3")), "refused", "catalog authority")
    checks["each register and verify run opened its own route and the refused digest opened none"] = services.routes == 3
    end_run(env, writer, "writer")
    return checks


@case
def owner_cli_rollback(env: Env) -> dict[str, bool]:
    """B2, F5: rollback from P3 recompares the frozen Mac source and returns to mac_writer; reusing the cutover ID is refused."""
    env.start()
    services = StandInServices(env.temp / "state")
    owner = Owner(env, services)
    owner.run("freeze", "--cutover", "cut1")
    owner.run("fence")
    owner.run("capture")
    owner.run("refresh")
    services.cut.lake.source.tables[services.cut.inventory.tables[0].identifier]["metadata"]["last-updated-ms"] = 5
    changed = code_of_cutover(lambda: owner.run("rollback"))
    checks = {"B2: a changed Mac source stops the rollback in source_verify": "differs" in changed and authority(env)["state"] == "source_verify"}
    services.cut.lake.source.tables[services.cut.inventory.tables[0].identifier]["metadata"]["last-updated-ms"] = 1
    restored = owner.run("rollback")
    checks["B2: the rollback then returns to mac_writer"] = restored["state"] == "mac_writer" and restored["phase"] == "P1"
    checks["B3: the rollback makes no AWS call"] = services.routes == 0
    retried: list[Any] = []
    refusal = code_of_cutover(lambda: retried.append(owner.run("rollback")))
    checks["B4: a repeated rollback replays the stored command and returns the stored record"] = refusal == "" and retried == [restored]
    checks["B4: the repeated rollback leaves authority and epoch unchanged"] = (
        authority(env)["state"] == "mac_writer" and authority(env)["epoch"] == restored["epoch"]
    )
    checks["F5: a freeze that reuses the cutover ID is refused"] = "already used" in code_of_cutover(lambda: owner.run("freeze", "--cutover", "cut1"))
    checks["a new cutover ID freezes again"] = owner.run("freeze", "--cutover", "cut2")["state"] == "freezing"
    return checks


def code_of_cutover(function: Callable[[], Any]) -> str:
    """The text of a CLI refusal, or an empty string when the step passed."""
    try:
        function()
    except (bronze_cutover.CutoverError, reg.RegistrationError, launcher.ClientError) as error:
        return f"{getattr(error, 'code', '')} {error}"
    return ""


def run_offline(name: str, report: Path) -> tuple[dict[str, bool], str]:
    with tempfile.TemporaryDirectory(prefix="bronze_cutover_") as scratch:
        try:
            return OFFLINE[name](Path(scratch)), ""
        except Exception as exc:
            return {}, f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"


ALL_CASES = (
    freeze_gate_and_fence,
    queued_write_fenced,
    mac_gate_arguments,
    gold_reconcile_read,
    rollback_paths,
    inventory_capture,
    inventory_identity,
    s3_metadata_reads,
    polaris_http,
    http_absolute_deadline,
    role_storage_reads,
    route_binding_and_evidence,
    mac_route_observations,
    owner_probe_deadline,
    owner_probe_cleanup,
    owner_probe_timeout_names,
    register_and_verify,
    preflight_refusals,
    register_failures,
    verify_failures,
    tunnel_route,
    owner_cli_sequence,
    owner_cli_rollback,
)


def run_cases(selected: list[str]) -> int:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report = OUT / stamp
    report.mkdir(parents=True)
    if {function.__name__ for function in ALL_CASES} != {*OFFLINE, *CASES}:
        raise SystemExit("ALL_CASES differs from the registered cases")
    results: list[dict[str, Any]] = []
    for name in selected or [*OFFLINE, *CASES]:
        started = time.monotonic()
        error = ""
        checks: dict[str, bool] = {}
        if name in OFFLINE:
            checks, error = run_offline(name, report)
        else:
            env = Env(name, report)
            try:
                checks = CASES[name](env)
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"
            checks["nothing of the case is left over"] = not env.close()
        status = "pass" if checks and all(checks.values()) and not error else "fail"
        results.append({"case": name, "status": status, "checks": checks, "error": error, "seconds": round(time.monotonic() - started, 1)})
        sys.stdout.write(f"{'ok' if status == 'pass' else 'FAIL':<5} {name} ({results[-1]['seconds']} s)\n")
        for check, ok in checks.items():
            if not ok:
                sys.stdout.write(f"      failed: {check}\n")
        if error:
            sys.stdout.write(f"      error: {error.splitlines()[0]}\n")
    head = run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip()
    passed = sum(1 for result in results if result["status"] == "pass")
    summary = {
        "feature": "bronze cutover: launcher authority, inventory, registration and owner CLI with stand-ins",
        "command": ".venv/bin/python -m scripts.lakehouse.run_bronze_cutover_e2e",
        "started_utc": stamp,
        "head": head,
        "cases": results,
        "passed": passed,
        "total": len(results),
    }
    (report / "report.json").write_text(json.dumps(summary, indent=2) + "\n")
    sys.stdout.write(f"{passed}/{len(results)}: {report.relative_to(ROOT)}/report.json\n")
    return 0 if passed == len(results) else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case", action="append", default=[], choices=sorted([*OFFLINE, *CASES]))
    return run_cases(parser.parse_args().case)


if __name__ == "__main__":
    sys.exit(main())
