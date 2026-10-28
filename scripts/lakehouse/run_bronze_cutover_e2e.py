"""E2E of the bronze cutover: launcher authority and Mac write gate, bronze inventory capture and registration with stand-ins.

Run ``.venv/bin/python -m scripts.lakehouse.run_bronze_cutover_e2e [--case NAME ...]``. A launcher case starts its own launcher on its own test
registry (the stand-ins of ``run_launcher_e2e``). An offline case uses in-memory catalog and object stand-ins (``run_catalog_registration_e2e``)
and a scratch folder. Nothing calls AWS or Docker. The report is written under ``data/e2e/bronze_cutover/<stamp>/``.
Failure IDs (F1 and the rest) refer to the bronze registration failure modes of the cutover design.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import stat
import sys
import tempfile
import threading
import time
import traceback
import urllib.parse
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from orchestration import catalog_authority
from scripts.acquisition.s3_store import AwsCli
from scripts.lakehouse import bronze_inventory
from scripts.lakehouse import register_aws_catalog as reg
from scripts.lakehouse import run_catalog_registration_e2e as registration_e2e
from scripts.lakehouse import run_launcher_e2e as launcher_e2e
from scripts.lakehouse.run_launcher_e2e import Env, catalog_script, catalog_up, expect_error, lifecycle_host, real_run, record_file, refused, run_lock
from scripts.process import run_command

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


class Lake:
    """A frozen Mac catalog whose tables match their versioned metadata objects in S3."""

    def __init__(self, required: frozenset[reg.TableId] = REQUIRED) -> None:
        self.events: list[str] = []
        self.clock = registration_e2e.Clock()
        self.objects = registration_e2e.Objects(self.events)
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


def run_offline(name: str, report: Path) -> tuple[dict[str, bool], str]:
    with tempfile.TemporaryDirectory(prefix="bronze_cutover_") as scratch:
        try:
            return OFFLINE[name](Path(scratch)), ""
        except Exception as exc:
            return {}, f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"


def run_cases(selected: list[str]) -> int:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report = OUT / stamp
    report.mkdir(parents=True)
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
