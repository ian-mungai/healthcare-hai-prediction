"""Exercise declared lineage contracts and local emission without launching containers."""

from __future__ import annotations

import argparse
import ast
import copy
import hashlib
import importlib
import importlib.util
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from importlib.metadata import version
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Any, TypedDict

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_ROOT = ROOT
EVIDENCE: Path | None = None
RUN = "20261017T010000Z-1234abcd"


class Identity(TypedDict):
    dag_id: str
    task_id: str
    dag_run_id: str


IDENTITY: Identity = {"dag_id": "silver_transform_dag", "task_id": "silver_build", "dag_run_id": "manual__2026-10-17T01:00:00+00:00"}
ENVELOPE = {
    "version": 1,
    "inputs": [{"namespace": "file", "name": "fixture/bronze.duckdb"}],
    "outputs": [{"namespace": "file", "name": "fixture/silver.duckdb"}],
}


def read(relative: str) -> dict[str, Any]:
    return json.loads((SCHEMA_ROOT / relative).read_text())


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def diagnostics(errors: list[ValidationError]) -> list[ValidationError]:
    return [detail for error in errors for detail in [error, *diagnostics(error.context)]]


def missing_lineage() -> None:
    document = read("config/orchestration/broker_templates.json")
    schema = Draft202012Validator(read("config/orchestration/broker_templates.schema.json"))
    schema.validate(document)
    document["templates"]["silver_build"]["modes"]["fixture"]["launcher"].pop("lineage", None)
    errors = diagnostics(list(schema.iter_errors(document)))
    check(any(e.validator == "required" and "lineage" in e.message for e in errors), "missing launcher lineage was accepted")


def old_request() -> None:
    schema = Draft202012Validator(read("data_contracts/orchestration/submission_request.schema.json"))
    payload = {"template": "silver_build", "run_id": RUN, "mode": "fixture", "attempt": 1}
    errors = list(schema.iter_errors(payload))
    check(any(e.validator == "required" for e in errors), "v1 request without caller identity was accepted")


def identity_bounds() -> None:
    schema = Draft202012Validator(read("data_contracts/orchestration/submission_request.schema.json"))
    payload = {"request_version": 2, "template": "silver_build", "run_id": RUN, "mode": "fixture", "attempt": 1, **IDENTITY}
    schema.validate(payload)
    for field in IDENTITY:
        missing = {key: value for key, value in payload.items() if key != field}
        check(any(e.validator == "required" and field in e.message for e in schema.iter_errors(missing)), f"missing {field} accepted")
        for value in ("", "x" * 251, "unsafe\nidentity", "unsafe/identity", "unsafe\n"):
            mutated = {**payload, field: value}
            errors = list(schema.iter_errors(mutated))
            check(any(list(e.path) == [field] for e in errors), f"{field} bound accepted")
    check(bool(list(schema.iter_errors({**payload, "request_version": 1}))), "old explicit request version accepted")
    check(bool(list(schema.iter_errors({**payload, "lineage": ENVELOPE}))), "caller-supplied lineage accepted")


def envelope_bounds() -> None:
    document = read("config/orchestration/broker_templates.json")
    schema = Draft202012Validator(read("config/orchestration/broker_templates.schema.json"))
    schema.validate(document)
    mutations = [
        {"version": 2, "inputs": [], "outputs": []},
        {"version": 1, "inputs": [], "outputs": [], "secret": "synthetic"},
        {"version": 1, "inputs": [{"namespace": "http://user:password@host", "name": "table"}], "outputs": []},
        {"version": 1, "inputs": [{"namespace": "s3://bucket?token=value", "name": "table"}], "outputs": []},
        {"version": 1, "inputs": [{"namespace": "file", "name": "bad\nname"}], "outputs": []},
    ]
    for envelope in mutations:
        mutated = copy.deepcopy(document)
        mutated["templates"]["silver_build"]["modes"]["fixture"]["launcher"]["lineage"] = envelope
        errors = diagnostics(list(schema.iter_errors(mutated)))
        check(any("lineage" in list(e.absolute_path) for e in errors), "invalid lineage envelope accepted")


def broker_records() -> None:
    from orchestration import launcher
    from orchestration.interface import SubmissionRefused, SubmissionRequest
    from scripts.lakehouse.run_lock import Registry
    from scripts.orchestration.broker import Broker, BrokerPaths, load_templates

    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        registry = Registry(root / "registry", "test")
        config = launcher.Config(registry, ROOT, "/unused", root / "tokens", root / "runs")
        client = launcher.LauncherClient(config, start=False)
        broker = Broker(BrokerPaths(registry, root), load_templates(), client, runs_root=root / "runs")
        request = SubmissionRequest("pin_bronze", RUN, "fixture", 1, **IDENTITY)
        handle = broker.submit(request)
        record = json.loads(Path(broker.record(handle)["record_path"]).read_text())
        check(all(record[key] == value for key, value in IDENTITY.items()), "caller identity missing from finished record")
        check(record.get("lineage") == {"version": 1, "inputs": [], "outputs": []}, "no-op lineage was not frozen")
        check(record["ledger"][0]["lineage"] == record["lineage"], "intent and terminal lineage disagree")
        conflict = SubmissionRequest("pin_bronze", RUN, "fixture", 1, **(IDENTITY | {"dag_id": "other_dag"}))
        try:
            broker.submit(conflict)
        except SubmissionRefused as error:
            check("identity" in str(error), "retry rejected for unrelated cause")
        else:
            raise AssertionError("changed caller identity accepted for existing submission")
        check(broker.status(handle).exit_code == 0, "identity recovery broke handle lookup")


def snapshot_completion(mutation: str | None = None) -> None:
    from orchestration import launcher
    from orchestration.interface import SubmissionRequest
    from orchestration.lineage import build_events
    from scripts.lakehouse.run_lock import Registry
    from scripts.orchestration.broker import Broker, BrokerPaths, load_templates

    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        registry = Registry(root / "registry", "test")
        client = launcher.LauncherClient(launcher.Config(registry, ROOT, "/unused", root / "tokens", root / "runs"), start=False)
        broker = Broker(BrokerPaths(registry, root), load_templates(), client, runs_root=root / "runs")
        request = SubmissionRequest("silver_build", RUN, "fixture", 1, **IDENTITY)
        row = broker.templates[request.template]["modes"][request.mode]
        (root / "runs" / RUN).mkdir(parents=True)
        (root / "runs" / RUN / "fixture_lakehouse.duckdb").write_bytes(b"synthetic fixture bronze bytes")
        broker.write_intent(request, row, broker.payload_of(request, row))
        intent = broker.lines_of(request)[0]
        output = intent["lineage"]["outputs"][0]
        synthetic = root / output["name"]
        synthetic.parent.mkdir(parents=True)
        synthetic.write_bytes(b"synthetic fixture bytes, no real build")
        digest = hashlib.sha256(synthetic.read_bytes()).hexdigest()
        snapshot: dict[str, Any] = {"version": 1, "invocation": request.as_dict(), "outputs": [{**output, "snapshot": {"kind": "file_sha256", "id": digest}}]}
        if mutation == "undeclared":
            snapshot["outputs"][0]["name"] = "fixture/undeclared.duckdb"
        elif mutation == "malformed":
            snapshot["outputs"][0]["snapshot"]["id"] = "bad_hash"
        elif mutation == "attempt":
            snapshot["invocation"]["attempt"] = 2
        elif mutation == "secret":
            broker._secrets.add(digest)
        broker.finish(request, 0, record={"snapshot_record": snapshot})
        lines = broker.lines_of(request)
        record_path = broker.paths.records / RUN / f"{request.container_name}.json"
        if mutation == "replay":
            from orchestration.interface import SubmissionRefused
            from scripts.orchestration.ledger import LedgerError

            original = record_path.read_bytes()
            changed = copy.deepcopy(snapshot)
            changed["outputs"][0]["snapshot"]["id"] = "b" * 64
            with suppress(SubmissionRefused, LedgerError):
                broker.finish(request, 0, record={"snapshot_record": changed})
            check(record_path.read_bytes() == original and len(broker.lines_of(request)) == len(lines), "repeat finalization replaced immutable snapshot bytes")
            return
        if mutation is not None:
            check(lines[-1]["event"] == "failed_closed", f"snapshot {mutation} was accepted by broker")
            check(not record_path.exists(), f"snapshot {mutation} persisted a completed record")
            return
        record = json.loads(record_path.read_text())
        check(record["snapshot_record"] == lines[-1].get("snapshot_record"), "snapshot record is not bound into the terminal ledger")
        events = build_events(record)
        version_facet = events[-1]["outputs"][0].get("facets", {}).get("version", {})
        check(version_facet.get("datasetVersion") == digest, "synthetic producer hash did not become a dataset version facet")
        check("version" not in events[0].get("outputs", [{}])[0].get("facets", {}) if events[0]["outputs"] else True, "START asserted a produced snapshot")
        if EVIDENCE is not None:
            (EVIDENCE / "snapshot_completion.json").write_text(json.dumps(record, indent=2) + "\n")
            (EVIDENCE / "snapshot_events.json").write_text(json.dumps(events, indent=2) + "\n")


def snapshot_undeclared() -> None:
    snapshot_completion("undeclared")


def snapshot_malformed() -> None:
    snapshot_completion("malformed")


def snapshot_other_attempt() -> None:
    snapshot_completion("attempt")


def snapshot_secret() -> None:
    snapshot_completion("secret")


def snapshot_replay() -> None:
    snapshot_completion("replay")


def snapshot_shapes() -> None:
    from orchestration.lineage import REQUEST_KEYS, LineageError, build_events

    source = record_fixture()
    source["mode"] = "publish_fixture"
    source["ledger"][0]["mode"] = "publish_fixture"
    outputs = [
        {"namespace": "iceberg://polaris:8181/hai_lakehouse", "name": "silver.synthetic_measure"},
        {"namespace": "s3://synthetic-fixture", "name": "synthetic/object.parquet"},
    ]
    source["lineage"]["outputs"] = outputs
    source["ledger"][0]["lineage"] = copy.deepcopy(source["lineage"])
    invocation = {key: source[key] for key in REQUEST_KEYS}
    snapshot: dict[str, Any] = {
        "version": 1,
        "invocation": invocation,
        "outputs": [
            {**outputs[0], "snapshot": {"kind": "iceberg_snapshot", "id": str(2**63 - 1)}},
            {**outputs[1], "snapshot": {"kind": "s3_object_version", "id": "synthetic+version/é="}},
        ],
    }
    source["snapshot_record"] = snapshot
    complete = build_events(source)
    check(complete[-1]["run"]["facets"]["haiSnapshotStatus"]["state"] == "complete", "complete snapshot record was not flagged complete")
    check(
        [item["facets"]["version"]["datasetVersion"] for item in complete[-1]["outputs"]] == [str(2**63 - 1), "synthetic+version/é="],
        "snapshot IDs changed in projection",
    )
    partial = copy.deepcopy(source)
    partial["snapshot_record"]["outputs"].pop()
    event = build_events(partial)[-1]
    check(
        event["run"]["facets"]["haiSnapshotStatus"]["state"] == "partial" and event["run"]["facets"]["haiSnapshotStatus"]["missingOutputs"] == [outputs[1]],
        "partial snapshot was not flagged",
    )
    check("facets" not in event["outputs"][1], "missing output version was fabricated")
    snapshot_schema = read("data_contracts/orchestration/snapshot_record.schema.json")
    check(
        snapshot_schema["properties"]["invocation"] == read("data_contracts/orchestration/submission_request.schema.json"), "snapshot invocation schema drifted"
    )
    check(read("data_contracts/orchestration/ledger_line.schema.json")["$defs"]["snapshot_record"] == snapshot_schema, "ledger snapshot schema drifted")
    mutations: list[tuple[str, dict[str, Any]]] = []
    for field, value in (("version", 2), ("unexpected", "synthetic_snapshot_sentinel")):
        changed = copy.deepcopy(source)
        changed["snapshot_record"][field] = value
        mutations.append(("schema", changed))
    changed = copy.deepcopy(source)
    changed["snapshot_record"]["outputs"][0]["snapshot"] = {"kind": "file_sha256", "id": "a" * 64}
    mutations.append(("schema", changed))
    for index, values in (
        (0, ["0", "-1", "01", str(2**63), "12\n", 12]),
        (1, ["null", "", "bad\nversion", "é" * 513, "/run/secrets/synthetic", "/fixture/.aws/credentials"]),
    ):
        for invalid_id in values:
            changed = copy.deepcopy(source)
            changed["snapshot_record"]["outputs"][index]["snapshot"]["id"] = invalid_id
            mutations.append(("", changed))
    for field, value in (("attempt", 2), ("run_id", "20261017T020000Z-abcdef12"), ("dag_id", "other_dag")):
        changed = copy.deepcopy(source)
        changed["snapshot_record"]["invocation"][field] = value
        mutations.append(("invocation", changed))
    changed = copy.deepcopy(source)
    changed["snapshot_record"]["outputs"][0]["name"] = "silver.undeclared"
    mutations.append(("not declared", changed))
    changed = copy.deepcopy(source)
    duplicate = copy.deepcopy(snapshot["outputs"][0])
    duplicate["snapshot"]["id"] = "1"
    changed["snapshot_record"]["outputs"].append(duplicate)
    mutations.append(("repeats", changed))
    for field, value in (("exit_code", 1), ("cancelled", True), ("mode", "fixture")):
        changed = copy.deepcopy(source)
        changed[field] = value
        if field == "mode":
            changed["ledger"][0][field] = value
            changed["snapshot_record"]["invocation"][field] = value
        mutations.append(("", changed))
    for expected, changed in mutations:
        try:
            build_events(changed)
        except LineageError as error:
            check(expected in str(error), "snapshot rejected for an unrelated reason")
            check("synthetic_snapshot_sentinel" not in str(error), "snapshot error echoed producer values")
        else:
            raise AssertionError("invalid persisted snapshot metadata was emitted")
    if EVIDENCE is not None:
        (EVIDENCE / "snapshot_format_events.json").write_text(json.dumps(complete + [event], indent=2) + "\n")


def snapshot_missing() -> None:
    from orchestration.lineage import build_events

    events = build_events(record_fixture())
    check(all("version" not in output.get("facets", {}) for event in events for output in event["outputs"]), "missing snapshot was fabricated")
    status = events[-1]["run"]["facets"].get("haiSnapshotStatus", {})
    check(status.get("state") == "missing" and status.get("missingOutputs") == ENVELOPE["outputs"], "missing snapshot record was not flagged")


def record_fixture() -> dict[str, Any]:
    return {
        "request_version": 2,
        "submission_id": f"{RUN}:silver_build:fixture:1",
        "template": "silver_build",
        "run_id": RUN,
        "mode": "fixture",
        "attempt": 1,
        **IDENTITY,
        "lineage": copy.deepcopy(ENVELOPE),
        "exit_code": 0,
        "written_at": "2026-10-17T01:02:00+00:00",
        "launcher": {"id": "at-synthetic", "n": 1, "ended_at": "2026-10-17T01:01:00+00:00"},
        "ledger": [
            {
                "event": "intent",
                "at": "2026-10-17T01:00:00+00:00",
                "request_version": 2,
                "template": "silver_build",
                "run_id": RUN,
                "mode": "fixture",
                "attempt": 1,
                **IDENTITY,
                "lineage": copy.deepcopy(ENVELOPE),
            }
        ],
    }


def emission() -> None:
    from orchestration.lineage import build_events

    source = record_fixture()
    shared_silver = [{"namespace": "iceberg://polaris:8181/hai_lakehouse", "name": "silver.synthetic_measure"}]
    source["lineage"]["outputs"] = copy.deepcopy(shared_silver)
    source["ledger"][0]["lineage"] = copy.deepcopy(source["lineage"])
    first = build_events(source)
    check(first == build_events(source), "event bytes or identity changed on resend")
    check([event["eventType"] for event in first] == ["START", "COMPLETE"], "success lifecycle missing")
    check(first[0]["job"] == {"namespace": "hai-prediction-local", "name": "silver_transform_dag.silver_build"}, "job identity wrong")
    check(first[-1]["outputs"] == shared_silver, "declared output missing")
    other_run = "20261017T020000Z-abcdef12"
    other = {
        **source,
        "template": "gold_build",
        "run_id": other_run,
        "submission_id": f"{other_run}:gold_build:fixture:1",
        "dag_id": "gold_transform_dag",
        "task_id": "gold_build",
        "dag_run_id": "manual__gold",
        "written_at": "2026-10-17T02:02:00+00:00",
        "launcher": {"id": "at-synthetic-gold", "ended_at": "2026-10-17T02:01:00+00:00"},
    }
    other["lineage"] = {
        "version": 1,
        "inputs": shared_silver,
        "outputs": [{"namespace": "iceberg://polaris:8181/hai_lakehouse", "name": "gold.synthetic_measure"}],
    }
    other["ledger"] = [
        {
            **other["ledger"][0],
            **{key: other[key] for key in ("template", "run_id", *IDENTITY)},
            "at": "2026-10-17T02:00:00+00:00",
            "lineage": copy.deepcopy(other["lineage"]),
        }
    ]
    second = build_events(other)
    check(second[-1]["inputs"] == first[-1]["outputs"], "shared silver dataset did not connect DAGs")
    check(second[0]["run"]["runId"] != first[0]["run"]["runId"], "two DAGs shared run identity")
    failed = build_events({**source, "exit_code": 1})
    check(failed[-1]["eventType"] == "FAIL", "failed task did not emit FAIL")
    sentinel = "synthetic_secret_789"
    polluted = {**source, "params": {"credential": sentinel}, "environment": {"BROKER_TOKEN": sentinel}, "detail": sentinel}
    check(sentinel not in json.dumps(build_events(polluted)), "excluded sensitive fields leaked")
    if EVIDENCE is not None:
        (EVIDENCE / "emitted_events.json").write_text(json.dumps(first + second + failed, indent=2) + "\n")
        (EVIDENCE / "completion_record.json").write_text(json.dumps(source, indent=2) + "\n")


def record_binding() -> None:
    from orchestration.lineage import LineageError, build_events

    source = record_fixture()
    source["ledger"][0].pop("lineage")
    try:
        build_events(source)
    except LineageError as error:
        check("intent" in str(error), "record rejected for unrelated cause")
    else:
        raise AssertionError("record accepted without lineage bound to its intent")


def outbox() -> None:
    from orchestration.lineage import Outbox, sweep

    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        records = root / "records"
        records.mkdir()
        (records / "record.json").write_text(json.dumps(record_fixture()))
        database = root / "outbox.sqlite"
        first = sweep(records, database)
        second = sweep(records, database)
        check(first["queued"] == 2 and second["queued"] == 0, "repeated sweep duplicated events")
        with Outbox(database) as box:
            before = box.pending(10)
            result = box.deliver("127.0.0.1", 9, timeout=0.05, limit=10)
            check(result["delivered"] == 0 and result["pending"] == 2, "outage lost pending events")
            check(box.pending(10) == before, "outage changed event bytes or IDs")
        with Outbox(database) as box:
            check(box.pending(10) == before, "restart lost pending events")
        if EVIDENCE is not None:
            shutil.copyfile(database, EVIDENCE / "outbox.sqlite")


def bounded_batches() -> None:
    from orchestration.lineage import Outbox, sweep

    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        records = root / "records"
        records.mkdir()
        for index in range(5):
            record = record_fixture()
            record["task_id"] = f"task_{index}"
            record["ledger"][0]["task_id"] = record["task_id"]
            (records / f"{index}.json").write_text(json.dumps(record))
        database = root / "outbox.sqlite"
        results = [sweep(records, database, limit=2) for _ in range(3)]
        check(all(item["scanned"] <= 2 for item in results), "sweep record bound exceeded")
        check(sum(item["queued"] for item in results) == 10, "bounded sweeps starved later records")
        with Outbox(database) as box:
            check(len(box.pending(20)) == 10, "batched records lost events")
        check(sweep(records, database, limit=2)["queued"] == 0, "cursor wrap duplicated events")


def transport() -> None:
    from orchestration.lineage import Outbox, build_events

    received: list[bytes] = []

    class Receiver(BaseHTTPRequestHandler):
        timeout = 1.0

        def log_message(self, _format: str, *args: Any) -> None:
            """Keep the synthetic receiver's request log out of the console."""

        def do_POST(self) -> None:
            """Drop the first reply and acknowledge the subsequent immutable event bytes."""
            received.append(self.rfile.read(int(self.headers["Content-Length"])))
            if len(received) > 1:
                self.send_response(201)
                self.send_header("Content-Length", "0")
                self.end_headers()

    server = HTTPServer(("127.0.0.1", 0), Receiver)
    server.timeout = 1.0

    def receive() -> None:
        for _ in range(3):
            server.handle_request()

    thread = threading.Thread(target=receive, name="lineage_synthetic_receiver")
    thread.start()
    try:
        with tempfile.TemporaryDirectory() as folder, Outbox(Path(folder) / "outbox.sqlite") as box:
            box.enqueue(build_events(record_fixture()))
            first = box.deliver("127.0.0.1", server.server_port, timeout=0.5, deadline_seconds=2)
            second = box.deliver("127.0.0.1", server.server_port, timeout=0.5, deadline_seconds=2)
            third = box.deliver("127.0.0.1", server.server_port, timeout=0.5, deadline_seconds=2)
            check(first == {"delivered": 0, "pending": 2}, "lost reply was acknowledged")
            check(second == {"delivered": 2, "pending": 0}, "retry did not acknowledge both events")
            check(third == {"delivered": 0, "pending": 0}, "acknowledged events were sent again")
            check(len(received) == 3 and received[0] == received[1], "lost reply retry changed event bytes")
    finally:
        thread.join(timeout=4)
        server.server_close()
    check(not thread.is_alive(), "synthetic receiver did not stop")
    if EVIDENCE is not None:
        (EVIDENCE / "transport_events.json").write_text(json.dumps([json.loads(value) for value in received], indent=2) + "\n")


def cli() -> None:
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        records = root / "records"
        records.mkdir()
        (records / "completion.json").write_text(json.dumps(record_fixture()))
        database = root / "outbox.sqlite"
        command = ["-m", "orchestration.lineage", "--records", str(records), "--outbox", str(database)]
        first = run_command(sys.executable, command, cwd=ROOT, timeout=10)
        second = run_command(sys.executable, command, cwd=ROOT, timeout=10)
        check(first.returncode == second.returncode == 0, "host emitter CLI failed")
        check(json.loads(first.stdout) == {"queued": 2, "scanned": 1, "legacy_skipped": 0}, "CLI did not collect its completed record")
        check(json.loads(second.stdout) == {"queued": 0, "scanned": 1, "legacy_skipped": 0}, "CLI retry duplicated persisted events")
        if EVIDENCE is not None:
            (EVIDENCE / "cli.json").write_text(json.dumps({"first": first.stdout, "second": second.stdout, "commands": command}, indent=2) + "\n")
            shutil.copyfile(database, EVIDENCE / "cli_outbox.sqlite")


def legacy_records() -> None:
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        records = root / "records"
        records.mkdir()
        (records / "v2.json").write_text(json.dumps(record_fixture()))
        (records / "legacy.json").write_text(json.dumps({"template": "silver_build", "run_id": RUN, "mode": "fixture", "attempt": 1, "exit_code": 0}))
        result = run_command(
            sys.executable, ["-m", "orchestration.lineage", "--records", str(records), "--outbox", str(root / "outbox.sqlite")], cwd=ROOT, timeout=10
        )
        check(result.returncode == 0, "legacy record prevented v2 collection")
        check(json.loads(result.stdout) == {"queued": 2, "scanned": 2, "legacy_skipped": 1}, "legacy records were emitted or not reported")


def generated_caller() -> None:
    from orchestration.interface import SubmissionRequest

    tree = ast.parse((SCHEMA_ROOT / "scripts/orchestration/run_real_e2e.py").read_text())
    assignment = next(
        node for node in tree.body if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "TEST_DAG" for target in node.targets)
    )
    value = assignment.value
    while isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute):
        value = value.func.value
    dag = ast.parse(ast.literal_eval(value))
    helper = next((node for node in dag.body if isinstance(node, ast.FunctionDef) and node.name == "submission"), None)
    if helper is None:
        raise AssertionError("generated scheduler DAG has no v2 caller migration")
    assignments = [node for node in helper.body if isinstance(node, ast.Assign)]
    check(len(assignments) == 1, "generated caller has ambiguous context binding")
    binding = assignments[0]
    check(
        len(binding.targets) == 1 and isinstance(binding.targets[0], ast.Name) and binding.targets[0].id == "instance",
        "generated context is not bound to the request",
    )
    check(
        isinstance(binding.value, ast.Subscript) and isinstance(binding.value.slice, ast.Constant) and binding.value.slice.value == "ti",
        "generated caller reads the wrong context entry",
    )
    context_reads = [
        node for node in ast.walk(helper) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "get_current_context"
    ]
    check(
        len(context_reads) == 1 and isinstance(binding.value, ast.Subscript) and binding.value.value is context_reads[0],
        "generated caller does not read Airflow context",
    )
    requests = [node for node in ast.walk(helper) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "SubmissionRequest"]
    check(len(requests) == 1 and len(requests[0].args) == 7, "generated caller does not use the v2 request signature")
    identity_args = requests[0].args[4:]
    check(
        all(isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "instance" for node in identity_args),
        "generated caller hard-coded identity",
    )
    check(
        [node.attr for node in identity_args if isinstance(node, ast.Attribute)] == ["dag_id", "task_id", "run_id"], "generated identity field mapping is wrong"
    )
    payload = SubmissionRequest("silver_build", RUN, "fixture", 1, **IDENTITY).as_dict()
    Draft202012Validator(read("data_contracts/orchestration/submission_request.schema.json")).validate(payload)
    calls = [node for node in ast.walk(dag) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "submission"]
    check(len(calls) == 3, "generated caller migration missed a task")


def service_spec() -> None:
    path = SCHEMA_ROOT / "services/lineage/docker-compose.lineage.yaml"
    check(path.is_file(), "lineage Compose proposal is missing")
    model = importlib.import_module("yaml").safe_load(path.read_text())
    check(model["name"] == "hai-lineage", "lineage project is not independent")
    services = model["services"]
    check(set(services) == {"postgres", "api", "web"}, "unexpected lineage services")
    for name, service in services.items():
        check("@sha256:" in service["image"] and len(service["image"].split("@sha256:")[1]) == 64, f"{name} image is not pinned")
        check(service["mem_limit"].startswith("${HAI_LINEAGE_MEM_"), f"{name} memory is not computed")
        expected_networks = {"postgres": ["backend"], "api": ["frontend", "backend"], "web": ["frontend"]}[name]
        check(service["networks"] == expected_networks and service.get("network_mode") is None, f"{name} network escaped its fixed role")
        check(service["healthcheck"]["timeout"] in ("2s", "3s"), f"{name} health wait is unbounded")
        for mount in service.get("volumes", []):
            check(mount.startswith("${HAI_LINEAGE_STATE}/"), f"{name} mounts outside lineage state")
            check(not any(value in mount for value in (".aws", "lakehouse", "/session")), f"{name} mounts foreign authority")
    check(not services["postgres"].get("ports"), "PostgreSQL is published")
    check(services["api"]["ports"] == ["127.0.0.1:${HAI_LINEAGE_API_PORT}:5000"], "API publishes beyond loopback or publishes admin")
    check(services["web"]["ports"] == ["127.0.0.1:${HAI_LINEAGE_WEB_PORT}:3000"], "UI publishes beyond loopback")
    check(model["networks"]["backend"]["internal"] is True, "PostgreSQL backend has external routing")
    check(model["networks"]["frontend"]["internal"] is False, "frontend prevents host ingress")
    check(services["api"]["environment"]["MARQUEZ_CONFIG"] == "/run/hai/config/marquez.yaml", "API config is not privately generated")


def launcher_identity() -> None:
    from orchestration.launcher import Launcher, LauncherError

    def submit(root: Path, params: dict[str, Any]) -> str:
        client: Any = SimpleNamespace(cfg=SimpleNamespace(root=root))
        context: Any = SimpleNamespace(params={"template": "unknown_lineage_fixture", **params})
        try:
            Launcher.m_submit(client, context)
        except LauncherError as exc:
            return exc.code
        except (OSError, ValueError, KeyError) as exc:
            return type(exc).__name__
        return "accepted"

    observed: dict[str, str] = {}
    with tempfile.TemporaryDirectory(prefix="hai_identity_contract_") as folder:
        root = Path(folder)
        observed["missing_schema"] = submit(root, {"caller_identity": IDENTITY})
        path = root / "data_contracts/orchestration/submission_request.schema.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"properties": {"dag_id": {"type": "string"}}}))
        observed["missing_keys"] = submit(root, {"caller_identity": IDENTITY})
        path.write_text(json.dumps(read("data_contracts/orchestration/submission_request.schema.json")))
        observed["valid"] = submit(root, {"caller_identity": IDENTITY})
        path.write_text("{}")
        observed["cached"] = submit(root, {"caller_identity": IDENTITY})
        observed["invalid"] = submit(root, {"caller_identity": {**IDENTITY, "dag_id": "bad/identity"}})
        observed["omitted"] = submit(root, {})
        observed["null"] = submit(root, {"caller_identity": None})
    expected = {
        "missing_schema": "identity_schema",
        "missing_keys": "identity_schema",
        "valid": "refused",
        "cached": "refused",
        "invalid": "args",
        "omitted": "refused",
        "null": "args",
    }
    check(observed == expected, f"launcher identity contract mismatch: {observed}; expected: {expected}")


FIXTURE_BRONZE = b"synthetic fixture bronze bytes"


def input_broker(root: Path) -> Any:
    """A real broker whose attempt always ended cleanly, so ``apply_attempt`` and ``finish`` run unchanged without a launcher."""
    from orchestration import launcher
    from orchestration.interface import SubmissionRequest
    from scripts.lakehouse.run_lock import Registry
    from scripts.orchestration.broker import Broker, BrokerPaths, load_templates
    from scripts.orchestration.ledger import now

    class Ended(Broker):
        def attempt_of(self, request: SubmissionRequest, forward: dict[str, Any]) -> dict[str, Any]:
            return {
                "id": "at-synthetic",
                "template": "fixture_dbt",
                "n": 1,
                "phase": "ended",
                "reason": "exited",
                "exit_code": 0,
                "container_id": None,
                "caller_identity": request.identity(),
                "ended_at": now(),
            }

        def saved_logs(self, request: SubmissionRequest, attempt: dict[str, Any]) -> None:
            return None

    registry = Registry(root / "registry", "test")
    client = launcher.LauncherClient(launcher.Config(registry, ROOT, "/unused", root / "tokens", root / "runs"), start=False)
    document = load_templates()
    document["templates"]["silver_build"]["modes"]["fixture"]["launcher"].pop("dbt_nodes")
    return Ended(BrokerPaths(registry, root), document, client, repo_root=root / "checkout", runs_root=root / "runs")


def input_started(root: Path) -> tuple[Any, Any, Path]:
    """Write the silver build intent in fixture mode, with the declared bronze file planted in the run folder."""
    from orchestration.interface import SubmissionRequest

    broker = input_broker(root)
    request = SubmissionRequest("silver_build", RUN, "fixture", 1, **IDENTITY)
    row = broker.templates[request.template]["modes"][request.mode]
    path = root / "runs" / RUN / PurePosixPath(row["launcher"]["lineage"]["inputs"][0]["name"]).name
    path.parent.mkdir(parents=True)
    path.write_bytes(FIXTURE_BRONZE)
    broker.write_intent(request, row, broker.payload_of(request, row))
    return broker, request, path


def input_pins_frozen() -> None:
    from orchestration.lineage import Outbox, build_events

    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        broker, request, path = input_started(root)
        digest = hashlib.sha256(FIXTURE_BRONZE).hexdigest()
        intent = broker.lines_of(request)[0]
        declared = intent["lineage"]["inputs"]
        check(
            intent.get("input_pins") == [{**declared[0], "snapshot": {"kind": "file_sha256", "id": digest}}],
            "intent did not freeze the input pin before launch",
        )
        path.write_bytes(b"edited by the job after intent")
        broker.apply_attempt(request, {})
        lines = broker.lines_of(request)
        check(lines[-1]["event"] == "finished", f"successful attempt did not finish: {lines[-1].get('reason')}")
        record = json.loads((broker.paths.records / RUN / f"{request.container_name}.json").read_text())
        check(record["input_snapshot_record"]["inputs"] == intent["input_pins"], "record was not built from the pins frozen at intent")
        check(record["input_snapshot_record"] == lines[-1].get("input_snapshot_record"), "input record is not bound into the terminal ledger")
        events = build_events(record)
        check(events == build_events(record), "input facet changed the event bytes on resend")
        for event in events:
            check({key: event["inputs"][0][key] for key in ("namespace", "name")} == declared[0], "input dataset identity changed")
            check(event["inputs"][0]["facets"]["version"]["datasetVersion"] == digest, "input pin facet missing or wrong")
        check(all("facets" not in item for item in events[-1]["outputs"]), "an input record changed an output facet")
        with Outbox(root / "outbox.sqlite") as box:
            check(box.enqueue(events) == len(events) and box.enqueue(events) == 0, "input facet replay was not accepted as the same bytes")
        if EVIDENCE is not None:
            (EVIDENCE / "input_pins_record.json").write_text(json.dumps(record, indent=2) + "\n")
            (EVIDENCE / "input_pins_events.json").write_text(json.dumps(events, indent=2) + "\n")


LOGICAL = f"data/analytics/dbt/runs/{RUN}"


def input_pin_sources() -> None:
    from unittest.mock import patch

    from scripts.orchestration import broker as broker_module

    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        broker = input_broker(root)
        run = root / "runs" / RUN
        (run / "sub").mkdir(parents=True)
        outside = root / "outside"
        outside.mkdir()
        (outside / "input.bin").write_bytes(b"outside")
        (run / "real.bin").write_bytes(b"x")
        (run / "sub" / "input.bin").write_bytes(b"nested")
        (run / "big.bin").write_bytes(b"0123456789")
        (run / "link.bin").symlink_to(run / "real.bin")
        (run / "alias").symlink_to(outside, target_is_directory=True)
        os.mkfifo(run / "fifo.bin")
        check(broker.fixture_digest(RUN, f"{LOGICAL}/real.bin") == hashlib.sha256(b"x").hexdigest(), "regular file was not pinned from its bytes")
        check(broker.fixture_digest(RUN, f"{LOGICAL}/sub/input.bin") == hashlib.sha256(b"nested").hexdigest(), "file in an ordinary directory was not pinned")
        for name, what in (
            (f"{LOGICAL}/link.bin", "a linked file"),
            (f"{LOGICAL}/alias/input.bin", "a linked directory"),
            (f"{LOGICAL}/sub", "a directory"),
            (f"{LOGICAL}/fifo.bin", "a pipe"),
            (f"{LOGICAL}/absent.bin", "an absent file"),
            (f"{LOGICAL}/sub/../real.bin", "a parent segment"),
            (str(run / "real.bin"), "an absolute path"),
            ("data/analytics/dbt/runs/20261017T020000Z-abcdef12/real.bin", "another run's file"),
            ("real.bin", "a name outside the run folder"),
        ):
            check(broker.fixture_digest(RUN, name) is None, f"{what} was pinned")
        (root / "runs" / "20261018T000000Z-00000000").mkdir()
        (root / "runs" / "20261018T000000Z-00000000" / "real.bin").write_bytes(b"x")
        (root / "runs" / "link-run").symlink_to(run, target_is_directory=True)
        check(broker.fixture_digest("link-run", "data/analytics/dbt/runs/link-run/real.bin") is None, "a linked run folder was pinned")
        with patch.object(broker_module, "FIXTURE_PIN_BYTES", 5):
            check(broker.fixture_digest(RUN, f"{LOGICAL}/big.bin") is None, "a file over the bound was pinned")
        request = SimpleNamespace(mode="fixture", run_id=RUN, submission_id=f"{RUN}:silver_build:fixture:1")
        both = [{"namespace": "file", "name": f"{LOGICAL}/real.bin"}, {"namespace": "file", "name": f"{LOGICAL}/big.bin"}]
        pins = broker.pin_inputs(request, {"inputs": both})
        check([pin["name"] for pin in pins] == [item["name"] for item in both], "two readable declared inputs were not both pinned under their logical names")
        check(
            broker.pin_inputs(SimpleNamespace(mode="real", run_id=RUN, submission_id="x"), {"inputs": both}) == [],
            "a real-mode input was pinned from a host file",
        )
        check(broker.pin_inputs(request, {"inputs": []}) == [], "a row without inputs was pinned")


def input_unpinnable_refused() -> None:
    """A new fixture submission whose declared inputs cannot all be pinned is refused before anything is appended or forwarded."""
    from unittest.mock import patch

    from orchestration.interface import SubmissionRefused, SubmissionRequest
    from scripts.orchestration import broker as broker_module

    def attempt(root: Path, inputs: list[dict[str, str]], prepare: Callable[[Path], None] | None = None) -> tuple[Any, Any, str | None]:
        broker = input_broker(root)
        request = SubmissionRequest("silver_build", RUN, "fixture", 1, **IDENTITY)
        row = broker.templates[request.template]["modes"][request.mode]
        row["launcher"]["lineage"]["inputs"] = inputs
        (root / "runs" / RUN).mkdir(parents=True)
        (root / "runs" / RUN / "real.bin").write_bytes(b"x")
        if prepare is not None:
            prepare(root / "runs" / RUN)
        try:
            broker.write_intent(request, row, broker.payload_of(request, row))
        except SubmissionRefused as error:
            return broker, request, str(error)
        return broker, request, None

    def named(name: str) -> list[dict[str, str]]:
        return [{"namespace": "file", "name": f"{LOGICAL}/{name}"}]

    outside = {"namespace": "file", "name": "fixture/bronze.duckdb"}
    other = {"namespace": "s3://synthetic-fixture", "name": f"{LOGICAL}/real.bin"}
    foreign = {"namespace": "file", "name": "data/analytics/dbt/runs/20261017T020000Z-abcdef12/real.bin"}
    cases: list[tuple[str, list[dict[str, str]], Callable[[Path], None] | None, str]] = [
        ("a name outside the run folder", [outside], None, "is not under the run folder"),
        ("another run's name", [foreign], None, "is not under the run folder"),
        ("a non-file namespace", [other], None, "is not a local file"),
        ("a missing source", named("absent.bin"), None, "cannot be read from the run folder"),
        ("a linked source", named("link.bin"), lambda run: (run / "link.bin").symlink_to(run / "real.bin"), "cannot be read from the run folder"),
        ("a source in a linked directory", named("alias/real.bin"), lambda run: (run / "alias").symlink_to(run, target_is_directory=True), "cannot be read"),
        ("a directory source", named("sub"), lambda run: (run / "sub").mkdir(), "cannot be read from the run folder"),
        ("one unmapped input among mapped ones", [*named("real.bin"), outside], None, "is not under the run folder"),
        ("one missing input among mapped ones", [*named("real.bin"), *named("absent.bin")], None, "cannot be read from the run folder"),
    ]
    for name, inputs, prepare, expected in cases:
        with tempfile.TemporaryDirectory() as folder:
            broker, request, reason = attempt(Path(folder), inputs, prepare)
            check(reason is not None, f"{name} was accepted")
            check(expected in str(reason), f"{name} was refused for another cause: {reason}")
            check("bronze.duckdb" not in str(reason) and RUN not in str(reason) and folder not in str(reason), f"{name} refusal named a path")
            check(broker.lines_of(request) == [], f"{name} left a ledger line before the refusal")
    with tempfile.TemporaryDirectory() as folder:
        with patch.object(broker_module, "FIXTURE_PIN_BYTES", 0):
            _, _, reason = attempt(Path(folder), named("real.bin"))
        check(reason is not None and "cannot be read from the run folder" in reason, "an oversized source was accepted")
    with tempfile.TemporaryDirectory() as folder:
        broker, request, reason = attempt(Path(folder), [*named("real.bin")])
        check(reason is None, f"a valid mapped input was refused: {reason}")
        check(broker.lines_of(request)[0]["input_pins"][0]["snapshot"]["id"] == hashlib.sha256(b"x").hexdigest(), "the valid mapped input was not pinned")
    with tempfile.TemporaryDirectory() as folder:
        broker = input_broker(Path(folder))
        request = SubmissionRequest("silver_build", RUN, "fixture", 1, **IDENTITY)
        row = broker.templates[request.template]["modes"][request.mode]
        with patch.object(broker, "pin_inputs", return_value=[]):
            broker.write_intent(request, row, broker.payload_of(request, row))
        check("input_pins" not in broker.lines_of(request)[0], "a historical intent gained pins")
        broker.apply_attempt(request, {})
        check(
            broker.lines_of(request)[-1]["event"] == "finished" and "input_snapshot_record" not in broker.lines_of(request)[-1],
            "a historical intent did not finish without a record",
        )


def input_pin_roots() -> None:
    """The pin follows the run folder the attempt gets, not the checkout: distinct roots, a seeded run file and a different shadow file."""
    from orchestration.interface import SubmissionRequest

    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        broker = input_broker(root)
        request = SubmissionRequest("silver_build", RUN, "fixture", 1, **IDENTITY)
        row = broker.templates["silver_build"]["modes"]["fixture"]
        name = PurePosixPath(row["launcher"]["lineage"]["inputs"][0]["name"]).name
        (root / "runs" / RUN).mkdir(parents=True)
        (root / "runs" / RUN / name).write_bytes(FIXTURE_BRONZE)
        shadow = root / "checkout" / "data/analytics/dbt/runs" / RUN / name
        shadow.parent.mkdir(parents=True)
        shadow.write_bytes(b"a different shadow file under the checkout")
        check(broker.repo_root != broker.runs_root.parent, "the roots are not distinct")
        broker.write_intent(request, row, broker.payload_of(request, row))
        pinned = broker.lines_of(request)[0]["input_pins"]
        check(pinned[0]["snapshot"]["id"] != hashlib.sha256(shadow.read_bytes()).hexdigest(), "the shadow file under the checkout was pinned")
        check(pinned[0]["snapshot"]["id"] == hashlib.sha256(FIXTURE_BRONZE).hexdigest(), "the run file's digest was not pinned")
        check(pinned[0]["name"] == broker.lines_of(request)[0]["lineage"]["inputs"][0]["name"], "the logical dataset name was not preserved")


def set_invocation(field: str, value: Any) -> Callable[[dict[str, Any]], None]:
    def change(record: dict[str, Any]) -> None:
        record["invocation"][field] = value

    return change


def set_identity(value: Any) -> Callable[[dict[str, Any]], None]:
    def change(record: dict[str, Any]) -> None:
        record["inputs"][0]["snapshot"]["id"] = value

    return change


def declared_inputs() -> list[dict[str, str]]:
    inputs: list[dict[str, str]] = record_fixture()["lineage"]["inputs"]
    return inputs


def input_mutations() -> list[tuple[str, str, Callable[[dict[str, Any]], None]]]:
    """Named record mutations with the cause each must cite: the table both input refusal cases share."""
    cases: list[tuple[str, str, Callable[[dict[str, Any]], None]]] = [
        ("undeclared", "not declared", lambda record: record["inputs"][0].update(name="data/undeclared.duckdb")),
        ("other_namespace", "closed schema", lambda record: record["inputs"][0].update(namespace="s3://synthetic-fixture")),
        ("unknown_field", "closed schema", lambda record: record.update(unexpected="synthetic_snapshot_sentinel")),
        ("version", "closed schema", lambda record: record.update(version=2)),
        ("empty", "closed schema", lambda record: record.update(inputs=[])),
        ("repeated", "repeats", lambda record: record["inputs"].append({**record["inputs"][0], "snapshot": {"kind": "file_sha256", "id": "c" * 64}})),
        ("not_frozen", "differs from the pins", lambda record: record["inputs"][0]["snapshot"].update(id="b" * 64)),
        ("kind", "closed schema", lambda record: record["inputs"][0]["snapshot"].update(kind="iceberg_snapshot", id="12")),
    ]
    for field, value in (
        ("attempt", 2),
        ("run_id", "20261017T020000Z-abcdef12"),
        ("dag_id", "other_dag"),
        ("template", "gold_build"),
        ("mode", "publish_fixture"),
    ):
        cases.append((f"invocation_{field}", "another broker invocation", set_invocation(field, value)))
    for index, bad in enumerate(("bad_hash", "A" * 64, "a" * 63, "a" * 65, "a" * 63 + "\n", 12, None)):
        cases.append((f"identity_{index}", "closed schema", set_identity(bad)))
    return cases


def input_refusals_broker() -> None:
    """Each refusal goes through ``finish``, so a bad record fails the submission closed and persists nothing."""
    from orchestration.interface import SubmissionRefused
    from scripts.orchestration.ledger import LedgerError

    with tempfile.TemporaryDirectory() as folder:
        broker, request, _ = input_started(Path(folder))
        pins = broker.lines_of(request)[0]["input_pins"]
        valid = {"version": 1, "invocation": request.as_dict(), "inputs": copy.deepcopy(pins)}
        broker.finish(request, 0, record={"input_snapshot_record": valid})
        check(broker.lines_of(request)[-1]["event"] == "finished", "a valid input record was refused")
    for name, expected, change in input_mutations():
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            broker, request, _ = input_started(root)
            record = {"version": 1, "invocation": request.as_dict(), "inputs": copy.deepcopy(pins)}
            change(record)
            broker.finish(request, 0, record={"input_snapshot_record": record})
            lines = broker.lines_of(request)
            check(lines[-1]["event"] == "failed_closed", f"input record {name} was accepted by the broker")
            check(expected in lines[-1]["reason"], f"input record {name} was refused for another cause: {lines[-1]['reason']}")
            check("synthetic_snapshot_sentinel" not in lines[-1]["reason"], f"input record {name} echoed a record value")
            check(not (broker.paths.records / RUN / f"{request.container_name}.json").exists(), f"input record {name} persisted a completed record")
    for name, expected, exit_code, extra in (
        ("failed_run", "successful uncancelled", 1, {}),
        ("cancelled_run", "successful uncancelled", 0, {"cancelled": True}),
    ):
        with tempfile.TemporaryDirectory() as folder:
            broker, request, _ = input_started(Path(folder))
            record = {"version": 1, "invocation": request.as_dict(), "inputs": copy.deepcopy(pins)}
            broker.finish(request, exit_code, record={"input_snapshot_record": record, **extra})
            lines = broker.lines_of(request)
            check(lines[-1]["event"] == "failed_closed" and expected in lines[-1]["reason"], f"{name} kept an input record")
    with tempfile.TemporaryDirectory() as folder:
        broker, request, _ = input_started(Path(folder))
        broker._secrets.add(pins[0]["snapshot"]["id"])
        record = {"version": 1, "invocation": request.as_dict(), "inputs": copy.deepcopy(pins)}
        broker.finish(request, 0, record={"input_snapshot_record": record})
        lines = broker.lines_of(request)
        check(lines[-1]["event"] == "failed_closed" and "known secret" in lines[-1]["reason"], "an input record holding a known secret was accepted")
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        broker, request, _ = input_started(root)
        broker.apply_attempt(request, {})
        path = broker.paths.records / RUN / f"{request.container_name}.json"
        original = path.read_bytes()
        changed = {"version": 1, "invocation": request.as_dict(), "inputs": copy.deepcopy(pins)}
        changed["inputs"][0]["snapshot"]["id"] = "d" * 64
        accepted = False
        with suppress(SubmissionRefused, LedgerError):
            broker.finish(request, 0, record={"input_snapshot_record": changed})
            accepted = True
        check(not accepted, "repeat finalization was not refused")
        check(path.read_bytes() == original, "repeat finalization replaced an immutable input record")


def input_refusals_emitter() -> None:
    """The emitter refuses the same records on its own, so a record edited after the broker wrote it never reaches Marquez."""
    from orchestration.lineage import REQUEST_KEYS, LineageError, build_events

    source = record_fixture()
    invocation = {key: source[key] for key in REQUEST_KEYS}
    pins = [{**declared_inputs()[0], "snapshot": {"kind": "file_sha256", "id": "a" * 64}}]
    source["ledger"][0]["input_pins"] = copy.deepcopy(pins)
    source["input_snapshot_record"] = {"version": 1, "invocation": invocation, "inputs": copy.deepcopy(pins)}
    check(len(build_events(source)) == 2, "a valid input record was refused by the emitter")
    for name, expected, change in input_mutations():
        changed = copy.deepcopy(source)
        change(changed["input_snapshot_record"])
        try:
            build_events(changed)
        except LineageError as error:
            check(expected in str(error), f"input record {name} was refused for another cause: {error}")
            check("synthetic_snapshot_sentinel" not in str(error), f"input record {name} echoed a record value")
        else:
            raise AssertionError(f"input record {name} was emitted")
    for name, expected, edit in (
        ("no_intent_pins", "differs from the pins", lambda record: record["ledger"][0].pop("input_pins")),
        ("failed_exit", "successful uncancelled", lambda record: record.update(exit_code=1)),
        ("cancelled", "successful uncancelled", lambda record: record.update(cancelled=True)),
    ):
        changed = copy.deepcopy(source)
        edit(changed)
        try:
            build_events(changed)
        except LineageError as error:
            check(expected in str(error), f"{name} was refused for another cause: {error}")
        else:
            raise AssertionError(f"{name} emitted an input record")
    shaped = copy.deepcopy(source)
    shaped["mode"] = shaped["ledger"][0]["mode"] = "publish_fixture"
    iceberg = {"namespace": "iceberg://polaris:8181/hai_lakehouse", "name": "bronze.synthetic_measure"}
    s3 = {"namespace": "s3://synthetic-fixture", "name": "synthetic/object.parquet"}
    shaped["lineage"]["inputs"] = shaped["ledger"][0]["lineage"]["inputs"] = [iceberg, s3]
    identities = [{"kind": "iceberg_snapshot", "id": str(2**63 - 1)}, {"kind": "s3_object_version", "id": "synthetic+version/é="}]
    shaped_pins = [{**iceberg, "snapshot": identities[0]}, {**s3, "snapshot": identities[1]}]
    shaped["ledger"][0]["input_pins"] = copy.deepcopy(shaped_pins)
    shaped["input_snapshot_record"] = {"version": 1, "invocation": {key: shaped[key] for key in REQUEST_KEYS}, "inputs": copy.deepcopy(shaped_pins)}
    partial = copy.deepcopy(shaped)
    partial["input_snapshot_record"]["inputs"].pop()
    partial["ledger"][0]["input_pins"].pop()
    try:
        build_events(partial)
    except LineageError as error:
        check("does not pin every declared input" in str(error), f"a partial input record was refused for another cause: {error}")
    else:
        raise AssertionError("a partial input record was emitted")
    versions = [item["facets"]["version"]["datasetVersion"] for item in build_events(shaped)[-1]["inputs"]]
    check(versions == [str(2**63 - 1), "synthetic+version/é="], "input snapshot IDs changed in projection")
    for index, bad, expected in (
        (0, str(2**63), "Iceberg snapshot ID exceeds"),
        (0, "0", "closed schema"),
        (0, "01", "closed schema"),
        (0, "12\n", "closed schema"),
        (1, "é" * 513, "S3 object version exceeds"),
        (1, "null", "closed schema"),
        (1, "bad\nversion", "closed schema"),
        (1, "/fixture/.aws/credentials", "closed schema"),
    ):
        changed = copy.deepcopy(shaped)
        changed["input_snapshot_record"]["inputs"][index]["snapshot"]["id"] = changed["ledger"][0]["input_pins"][index]["snapshot"]["id"] = bad
        try:
            build_events(changed)
        except LineageError as error:
            check(expected in str(error), f"identity bound {bad[:12]!r} was refused for another cause: {error}")
        else:
            raise AssertionError(f"identity bound {bad[:12]!r} was emitted")


def input_emission() -> None:
    from orchestration.lineage import REQUEST_KEYS, build_events

    source = record_fixture()
    plain = build_events(source)
    check(all(event["inputs"] == ENVELOPE["inputs"] for event in plain), "inputs changed without an input record")
    check(all("facets" not in item for event in plain for item in event["inputs"]), "an input facet was fabricated without an input record")
    pins = [{**declared_inputs()[0], "snapshot": {"kind": "file_sha256", "id": "a" * 64}}]
    source["ledger"][0]["input_pins"] = copy.deepcopy(pins)
    source["input_snapshot_record"] = {"version": 1, "invocation": {key: source[key] for key in REQUEST_KEYS}, "inputs": copy.deepcopy(pins)}
    pinned = build_events(source)
    check([event["eventType"] for event in pinned] == ["START", "COMPLETE"], "input record changed the lifecycle")
    for event in pinned:
        check(
            [(item["namespace"], item["name"]) for item in event["inputs"]] == [(item["namespace"], item["name"]) for item in declared_inputs()],
            "input datasets changed",
        )
        check(event["inputs"][0]["facets"]["version"]["datasetVersion"] == "a" * 64, "input pin facet missing or wrong")
    check(pinned[-1]["outputs"] == ENVELOPE["outputs"] and pinned[0]["outputs"] == [], "an input record changed the output events")
    check(pinned[-1]["run"]["facets"]["haiSnapshotStatus"]["state"] == "missing", "an input record changed the output snapshot status")
    check(pinned == build_events(source), "pinned input event bytes changed on resend")
    check(
        [event["run"]["facets"]["haiIdentity"]["eventId"] for event in pinned] == [event["run"]["facets"]["haiIdentity"]["eventId"] for event in plain],
        "event identity moved",
    )
    failed = copy.deepcopy(source)
    failed.pop("input_snapshot_record")
    failed["exit_code"] = 1
    check(all("facets" not in item for item in build_events(failed)[-1]["inputs"]), "a failed run carries an input pin")
    if EVIDENCE is not None:
        (EVIDENCE / "input_emission_events.json").write_text(json.dumps(pinned, indent=2) + "\n")


def input_ledger_schema() -> None:
    from jsonschema import Draft202012Validator

    ledger = read("data_contracts/orchestration/ledger_line.schema.json")
    standalone = read("data_contracts/orchestration/input_snapshot_record.schema.json")
    check(ledger["$defs"]["input_snapshot_record"] == standalone, "ledger input schema drifted")
    check(standalone["properties"]["invocation"] == read("data_contracts/orchestration/submission_request.schema.json"), "input invocation schema drifted")
    check(
        standalone["properties"]["inputs"]["items"] == read("data_contracts/orchestration/snapshot_record.schema.json")["properties"]["outputs"]["items"],
        "identity shapes drifted",
    )
    validator = Draft202012Validator(ledger)
    with tempfile.TemporaryDirectory() as folder:
        broker, request, _ = input_started(Path(folder))
        broker.apply_attempt(request, {})
        lines = broker.lines_of(request)
    intent, finished = lines[0], lines[-1]
    check(not list(validator.iter_errors(intent)) and not list(validator.iter_errors(finished)), "the ledger refused a valid pinned intent or finished line")
    for name, line in (
        ("record on an intent line", {**intent, "input_snapshot_record": finished["input_snapshot_record"]}),
        ("record with a failed exit", {**finished, "exit_code": 1}),
        ("pins on a finished line", {**finished, "input_pins": intent["input_pins"]}),
        ("malformed pin", {**intent, "input_pins": [{**intent["input_pins"][0], "snapshot": {"kind": "file_sha256", "id": "bad"}}]}),
    ):
        check(bool(list(validator.iter_errors(line))), f"the ledger accepted {name}")
    check(
        not list(validator.iter_errors({key: value for key, value in {**finished, "exit_code": 1}.items() if key != "input_snapshot_record"})),
        "a failed finished line is invalid without the record",
    )


def consumer_input_facets() -> None:
    """The native check of every consuming invocation, on synthetic records and events: another invocation's events must never satisfy it."""
    from orchestration.lineage import JOB_NAMESPACE
    from scripts.orchestration import run_lineage_scheduler_e2e as scheduler

    shared = {"namespace": "file", "name": f"{LOGICAL}/staging.duckdb"}
    bronze = {"namespace": "file", "name": f"{LOGICAL}/fixture_lakehouse.duckdb"}

    def record(task: str, dataset: dict[str, str], digest: str, attempt: int = 1) -> dict[str, Any]:
        pins = [{**dataset, "snapshot": {"kind": "file_sha256", "id": digest}}]
        return {
            "submission_id": f"{RUN}:{task}:fixture:{attempt}",
            "run_id": RUN,
            "attempt": attempt,
            "dag_id": "dag",
            "task_id": task,
            "lineage": {"inputs": [dataset]},
            "ledger": [{"input_pins": pins}],
            "input_snapshot_record": {"inputs": pins},
        }

    def events(task: str, dataset: dict[str, str], digest: str | None, uuid: str, *, attempt: int = 1, namespace: str = JOB_NAMESPACE) -> list[dict[str, Any]]:
        facets = {"facets": {"version": {"datasetVersion": digest}}} if digest else {}
        identity = {"haiIdentity": {"runId": RUN, "attempt": attempt}}
        return [
            {
                "job": {"namespace": namespace, "name": f"dag.{task}"},
                "run": {"runId": uuid, "facets": identity},
                "eventType": kind,
                "inputs": [{**dataset, **facets}],
            }
            for kind in ("START", "COMPLETE")
        ]

    records = [record("silver_build", bronze, "a" * 64), record("gold_build", shared, "b" * 64), record("gold_validate", shared, "c" * 64)]
    uuids = {item["submission_id"]: f"00000000-0000-0000-0000-00000000000{index}" for index, item in enumerate(records)}
    own = [
        events("silver_build", bronze, "a" * 64, uuids[records[0]["submission_id"]]),
        events("gold_build", shared, "b" * 64, uuids[records[1]["submission_id"]]),
    ]
    good = [*own[0], *own[1], *events("gold_validate", shared, "c" * 64, uuids[records[2]["submission_id"]])]
    check(scheduler.verify_consumer_facets(records, good, uuids) == {"consumers": 3, "inputs": 3}, "consistent consumers were refused")
    uuid_of = {task: uuids[item["submission_id"]] for task, item in zip(("silver", "gold", "validate"), records, strict=True)}
    first, second = record("gold_build", shared, "b" * 64, 1), record("gold_build", shared, "b" * 64, 2)
    pair = {first["submission_id"]: uuid_of["gold"], second["submission_id"]: "00000000-0000-0000-0000-0000000000ff"}
    cases: list[tuple[str, list[dict[str, Any]], list[dict[str, Any]], dict[str, str], str]] = [
        ("one consumer lost its facet", records, [*good[:2], *events("gold_build", shared, None, uuid_of["gold"]), *good[4:]], uuids, "gold_build"),
        (
            "a consumer carries another version",
            records,
            [*good[:4], *events("gold_validate", shared, "b" * 64, uuid_of["validate"])],
            uuids,
            "another input version",
        ),
        (
            "a consumer lost its terminal version",
            records,
            [event for event in good if not (event["job"]["name"] == "dag.gold_build" and event["eventType"] == "COMPLETE")],
            uuids,
            "gold_build",
        ),
        (
            "events of a foreign run UUID",
            records,
            [*good[:2], *events("gold_build", shared, "b" * 64, "00000000-0000-0000-0000-0000000000aa"), *good[4:]],
            uuids,
            "gold_build",
        ),
        (
            "events of a foreign job namespace",
            records,
            [*good[:2], *events("gold_build", shared, "b" * 64, uuid_of["gold"], namespace="other"), *good[4:]],
            uuids,
            "gold_build",
        ),
        (
            "events of a wrong attempt",
            records,
            [*good[:2], *events("gold_build", shared, "b" * 64, uuid_of["gold"], attempt=9), *good[4:]],
            uuids,
            "gold_build",
        ),
        ("two same-job invocations, events for one", [first, second], events("gold_build", shared, "b" * 64, uuid_of["gold"]), pair, "gold_build"),
        ("no native run for an invocation", records, good, {}, "no native run"),
        (
            "intent pins differ from the record",
            [{**records[0], "ledger": [{"input_pins": [{**bronze, "snapshot": {"kind": "file_sha256", "id": "d" * 64}}]}]}],
            good,
            uuids,
            "intent pins of dag.silver_build",
        ),
        ("a consumer has no intent pins", [{**records[1], "ledger": [{}]}], good, uuids, "intent pins of dag.gold_build"),
        ("no consumer", [], good, uuids, "no successful fixture invocation"),
    ]
    for name, subject, source, runs, expected in cases:
        try:
            scheduler.verify_consumer_facets(subject, source, runs)
        except scheduler.CheckFailure as error:
            check(expected in str(error), f"{name} was refused for another cause: {error}")
        else:
            raise AssertionError(f"{name} passed the consumer check")
    both = {first["submission_id"]: uuid_of["gold"], second["submission_id"]: "00000000-0000-0000-0000-0000000000ff"}
    both_events = [
        *events("gold_build", shared, "b" * 64, uuid_of["gold"]),
        *events("gold_build", shared, "b" * 64, "00000000-0000-0000-0000-0000000000ff", attempt=2),
    ]
    check(scheduler.verify_consumer_facets([first, second], both_events, both)["consumers"] == 2, "two invocations with their own events were refused")


def copy_escape(text: str) -> str:
    for raw, coded in (("\\", "\\\\"), ("\n", "\\n"), ("\r", "\\r"), ("\t", "\\t")):
        text = text.replace(raw, coded)
    return text


def marquez_time(text: str) -> str:
    """The event time as Marquez stores it: a UTC instant that ends in Z and shows no fraction, milliseconds or microseconds."""
    parsed = datetime.fromisoformat(text).astimezone(UTC)
    micro = parsed.microsecond
    fraction = "" if micro == 0 else f".{micro // 1000:03d}" if micro % 1000 == 0 else f".{micro:06d}"
    return parsed.strftime("%Y-%m-%dT%H:%M:%S") + fraction + "Z"


def synthetic_dump(events: list[dict[str, Any]], extra_row: str | None = None) -> str:
    stored = [{**event, "eventTime": marquez_time(event["eventTime"])} for event in events]
    rows = [f"{row['eventTime']}\t{row['eventType']}\t{row['run']['runId']}\tjob\thai\t{copy_escape(json.dumps(row))}\tproducer" for row in stored]
    return "-- table datasets\nd1\tfile\n-- table lineage_events\n" + "\n".join([*rows, *([extra_row] if extra_row else [])]) + "\n-- table runs\nr1\n"


def dump_event_counts() -> None:
    """The delivered events spell their time with +00:00 and Marquez stores it with Z; a dropped, doubled, extra or altered event must still fail."""
    from orchestration.lineage import Outbox, build_events
    from scripts.orchestration import run_lineage_scheduler_e2e as scheduler

    events = build_events(record_fixture())
    events[0]["run"]["facets"]["note"] = 'quote " backslash \\ end'
    events[1]["eventTime"] = "2026-10-17T01:01:00.500000+00:00"
    other = {**events[1], "eventTime": "2026-10-17T01:03:00+00:00"}
    later = {**events[1], "eventTime": "2026-10-17T01:01:00.500001+00:00"}
    changed = {**events[0], "run": {**events[0]["run"], "facets": {**events[0]["run"]["facets"], "note": "changed"}}}

    def raw_row(event: dict[str, Any]) -> str:
        return f"x\t{copy_escape(json.dumps(event))}"

    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        with Outbox(root / "outbox.sqlite") as box, box.connection:
            box.enqueue(events)
            box.connection.execute("UPDATE events SET acked=1")
        dump = root / "marquez.sql"
        dump.write_text(synthetic_dump(events))
        check(scheduler.require_one_row_per_event(dump, root / "outbox.sqlite")["stored"] == 2, "one row per event was refused")
        keys = scheduler.delivered_rows(root / "outbox.sqlite")
        clean = scheduler.stored_event_rows(synthetic_dump(events))
        check(clean == keys and sum(keys.values()) == 2, "the stored and delivered spellings of one event were not the same row")
        check(
            scheduler.compare_rows(clean, keys) == {"delivered": 2, "stored": 2, "duplicated": 0, "missing": 0, "unexpected": 0},
            "a clean dump was not counted as clean",
        )
        doubled = scheduler.stored_event_rows(synthetic_dump([*events, events[1]]))
        check(scheduler.compare_rows(doubled, keys)["duplicated"] == 1, "a doubled row was not counted")
        check(scheduler.compare_rows(scheduler.stored_event_rows(synthetic_dump(events[:1])), keys)["missing"] == 1, "a missing row was not counted")
        check(scheduler.compare_rows(scheduler.stored_event_rows(synthetic_dump([*events, other])), keys)["unexpected"] == 1, "an extra row was not counted")
        for name, text, expected in (
            ("doubled row", synthetic_dump([*events, events[1]]), "duplicated 1"),
            ("missing row", synthetic_dump(events[:1]), "missing 1"),
            ("extra row", synthetic_dump([*events, other]), "unexpected 1"),
            ("event time one microsecond later", synthetic_dump([events[0], later]), "missing 1, unexpected 1"),
            (
                "event time one nanosecond later",
                synthetic_dump(events[:1], extra_row=raw_row({**events[1], "eventTime": "2026-10-17T01:01:00.500000001Z"})),
                "missing 1, unexpected 1",
            ),
            ("event body altered", synthetic_dump([changed, events[1]]), "missing 1, unexpected 1"),
            ("stored time not UTC", synthetic_dump(events, extra_row=raw_row({**events[0], "eventTime": "2026-10-17T01:00:00+02:00"})), "not a UTC instant"),
            ("stored time not a timestamp", synthetic_dump(events, extra_row=raw_row({**events[0], "eventTime": "soon"})), "not a UTC instant"),
            ("no table", "-- table runs\nr1\n", "no lineage_events table"),
            ("empty table", "-- table lineage_events\n-- table runs\nr1\n", "no raw event row"),
            ("unknown escape", synthetic_dump(events, extra_row="x\t{\\q}"), "unknown COPY escape"),
            ("row without an event", synthetic_dump(events, extra_row="x\ty"), "no OpenLineage event"),
        ):
            dump.write_text(text)
            try:
                scheduler.require_one_row_per_event(dump, root / "outbox.sqlite")
            except scheduler.CheckFailure as error:
                check(expected in str(error), f"{name} was refused for another cause: {error}")
            else:
                raise AssertionError(f"{name} passed the one-row-per-event check")


def lan_label() -> None:
    from scripts.orchestration import run_lineage_stack_e2e as stack

    both = {name: {"ipv4_tried": 1, "ipv6_tried": 2, "all_refused": True} for name in ("api", "web")}
    one = {name: {"ipv4_tried": 2, "ipv6_tried": 0, "all_refused": True} for name in ("api", "web")}
    labels = [stack.host_coverage(both), stack.host_coverage(one)]
    check(labels[0] == "host_addresses_both_families" and labels[1].startswith("host_addresses_one_family"), "host coverage labels are wrong")
    check(not any(label.lower().startswith("complete") for label in labels), "a host-local label still reads as complete")
    reason = stack.VERIFICATION_5["reason"]
    check(stack.VERIFICATION_5["status"] == "pending", "Verification 5 is not listed as pending")
    check(
        all(word in reason for word in ("second device", "same LAN", "API and UI", "IPv4 and IPv6", "this host only")),
        "the pending reason omits what is still unproven",
    )


def hold_bounds() -> None:
    folder = ROOT / "data/e2e/lineage_stack"
    before = sorted(path.name for path in folder.glob("*")) if folder.exists() else []
    for value in ("0", "3601", "-5", "abc", "1.5", ""):
        result = run_command(sys.executable, ["-m", "scripts.orchestration.run_lineage_stack_e2e", "--hold-seconds", value], cwd=ROOT, timeout=30)
        refused = result.returncode == 2 and ("must be between 1 and 3600" in result.stderr or "invalid hold_seconds value" in result.stderr)
        check(refused, f"hold value {value!r} was not refused by the parser")
    check(before == (sorted(path.name for path in folder.glob("*")) if folder.exists() else []), "a refused hold value still started a run")


CASES: dict[str, Callable[[], None]] = {
    "input_pins_frozen": input_pins_frozen,
    "input_pin_sources": input_pin_sources,
    "input_pin_roots": input_pin_roots,
    "input_unpinnable_refused": input_unpinnable_refused,
    "input_refusals_broker": input_refusals_broker,
    "input_refusals_emitter": input_refusals_emitter,
    "input_emission": input_emission,
    "input_ledger_schema": input_ledger_schema,
    "consumer_input_facets": consumer_input_facets,
    "dump_event_counts": dump_event_counts,
    "lan_label": lan_label,
    "hold_bounds": hold_bounds,
    "snapshot_producer": snapshot_completion,
    "snapshot_replay": snapshot_replay,
    "snapshot_secret": snapshot_secret,
    "snapshot_shapes": snapshot_shapes,
    "snapshot_undeclared": snapshot_undeclared,
    "snapshot_malformed": snapshot_malformed,
    "snapshot_other_attempt": snapshot_other_attempt,
    "snapshot_missing": snapshot_missing,
    "generated_caller": generated_caller,
    "service_spec": service_spec,
    "launcher_identity": launcher_identity,
    "missing_lineage": missing_lineage,
    "old_request": old_request,
    "identity_bounds": identity_bounds,
    "envelope_bounds": envelope_bounds,
    "broker_records": broker_records,
    "emission": emission,
    "record_binding": record_binding,
    "outbox": outbox,
    "bounded_batches": bounded_batches,
    "transport": transport,
    "cli": cli,
    "legacy_records": legacy_records,
}


def main() -> int:
    """Run selected light cases and retain results under the ignored E2E folder."""
    global SCHEMA_ROOT, EVIDENCE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=sorted(CASES))
    parser.add_argument("--schema-root", type=Path)
    parser.add_argument("--emitter-source", type=Path)
    parser.add_argument("--broker-source", type=Path)
    parser.add_argument("--launcher-source", type=Path)
    parser.add_argument("--snapshot-source", type=Path)
    args = parser.parse_args()
    SCHEMA_ROOT = args.schema_root or ROOT
    for name, source in (
        ("orchestration.lineage", args.emitter_source),
        ("scripts.orchestration.broker", args.broker_source),
        ("orchestration.launcher", args.launcher_source),
        ("orchestration.snapshots", args.snapshot_source),
    ):
        if source is None:
            continue
        spec = importlib.util.spec_from_file_location(name, source)
        if spec is None or spec.loader is None:
            parser.error("subject source cannot be loaded")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    output = ROOT / "data/e2e/lineage" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    output.mkdir(parents=True)
    EVIDENCE = output
    sources = sorted([*ROOT.glob("orchestration/*.py"), *ROOT.glob("scripts/orchestration/*.py")])
    code_before = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    result: dict[str, Any] = {
        "boundary": "light contracts and local emitter, no launcher, scheduler or Marquez",
        "mode": "light, not commit evidence",
        "environment": {"python": sys.version.split()[0], "platform": sys.platform, "jsonschema": version("jsonschema"), "sqlite": sqlite3.sqlite_version},
        "command": [sys.executable, "-m", "scripts.orchestration.run_lineage_e2e", *sys.argv[1:]],
        "revision": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, timeout=10).stdout.strip(),
        "cases": {},
        "cleanup": "no launcher or container started; the synthetic receiver joined and closed its socket when selected",
        "input_sha256": {
            name: hashlib.sha256((SCHEMA_ROOT / name).read_bytes()).hexdigest()
            for name in (
                "config/orchestration/broker_templates.json",
                "config/orchestration/broker_templates.schema.json",
                "data_contracts/orchestration/submission_request.schema.json",
            )
        },
        "extra_input_sha256": {
            name: hashlib.sha256((SCHEMA_ROOT / name).read_bytes()).hexdigest()
            for name in (
                "services/lineage/docker-compose.lineage.yaml",
                "services/lineage/marquez.yaml.tmpl",
                "scripts/orchestration/run_real_e2e.py",
                "data_contracts/orchestration/snapshot_record.schema.json",
                "data_contracts/orchestration/input_snapshot_record.schema.json",
                "services/lineage/snapshot_status_facet.schema.json",
            )
            if (SCHEMA_ROOT / name).is_file()
        },
        "code_sha256": code_before,
        "overrides": {
            name: {"path": str(source), "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}
            for name, source in (
                ("emitter", args.emitter_source),
                ("broker", args.broker_source),
                ("launcher", args.launcher_source),
                ("snapshots", args.snapshot_source),
            )
            if source is not None
        },
    }
    for name in args.case or CASES:
        started = time.monotonic()
        try:
            CASES[name]()
        except (AssertionError, OSError, ValueError, TypeError, ImportError, sqlite3.Error, ValidationError) as error:
            result["cases"][name] = {"pass": False, "error": str(error), "seconds": time.monotonic() - started}
        else:
            result["cases"][name] = {"pass": True, "seconds": time.monotonic() - started}
    check(code_before == {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}, "code changed during run")
    (output / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    (output / "retention.json").write_text(
        json.dumps(
            {"class": 4, "purpose": "lineage verification summary", "created": datetime.now(UTC).isoformat(), "end_condition": "no record cites this run"}
        )
        + "\n"
    )
    sys.stdout.write(json.dumps({"report": str(output / "report.json"), "cases": result["cases"]}, indent=2) + "\n")
    return 0 if all(item["pass"] for item in result["cases"].values()) else 1


if __name__ == "__main__":
    sys.exit(main())
