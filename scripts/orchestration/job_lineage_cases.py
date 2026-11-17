"""Cases of the lineage events that a layer job sends from inside its own phases (``orchestration/job_lineage.py``, hooked into ``layer_run.py``).

Isolation is justified because the receiver must fail, stall and refuse on cue, and the evidence must be wrong on cue. Every case drives the real
``orchestration.job_lineage``, ``orchestration.ecs_lineage`` and ``scripts.lakehouse.layer_run``. The Airflow side's events are built by the real
``ecs_lineage.layer_table_events`` over the in-memory evidence bucket of ``ecs_lineage_cases``, and the receiver is its local stand-in of the OpenMetadata
lineage route, which accepts only the event time form production sends. The job side reads the same records from a run folder, as the job has them.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import json
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import ecs_lineage, job_lineage, snapshots
from scripts.lakehouse import layer_run
from scripts.orchestration import ecs_lineage_cases as base
from scripts.orchestration.ecs_launch_cases import BUCKET, refuses, require

ROOT = Path(__file__).resolve().parents[2]
CREDENTIAL = base.CREDENTIAL
ENDED = "2026-11-04T10:30:00+00:00"
TEXT_FOR = {
    "silver": (base.RUN_OF["silver"], "latest", ""),
    "gold": (base.RUN_OF["gold"], base.RUN_OF["silver"], ""),
    "features": (base.RUN_OF["features"], base.RUN_OF["gold"], ""),
}
PHASE_FILES = {"silver": "pins.json", "gold": "silver_pins.json", "features": "gold_pins.json"}


def request_of(layer: str, input_run: str | None = None, task_id: str = "job") -> dict[str, Any]:
    """The launching step's submission request (a one-job chain launches in its ``job`` task)."""
    request = {**base.invocation(layer), "task_id": task_id}
    if input_run not in (None, "latest"):
        request["input_run_id"] = input_run
    return request


def environ_for(url: str, text: str, kind: str = base.PROJECT_TRANSPORT) -> dict[str, str]:
    return {
        ecs_lineage.TRANSPORT_TYPE: kind,
        ecs_lineage.TRANSPORT_URL: url,
        ecs_lineage.TRANSPORT_KEY: CREDENTIAL,
        job_lineage.INVOCATION_VARIABLE: text,
    }


def write_run_folder(bucket: Any, layer: str, run: str, folder: Path) -> bytes | None:
    """The run folder the job has at its upload: every record of the run that the evidence bucket holds under its prefix (the upload copies the folder to
    those keys, so the folder holds them under the same relative names), except the claim, which is a bucket object only. Returns the per-table inputs
    record that the upload writes beside the dbt copy (None when the evidence has none)."""
    prefix = f"{ecs_lineage.EVIDENCE_PREFIX}/{layer}/{run}/"
    for key, body in bucket.objects.items():
        if key.startswith(prefix) and not key.startswith(prefix + "state/"):
            path = folder / key[len(prefix) :]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
    return bucket.objects.get(ecs_lineage.INPUTS_KEY.format(layer=layer, run=run))


def job_of(layer: str, folder: Path, feature_set: str = "") -> job_lineage.JobRun:
    run, input_run, _ = TEXT_FOR[layer]
    text = ecs_lineage.invocation_text(request_of(layer, input_run))
    return job_lineage.job_run(text, layer, run, feature_set, folder, base.START)


def airflow_lines(layer: str, bucket: Any, declared: dict[str, Any], found: ecs_lineage.LaunchFacts | None = None) -> list[str]:
    """The per-table events the Airflow evidence step sends for the same run: the launch's task is ``job`` and its facts the launch records'."""
    events = ecs_lineage.layer_table_events(
        ecs_lineage.S3Evidence(bucket, BUCKET), declared, ecs_lineage.step_invocation(request_of(layer, TEXT_FOR[layer][1])), found or base.facts(layer)
    )
    return [ecs_lineage.canonical(event) for event in events]


@contextmanager
def folder_of(layer: str) -> Iterator[tuple[Any, Path, bytes | None]]:
    bucket = base.template_bucket()
    with tempfile.TemporaryDirectory() as name:
        folder = Path(name)
        yield bucket, folder, write_run_folder(bucket, layer, TEXT_FOR[layer][0], folder)


class FixedStatus:
    """A receiver that answers every event with one status."""

    def __init__(self, status: int) -> None:
        self.requests = 0
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                owner.requests += 1
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, format: str, *args: Any) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"


@contextmanager
def answering(status: int) -> Iterator[FixedStatus]:
    receiver = FixedStatus(status)
    worker = threading.Thread(target=receiver.server.serve_forever, daemon=True)
    worker.start()
    try:
        yield receiver
    finally:
        receiver.server.shutdown()
        receiver.server.server_close()
        worker.join(10)


def job_events_equal_the_airflow_side_events() -> None:
    """For every layer job the job's per-table events equal, byte for byte, the ones the Airflow evidence step builds from the same records (same run IDs,
    same job names, same datasets and versions), so a second send of either is an upsert. The job reads its own run folder and no bucket."""
    for layer in ("silver", "gold", "features"):
        declared = ecs_lineage.declared_lineage(layer)
        with folder_of(layer) as (bucket, folder, inputs):
            lines = job_lineage.table_lines(job_of(layer, folder), ENDED, inputs)
            wanted = airflow_lines(layer, bucket, declared)
        require(lines == wanted and len(lines) > 2, f"{layer}: {len(lines)} job events differ from {len(wanted)} Airflow events")
        check = base.validator()
        require(all(not check(json.loads(line)) for line in lines), f"{layer}: an event is not a valid OpenLineage event")
    bucket, declared, found = base.model_set_evidence()
    domain = [item["name"].split(".")[-1] for item in declared["inputs"]]
    reads = {item["name"].split(".")[-1]: [{"layer": "features", "table": table} for table in domain] for item in declared["outputs"]}
    base.put(bucket, f"metadata/dbt/features/{base.MODEL_RUN}/table_inputs.json", {"schema": 1, "layer": "features", "run_id": base.MODEL_RUN, "tables": reads})
    with tempfile.TemporaryDirectory() as name:
        inputs = write_run_folder(bucket, "features", base.MODEL_RUN, Path(name))
        request = {**base.invocation("features"), "run_id": base.MODEL_RUN, "task_id": "job", "input_run_id": base.RUN_OF["features"]}
        text = ecs_lineage.invocation_text(request)
        run = job_lineage.job_run(text, "features", base.MODEL_RUN, "m1", Path(name), base.START)
        lines = job_lineage.table_lines(run, ENDED, inputs, declared=declared)
    wanted = [
        ecs_lineage.canonical(event)
        for event in ecs_lineage.layer_table_events(ecs_lineage.S3Evidence(bucket, BUCKET), declared, ecs_lineage.step_invocation(request), found)
    ]
    require(lines == wanted and len(lines) == 2, f"a model set's job events differ: {len(lines)} against {len(wanted)}")


def job_events_resolve_in_the_stand_in_receiver() -> None:
    """The stand-in of the OpenMetadata lineage route, which accepts only the production event time form and finds a dataset by its schema and table, takes
    every event the job sends: no refusal, no unresolved dataset, and its edges are the table pairs of the events."""
    wanted = base.catalogue_of_the_mirror()
    for layer in ("silver", "gold", "features"):
        with folder_of(layer) as (_, folder, inputs), base.receiving(catalogue=wanted) as receiver:
            run = job_of(layer, folder)
            sent = job_lineage.emit(
                "table events", partial_lines(run, inputs), environ_for(receiver.url, ecs_lineage.invocation_text(request_of(layer, TEXT_FOR[layer][1]))), 30.0
            )
            lines = job_lineage.table_lines(run, ENDED, inputs)
        require(sent == len(lines) > 0, f"{layer}: sent {sent} of {len(lines)}")
        statuses = [status for status, _ in receiver.replies]
        require(statuses == [200] * len(lines), f"{layer}: statuses {sorted(set(statuses))}")
        require(all(reply["unresolvedDatasets"] == [] for _, reply in receiver.replies), f"{layer}: an unresolved dataset")
        pairs = base.table_pairs([json.loads(line) for line in lines])
        require(sum(reply["lineageEdgesCreated"] for _, reply in receiver.replies) == len(pairs) > 0, f"{layer}: the edges differ from the table pairs")
        require(all(item["authorization"] == f"Bearer {CREDENTIAL}" for item in receiver.requests), f"{layer}: a request lacks the bearer credential")


def partial_lines(run: job_lineage.JobRun, inputs: bytes | None) -> Callable[[], list[str]]:
    return lambda: job_lineage.table_lines(run, ENDED, inputs)


def a_failed_send_never_fails_the_job() -> None:
    """A receiver that answers 500, a refused connection, a credential the HTTP client refuses, a builder that raises, and a receiver that stalls past the
    deadline each end ``emit`` with a count and no exception, inside the deadline, with one line on standard error that never shows the credential."""
    text = ecs_lineage.invocation_text(request_of("silver"))
    lines = ["{}", "{}"]
    with answering(500) as failing:
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            sent = job_lineage.emit("table events", lambda: lines, environ_for(failing.url, text), 5.0)
        require(sent == 0 and failing.requests == 1, f"a 500 stops the sends at the first event: sent {sent} after {failing.requests} requests")
        require(CREDENTIAL not in err.getvalue() and err.getvalue().count("\n") == 1, f"standard error: {err.getvalue()!r}")
    unused = answering(200)
    with unused as closed:
        gone = closed.url
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        require(job_lineage.emit("table events", lambda: lines, environ_for(gone, text), 5.0) == 0, "a refused connection sends nothing")
    for label, environ in (("a credential with a line break", {**environ_for(gone, text), ecs_lineage.TRANSPORT_KEY: CREDENTIAL + "\n"}),):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            require(job_lineage.emit("table events", lambda: lines, environ, 5.0) == 0, label)
        require(CREDENTIAL not in err.getvalue(), f"{label} leaks the credential: {err.getvalue()!r}")
    for error in (ecs_lineage.LineageRefused("a rule"), ecs_lineage.EvidenceUnavailable("a read"), RuntimeError(CREDENTIAL), KeyError(CREDENTIAL)):
        err = io.StringIO()

        def broken(error: Exception = error) -> list[str]:
            raise error

        with contextlib.redirect_stderr(err):
            require(job_lineage.emit("table events", broken, environ_for(gone, text), 5.0) == 0, f"{type(error).__name__} from the builder")
        require(CREDENTIAL not in err.getvalue() and err.getvalue().count("\n") == 1, f"{type(error).__name__}: standard error {err.getvalue()!r}")
    with base.receiving(delay=30.0) as slow:
        began = time.monotonic()
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            sent = job_lineage.emit("table events", lambda: lines, environ_for(slow.url, text), 1.5)
        spent = time.monotonic() - began
    require(sent == 0 and spent < 4.0, f"a stalled receiver is left at the deadline: sent {sent} after {spent:.2f} s")


def nothing_is_read_or_sent_without_the_settings() -> None:
    """Without the HTTP settings or the invocation, the builder never runs and no request is made; a transport type that is not an HTTP one sends nothing."""
    text = ecs_lineage.invocation_text(request_of("silver"))
    calls: list[int] = []

    def build() -> list[str]:
        calls.append(1)
        return ["{}"]

    with base.receiving() as receiver:
        good = environ_for(receiver.url, text)
        for label, environ in (
            ("no settings", {}),
            ("a console transport", {**good, ecs_lineage.TRANSPORT_TYPE: "console"}),
            ("no address", {key: value for key, value in good.items() if key != ecs_lineage.TRANSPORT_URL}),
            ("no key", {key: value for key, value in good.items() if key != ecs_lineage.TRANSPORT_KEY}),
            ("no invocation", {key: value for key, value in good.items() if key != job_lineage.INVOCATION_VARIABLE}),
        ):
            require(job_lineage.emit("table events", build, environ, 5.0) == 0, label)
        require(calls == [] and receiver.requests == [], f"the builder ran or a request was made: {calls} {len(receiver.requests)}")
        require(job_lineage.emit("table events", build, good, 5.0) == 1 and calls == [1], "the settings together send")


def the_invocation_variable_is_closed() -> None:
    """The variable holds exactly the launching step's six identity fields. Anything else is refused by name, so a job never sends an event under an identity
    the launch did not give it."""
    good = json.loads(ecs_lineage.invocation_text(request_of("gold", base.RUN_OF["silver"])))
    require(set(good) == {"template", "dag_id", "task_id", "dag_run_id", "attempt", "input_run"} and good["task_id"] == ecs_lineage.JOB_TASK, f"fields {good}")
    run, _, _ = TEXT_FOR["gold"]

    def parse(text: str) -> None:
        job_lineage.job_run(text, "gold", run, "", Path("/nonexistent"), base.START)

    parse(json.dumps(good))
    broken = {
        "not JSON": "{",
        "a list": "[]",
        "a missing field": json.dumps({key: value for key, value in good.items() if key != "attempt"}),
        "an extra field": json.dumps({**good, "mode": "real"}),
        "an attempt of 0": json.dumps({**good, "attempt": 0}),
        "an attempt of 100": json.dumps({**good, "attempt": 100}),
        "a text attempt": json.dumps({**good, "attempt": "1"}),
        "a boolean attempt": json.dumps({**good, "attempt": True}),
        "a DAG run ID with a space": json.dumps({**good, "dag_run_id": "manual 1"}),
        "a task of another name": json.dumps({**good, "task_id": "gold_build"}),
        "a template with capitals": json.dumps({**good, "template": "Gold_Build"}),
        "an input run that is no run": json.dumps({**good, "input_run": "yesterday"}),
        "a DAG ID with a slash": json.dumps({**good, "dag_id": "a/b"}),
        "a long DAG ID": json.dumps({**good, "dag_id": "d" * 251}),
        "duplicate keys": '{"attempt": 1, "attempt": 2}',
    }
    for label, text in broken.items():
        refuses(ecs_lineage.LineageRefused, partial(parse, text))
        require(True, label)
    refuses(ecs_lineage.LineageRefused, lambda: job_lineage.job_run(json.dumps(good), "gold", "not-a-run", "", Path("/x"), base.START))
    refuses(ecs_lineage.LineageRefused, lambda: job_lineage.job_run(json.dumps(good), "bronze", run, "", Path("/x"), base.START))
    refuses(ecs_lineage.LineageRefused, lambda: job_lineage.job_run(json.dumps(good), "gold", run, "m9", Path("/x"), base.START))


def start_and_failure_events_name_no_dataset() -> None:
    """The job's START and FAIL are the task's own pair under the Airflow task's run ID, with the run facet and no dataset (OpenMetadata 2.0.7 writes
    nothing for an event without both sides, so they only say what happened). The FAIL names the phase that failed and is never a COMPLETE."""
    check = base.validator()
    with tempfile.TemporaryDirectory() as name:
        run = job_of("gold", Path(name))
        started = [json.loads(line) for line in job_lineage.start_lines(run)]
        failed = [json.loads(line) for line in job_lineage.failure_lines(run, "upload", ENDED)]
    require([event["eventType"] for event in started] == ["START"] and [event["eventType"] for event in failed] == ["FAIL"], "one START and one FAIL")
    task = ecs_lineage.step_invocation(request_of("gold", base.RUN_OF["silver"]))
    for event in (*started, *failed):
        require(not check(event), f"{event['eventType']}: {check(event)[:3]}")
        require(event["inputs"] == [] and event["outputs"] == [], f"{event['eventType']} names a dataset")
        require(event["run"]["runId"] == ecs_lineage.run_uuid(task), "the run ID is the Airflow task's")
        require(event["job"]["name"] == "gold_transform_dag.job", f"job name {event['job']['name']}")
    require(started[0]["eventTime"] == "2026-11-04T10:00:00Z" and failed[0]["eventTime"] == "2026-11-04T10:30:00Z", "event times in the Z form")
    require("upload" in failed[0]["run"]["facets"]["haiLaunch"]["refusal"], "the FAIL names the phase")


def record_checks_without_jsonschema_equal_the_schema_checks() -> None:
    """The record checks the job runs (no closed-schema step, because the spark image holds no jsonschema) refuse what the schema checks refuse among the
    defects a record built from the job's own files can have: an Iceberg snapshot ID that is not a plain 64-bit number, an output or an input the job did not
    declare, a repeated dataset, a missing pin and a snapshot of another kind. On good evidence both give the same lineage."""
    declared = ecs_lineage.declared_lineage("silver")
    inv = ecs_lineage.step_invocation(request_of("silver"))
    schema_checks, job_checks = ecs_lineage.SCHEMA_CHECKS, job_lineage.JOB_CHECKS
    outcomes = {}
    for label, change in {
        "good": lambda bucket: None,
        "a non-numeric snapshot ID": lambda bucket: edit_output(
            bucket, lambda record: record["snapshot_set"].update({next(iter(record["snapshot_set"])): "abc"})
        ),
        "a snapshot ID with a leading zero": lambda bucket: edit_output(
            bucket, lambda record: record["snapshot_set"].update({next(iter(record["snapshot_set"])): "0123"})
        ),
        "a snapshot ID over 64 bits": lambda bucket: edit_output(
            bucket, lambda record: record["snapshot_set"].update({next(iter(record["snapshot_set"])): 2**63})
        ),
        "an undeclared output table": lambda bucket: edit_output(bucket, lambda record: record["snapshot_set"].update({"not_a_silver_table": 5})),
        "a missing input pin": lambda bucket: edit_pins(bucket, lambda pins: pins.pop(next(iter(pins)))),
        "an input the job did not declare": lambda bucket: edit_pins(bucket, lambda pins: pins.update({"not_a_bronze_table": "5"})),
    }.items():
        results: list[tuple[Any, ...]] = []
        for checks in (schema_checks, job_checks):
            bucket = base.template_bucket()
            change(bucket)
            try:
                found = ecs_lineage.job_lineage("silver", inv, declared, ecs_lineage.S3Evidence(bucket, BUCKET), base.facts("silver"), checks)
                results.append(("ok", found.launch["snapshotState"], found.inputs, found.outputs))
            except (ecs_lineage.LineageRefused, snapshots.SnapshotError):
                results.append(("refused",))
        outcomes[label] = results
        require(results[0] == results[1], f"{label}: the schema checks give {results[0][:2]}, the job's {results[1][:2]}")
    require(outcomes["good"][0][0] == "ok" and all(outcomes[label][0] == ("refused",) for label in outcomes if label != "good"), f"outcomes {outcomes}")


def edit_output(bucket: Any, change: Callable[[dict[str, Any]], Any]) -> None:
    base.edit(bucket, f"lakehouse/layer_runs/silver/{base.RUN_OF['silver']}/silver_snapshot.json", change)


def edit_pins(bucket: Any, change: Callable[[dict[str, Any]], Any]) -> None:
    base.edit(bucket, f"lakehouse/layer_runs/silver/{base.RUN_OF['silver']}/pins.json", change)


def the_job_images_need_no_jsonschema() -> None:
    """The spark image has no ``jsonschema`` or ``referencing``. With neither importable and every ``orchestration`` module imported afresh, the job module,
    ``ecs_lineage``, ``snapshots`` and the record checks import and build the same per-table events as with them."""
    with folder_of("gold") as (_, folder, inputs):
        wanted = job_lineage.table_lines(job_of("gold", folder), ENDED, inputs)
        text = ecs_lineage.invocation_text(request_of("gold", base.RUN_OF["silver"]))
        with mock.patch.dict(sys.modules):  # restores the whole table on exit, so the real modules come back
            for name in [name for name in sys.modules if name.split(".")[0] in ("orchestration", "jsonschema", "referencing", "jsonschema_specifications")]:
                del sys.modules[name]
            blocked: dict[str, Any] = {"jsonschema": None, "referencing": None}
            sys.modules.update(blocked)
            fresh = importlib.import_module("orchestration.job_lineage")
            run = fresh.job_run(text, "gold", base.RUN_OF["gold"], "", folder, base.START)
            found = fresh.table_lines(run, ENDED, inputs)
            require(fresh is not job_lineage, "the module was imported afresh")
            refuses(ImportError, lambda: importlib.import_module("jsonschema"))
    require(found == wanted and len(found) > 2, "the events built without jsonschema differ from the events built with it")


class Phase:
    """One run of ``layer_run.main`` for a phase, over a real run folder and the stand-in receiver, with the phase's own work stubbed."""

    def __init__(self, layer: str, phase: str, url: str | None, *, fails: bool = False, extra: dict[str, str] | None = None) -> None:
        self.layer, self.phase, self.url, self.fails, self.extra = layer, phase, url, fails, extra or {}

    def run(self) -> int:
        run, input_run, _ = TEXT_FOR[self.layer]
        bucket = base.template_bucket()
        text = ecs_lineage.invocation_text(request_of(self.layer, input_run))
        with tempfile.TemporaryDirectory() as name:
            work = Path(name)
            folder = work / "code/data/analytics/dbt/runs" / run
            inputs = write_run_folder(bucket, self.layer, run, folder)
            manifest = folder / layer_run.DBT_TARGET.get(self.layer, Path("target")) / "manifest.json"
            manifest.parent.mkdir(parents=True, exist_ok=True)
            manifest.write_text("{}")
            environ = {"LAYER_DRY_RUN": "", "LAYER_PHASE": "", **self.extra}
            if self.url:
                environ |= environ_for(self.url, text)
            arguments = [self.phase, "--layer", self.layer, "--run", run, "--work", name, "--code-run-id", run, "--bucket", BUCKET, "--input-run", input_run]

            def dispatch(job: Any, phase: str, args: Any) -> None:
                if self.fails:
                    raise layer_run.StepError(f"{phase} failed")

            with (
                mock.patch.dict("os.environ", environ, clear=False),
                mock.patch.object(layer_run, "dispatch", dispatch),
                mock.patch.object(layer_run, "startup"),
                mock.patch.object(layer_run, "check_code_run"),
                mock.patch.object(layer_run, "check_feature_set"),
                mock.patch.object(layer_run.task_capacity, "apply"),
                mock.patch.object(layer_run, "inputs_record", lambda job, manifest, tables: inputs),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                return int(layer_run.main(arguments))


def the_layer_driver_sends_at_pin_and_upload_only() -> None:
    """START goes out after a successful pin, the per-table events after a successful upload and a FAIL after a failed pin or upload. No other phase sends
    anything, a failed send never changes the exit code, and a dry run, a release launch and a run with no settings send nothing."""
    wanted = base.catalogue_of_the_mirror()
    for layer in ("silver", "gold", "features"):
        with base.receiving(catalogue=wanted) as receiver:
            for phase in layer_run.PHASES[layer]:
                before = len(receiver.requests)
                require(Phase(layer, phase, receiver.url).run() == 0, f"{layer} {phase} exit code")
                sent = len(receiver.requests) - before
                if phase == "pin":
                    require(sent == 1, f"{layer} pin sent {sent} events, not its START")
                elif phase == "upload":
                    require(sent > 2 and sent % 2 == 0, f"{layer} upload sent {sent} events: a START and a COMPLETE per table")
                else:
                    require(sent == 0, f"{layer} {phase} sent {sent} events")
            before = len(receiver.requests)
            require(Phase(layer, "upload", receiver.url, fails=True).run() == 1, f"{layer}: a failed upload exits 1")
            require(len(receiver.requests) - before == 1, f"{layer}: a failed upload sends one FAIL")
            require(json.loads(receiver.requests[-1]["body"])["eventType"] == "FAIL", f"{layer}: the failed upload sends a FAIL")
            before = len(receiver.requests)
            require(Phase(layer, "build", receiver.url, fails=True).run() == 1 and len(receiver.requests) == before, f"{layer}: a failed build sends nothing")
            require(Phase(layer, "pin", receiver.url, fails=True).run() == 1 and len(receiver.requests) == before + 1, f"{layer}: a failed pin sends a FAIL")
            before = len(receiver.requests)
            require(
                Phase(layer, "upload", receiver.url, extra={"LAYER_DRY_RUN": "1"}).run() == 0 and len(receiver.requests) == before,
                f"{layer}: a dry run sends nothing",
            )
        require(Phase(layer, "upload", None).run() == 0, f"{layer}: no settings, no failure")
    with answering(500) as failing:
        require(Phase("gold", "pin", failing.url).run() == 0 and failing.requests == 1, "a receiver that answers 500 leaves the pin phase's exit code at 0")
        require(Phase("gold", "upload", failing.url).run() == 0 and failing.requests == 2, "and the upload phase's")
        require(Phase("gold", "upload", failing.url, fails=True).run() == 1, "a failed upload still exits 1 when its FAIL is refused")


CASES: dict[str, Callable[[], None]] = {
    "job_events_equal_the_airflow_side_events": job_events_equal_the_airflow_side_events,
    "job_events_resolve_in_the_stand_in_receiver": job_events_resolve_in_the_stand_in_receiver,
    "a_failed_send_never_fails_the_job": a_failed_send_never_fails_the_job,
    "nothing_is_read_or_sent_without_the_settings": nothing_is_read_or_sent_without_the_settings,
    "the_invocation_variable_is_closed": the_invocation_variable_is_closed,
    "start_and_failure_events_name_no_dataset": start_and_failure_events_name_no_dataset,
    "record_checks_without_jsonschema_equal_the_schema_checks": record_checks_without_jsonschema_equal_the_schema_checks,
    "the_job_images_need_no_jsonschema": the_job_images_need_no_jsonschema,
    "the_layer_driver_sends_at_pin_and_upload_only": the_layer_driver_sends_at_pin_and_upload_only,
}
