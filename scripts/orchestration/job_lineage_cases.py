"""Cases of the lineage events that a layer job sends from inside its own phases (``orchestration/job_lineage.py``, hooked into ``layer_run.py``).

Isolation is justified because the receiver must fail, stall and refuse on cue, and the evidence must be wrong on cue. Every case drives the real
``orchestration.job_lineage``, ``orchestration.ecs_lineage`` and ``scripts.lakehouse.layer_run``. The Airflow side's events are built by the real
``ecs_lineage.layer_table_events`` over the in-memory evidence bucket of ``ecs_lineage_cases``, and the receiver is its local stand-in of the OpenMetadata
lineage route, which accepts only the event time form production sends. The job side reads the same records from a run folder, as the job has them.
"""

from __future__ import annotations

import contextlib
import http.client
import importlib
import inspect
import io
import json
import re
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest import mock

from botocore.exceptions import ClientError

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


def request_of(layer: str, input_run: str | None = None, task_id: str = "job") -> dict[str, Any]:
    """The launching step's submission request (a one-job chain launches in its ``job`` task)."""
    request = {**base.invocation(layer), "task_id": task_id}
    if input_run not in (None, "latest"):
        request["input_run_id"] = input_run
    return request


def text_of(request: dict[str, Any], left: float = 6 * 3600) -> str:
    """``LINEAGE_INVOCATION`` for a launch whose absolute deadline is ``left`` seconds away (a layer job's deadline is 6 hours)."""
    return ecs_lineage.invocation_text(request, int(time.time() + left))


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
    text = text_of(request_of(layer, input_run))
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
        text = text_of(request)
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
            sent = job_lineage.emit("table events", partial_lines(run, inputs), environ_for(receiver.url, text_of(request_of(layer, TEXT_FOR[layer][1]))), 30.0)
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
    text = text_of(request_of("silver"))
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
    text = text_of(request_of("silver"))
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
    """The variable holds exactly the launching step's five identity fields, the input run bound at launch and the absolute launch deadline. Anything else
    is refused by name, so a job never sends an event under an identity the launch did not give it."""
    good = json.loads(text_of(request_of("gold", base.RUN_OF["silver"])))
    fields = {"template", "dag_id", "task_id", "dag_run_id", "attempt", "input_run", "deadline_epoch"}
    require(set(good) == fields and good["task_id"] == ecs_lineage.JOB_TASK, f"fields {good}")
    run, _, _ = TEXT_FOR["gold"]

    def parse(text: str) -> None:
        job_lineage.job_run(text, "gold", run, "", Path("/nonexistent"), base.START)

    parse(json.dumps(good))
    broken = {
        "not JSON": "{",
        "a list": "[]",
        "a missing field": json.dumps({key: value for key, value in good.items() if key != "attempt"}),
        "an extra field": json.dumps({**good, "mode": "real"}),
        "no launch deadline": json.dumps({key: value for key, value in good.items() if key != "deadline_epoch"}),
        "a launch deadline of 0": json.dumps({**good, "deadline_epoch": 0}),
        "a text launch deadline": json.dumps({**good, "deadline_epoch": str(good["deadline_epoch"])}),
        "a boolean launch deadline": json.dumps({**good, "deadline_epoch": True}),
        "a fractional launch deadline": json.dumps({**good, "deadline_epoch": good["deadline_epoch"] + 0.5}),
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


def the_input_run_is_a_run_id_of_the_one_form_or_latest() -> None:
    """The input run of ``LINEAGE_INVOCATION`` is what the launch binds (``request.input_run_id or "latest"``): ``latest`` or a run ID of the one form of
    ``training_contract`` (``<name>_<8 hex>[_<attempt>]``, qualified by a model or an infection for the runs that are). The job takes exactly what the
    submission request schema takes for ``input_run_id``, so a copy of the old stamp form cannot come back and refuse every live launch."""
    schema = json.loads((ROOT / "data_contracts/orchestration/submission_request.schema.json").read_text())
    request_form = re.compile(schema["properties"]["input_run_id"]["pattern"])
    good = json.loads(text_of(request_of("gold", base.RUN_OF["silver"])))
    run, _, _ = TEXT_FOR["gold"]
    taken = (
        "latest",
        "ingestion_abcdef01",
        "silver_abcdef01",
        "gold_abcdef01_2",
        "features_abcdef01",
        "features_elastic_net_abcdef01_3",
        "training_spatial_abcdef01",
        "drift_ols_abcdef01",
        "ingestion_retry_abcdef01_2_2",
    )
    refused = ("", "yesterday", "20261116T201053Z-b10a4f2d", "silver_abcdef0", "Silver_abcdef01", "silver_abcdef01_1", "silver_abcdef01\n", "latest2")
    for text in (*taken, *refused):
        expected = text == "latest" or request_form.fullmatch(text) is not None
        require(expected == (text in taken), f"the fixture lists {text!r} wrongly against the submission request schema")
        invocation = json.dumps({**good, "input_run": text})
        if expected:
            found = job_lineage.job_run(invocation, "gold", run, "", Path("/nonexistent"), base.START)
            require(found.input_run == text, f"the input run {text!r} was changed to {found.input_run!r}")
        else:
            refuses(ecs_lineage.LineageRefused, partial(job_lineage.job_run, invocation, "gold", run, "", Path("/nonexistent"), base.START))


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
        text = text_of(request_of("gold", base.RUN_OF["silver"]))
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


@dataclass
class Workspace:
    """The job's work folder at a phase: the run folder with the records of the run, the per-table inputs record and the evidence bucket they came from."""

    layer: str
    work: Path
    folder: Path
    inputs: bytes | None
    bucket: Any


@contextmanager
def workspace(layer: str, init_age: float = 120.0) -> Iterator[Workspace]:
    """A work folder whose init container wrote the task start ``init_age`` seconds ago."""
    run, _, _ = TEXT_FOR[layer]
    bucket = base.template_bucket()
    with tempfile.TemporaryDirectory() as name:
        work = Path(name)
        folder = work / "code/data/analytics/dbt/runs" / run
        inputs = write_run_folder(bucket, layer, run, folder)
        manifest = folder / layer_run.DBT_TARGET.get(layer, Path("target")) / "manifest.json"
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text("{}")
        (work / layer_run.STARTED_FILE).write_text(f"{time.time() - init_age:.0f}\n")
        yield Workspace(layer, work, folder, inputs, bucket)


class Phase:
    """One run of ``layer_run.main`` for a phase, over a real run folder and the stand-in receiver, with the phase's own work stubbed. ``left`` is how many
    seconds of the launch deadline remain, ``fails`` makes the phase raise a ``StepError`` and ``error`` any exception or exit instead."""

    def __init__(
        self,
        layer: str,
        phase: str,
        url: str | None,
        *,
        fails: bool = False,
        error: BaseException | None = None,
        extra: dict[str, str] | None = None,
        left: float = 6 * 3600,
        init_age: float = 120.0,
        ws: Workspace | None = None,
        inputs_delay: float = 0.0,
        inputs_error: Exception | None = None,
    ) -> None:
        self.layer, self.phase, self.url, self.fails, self.error, self.extra = layer, phase, url, fails, error, extra or {}
        self.left, self.init_age, self.ws, self.inputs_delay, self.inputs_error = left, init_age, ws, inputs_delay, inputs_error
        self.err = io.StringIO()

    def run(self) -> int:
        if self.ws is not None:
            return self.go(self.ws)
        with workspace(self.layer, self.init_age) as ws:
            return self.go(ws)

    def go(self, ws: Workspace) -> int:
        run, input_run, _ = TEXT_FOR[self.layer]
        text = text_of(request_of(self.layer, input_run), self.left)
        environ = {"LAYER_DRY_RUN": "", "LAYER_PHASE": "", **self.extra}
        if self.url:
            environ |= environ_for(self.url, text)
        arguments = [
            self.phase,
            "--layer",
            self.layer,
            "--run",
            run,
            "--work",
            str(ws.work),
            "--code-run-id",
            run,
            "--bucket",
            BUCKET,
            "--input-run",
            input_run,
        ]

        def slow_inputs(job: Any, manifest: Any, tables: Any) -> bytes | None:
            time.sleep(self.inputs_delay)  # the manifest read and parse of the upload hook, as slow as the case says
            if self.inputs_error is not None:
                raise self.inputs_error
            return ws.inputs

        def dispatch(job: Any, phase: str, args: Any) -> None:
            if self.error is not None:
                raise self.error
            if self.fails:
                raise layer_run.StepError(f"{phase} failed")

        with (
            mock.patch.dict("os.environ", environ, clear=False),
            mock.patch.object(layer_run, "dispatch", dispatch),
            mock.patch.object(layer_run, "startup"),
            mock.patch.object(layer_run, "check_code_run"),
            mock.patch.object(layer_run, "check_feature_set"),
            mock.patch.object(layer_run.task_capacity, "apply"),
            mock.patch.object(layer_run, "inputs_record", slow_inputs),
            contextlib.redirect_stderr(self.err),
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


def delivery_never_crosses_the_launch_deadline() -> None:
    """A job's lineage delivery runs inside what is left of the absolute launch deadline that the launcher also counts (which starts before the init
    container), after a reserve for the phases that follow the upload and the launcher's own poll, and is skipped with a recorded reason when that time is
    short, so a send never turns a successful job into a launcher timeout. The review's case: 2 s left, a receiver that answers after 3 s."""
    cap, reserve = job_lineage.TABLE_SECONDS, job_lineage.RESERVE_SECONDS
    require(reserve >= 600, f"the reserve of {reserve} s is under the launcher's stop and poll time and the maintenance and Feast phases that follow")
    for left, seconds in ((cap + reserve + 1, cap), (cap + reserve, cap), (cap + reserve - 1, 0.0), (2.0, 0.0), (0.0, 0.0), (-5.0, 0.0)):
        got, why = job_lineage.delivery_budget(cap, left)
        require(got == seconds and bool(why) == (seconds == 0.0), f"{left} s left gives {got} s and {why!r}, not {seconds}")
    for left, seconds in ((600.0, 10.0), (3.0, 3.0), (0.5, 0.5), (0.0, 0.0), (-1.0, 0.0)):
        got, why = job_lineage.delivery_budget(10.0, left, failed=True)
        require(got == seconds and bool(why) == (seconds == 0.0), f"a FAIL with {left} s left gets {got} s and {why!r}, not {seconds}")
    with base.receiving(delay=3.0) as slow:
        phase = Phase("silver", "upload", slow.url, left=2.0)
        began = time.monotonic()
        code = phase.run()
        spent = time.monotonic() - began
        require(code == 0 and slow.requests == [] and spent < 2.5, f"2 s left: exit {code}, {len(slow.requests)} requests, {spent:.2f} s")
        require("not sent" in phase.err.getvalue() and "deadline" in phase.err.getvalue(), f"the skip is not recorded: {phase.err.getvalue()!r}")
        phase = Phase("silver", "pin", slow.url, left=reserve - 1)
        require(phase.run() == 0 and slow.requests == [] and "deadline" in phase.err.getvalue(), "a START is skipped inside the reserve too")
    with base.receiving() as fast:
        late = Phase("gold", "upload", fast.url, init_age=21600 - 100)
        require(
            late.run() == 0 and fast.requests == [] and "deadline" in late.err.getvalue(),
            "a task whose init container started 100 s before its own deadline sends nothing",
        )
        enough = Phase("gold", "upload", fast.url, left=cap + reserve + 60)
        require(enough.run() == 0 and len(fast.requests) > 2, f"with the time and the reserve the events go out: {len(fast.requests)}")
        before = len(fast.requests)
        require(
            Phase("gold", "upload", fast.url, fails=True, left=2.0).run() == 1 and len(fast.requests) == before + 1, "a FAIL inside the last seconds still goes"
        )
        require(
            Phase("gold", "upload", fast.url, fails=True, left=-5.0).run() == 1 and len(fast.requests) == before + 1, "no FAIL after the deadline has passed"
        )


def the_two_senders_share_one_set_of_event_times() -> None:
    """The job and the Airflow evidence step send the same table events for one run, byte for byte, with the times of one record that the job writes beside
    its other run records (``lineage_times.json``: the init container's task start, written once by the pin, and the time the upload began, written once
    by it), which the upload copies to S3 where the evidence step reads it. Launch, init, upload and task-end times all differ here, and the events carry
    only the record's. A run whose record is not found keeps the launch records' times; a record without the upload time is refused."""
    wanted = base.catalogue_of_the_mirror()
    launch_times = {"2026-11-04T10:00:00Z", "2026-11-04T10:30:00Z"}
    for layer in ("silver", "gold", "features"):
        with workspace(layer) as ws, base.receiving(catalogue=wanted) as receiver:
            require(Phase(layer, "pin", receiver.url, ws=ws).run() == 0, f"{layer}: pin")
            started_event = json.loads(receiver.requests[-1]["body"])
            before = len(receiver.requests)
            require(Phase(layer, "upload", receiver.url, ws=ws).run() == 0, f"{layer}: upload")
            sent = [item["body"].decode() for item in receiver.requests[before:]]
            record = json.loads((ws.folder / ecs_lineage.TIMES_FILE).read_text())
            copied = [
                path.relative_to(ws.folder).as_posix() for path in layer_run.uploadable(layer_run.Job(layer, TEXT_FOR[layer][0], "real", ws.work / "code"))
            ]
            require(ecs_lineage.TIMES_FILE in copied, f"{layer}: the upload does not copy the lineage times record to S3: {copied}")
            run = TEXT_FOR[layer][0]
            base.put(
                ws.bucket, f"{ecs_lineage.EVIDENCE_PREFIX}/{layer}/{run}/{ecs_lineage.TIMES_FILE}", record
            )  # the upload copies the run folder to these keys
            declared = ecs_lineage.declared_lineage(layer)
            airflow = airflow_lines(layer, ws.bucket, declared)
            absent = airflow_lines(layer, base.template_bucket(), declared)
            unfinished = base.template_bucket()
            base.put(
                unfinished, f"{ecs_lineage.EVIDENCE_PREFIX}/{layer}/{run}/{ecs_lineage.TIMES_FILE}", {k: v for k, v in record.items() if k != "completed_at"}
            )
            incomplete: list[str] = []
        require(
            set(record) == {"schema", "run_id", "started_at", "completed_at"} and record["started_at"] != record["completed_at"], f"{layer}: record {record}"
        )
        require(sent == airflow and len(sent) > 2, f"{layer}: {len(sent)} job events differ from {len(airflow)} Airflow events")
        started, completed = ecs_lineage.utc_time(record["started_at"]), ecs_lineage.utc_time(record["completed_at"])
        times: dict[str, set[str]] = {event["eventType"]: set() for event in map(json.loads, sent)}
        for event in map(json.loads, sent):
            times[event["eventType"]].add(event["eventTime"])
        require(times == {"START": {started}, "COMPLETE": {completed}}, f"{layer}: event times {times}, the record has {started} and {completed}")
        require(not ({started, completed} & launch_times), f"{layer}: the case does not tell the record's times from the launch's")
        require(started_event["eventTime"] == started, f"{layer}: the task START carries the record's start")
        found = {json.loads(line)["eventTime"] for line in absent}
        require(found == launch_times, f"{layer}, no record: the evidence step's times are {found}, not the launch records'")
        refused = refuses(ecs_lineage.LineageRefused, partial(airflow_lines, layer, unfinished, declared))
        require(
            ecs_lineage.TIMES_FILE in str(refused) and incomplete == [],
            f"{layer}: a succeeded job's record without its upload time is not read as absent: {refused}",
        )


def the_replay_task_start_shares_the_job_start() -> None:
    """The replay's task START has the job's START time when the job wrote it, and the job's START and the replay's differ in the run facet's outcome fields
    only: the job sends its START at the end of the pin phase, before the outcome of the run is known, so it says not_applicable where the replay, which
    reads the finished run, says complete. Nothing else differs."""
    rig = base.Rig()
    base.evidence_bucket(rig)
    base.operator(rig, None).execute(base.context())
    inv = ecs_lineage.step_invocation(request_of("gold", base.RUN_OF["silver"]))
    started = "2026-11-04T09:59:30+00:00"
    base.put(
        rig.bucket,
        f"{ecs_lineage.EVIDENCE_PREFIX}/gold/{base.RUN_OF['gold']}/{ecs_lineage.TIMES_FILE}",
        {"schema": 1, "run_id": base.RUN_OF["gold"], "started_at": started, "completed_at": "2026-11-04T10:20:00+00:00"},
    )
    source = ecs_lineage.LineageSource(
        "gold",
        base.RUN_OF["gold"],
        1,
        inv,
        base.DECLARED["gold"],
        ecs_lineage.S3Evidence(rig.bucket, BUCKET),
        base.S3LaunchStore(rig.bucket, BUCKET, base.PREFIX),
    )
    events = source.events()
    require(events[0]["eventType"] == "START" and events[0]["eventTime"] == "2026-11-04T09:59:30Z", f"the replay START has time {events[0]['eventTime']}")
    run = job_lineage.job_run(text_of(request_of("gold", base.RUN_OF["silver"])), "gold", base.RUN_OF["gold"], "", Path("/nonexistent"), started)
    mine = json.loads(job_lineage.start_lines(run)[0])
    meant_to_differ = ("snapshotState", "missingOutputs", "refusal")

    def plain(event: dict[str, Any]) -> dict[str, Any]:
        found = json.loads(json.dumps(event))
        for name in meant_to_differ:
            found["run"]["facets"]["haiLaunch"].pop(name, None)
        return found

    require(plain(mine) == plain(events[0]), f"the job's START and the replay's differ beyond {meant_to_differ}")
    require(
        mine["run"]["facets"]["haiLaunch"]["snapshotState"] == "not_applicable" and events[0]["run"]["facets"]["haiLaunch"]["snapshotState"] == "complete",
        "the outcome fields",
    )


def any_ordinary_exception_sends_the_fail_and_propagates() -> None:
    """A phase that dies of any ordinary exception, an SDK failure included (a denied pin claim is a botocore ClientError, a failed upload a boto3
    S3UploadFailedError), sends the FAIL and then raises the same exception, so the job's failure and exit code are what they were. An exit that is not an
    ``Exception`` (an interrupt, ``SystemExit``) sends nothing and propagates; with no settings nothing is sent; a refused FAIL still leaves the error."""
    from boto3.exceptions import S3UploadFailedError
    from botocore.exceptions import ClientError

    denied = ClientError({"Error": {"Code": "AccessDenied", "Message": "not allowed"}}, "PutObject")
    for phase, error in (
        ("pin", denied),
        ("upload", S3UploadFailedError(f"Failed to upload part: {CREDENTIAL}")),
        ("upload", RuntimeError("a bug")),
        ("pin", ZeroDivisionError("a bug")),
    ):
        with base.receiving() as receiver:
            run = Phase("silver", phase, receiver.url, error=error)
            raised = refuses(type(error), run.run)
            require(raised is error, f"{phase} {type(error).__name__}: another exception came out: {raised!r}")
            sent = [json.loads(item["body"]) for item in receiver.requests]
            require([event["eventType"] for event in sent] == ["FAIL"], f"{phase} {type(error).__name__}: {len(sent)} requests")
            require(phase in sent[0]["run"]["facets"]["haiLaunch"]["refusal"], f"{phase}: the FAIL names the phase")
            require(CREDENTIAL not in json.dumps(sent), f"{phase} {type(error).__name__}: the FAIL carries the exception's text")
    for exit_error in (KeyboardInterrupt(), SystemExit(3)):
        with base.receiving() as receiver:
            came = refuses(type(exit_error), Phase("gold", "upload", receiver.url, error=exit_error).run)
            require(came is exit_error and receiver.requests == [], f"{exit_error!r} sent a request")
    with base.receiving() as receiver:
        refuses(RuntimeError, Phase("gold", "upload", None, error=RuntimeError("x")).run)
        require(receiver.requests == [], "no settings, no request")
    with answering(500) as refusing:
        own = RuntimeError("the job's own failure")
        require(refuses(RuntimeError, Phase("gold", "upload", refusing.url, error=own).run) is own and refusing.requests == 1, "a refused FAIL changes nothing")
    with base.receiving() as receiver:
        refuses(RuntimeError, Phase("gold", "build", receiver.url, error=RuntimeError("a bug")).run)
        require(receiver.requests == [], "a phase that sends no lineage sends no FAIL for an ordinary exception either")


def a_slow_builder_cannot_outlast_the_delivery_budget() -> None:
    """One absolute deadline covers building the events and sending them. A builder that is slower than the budget (the manifest read and the event build)
    makes ``emit`` return at the budget, with nothing sent, nothing sent later by the builder that is left running, and the job's outcome untouched. The
    driver case stalls the upload hook's manifest read."""
    text = text_of(request_of("silver"))
    with base.receiving() as receiver:
        late = threading.Event()

        def slow() -> list[str]:
            time.sleep(0.35)
            late.set()
            return ["{}"]

        err = io.StringIO()
        began = time.monotonic()
        with contextlib.redirect_stderr(err):
            sent = job_lineage.emit("table events", slow, environ_for(receiver.url, text), 0.1)
        spent = time.monotonic() - began
        require(sent == 0 and spent < 0.25, f"a builder of 0.35 s under a budget of 0.1 s: sent {sent} after {spent:.3f} s")
        require("deadline" in err.getvalue() and err.getvalue().count("\n") == 1 and CREDENTIAL not in err.getvalue(), f"standard error: {err.getvalue()!r}")
        require(late.wait(2.0), "the builder never finished: the case does not test a late builder")
        time.sleep(0.3)
        require(receiver.requests == [], f"a builder that finished after the deadline sent {len(receiver.requests)} events")
        failing = io.StringIO()

        def broken() -> list[str]:
            time.sleep(0.2)
            raise RuntimeError(CREDENTIAL)

        with contextlib.redirect_stderr(failing):
            require(job_lineage.emit("table events", broken, environ_for(receiver.url, text), 0.05) == 0, "a builder that fails after the deadline")
        time.sleep(0.3)
        require(CREDENTIAL not in failing.getvalue(), f"a late failure leaks: {failing.getvalue()!r}")
        within = io.StringIO()
        with contextlib.redirect_stderr(within):
            require(job_lineage.emit("table events", lambda: ["{}"], environ_for(receiver.url, text), 5.0) == 1, "a fast builder inside the budget still sends")
    with base.receiving() as receiver, mock.patch.object(job_lineage, "TABLE_SECONDS", 0.1):
        phase = Phase("gold", "upload", receiver.url, inputs_delay=0.35)
        began = time.monotonic()
        code = phase.run()
        spent = time.monotonic() - began
        time.sleep(0.4)
        require(
            code == 0 and receiver.requests == [] and spent < 0.3,
            f"the upload hook with a stalled manifest read: exit {code}, {len(receiver.requests)} requests, {spent:.3f} s",
        )
        require("deadline" in phase.err.getvalue(), f"the stall is not recorded: {phase.err.getvalue()!r}")


def untraced(frame: Any, event: str, arg: Any) -> Any:
    return None


class Pause:
    """Holds a worker at one line of a function through a line trace, so a case can make the caller's deadline pass in exactly that gap and release the worker
    afterwards. Neither the sender nor the HTTP transport is replaced. ``before_send`` holds the delivery worker of ``job_lineage.emit`` where it hands its
    built events to the sender; ``before_write`` holds the transport's ``post_event`` after its connection is made and its last cancellation check passed, one
    line before the first HTTP write."""

    def __init__(self, function: Callable[..., Any], needle: str, name: str, file: str) -> None:
        source, first = inspect.getsourcelines(function)
        self.line = first + next(index for index, text in enumerate(source) if all(part in text for part in needle.split("&")))
        self.name, self.file = name, file
        self.paused, self.release, self.ended = threading.Event(), threading.Event(), threading.Event()
        self.workers: list[threading.Thread] = []

    @classmethod
    def before_send(cls) -> Pause:
        return cls(job_lineage.emit, "(lines&send", "work", job_lineage.__file__)

    @classmethod
    def before_write(cls) -> Pause:
        return cls(ecs_lineage.post_event, 'connection.request("POST"', "post_event", ecs_lineage.__file__)

    def trace(self, frame: Any, event: str, arg: Any) -> Any:
        if frame.f_code.co_filename == self.file and frame.f_code.co_name == self.name:
            if event == "line" and frame.f_lineno == self.line:
                self.workers.append(threading.current_thread())
                self.paused.set()
                if not self.release.wait(5):
                    raise RuntimeError("the case never released the worker")
            if event == "return":
                self.ended.set()
        return self.trace

    @contextmanager
    def holding(self) -> Iterator[Pause]:
        threading.settrace(self.trace)
        try:
            yield self
        finally:
            self.release.set()
            threading.settrace(untraced)


def no_event_is_sent_after_the_deadline() -> None:
    """The delivery has one absolute deadline and one cancellation that the caller sets before it returns. A worker that is paused between its last check and
    its first send, and resumes after the caller returned (the review's race: the worker took a fresh relative budget and sent all 142 events after the driver's
    exit 0), sends nothing."""
    text = text_of(request_of("silver"))
    with base.receiving() as receiver, Pause.before_send().holding() as pause:
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            sent = job_lineage.emit("race", lambda: ["{}", "{}"], environ_for(receiver.url, text), 0.05)
        require(pause.paused.is_set() and sent == 0 and receiver.requests == [], f"the case did not reach its pause: {sent} {len(receiver.requests)}")
        pause.release.set()
        require(pause.ended.wait(3), "the worker never ended")
        for worker in pause.workers:
            worker.join(2)
        require(receiver.requests == [], f"{len(receiver.requests)} events were sent after the caller returned")
        require("not sent" in err.getvalue() and CREDENTIAL not in err.getvalue(), f"standard error: {err.getvalue()!r}")


def no_event_follows_the_drivers_exit() -> None:
    """The same race through the real upload driver: its table events are built, the worker is held before its first send, the budget passes, the driver
    returns exit 0 and only then is the worker released. Nothing is sent."""
    with (
        base.receiving(catalogue=base.catalogue_of_the_mirror()) as receiver,
        Pause.before_send().holding() as pause,
        mock.patch.object(job_lineage, "TABLE_SECONDS", 0.3),
    ):
        driver = Phase("gold", "upload", receiver.url)
        code = driver.run()
        require(
            pause.paused.is_set() and code == 0 and receiver.requests == [],
            f"the driver case did not reach its pause: exit {code}, {len(receiver.requests)} requests",
        )
        pause.release.set()
        require(pause.ended.wait(3), "the driver's worker never ended")
        time.sleep(0.3)
        require(receiver.requests == [], f"{len(receiver.requests)} events were sent after the driver returned exit 0")


def a_send_in_flight_is_aborted_at_the_deadline() -> None:
    """A send that is waiting for a receiver that does not answer when the deadline passes is aborted by the stop (its connection is shut down), and the
    delivery worker is gone within the stop grace, before the caller returns. The transport's own socket timeout is set far above the budget here, so only the
    cancellation can end the send in time: a timeout that equalled the budget would end it with no cancellation at all."""
    text = text_of(request_of("silver"))
    real = ecs_lineage.post_event

    def patient(url: str, payload: bytes, credential: str, timeout: float = 0.0, cancel: ecs_lineage.Cancellation | None = None) -> int:
        return real(url, payload, credential, timeout=30.0, cancel=cancel)

    with base.receiving(delay=30.0) as stalled, mock.patch.object(ecs_lineage, "post_event", patient):
        began = time.monotonic()
        with contextlib.redirect_stderr(io.StringIO()):
            sent = job_lineage.emit("stalled", lambda: ["{}"], environ_for(stalled.url, text), 0.3)
        spent = time.monotonic() - began
        alive = [
            thread.name for thread in threading.enumerate() if thread.name.endswith(("(work)", "(run)"))
        ]  # the delivery worker and the sender thread of send_within
        require(sent == 0 and spent < 1.8, f"a send in flight at the deadline: {sent} after {spent:.2f} s")
        require(not alive, f"the worker of a send in flight, whose own timeout is 30 s, is still running after the caller returned: {alive}")
        require(job_lineage.STOP_GRACE_SECONDS <= 1.0, "the wait for a worker to stop is bounded at one second")


def no_event_is_written_after_the_last_cancellation_check() -> None:
    """The transport has made its connection and passed its last cancellation check, and is held one line before its first HTTP write. The deadline passes,
    the caller stops the sending and returns (through ``emit`` and through the real upload driver, which exits 0), and only then is the sender released. The
    stop has shut the connection down, so its write fails: no request completes at the receiver. Neither the deadline nor a check can help here, because both
    were passed before the pause: only the stop can."""
    text = text_of(request_of("silver"))
    with base.receiving() as receiver, Pause.before_write().holding() as pause:
        with contextlib.redirect_stderr(io.StringIO()):
            sent = job_lineage.emit("write", lambda: ["{}", "{}"], environ_for(receiver.url, text), 0.3)
        require(pause.paused.is_set() and sent == 0, f"the case did not reach its pause: sent {sent}, paused {pause.paused.is_set()}")
        pause.release.set()
        require(pause.ended.wait(3), "the sender never ended")
        time.sleep(0.3)
        require(receiver.requests == [], f"{len(receiver.requests)} requests completed after emit returned")
    with (
        base.receiving(catalogue=base.catalogue_of_the_mirror()) as receiver,
        Pause.before_write().holding() as pause,
        mock.patch.object(job_lineage, "TABLE_SECONDS", 0.3),
    ):
        code = Phase("gold", "upload", receiver.url).run()
        require(
            pause.paused.is_set() and code == 0 and receiver.requests == [],
            f"the driver case did not reach its pause: exit {code}, {len(receiver.requests)} requests",
        )
        pause.release.set()
        require(pause.ended.wait(3), "the driver's sender never ended")
        time.sleep(0.3)
        require(receiver.requests == [], f"{len(receiver.requests)} requests completed after the driver returned exit 0")


def an_expired_deadline_sends_nothing() -> None:
    """At the send boundary an absolute deadline that has already passed refuses the send before any HTTP work, with a cancellation that has not been stopped,
    so the deadline guards the sender on its own. A deadline that has not passed still sends (the control)."""
    with base.receiving() as receiver:
        cancel = ecs_lineage.Cancellation()
        refused = refuses(ecs_lineage.DeliveryFailed, partial(job_lineage.send_until, ["{}", "{}"], receiver.url, CREDENTIAL, time.monotonic() - 1.0, cancel))
        require("deadline" in str(refused) and receiver.requests == [], f"an expired deadline: {refused}, {len(receiver.requests)} requests")
        job_lineage.send_until(["{}", "{}"], receiver.url, CREDENTIAL, time.monotonic() + 5.0, ecs_lineage.Cancellation())
        require(len(receiver.requests) == 2, f"a deadline that has not passed sent {len(receiver.requests)} of 2")
        stopped = ecs_lineage.Cancellation()
        stopped.stop()
        refuses(ecs_lineage.DeliveryFailed, partial(job_lineage.send_until, ["{}"], receiver.url, CREDENTIAL, time.monotonic() + 5.0, stopped))
        require(len(receiver.requests) == 2, "a stopped cancellation sent")


def the_cancellation_orders_its_checks_with_the_stop() -> None:
    """``begin``, ``check``, ``end`` and ``stop`` each take the cancellation's lock, so a check or a registration and a stop never interleave: the stop that
    shuts a connection down and the send that registers it, or that checks before its write, are ordered. While another thread holds the lock none of them
    completes, and each completes once it is released."""
    cancel = ecs_lineage.Cancellation()
    done: dict[str, threading.Event] = {name: threading.Event() for name in ("begin", "check", "end", "stop")}
    calls: dict[str, Callable[[], None]] = {
        "begin": lambda: cancel.begin(http.client.HTTPConnection("127.0.0.1")),
        "check": cancel.check,
        "end": cancel.end,
        "stop": cancel.stop,
    }
    threads = []

    def run(name: str) -> None:
        calls[name]()
        done[name].set()

    with cancel.lock:
        for name in ("begin", "check", "end"):
            thread = threading.Thread(target=run, args=(name,), daemon=True)
            thread.start()
            threads.append(thread)
        time.sleep(0.3)
        require(
            not any(done[name].is_set() for name in ("begin", "check", "end")),
            f"a call completed while another thread held the lock: {[n for n, e in done.items() if e.is_set()]}",
        )
    for thread in threads:
        thread.join(2)
    require(all(done[name].is_set() for name in ("begin", "check", "end")), "a call never completed after the lock was released")
    with cancel.lock:
        stopper = threading.Thread(target=run, args=("stop",), daemon=True)
        stopper.start()
        time.sleep(0.3)
        require(not done["stop"].is_set(), "stop completed while another thread held the lock")
    stopper.join(2)
    require(done["stop"].is_set() and cancel.stopped, "stop never completed after the lock was released")


class FlakyBucket:
    """The evidence bucket with one key that answers every read with the given error code."""

    def __init__(self, bucket: Any, key: str, code: str) -> None:
        self.bucket, self.key, self.code = bucket, key, code

    def get_object(self, **arguments: Any) -> Any:
        if arguments["Key"] == self.key:
            raise ClientError({"Error": {"Code": self.code, "Message": "synthetic"}}, "GetObject")
        return self.bucket.get_object(**arguments)


def only_a_missing_times_record_is_absent() -> None:
    """Only a key that is not found is read as absent (the launch records' times then stand). Every other error of the read, a denial included, propagates
    and no event is built from it: it would carry the launch times where the job sent the record's, so every table event of the run would differ. In
    production an absent key also answers AccessDenied (the launch role cannot list the bucket), so a run whose record cannot be told from a denied one has no
    Airflow-side events, visibly. The Airflow step logs it and goes on, the replay command fails with its reason, and the job's own send skips and says why."""
    layer = "gold"
    run = TEXT_FOR[layer][0]
    key = f"{ecs_lineage.EVIDENCE_PREFIX}/{layer}/{run}/{ecs_lineage.TIMES_FILE}"
    record = {"schema": 1, "run_id": run, "started_at": "2026-11-04T10:00:30+00:00", "completed_at": "2026-11-04T10:29:00+00:00"}
    declared = ecs_lineage.declared_lineage(layer)

    def build(bucket: Any) -> list[str]:
        return airflow_lines(layer, bucket, declared)

    readable = base.template_bucket()
    base.put(readable, key, record)
    require({json.loads(line)["eventTime"] for line in build(readable)} == {"2026-11-04T10:00:30Z", "2026-11-04T10:29:00Z"}, "a readable record sets the times")
    missing = base.template_bucket()
    require(
        {json.loads(line)["eventTime"] for line in build(missing)} == {"2026-11-04T10:00:00Z", "2026-11-04T10:30:00Z"},
        "a key that is not found leaves the launch times",
    )
    denied = base.template_bucket()
    base.put(denied, key, record)
    denied.deny_keys.add(key)
    error = refuses(ecs_lineage.EvidenceDenied, partial(build, denied))
    require(key in str(error) and "AccessDenied" in str(error), f"a denied read of an existing record is not named: {error}")
    cannot_list = base.template_bucket()
    cannot_list.absent_reads_denied = True
    refuses(ecs_lineage.EvidenceDenied, partial(build, cannot_list))  # production: absent and denied answer alike, so neither is read as absent
    for code in ("InternalError", "SlowDown", "RequestTimeout", "NoSuchBucket"):
        flaky = base.template_bucket()
        base.put(flaky, key, record)
        evidence = ecs_lineage.S3Evidence(FlakyBucket(flaky, key, code), BUCKET)
        error = refuses(
            ecs_lineage.EvidenceUnavailable,
            lambda evidence=evidence: ecs_lineage.layer_table_events(
                evidence, declared, ecs_lineage.step_invocation(request_of(layer, TEXT_FOR[layer][1])), base.facts(layer)
            ),
        )
        require(key in str(error) and code in str(error), f"{code}: {error}")
    require(issubclass(ecs_lineage.EvidenceDenied, ecs_lineage.EvidenceUnavailable), "the callers' handlers cover a denial")
    steps = (ROOT / "orchestration/airflow_steps.py").read_text()
    require("except (DeliveryFailed, LineageRefused, EvidenceUnavailable)" in steps, "the Airflow step no longer logs an unavailable record and goes on")
    # the replay command fails with its reason
    rig = base.Rig()
    base.evidence_bucket(rig)
    base.operator(rig, None).execute(base.context())
    rig.bucket.objects[f"{ecs_lineage.EVIDENCE_PREFIX}/gold/{base.RUN_OF['gold']}/{ecs_lineage.TIMES_FILE}"] = json.dumps(
        {**record, "run_id": base.RUN_OF["gold"]}
    ).encode()
    rig.bucket.deny_keys.add(f"{ecs_lineage.EVIDENCE_PREFIX}/gold/{base.RUN_OF['gold']}/{ecs_lineage.TIMES_FILE}")
    arguments = ["replay", "--bucket", BUCKET, "--job", "gold", "--run-id", base.RUN_OF["gold"], "--attempt", "1", "--template", "gold_build"]
    arguments += ["--dag-id", "gold_transform_dag", "--task-id", "job", "--dag-run-id", f"manual__{base.RUN_OF['gold']}"]
    err = io.StringIO()
    with (
        contextlib.redirect_stderr(err),
        contextlib.redirect_stdout(io.StringIO()),
        mock.patch.object(ecs_lineage, "declared_lineage", lambda job: base.DECLARED[job]),
    ):
        replayed = ecs_lineage.main(arguments, client_factory=lambda: rig.bucket)
    require(
        replayed == 1 and "replay refused" in err.getvalue() and ecs_lineage.TIMES_FILE in err.getvalue(),
        f"the replay of a denied record: exit {replayed}, {err.getvalue()!r}",
    )


def the_job_skips_and_says_why_when_its_own_records_fail() -> None:
    """The job reads its own records from its run folder. A times record it cannot read (a permission error, a directory in its place) or that lacks the
    upload time (the stamp failed) is an error and not an absent record: the send is skipped with the key named, the phase's exit code stays 0 and nothing
    is sent from launch times. A record that is simply not there is the one case that is absent."""
    for label, damage in (
        ("an unreadable record", lambda path: (path.write_text("{}"), path.chmod(0))),
        ("a directory in its place", lambda path: path.mkdir()),
        (
            "a record without the upload time",
            lambda path: path.write_text(json.dumps({"schema": 1, "run_id": TEXT_FOR["gold"][0], "started_at": "2026-11-04T10:00:30+00:00"})),
        ),
    ):
        with workspace("gold") as ws, base.receiving() as receiver:
            require(Phase("gold", "pin", receiver.url, ws=ws).run() == 0, "pin")
            before = len(receiver.requests)
            path = ws.folder / ecs_lineage.TIMES_FILE
            path.unlink()
            damage(path)
            phase = Phase("gold", "upload", receiver.url, ws=ws)
            stamp = mock.patch.object(layer_run, "stamp_upload_time", lambda *args, **kwargs: None) if "without" in label else contextlib.nullcontext()
            try:  # a record without the upload time is what a failed stamp leaves, so that case turns the stamp off
                with stamp:
                    code = phase.run()
            finally:
                if path.is_file():
                    path.chmod(0o600)
            require(code == 0 and len(receiver.requests) == before, f"{label}: exit {code}, {len(receiver.requests) - before} events sent")
            require(
                "not sent" in phase.err.getvalue() and ecs_lineage.TIMES_FILE in phase.err.getvalue(),
                f"{label}: the skip is not recorded: {phase.err.getvalue()!r}",
            )
    with workspace("gold") as ws:
        run = job_of("gold", ws.folder)
        evidence = job_lineage.FolderEvidence(run, None)
        key = f"{ecs_lineage.EVIDENCE_PREFIX}/gold/{TEXT_FOR['gold'][0]}/silver_pins.json"
        require(
            evidence.read(key) is not None and evidence.read(key.replace("silver_pins", "not_there")) is None,
            "a file is read, a file that is not there is absent",
        )
        locked = ws.folder / "silver_pins.json"
        locked.chmod(0)
        try:
            error = refuses(ecs_lineage.EvidenceUnavailable, partial(evidence.read, key))
        finally:
            locked.chmod(0o600)
        require("PermissionError" in str(error), f"an unreadable record is not read as absent: {error}")
        folder_key = key.replace("silver_pins.json", "a_directory")
        (ws.folder / "a_directory").mkdir()
        refuses(ecs_lineage.EvidenceUnavailable, partial(evidence.read, folder_key))
        refuses(
            ecs_lineage.EvidenceUnavailable, partial(evidence.read, key.replace(str(TEXT_FOR["gold"][0]), "20261104T100000Z-00000000"))
        )  # another run's key is not the job's to read
    with workspace("gold") as ws, base.receiving() as receiver:
        broken = Phase("gold", "upload", receiver.url, ws=ws, inputs_error=PermissionError(CREDENTIAL))
        code = broken.run()
        require(code == 0 and receiver.requests == [], f"a manifest that cannot be read: exit {code}, {len(receiver.requests)} events")
        require(
            "not sent" in broken.err.getvalue() and "PermissionError" in broken.err.getvalue() and CREDENTIAL not in broken.err.getvalue(),
            f"{broken.err.getvalue()!r}",
        )


CASES: dict[str, Callable[[], None]] = {
    "job_events_equal_the_airflow_side_events": job_events_equal_the_airflow_side_events,
    "job_events_resolve_in_the_stand_in_receiver": job_events_resolve_in_the_stand_in_receiver,
    "a_failed_send_never_fails_the_job": a_failed_send_never_fails_the_job,
    "nothing_is_read_or_sent_without_the_settings": nothing_is_read_or_sent_without_the_settings,
    "the_invocation_variable_is_closed": the_invocation_variable_is_closed,
    "the_input_run_is_a_run_id_of_the_one_form_or_latest": the_input_run_is_a_run_id_of_the_one_form_or_latest,
    "start_and_failure_events_name_no_dataset": start_and_failure_events_name_no_dataset,
    "record_checks_without_jsonschema_equal_the_schema_checks": record_checks_without_jsonschema_equal_the_schema_checks,
    "the_job_images_need_no_jsonschema": the_job_images_need_no_jsonschema,
    "the_layer_driver_sends_at_pin_and_upload_only": the_layer_driver_sends_at_pin_and_upload_only,
    "delivery_never_crosses_the_launch_deadline": delivery_never_crosses_the_launch_deadline,
    "the_two_senders_share_one_set_of_event_times": the_two_senders_share_one_set_of_event_times,
    "the_replay_task_start_shares_the_job_start": the_replay_task_start_shares_the_job_start,
    "any_ordinary_exception_sends_the_fail_and_propagates": any_ordinary_exception_sends_the_fail_and_propagates,
    "a_slow_builder_cannot_outlast_the_delivery_budget": a_slow_builder_cannot_outlast_the_delivery_budget,
    "no_event_is_sent_after_the_deadline": no_event_is_sent_after_the_deadline,
    "no_event_follows_the_drivers_exit": no_event_follows_the_drivers_exit,
    "a_send_in_flight_is_aborted_at_the_deadline": a_send_in_flight_is_aborted_at_the_deadline,
    "no_event_is_written_after_the_last_cancellation_check": no_event_is_written_after_the_last_cancellation_check,
    "an_expired_deadline_sends_nothing": an_expired_deadline_sends_nothing,
    "the_cancellation_orders_its_checks_with_the_stop": the_cancellation_orders_its_checks_with_the_stop,
    "only_a_missing_times_record_is_absent": only_a_missing_times_record_is_absent,
    "the_job_skips_and_says_why_when_its_own_records_fail": the_job_skips_and_says_why_when_its_own_records_fail,
}
