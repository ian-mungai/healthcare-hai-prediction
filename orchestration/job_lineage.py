"""Lineage events that a layer job sends from inside its own phases: silver, gold, features and the six model sets of the features job.

``scripts/lakehouse/layer_run.py`` calls this module after the ``pin`` and ``upload`` phases of a job, the only two containers that hold the lineage
settings (``infra/jobs/layers.tf``): the HTTP transport settings of the Airflow scheduler (``OPENLINEAGE__TRANSPORT__*``, the project transport that sends
the event time in the Z form OpenMetadata 2.0.7 accepts), the lineage bot token as an ECS secret and ``LINEAGE_INVOCATION``, which the launch fills. With
any of them missing nothing is read or sent.

- After a successful ``pin``: the task's START (``start_lines``).
- After a successful ``upload``: one START and one COMPLETE per published table (``table_lines``), built by the same code as the Airflow evidence step's
  (``ecs_lineage.layer_table_events``) from the run folder that the upload just copied to S3: the pin file, the output record and the per-table inputs
  record. The versions are the snapshots the job read and published. With the identity of ``LINEAGE_INVOCATION`` and the two times the job writes into
  its run folder (``stamp_times``: the task start by the pin, the time the upload began by the upload), which the upload copies to S3 where the evidence
  step reads them (``ecs_lineage.job_times``), the events have the same run IDs, job names, times and bytes as the evidence step's, so an event sent from
  here and one sent from Airflow for the same table are the same event and OpenMetadata keeps one edge. The only events that differ in content are the
  task's START (the job sends it before the outcome of the run is known, so its run facet says not_applicable where the replay's says complete) and the
  task's FAIL and terminal events (the job's clock at the failure against the launch records' end time).
- After a failed ``pin`` or ``upload``: the task's FAIL (``failure_lines``). A job that is killed sends nothing; the Airflow side sends its FAIL from the
  launch records. OpenMetadata 2.0.7 writes nothing for an event without both inputs and outputs, so START and FAIL say what happened and draw no edge;
  only a table's COMPLETE does.

The job reads only its own run folder: a record that is not there is absent, and any other failure of a read (permissions, a directory in its place) skips
the send with the reason on standard error. ``emit`` runs the building and the sending of the events inside one absolute deadline.

Sending is bounded and never fails the job (``emit``): every failure, including a bug of this module, is one line on standard error without the credential.
It also never makes a succeeding job late: ``LINEAGE_INVOCATION`` carries the absolute launch deadline, and a send happens only when it can use its whole
budget and ``RESERVE_SECONDS`` remain after it (``delivery_budget``); otherwise nothing is sent and the line on standard error says why. The Airflow side
sends the same events later.

The job's image has no ``jsonschema``: the events are not checked against the OpenLineage schema here (the cases check them), and the records are checked
by every rule but their closed schemas (``JOB_CHECKS``). The job does not read the input run's evidence again, because its pin phase compared the pins
with it.

This module imports nothing from Airflow, from ``scripts`` or from ``jsonschema``.
"""

from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from orchestration import ecs_lineage
from orchestration.ecs_lineage import EVIDENCE_BYTES, DeliveryFailed, EvidenceUnavailable, JobLineage, LaunchFacts, LineageRefused, S3Evidence
from orchestration.snapshots import SnapshotError, checked_input_record, checked_snapshot_record

INVOCATION_VARIABLE = "LINEAGE_INVOCATION"
START_SECONDS = 10.0  # the whole send of the task's START or FAIL, builder included
TABLE_SECONDS = 120.0  # the whole send of a layer's per-table events, builder included: about 140 events at 0.22 s each
STOP_GRACE_SECONDS = 1.0  # how long the caller waits, after it stopped the sending, for a send in flight to end
RESERVE_SECONDS = 1800.0  # time a send never uses: the phases after the upload (gold maintenance, features Feast), the scratch wipe, the launcher's poll
JOB_CHECKS = ecs_lineage.Checks(inputs=checked_input_record, outputs=checked_snapshot_record, frozen=False)
DAG_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,249}")  # the patterns of data_contracts/orchestration/submission_request.schema.json
DAG_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:+~=-]{0,249}")
TEMPLATE = re.compile(r"[a-z][a-z0-9_]{1,40}")
INPUT_RUN = re.compile(r"latest|[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
UNSET_SETS = ("", "base")  # the feature sets that run in the features family itself


@dataclass(frozen=True)
class JobRun:
    """One layer job's run as its phases know it: the layer, the run, the feature set, the run folder, the identity the launch gave (the invocation the evidence
    step also builds), the input run bound at launch and the time the task started."""

    layer: str
    run: str
    feature_set: str
    folder: Path
    invocation: dict[str, Any]
    input_run: str
    started_at: str
    deadline: float = 0.0  # the absolute launch deadline, epoch seconds: the job must have ended by then

    @property
    def model(self) -> str:
        """The model set whose family this is, else empty."""
        return "" if self.feature_set in UNSET_SETS else self.feature_set

    @property
    def family(self) -> str:
        """The host family whose declared lineage this job has: a model set's own family, else the layer."""
        return f"{self.layer}_{self.model}" if self.model else self.layer


def parse_invocation(text: str) -> dict[str, Any]:
    """The fields of ``LINEAGE_INVOCATION`` (``ecs_lineage.invocation_text``), exactly those: the identity, each field in the form the submission request
    allows, the input run bound at launch and the absolute launch deadline."""
    try:
        found = json.loads(text, object_pairs_hook=ecs_lineage.unique_object)
    except (ValueError, RecursionError):
        raise LineageRefused(f"{INVOCATION_VARIABLE} is not JSON") from None
    fields = {*ecs_lineage.INVOCATION_FIELDS, "input_run", "deadline_epoch"}
    if not isinstance(found, dict) or set(found) != fields:
        raise LineageRefused(f"{INVOCATION_VARIABLE} does not have exactly the fields {sorted(fields)}")
    attempt = found["attempt"]
    texts = {"template": TEMPLATE, "dag_id": DAG_ID, "dag_run_id": DAG_RUN_ID, "input_run": INPUT_RUN}
    if any(not isinstance(found[name], str) or not pattern.fullmatch(found[name]) for name, pattern in texts.items()):
        raise LineageRefused(f"{INVOCATION_VARIABLE} holds a template, DAG ID, DAG run ID or input run in a form the submission request does not allow")
    if found["task_id"] != ecs_lineage.JOB_TASK:
        raise LineageRefused(f"{INVOCATION_VARIABLE} names task {found['task_id']!r}, not the job task {ecs_lineage.JOB_TASK!r}")
    if not isinstance(attempt, int) or isinstance(attempt, bool) or not 1 <= attempt <= 99:
        raise LineageRefused(f"{INVOCATION_VARIABLE} holds an attempt that is not a whole number from 1 to 99")
    deadline = found["deadline_epoch"]
    if not isinstance(deadline, int) or isinstance(deadline, bool) or not 0 < deadline < 2**33:
        raise LineageRefused(f"{INVOCATION_VARIABLE} holds a launch deadline that is not a whole number of epoch seconds")
    return dict(found)


def job_run(text: str, layer: str, run: str, feature_set: str, folder: Path, started_at: str) -> JobRun:
    """The run of a layer job from its settings. Anything the launch or the driver could not have set is refused by name."""
    if layer not in ecs_lineage.TABLE_EVENT_JOBS:
        raise LineageRefused(f"{layer} is not a layer job that sends table events")
    if not ecs_lineage.RUN_ID.fullmatch(run):
        raise LineageRefused(f"{run!r} is not a run ID")
    sets = json.loads((ecs_lineage.ROOT / ecs_lineage.FEATURE_SCHEMA_FILE).read_text())["model_sets"]
    if feature_set not in (*UNSET_SETS, *sets) or (feature_set and layer != "features"):
        raise LineageRefused(f"{feature_set!r} is not a feature set of the {layer} job")
    found = parse_invocation(text)
    invocation = {
        "request_version": 2,
        "template": found["template"],
        "run_id": run,
        "mode": ecs_lineage.MODE,
        "attempt": found["attempt"],
        "dag_id": found["dag_id"],
        "task_id": found["task_id"],
        "dag_run_id": found["dag_run_id"],
    }
    return JobRun(layer, run, feature_set, folder, invocation, found["input_run"], started_at, float(found["deadline_epoch"]))


def delivery_budget(cap: float, left: float, *, failed: bool = False) -> tuple[float, str]:
    """The seconds a send may take and, when it may take none, why. ``left`` is the time before the earlier of the launch's absolute deadline and the task's
    own. A job that is still succeeding sends only when ``RESERVE_SECONDS`` remain after the whole of ``cap``, so no send can push it past the launcher's
    deadline, where the launcher stops the task and records a timeout for work that finished. A job that has already failed has no outcome left to lose: its
    FAIL gets what remains up to ``cap``."""
    if failed:
        return (min(cap, left), "") if left > 0 else (0.0, f"the deadline has passed ({left:.0f} s left)")
    if left < cap + RESERVE_SECONDS:
        need = f"{cap + RESERVE_SECONDS:.0f} s (its {cap:.0f} s and a reserve of {RESERVE_SECONDS:.0f} s)"
        return 0.0, f"only {max(left, 0):.0f} s remain before the deadline and a send needs {need}"
    return cap, ""


def stamp_times(folder: Path, run_id: str, *, started_at: str | None = None, completed_at: str | None = None) -> dict[str, Any]:
    """Write the run's lineage times (``ecs_lineage.TIMES_FILE``) into its run folder and return the record. Each time is written once and never changed: the
    task start by the pin, the time the upload began by the upload, so a repeated phase keeps the first. The upload copies the record to S3, where the
    evidence step reads the same two times that the job sends its events with."""
    path = folder / ecs_lineage.TIMES_FILE
    record: dict[str, Any] = {"schema": 1, "run_id": run_id}
    try:
        stored = path.read_bytes()
    except FileNotFoundError:
        stored = None
    except OSError as error:
        raise EvidenceUnavailable(f"{path.name}: {type(error).__name__}") from None
    if stored is not None:
        record = ecs_lineage.parse_record(path.name, stored)
        if not isinstance(record, dict) or record.get("schema") != 1 or record.get("run_id") != run_id:
            raise LineageRefused(f"{path.name}: is not the record of run {run_id}")
    for name, value in (("started_at", started_at), ("completed_at", completed_at)):
        if value is not None and name not in record:
            record[name] = value
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".part")
    temporary.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)
    return record


def facts(run: JobRun, ended_at: str, outcome: str) -> LaunchFacts:
    """What the launch records would say of this attempt, from what the job knows: the same fields, the task's start and the time the job ended."""
    return LaunchFacts(run.run, run.layer, int(run.invocation["attempt"]), run.started_at, ended_at, outcome, run.input_run, run.model)


class FolderEvidence(S3Evidence):
    """The run's own records as the job has them in its run folder, read under the keys the evidence step reads them by. The upload phase copies the folder to
    those keys, so the folder holds the same bytes. A key of another run or record is not the job's to read: it is refused, never fetched."""

    def __init__(self, run: JobRun, inputs: bytes | None) -> None:
        super().__init__(None, "")
        self.prefix = f"{ecs_lineage.EVIDENCE_PREFIX}/{run.layer}/{run.run}/"
        self.inputs_key = ecs_lineage.INPUTS_KEY.format(layer=run.layer, run=run.run)
        self.folder, self.inputs = run.folder, inputs

    def read(self, key: str) -> Any:
        if key == self.inputs_key:
            return None if self.inputs is None else ecs_lineage.parse_record(key, self.inputs)
        relative = Path(key.removeprefix(self.prefix))
        if not key.startswith(self.prefix) or relative.is_absolute() or ".." in relative.parts:
            raise EvidenceUnavailable(f"{key}: a job reads only the records of its own run")
        try:
            with (self.folder / relative).open("rb") as handle:
                body = handle.read(EVIDENCE_BYTES + 1)
        except FileNotFoundError:
            return None  # the only absence: a file that is not there. A permission error, a directory or any other failure is an error, never an absent record
        except OSError as error:
            raise EvidenceUnavailable(f"{key}: {type(error).__name__}") from None
        return ecs_lineage.parse_record(key, body)


def task_line(run: JobRun, ended_at: str, outcome: str, state: str, refusal: str | None) -> str:
    """The task's own START (for a task that has not ended) or terminal event: the run facet and no dataset, under the run ID of the Airflow task."""
    found = facts(run, ended_at, outcome)
    launch = ecs_lineage.launch_facet(run.invocation, found, state, [], refusal)
    pair = ecs_lineage.events(JobLineage([], [], launch), run.invocation, found)
    return ecs_lineage.canonical(pair[0] if outcome == "running" else pair[1])


def start_lines(run: JobRun) -> list[str]:
    return [task_line(run, run.started_at, "running", "not_applicable", None)]


def failure_lines(run: JobRun, phase: str, ended_at: str) -> list[str]:
    return [task_line(run, ended_at, "failed", "missing", f"the job's {phase} phase failed")]


def table_lines(run: JobRun, ended_at: str, inputs: bytes | None, declared: Mapping[str, Any] | None = None) -> list[str]:
    """The START and COMPLETE of each published table, as canonical JSON lines. ``inputs`` is the per-table inputs record the upload wrote beside the dbt copy
    (None when it wrote none, which refuses the build)."""
    lineage = ecs_lineage.declared_lineage(run.family) if declared is None else declared
    found = ecs_lineage.layer_table_events(FolderEvidence(run, inputs), lineage, run.invocation, facts(run, ended_at, "succeeded"), JOB_CHECKS)
    return [ecs_lineage.canonical(event) for event in found]


def settings(environ: Mapping[str, str]) -> tuple[str, str, str] | None:
    """The receiver address, the credential and the identity, or None unless all of them are set and the transport is an HTTP one."""
    target = ecs_lineage.http_lineage(environ)
    text = environ.get(INVOCATION_VARIABLE, "")
    return (target[0], target[1], text) if target and text else None


def note(what: str, message: str) -> None:
    """One line on standard error. Nothing here may hold the credential: callers pass the kind of a failure or the text of an error that names none."""
    sys.stderr.write(f"job_lineage: {what}: {' '.join(message.split())[:300]}\n")


def explain(error: Exception) -> str:
    """The one line for a failed send. Only the kinds of failure whose texts name a key, a rule or a kind of failure show their text, never the credential's."""
    if isinstance(error, (DeliveryFailed, LineageRefused, EvidenceUnavailable, SnapshotError)):
        return f"{type(error).__name__}: {error}"
    return type(error).__name__  # the text of an unexpected error can hold a header


def send_until(lines: list[str], url: str, credential: str, deadline: float, cancel: ecs_lineage.Cancellation) -> None:
    """Send each line in order, every one only before the absolute ``deadline`` (a ``time.monotonic`` time, never a budget that starts when the send does) and
    only while ``cancel`` has not stopped the sending. Raises ``DeliveryFailed`` for the first failure, the deadline and a stop."""
    for line in lines:
        cancel.check()
        left = deadline - time.monotonic()
        if left <= 0:
            raise DeliveryFailed("the deadline passed with events unsent")
        ecs_lineage.post_event(url, line.encode(), credential, timeout=min(ecs_lineage.DELIVERY_SECONDS, left), cancel=cancel)


def emit(what: str, build: Callable[[], list[str]], environ: Mapping[str, str], seconds: float) -> int:
    """Build the events and send them, all inside one absolute deadline of ``seconds``, and return how many were sent. Nothing is built or sent unless the
    settings are all there. The building (the manifest read, the evidence reads, the events) and the sending run together in a worker, and the deadline is a
    ``time.monotonic`` time that the worker keeps from its start to every send. At the deadline the caller stops the sending (``ecs_lineage.Cancellation``)
    before it returns: a worker that is still building, or was paused after its last check, finds the stop at its next send and sends nothing, and a send in
    flight is aborted, with ``STOP_GRACE_SECONDS`` to end. A thread cannot be stopped while it builds, so the caller does not wait for that and the process
    ends the worker when the phase ends. No event is sent after the caller returns. Any failure (the receiver, the evidence, a bug, the deadline) is one line on
    standard error and a count of 0; nothing is raised, so the job's exit code never changes."""
    target = settings(environ)
    if target is None or seconds <= 0:
        return 0
    deadline = time.monotonic() + seconds
    cancel = ecs_lineage.Cancellation()
    outcome: dict[str, Any] = {}

    def work() -> None:
        try:
            lines = build()
            outcome["sending"] = True
            send_until(lines, target[0], target[1], deadline, cancel)
            outcome["sent"] = len(lines)
        except Exception as error:  # nothing of the worker may reach standard error on its own
            outcome["error"] = error

    worker = threading.Thread(target=work, daemon=True)
    worker.start()
    worker.join(max(deadline - time.monotonic(), 0.0))
    if worker.is_alive():
        cancel.stop()
        if outcome.get("sending"):
            worker.join(STOP_GRACE_SECONDS)
        note(what, f"not sent: the deadline of {seconds:g} s passed while the events were built or sent")
        return 0
    cancel.stop()
    if "error" in outcome:
        note(what, f"not sent: {explain(outcome['error'])}")
        return 0
    note(what, f"sent {outcome['sent']} events")
    return int(outcome["sent"])
