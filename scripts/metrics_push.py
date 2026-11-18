"""Push a job's own metrics to the Pushgateway on the host, from the job process, and never let a push change the job's result.

Every ECS job entry point runs its ``main`` through ``resource_peaks.recorded``, which calls this module. A job pushes three times: a start and a heartbeat
every ``HEARTBEAT_SECONDS`` while it runs (group outcome ``running``), and an exit push (group outcome ``succeeded``, ``failed`` or ``interrupted``) that also
deletes the running group. A job killed hard never reaches its exit push: its running group keeps the last heartbeat, which the alert rules read.

- The address is ``PUSHGATEWAY_URL`` (plain http, the host's private address) and the job family is ``HAI_JOB``. Without the address nothing is sent.
- Tasks of one family that run more than once would replace each other's group, so the instance is a grouping key too: ``model_set`` (m1 to m6) for a features
  task of a set (``HAI_JOB_SET``) and a train task (the ``model`` of its ``TRAINING_PARAMETERS``), and ``job_type`` (a HAI type) for a promote task (its
  ``TRAINING_PARAMETERS``) and a serve task (its ``SERVING_REQUEST``). A value outside its closed set adds no key.
- The grouping keys come from closed sets: ``job`` (``JOB_PHASES``), ``phase`` (the phase word of the command line) and ``outcome`` (``OUTCOMES``). A run ID,
  a hospital or a key is never a label: a sample whose metric or label is not in ``CATALOG`` is dropped and named on standard error.
- Every request has one absolute deadline, enforced through the connect, the send and every read of the answer (an inactivity timeout alone lets a slow peer
  that keeps sending bytes hold the job), and the exit push has a total deadline over its requests. A failure writes one ``event=metrics_push_failed`` line
  and returns False. The address is an IP address, so no name lookup waits.
"""

from __future__ import annotations

import json
import math
import re
import socket
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from urllib.parse import quote, urlparse

URL_VARIABLE = "PUSHGATEWAY_URL"
JOB_VARIABLE = "HAI_JOB"
JOB_SET_VARIABLE = "HAI_JOB_SET"
MODEL_SETS = ("m1", "m2", "m3", "m4", "m5", "m6")
JOB_TYPES = tuple(f"HAI_{index}" for index in range(1, 7))
# The families whose tasks run once per model set, per model or per HAI type take that value as one more grouping key. Each entry: the variable that holds the
# task's JSON parameters, the field of that JSON and the grouping key.
INSTANCE_PARAMETERS = {
    "train": ("TRAINING_PARAMETERS", "model", "model_set"),
    "promote": ("TRAINING_PARAMETERS", "hai_type", "job_type"),
    "serve": ("SERVING_REQUEST", "hai_type", "job_type"),
}
INSTANCE_VALUES = {"model_set": MODEL_SETS, "job_type": JOB_TYPES}
REQUEST_TIMEOUT = 3.0
TOTAL_TIMEOUT = 5.0
STATUS_LINE_LIMIT = 8192
HEARTBEAT_SECONDS = 30.0
OUTCOMES = ("running", "succeeded", "failed", "interrupted")
FINAL_OUTCOMES = OUTCOMES[1:]
JOB_PHASES: dict[str, tuple[str, ...]] = {
    "bronze": ("pin", "load", "check", "inventory", "spark", "stage"),
    "catalog_setup": ("catalog", "principal", "check", "inspect"),
    "silver": ("pin", "build", "validate", "verify", "export", "publish", "upload"),
    "gold": ("pin", "build", "validate", "export", "publish", "upload", "maintain"),
    "features": ("pin", "build", "validate", "export", "publish", "upload", "feast", "reload"),
    "train": ("prepare", "extract", "train", "research"),
    "promote": ("promote", "canary", "notify"),
    "drift": ("baseline", "current", "check", "publish"),
    "serve": ("serve",),
    "metadata": ("ingest",),
    "publisher": ("run",),
}
HAI_TYPE = re.compile(r"HAI_[1-6]")
WORD = re.compile(r"[a-z][a-z_]{0,23}")
SET_NAME = re.compile(r"[a-z][a-z0-9_]{0,15}")
SLOT = re.compile(r"[a-z][a-z0-9_]{0,30}")
DATA_SOURCE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,39}")
LABEL_VALUES: dict[str, Callable[[str], bool]] = {
    "hai_type": lambda value: HAI_TYPE.fullmatch(value) is not None,
    "status": lambda value: value in ("checked", "skipped", "success", "error", "pass", "fail", "warn"),
    "kind": lambda value: value in ("silver", "gold", "cross", "features"),
    "source": lambda value: value in ("glue", "dbt", "raw_files"),
    "model": lambda value: value in ("m1", "m2", "m3", "m4", "m5", "m6"),
    "slot": lambda value: SLOT.fullmatch(value) is not None,
    "action": lambda value: value in ("promote", "rollback"),
    "decision": lambda value: value in ("promote", "hold"),
    "verdict": lambda value: value in ("promoted", "promoted_not_rolled_out", "held", "rolled_back"),
    "state": lambda value: (
        value
        in (
            "completed",
            "unchanged",
            "changed",
            "failed",
            "refused",
            "skipped_complete",
            "skipped_recent_stale",
            "paused_not_attempted",
            "manual_not_attempted",
            "keyed_not_attempted",
        )
    ),
    "run_kind": lambda value: value in ("bootstrap", "monthly", "retry"),
    "data_source": lambda value: DATA_SOURCE.fullmatch(value) is not None,
    "feature_set": lambda value: SET_NAME.fullmatch(value) is not None,
    "mode": lambda value: value in ("load", "hold", "release", "offline"),
    "result": lambda value: WORD.fullmatch(value) is not None,
    "reason": lambda value: value in ("no_load_evidence", "held_not_released"),
}
# Extra grouping keys of a group that one process pushes for each set, from closed patterns. The key is in the path, never in the body.
GROUP_KEYS: dict[str, Callable[[str], bool]] = {"feature_set": lambda value: SET_NAME.fullmatch(value) is not None}
COMMON = {
    "hai_job_start_timestamp_seconds": "Unix time the job process started.",
    "hai_job_heartbeat_timestamp_seconds": "Unix time of the latest heartbeat of the running job.",
    "hai_job_memory_current_bytes": "Memory the job container uses at the latest heartbeat.",
    "hai_job_memory_peak_bytes": "Peak memory of the job container so far, at the latest heartbeat.",
    "hai_job_scratch_peak_bytes": "Peak scratch use of the job so far, at the latest heartbeat.",
    "hai_job_finish_timestamp_seconds": "Unix time the job process ended.",
    "hai_job_duration_seconds": "Seconds from the start of the job process to its exit.",
    "hai_job_exit_code": "Exit code of the job process.",
    "hai_job_peak_memory_bytes": "Peak memory of the job container at exit.",
    "hai_job_peak_scratch_bytes": "Peak scratch use of the job at exit.",
}
CATALOG: dict[str, tuple[str, tuple[str, ...]]] = {
    **{name: (text, ()) for name, text in COMMON.items()},
    "hai_drift_status": ("1 for the status of the latest drift check of a HAI type.", ("hai_type", "status")),
    "hai_drift_share": ("Share of features that drifted in the latest drift check.", ("hai_type",)),
    "hai_drift_features_drifted": ("Number of features that drifted in the latest drift check.", ("hai_type",)),
    "hai_drift_triggered": ("1 when the latest drift check triggered retraining.", ("hai_type",)),
    "hai_drift_current_rows": ("Rows in the current window of the latest drift check.", ("hai_type",)),
    "hai_drift_unmonitored": ("1 when a HAI type has no Production baseline.", ("hai_type",)),
    "hai_feature_set_publish_timestamp_seconds": ("Unix time of the latest publish of a feature set.", ("feature_set",)),
    "hai_feature_set_online_mode": ("1 for the online load mode of the latest feature set publish.", ("feature_set", "mode")),
    "hai_feature_reload_result": ("1 for the result of the latest online store reload of a feature set.", ("feature_set", "result")),
    "hai_feature_release_result": ("1 for the result of the latest release of a held online load of a feature set.", ("feature_set", "result")),
    "hai_online_reload_result": (
        "1 for the result of the latest online store reload of the set at host start: loaded, already_loaded, skipped or failed.",
        ("result",),
    ),
    "hai_online_reload_skip_reason": ("1 for the reason the latest online store reload of the set was skipped.", ("reason",)),
    "hai_online_reload_rows": ("Entities the latest online store reload of the set loaded.", ()),
    "hai_online_reload_duration_seconds": ("Seconds the latest online store reload of the set took.", ()),
    "hai_online_reload_timestamp_seconds": ("Unix time the latest online store reload of the set ended.", ()),
    "hai_spark_stages_active": ("Spark stages that run now in the bronze session.", ()),
    "hai_spark_tasks_running": ("Spark tasks that run now across the active stages.", ()),
    "hai_spark_tasks_completed": ("Spark tasks completed in the bronze session so far.", ()),
    "hai_spark_tasks_failed": ("Spark tasks that failed in the bronze session so far.", ()),
    "hai_spark_jobs_running": ("Spark jobs that run now in the bronze session.", ()),
    "hai_spark_jobs_completed": ("Spark jobs that succeeded in the bronze session so far.", ()),
    "hai_spark_jobs_failed": ("Spark jobs that failed in the bronze session so far.", ()),
    "hai_spark_poll_timestamp_seconds": ("Unix time of the latest read of the Spark status tracker.", ()),
    "hai_dbt_models": ("Models of the latest dbt run of the layer, by status.", ("status",)),
    "hai_dbt_tests": ("Tests of the latest dbt run of the layer, by status.", ("status",)),
    "hai_quality_checks_run": ("Quality checks the layer's gate ran, by gate.", ("kind",)),
    "hai_quality_checks_failed": ("Quality checks of the gate that failed.", ("kind",)),
    "hai_quality_checks_warned": ("Quality checks of the gate that warned.", ("kind",)),
    "hai_publish_tables": ("Tables the latest publish of the layer committed.", ()),
    "hai_publish_rows": ("Rows in the tables of the latest publish of the layer.", ()),
    "hai_publish_tables_changed": ("Tables of the latest publish of the layer whose snapshot moved, a new table included.", ()),
    "hai_publish_tables_empty": ("Tables of the latest publish of the layer with no rows.", ()),
    "hai_publish_commit_timestamp_seconds": ("Unix time the latest publish of the layer was committed.", ()),
    "hai_openmetadata_ingest_entities": ("Entities the latest OpenMetadata ingestion of the source ingested.", ("source",)),
    "hai_openmetadata_ingest_failures": ("Failures of the source and the sink in the latest OpenMetadata ingestion of the source.", ("source",)),
    "hai_training_holdout_rmse": ("Holdout RMSE of a model slot of a type in the latest fit.", ("hai_type", "model", "slot")),
    "hai_training_selected": ("1 for the model slot that the latest fit of a type selected.", ("hai_type", "model", "slot")),
    "hai_training_challenger_registered": ("1 when the latest fit of a model registered a candidate for the type.", ("hai_type", "model")),
    "hai_research_candidate_rmse": ("Holdout RMSE of a candidate model in the latest research of a type.", ("hai_type", "model")),
    "hai_research_eligible_candidates": ("Candidates of the latest research of a type that are eligible.", ("hai_type",)),
    "hai_research_challenger": ("1 when the latest research of a type chose a challenger.", ("hai_type",)),
    "hai_research_challenger_model": ("1 for the model that the latest research chose as challenger.", ("hai_type", "model")),
    "hai_research_production_version": ("The Production version of the type's registered model at its latest research.", ("hai_type",)),
    "hai_canary_result": ("1 for the decision of the latest canary of a type.", ("hai_type", "decision")),
    "hai_promotion_executed": ("1 for the latest promotion or rollback executed for a type, by action.", ("hai_type", "action")),
    "hai_promotion_version": ("The version that the latest executed promotion or rollback made Production.", ("hai_type",)),
    "hai_promotion_notice": ("1 for the verdict of the latest owner notice about a type's challenger.", ("hai_type", "verdict")),
    "hai_publisher_units": ("Units of the latest publisher run by state.", ("state",)),
    "hai_publisher_files_fetched": ("Files the latest publisher run fetched (completed, unchanged or changed units).", ()),
    "hai_publisher_files_missing": ("Files the latest publisher run could not capture (failed, refused or not attempted units), by run kind.", ("run_kind",)),
    "hai_publisher_bytes_downloaded": ("Bytes the latest publisher run downloaded, which is the bytes it stored.", ()),
    "hai_publisher_http_requests": ("HTTP requests the latest publisher run made.", ()),
    "hai_publisher_objects_created": ("Objects the latest publisher run created in the capture root.", ()),
    "hai_publisher_evidence_problems": ("Problems the evidence check found in the latest publisher run.", ()),
    "hai_publisher_source_last_success_timestamp_seconds": (
        "Unix time of the latest publisher run that fetched the data source with no unit failed.",
        ("data_source",),
    ),
    "hai_publisher_source_units_unfinished": ("Units of the data source that failed or were refused in the latest publisher run.", ("data_source",)),
    "hai_promotion_result": ("1 for the result of the latest promotion phase.", ("result",)),
}
# The metrics that a process pushes in a group with extra grouping keys: the labels the group adds to each series.
GROUP_LABELS: dict[str, tuple[str, ...]] = {name: ("feature_set",) for name in CATALOG if name.startswith("hai_online_reload_")}
PENDING: list[Sample] = []
PENDING_LOCK = threading.Lock()


@dataclass(frozen=True)
class Sample:
    name: str
    value: float
    labels: Mapping[str, str] = field(default_factory=dict)


def add(name: str, value: float, **labels: str) -> None:
    """Register a family metric that the exit push of this process sends. A bad sample is dropped at the push, never here, so a job cannot fail on it."""
    with PENDING_LOCK:
        PENDING[:] = [sample for sample in PENDING if not (sample.name == name and dict(sample.labels) == labels)]
        PENDING.append(Sample(name, value, dict(labels)))


def drain() -> list[Sample]:
    with PENDING_LOCK:
        taken = list(PENDING)
        PENDING.clear()
    return taken


def line(event: str, **fields: str) -> None:
    """One line on standard error that cannot raise; the text is cut at one line."""
    text = " ".join(f"{key}={str(value).splitlines()[0] if str(value) else '-'}" for key, value in fields.items())
    try:
        sys.stderr.write(f"event={event} {text}\n")
        sys.stderr.flush()
    except Exception:
        return


def number(value: float) -> str:
    return str(int(value)) if float(value).is_integer() and abs(value) < 1e15 else repr(float(value))


def problem(sample: Sample) -> str | None:
    """Why a sample may not be sent, or None. Label values are never part of the reason."""
    entry = CATALOG.get(sample.name)
    if entry is None:
        return "the metric is not in the catalog"
    if set(sample.labels) != set(entry[1]):
        return f"the labels are not {list(entry[1])}"
    for label, value in sample.labels.items():
        if not isinstance(value, str) or not LABEL_VALUES[label](value):
            return f"the value of label {label} is not in its closed set"
    if isinstance(sample.value, bool) or not isinstance(sample.value, (int, float)) or not math.isfinite(sample.value):
        return "the value is not a finite number"
    return None


def exposition(samples: Sequence[Sample]) -> str:
    """The text exposition of the samples, one HELP and TYPE pair per metric in first-seen order."""
    order: dict[str, list[str]] = {}
    for sample in samples:
        labels = ",".join(f'{key}="{sample.labels[key]}"' for key in CATALOG[sample.name][1])
        order.setdefault(sample.name, []).append(f"{sample.name}{{{labels}}} {number(sample.value)}" if labels else f"{sample.name} {number(sample.value)}")
    lines: list[str] = []
    for name, rows in order.items():
        lines += [f"# HELP {name} {CATALOG[name][0]}", f"# TYPE {name} gauge", *rows]
    return "\n".join(lines) + "\n"


def phase_of(job: str, argv: Sequence[str]) -> str | None:
    """The phase of a job command: its first argument that is not an option when the family has several phases, or its only phase."""
    phases = JOB_PHASES[job]
    if len(phases) == 1:
        return phases[0]
    rest = list(argv[1:])
    while "--seconds" in rest:
        del rest[rest.index("--seconds") : rest.index("--seconds") + 2]
    first = next((argument for argument in rest if not argument.startswith("-")), None)
    return first if first in phases else None


def instance_of(job: str, environ: Mapping[str, str]) -> dict[str, str]:
    """The instance keys of this task: the set of a features task from ``HAI_JOB_SET`` (an explicit setting, so a bad value raises ValueError and the push is
    skipped by name), and the model or HAI type that the task's own parameters name for the train, promote and serve families. Parameters that cannot be read,
    or whose value is outside its closed set, add no key."""
    explicit = environ.get(JOB_SET_VARIABLE)
    if explicit:
        if job != "features" or explicit not in MODEL_SETS:
            raise ValueError(f"{JOB_SET_VARIABLE} is allowed only for the features family and must be one of m1 to m6")
        return {"model_set": explicit}
    source = INSTANCE_PARAMETERS.get(job)
    if source is None:
        return {}
    variable, field_name, key = source
    try:
        value = json.loads(environ.get(variable, "")).get(field_name)
    except (ValueError, AttributeError):
        return {}
    return {key: value} if isinstance(value, str) and value in INSTANCE_VALUES[key] else {}


def exchange(host: str, port: int, request: bytes, deadline: float) -> int:
    """Send one HTTP request and return the status of the answer, with one absolute monotonic deadline over the connect, the send and every read. Each
    socket operation gets only the time that is left, so a peer that answers one byte at a time ends at the deadline like a silent one. Only the status line
    is read."""

    def left() -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("timed out: the request deadline passed")
        return remaining

    with socket.create_connection((host, port), timeout=left()) as connection:
        view = memoryview(request)
        while view:
            connection.settimeout(left())
            view = view[connection.send(view) :]
        seen = b""
        while b"\r\n" not in seen and len(seen) < STATUS_LINE_LIMIT:
            connection.settimeout(left())
            chunk = connection.recv(1024)
            if not chunk:
                break
            seen += chunk
    parts = seen.split(b"\r\n", 1)[0].split(b" ", 2)
    if len(parts) < 2 or not parts[0].startswith(b"HTTP/") or not parts[1].isdigit():
        raise ValueError("the answer has no status line")
    return int(parts[1])


class Pusher:
    """The pushes of one job process."""

    def __init__(
        self,
        url: str,
        job: str,
        phase: str,
        *,
        request_timeout: float | None = None,
        total_timeout: float | None = None,
        instance: Mapping[str, str] | None = None,
        group: Mapping[str, str] | None = None,
    ) -> None:
        parsed = urlparse(url)
        if parsed.scheme != "http" or not parsed.hostname:
            raise ValueError("the address must be a plain http address with a host")
        if phase not in JOB_PHASES.get(job, ()):
            raise ValueError("the job or phase is not in the closed table")
        self.instance = dict(instance or {})
        if any(value not in INSTANCE_VALUES.get(key, ()) for key, value in self.instance.items()):
            raise ValueError("an instance key must be model_set (m1 to m6) or job_type (HAI_1 to HAI_6)")
        for key, value in (group or {}).items():
            if key not in GROUP_KEYS or not isinstance(value, str) or not GROUP_KEYS[key](value):
                raise ValueError("the extra grouping key or its value is not in its closed set")
        self.group = dict(group or {})
        self.host, self.port, self.job, self.phase = parsed.hostname, parsed.port or 80, job, phase
        self.request_timeout, self.total_timeout = request_timeout, total_timeout
        self.started_at = time.time()
        self.running_sent = False
        self.closed = False
        self.heartbeat_failures = 0
        self.sample_failures = 0
        self.lock = threading.Lock()

    @classmethod
    def create(cls, environ: Mapping[str, str], argv: Sequence[str]) -> Pusher | None:
        """A pusher for this process, or None when no address is set or the job or phase is not in the closed table (the reason is written, never raised)."""
        url = environ.get(URL_VARIABLE, "")
        if not url:
            return None
        job = environ.get(JOB_VARIABLE, "")
        if not job:
            line("metrics_push_skipped", reason=f"{JOB_VARIABLE} is not set")
            return None
        if job not in JOB_PHASES:
            line("metrics_push_skipped", reason=f"{JOB_VARIABLE} is not a known job family")
            return None
        phase = phase_of(job, argv)
        if phase is None:
            line("metrics_push_skipped", reason="the command has no known phase of this family")
            return None
        try:
            return cls(url, job, phase, instance=instance_of(job, environ))
        except ValueError as error:
            line("metrics_push_skipped", reason=str(error))
            return None

    def path(self, outcome: str) -> str:
        base = f"/metrics/job/{quote(self.job, safe='')}/phase/{quote(self.phase, safe='')}/outcome/{quote(outcome, safe='')}"
        extra = "".join(f"/{quote(key, safe='')}/{quote(value, safe='')}" for key, value in self.group.items())
        return base + "".join(f"/{key}/{quote(value, safe='')}" for key, value in sorted(self.instance.items())) + extra

    def send(self, method: str, outcome: str, body: str, timeout: float, *, quiet: bool = False) -> bool:
        """One request; False and one named line on any failure, unless ``quiet``."""
        step = "delete" if method == "DELETE" else "put"

        def failed(reason: str) -> bool:
            if not quiet:
                line("metrics_push_failed", step=step, reason=reason)
            return False

        if timeout <= 0:
            return failed("the deadline passed")
        payload = body.encode("utf-8") if method == "PUT" else b""
        head = (
            f"{method} {self.path(outcome)} HTTP/1.1\r\nHost: {self.host}:{self.port}\r\nContent-Type: text/plain; version=0.0.4\r\n"
            f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n"
        )
        try:
            status = exchange(self.host, self.port, head.encode("ascii") + payload, time.monotonic() + timeout)
        except Exception as error:
            return failed(f"{type(error).__name__}: {error}")
        return failed(f"HTTP {status}") if status >= 300 else True

    def clean(self, samples: Sequence[Sample]) -> list[Sample]:
        kept = []
        for sample in samples:
            reason = problem(sample)
            if reason is None:
                kept.append(sample)
            else:
                line("metrics_push_dropped", metric=sample.name, reason=reason)
        return kept

    def running(self, memory_current: float | None, memory_peak: float | None, scratch_peak: float | None) -> list[Sample]:
        values = {
            "hai_job_start_timestamp_seconds": self.started_at,
            "hai_job_heartbeat_timestamp_seconds": time.time(),
            "hai_job_memory_current_bytes": memory_current,
            "hai_job_memory_peak_bytes": memory_peak,
            "hai_job_scratch_peak_bytes": scratch_peak,
        }
        return [Sample(name, value) for name, value in values.items() if value is not None]

    def heartbeat(
        self, *, memory_current: float | None = None, memory_peak: float | None = None, scratch_peak: float | None = None, first: bool = False
    ) -> bool:
        """Replace the running group with the latest values. The first failure of a run is logged; later heartbeat failures are not."""
        timeout = self.request_timeout if self.request_timeout is not None else REQUEST_TIMEOUT
        if not self.lock.acquire(timeout=timeout):
            return False
        try:
            if self.closed:
                return False
            samples = self.clean(self.running(memory_current, memory_peak, scratch_peak))
            ok = self.send("PUT", "running", exposition(samples), timeout, quiet=bool(self.heartbeat_failures) and not first)
            self.heartbeat_failures = 0 if ok else self.heartbeat_failures + 1
            self.running_sent = self.running_sent or ok
            return ok
        except Exception as error:
            line("metrics_push_failed", step="put", reason=f"{type(error).__name__}: {error}")
            return False
        finally:
            self.lock.release()

    def budget(self) -> float:
        """The seconds that the pushes at the end of a process may take in all."""
        return self.total_timeout if self.total_timeout is not None else TOTAL_TIMEOUT

    def push_samples(self, outcome: str, samples: Sequence[Sample], *, within: float | None = None) -> bool:
        """Replace one group with the samples, for a process that pushes its own group (the Spark reporter). The first failure is logged, later ones are not.
        ``within`` shortens the request to the seconds that are left of a total budget."""
        if outcome not in OUTCOMES:
            line("metrics_push_skipped", reason="the outcome is not in the closed set")
            return False
        timeout = self.request_timeout if self.request_timeout is not None else REQUEST_TIMEOUT
        if within is not None:
            timeout = min(timeout, within)
        try:
            ok = self.send("PUT", outcome, exposition(self.clean(samples)), timeout, quiet=self.sample_failures > 0)
        except Exception as error:
            line("metrics_push_failed", step="put", reason=f"{type(error).__name__}: {error}")
            return False
        self.sample_failures = 0 if ok else self.sample_failures + 1
        return ok

    def delete_group(self, outcome: str, *, within: float | None = None) -> bool:
        """Delete one group, best effort. ``within`` shortens the request to the seconds that are left of a total budget."""
        timeout = self.request_timeout if self.request_timeout is not None else REQUEST_TIMEOUT
        if within is not None:
            timeout = min(timeout, within)
        return outcome in OUTCOMES and self.send("DELETE", outcome, "", timeout, quiet=self.sample_failures > 0)

    def start(self, *, memory_current: float | None = None, memory_peak: float | None = None, scratch_peak: float | None = None) -> bool:
        return self.heartbeat(memory_current=memory_current, memory_peak=memory_peak, scratch_peak=scratch_peak, first=True)

    def finish(
        self,
        outcome: str,
        *,
        exit_code: int,
        duration: float,
        peak_memory: float | None = None,
        peak_scratch: float | None = None,
        samples: Sequence[Sample] = (),
    ) -> bool:
        """Push the outcome group, then delete the running group when this process pushed one. True when the outcome group was accepted."""
        if outcome not in FINAL_OUTCOMES:
            line("metrics_push_skipped", reason="the outcome is not a final outcome")
            return False
        total = self.total_timeout if self.total_timeout is not None else TOTAL_TIMEOUT
        each = self.request_timeout if self.request_timeout is not None else REQUEST_TIMEOUT
        ends = time.monotonic() + total
        self.closed = True
        waited = self.lock.acquire(timeout=max(0.0, min(each, ends - time.monotonic())))
        try:
            values = {
                "hai_job_finish_timestamp_seconds": time.time(),
                "hai_job_duration_seconds": duration,
                "hai_job_exit_code": exit_code,
                "hai_job_peak_memory_bytes": peak_memory,
                "hai_job_peak_scratch_bytes": peak_scratch,
            }
            body = exposition(self.clean([*(Sample(name, value) for name, value in values.items() if value is not None), *samples]))
            if not self.send("PUT", outcome, body, min(each, ends - time.monotonic())):
                return False
            if self.running_sent or not waited:
                self.send("DELETE", "running", "", min(each, ends - time.monotonic()))
            return True
        except Exception as error:
            line("metrics_push_failed", step="put", reason=f"{type(error).__name__}: {error}")
            return False
        finally:
            if waited:
                self.lock.release()


class Heartbeat(threading.Thread):
    """Start push at once, then a heartbeat every ``HEARTBEAT_SECONDS``, until stopped. It never runs on the job's thread."""

    def __init__(self, pusher: Pusher, readings: Callable[[], Mapping[str, float | None]]) -> None:
        super().__init__(name="metrics_heartbeat", daemon=True)
        self.pusher, self.readings = pusher, readings
        self.stopped = threading.Event()

    def beat(self, first: bool) -> None:
        try:
            self.pusher.heartbeat(first=first, **self.readings())
        except Exception as error:
            line("metrics_push_failed", step="put", reason=f"{type(error).__name__}: {error}")

    def run(self) -> None:
        self.beat(True)
        while not self.stopped.wait(HEARTBEAT_SECONDS):
            self.beat(False)
