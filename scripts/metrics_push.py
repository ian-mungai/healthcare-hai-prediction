"""Push a job's own metrics to the Pushgateway on the host, from the job process, and never let a push change the job's result.

Every ECS job entry point runs its ``main`` through ``resource_peaks.recorded``, which calls this module. A job pushes three times: a start and a heartbeat
every ``HEARTBEAT_SECONDS`` while it runs (group outcome ``running``), and an exit push (group outcome ``succeeded``, ``failed`` or ``interrupted``) that also
deletes the running group. A job killed hard never reaches its exit push: its running group keeps the last heartbeat, which the alert rules read.

- The address is ``PUSHGATEWAY_URL`` (plain http, the host's private address) and the job family is ``HAI_JOB``. Without the address nothing is sent.
- The grouping keys come from closed sets: ``job`` (``JOB_PHASES``), ``phase`` (the phase word of the command line) and ``outcome`` (``OUTCOMES``). A run ID,
  a hospital or a key is never a label: a sample whose metric or label is not in ``CATALOG`` is dropped and named on standard error.
- Every request has a hard timeout and the exit push has a total deadline. A failure writes one ``event=metrics_push_failed`` line and returns False.
"""

from __future__ import annotations

import http.client
import math
import re
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from urllib.parse import quote, urlparse

URL_VARIABLE = "PUSHGATEWAY_URL"
JOB_VARIABLE = "HAI_JOB"
REQUEST_TIMEOUT = 3.0
TOTAL_TIMEOUT = 5.0
HEARTBEAT_SECONDS = 30.0
OUTCOMES = ("running", "succeeded", "failed", "interrupted")
FINAL_OUTCOMES = OUTCOMES[1:]
JOB_PHASES: dict[str, tuple[str, ...]] = {
    "bronze": ("pin", "load", "check", "inventory"),
    "silver": ("pin", "build", "validate", "verify", "export", "publish", "upload"),
    "gold": ("pin", "build", "validate", "export", "publish", "upload", "maintain"),
    "features": ("pin", "build", "validate", "export", "publish", "upload", "feast"),
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
LABEL_VALUES: dict[str, Callable[[str], bool]] = {
    "hai_type": lambda value: HAI_TYPE.fullmatch(value) is not None,
    "status": lambda value: value in ("checked", "skipped"),
    "feature_set": lambda value: SET_NAME.fullmatch(value) is not None,
    "mode": lambda value: value in ("load", "hold", "release", "offline"),
    "result": lambda value: WORD.fullmatch(value) is not None,
}
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
    "hai_promotion_result": ("1 for the result of the latest promotion phase.", ("result",)),
}
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
    first = next((argument for argument in argv[1:] if not argument.startswith("-")), None)
    return first if first in phases else None


class Pusher:
    """The pushes of one job process."""

    def __init__(self, url: str, job: str, phase: str, *, request_timeout: float | None = None, total_timeout: float | None = None) -> None:
        parsed = urlparse(url)
        if parsed.scheme != "http" or not parsed.hostname:
            raise ValueError("the address must be a plain http address with a host")
        if phase not in JOB_PHASES.get(job, ()):
            raise ValueError("the job or phase is not in the closed table")
        self.host, self.port, self.job, self.phase = parsed.hostname, parsed.port or 80, job, phase
        self.request_timeout, self.total_timeout = request_timeout, total_timeout
        self.started_at = time.time()
        self.running_sent = False
        self.closed = False
        self.heartbeat_failures = 0
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
            return cls(url, job, phase)
        except ValueError as error:
            line("metrics_push_skipped", reason=str(error))
            return None

    def path(self, outcome: str) -> str:
        return f"/metrics/job/{quote(self.job, safe='')}/phase/{quote(self.phase, safe='')}/outcome/{quote(outcome, safe='')}"

    def send(self, method: str, outcome: str, body: str, timeout: float, *, quiet: bool = False) -> bool:
        """One request; False and one named line on any failure, unless ``quiet``."""
        step = "delete" if method == "DELETE" else "put"

        def failed(reason: str) -> bool:
            if not quiet:
                line("metrics_push_failed", step=step, reason=reason)
            return False

        if timeout <= 0:
            return failed("the deadline passed")
        connection = None
        try:
            connection = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
            connection.request(
                method, self.path(outcome), body=body.encode("utf-8") if method == "PUT" else None, headers={"Content-Type": "text/plain; version=0.0.4"}
            )
            status = connection.getresponse().status
        except Exception as error:
            return failed(f"{type(error).__name__}: {error}")
        finally:
            if connection is not None:
                connection.close()
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
