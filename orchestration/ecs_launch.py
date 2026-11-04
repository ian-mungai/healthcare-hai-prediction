"""Launch one batch job as one ECS task on the host and keep the guarantees the job broker gave.

A job attempt is named ``<run>:<job>:<attempt>``. The launcher writes the facts of an attempt as write-once records (``LaunchStore``): ``intent`` before the
RunTask call, ``started`` with the task ARN after it, ``end`` with the outcome. A record is never replaced, so a restart, a retry or a second caller reads the
same story. The calls are split so that an Airflow operator can use them as hooks (``attach``, ``start``, ``wait``, ``conclude``) and ``run`` chains them.

1. Adoption: an attempt with an intent and no ``started`` finds its task by identity (the adapter's ``startedBy`` tag) instead of launching again.
2. Reattach: a new attempt of the same run and job joins the live task of an earlier attempt and keeps its start time and deadline, only when the bound inputs
   (code digest, request hash and input run) equal those of the first attempt. Different bound inputs are refused. Nothing launches beside a live task, and a
   task whose state cannot be read blocks the launch. An earlier attempt that succeeded is adopted and never launched again.
3. The deadline counts from the recorded start of the task and is the recorded one, so it holds across restarts.
4. A task ends in success only when every acceptance container exited 0. The adapter already leaves out the guard container.
5. A stop is confirmed (the task reports STOPPED within 120 seconds) before anything is recorded; otherwise ``StopFailed`` leaves the stop repeatable.
6. A job that does not fit waits at most 10 minutes (the host's free CPU, memory and scratch, and RunTask capacity answers), then ends ``failed_closed`` with a
   fit report. There is no fallback to another capacity provider.

Every wait has a deadline. The library calls no process and starts no thread.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from botocore.exceptions import BotoCoreError, ClientError

from orchestration.ecs_adapter import HOST, EcsAdapter, TaskRefused, TaskReport, TaskUnavailable, classify, scrub

BOUND_FIELDS = ("code_digest", "request_hash", "input_run")
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
JOB = re.compile(r"[a-z][a-z0-9_]{1,40}")
EVENTS = ("intent", "started", "end")
OUTCOMES = frozenset({"succeeded", "failed", "timeout", "cancelled", "failed_closed"})
TIMEOUT_EXIT = 124
NO_CHECK_EXIT = 125
CANCEL_EXIT = 143
CLOSED_EXIT = 1
STOP_WAIT_SECONDS = 120.0
STOP_POLL_SECONDS = 2.0
FIT_WAIT_SECONDS = 600.0
FIT_POLL_SECONDS = 15.0
POLL_SECONDS = 6.0
POLL_FAILURES = 5
ABSENT_CODES = frozenset({"NoSuchKey", "AccessDenied"})
CONFIG_FILE = Path(__file__).resolve().parents[1] / "config" / "orchestration" / "ecs_launch.json"
PREFIX_PATTERN = re.compile(r"[a-z0-9_]+(/[a-z0-9_]+)*")
REPORT_LIMIT = 300
RETRYABLE_CODES = frozenset({"SlowDown", "ServiceUnavailable", "InternalError", "RequestTimeout", "ConditionalRequestConflict", "ThrottlingException"})
LIVE = ("pending", "running")


class LaunchError(Exception):
    """Base of the launch library's errors."""


class LaunchRefused(LaunchError):
    """The request cannot be launched as it stands."""


class LaunchUnavailable(LaunchError):
    """A later identical call can succeed. Nothing was launched beside a task that may be live."""


class StopFailed(LaunchError):
    """A stop was not confirmed. Nothing was recorded, so the stop can be repeated."""


class CleanupFailed(Exception):
    """A cleanup hook could not finish. The job outcome stays as recorded."""


@dataclass(frozen=True)
class Resources:
    """CPU units, memory and scratch bytes, needed or free."""

    cpu_units: int
    memory_mib: int
    scratch_bytes: int


@dataclass(frozen=True)
class JobSpec:
    """One job attempt. ``bound`` holds the code digest, request hash and input run that every attempt of a run and job must share. ``environment`` maps a
    container name to the variables it receives. ``acceptance`` names the containers that must exit 0."""

    run_id: str
    job: str
    attempt: int
    environment: Mapping[str, Mapping[str, str]]
    bound: Mapping[str, str]
    deadline_seconds: int
    need: Resources
    acceptance: tuple[str, ...] = ()
    log_container: str = ""
    log_prefix: str = ""
    log_lines: int = 50

    @property
    def key(self) -> str:
        return f"{self.run_id}:{self.job}:{self.attempt}"


@dataclass(frozen=True)
class Outcome:
    """How a job attempt ended. ``adopted`` is True when the result is an earlier attempt's recorded success. ``logs`` and ``cleanup_note`` come from the
    call that concluded the attempt and are not part of the recorded outcome."""

    outcome: str
    exit_code: int
    reason: str
    task_arn: str
    adopted: bool = False
    logs: str = field(default="", compare=False)
    cleanup_note: str = field(default="", compare=False)

    @property
    def ok(self) -> bool:
        return self.outcome == "succeeded"


@dataclass
class Clock:
    """Time and waiting, replaceable so a case can run ten minutes of waiting at once."""

    wall: Callable[[], float] = time.time
    monotonic: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep


class LaunchStore(Protocol):
    """Write-once launch records."""

    def put_once(self, record: Mapping[str, Any]) -> dict[str, Any]:
        """Store the record under its key unless the key exists. Return the record that stands: the new one, or the one that was there."""
        ...

    def records(self, run_id: str, job: str, attempts: Sequence[int]) -> list[dict[str, Any]]:
        """The records of the given attempts of the run and job, by attempt and then by event order. The store never lists: it reads known keys."""
        ...


class FitProbe(Protocol):
    """What the host has free now."""

    def free(self) -> Resources: ...


@dataclass
class Launch:
    """The state of one attempt while it is launched, watched and concluded."""

    spec: JobSpec
    arn: str | None = None
    started_at: float | None = None
    deadline_seconds: int = 0
    report: TaskReport | None = None
    timed_out: bool = False
    outcome: Outcome | None = None
    recovered_intent: bool = False


def launch_record_prefix() -> str:
    """The S3 prefix of the launch records, from ``config/orchestration/ecs_launch.json``: the one place the prefix is written for the code."""
    prefix = str(json.loads(CONFIG_FILE.read_text())["record_prefix"]).strip("/")
    if not PREFIX_PATTERN.fullmatch(prefix):
        raise LaunchRefused(f"{CONFIG_FILE.name}: record_prefix must be lowercase words joined by slashes")
    return prefix


def iso(seconds: float) -> str:
    return datetime.fromtimestamp(seconds, UTC).isoformat()


def stamp(text: str) -> float:
    return datetime.fromisoformat(text).timestamp()


def outcome_of(record: Mapping[str, Any], adopted: bool = False) -> Outcome:
    return Outcome(record["outcome"], record["exit_code"], record["reason"], record.get("task_arn", ""), adopted)


def parse_record(raw: bytes, where: str) -> dict[str, Any]:
    """A stored record as a validated dict. A record that does not parse or does not fit its event stops the launch with its key."""
    try:
        record = json.loads(raw)
    except ValueError:
        raise LaunchError(f"launch record {where} is not JSON") from None
    event = record.get("event") if isinstance(record, dict) else None
    needed: dict[str, type | tuple[type, ...]] = {"schema_version": int, "event": str, "run_id": str, "job": str, "attempt": int, "at": str}
    if event == "intent":
        needed |= {"bound": dict, "deadline_seconds": int, "acceptance": list, "need": dict}
    elif event == "started":
        needed |= {"task_arn": str, "job_started_at": str}
    elif event == "end":
        needed |= {"outcome": str, "exit_code": int, "reason": str}
    problem = next((name for name, kind in needed.items() if not isinstance(record.get(name) if isinstance(record, dict) else None, kind)), None)
    if event not in EVENTS or problem is not None or (event == "end" and record["outcome"] not in OUTCOMES) or record["schema_version"] != 1:
        raise LaunchError(f"launch record {where} is invalid ({problem or 'event or outcome'})")
    return dict(record)


class S3LaunchStore:
    """Launch records as write-once S3 objects: ``<prefix>/<run>/<job>/<attempt>/<event>.json``, with ``prefix`` from ``launch_record_prefix``.

    The writer's role grants put and get on that prefix and no listing. So every write sends ``IfNoneMatch="*"`` (a 412 means the record exists), nothing is
    deleted or overwritten, and only known keys are read. S3 answers a read of an absent key with AccessDenied when the caller cannot list, so a denied
    read counts as a missing record. A write that cannot be read back right after it succeeds means the read grant is missing, and it fails loudly, because
    every other absent record would then look missing too.
    """

    def __init__(self, client: Any, bucket: str, prefix: str) -> None:
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def key_of(self, run_id: str, job: str, attempt: int, event: str) -> str:
        return f"{self.prefix}/{run_id}/{job}/{attempt:02d}/{event}.json"

    def get(self, key: str) -> dict[str, Any] | None:
        try:
            body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code in ABSENT_CODES:
                return None
            raise self.refusal(code) from None
        except BotoCoreError as error:
            raise LaunchUnavailable(f"launch record store: {type(error).__name__}") from None
        return parse_record(body, key)

    @staticmethod
    def refusal(code: str) -> LaunchError:
        if code in RETRYABLE_CODES:
            return LaunchUnavailable(f"launch record store: {code}")
        return LaunchRefused(f"launch record store refused the call: {code or 'ClientError'}")

    def put_once(self, record: Mapping[str, Any]) -> dict[str, Any]:
        key = self.key_of(record["run_id"], record["job"], record["attempt"], record["event"])
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=json.dumps(record, sort_keys=True).encode(), IfNoneMatch="*")
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code != "PreconditionFailed":
                raise self.refusal(code) from None
            stored = self.get(key)
            if stored is None:
                raise LaunchRefused("a launch record exists but cannot be read: check that the role can read the launch record prefix") from None
            return stored
        except BotoCoreError:
            stored = self.get(key)
            if stored is None:
                raise LaunchUnavailable("launch record store: the write is not confirmed") from None
            return stored
        if self.get(key) is None:
            raise LaunchRefused("a launch record was written but cannot be read back: check that the role can read the launch record prefix")
        return dict(record)

    def records(self, run_id: str, job: str, attempts: Sequence[int]) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = []
        for attempt in attempts:
            intent = self.get(self.key_of(run_id, job, attempt, "intent"))
            if intent is not None:
                found.append(intent)
                found += [record for event in ("started", "end") if (record := self.get(self.key_of(run_id, job, attempt, event))) is not None]
        return found


class EcsFitProbe:
    """The free CPU and memory of the host's container instance with the most free memory, and the free scratch bytes from ``scratch_free``."""

    def __init__(self, ecs: Any, cluster: str, scratch_free: Callable[[], int]) -> None:
        self.ecs = ecs
        self.cluster = cluster
        self.scratch_free = scratch_free

    def free(self) -> Resources:
        try:
            arns = self.ecs.list_container_instances(cluster=self.cluster, status="ACTIVE")["containerInstanceArns"]
            instances = self.ecs.describe_container_instances(cluster=self.cluster, containerInstances=arns)["containerInstances"] if arns else []
        except (ClientError, BotoCoreError) as error:
            raise classify(error) from None
        best = (0, 0)
        for instance in instances:
            left = {item["name"]: int(item.get("integerValue", 0)) for item in instance.get("remainingResources", [])}
            best = max(best, (left.get("MEMORY", 0), left.get("CPU", 0)))
        return Resources(cpu_units=best[1], memory_mib=best[0], scratch_bytes=int(self.scratch_free()))


def shortfalls(need: Resources, free: Resources) -> list[str]:
    pairs = (
        ("cpu_units", need.cpu_units, free.cpu_units),
        ("memory_mib", need.memory_mib, free.memory_mib),
        ("scratch_bytes", need.scratch_bytes, free.scratch_bytes),
    )
    return [f"{name} needed {wanted} free {available}" for name, wanted, available in pairs if wanted > available]


class Launcher:
    """Launch, reattach, watch, stop and conclude job attempts."""

    def __init__(
        self,
        adapter: EcsAdapter,
        store: LaunchStore,
        probe: FitProbe,
        clock: Clock | None = None,
        cleanup: Callable[[JobSpec], None] | None = None,
    ) -> None:
        """``cleanup`` runs once after a successful job (the scratch wipe). It raises ``CleanupFailed`` when it cannot finish."""
        self.adapter = adapter
        self.store = store
        self.probe = probe
        self.clock = clock or Clock()
        self.cleanup = cleanup

    def validate(self, spec: JobSpec) -> None:
        """Refuse a malformed request before any record or cloud call."""
        problems = []
        if not RUN_ID.fullmatch(spec.run_id):
            problems.append("run_id")
        if not JOB.fullmatch(spec.job) or spec.job not in self.adapter.settings.families:
            problems.append("job")
        if not isinstance(spec.attempt, int) or not 1 <= spec.attempt <= 99:
            problems.append("attempt")
        if not isinstance(spec.deadline_seconds, int) or spec.deadline_seconds <= 0:
            problems.append("deadline_seconds")
        if set(spec.bound) != set(BOUND_FIELDS) or not all(isinstance(value, str) and value for value in spec.bound.values()):
            problems.append(f"bound (exactly {', '.join(BOUND_FIELDS)}, none empty)")
        if not all(
            isinstance(name, str) and all(isinstance(key, str) and isinstance(value, str) for key, value in values.items())
            for name, values in spec.environment.items()
        ):
            problems.append("environment")
        if not all(isinstance(value, int) and value >= 0 for value in (spec.need.cpu_units, spec.need.memory_mib, spec.need.scratch_bytes)):
            problems.append("need")
        if not all(isinstance(name, str) and name for name in spec.acceptance):
            problems.append("acceptance")
        if problems:
            raise LaunchRefused(f"request refused: {', '.join(problems)}")

    def require_bound(self, spec: JobSpec, records: list[dict[str, Any]]) -> None:
        """Every attempt of a run and job uses the bound inputs of its first attempt. A changed input needs a new run."""
        first = next((record for record in records if record["event"] == "intent"), None)
        if first is None:
            return
        for name in BOUND_FIELDS:
            if first["bound"].get(name) != spec.bound[name]:
                raise LaunchRefused(
                    f"run {spec.run_id} job {spec.job} is bound to the {name} of its first attempt ({str(first['bound'].get(name, ''))[:12]}), "
                    f"but this request has {spec.bound[name][:12]}: start a new run for a changed {name}"
                )

    def put(self, event: str, attempt: int, base: Mapping[str, Any], **fields: Any) -> dict[str, Any]:
        record = {"schema_version": 1, "event": event, "run_id": base["run_id"], "job": base["job"], "attempt": attempt, "at": iso(self.clock.wall()), **fields}
        return self.store.put_once(record)

    def poll(self, arn: str) -> TaskReport:
        try:
            return self.adapter.poll(arn)
        except TaskUnavailable as error:
            raise LaunchUnavailable(f"the task cannot be read now ({error})") from None

    def find(self, run_id: str, job: str, attempt: int) -> str | None:
        try:
            return self.adapter.find(f"{run_id}:{job}:{attempt}")
        except TaskUnavailable as error:
            raise LaunchUnavailable(f"the task of attempt {attempt} cannot be looked up now ({error})") from None

    def judge(self, report: TaskReport, acceptance: list[str] | tuple[str, ...]) -> tuple[str, int, str]:
        """The recorded outcome of a task that has ended: outcome, exit code and reason."""
        if report.state == "gone":
            return "failed_closed", CLOSED_EXIT, "task_not_found"
        if report.state == "succeeded":
            seen = dict(report.containers)
            missing = [name for name in acceptance if seen.get(name) != 0]
            if missing:
                return "failed", NO_CHECK_EXIT, f"acceptance_check_missing:{','.join(missing)}"
            return "succeeded", 0, report.reason
        return "failed", int(report.exit_code or CLOSED_EXIT), report.reason

    def attach(self, spec: JobSpec) -> Launch:
        """Validate the request and reconcile it with the records and the cloud: an ended attempt, an earlier attempt's success, the live task of this or an
        earlier attempt, or nothing yet. Nothing is launched here."""
        self.validate(spec)
        records = self.store.records(spec.run_id, spec.job, range(1, spec.attempt + 1))
        self.require_bound(spec, records)
        launch = Launch(spec)
        mine = {record["event"]: record for record in records if record["attempt"] == spec.attempt}
        if "end" in mine:
            launch.outcome = outcome_of(mine["end"])
            return launch
        if "started" in mine:
            launch.arn, launch.started_at = mine["started"]["task_arn"], stamp(mine["started"]["job_started_at"])
            launch.deadline_seconds = mine["intent"]["deadline_seconds"]
            return launch
        for attempt in sorted({record["attempt"] for record in records if record["attempt"] != spec.attempt}):
            self.consider_earlier(launch, {record["event"]: record for record in records if record["attempt"] == attempt})
            if launch.outcome is not None or launch.arn is not None:
                return launch
        launch.recovered_intent = "intent" in mine
        return launch

    def consider_earlier(self, launch: Launch, theirs: dict[str, dict[str, Any]]) -> None:
        """Join the live task of an earlier attempt, adopt its recorded or recovered success, or leave it when it failed or never started a task."""
        origin = theirs["intent"]
        attempt = origin["attempt"]
        if "end" in theirs:
            if theirs["end"]["outcome"] == "succeeded":
                launch.outcome = outcome_of(theirs["end"], adopted=True)
            return
        arn = theirs["started"]["task_arn"] if "started" in theirs else self.find(origin["run_id"], origin["job"], attempt)
        if arn is None:
            return
        report = self.poll(arn)
        if report.state in LIVE:
            began = theirs["started"]["job_started_at"] if "started" in theirs else origin["at"]
            self.join(launch, origin, arn, began)
            return
        outcome, code, reason = self.judge(report, origin["acceptance"])
        end = self.put("end", attempt, origin, outcome=outcome, exit_code=code, reason=reason[:REPORT_LIMIT], task_arn=arn)
        if end["outcome"] == "succeeded":
            launch.outcome = outcome_of(end, adopted=True)

    def join(self, launch: Launch, origin: Mapping[str, Any], arn: str, began: str) -> None:
        """Record this attempt as a reattachment to the live task of an earlier attempt, with that attempt's start time and deadline."""
        spec = launch.spec
        intent = self.put(
            "intent",
            spec.attempt,
            origin,
            bound=dict(spec.bound),
            deadline_seconds=origin["deadline_seconds"],
            acceptance=list(spec.acceptance),
            need=asdict(spec.need),
            reattached_from=origin["attempt"],
        )
        started = self.put("started", spec.attempt, origin, task_arn=arn, job_started_at=began)
        launch.arn, launch.started_at, launch.deadline_seconds = started["task_arn"], stamp(started["job_started_at"]), intent["deadline_seconds"]

    def start(self, launch: Launch) -> None:
        """Record the intent, check that the job fits, run the task once and record it. Does nothing when the attempt already has a task or an outcome."""
        if launch.outcome is not None or launch.arn is not None:
            return
        spec = launch.spec
        intent = self.put(
            "intent",
            spec.attempt,
            {"run_id": spec.run_id, "job": spec.job},
            bound=dict(spec.bound),
            deadline_seconds=spec.deadline_seconds,
            acceptance=list(spec.acceptance),
            need=asdict(spec.need),
        )
        launch.deadline_seconds = intent["deadline_seconds"]
        arn = self.find(spec.run_id, spec.job, spec.attempt) if launch.recovered_intent else None
        if arn is None:
            arn = self.run_when_it_fits(launch)
        if arn is not None:
            started = self.put("started", spec.attempt, intent, task_arn=arn, job_started_at=iso(self.clock.wall()))
            if started["task_arn"] != arn:
                raise LaunchRefused("another task is already recorded for this attempt")
            launch.arn, launch.started_at = arn, stamp(started["job_started_at"])

    def run_when_it_fits(self, launch: Launch) -> str | None:
        """Run the task once the host has room, waiting at most ``FIT_WAIT_SECONDS``. Ends the attempt ``failed_closed`` with the fit report otherwise."""
        spec = launch.spec
        end = self.clock.monotonic() + FIT_WAIT_SECONDS
        last = ""
        while True:
            try:
                free = self.probe.free()
            except TaskUnavailable as error:
                free, last = None, f"host capacity unreadable ({error})"
            except TaskRefused as error:
                self.close(launch, "failed_closed", CLOSED_EXIT, f"fit_probe_refused:{error}")
                return None
            short = shortfalls(spec.need, free) if free is not None else []
            if free is not None and short:
                last = "; ".join(short)
            elif free is not None:
                try:
                    return self.adapter.start(spec.job, spec.key, spec.environment, "", HOST)
                except TaskUnavailable as error:
                    last = f"RunTask answered {error}"
                except TaskRefused as error:
                    self.close(launch, "failed_closed", CLOSED_EXIT, f"launch_refused:{error}")
                    return None
            if self.clock.monotonic() >= end:
                self.close(launch, "failed_closed", CLOSED_EXIT, f"does_not_fit after {FIT_WAIT_SECONDS:g} s: {last}"[:REPORT_LIMIT])
                return None
            self.clock.sleep(FIT_POLL_SECONDS)

    def close(self, launch: Launch, outcome: str, code: int, reason: str) -> None:
        spec = launch.spec
        end = self.put("end", spec.attempt, {"run_id": spec.run_id, "job": spec.job}, outcome=outcome, exit_code=code, reason=reason, task_arn=launch.arn or "")
        launch.outcome = outcome_of(end)

    def wait(self, launch: Launch) -> None:
        """Poll the task until it ends or its recorded deadline passes. Past the deadline the task is stopped, with the stop confirmed."""
        if launch.outcome is not None or launch.arn is None or launch.started_at is None:
            return
        due = launch.started_at + launch.deadline_seconds
        failures = 0
        while True:
            try:
                report = self.adapter.poll(launch.arn)
            except TaskUnavailable as error:
                failures += 1
                if failures > POLL_FAILURES:
                    raise LaunchUnavailable(
                        f"the task could not be read {failures} times in a row ({error}). It keeps running, call again to reattach"
                    ) from None
            else:
                failures = 0
                if report.state not in LIVE:
                    launch.report = report
                    return
            if self.clock.wall() > due:
                self.confirm_stop(launch.arn, "deadline")
                launch.timed_out = True
                return
            self.clock.sleep(POLL_SECONDS)

    def confirm_stop(self, arn: str, reason: str) -> None:
        try:
            self.adapter.stop(arn, reason)
        except TaskUnavailable as error:
            raise LaunchUnavailable(f"the stop could not be sent now ({error})") from None
        except TaskRefused as error:
            raise StopFailed(f"the cloud refused to stop the task ({error}). Nothing was recorded, so the stop can be repeated") from None
        end = self.clock.monotonic() + STOP_WAIT_SECONDS
        while self.poll(arn).state in LIVE:
            if self.clock.monotonic() >= end:
                raise StopFailed(f"the task is still stopping after {STOP_WAIT_SECONDS:g} seconds. Nothing was recorded, so the stop can be repeated")
            self.clock.sleep(STOP_POLL_SECONDS)

    def conclude(self, launch: Launch) -> Outcome:
        """Record how the attempt ended, tail the logs of a failure and clean up after a success. An attempt that already has an outcome returns it."""
        if launch.outcome is not None:
            return launch.outcome
        spec = launch.spec
        if launch.timed_out:
            self.close(launch, "timeout", TIMEOUT_EXIT, "deadline")
        elif launch.report is not None:
            self.close(launch, *self.judge(launch.report, spec.acceptance))
        else:
            raise LaunchError("the attempt has not ended")
        outcome = launch.outcome
        if outcome is None:
            raise LaunchError("the end record is missing")
        if not outcome.ok:
            return Outcome(outcome.outcome, outcome.exit_code, outcome.reason, outcome.task_arn, logs=self.tail(launch))
        note = ""
        if self.cleanup is not None:
            try:
                self.cleanup(spec)
            except CleanupFailed as error:
                note = str(error)
        return Outcome(outcome.outcome, outcome.exit_code, outcome.reason, outcome.task_arn, cleanup_note=note)

    def tail(self, launch: Launch) -> str:
        spec = launch.spec
        if not (launch.arn and spec.log_container):
            return ""
        try:
            return scrub(self.adapter.tail(launch.arn, spec.log_container, spec.log_prefix, spec.log_lines))
        except (TaskUnavailable, TaskRefused):
            return ""

    def run(self, spec: JobSpec) -> Outcome:
        """Attach, start, wait and conclude one attempt. A repeat of an ended attempt returns its recorded outcome."""
        launch = self.attach(spec)
        self.start(launch)
        self.wait(launch)
        return self.conclude(launch)

    def cancel(self, run_id: str, job: str, attempt: int, reason: str = "cancel") -> Outcome:
        """Stop the attempt's task with the stop confirmed, then record ``cancelled``. An ended attempt returns its record."""
        records = self.store.records(run_id, job, [attempt])
        mine = {record["event"]: record for record in records}
        if "end" in mine:
            return outcome_of(mine["end"])
        if "intent" not in mine:
            raise LaunchRefused(f"no launch record for attempt {attempt} of {job} in run {run_id}")
        arn = mine["started"]["task_arn"] if "started" in mine else self.find(run_id, job, attempt)
        if arn is not None:
            self.confirm_stop(arn, reason)
        end = self.put("end", attempt, mine["intent"], outcome="cancelled", exit_code=CANCEL_EXIT, reason=reason[:REPORT_LIMIT], task_arn=arn or "")
        return outcome_of(end)
