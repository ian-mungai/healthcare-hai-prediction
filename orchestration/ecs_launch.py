"""Launch one batch job as one ECS task on the host and keep the guarantees the job broker gave.

A job attempt is named ``<run>:<job>:<attempt>``. A run and job have one launch. The launcher writes the facts as write-once records (``LaunchStore``): a
``claim`` (created with a conditional write) says which attempt may start the task, and the attempt's ``intent``, ``started`` (the task ARN) and ``end`` (the
outcome) records follow. A record is never replaced, so a restart, a retry or a second caller reads the same story. The calls are split so that an Airflow
operator can use them as hooks (``attach``, ``start``, ``wait``, ``conclude``) and ``run`` chains them.

1. The claim is the authority of the run and job, written before any ECS action. History that cannot be read blocks the launch: a record is missing only
   when S3 answers NoSuchKey, and a denied read (403) refuses with a clear message and starts nothing.
2. One launch: only the attempt that wins the conditional create of the claim starts a task. Every other caller settles the claim it finds instead: it joins
   the live task, reports the recorded outcome, or completes an unresolved launch under that launch's own identity. The claim that stands is checked against
   the caller's bound inputs before anything else happens.
3. An ended launch is terminal. A success is adopted. A failure, timeout, closed launch or cancel is reported by every later attempt, which launches nothing:
   trying again needs a new run.
4. Unresolved launches: a claim with no task record is resolved by asking ECS for the task of its identity (the ``startedBy`` tag), running or stopped, and, if
   none is listed, by repeating the RunTask call with the same identity and client token, which returns the task if one was accepted. A RunTask reply that is
   lost or timed out is unknown, never a refusal: the launch stays unresolved until a lookup answers.
5. Reattach: a new attempt joins the live task of the launch and keeps its start time and deadline, only when the bound inputs (code digest, request hash and
   input run) equal those of the launch. Different bound inputs are refused.
6. The deadline counts from the recorded start of the task and is the recorded one, so it holds across restarts. A recovered task starts when ECS created it,
   else when the claim was written, never when the recovery ran.
7. A task ends in success only when every acceptance container exited 0. The adapter already leaves out the guard container.
8. A stop is confirmed (the task reports STOPPED within 120 seconds) before anything is recorded; otherwise ``StopFailed`` leaves the stop repeatable.
9. A job that does not fit waits at most 10 minutes counted from its claim (the host's free CPU, memory and scratch, and RunTask capacity answers), asking for
   a task of its identity after every wait, then ends ``failed_closed`` with a fit report. There is no fallback to another capacity provider.

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

from orchestration.ecs_adapter import HOST, CapacityUnavailable, EcsAdapter, TaskFound, TaskRefused, TaskReport, TaskUnavailable, classify, scrub

BOUND_FIELDS = ("code_digest", "request_hash", "input_run")
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
JOB = re.compile(r"[a-z][a-z0-9_]{1,40}")
EVENTS = ("claim", "intent", "started", "end")
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
ABSENT_CODE = "NoSuchKey"
DENIED_CODES = frozenset({"AccessDenied", "Forbidden", "403"})
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
    """How a job attempt ended. ``adopted`` is True when the result is an earlier attempt's recorded outcome, which this attempt reports without
    launching. ``logs`` and ``cleanup_note`` come from the call that concluded the attempt and are not part of the recorded outcome."""

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
        """The intent, started and end records of the given attempts of the run and job, by attempt and then by event order. The store never lists: it reads
        known keys."""
        ...

    def claim(self, run_id: str, job: str) -> dict[str, Any] | None:
        """The claim of the run and job: the one launch the run and job may have. None while nobody holds it."""
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
    claim: dict[str, Any] | None = None
    owner: int | None = None


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
    if event in ("claim", "intent"):
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
    """Launch records as write-once S3 objects: ``<prefix>/<run>/<job>/claim.json`` (the authority of the run and job) and
    ``<prefix>/<run>/<job>/<attempt>/<event>.json``, with ``prefix`` from ``launch_record_prefix``.

    Every write sends ``IfNoneMatch="*"`` (a 412 means the record exists), nothing is deleted or overwritten, and only known keys are read. The code never
    lists. A record is missing only when S3 answers NoSuchKey. A denied read (403) is never read as missing: it means the history cannot be read, so it
    refuses. S3 answers NoSuchKey instead of AccessDenied for an absent key only when the role may list the prefix, so the store checks that once with a key
    that cannot exist, and refuses to work with a role that cannot tell the two apart.
    """

    def __init__(self, client: Any, bucket: str, prefix: str) -> None:
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.distinguishes = False

    def key_of(self, run_id: str, job: str, attempt: int, event: str) -> str:
        return f"{self.prefix}/{run_id}/{job}/{attempt:02d}/{event}.json"

    def claim_key(self, run_id: str, job: str) -> str:
        return f"{self.prefix}/{run_id}/{job}/claim.json"

    def require_distinguishable(self) -> None:
        """Read a key that cannot exist. The answer must be NoSuchKey: with AccessDenied a missing record and a denied record look the same."""
        if self.distinguishes:
            return
        try:
            self.client.get_object(Bucket=self.bucket, Key=f"{self.prefix}/absent-probe.json")
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code == ABSENT_CODE:
                self.distinguishes = True
                return
            if code in DENIED_CODES:
                raise LaunchRefused(
                    "launch records cannot be read safely: a read of a record that does not exist answered AccessDenied, so a missing record and a denied "
                    "record look the same. The role needs s3:ListBucket on the launch record prefix, so that a missing record answers 404 "
                    "(the code never lists)."
                ) from None
            raise self.refusal(code) from None
        except BotoCoreError as error:
            raise LaunchUnavailable(f"launch record store: {type(error).__name__}") from None
        raise LaunchError("a record that cannot exist was read")

    def get(self, key: str) -> dict[str, Any] | None:
        self.require_distinguishable()
        try:
            body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code == ABSENT_CODE:
                return None
            if code in DENIED_CODES:
                raise LaunchRefused(f"launch history cannot be read ({key} answered AccessDenied): a launch never starts on history it cannot read") from None
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
        if record["event"] == "claim":
            key = self.claim_key(record["run_id"], record["job"])
        else:
            key = self.key_of(record["run_id"], record["job"], record["attempt"], record["event"])
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=json.dumps(record, sort_keys=True).encode(), IfNoneMatch="*")
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code != "PreconditionFailed":
                raise self.refusal(code) from None
            stored = self.get(key)
            if stored is None:
                raise LaunchUnavailable("a launch record exists but is not readable yet: call again") from None
            return stored
        except BotoCoreError:
            stored = self.get(key)
            if stored is None:
                raise LaunchUnavailable("launch record store: the write is not confirmed") from None
            return stored
        if self.get(key) is None:
            raise LaunchUnavailable("a launch record was written but is not readable yet: call again")
        return dict(record)

    def claim(self, run_id: str, job: str) -> dict[str, Any] | None:
        return self.get(self.claim_key(run_id, job))

    def records(self, run_id: str, job: str, attempts: Sequence[int]) -> list[dict[str, Any]]:
        keys = [self.key_of(run_id, job, attempt, event) for attempt in attempts for event in ("intent", "started", "end")]
        return [record for key in keys if (record := self.get(key)) is not None]


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

    def events(self, run_id: str, job: str, attempt: int) -> dict[str, dict[str, Any]]:
        """The intent, started and end records of one attempt, by event."""
        return {record["event"]: record for record in self.store.records(run_id, job, [attempt])}

    def check_bound(self, spec: JobSpec, first: Mapping[str, Any]) -> None:
        """The request uses the bound inputs of the first launch of its run and job. A changed input needs a new run."""
        for name in BOUND_FIELDS:
            if first["bound"].get(name) != spec.bound[name]:
                raise LaunchRefused(
                    f"run {spec.run_id} job {spec.job} is bound to the {name} of its first launch ({str(first['bound'].get(name, ''))[:12]}), "
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

    def lookup(self, claim: Mapping[str, Any]) -> TaskFound | None:
        """The task started under the identity of the claim's attempt, running or stopped. None only when ECS lists no such task."""
        try:
            return self.adapter.lookup(f"{claim['run_id']}:{claim['job']}:{claim['attempt']}")
        except TaskUnavailable as error:
            raise LaunchUnavailable(f"the task of attempt {claim['attempt']} cannot be looked up now ({error})") from None

    @staticmethod
    def began_of(found: TaskFound, claim: Mapping[str, Any]) -> str:
        """When the task began: the time ECS created it, else the time of the durable claim. Never the time of a recovery."""
        return iso(found.created_at) if found.created_at is not None else str(claim["at"])

    def judge(self, report: TaskReport, acceptance: Sequence[str]) -> tuple[str, int, str]:
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
        """Validate the request and reconcile it with the records and the cloud: an ended attempt, the recorded outcome of the run and job's one launch, the
        live task of that launch, an unresolved launch, or nothing yet. Nothing is launched here."""
        self.validate(spec)
        claim = self.store.claim(spec.run_id, spec.job)
        mine = self.events(spec.run_id, spec.job, spec.attempt)
        first = claim or mine.get("intent")
        if first is not None:
            self.check_bound(spec, first)
        launch = Launch(spec)
        if "end" in mine:
            launch.outcome = outcome_of(mine["end"])
        elif "started" in mine:
            launch.arn, launch.started_at = mine["started"]["task_arn"], stamp(mine["started"]["job_started_at"])
            launch.deadline_seconds = mine["intent"]["deadline_seconds"]
            launch.owner = mine["intent"].get("reattached_from")
        else:
            self.settle(launch, claim, mine)
        return launch

    def settle(self, launch: Launch, claim: dict[str, Any] | None, mine: dict[str, dict[str, Any]]) -> None:
        """Settle the one launch of the run and job. An ended launch is terminal: a success is adopted, and a failure, timeout or cancel is reported, never
        launched again, because trying again needs a new run. A live task is joined. A launch with no task record is looked up by its identity, stopped tasks
        too, and one that cannot be found stays unresolved in ``launch.claim``: nothing else may launch until it is resolved."""
        spec = launch.spec
        launch.claim = None
        if claim is None:
            return
        attempt = claim["attempt"]
        theirs = mine if attempt == spec.attempt else self.events(spec.run_id, spec.job, attempt)
        if "end" in theirs:
            launch.outcome = outcome_of(theirs["end"], adopted=attempt != spec.attempt)
            return
        if "started" in theirs:
            arn, began = theirs["started"]["task_arn"], theirs["started"]["job_started_at"]
        else:
            found = self.lookup(claim)
            if found is None:
                launch.claim = claim
                return
            self.record_intent(claim)
            stored = self.record_started(claim, found.arn, self.began_of(found, claim))
            arn, began = stored["task_arn"], stored["job_started_at"]
        if attempt == spec.attempt:
            launch.arn, launch.started_at, launch.deadline_seconds = arn, stamp(began), claim["deadline_seconds"]
            return
        report = self.poll(arn)
        if report.state in LIVE:
            self.join(launch, claim, arn, began)
            return
        outcome, code, reason = self.judge(report, claim["acceptance"])
        end = self.put("end", attempt, claim, outcome=outcome, exit_code=code, reason=reason[:REPORT_LIMIT], task_arn=arn)
        launch.outcome = outcome_of(end, adopted=True)

    def record_intent(self, claim: Mapping[str, Any]) -> dict[str, Any]:
        return self.put(
            "intent",
            claim["attempt"],
            claim,
            bound=claim["bound"],
            deadline_seconds=claim["deadline_seconds"],
            acceptance=claim["acceptance"],
            need=claim["need"],
        )

    def record_started(self, claim: Mapping[str, Any], arn: str, began: str) -> dict[str, Any]:
        started = self.put("started", claim["attempt"], claim, task_arn=arn, job_started_at=began)
        if started["task_arn"] != arn:
            raise LaunchRefused("another task is already recorded for this launch")
        return started

    def join(self, launch: Launch, claim: Mapping[str, Any], arn: str, began: str) -> None:
        """Record this attempt as a reattachment to the task of another launch, with that launch's start time and deadline."""
        spec = launch.spec
        intent = self.put(
            "intent",
            spec.attempt,
            claim,
            bound=dict(spec.bound),
            deadline_seconds=claim["deadline_seconds"],
            acceptance=list(spec.acceptance),
            need=asdict(spec.need),
            reattached_from=claim["attempt"],
        )
        started = self.put("started", spec.attempt, claim, task_arn=arn, job_started_at=began)
        launch.arn, launch.started_at, launch.deadline_seconds = started["task_arn"], stamp(started["job_started_at"]), intent["deadline_seconds"]
        launch.owner = claim["attempt"]

    def admit(self, launch: Launch) -> bool:
        """Claim the one launch of the run and job with a conditional create, the single step that lets a caller start a task. Returns False when another
        caller holds the claim. The claim that stands is checked against this request before anything else happens."""
        spec = launch.spec
        body = {
            "schema_version": 1,
            "event": "claim",
            "run_id": spec.run_id,
            "job": spec.job,
            "attempt": spec.attempt,
            "at": iso(self.clock.wall()),
            "bound": dict(spec.bound),
            "deadline_seconds": spec.deadline_seconds,
            "acceptance": list(spec.acceptance),
            "need": asdict(spec.need),
        }
        stored = self.store.put_once(body)
        self.check_bound(spec, stored)
        if stored["attempt"] != spec.attempt:
            return False
        launch.claim = stored
        return True

    def start(self, launch: Launch) -> None:
        """Claim the launch, record the intent, check that the job fits, run the task once and record it. When another caller holds the claim, its launch is
        settled instead: the task of an unresolved launch is looked up by its identity and, if none exists, started under that identity, so a second task
        never starts."""
        spec = launch.spec
        if launch.outcome is not None or launch.arn is not None:
            return
        fresh = launch.claim is None
        if fresh and not self.admit(launch):
            self.settle(launch, self.store.claim(spec.run_id, spec.job), self.events(spec.run_id, spec.job, spec.attempt))
            if launch.outcome is not None or launch.arn is not None:
                return
            fresh = False
        claim = launch.claim
        if claim is None:
            raise LaunchError("the claim is missing")
        self.check_bound(spec, claim)
        self.record_intent(claim)
        launch.deadline_seconds = claim["deadline_seconds"]
        found = self.run_when_it_fits(launch, claim, fresh)
        if found is None:
            return
        stored = self.record_started(claim, *found)
        if claim["attempt"] == spec.attempt:
            launch.arn, launch.started_at = stored["task_arn"], stamp(stored["job_started_at"])
        else:
            self.join(launch, claim, stored["task_arn"], stored["job_started_at"])

    def run_when_it_fits(self, launch: Launch, claim: dict[str, Any], fresh: bool) -> tuple[str, str] | None:
        """Return the ARN and start time of the task under the claim, started now or found by identity, waiting for room at most ``FIT_WAIT_SECONDS`` counted
        from the claim. After any wait, or when the claim is not new, the identity is asked first, stopped tasks too. Returns None when the launch was closed.
        A RunTask reply that is lost or timed out is never read as a refusal: past the deadline it leaves the launch unresolved."""
        spec = launch.spec
        owner = claim["attempt"] == spec.attempt
        key = f"{claim['run_id']}:{claim['job']}:{claim['attempt']}"
        end = stamp(claim["at"]) + FIT_WAIT_SECONDS
        ask = unknown = not fresh
        last = ""
        while True:
            if ask:
                try:
                    found = self.lookup(claim)
                except LaunchUnavailable as error:
                    unknown, last = True, str(error)
                else:
                    if found is not None:
                        return found.arn, self.began_of(found, claim)
                    unknown = False
            fits = True
            if owner:
                try:
                    free = self.probe.free()
                except TaskUnavailable as error:
                    fits, last = False, f"host capacity unreadable ({error})"
                except TaskRefused as error:
                    self.end_claim(launch, claim, f"fit_probe_refused:{error}")
                    return None
                else:
                    short = shortfalls(spec.need, free)
                    fits, last = (False, "; ".join(short)) if short else (True, last)
            if fits:
                try:
                    return self.adapter.start(spec.job, key, spec.environment, "", HOST), iso(self.clock.wall())
                except CapacityUnavailable as error:
                    last = f"RunTask answered {error}"
                except TaskUnavailable as error:
                    unknown, last = True, f"the RunTask reply is unknown ({error})"
                except TaskRefused as error:
                    self.end_claim(launch, claim, f"launch_refused:{error}")
                    return None
                if not owner:
                    raise LaunchUnavailable(f"the launch of attempt {claim['attempt']} could not be completed now ({last})")
            if self.clock.wall() >= end:
                if unknown:
                    raise LaunchUnavailable(f"a task may exist under this launch ({last}). It stays unresolved: call again to find it")
                self.end_claim(launch, claim, f"does_not_fit after {FIT_WAIT_SECONDS:g} s: {last}"[:REPORT_LIMIT])
                return None
            ask = True
            self.clock.sleep(FIT_POLL_SECONDS)

    def end_claim(self, launch: Launch, claim: Mapping[str, Any], reason: str) -> None:
        """End a launch that definitely started no task. The run and job's one launch is over, so this attempt's outcome is that failure."""
        end = self.put("end", claim["attempt"], claim, outcome="failed_closed", exit_code=CLOSED_EXIT, reason=reason[:REPORT_LIMIT], task_arn="")
        launch.outcome = outcome_of(end, adopted=claim["attempt"] != launch.spec.attempt)

    def close(self, launch: Launch, outcome: str, code: int, reason: str) -> None:
        """Record how this attempt ended. An attempt that joined another's task also records the end on the attempt that holds the claim, so a later attempt
        reports the same outcome."""
        spec = launch.spec
        base = {"run_id": spec.run_id, "job": spec.job}
        arn = launch.arn or ""
        end = self.put("end", spec.attempt, base, outcome=outcome, exit_code=code, reason=reason, task_arn=arn)
        launch.outcome = outcome_of(end)
        if launch.owner is not None and launch.owner != spec.attempt:
            self.put("end", launch.owner, base, outcome=end["outcome"], exit_code=end["exit_code"], reason=end["reason"], task_arn=arn)

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
                        f"the task could not be read {failures} times in a row ({error}); it keeps running, call again to reattach"
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
        mine = self.events(run_id, job, attempt)
        if "end" in mine:
            return outcome_of(mine["end"])
        claim = self.store.claim(run_id, job)
        base = claim if claim is not None and claim["attempt"] == attempt else mine.get("intent")
        if base is None:
            raise LaunchRefused(f"no launch record for attempt {attempt} of {job} in run {run_id}")
        if "started" in mine:
            arn: str | None = mine["started"]["task_arn"]
        else:
            found = self.lookup(base)
            arn = found.arn if found is not None else None
        if arn is not None:
            self.confirm_stop(arn, reason)
        end = self.put("end", attempt, base, outcome="cancelled", exit_code=CANCEL_EXIT, reason=reason[:REPORT_LIMIT], task_arn=arn or "")
        return outcome_of(end)
