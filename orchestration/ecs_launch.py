"""Launch one batch job as one ECS task on the host and keep its guarantees: one launch per run and job, a write-once record of each fact.

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
4. One decision record, created with a conditional write, is the authority over sending a RunTask call and closing without a task, and the two exclude
   each other durably. A caller sends only while it holds the send token and closes only by winning the close token (or by handing back its own send token when
   every call it sent was answered, with a version-checked replacement). Once a RunTask call may have been sent, the claim stays unresolved until positive
   evidence: the task found by identity and settled. No timer and no empty listing proves that nothing started. The call raises LaunchUnavailable and names
   the automatic settle of the next launch of the family (item 11). RunTask is sent only inside the 10 minute window and never after a cancel request, both
   checked immediately before every call. The adapter's RunTask client has no SDK retry, so every request that may reach ECS passes that check, and once one
   request had no answer the uncertainty stays for the life of the launch: a later refusal, capacity answer or end of the window does not clear it.
5. Reattach: a new attempt joins the live task of the launch and keeps its start time and deadline, only when the bound inputs (code digest, request hash and
   input run) equal those of the launch. Different bound inputs are refused.
6. The deadline counts from the recorded start of the task and is the recorded one, so it holds across restarts. A recovered task starts when ECS created it,
   else when the claim was written, never when the recovery ran.
7. A task ends in success only when every acceptance container exited 0. The adapter already leaves out the guard container.
8. A stop is confirmed (the task reports STOPPED within 120 seconds) before anything is recorded; otherwise ``StopFailed`` leaves the stop repeatable. A
   DescribeTasks reply that describes no task, or has no task list, is never evidence: ECS documents no guarantee that it describes a task it created.
   The adapter reports it as unavailable, so the launch stays unresolved, and a failed, timed-out or cancelled outcome is recorded only for a task shown
   STOPPED.
9. A job that does not fit waits at most 10 minutes counted from its claim (the host's free CPU, memory and scratch, and RunTask capacity answers), asking for
   a task of its identity after every wait, then ends ``failed_closed`` with a fit report. There is no fallback to another capacity provider. The needs
   are the claim's, so every caller that may create a task is held to them.
10. The end record of the attempt that holds the claim is written first and is the one outcome: every attempt that joined, concluded or cancelled records and
   reports it. A cancel is a durable request first, so a launch call that is still starting its task stops that task; the cancel then confirms the stop
   before it records ``cancelled``.

11. Family gate (owner, Nov 7 2026): the host keeps one scratch folder per family, which the first container of the task empties. Before a new run claims
   a launch, it is refused (``FamilyBusy``, nothing claimed, so the run can launch later) while ECS lists a task of the family's host definition that it
   does not show STOPPED (running, pending or stopping) or while a run in the write-once family index has a claimed attempt without an end record. A
   listing that fails refuses too. An unfinished launch is settled by the gate itself (``auto_settle``, automatic launch recovery, owner Nov 11 2026): it
   cancels, which ends the launch only with proof: its task is found by identity and its stop is confirmed, or no RunTask call was ever sent. A launch whose
   call may have been sent and whose task is not found, and a launch whose saved task ECS no longer describes, stay unresolved at the gate, and the new run is
   refused with the old run, job, attempt, task identity and saved ARN: no timer, no send window and no empty listing ends it, because ECS lists a new task
   late and a request that passed its guard may still be in flight. No statement of the owner ends it either.
   Host stop (owner decision A, Nov 24 2026): a stopped host is the one proof that ends such a launch without a task. When the host session (``stop``,
   ``stop_after_failure`` and the ``start`` that follows) has read that the host group has desired capacity 0 and no instance, that ECS then lists no ACTIVE
   container instance, and that ECS lists no task of the job's host definition that is not STOPPED, it records ``cancelled`` for every claimed launch with
   no end record whose claim is older than the send window plus ``SETTLE_MARGIN_SECONDS`` (``HostStopSettler``, write-once end record, the proof passed
   in as a ``HostStopProof``). Any other launch stays unresolved and is reported by run, job and attempt.
   Accepted limit (owner, Nov 24 2026): the proof is a set of reads, not a lock. A host start by another session, or a RunTask request placed more than 20
   minutes late, in the seconds between the final reread and the end write, could leave a cancelled end record for a running task. The operating rule is one
   host session at a time. The settler writes the durable ``cancel`` request after the reread and before the end, so a sender that gets a late RunTask reply
   sees it at its next check and stops its own task.
12. Checked revision (owner A, Nov 10 2026): a request names the ARN of the revision of the job's host definition that the preflight checked against the
   current host capacity (``JobSpec.task_definition``). A request without one is refused before any record, and RunTask names that revision, never the
   family, so ECS cannot substitute a later latest revision.

Every wait has a deadline. The library calls no process and starts no thread.
"""

from __future__ import annotations

import json
import os
import re
import socket
import time
import uuid
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from functools import partial, wraps
from pathlib import Path
from typing import Any, Protocol

from botocore.exceptions import BotoCoreError, ClientError

from orchestration.ecs_adapter import (
    FAILURE_PHASES,
    GUARD,
    CapacityUnavailable,
    EcsAdapter,
    SendUnknown,
    SendWithheld,
    TaskFound,
    TaskRefused,
    TaskReport,
    TaskUnavailable,
    classify,
    failure_from_tail,
    render_failure,
    scrub,
    stored_failure,
)
from orchestration.training_contract import CODE_STAMP_RUN
from orchestration.training_contract import RUN_ID as RUN_ID_PATTERN

BOUND_FIELDS = ("code_digest", "request_hash", "input_run")
SHUTDOWN_CODES = frozenset({143})  # SIGTERM: ECS stopped the container because another one failed
KILLED_CODE = 137  # SIGKILL: a primary failure only when ECS gives a container reason for it, such as OutOfMemoryError
GUARD_TIMEOUT_LINE = "guard: the task passed its time limit, so it stops"  # the fixed line of scripts/orchestration/task_guard.py
RUN_ID = re.compile(RUN_ID_PATTERN)
# The run ID of before the run ID redesign: a commit time and eight hex digits, the form ``CODE_STAMP_RUN`` names. Earlier launches of a family are indexed
# and recorded under it. It is read as history only (the family index listing); a request (``validate``) and a family index write take ``RUN_ID`` alone.
LEGACY_RUN_ID = re.compile(CODE_STAMP_RUN)
JOB = re.compile(r"[a-z][a-z0-9_]{1,40}")
EVENTS = ("claim", "decision", "cancel", "intent", "started", "end")
RUN_EVENTS = ("claim", "decision", "cancel")
OUTCOMES = frozenset({"succeeded", "failed", "timeout", "cancelled", "failed_closed"})
TIMEOUT_EXIT = 124
NO_CHECK_EXIT = 125
CANCEL_EXIT = 143
CLOSED_EXIT = 1
STOP_WAIT_SECONDS = 120.0
STOP_POLL_SECONDS = 2.0
FIT_WAIT_SECONDS = 600.0
FIT_POLL_SECONDS = 15.0
SETTLE_MARGIN_SECONDS = 600.0  # beyond the send window (FIT_WAIT_SECONDS): a request that passed its guard has had 10 more minutes to land
PROOF_MAX_AGE_SECONDS = 900.0  # the longest a host proof is used after it was read
HOST_STOP_REASON = (
    "host stopped: when read, the host group had no instance, ECS listed no container instance and no unstopped task of the family, and the claim was older "
    "than the send window plus the margin"
)
HOST_STOP_NOTE = (
    "a host stop (host_session stop, or the start after it) ends the launch as cancelled when the host group has no instance, ECS lists no container "
    "instance and no unstopped task of the family, and the claim is older than 20 minutes"
)
POLL_SECONDS = 6.0
POLL_FAILURES = 5
ABSENT_CODE = "NoSuchKey"
DENIED_CODES = frozenset({"AccessDenied", "Forbidden", "403"})
CONFIG_FILE = Path(__file__).resolve().parents[1] / "config" / "orchestration" / "ecs_launch.json"
PREFIX_PATTERN = re.compile(r"[a-z0-9_]+(/[a-z0-9_]+)*")
REPORT_LIMIT = 300
RETRYABLE_CODES = frozenset({"SlowDown", "ServiceUnavailable", "InternalError", "RequestTimeout", "ConditionalRequestConflict", "ThrottlingException"})
LIVE = ("pending", "running")
ENDED = ("succeeded", "failed")  # the states of a task shown STOPPED


class LaunchError(Exception):
    """Base of the launch library's errors."""


class LaunchRefused(LaunchError):
    """The request cannot be launched as it stands."""


class FamilyBusy(LaunchRefused):
    """Another launch of the family may still use the family's host scratch folder: a task of its host definition runs, or an earlier claimed launch has no
    end record. Nothing was claimed, so the same run can launch once the family is free."""


class LaunchUnavailable(LaunchError):
    """A later identical call can succeed. Nothing was launched beside a task that may be live."""


class RecordStoreUnavailable(LaunchUnavailable):
    """A launch record could not be read or written now, or a write is not confirmed. The failure is temporary: the same call can succeed."""


class StopFailed(LaunchError):
    """A stop was not confirmed. Nothing was recorded, so the stop can be repeated."""


class CleanupFailed(Exception):
    """A cleanup hook could not finish. The job outcome stays as recorded."""


OUTCOME_COUNT = "hai.launch.outcome.{family}.{result}"
REFUSAL_COUNT = "hai.launch.refusal.{family}.{kind}"
REFUSAL_KINDS = ("refused", "family_busy", "unavailable", "record_store_unavailable", "stop_failed", "error")
FAMILY = re.compile(r"[a-z][a-z0-9_]{0,31}")
STATSD_HOST, STATSD_PORT = "AIRFLOW__METRICS__STATSD_HOST", "AIRFLOW__METRICS__STATSD_PORT"


def send_count(name: str, environment: Mapping[str, str] | None = None) -> None:
    """Send one StatsD counter datagram to the exporter on this host, named by the scheduler's own metric settings (set only while monitoring is on). Nothing
    is sent without them, and a problem here never raises or waits, so counting cannot change what the launcher returns or raises."""
    try:
        env = os.environ if environment is None else environment
        host, port = env.get(STATSD_HOST, ""), env.get(STATSD_PORT, "")
        if not host or not port.isdecimal() or not 0 < int(port) < 65536:
            return
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
            sender.settimeout(0.05)
            sender.sendto(f"{name}:1|c".encode(), (host, int(port)))
    except Exception:
        return


def count_outcome(family: str, result: str, environment: Mapping[str, str] | None = None) -> None:
    """Count one ended launch of the family by its outcome. No run ID, task ARN or error text is part of the name."""
    if FAMILY.fullmatch(family) and result in OUTCOMES:
        send_count(OUTCOME_COUNT.format(family=family, result=result), environment)


def count_refusal_kind(family: str, kind: str, environment: Mapping[str, str] | None = None) -> None:
    """Count one refused launch call of the family by the kind of its refusal."""
    if FAMILY.fullmatch(family) and kind in REFUSAL_KINDS:
        send_count(REFUSAL_COUNT.format(family=family, kind=kind), environment)


KINDS = (
    (FamilyBusy, "family_busy"),
    (RecordStoreUnavailable, "record_store_unavailable"),
    (LaunchUnavailable, "unavailable"),
    (LaunchRefused, "refused"),
    (StopFailed, "stop_failed"),
)


def refusal_kind(error: LaunchError) -> str:
    """The closed kind of a refusal: its most specific class, or ``error`` for any other launch error."""
    return next((kind for cls, kind in KINDS if isinstance(error, cls)), "error")


def counted[Result](method: Callable[[Any, Any], Result]) -> Callable[[Any, Any], Result]:
    """Count a launch error that the method raises, by the family of its subject and the kind of the error, and raise it again unchanged."""

    @wraps(method)
    def wrapper(self: Any, subject: Any) -> Result:
        try:
            return method(self, subject)
        except LaunchError as error:
            self.count_refusal(subject, error)
            raise

    return wrapper


@dataclass(frozen=True)
class Resources:
    """CPU units, memory and scratch bytes, needed or free."""

    cpu_units: int
    memory_mib: int
    scratch_bytes: int


@dataclass(frozen=True)
class JobSpec:
    """One job attempt. ``bound`` holds the code digest, request hash and input run that every attempt of a run and job must share. ``environment`` maps a
    container name to the variables it receives. ``acceptance`` names the containers that must exit 0. ``task_definition`` is the ARN of the revision of
    the job's host definition that the preflight checked against the current host capacity: the task runs that revision, never the family's latest one."""

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
    task_definition: str = ""

    @property
    def key(self) -> str:
        return f"{self.run_id}:{self.job}:{self.attempt}"


@dataclass(frozen=True)
class Outcome:
    """How a job attempt ended. ``adopted`` is True when the result is an earlier attempt's recorded outcome, which this attempt reports without
    launching. ``failure`` is the message of the closed failure record (checked phase, fixed codes, numeric facts) that the end record keeps, rendered from
    fixed templates, and empty when the record holds none. ``logs`` and ``cleanup_note`` come from the call that concluded the attempt and are not part of the
    recorded outcome."""

    outcome: str
    exit_code: int
    reason: str
    task_arn: str
    adopted: bool = False
    failure: str = ""
    logs: str = field(default="", compare=False)
    cleanup_note: str = field(default="", compare=False)

    @property
    def ok(self) -> bool:
        return self.outcome == "succeeded"


@dataclass(frozen=True)
class Diagnosis:
    """What the one causal container of a failed task says: the closed failure record for the end record (None when no container caused it) and its log tail."""

    record: dict[str, Any] | None
    logs: str


@dataclass
class Clock:
    """Time and waiting, replaceable so a case can run ten minutes of waiting at once."""

    wall: Callable[[], float] = time.time
    monotonic: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep


def put_record(store: LaunchStore, now: float, event: str, attempt: int, base: Mapping[str, Any], **fields: Any) -> dict[str, Any]:
    """Write one record of the run and job of ``base`` once and return the record that stands."""
    record = {"schema_version": 1, "event": event, "run_id": base["run_id"], "job": base["job"], "attempt": attempt, "at": iso(now), **fields}
    return store.put_once(record)


def write_end(
    store: LaunchStore,
    now: float,
    families: Collection[str],
    base: Mapping[str, Any],
    owner: int,
    mine: int,
    outcome: str,
    code: int,
    reason: str,
    arn: str,
    failure: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Record how the launch ended. The end record of the attempt that holds the claim is written first and is the one outcome: an attempt that joined it
    records and reports whatever is stored there, even when it judged the task differently. A closed ``failure`` record is kept in it, and copied."""
    first = put_record(
        store, now, "end", owner, base, outcome=outcome, exit_code=code, reason=reason, task_arn=arn, **({"failure": dict(failure)} if failure else {})
    )
    if mine == owner:
        if base["job"] in families:
            count_outcome(base["job"], first["outcome"])
        return first
    return put_record(store, now, "end", mine, base, **copied(first))


class LaunchStore(Protocol):
    """Write-once launch records."""

    def put_once(self, record: Mapping[str, Any]) -> dict[str, Any]:
        """Store the record under its key unless the key exists. Return the record that stands: the new one, or the one that was there."""
        ...

    def records(self, run_id: str, job: str, attempts: Sequence[int]) -> list[dict[str, Any]]:
        """The intent, started and end records of the given attempts of the run and job, by attempt and then by event order. The store never lists: it reads
        known keys."""
        ...

    def end_record(self, run_id: str, job: str, attempt: int) -> dict[str, Any] | None:
        """The end record of one attempt, or None while nobody has written it: one key read, for a caller that scans many launches."""
        ...

    def run_record(self, run_id: str, job: str, event: str) -> dict[str, Any] | None:
        """A record of the whole run and job: the ``claim`` (the one launch it may have), the ``decision`` (the one authority over sending a RunTask call and
        closing without a task) or a ``cancel`` request. None while nobody has written it."""
        ...

    def decision(self, run_id: str, job: str) -> tuple[dict[str, Any], str] | None:
        """The decision record with the version it was read at, or None."""
        ...

    def replace_decision(self, record: Mapping[str, Any], version: str) -> bool:
        """Replace the decision record if it still has the given version. False when it changed."""
        ...

    def index_family(self, claim: Mapping[str, Any]) -> None:
        """Record once, in the family index of the claim's job, that the run is about to claim a launch of the family. It is written before the claim."""
        ...

    def family_runs(self, job: str) -> list[str]:
        """The runs in the family index of the job: every run that claimed, or was about to claim, a launch of the family. The one listing the store makes."""
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
    holder: str = field(default_factory=lambda: uuid.uuid4().hex)
    ambiguous: bool = False


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
    shown = render_failure(stored_failure(record["failure"])) if "failure" in record else ""
    return Outcome(record["outcome"], record["exit_code"], record["reason"], record.get("task_arn", ""), adopted, shown)


def copied(end: Mapping[str, Any]) -> dict[str, Any]:
    """The fields of an end record that another attempt's end record repeats: the outcome and, when the record holds one, the closed failure record."""
    found = {"outcome": end["outcome"], "exit_code": end["exit_code"], "reason": end["reason"], "task_arn": end.get("task_arn", "")}
    return {**found, "failure": end["failure"]} if end.get("failure") else found


def parse_family_record(raw: bytes, where: str) -> dict[str, Any]:
    """A family index entry as a validated dict: exactly the fields ``index_family`` writes. Anything else stops the launch with its key."""
    try:
        record = json.loads(raw)
    except ValueError:
        raise LaunchError(f"family index entry {where} is not JSON") from None
    fields: dict[str, type] = {"schema_version": int, "event": str, "run_id": str, "job": str, "attempt": int, "at": str}
    if (
        not isinstance(record, dict)
        or set(record) != set(fields)
        or not all(isinstance(record[name], kind) for name, kind in fields.items())
        or record["event"] != "family"
        or record["schema_version"] != 1
        or not RUN_ID.fullmatch(record["run_id"])
        or not where.endswith(f"/families/{record['job']}/{record['run_id']}.json")
    ):
        raise LaunchError(f"family index entry {where} is invalid")
    return dict(record)


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
    elif event == "cancel":
        needed |= {"reason": str}
    elif event == "decision":
        kind = record.get("decision") if isinstance(record, dict) else None
        needed |= {"decision": str}
        needed |= {"holder": str} if kind == "send" else {"outcome": str, "exit_code": int, "reason": str}
    elif event == "started":
        needed |= {"task_arn": str, "job_started_at": str}
    elif event == "end":
        needed |= {"outcome": str, "exit_code": int, "reason": str}
        if isinstance(record, dict) and "failure" in record:
            try:
                stored_failure(record["failure"])
            except ValueError:
                raise LaunchError(f"launch record {where} is invalid (failure)") from None
    problem = next((name for name, kind in needed.items() if not isinstance(record.get(name) if isinstance(record, dict) else None, kind)), None)
    closing = event == "end" or (event == "decision" and record.get("decision") == "close")
    if event not in EVENTS or problem is not None or (closing and record["outcome"] not in OUTCOMES) or record["schema_version"] != 1:
        raise LaunchError(f"launch record {where} is invalid ({problem or 'event or outcome'})")
    return dict(record)


class S3LaunchStore:
    """Launch records as write-once S3 objects: ``<prefix>/<run>/<job>/claim.json`` (the authority of the run and job) and
    ``<prefix>/<run>/<job>/<attempt>/<event>.json``, with ``prefix`` from ``launch_record_prefix``.

    Every write sends ``IfNoneMatch="*"`` (a 412 means the record exists), nothing is deleted, the decision record is the only one ever replaced (with
    ``IfMatch`` on the version it was read at), and only known keys are read. The code never
    lists, except the family index ``<prefix>/families/<job>/<run>.json`` (write-once, one key per run that claimed a launch of the family), which the
    family gate lists before a claim; S3 lists strongly consistently, so a run indexed before the listing is in it. A record is missing only when S3
    answers NoSuchKey. A denied read (403) is never read as missing: it means the history cannot be read, so it
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

    def run_key(self, run_id: str, job: str, event: str) -> str:
        return f"{self.prefix}/{run_id}/{job}/{event}.json"

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
            raise RecordStoreUnavailable(f"launch record store: {type(error).__name__}") from None
        raise LaunchError("a record that cannot exist was read")

    def get_with_etag(self, key: str, parse: Callable[[bytes, str], dict[str, Any]] = parse_record) -> tuple[dict[str, Any], str] | None:
        self.require_distinguishable()
        try:
            reply = self.client.get_object(Bucket=self.bucket, Key=key)
            body = reply["Body"].read()
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code == ABSENT_CODE:
                return None
            if code in DENIED_CODES:
                raise LaunchRefused(f"launch history cannot be read ({key} answered AccessDenied): a launch never starts on history it cannot read") from None
            raise self.refusal(code) from None
        except BotoCoreError as error:
            raise RecordStoreUnavailable(f"launch record store: {type(error).__name__}") from None
        return parse(body, key), str(reply.get("ETag", ""))

    def get(self, key: str) -> dict[str, Any] | None:
        found = self.get_with_etag(key)
        return found[0] if found else None

    @staticmethod
    def refusal(code: str) -> LaunchError:
        if code in RETRYABLE_CODES:
            return RecordStoreUnavailable(f"launch record store: {code}")
        return LaunchRefused(f"launch record store refused the call: {code or 'ClientError'}")

    def put_once(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if record["event"] in RUN_EVENTS:
            key = self.run_key(record["run_id"], record["job"], record["event"])
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
                raise RecordStoreUnavailable("a launch record exists but is not readable yet: call again") from None
            return stored
        except BotoCoreError:
            stored = self.get(key)
            if stored is None:
                raise RecordStoreUnavailable("launch record store: the write is not confirmed") from None
            return stored
        if self.get(key) is None:
            raise RecordStoreUnavailable("a launch record was written but is not readable yet: call again")
        return dict(record)

    def run_record(self, run_id: str, job: str, event: str) -> dict[str, Any] | None:
        return self.get(self.run_key(run_id, job, event))

    def end_record(self, run_id: str, job: str, attempt: int) -> dict[str, Any] | None:
        return self.get(self.key_of(run_id, job, attempt, "end"))

    def decision(self, run_id: str, job: str) -> tuple[dict[str, Any], str] | None:
        return self.get_with_etag(self.run_key(run_id, job, "decision"))

    def replace_decision(self, record: Mapping[str, Any], version: str) -> bool:
        """The one overwrite: a version-checked replacement of the decision record, by the holder of the send token, to close without a task."""
        key = self.run_key(record["run_id"], record["job"], "decision")
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=json.dumps(record, sort_keys=True).encode(), IfMatch=version)
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code == "PreconditionFailed":
                return False
            raise self.refusal(code) from None
        except BotoCoreError:
            stored = self.get(key)
            return stored is not None and stored == dict(record)
        return True

    def records(self, run_id: str, job: str, attempts: Sequence[int]) -> list[dict[str, Any]]:
        keys = [self.key_of(run_id, job, attempt, event) for attempt in attempts for event in ("intent", "started", "end")]
        return [record for key in keys if (record := self.get(key)) is not None]

    def family_prefix(self, job: str) -> str:
        return f"{self.prefix}/families/{job}/"

    def index_family(self, claim: Mapping[str, Any]) -> None:
        body = {"schema_version": 1, "event": "family", "run_id": claim["run_id"], "job": claim["job"], "attempt": claim["attempt"], "at": claim["at"]}
        key = f"{self.family_prefix(str(claim['job']))}{claim['run_id']}.json"
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=json.dumps(body, sort_keys=True).encode(), IfNoneMatch="*")
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code != "PreconditionFailed":
                raise self.refusal(code) from None
        except BotoCoreError:
            if self.get_with_etag(key, parse_family_record) is None:
                raise RecordStoreUnavailable("launch record store: the family index write is not confirmed") from None

    def family_runs(self, job: str) -> list[str]:
        prefix = self.family_prefix(job)
        runs: list[str] = []
        token: str | None = None
        seen: set[str] = set()
        try:
            while True:
                page = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, **({"ContinuationToken": token} if token else {}))
                for item in page.get("Contents", []):
                    name = str(item["Key"])[len(prefix) :].removesuffix(".json")
                    if not (RUN_ID.fullmatch(name) or LEGACY_RUN_ID.fullmatch(name)):
                        raise LaunchRefused(f"the family index of {job} holds an unexpected key: {item['Key']}")
                    runs.append(name)
                if not page.get("IsTruncated"):
                    return runs
                token = str(page["NextContinuationToken"])
                if token in seen:
                    raise LaunchRefused(f"the family index of {job} repeated a continuation token: the listing is not read to its end")
                seen.add(token)
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code in DENIED_CODES:
                raise LaunchRefused(f"the family index of {job} cannot be listed ({code}): a launch never starts beside launches it cannot see") from None
            raise self.refusal(code) from None
        except BotoCoreError as error:
            raise RecordStoreUnavailable(f"launch record store: {type(error).__name__}") from None


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
        scratch_usage: Callable[[], Mapping[str, int]] | None = None,
        send_check: Callable[[Resources], None] | None = None,
    ) -> None:
        """``cleanup`` runs once after a successful job. It raises ``CleanupFailed`` when it cannot finish. ``scratch_usage`` gives the bytes each family's
        scratch folder holds (owner decision A, Nov 7 2026): the launching family's own bytes count as free, because the gate has shown the family idle and
        the task's first container empties that folder; another family's bytes never count, and the fit report names the families that hold any."""
        self.adapter = adapter
        self.store = store
        self.probe = probe
        self.clock = clock or Clock()
        self.cleanup = cleanup
        self.scratch_usage = scratch_usage
        # Run at the send boundary with the claim's needs, after the cancel and window checks; it raises SendWithheld to withhold the RunTask.
        self.send_check = send_check

    def count_refusal(self, subject: Any, error: LaunchError) -> None:
        """Count a refused call of a family the launcher knows; the subject is the request or the launch that the call was given."""
        family = getattr(getattr(subject, "spec", subject), "job", "")
        if isinstance(family, str) and family in self.adapter.settings.families:
            count_refusal_kind(family, refusal_kind(error))

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
        if "job" not in problems and not (isinstance(spec.task_definition, str) and self.adapter.is_revision(spec.job, spec.task_definition)):
            problems.append("task_definition (the checked revision of the job's host definition)")
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
        return put_record(self.store, self.clock.wall(), event, attempt, base, **fields)

    def poll(self, arn: str) -> TaskReport:
        try:
            return self.adapter.poll(arn)
        except TaskUnavailable as error:
            raise LaunchUnavailable(f"the task cannot be read now ({error})") from None

    def lookup(self, claim: Mapping[str, Any]) -> TaskFound | None:
        """The task started under the identity of the claim's attempt, running or stopped. None only when ECS lists no such task."""
        try:
            return self.adapter.lookup(f"{claim['run_id']}:{claim['job']}:{claim['attempt']}", self.adapter.definition(str(claim["job"])))
        except TaskUnavailable as error:
            raise LaunchUnavailable(f"the task of attempt {claim['attempt']} cannot be looked up now ({error})") from None

    @staticmethod
    def began_of(found: TaskFound, claim: Mapping[str, Any]) -> str:
        """When the task began: the time ECS created it, else the time of the durable claim. Never the time of a recovery."""
        return iso(found.created_at) if found.created_at is not None else str(claim["at"])

    def judge(self, report: TaskReport, acceptance: Sequence[str]) -> tuple[str, int, str]:
        """The recorded outcome of a task shown STOPPED: outcome, exit code and reason."""
        if report.state not in ENDED:
            raise LaunchError(f"a task in state {report.state} has no outcome")
        if report.state == "succeeded":
            seen = dict(report.containers)
            missing = [name for name in acceptance if seen.get(name) != 0]
            if missing:
                return "failed", NO_CHECK_EXIT, f"acceptance_check_missing:{','.join(missing)}"
            return "succeeded", 0, report.reason
        return "failed", int(report.exit_code or CLOSED_EXIT), report.reason

    @counted
    def attach(self, spec: JobSpec) -> Launch:
        """Validate the request and reconcile it with the records and the cloud: an ended attempt, the recorded outcome of the run and job's one launch, the
        live task of that launch, an unresolved launch, or nothing yet. Nothing is launched here."""
        self.validate(spec)
        claim = self.store.run_record(spec.run_id, spec.job, "claim")
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
        decision = self.store.run_record(spec.run_id, spec.job, "decision")
        if decision is not None and decision["decision"] == "close":
            self.adopt_closure(launch, claim, decision)
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
        end = self.put("end", attempt, claim, **self.ended(spec, arn, report, outcome, code, reason))
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

    def unstopped_tasks(self, definition: str, consequence: str) -> list[str]:
        """The tasks of the family's host definition that ECS does not show STOPPED (running, pending or stopping). A listing that fails is no evidence."""
        try:
            return self.adapter.unstopped(definition)
        except TaskUnavailable as error:
            raise LaunchUnavailable(f"the tasks of {definition} cannot be listed now ({error}); {consequence}") from None
        except TaskRefused as error:
            raise LaunchRefused(f"the tasks of {definition} cannot be read ({error}); {consequence}") from None

    def require_family_free(self, spec: JobSpec) -> None:
        """Refuse a new launch, before its claim, while another launch of the family may use the family's host scratch folder (owner, Nov 7 2026): while
        ECS lists a task of the family's host definition that it does not show STOPPED (running, pending or stopping), or while a run in the family index
        has a claimed attempt with no end record (an index entry
        with no claim started nothing: a task starts only under a claim). A listing that fails refuses too. A claimed launch with no end record is settled
        here (``auto_settle``) and the gate goes on when that ends it, which needs proof about its task; a launch that stays
        unresolved refuses the run with its reason. The gate alone never ends a launch whose task ECS no longer describes or whose RunTask call may have
        been sent and found no task: a host stop that verifies no instance, no container instance and no unstopped task of the family does
        (``HostStopSettler``, rule 11 of the module docstring), and the refusal says so. Two runs that pass this gate at the
        same moment are kept apart by the Airflow pool of host jobs (a full-size job takes all its slots) and, when they both settle the same launch, by the
        write-once end record. The run is not claimed, so it launches once the earlier launch is settled."""
        definition = self.adapter.definition(spec.job)
        busy = self.unstopped_tasks(definition, "nothing was claimed")
        if busy:
            raise FamilyBusy(
                f"a task of {definition} has not stopped ({busy[0]}): the {spec.job} family shares one host scratch folder, which the next task empties, so "
                f"run {spec.run_id} waits until ECS shows that task STOPPED. Nothing was claimed"
            )
        for run in self.store.family_runs(spec.job):
            if run == spec.run_id:
                continue
            claim = self.store.run_record(run, spec.job, "claim")
            if claim is not None and "end" not in self.events(run, spec.job, int(claim["attempt"])):
                try:
                    self.auto_settle(run, spec.job, claim)
                except FamilyBusy as error:
                    raise FamilyBusy(f"{error}; run {spec.run_id} was not claimed") from None

    def admit(self, launch: Launch) -> bool:
        """Claim the one launch of the run and job with a conditional create, the single step that lets a caller start a task. Returns False when another
        caller holds the claim. The claim that stands is checked against this request before anything else happens."""
        spec = launch.spec
        self.validate(spec)  # a stamp-form run ID is history to read: no claim or family index entry is ever created under it
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
        self.store.index_family(body)  # before the claim, so a claimed launch is always in the index; an entry with no claim started nothing
        stored = self.store.put_once(body)
        self.check_bound(spec, stored)
        if stored["attempt"] != spec.attempt:
            return False
        launch.claim = stored
        return True

    @counted
    def start(self, launch: Launch) -> None:
        """Claim the launch, record the intent, check that the job fits, run the task once and record it. When another caller holds the claim, its launch is
        settled instead: the task of an unresolved launch is looked up by its identity and, if none exists, started under that identity, so a second task
        never starts. A cancel request that arrives while the task starts is honored as soon as the task is known."""
        spec = launch.spec
        if launch.outcome is not None or launch.arn is not None:
            return
        self.validate(spec)  # before the gate settles any other run: a launch that reaches here without ``attach`` takes the run ID form alone too
        if launch.claim is None and self.store.run_record(spec.run_id, spec.job, "claim") is None:
            self.require_family_free(spec)
        if launch.claim is None and not self.admit(launch):
            self.settle(launch, self.store.run_record(spec.run_id, spec.job, "claim"), self.events(spec.run_id, spec.job, spec.attempt))
            if launch.outcome is not None or launch.arn is not None:
                return
        claim = launch.claim
        if claim is None:
            raise LaunchError("the claim is missing")
        self.check_bound(spec, claim)
        self.record_intent(claim)
        launch.deadline_seconds = claim["deadline_seconds"]
        found = self.run_when_it_fits(launch, claim)
        if found is None:
            return
        stored = self.record_started(claim, *found)
        if claim["attempt"] == spec.attempt:
            launch.arn, launch.started_at = stored["task_arn"], stamp(stored["job_started_at"])
        else:
            self.join(launch, claim, stored["task_arn"], stored["job_started_at"])
        if self.cancel_requested(spec.run_id, spec.job):
            self.confirm_stop(stored["task_arn"], "cancel")
            end = self.finalize(claim, claim["attempt"], spec.attempt, "cancelled", CANCEL_EXIT, "cancel", stored["task_arn"])
            launch.outcome = outcome_of(end, adopted=claim["attempt"] != spec.attempt)

    def cancel_requested(self, run_id: str, job: str) -> bool:
        return self.store.run_record(run_id, job, "cancel") is not None

    def run_when_it_fits(self, launch: Launch, claim: dict[str, Any]) -> tuple[str, str] | None:
        """Return the ARN and start time of the task under the claim, started now or found by identity. Returns None when the launch was closed.

        One decision record, created with a conditional write, is the authority over two things that exclude each other: sending a RunTask call (the send
        token) and closing without a task (the close token). A caller sends only after it holds the send token, and closes only by winning the close token or by
        handing back its own send token when every call it sent was answered. When another caller holds the send token, a RunTask call may have been sent, and
        only positive evidence settles it: the task of the launch's identity found and settled. No timer and no empty listing proves that nothing started.

        RunTask is sent only before the end of the window (``FIT_WAIT_SECONDS`` after the claim) and never after a cancel request, and both are checked
        immediately before every call, after the capacity and discovery reads. The host's room is checked against the claim's needs for every caller."""
        spec = launch.spec
        run_id, job = claim["run_id"], claim["job"]
        key = f"{run_id}:{job}:{claim['attempt']}"
        need = Resources(**claim["need"])
        end = stamp(claim["at"]) + FIT_WAIT_SECONDS
        last = ""
        while True:
            decision = self.store.run_record(run_id, job, "decision")
            if decision is not None and decision["decision"] == "close":
                self.adopt_closure(launch, claim, decision)
                return None
            if decision is not None and decision["holder"] != launch.holder:
                found = self.lookup(claim)
                if found is not None:
                    return found.arn, self.began_of(found, claim)
                raise self.unresolved(claim)
            if decision is not None and launch.ambiguous:
                try:
                    found = self.lookup(claim)
                except LaunchUnavailable as error:
                    last = str(error)
                else:
                    if found is not None:
                        return found.arn, self.began_of(found, claim)
            cancelled = self.cancel_requested(run_id, job)
            if cancelled or self.clock.wall() >= end:
                self.give_up(launch, claim, cancelled, last)
                return None
            fits = True
            try:
                free = self.probe.free()
                held = dict(self.scratch_usage()) if self.scratch_usage is not None else {}
            except TaskUnavailable as error:
                fits, last = False, f"host capacity unreadable ({error})"
            except TaskRefused as error:
                self.give_up(launch, claim, False, "", f"fit_probe_refused:{error}")
                return None
            except OSError as error:
                fits, last = False, f"scratch folders unreadable ({type(error).__name__})"
            else:
                credited = Resources(free.cpu_units, free.memory_mib, free.scratch_bytes + max(0, int(held.get(job, 0))))
                short = shortfalls(need, credited)
                others = {family: size for family, size in held.items() if family != job and size > 0}
                if short and need.scratch_bytes > credited.scratch_bytes and others:
                    short.append("scratch held by other families: " + ", ".join(f"{family} {size / 2**30:.1f} GiB" for family, size in sorted(others.items())))
                if short:
                    fits, last = False, "; ".join(short)
            if fits:
                if decision is None and not self.win_send(launch, claim):
                    continue
                try:
                    arn = self.adapter.start(
                        spec.job, key, spec.environment, guard=partial(self.send_guard, run_id, job, end, need), revision=spec.task_definition
                    )
                    return arn, iso(self.clock.wall())
                except SendWithheld:
                    continue
                except CapacityUnavailable as error:
                    last = f"RunTask answered {error}"
                except SendUnknown as error:
                    launch.ambiguous, last = True, f"the RunTask reply is unknown ({error})"
                except TaskUnavailable as error:
                    last = f"RunTask was not sent ({error})"
                except TaskRefused as error:
                    self.give_up(launch, claim, False, "", f"launch_refused:{error}")
                    return None
            self.clock.sleep(FIT_POLL_SECONDS)

    def guard(self, run_id: str, job: str, end: float) -> None:
        """Called immediately before every RunTask call. A cancel request or the end of the window withholds the call."""
        if self.cancel_requested(run_id, job) or self.clock.wall() >= end:
            raise SendWithheld

    def send_guard(self, run_id: str, job: str, end: float, need: Resources) -> None:
        """The guard of every RunTask call: ``guard``, then ``send_check`` with the claim's needs, which raises ``SendWithheld`` to withhold it."""
        self.guard(run_id, job, end)
        if self.send_check is not None:
            self.send_check(need)

    def unresolved(self, claim: Mapping[str, Any], prefix: str = "") -> LaunchUnavailable:
        """The launch may have a task that has not been found. Nothing closes it and nothing else starts until the next launch of the family settles it with
        evidence about the task, or a host stop that finds the host empty settles it (``HostStopSettler``)."""
        key = f"{claim['run_id']}:{claim['job']}:{claim['attempt']}"
        return LaunchUnavailable(
            f"{prefix}A RunTask call may have been sent for attempt {claim['attempt']} of {claim['job']} in run {claim['run_id']} and no task has been "
            f"found, so the launch stays unresolved and nothing else will start. The next launch of the family settles it when it finds the task: it stops "
            f"the task, confirms the stop and records the end, or a host stop settles it ({HOST_STOP_NOTE}). No timer and no empty listing ends the launch. "
            f"The task, if one exists, carries the tag "
            f"{self.adapter.identity(key)} (the client token is the SHA-256 of {key})."
        )

    def win_send(self, launch: Launch, claim: Mapping[str, Any]) -> bool:
        """Win the send token: the conditional create of the decision record. False when another caller holds the send token or closed the launch."""
        spec = launch.spec
        record = {
            "schema_version": 1,
            "event": "decision",
            "run_id": claim["run_id"],
            "job": claim["job"],
            "attempt": spec.attempt,
            "at": iso(self.clock.wall()),
            "decision": "send",
            "holder": launch.holder,
        }
        stored = self.store.put_once(record)
        return bool(stored["decision"] == "send" and stored["holder"] == launch.holder)

    def give_up(self, launch: Launch, claim: Mapping[str, Any], cancelled: bool, last: str, reason: str = "") -> None:
        """The launch will not send: a cancel was requested, the window is over or the call failed for good. It closes without a task."""
        if cancelled:
            self.close_without_task(launch, claim, "cancelled", CANCEL_EXIT, "cancelled before the task started")
        elif reason:
            self.close_without_task(launch, claim, "failed_closed", CLOSED_EXIT, reason)
        else:
            why = f"does_not_fit after {FIT_WAIT_SECONDS:g} s: {last}" if last else "no task was started within the launch window"
            self.close_without_task(launch, claim, "failed_closed", CLOSED_EXIT, why)

    def close_without_task(self, launch: Launch, claim: Mapping[str, Any], outcome: str, code: int, reason: str) -> None:
        """Close the launch without a task, which only the close token allows. A caller without the send token wins the close token with a conditional create.
        The holder of the send token may hand it back with a version-checked replacement, only when every call it sent was answered. In every other case a
        RunTask call may have been sent, so the launch stays unresolved."""
        spec = launch.spec
        record = {
            "schema_version": 1,
            "event": "decision",
            "run_id": claim["run_id"],
            "job": claim["job"],
            "attempt": spec.attempt,
            "at": iso(self.clock.wall()),
            "decision": "close",
            "outcome": outcome,
            "exit_code": code,
            "reason": reason[:REPORT_LIMIT],
        }
        current = self.store.decision(claim["run_id"], claim["job"])
        if current is None:
            stored = self.store.put_once(record)
            if stored["decision"] != "close":
                raise self.unresolved(claim)
        elif current[0]["decision"] == "close":
            stored = current[0]
        elif current[0]["holder"] == launch.holder and not launch.ambiguous and self.store.replace_decision(record, current[1]):
            stored = record
        else:
            raise self.unresolved(claim)
        self.adopt_closure(launch, claim, stored)

    def adopt_closure(self, launch: Launch, claim: Mapping[str, Any], decision: Mapping[str, Any]) -> None:
        """The launch was closed without a task. The decision is the one outcome: it becomes the end record of the attempt that holds the claim."""
        owner = claim["attempt"]
        end = self.finalize(claim, owner, launch.spec.attempt, decision["outcome"], decision["exit_code"], decision["reason"], "")
        launch.outcome = outcome_of(end, adopted=owner != launch.spec.attempt)

    def finalize(
        self, base: Mapping[str, Any], owner: int, mine: int, outcome: str, code: int, reason: str, arn: str, failure: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        """``write_end`` with this launcher's store, clock and families."""
        return write_end(self.store, self.clock.wall(), self.adapter.settings.families, base, owner, mine, outcome, code, reason, arn, failure)

    def close(self, launch: Launch, outcome: str, code: int, reason: str, failure: Mapping[str, Any] | None = None) -> None:
        """Record how this attempt ended, through the end record of the attempt that holds the claim."""
        spec = launch.spec
        owner = launch.owner if launch.owner is not None else spec.attempt
        end = self.finalize({"run_id": spec.run_id, "job": spec.job}, owner, spec.attempt, outcome, code, reason, launch.arn or "", failure)
        launch.outcome = outcome_of(end)

    @counted
    def wait(self, launch: Launch) -> None:
        """Poll the task until it is shown STOPPED or its recorded deadline passes. Past the deadline the task is stopped, with the stop confirmed. A reply
        that describes no task counts as a failed read: it says nothing about how the task ended."""
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
                        f"the task could not be read {failures} times in a row ({error}); it may keep running, call again to reattach"
                    ) from None
            else:
                failures = 0
                if report.state in ENDED:
                    launch.report = report
                    return
            if self.clock.wall() > due:
                launch.report = self.confirm_stop(launch.arn, "deadline")  # kept for the tail of the timeout: which phase was active
                launch.timed_out = True
                return
            self.clock.sleep(POLL_SECONDS)

    def confirm_stop(self, arn: str, reason: str) -> TaskReport:
        """Send the stop and wait until the task is shown STOPPED, and return its stopped report. A read that fails, including a reply that describes no task,
        records nothing."""
        try:
            self.adapter.stop(arn, reason)
        except TaskUnavailable as error:
            raise LaunchUnavailable(f"the stop could not be sent now ({error})") from None
        except TaskRefused as error:
            raise StopFailed(f"the cloud refused to stop the task ({error}). Nothing was recorded, so the stop can be repeated") from None
        end = self.clock.monotonic() + STOP_WAIT_SECONDS
        report = self.poll(arn)
        while report.state not in ENDED:
            if self.clock.monotonic() >= end:
                raise StopFailed(f"the task is still stopping after {STOP_WAIT_SECONDS:g} seconds. Nothing was recorded, so the stop can be repeated")
            self.clock.sleep(STOP_POLL_SECONDS)
            report = self.poll(arn)
        return report

    @counted
    def conclude(self, launch: Launch) -> Outcome:
        """Record how the attempt ended, tail the logs of a failure and clean up after a success. An attempt that already has an outcome returns it."""
        if launch.outcome is not None:
            return launch.outcome
        spec = launch.spec
        diagnosis: Diagnosis | None = None
        if launch.timed_out:
            self.close(launch, "timeout", TIMEOUT_EXIT, "deadline")
        elif launch.report is not None:
            if launch.report.state != "succeeded" and self.cancel_requested(spec.run_id, spec.job):
                self.close(launch, "cancelled", CANCEL_EXIT, "cancel")
            else:
                verdict, code, reason = self.judge(launch.report, spec.acceptance)
                diagnosis = self.diagnose(spec, launch.arn or "", launch.report) if verdict == "failed" else None
                record = diagnosis.record if diagnosis else None
                code, reason = self.agreed(code, reason, record)
                self.close(launch, verdict, code, reason, record)
        else:
            raise LaunchError("the attempt has not ended")
        outcome = launch.outcome
        if outcome is None:
            raise LaunchError("the end record is missing")
        if not outcome.ok:
            logs = diagnosis.logs if diagnosis is not None else self.tail(launch)
            return Outcome(outcome.outcome, outcome.exit_code, outcome.reason, outcome.task_arn, failure=outcome.failure, logs=logs)
        note = ""
        if self.cleanup is not None:
            try:
                self.cleanup(spec)
            except CleanupFailed as error:
                note = str(error)
        return Outcome(outcome.outcome, outcome.exit_code, outcome.reason, outcome.task_arn, cleanup_note=note)

    def ended(self, spec: JobSpec, arn: str, report: TaskReport, outcome: str, code: int, reason: str) -> dict[str, Any]:
        """The fields of the end record of a task shown STOPPED: its outcome, exit code, reason and ARN, and the closed failure record of a failure."""
        record = self.diagnose(spec, arn, report).record if outcome == "failed" else None
        code, reason = self.agreed(code, reason, record)
        return {"outcome": outcome, "exit_code": code, "reason": reason[:REPORT_LIMIT], "task_arn": arn, **({"failure": record} if record else {})}

    @staticmethod
    def agreed(code: int, reason: str, record: Mapping[str, Any] | None) -> tuple[int, str]:
        """The exit code and the plain reason of an end record that holds a closed failure record: ECS's code of the container that caused the failure and the
        message of the record. The task's own first non-zero code and stopped reason describe the task, not the cause: a phase that fails stops the
        containers before it with SIGTERM (143), and ECS words that stop "Task failed to start". A failure with no record keeps both."""
        if record is None:
            return code, reason
        return record["facts"].get("exit_code", code), render_failure(record)

    def cause(self, spec: JobSpec, arn: str, report: TaskReport) -> tuple[str, str] | None:
        """The one container that caused a failed task, and how it is known: ``job`` (it exited with an error of its own), ``task`` (ECS reports it with no exit
        code, or stopped it) or ``guard`` (the guard reports its own time limit). A primary phase failure beats a container that ECS stopped with SIGTERM when
        another one failed. A phase that never ran and has a reason code from ECS comes next. The guard is the cause only for its own time limit: it also exits
        with an error to stop a task whose phase failed, and then that phase is the cause. Among containers of one kind the earlier one in the task definition
        wins, so the order of ECS's reply never decides. No cause gives None."""
        reasons = dict(report.reasons)
        place = {name: index for index, name in enumerate(spec.environment)}

        def first(names: list[str]) -> str:
            return min(names, key=lambda name: (place.get(name, len(place)), name))

        phases = [(name, code) for name, code in report.containers if name != GUARD]
        primary = [n for n, c in phases if c not in (0, None) and c not in SHUTDOWN_CODES and (c != KILLED_CODE or reasons.get(n) == "container_out_of_memory")]
        if primary:
            return first(primary), "job"
        unrun = [n for n, c in phases if c is None and reasons.get(n)]
        if unrun:
            return first(unrun), "task"
        if dict(report.containers).get(GUARD) not in (0, None) and self.guard_timed_out(spec, arn):
            return GUARD, "guard"
        stopped = [n for n, c in phases if c in SHUTDOWN_CODES or c == KILLED_CODE]
        return (first(stopped), "task") if stopped else None

    def guard_timed_out(self, spec: JobSpec, arn: str) -> bool:
        """Whether the guard's own log holds its fixed time limit line. Only that fixed line is compared, and nothing of the log is kept."""
        return GUARD_TIMEOUT_LINE in self.read_log(arn, GUARD, spec).splitlines()

    def read_log(self, arn: str, container: str, spec: JobSpec) -> str:
        if not (arn and container):
            return ""
        try:
            return self.adapter.tail(arn, container, spec.log_prefix, spec.log_lines)
        except (TaskUnavailable, TaskRefused):
            return ""

    def diagnose(self, spec: JobSpec, arn: str, report: TaskReport) -> Diagnosis:
        """The closed failure record of a failed task and the immediate log tail, both from the one container that caused it (``cause``). The record is the
        container's own closed record when its log ends with one, else one made from the state of the task alone; no text of a log, an exception or a container
        reason is kept. A task with no identifiable cause has no record, and its tail is the job's log container."""
        found = self.cause(spec, arn, report)
        if found is None:
            return Diagnosis(None, scrub(self.read_log(arn, spec.log_container, spec)))
        container, source = found
        log = self.read_log(arn, container, spec)
        exits, reasons = dict(report.containers), dict(report.reasons)
        code = exits.get(container)
        # The exit code is ECS's, never the job's. A job's own line counts only when its count of other containers fits this task.
        facts = {"exit_code": code} if type(code) is int and 1 <= code <= 255 else {}
        name = container if container in FAILURE_PHASES else "unknown"
        marker = failure_from_tail(log) if source == "job" else None
        if marker is not None and marker["facts"].get("siblings", 0) > len(report.containers) - 1:
            marker = None
        if marker is not None:
            record = {**marker, "facts": {**marker["facts"], **facts}, "container": name, "source": "job"}
        else:
            if source == "guard":
                codes = ["guard_timeout"]
            elif container in reasons:
                codes = [reasons[container]]
            else:
                codes = ["phase_failed" if source == "job" else "phase_stopped"]
            record = {"v": 1, "phase": name, "codes": codes, "facts": facts, "container": name, "source": "guard" if source == "guard" else "task"}
        return Diagnosis(stored_failure(record), scrub(log))

    def tail(self, launch: Launch) -> str:
        """The last log lines of the phase that was active when a timeout or a cancel stopped the task: the first container, in task order, that ended with a
        code other than zero (the stopped phase, or one that failed) and has a log. The job's log container is the last one and may never have run, so it
        comes only when no such log exists. A failed task uses ``diagnose``."""
        spec, arn = launch.spec, launch.arn or ""
        place = {name: index for index, name in enumerate(spec.environment)}
        stopped = [name for name, code in (launch.report.containers if launch.report else ()) if name != GUARD and code not in (0, None)]
        for container in sorted(stopped, key=lambda name: (place.get(name, len(place)), name)):
            found = self.read_log(arn, container, spec)
            if found:
                return scrub(found)
        return scrub(self.read_log(arn, spec.log_container, spec))

    def run(self, spec: JobSpec) -> Outcome:
        """Attach, start, wait and conclude one attempt. A repeat of an ended attempt returns its recorded outcome."""
        launch = self.attach(spec)
        self.start(launch)
        self.wait(launch)
        return self.conclude(launch)

    def auto_settle(self, run_id: str, job: str, claim: Mapping[str, Any] | None = None) -> Outcome:
        """Settle a launch that has no end record, with no statement of the owner (automatic launch recovery). It cancels: the cancel request is recorded, so no
        sender of the launch sends again after its next check, and the launch ends only with proof about its task: the task is found by identity and its stop is
        confirmed (``cancelled``), or no RunTask call was ever sent and the close token is won (``cancelled`` without a task). Otherwise the launch stays
        unresolved and this raises ``FamilyBusy`` with the launch's run, job, attempt and task identity: when a saved task's stop cannot be confirmed (ECS does
        not describe it, or the stop is not confirmed in time) the message also holds the saved ARN, and when a RunTask call may have been sent and no task is
        found the end of the send window and an empty family listing prove nothing, because ECS lists a new task late and a request that passed its guard may
        still be in flight. A launch record that cannot be read or written now, at any step of the settle, is a temporary failure, not a refusal: it raises
        ``RecordStoreUnavailable`` (a ``LaunchUnavailable``) with the launch's identity as far as it is known, claims no cancel and no end, and the same call
        repeats the settle. The caller may pass the claim it read (a claim is write-once), which the settle then does not read again. A repeat, or a second
        caller at the same moment, reads the one end record, because every record is write-once. A launch this leaves unresolved is ended only by a host
        stop with the proof of ``HostStopSettler``; the ``FamilyBusy`` message says so."""
        try:
            if claim is None:
                claim = self.store.run_record(run_id, job, "claim")
            if claim is None:
                raise LaunchRefused(f"no launch of {job} in run {run_id}: nothing to settle")
            attempt = int(claim["attempt"])
            try:
                return self.cancel(run_id, job, attempt, reason="automatic settle")
            except RecordStoreUnavailable:
                raise
            except (LaunchUnavailable, StopFailed) as error:
                started = self.events(run_id, job, attempt).get("started")
                who = self.blocking_launch(run_id, job, attempt, started)
                if started is not None:
                    raise FamilyBusy(
                        f"{who} has no end record and was not settled: its task is saved in the launch records and its stop could not be confirmed "
                        f"({error}). {self.cancel_note(run_id, job)}The run launches once ECS describes that task STOPPED, which the next launch of the "
                        f"family then records, or once a host stop settles it: {HOST_STOP_NOTE}"
                    ) from None
            raise FamilyBusy(
                f"{who} has no end record and was not settled: a RunTask call may have been sent and no task of it is found, and neither the end of its send "
                f"window nor an empty listing proves that none exists. {self.cancel_note(run_id, job)}The run launches once ECS lists that task, which the "
                f"next launch of the family then stops and records, or once a host stop settles it: {HOST_STOP_NOTE}"
            )
        except RecordStoreUnavailable as error:
            attempt_known = int(claim["attempt"]) if claim is not None else None
            saved = self.saved_task(run_id, job, attempt_known)
            who = self.blocking_launch(run_id, job, attempt_known, saved) if attempt_known is not None else f"the {job} launch of run {run_id}"
            raise RecordStoreUnavailable(
                f"{who} was not confirmed settled: a launch record could not be read or written ({error}). This may be temporary: call the launch again "
                "and the family gate settles it or reads its end record"
            ) from None

    def saved_task(self, run_id: str, job: str, attempt: int | None) -> Mapping[str, Any] | None:
        """The started record of the attempt when it can be read now, for a diagnostic: a read that fails says nothing."""
        if attempt is None:
            return None
        try:
            return self.events(run_id, job, attempt).get("started")
        except RecordStoreUnavailable:
            return None

    def blocking_launch(self, run_id: str, job: str, attempt: int, started: Mapping[str, Any] | None = None) -> str:
        """The old launch as a refusal names it: its run, job and attempt, its task identity and, when its started record is saved, its task ARN."""
        task = f"task {started['task_arn']}, " if started is not None else ""
        return f"the {job} launch of run {run_id} (attempt {attempt}; {task}identity {self.adapter.identity(f'{run_id}:{job}:{attempt}')})"

    def cancel_note(self, run_id: str, job: str) -> str:
        """The sentence that says the cancel is recorded, only when the record is read back."""
        try:
            recorded = self.store.run_record(run_id, job, "cancel") is not None
        except RecordStoreUnavailable:
            return ""
        return "The cancel is recorded, so no sender sends again after its next check. " if recorded else ""

    def cancel(self, run_id: str, job: str, attempt: int, reason: str = "cancel") -> Outcome:
        """Cancel the run and job's launch. A durable cancel request comes first, so a sender that holds the send token sees it before every RunTask call and
        stops a task it has started. Without a send token the cancel wins the close token and records ``cancelled`` with no task. With one, a RunTask call may
        have been sent, so ``cancelled`` is recorded only after the task is found by identity and its stop is confirmed; until then this raises
        ``LaunchUnavailable`` and the launch stays unresolved. The end record of the attempt that holds the claim is the one outcome."""
        claim = self.store.run_record(run_id, job, "claim")
        if claim is None:
            raise LaunchRefused(f"no launch of {job} in run {run_id}: nothing to cancel")
        owner = claim["attempt"]
        mine = self.events(run_id, job, attempt)
        theirs = mine if attempt == owner else self.events(run_id, job, owner)
        if "end" in theirs:
            stored = theirs["end"]
            if attempt != owner and "end" not in mine:
                self.put("end", attempt, claim, **copied(stored))
            return outcome_of(stored, adopted=attempt != owner)
        self.put("cancel", attempt, claim, reason=reason[:REPORT_LIMIT])
        if "started" in theirs:
            arn = theirs["started"]["task_arn"]
        else:
            decision = self.store.run_record(run_id, job, "decision")
            if decision is None:
                closing = {
                    "schema_version": 1,
                    "event": "decision",
                    "run_id": run_id,
                    "job": job,
                    "attempt": attempt,
                    "at": iso(self.clock.wall()),
                    "decision": "close",
                    "outcome": "cancelled",
                    "exit_code": CANCEL_EXIT,
                    "reason": reason[:REPORT_LIMIT],
                }
                decision = self.store.put_once(closing)
            if decision["decision"] == "close":
                end = self.finalize(claim, owner, attempt, decision["outcome"], decision["exit_code"], decision["reason"], "")
                return outcome_of(end, adopted=attempt != owner)
            found = self.lookup(claim)
            if found is None:
                raise self.unresolved(claim, "The cancel is recorded, and the launch is unresolved. ")
            self.record_intent(claim)
            arn = self.record_started(claim, found.arn, self.began_of(found, claim))["task_arn"]
        self.confirm_stop(arn, reason)
        end = self.finalize(claim, owner, attempt, "cancelled", CANCEL_EXIT, reason[:REPORT_LIMIT], arn)
        return outcome_of(end, adopted=attempt != owner)


@dataclass(frozen=True)
class HostStopProof:
    """What the host session read to show that the host is stopped, each read with its time in epoch seconds. The caller reads; ``HostStopSettler`` judges.

    The group was read first (``desired_capacity`` and ``group_instances``), then the ACTIVE container instances of the cluster
    (``active_container_instances``), then the tasks of the project's host definitions that ECS does not show STOPPED: ``busy_jobs`` holds the job of every
    such task, and every job when a task cannot be tied to one job. A read that failed gives no proof, so the caller builds none."""

    desired_capacity: int
    group_instances: int
    group_read_at: float
    active_container_instances: int
    instances_read_at: float
    tasks_read_at: float
    busy_jobs: frozenset[str]

    def refusal(self, job: str, now: float, claim_at: float) -> str:
        """Why this proof does not end the launch of ``job`` claimed at ``claim_at`` (epoch seconds), or an empty string when it does."""
        first_read = min(self.group_read_at, self.instances_read_at, self.tasks_read_at)
        needed = FIT_WAIT_SECONDS + SETTLE_MARGIN_SECONDS
        if self.desired_capacity != 0 or self.group_instances != 0:
            return f"the host group shows desired capacity {self.desired_capacity} and {self.group_instances} instance(s)"
        if self.active_container_instances != 0:
            return f"ECS lists {self.active_container_instances} ACTIVE container instance(s)"
        if self.instances_read_at < self.group_read_at:
            return "the container instances were read before the host group"
        if job in self.busy_jobs:
            return f"ECS lists a task of the {job} host definition that is not STOPPED"
        if now - first_read > PROOF_MAX_AGE_SECONDS:
            return f"the host was read {int(now - first_read)} s ago, more than {int(PROOF_MAX_AGE_SECONDS)} s"
        if first_read - claim_at < needed:
            return (
                f"its claim is {int(first_read - claim_at)} s old and a RunTask request that passed its guard may still be in flight until it is "
                f"{int(needed)} s old (the {int(FIT_WAIT_SECONDS)} s send window plus {int(SETTLE_MARGIN_SECONDS)} s)"
            )
        return ""


@dataclass(frozen=True)
class SettleResult:
    """What one settle did to one claimed launch with no end record: ``settled`` (``cancelled`` is recorded, by this call or by an equal call),
    ``ended_by_other`` (another caller's end record stands) or ``unresolved`` (nothing was recorded; ``reason`` says which part of the proof is missing).
    ``saved_task`` is the task ARN of the launch's started record, or empty. ``ended_at`` is the time of the end record that stands, or empty.
    ``settled`` means the standing end record is a host stop cancel (its reason is ``HOST_STOP_REASON``), whoever wrote it."""

    run_id: str
    job: str
    attempt: int
    status: str
    reason: str
    saved_task: str
    ended_at: str


class HostStopSettler:
    """Ends claimed launches that have no end record, as ``cancelled``, on the proof that the host is stopped (owner decision A, Nov 24 2026, rule 11).

    The gate (``Launcher.auto_settle``) cannot end a launch whose task ECS no longer describes, or whose RunTask call may have been sent and found no
    task: only positive evidence about the task ends it, and no timer or empty listing is that. A stopped host is the evidence. The host session reads that
    the group has capacity 0 and no instance, that ECS lists no ACTIVE container instance and no task of the host definition that is not STOPPED
    (``HostStopProof``), and the claim is older than the send window plus ``SETTLE_MARGIN_SECONDS``, so no request that passed its guard can still land.
    The proof is read once and a scan of the records takes time, so ``reread`` reads the host again immediately before each end write (it returns None when
    the host group shows capacity) and that fresh proof, judged at that moment, must refuse nothing. The settler writes the write-once ``end`` record through
    ``write_end``, as ``Launcher.cancel`` does, after the durable ``cancel`` request (``HOST_STOP_REASON``, write-once, so one already there stands). The
    cancel is written after the reread passes and before the end: a sender that gets a late RunTask reply sees it at its next check and stops its own task.
    The settler calls no cloud service but the record store, so a repeat or a second settler reads the one end record. A record store failure raises
    ``RecordStoreUnavailable`` and the same call repeats the settle.

    Accepted limit (owner, Nov 24 2026): the proof is a set of reads, not a lock. A host start by another session, or a RunTask request placed more than 20
    minutes late, in the seconds between the final reread and the end write, could leave a cancelled end record for a running task. The operating rule is one
    host session at a time, and the cancel request is the only added protection (no lease or lock)."""

    def __init__(self, store: LaunchStore, clock: Clock | None, families: Collection[str], reread: Callable[[], HostStopProof | None]) -> None:
        self.store = store
        self.clock = clock or Clock()
        self.families = frozenset(families)
        self.reread = reread

    def settle_family(self, job: str, proof: HostStopProof) -> Iterator[SettleResult]:
        """Settle each claimed launch of the family index of ``job`` that has no end record, one result each. A launch with an end record is not reported."""
        for run in self.store.family_runs(job):
            claim = self.store.run_record(run, job, "claim")
            if claim is None:
                continue
            attempt = int(claim["attempt"])
            if self.store.end_record(run, job, attempt) is not None:
                continue
            events = {record["event"]: record for record in self.store.records(run, job, [attempt])}
            if "end" not in events:
                yield self.settle_launch(claim, events, proof)

    def settle_launch(self, claim: Mapping[str, Any], events: Mapping[str, Mapping[str, Any]], proof: HostStopProof) -> SettleResult:
        """Settle the launch of ``claim``, whose own attempt's records read ``events`` (no end among them)."""
        run, job, attempt = str(claim["run_id"]), str(claim["job"]), int(claim["attempt"])
        saved = str(events["started"]["task_arn"]) if "started" in events else ""
        claim_at = stamp(str(claim["at"]))
        why = proof.refusal(job, self.clock.wall(), claim_at)
        if not why:
            fresh = self.reread()
            why = "the host group shows capacity" if fresh is None else fresh.refusal(job, self.clock.wall(), claim_at)
            why = f"the host changed before the end was written: {why}" if why else ""
        if why:
            return SettleResult(run, job, attempt, "unresolved", why, saved, "")
        put_record(self.store, self.clock.wall(), "cancel", attempt, claim, reason=HOST_STOP_REASON[:REPORT_LIMIT])
        end = write_end(self.store, self.clock.wall(), self.families, claim, attempt, attempt, "cancelled", CANCEL_EXIT, HOST_STOP_REASON, saved)
        ours = end["outcome"] == "cancelled" and end["reason"] == HOST_STOP_REASON
        reason = HOST_STOP_REASON if ours else f"{end['outcome']}: {end['reason']}"
        return SettleResult(run, job, attempt, "settled" if ours else "ended_by_other", reason, saved, str(end["at"]))
