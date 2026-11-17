"""The state of the AWS catalog setup job, kept where the job runs: write-once S3 records, a leased lock per principal and the host launch hold.

The setup runs as a one-off task on the host, so it has no checkout folder to keep local state in. Three things lived there and live here now:

- The namespace approvals are write-once objects ``<prefix>/approvals/<identity>.json``, created with ``IfNoneMatch="*"``. A second approval for the same
  identity never replaces the first.
- The lock of one principal (P11) is the lease object ``<prefix>/locks/<role>.json``. It is created with ``IfNoneMatch="*"``, replaced only with
  ``IfMatch`` on the version that was read (after its own expiry) and released by writing its expiry as zero at the version this run holds. Its expiry is
  the step's absolute deadline plus the longest a pair write past that deadline is awaited, so a lease that has expired belongs to a run that can no longer
  change anything. A crashed task leaves its lease; the next run waits for it, at most to the end of its own wait bound, then refuses with the expiry time.
- The mark of an unconfirmed pair write is the object ``<prefix>/pending/<role>.json``, which holds the time until which the write may still commit.
- The launch hold (P5) is the services apply lock parameter, which every host launch honours. The setup creates it without overwrite with a token that
  ``services_apply resolve`` accepts, waits the longest time a launch admitted before the hold can still need to reach RunTask, proves that no job task of
  the project is without a confirmed end (a task ECS cannot describe is unknown, and unknown is never safe), and deletes the hold only with its own token.
  After a pair write that may still commit the hold stays, so no job starts on an unknown pair. A services apply that holds the parameter stops the setup.

Every request is bounded by the step's absolute deadline: each client is built with connect and read timeouts that are no longer than the time left, and no
call starts after the deadline. The writes that undo or mark something (lease release, pending mark, hold release) take their own short window instead.

Nothing here holds a secret: records and parameters name identities, roles, times and random tokens only.
"""

from __future__ import annotations

import contextlib
import json
import os
import secrets
import sys
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError, ConnectTimeoutError, ReadTimeoutError

PREFIX = "lakehouse/catalog_setup"
POLL_SECONDS = 0.2
# The most takeovers of an expired lease one acquisition tries before it waits like any other holder.
MAX_TAKEOVERS = 3
# The longest a pair write past its deadline may still commit: botocore's own bound for one call (secret_store.CLIENT_OPTIONS) plus the wait for it.
WRITE_MAY_COMMIT_SECONDS = 90.0
CONNECT_SECONDS = 5.0
READ_SECONDS = 10.0
# The wait after the hold is taken. A launch that read the hold as clear within orchestration.ecs_preflight.SEND_AUTHORIZATION_SECONDS (10 s) before it was set
# can still send RunTask, which orchestration.ecs_adapter.send_config answers within 5 s to connect and 10 s to read: 25 s in all. This is the 30 s that
# scripts.infrastructure.services_apply.HOLD_SETTLE_SECONDS waits for the same window; the E2E compares all four numbers.
DRAIN_SECONDS = 30.0
# ECS lists and describes tasks with eventual consistency, so a task that cannot be described yet is unknown, not ended.
UNKNOWN_WAIT_SECONDS = 30.0
UNKNOWN_POLL_SECONDS = 2.0
# The window of a write that undoes or marks something and so must run even when the step deadline has passed.
RELEASE_SECONDS = 10.0
DESCRIBE_LIMIT = 100
MAX_PAGES = 50
GROUP_PREFIX = "family:"
TERMINAL = ("STOPPED", "DELETED")
DESIRED_STATUSES = ("RUNNING", "STOPPED")
MISSING_CODES = ("NoSuchKey", "404", "NotFound")
REFUSED_CODES = ("PreconditionFailed", "412", "ConditionalRequestConflict", "409")
TIMEOUTS = (ConnectTimeoutError, ReadTimeoutError)
HOLD_HOLDER = "catalog-setup"
RESOLVE = "services_apply resolve --statement '<what you checked>' --token"
Clock = Callable[[], float]
Clients = Callable[[str, float], Any]


class StateError(RuntimeError):
    """A state step failed; ``code`` names the step and the message holds no value. ``unconfirmed`` is true when a write may still have happened."""

    def __init__(self, code: str, message: str, *, unconfirmed: bool = False) -> None:
        super().__init__(message)
        self.code, self.unconfirmed = code, unconfirmed


def error_code(error: ClientError | BotoCoreError) -> str:
    return str(error.response.get("Error", {}).get("Code", "unknown")) if isinstance(error, ClientError) else type(error).__name__


def utc(seconds: float) -> str:
    return datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def client_options(left: float) -> dict[str, Any]:
    """The botocore options of one call: connect and read limits that are no longer than the time left, and one attempt."""
    return {"connect_timeout": min(CONNECT_SECONDS, left), "read_timeout": min(READ_SECONDS, left), "retries": {"total_max_attempts": 1, "mode": "standard"}}


class Deadline:
    """One absolute deadline on a monotonic clock."""

    def __init__(self, end: float, clock: Clock = time.monotonic) -> None:
        self.end, self.clock = end, clock

    @classmethod
    def after(cls, seconds: float, clock: Clock = time.monotonic) -> Deadline:
        return cls(clock() + seconds, clock)

    def seconds_left(self) -> float:
        return self.end - self.clock()

    def left(self, what: str) -> float:
        """The seconds left; a call that would start after the deadline raises instead."""
        left = self.seconds_left()
        if left <= 0:
            raise StateError("deadline", f"{what} would start after the step deadline")
        return left


class CatalogState:
    """The S3 records of the setup. ``clients(service, seconds_left)`` gives an S3 client of the job's task role whose timeouts fit the time left;
    ``clock`` is wall-clock seconds and ``monotonic`` the clock of the deadlines."""

    def __init__(
        self,
        clients: Clients,
        bucket: str,
        *,
        deadline: Deadline | None = None,
        clock: Clock = time.time,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Clock = time.monotonic,
    ) -> None:
        self.clients, self.bucket, self.deadline, self.clock, self.sleep, self.monotonic = clients, bucket, deadline, clock, sleep, monotonic

    def bounded(self, deadline: Deadline) -> CatalogState:
        """The same records with every call held to the absolute deadline."""
        return CatalogState(self.clients, self.bucket, deadline=deadline, clock=self.clock, sleep=self.sleep, monotonic=self.monotonic)

    def own_window(self) -> Deadline:
        return Deadline.after(RELEASE_SECONDS, self.monotonic)

    def call(self, operation: str, *, window: Deadline | None = None, **arguments: Any) -> Any:
        """One S3 call within the step deadline, or within ``window`` for a write that undoes or marks something. A call that gets no answer in time
        raises with ``unconfirmed`` set."""
        bound = window or self.deadline
        if bound is None:
            raise StateError("deadline", "the state was used without a step deadline")
        left = bound.left(operation)
        try:
            return getattr(self.clients("s3", left), operation)(Bucket=self.bucket, **arguments)
        except TIMEOUTS:
            raise StateError("deadline", f"{operation} got no answer before the step deadline", unconfirmed=True) from None

    def key(self, kind: str, name: str) -> str:
        return f"{PREFIX}/{kind}/{name}.json"

    def read(self, key: str) -> tuple[dict[str, Any], str] | None:
        """The record and its version, or None when S3 answers that the key is absent. A denied or failed read is never read as absent."""
        try:
            reply = self.call("get_object", Key=key)
            document = json.loads(reply["Body"].read())
        except (ClientError, BotoCoreError) as error:
            if error_code(error) in MISSING_CODES:
                return None
            raise StateError("state", f"reading {key} failed ({error_code(error)})") from None
        except (ValueError, KeyError, TypeError):
            raise StateError("state", f"{key} is not a record") from None
        if not isinstance(document, dict):
            raise StateError("state", f"{key} is not a record")
        return document, str(reply.get("ETag", ""))

    def create(self, key: str, document: dict[str, Any]) -> str | None:
        """Create the record only while the key is absent: its version, or None when it exists."""
        try:
            reply = self.call("put_object", Key=key, Body=json.dumps(document, sort_keys=True).encode(), IfNoneMatch="*")
        except (ClientError, BotoCoreError) as error:
            if error_code(error) in REFUSED_CODES:
                return None
            raise StateError("state", f"writing {key} failed ({error_code(error)})") from None
        return str(reply.get("ETag", ""))

    def replace(self, key: str, document: dict[str, Any], version: str, *, window: Deadline | None = None) -> str | None:
        """Replace the record only while it is still at this version: the new version, or None when another run replaced it since."""
        try:
            reply = self.call("put_object", window=window, Key=key, Body=json.dumps(document, sort_keys=True).encode(), IfMatch=version)
        except (ClientError, BotoCoreError) as error:
            if error_code(error) in (*REFUSED_CODES, *MISSING_CODES):
                return None
            raise StateError("state", f"writing {key} failed ({error_code(error)})") from None
        return str(reply.get("ETag", ""))

    def forget(self, key: str) -> None:
        """Delete a record only this run writes (a pending mark); a failed delete leaves a mark that expires by its own time."""
        with contextlib.suppress(ClientError, BotoCoreError, StateError):
            self.call("delete_object", window=self.own_window(), Key=key)

    def approval(self, identity: str) -> dict[str, Any] | None:
        found = self.read(self.key("approvals", identity))
        return None if found is None else found[0]

    def record_approval(self, identity: str, document: dict[str, Any]) -> None:
        """Store the approval once. The same approval again is accepted; another one for the same identity is refused."""
        if self.create(self.key("approvals", identity), {**document, "recorded_at_utc": utc(self.clock())}) is not None:
            return
        found = self.approval(identity)
        if found is None or {name: found.get(name) for name in document} != document:
            raise StateError("approval", f"{identity} already has another recorded namespace approval; it is never replaced")

    @contextlib.contextmanager
    def lease(self, role: str, *, wait: float, hold: float) -> Iterator[None]:
        """Hold the principal's lock for at most ``hold`` seconds, waiting at most ``wait`` seconds for it (P11).

        The lease is one object that only conditional writes change: it is created while absent, and an expired or released lease is replaced at the
        exact version that was read, so of two runs that read the same expired lease one wins. Release writes the expiry as zero at the version this run
        holds; it is never deleted, because a delete has no version condition on a general-purpose bucket.
        """
        key, end = self.key("locks", role), self.clock() + wait
        takeovers = 0
        while True:
            expires = self.clock() + hold
            mine = {"token": secrets.token_hex(16), "expires_at": expires, "expires_utc": utc(expires)}
            version = self.create(key, mine)
            if version is not None:
                break
            found = self.read(key)
            if found is not None and found[0].get("expires_at", float("inf")) <= self.clock() and takeovers < MAX_TAKEOVERS:
                takeovers += 1
                version = self.replace(key, mine, found[1])
                if version is not None:
                    break
                continue
            if self.clock() < end:
                self.sleep(min(POLL_SECONDS, max(end - self.clock(), 0.0)))
            if self.clock() >= end:
                until = f"; its lease ends {found[0].get('expires_utc', 'at an unknown time')}" if found is not None else ""
                raise StateError("credentials", f"another update of principal {role} did not finish within {wait:g} s{until}")
        try:
            yield
        finally:
            # A lease that cannot be released expires by itself; the result of the step is not hidden by it.
            with contextlib.suppress(StateError):
                self.replace(key, {"token": mine["token"], "expires_at": 0, "released": True}, version, window=self.own_window())

    def refuse_pending_write(self, role: str) -> None:
        """Stop while an earlier pair write of the principal may still commit: its mark stays until the write ends or its time has passed."""
        key = self.key("pending", role)
        try:
            found = self.read(key)
        except StateError:
            raise StateError("credentials", f"the pending-write mark of principal {role} cannot be read; no pair change is safe until it is resolved") from None
        if found is None:
            return
        until = found[0].get("until")
        if not isinstance(until, int | float) or isinstance(until, bool):
            raise StateError("credentials", f"the pending-write mark of principal {role} names no time; no pair change is safe until it is resolved")
        if until > self.clock():
            raise StateError("credentials", f"an earlier pair write of principal {role} may still commit until {utc(until)}; rerun after that")
        self.forget(key)

    def pending(self, role: str) -> PendingMark:
        return PendingMark(self, role)


@dataclass(frozen=True)
class PendingMark:
    """The mark of one principal's unconfirmed pair write: ``set`` before the locks go, ``clear`` when the write ends. Both run in their own window,
    because the mark is written exactly when the step deadline has passed."""

    state: CatalogState
    role: str

    def set(self) -> None:
        key, until = self.state.key("pending", self.role), self.state.clock() + WRITE_MAY_COMMIT_SECONDS
        try:
            self.state.call("put_object", window=self.state.own_window(), Key=key, Body=json.dumps({"until": until, "until_utc": utc(until)}).encode())
        except (ClientError, BotoCoreError) as error:
            raise StateError("credentials", f"the pending-write mark of principal {self.role} could not be written ({error_code(error)})") from None

    def clear(self) -> None:
        self.state.forget(self.state.key("pending", self.role))


@dataclass(frozen=True)
class LaunchSpec:
    """What the launch guard names: the lock parameter, the cluster and the family prefix of the project's job tasks."""

    lock_parameter: str
    cluster: str
    family_prefix: str


@dataclass
class Hold:
    """The launch hold this run took. ``keep`` leaves it in place after the step, for a pair write that may still commit."""

    token: str
    kept: bool = False

    def keep(self) -> None:
        self.kept = True


def new_token() -> str:
    """A hold token in the form ``services_apply resolve`` accepts: holder, process ID, UTC time and a random part."""
    return f"{HOLD_HOLDER}:{os.getpid()}:{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}:{secrets.token_hex(4)}"


class LaunchGuard:
    """The host launch hold and the proof that no job task of the project is live (P5)."""

    def __init__(
        self,
        clients: Clients,
        spec: LaunchSpec,
        *,
        drain: float = DRAIN_SECONDS,
        clock: Clock = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.clients, self.spec, self.drain, self.clock, self.sleep = clients, spec, drain, clock, sleep

    def call(self, service: str, operation: str, window: Deadline, **arguments: Any) -> Any:
        left = window.left(f"{service} {operation}")
        try:
            return getattr(self.clients(service, left), operation)(**arguments)
        except TIMEOUTS:
            raise StateError("writer_try", f"{service} {operation} got no answer before its bound", unconfirmed=True) from None

    @contextlib.contextmanager
    def no_live_writer_try(self, deadline: Deadline) -> Iterator[Hold]:
        """Hold the launch hold, wait for every launch admitted before it, and stop unless every job task of the project is confirmed ended.

        The deadline must leave room for the whole wait, or nothing is taken. The hold is released at the end unless the caller keeps it.
        """
        self.require_room(deadline)
        hold = Hold(new_token())
        self.take(hold.token, deadline)
        try:
            self.sleep(self.drain)
            self.prove_no_live_task(deadline)
            yield hold
        finally:
            failing = sys.exc_info()[0] is not None
            if not hold.kept:
                try:
                    self.release(hold.token)
                except StateError as error:
                    if not failing:
                        raise
                    sys.stderr.write(f"aws catalog setup: {error}\n")

    def require_room(self, deadline: Deadline) -> None:
        """Refuse a step whose deadline leaves no time for the wait after the hold, before the step changes anything."""
        if deadline.seconds_left() <= self.drain:
            raise StateError("writer_try", "the step deadline leaves no time to wait for the launches admitted before the hold, so nothing was changed")

    def take(self, token: str, deadline: Deadline) -> None:
        """Create the hold without overwrite. An answer that never came, or a connection that broke, leaves it unknown whether the hold exists, so the
        hold is released by its token before the step stops."""
        try:
            self.call("ssm", "put_parameter", deadline, Name=self.spec.lock_parameter, Value=token, Type="String", Overwrite=False)
        except ClientError as error:
            if error_code(error) != "ParameterAlreadyExists":
                raise StateError("writer_try", f"taking the host launch hold failed ({error_code(error)})") from None
            raise StateError("writer_try", self.held_by_another(deadline)) from None
        except (BotoCoreError, StateError) as error:
            if isinstance(error, StateError) and not error.unconfirmed:
                raise
            with contextlib.suppress(StateError):
                self.release(token)
            raise StateError(
                "writer_try", f"taking the host launch hold got no answer; if it exists, clear it with {RESOLVE} {token}", unconfirmed=True
            ) from None

    def held_by_another(self, deadline: Deadline) -> str:
        holder = "an unreadable value"
        with contextlib.suppress(ClientError, BotoCoreError, StateError, KeyError):
            holder = str(self.call("ssm", "get_parameter", deadline, Name=self.spec.lock_parameter)["Parameter"]["Value"])[:120]
        if holder.startswith(f"{HOLD_HOLDER}:"):
            leftover = f"a catalog setup holds or left the host launch hold ({holder})"
            return f"{leftover}; if no catalog setup task runs, confirm that and clear it with {RESOLVE} {holder}"
        return f"a services apply holds the host launch hold ({holder}), so no live writer try can be ruled out"

    def release(self, token: str) -> None:
        """Delete the hold only while it still holds this token, in its own window."""
        window = Deadline.after(RELEASE_SECONDS, self.clock)
        try:
            held = self.call("ssm", "get_parameter", window, Name=self.spec.lock_parameter)["Parameter"]["Value"]
            if held == token:
                self.call("ssm", "delete_parameter", window, Name=self.spec.lock_parameter)
        except ClientError as error:
            if error_code(error) == "ParameterNotFound":
                return
            raise StateError(
                "writer_try", f"the host launch hold stays; confirm that no catalog setup task runs, then clear it with {RESOLVE} {token}"
            ) from None
        except (BotoCoreError, KeyError, StateError):
            raise StateError(
                "writer_try", f"the host launch hold stays; confirm that no catalog setup task runs, then clear it with {RESOLVE} {token}"
            ) from None

    def prove_no_live_task(self, deadline: Deadline) -> None:
        """Stop while a job task is live; wait, within a bound, for tasks ECS cannot confirm; stop when they stay unconfirmed."""
        end = self.clock() + UNKNOWN_WAIT_SECONDS
        while True:
            live, unknown = self.scan(deadline)
            if live:
                raise StateError("writer_try", f"{live} job task(s) of the project have no confirmed end; rerun after they end")
            if not unknown:
                return
            if self.clock() >= end:
                raise StateError("writer_try", f"{unknown} job task(s) of the project could not be confirmed ended, so no live writer try can be ruled out")
            self.sleep(UNKNOWN_POLL_SECONDS)

    def scan(self, deadline: Deadline) -> tuple[int, int]:
        """The job tasks of the project that are not ended and the tasks whose state cannot be confirmed, whatever their desired status."""
        arns: dict[str, None] = {}
        try:
            for desired in DESIRED_STATUSES:
                token: str | None = None
                for _ in range(MAX_PAGES):
                    page = self.call("ecs", "list_tasks", deadline, cluster=self.spec.cluster, desiredStatus=desired, **({"nextToken": token} if token else {}))
                    arns |= dict.fromkeys(page.get("taskArns", []))
                    token = page.get("nextToken")
                    if not token:
                        break
                else:
                    raise StateError("writer_try", f"list-tasks has more than {MAX_PAGES} pages, so no live writer try can be ruled out")
            names = list(arns)
            live = unknown = 0
            for start in range(0, len(names), DESCRIBE_LIMIT):
                chunk = names[start : start + DESCRIBE_LIMIT]
                described = self.call("ecs", "describe_tasks", deadline, cluster=self.spec.cluster, tasks=chunk)
                answered = {task.get("taskArn") for task in described.get("tasks", [])} | {failure.get("arn") for failure in described.get("failures", [])}
                unknown += len(described.get("failures", [])) + sum(1 for arn in chunk if arn not in answered)
                for task in described.get("tasks", []):
                    kind = self.kind(task)
                    ended = task.get("lastStatus") in TERMINAL
                    if kind == "job" and not ended:
                        live += 1
                    elif kind is None and not ended:
                        unknown += 1
        except (ClientError, BotoCoreError) as error:
            raise StateError("writer_try", f"listing the job tasks failed ({error_code(error)}), so no live writer try can be ruled out") from None
        return live, unknown

    def kind(self, task: dict[str, Any]) -> str | None:
        """``job`` for a task of a project job family, ``other`` for any other task, None when the task names neither group nor definition."""
        group, definition = task.get("group"), task.get("taskDefinitionArn")
        grouped = group.removeprefix(GROUP_PREFIX) if isinstance(group, str) and group.startswith(GROUP_PREFIX) else ""
        defined = definition.rsplit("/", 1)[-1].rsplit(":", 1)[0] if isinstance(definition, str) and "/" in definition else ""
        if grouped.startswith(self.spec.family_prefix) or defined.startswith(self.spec.family_prefix):
            return "job"
        return "other" if grouped or defined else None
