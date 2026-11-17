"""The state of the AWS catalog setup job, kept where the job runs: write-once S3 records, a leased lock per principal and the host launch hold.

The setup runs as a one-off task on the host, so it has no checkout folder to keep local state in. Three things lived there and live here now:

- The namespace approvals are write-once objects ``<prefix>/approvals/<identity>.json``, created with ``IfNoneMatch="*"``. A second approval for the same
  identity never replaces the first.
- The lock of one principal (P11) is the lease object ``<prefix>/locks/<role>.json``. It is created with ``IfNoneMatch="*"``, deleted only with
  ``IfMatch`` on the version this run created, and replaced only after its own expiry. Its expiry is the step's absolute deadline plus the longest a pair write
  past that deadline is awaited, so a lease that has expired belongs to a run that can no longer change anything. A crashed task leaves its lease; the next
  run waits for it, at most to the end of its own wait bound, then refuses with the expiry time.
- The mark of an unconfirmed pair write is the object ``<prefix>/pending/<role>.json``, which holds the time until which the write may still commit.
- The launch hold (P5) is the services apply lock parameter, which every host launch honours: the setup creates it without overwrite with its own token,
  proves that no job task of the project has a task without a confirmed end, and deletes it only with its own token. A services apply that holds the
  parameter stops the setup, and the setup stops a services apply for the few seconds it holds it.

Nothing here holds a secret: records and parameters name identities, roles, times and random tokens only.
"""

from __future__ import annotations

import contextlib
import json
import secrets
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

PREFIX = "lakehouse/catalog_setup"
POLL_SECONDS = 0.2
# The most takeovers of an expired lease one acquisition tries before it waits like any other holder.
MAX_TAKEOVERS = 3
# The longest a pair write past its deadline may still commit: botocore's own bound for one call (secret_store.CLIENT_OPTIONS) plus the wait for it.
WRITE_MAY_COMMIT_SECONDS = 90.0
DESCRIBE_LIMIT = 100
MAX_PAGES = 50
GROUP_PREFIX = "family:"
STOPPED = "STOPPED"
DESIRED_STATUSES = ("RUNNING", "STOPPED")
MISSING_CODES = ("NoSuchKey", "404", "NotFound")
REFUSED_CODES = ("PreconditionFailed", "412", "ConditionalRequestConflict", "409")
Clock = Callable[[], float]


class StateError(RuntimeError):
    """A state step failed; ``code`` names the step and the message holds no value."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def error_code(error: ClientError | BotoCoreError) -> str:
    return str(error.response.get("Error", {}).get("Code", "unknown")) if isinstance(error, ClientError) else type(error).__name__


def utc(seconds: float) -> str:
    return datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class CatalogState:
    """The S3 records of the setup. ``client`` is an S3 client of the job's task role; ``clock`` is wall-clock seconds."""

    def __init__(self, client: Any, bucket: str, *, clock: Clock = time.time, sleep: Callable[[float], None] = time.sleep) -> None:
        self.client, self.bucket, self.clock, self.sleep = client, bucket, clock, sleep

    def key(self, kind: str, name: str) -> str:
        return f"{PREFIX}/{kind}/{name}.json"

    def read(self, key: str) -> tuple[dict[str, Any], str] | None:
        """The record and its version, or None when S3 answers that the key is absent. A denied or failed read is never read as absent."""
        try:
            reply = self.client.get_object(Bucket=self.bucket, Key=key)
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
            reply = self.client.put_object(Bucket=self.bucket, Key=key, Body=json.dumps(document, sort_keys=True).encode(), IfNoneMatch="*")
        except (ClientError, BotoCoreError) as error:
            if error_code(error) in REFUSED_CODES:
                return None
            raise StateError("state", f"writing {key} failed ({error_code(error)})") from None
        return str(reply.get("ETag", ""))

    def delete_version(self, key: str, version: str) -> bool:
        """Delete the record only at this version: False when it is gone or another run replaced it."""
        try:
            self.client.delete_object(Bucket=self.bucket, Key=key, IfMatch=version)
        except (ClientError, BotoCoreError) as error:
            if error_code(error) in (*REFUSED_CODES, *MISSING_CODES):
                return False
            raise StateError("state", f"deleting {key} failed ({error_code(error)})") from None
        return True

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
        """Hold the principal's lock for at most ``hold`` seconds, waiting at most ``wait`` seconds for it (P11)."""
        key, end = self.key("locks", role), self.clock() + wait
        takeovers = 0
        while True:
            expires = self.clock() + hold
            version = self.create(key, {"token": secrets.token_hex(16), "expires_at": expires, "expires_utc": utc(expires)})
            if version is not None:
                break
            found = self.read(key)
            if found is not None and found[0].get("expires_at", float("inf")) <= self.clock() and takeovers < MAX_TAKEOVERS:
                # An expired lease belongs to a run that can no longer change anything. The delete is at its exact version, so a lease another run
                # created since is never deleted; at most MAX_TAKEOVERS tries bound the loop.
                takeovers += 1
                self.delete_version(key, found[1])
                continue
            if self.clock() >= end:
                until = f"; its lease ends {found[0].get('expires_utc', 'at an unknown time')}" if found is not None else ""
                raise StateError("credentials", f"another update of principal {role} did not finish within {wait:g} s{until}")
            self.sleep(min(POLL_SECONDS, max(end - self.clock(), 0.0)))
        try:
            yield
        finally:
            # A lease that cannot be deleted expires by itself; the result of the step is not hidden by it.
            with contextlib.suppress(StateError):
                self.delete_version(key, version)

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
        self.delete_version(key, found[1])

    def pending(self, role: str) -> PendingMark:
        return PendingMark(self, role)


@dataclass(frozen=True)
class PendingMark:
    """The mark of one principal's unconfirmed pair write: ``set`` before the locks go, ``clear`` when the write ends."""

    state: CatalogState
    role: str

    def set(self) -> None:
        key, until = self.state.key("pending", self.role), self.state.clock() + WRITE_MAY_COMMIT_SECONDS
        try:
            self.state.client.put_object(Bucket=self.state.bucket, Key=key, Body=json.dumps({"until": until, "until_utc": utc(until)}).encode())
        except (ClientError, BotoCoreError) as error:
            raise StateError("credentials", f"the pending-write mark of principal {self.role} could not be written ({error_code(error)})") from None

    def clear(self) -> None:
        with contextlib.suppress(ClientError, BotoCoreError):
            self.state.client.delete_object(Bucket=self.state.bucket, Key=self.state.key("pending", self.role))


@dataclass(frozen=True)
class LaunchSpec:
    """What the launch guard names: the lock parameter, the cluster and the family prefix of the project's job tasks."""

    lock_parameter: str
    cluster: str
    family_prefix: str


class LaunchGuard:
    """The host launch hold and the proof that no job task of the project is live (P5)."""

    def __init__(self, ssm: Any, ecs: Any, spec: LaunchSpec) -> None:
        self.ssm, self.ecs, self.spec = ssm, ecs, spec

    @contextlib.contextmanager
    def no_live_writer_try(self) -> Iterator[None]:
        """Hold the launch hold, under which no host launch is admitted, and stop unless no job task has a task without a confirmed end.

        A task that ECS cannot list or describe proves nothing and stops the setup too. The hold stays until the credentials are stored.
        """
        token = f"catalog_setup-{secrets.token_hex(16)}"
        try:
            self.ssm.put_parameter(Name=self.spec.lock_parameter, Value=token, Type="String", Overwrite=False)
        except (ClientError, BotoCoreError) as error:
            if error_code(error) == "ParameterAlreadyExists":
                raise StateError(
                    "writer_try", "a services apply or another catalog setup holds the host launch hold, so no live writer try can be ruled out"
                ) from None
            raise StateError("writer_try", f"taking the host launch hold failed ({error_code(error)})") from None
        try:
            live = self.live_tasks()
            if live:
                raise StateError("writer_try", f"{live} job task(s) of the project have no confirmed end; rerun after they end")
            yield
        finally:
            self.release(token)

    def release(self, token: str) -> None:
        """Delete the hold only while it still holds this token."""
        try:
            held = self.ssm.get_parameter(Name=self.spec.lock_parameter)["Parameter"]["Value"]
            if held == token:
                self.ssm.delete_parameter(Name=self.spec.lock_parameter)
        except (ClientError, BotoCoreError, KeyError):
            raise StateError("writer_try", "the host launch hold could not be released; run services_apply resolve to clear it") from None

    def live_tasks(self) -> int:
        """The count of job tasks of the project that are not STOPPED, whatever their desired status."""
        arns: dict[str, None] = {}
        try:
            for desired in DESIRED_STATUSES:
                token: str | None = None
                for _ in range(MAX_PAGES):
                    page = self.ecs.list_tasks(cluster=self.spec.cluster, desiredStatus=desired, **({"nextToken": token} if token else {}))
                    arns |= dict.fromkeys(page.get("taskArns", []))
                    token = page.get("nextToken")
                    if not token:
                        break
                else:
                    raise StateError("writer_try", f"list-tasks has more than {MAX_PAGES} pages, so no live writer try can be ruled out")
            names = list(arns)
            live = 0
            for start in range(0, len(names), DESCRIBE_LIMIT):
                described = self.ecs.describe_tasks(cluster=self.spec.cluster, tasks=names[start : start + DESCRIBE_LIMIT])
                if any(failure.get("reason") != "MISSING" for failure in described.get("failures", [])):
                    raise StateError("writer_try", "describe-tasks reported failures, so no live writer try can be ruled out")
                live += sum(1 for task in described.get("tasks", []) if self.is_job_task(task) and task.get("lastStatus") != STOPPED)
        except (ClientError, BotoCoreError) as error:
            raise StateError("writer_try", f"listing the job tasks failed ({error_code(error)}), so no live writer try can be ruled out") from None
        return live

    def is_job_task(self, task: dict[str, Any]) -> bool:
        group, definition = task.get("group"), task.get("taskDefinitionArn")
        grouped = group.removeprefix(GROUP_PREFIX) if isinstance(group, str) and group.startswith(GROUP_PREFIX) else ""
        defined = definition.rsplit("/", 1)[-1].rsplit(":", 1)[0] if isinstance(definition, str) and "/" in definition else ""
        return grouped.startswith(self.spec.family_prefix) or defined.startswith(self.spec.family_prefix)
