"""Keep secure run values in memory while the registry owns acquire and retirement intents."""

from __future__ import annotations

import hashlib
import math
import re
import secrets
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from orchestration import launcher
from scripts.lakehouse import run_lock

SIGNING_NAME = "/healthcare_hai_prediction/controller_signing_key"
RUN_PREFIX = "/healthcare_hai_prediction/controller_run_tokens"
ACTIVE_STATES = frozenset({"held", "finalizing", "recovering", "failed_retained"})


class ParameterClient(Protocol):
    """Bound each closed Parameter Store call and confirm ownership cleanup before returning."""

    def call(self, operation: str, params: Mapping[str, Any], *, deadline: float) -> dict[str, Any]:
        """Return a closed SDK reply without logging request or response values."""
        ...

    def close(self, *, deadline: float) -> dict[str, Any]:
        """Stop owned SDK workers and report whether their absence is confirmed."""
        ...


class StoreError(Exception):
    """Carry a sanitized operation outcome without service bodies or credential values."""

    def __init__(self, code: str, kind: str = "service") -> None:
        super().__init__(code)
        self.code = code
        self.kind = kind


@dataclass(frozen=True)
class TokenJob:
    """Bind one off-loop operation to its committed request, generation and deadline."""

    run_id: str
    operation: int
    generation: int
    name: str
    token_hash: str
    request_key: str
    payload_hash: str
    deadline: float
    cleanup: bool

    @property
    def key(self) -> str:
        """Correlate the worker result without including any secret bytes."""
        return f"controller-token:{self.run_id}:{self.operation}"


class SecureRunTokens:
    """Reserve, reconcile and retire one registry's SecureString tokens outside registry locks."""

    def __init__(
        self,
        registry: run_lock.Registry,
        client: ParameterClient,
        *,
        seconds: float = 30.0,
        call_seconds: float = 5.0,
        retry_delay: float = 0.1,
        token_factory: Callable[[], str] = lambda: secrets.token_urlsafe(32),
    ) -> None:
        if (
            not all(math.isfinite(value) for value in (seconds, call_seconds, retry_delay))
            or not 0 < call_seconds <= seconds <= 30
            or not 0 <= retry_delay <= 1
        ):
            raise ValueError("invalid_token_operation_budget")
        self.registry = registry
        self.client = client
        self.seconds, self.call_seconds, self.retry_delay = seconds, call_seconds, retry_delay
        self.token_factory = token_factory
        self.stopping = threading.Event()
        self.condition = threading.Condition()
        self.active = 0
        self.jobs: dict[str, TokenJob] = {}
        self.pending_values: dict[str, str] = {}
        self.cache: dict[str, tuple[str, int, str]] = {}
        self.notifications: list[tuple[str, launcher.LauncherError]] = []
        self.closed = False

    def _call(self, operation: str, params: dict[str, Any], deadline: float) -> dict[str, Any]:
        for attempt in range(3):
            if self.stopping.is_set():
                raise StoreError("shutdown", "shutdown")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise StoreError("token_operation_deadline", "deadline")
            reply = self.client.call(operation, params, deadline=min(deadline, time.monotonic() + self.call_seconds))
            if time.monotonic() >= deadline:
                raise StoreError("token_operation_deadline", "deadline")
            if reply.get("ok") is True:
                return dict(reply["result"])
            error = reply.get("error", {})
            code, kind = str(error.get("code", "token_protocol_error")), str(error.get("kind", "protocol"))
            retryable = kind == "throttle" or (kind == "network" and operation == "controller_get_parameter")
            if not retryable or attempt == 2:
                raise StoreError(code, kind)
            delay = min(self.retry_delay * 2**attempt, max(0, deadline - time.monotonic()))
            if self.stopping.wait(delay):
                raise StoreError("shutdown", "shutdown")
        raise StoreError("token_retry_limit")

    def signing_key(self) -> bytes:
        """Read the owner-provisioned hexadecimal key and pin its version and digest without storing its value."""
        with run_lock.transaction(self.registry, create_schema=run_lock.LAUNCHER_SCHEMA):
            pass
        try:
            row = self._call("controller_get_parameter", {"name": SIGNING_NAME}, time.monotonic() + self.seconds)
            value = row["value"]
            if not isinstance(value, str) or re.fullmatch(r"[0-9a-fA-F]{64}", value) is None:
                raise ValueError("invalid_signing_key_encoding")
            key = bytes.fromhex(value)
            if len(key) != 32 or row["name"] != SIGNING_NAME or type(row["version"]) is not int or row["version"] < 1:
                raise ValueError("invalid_signing_key")
        except StoreError as error:
            code = "token_missing" if error.code == "ParameterNotFound" else "token_unavailable"
            raise launcher.LauncherError("controller signing key is missing or unavailable", code) from None
        except (ValueError, TypeError, KeyError):
            raise launcher.LauncherError("controller signing key must encode the existing 32 bytes as hexadecimal", "token_identity") from None
        identity = {"name": SIGNING_NAME, "version": row["version"], "sha256": hashlib.sha256(key).hexdigest()}
        with run_lock.transaction(self.registry) as state:
            pinned = state["launcher"].get("controller_signing_key")
            if pinned is not None and pinned != identity:
                raise launcher.LauncherError("controller signing-key identity changed. Restore the recorded version.", "token_identity")
            state["launcher"]["controller_signing_key"] = identity
        return key

    @staticmethod
    def _name(state: Mapping[str, Any], run_id: str) -> str:
        return f"{RUN_PREFIX}/{state['registry_id']}/{run_id}"

    def _record(self, state: Mapping[str, Any], run_id: str) -> dict[str, Any]:
        entry = state["runs"][run_id]
        record = entry.get("secure_token")
        if not isinstance(record, dict) or record.get("name") != self._name(state, run_id):
            raise launcher.LauncherError("run token has no valid recorded parameter name", "token_identity")
        return record

    def start(self, server: launcher.Launcher) -> None:
        """Rearm known pending work under the same request and generation, with one fresh bounded operation."""
        server.message_handlers["controller_token"] = lambda key, outcome, value: self._result(server, key, outcome, value)
        with server.tx() as state:
            for run_id, entry in state["runs"].items():
                if "secure_token" not in entry and entry["state"] in ACTIVE_STATES:
                    entry["secure_token"] = {"name": self._name(state, run_id), "status": "active", "version": None, "operation": 0}
                if "secure_token" not in entry:
                    continue
                record = self._record(state, run_id)
                if entry["state"] in {"acquire_pending", "acquire_unconfirmed"}:
                    self._rearm(state, run_id, cleanup=False)
                elif entry["state"] == "released" and record["status"] == "token_cleanup_pending":
                    self._rearm(state, run_id, cleanup=True)

    def _rearm(self, state: dict[str, Any], run_id: str, *, cleanup: bool) -> None:
        record = self._record(state, run_id)
        record.update(operation=int(record.get("operation", 0)) + 1, deadline_utc=time.time() + self.seconds, attempted=False, last_error=None)
        if cleanup:
            record["status"] = "token_cleanup_pending"
        else:
            record["status"] = "acquire_pending"
            state["runs"][run_id]["state"] = "acquire_pending"
            request = state["requests"].get(record["request_key"])
            if request is None or request.get("method") != "acquire":
                raise launcher.LauncherError("pending token request identity is missing", "token_identity")
            request.update(state="pending", result=None)
            request.pop("outcome", None)

    def acquire(self, server: launcher.Launcher, ctx: launcher.Ctx) -> Any:
        """Reserve the run and token hash in the request transaction, without starting any network work."""
        run_id = str(ctx.params["run_id"])
        token = self.token_factory()
        run_lock.new_run(
            ctx.state,
            str(ctx.params["domain"]),
            run_id,
            str(ctx.params["mode"]),
            str(ctx.params.get("holder", "manual")),
            int(ctx.params.get("pid") or ctx.conn.peer.pid),
            str(run_lock.process_start(int(ctx.params.get("pid") or ctx.conn.peer.pid))),
            [str(prefix) for prefix in ctx.params.get("write_prefixes", [])],
            str(ctx.params.get("checkout") or server.cfg.root),
            token=token,
        )
        entry = ctx.state["runs"][run_id]
        entry.update(token_sha256=run_lock.digest(token), state="acquire_pending", history=[{"state": "acquire_pending", "at": run_lock.now()}])
        entry["secure_token"] = {
            "name": self._name(ctx.state, run_id),
            "request_key": ctx.key,
            "generation": entry["generation"],
            "operation": 1,
            "status": "acquire_pending",
            "deadline_utc": time.time() + self.seconds,
            "attempted": False,
            "version": None,
        }
        self.pending_values[run_id] = token
        return launcher.PENDING

    def _confirmed(self, job: TokenJob, token: str | None) -> dict[str, Any]:
        if token is not None:
            try:
                self._call("controller_put_parameter", {"name": job.name, "value": token}, job.deadline)
                self.pending_values.pop(job.run_id, None)
            except StoreError as error:
                if error.code == "ParameterLimitExceeded":
                    raise StoreError(error.code, "quota") from None
                if error.code != "ParameterAlreadyExists" and error.kind not in {"network", "deadline", "worker_died", "protocol"}:
                    raise
        row = self._call("controller_get_parameter", {"name": job.name}, job.deadline)
        if row["name"] != job.name or not secrets.compare_digest(run_lock.digest(str(row["value"])), job.token_hash):
            raise StoreError("token_hash_conflict", "conflict")
        if type(row["version"]) is not int or row["version"] < 1:
            raise StoreError("token_version_invalid", "protocol")
        return row

    def _deleted(self, job: TokenJob) -> dict[str, Any]:
        try:
            self._call("controller_delete_parameter", {"name": job.name}, job.deadline)
        except StoreError as error:
            if error.code != "ParameterNotFound" and error.kind not in {"network", "deadline", "worker_died", "protocol"}:
                raise
        try:
            self._call("controller_get_parameter", {"name": job.name}, job.deadline)
        except StoreError as error:
            if error.code == "ParameterNotFound":
                return {"retired": True}
            raise
        raise StoreError("token_delete_unconfirmed")

    def _perform(self, job: TokenJob, token: str | None) -> dict[str, Any]:
        with self.condition:
            self.active += 1
        try:
            if self.stopping.is_set():
                raise StoreError("shutdown", "shutdown")
            return self._deleted(job) if job.cleanup else self._confirmed(job, token)
        finally:
            with self.condition:
                self.active -= 1
                self.condition.notify_all()

    def _unconfirmed(self, state: dict[str, Any], run_id: str, code: str, *, kind: str = "service") -> tuple[str, launcher.LauncherError]:
        entry, record = state["runs"][run_id], self._record(state, run_id)
        if entry["state"] != "acquire_unconfirmed":
            run_lock.set_state(state, run_id, "acquire_unconfirmed")
        if kind == "conflict":
            self.pending_values.pop(run_id, None)
        recovery = (
            "Abort the recorded acquire. Its parameter contains a different token."
            if kind == "conflict"
            else "Retry re-puts the same token from memory, or abort the recorded acquire."
            if run_id in self.pending_values
            else "Retry is readback only because no token remains in memory. If the recorded parameter is absent, abort this acquire."
        )
        record.update(status="acquire_unconfirmed", last_error=code, recovery_action=f"{recovery} Use token_recover.")
        public = "token_quota" if kind == "quota" else "token_conflict" if kind == "conflict" else "acquire_unconfirmed"
        message = "run token acquisition is unconfirmed. Inspect token recovery status."
        if kind == "quota":
            pending = sum(run.get("secure_token", {}).get("status") == "token_cleanup_pending" for run in state["runs"].values())
            message = f"run token quota is exhausted. Pending cleanup intents: {pending}."
        return record["request_key"], launcher.LauncherError(f"{message} {recovery}", public)

    def tick(self, server: launcher.Launcher) -> None:
        """Launch committed work off-loop and expire pending acquisition without blocking status or cancellation."""
        actions: list[tuple[TokenJob, str | None]] = []
        notifications, self.notifications = self.notifications, []
        with server.tx() as state:
            for run_id, entry in state["runs"].items():
                if "secure_token" not in entry:
                    continue
                record = self._record(state, run_id)
                pending = entry["state"] == "acquire_pending" and record["status"] == "acquire_pending"
                cleanup = entry["state"] == "released" and record["status"] == "token_cleanup_pending"
                if not pending and not cleanup:
                    continue
                remaining = float(record["deadline_utc"]) - time.time()
                running = next((job for job in self.jobs.values() if job.run_id == run_id), None)
                if remaining <= 0 or (running is not None and time.monotonic() >= running.deadline):
                    if pending:
                        notifications.append(self._unconfirmed(state, run_id, "token_operation_deadline", kind="deadline"))
                    elif not record.get("last_error"):
                        record.update(last_error="token_cleanup_deadline", recovery_action="retry token cleanup through token_recover")
                    continue
                if record.get("attempted") or any(job.run_id == run_id for job in self.jobs.values()):
                    continue
                request_key = str(record.get("request_key", ""))
                job = TokenJob(
                    run_id,
                    int(record["operation"]),
                    int(entry["generation"]),
                    str(record["name"]),
                    str(entry["token_sha256"]),
                    request_key,
                    str(state["requests"].get(request_key, {}).get("payload_sha256", "")),
                    time.monotonic() + min(self.seconds, remaining),
                    cleanup,
                )
                record["attempted"] = True
                self.jobs[job.key] = job
                actions.append((job, self.pending_values.get(run_id)))
        for job, token in actions:
            server.spawn(job.key, "controller_token", self._perform, job, token)
        for request_key, error in notifications:
            server.complete_request(request_key, {}, error)

    def _result(self, server: launcher.Launcher, key: str, outcome: str, value: Any) -> None:
        job = self.jobs.pop(key, None)
        if job is None or self.stopping.is_set():
            return
        notification: tuple[str, launcher.LauncherError] | None = None
        completed = False
        with server.tx() as state:
            entry = state["runs"].get(job.run_id)
            if entry is None:
                return
            record = self._record(state, job.run_id)
            if record.get("operation") != job.operation:
                return
            if job.cleanup:
                if entry["state"] != "released" or record["status"] != "token_cleanup_pending":
                    return
                if outcome == "ok" and value.get("retired") and time.monotonic() < job.deadline:
                    record.update(status="retired", last_error=None)
                    record.pop("recovery_action", None)
                else:
                    record.update(
                        last_error=value.code if isinstance(value, StoreError) else "token_cleanup_unconfirmed",
                        recovery_action="retry token cleanup through token_recover",
                    )
                return
            if entry["state"] != "acquire_pending" or record["status"] != "acquire_pending":
                return
            request = state["requests"].get(job.request_key, {})
            unchanged = (
                entry["generation"] == job.generation == record.get("generation")
                and entry["token_sha256"] == job.token_hash
                and record.get("request_key") == job.request_key
                and request.get("payload_sha256") == job.payload_hash
                and request.get("state") == "pending"
            )
            if not unchanged or time.monotonic() >= job.deadline or time.time() >= float(record["deadline_utc"]):
                notification = self._unconfirmed(state, job.run_id, "token_acquire_identity_or_deadline")
            elif outcome != "ok":
                error = value if isinstance(value, StoreError) else StoreError("token_worker_failed")
                notification = self._unconfirmed(state, job.run_id, error.code, kind=error.kind)
            else:
                run_lock.set_state(state, job.run_id, "held")
                record.update(status="active", version=value["version"], last_error=None)
                record.pop("recovery_action", None)
                self.pending_values.pop(job.run_id, None)
                self.cache[job.run_id] = (job.token_hash, value["version"], value["value"])
                result = {"run_id": job.run_id, "token_file": None}
                request.update(state="done", result=result, outcome="ok")
                completed = True
        if notification is not None:
            server.complete_request(notification[0], {}, notification[1])
        elif completed:
            server.complete_request(job.request_key, {"run_id": job.run_id, "token_file": None})

    def release(self, server: launcher.Launcher, ctx: launcher.Ctx) -> None:
        """Record token retirement in the same transaction as terminal state and invalidate memory lookup immediately."""
        run_id = str(ctx.principal["run_id"])
        record = self._record(ctx.state, run_id)
        if ctx.state["runs"][run_id]["state"] != "released":
            raise launcher.LauncherError("token cleanup requires a released run", "refused")
        record.update(status="token_cleanup_pending", recovery_action="token cleanup pending")
        self._rearm(ctx.state, run_id, cleanup=True)
        self.cache.pop(run_id, None)
        self.pending_values.pop(run_id, None)

    def recover(self, server: launcher.Launcher, ctx: launcher.Ctx) -> dict[str, Any]:
        """Abort an unconfirmed acquire or retry a recorded operation without changing its original request identity."""
        run_id, action = str(ctx.params.get("run_id")), ctx.params.get("action")
        entry = ctx.state["runs"].get(run_id)
        if entry is None:
            raise launcher.LauncherError("token recovery needs a recorded run", "args")
        record = self._record(ctx.state, run_id)
        if action == "abort" and entry["state"] in {"acquire_pending", "acquire_unconfirmed"}:
            entry["generation"] += 1
            run_lock.set_state(ctx.state, run_id, "released")
            self._rearm(ctx.state, run_id, cleanup=True)
            self.cache.pop(run_id, None)
            self.pending_values.pop(run_id, None)
            self.notifications.append((record["request_key"], launcher.LauncherError("run token acquisition was explicitly aborted", "acquire_unconfirmed")))
        elif action == "retry" and entry["state"] in {"released", "acquire_unconfirmed"}:
            if entry["state"] == "released" and record["status"] != "token_cleanup_pending":
                raise launcher.LauncherError("run token has no pending cleanup", "refused")
            self._rearm(ctx.state, run_id, cleanup=entry["state"] == "released")
        else:
            raise launcher.LauncherError("token recovery needs abort on an unconfirmed acquire or retry on pending recovery", "refused")
        return {"state": entry["state"], "token_status": record["status"]}

    def read(self, run_id: str) -> str:
        """Check durable run state before using a cached token and verify every uncached read against its recorded hash."""
        with run_lock.transaction(self.registry, write=False) as state:
            entry = state["runs"].get(run_id)
            if entry is None or entry["state"] not in ACTIVE_STATES:
                self.cache.pop(run_id, None)
                raise launcher.LauncherError("secure run token is absent or its run is not active", "token_missing")
            record = dict(self._record(state, run_id))
            token_hash = str(entry["token_sha256"])
            cached = self.cache.get(run_id)
            if cached is not None and cached[:2] == (token_hash, record["version"]):
                return cached[2]
        try:
            row = self._call("controller_get_parameter", {"name": record["name"]}, time.monotonic() + self.seconds)
        except StoreError as error:
            code = "token_missing" if error.code == "ParameterNotFound" else "token_unavailable"
            raise launcher.LauncherError("secure run token is absent or unavailable", code) from None
        if row["name"] != record["name"] or run_lock.digest(str(row["value"])) != token_hash or record["version"] not in (None, row["version"]):
            raise launcher.LauncherError("secure run token differs from its recorded identity", "token_identity")
        with run_lock.transaction(self.registry) as state:
            entry = state["runs"].get(run_id)
            if entry is None or entry["state"] not in ACTIVE_STATES or entry["token_sha256"] != token_hash:
                raise launcher.LauncherError("secure run token became inactive during read", "token_missing")
            current = self._record(state, run_id)
            if current != record:
                raise launcher.LauncherError("secure run token identity changed during read", "token_identity")
            current["version"] = row["version"]
            self.cache[run_id] = (token_hash, row["version"], row["value"])
        return str(row["value"])

    def absent(self, run_id: str) -> bool:
        """Confirm remote absence only for an unused run identity, never for a pending or released run."""
        with run_lock.transaction(self.registry, write=False) as state:
            if run_id in state["runs"]:
                return False
            name = self._name(state, run_id)
        try:
            self._call("controller_get_parameter", {"name": name}, time.monotonic() + self.seconds)
        except StoreError as error:
            return error.code == "ParameterNotFound"
        return False

    def close(self, *, deadline: float) -> None:
        """Cancel owned SDK work and finish active calls within the caller's cleanup deadline."""
        if self.closed:
            return
        self.stopping.set()
        result = self.client.close(deadline=deadline)
        with self.condition:
            while self.active and time.monotonic() < deadline:
                self.condition.wait(timeout=max(0, deadline - time.monotonic()))
        self.pending_values.clear()
        self.cache.clear()
        if not result.get("confirmed") or self.active:
            raise TimeoutError("controller token worker cleanup is unconfirmed")
        self.closed = True
