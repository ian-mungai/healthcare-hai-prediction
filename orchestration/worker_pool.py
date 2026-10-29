"""Admit four SDK callers while retaining ownership of every unconfirmed worker process."""

from __future__ import annotations

import contextlib
import os
import secrets
import select
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from orchestration.worker_protocol import CONTROL_OPERATIONS, secret_bearing
from orchestration.worker_recovery import WorkerLedger
from orchestration.worker_stderr import StderrReader
from scripts.process_group import OwnedProcess, StopResult, group_state, prepare_parent, record_leader_start, spawn_owned, stop_owned

MAX_ACTIVE = 4
MAX_STOPPING = 2
MAX_LIVE = 6
MAX_CALLS = 500
MAX_AGE_SECONDS = 600.0
ENVIRONMENT_NAMES = frozenset({"PATH", "HOME", "TMPDIR"})


@dataclass(frozen=True)
class WorkerConfig:
    """Nonsecret launch settings. Synthetic endpoints are restricted to loopback listeners."""

    region: str
    profile: str | None = None
    python: str = sys.executable
    worker: Path = field(default_factory=lambda: Path(__file__).with_name("sdk_worker.py"))
    pooled: bool = True
    test_endpoint: str | None = None
    test_control_endpoint: str | None = None

    def __post_init__(self) -> None:
        if not self.region or not Path(self.python).is_absolute() or not self.worker.is_absolute():
            raise ValueError("worker_requires_region_and_absolute_paths")
        for value in (self.test_endpoint, self.test_control_endpoint):
            if value is None:
                continue
            parsed = urlsplit(value)
            if (
                parsed.scheme != "http"
                or parsed.hostname != "127.0.0.1"
                or parsed.username
                or parsed.password
                or parsed.path not in {"", "/"}
                or parsed.query
                or parsed.fragment
                or not parsed.port
            ):
                raise ValueError("worker_test_endpoint_must_be_loopback")
        if self.test_control_endpoint and not self.test_endpoint:
            raise ValueError("worker_test_control_requires_test_endpoint")


@dataclass
class Worker:
    """One admitted process, its reuse identity and its outstanding cleanup duty."""

    handle: OwnedProcess
    lane: str
    pool_key: str | None
    born: float
    one_shot: bool
    status: str = "busy"
    calls: int = 0
    stopping: bool = False
    cleaned: bool = False
    owner_released: bool = False
    partials: set[Path] = field(default_factory=set)


class WorkerPool:
    """Keep separate control capacity and enforce absolute admission and aggregate cleanup deadlines."""

    def __init__(self, config: WorkerConfig, ledger: WorkerLedger, session: str) -> None:
        prepare_parent()
        self.config = config
        self.ledger = ledger
        self.session = session
        self.condition = threading.Condition(threading.RLock())
        self.closing = threading.Event()
        self.workers: list[Worker] = []
        self.stop_threads: list[threading.Thread] = []
        self.reserved_partials: set[Path] = set()
        self.failed = False

    @property
    def handles(self) -> tuple[OwnedProcess, ...]:
        """Return all retained ownership handles, including closed ones for verification."""
        with self.condition:
            return tuple(worker.handle for worker in self.workers)

    def _environment(self, witness: str) -> dict[str, str]:
        environment = {name: os.environ[name] for name in ENVIRONMENT_NAMES if name in os.environ}
        environment.update(PYTHONDONTWRITEBYTECODE="1", HAI_WORKER_WITNESS=witness, AWS_REGION=self.config.region)
        if self.config.profile:
            environment["AWS_PROFILE"] = self.config.profile
        if self.config.test_endpoint:
            environment.update(AWS_CONFIG_FILE=os.devnull, AWS_SHARED_CREDENTIALS_FILE=os.devnull, AWS_EC2_METADATA_DISABLED="true")
        return environment

    def _spawn(self, lane: str, pool_key: str | None, one_shot: bool, deadline: float, *, secret_stream: bool) -> Worker:
        nonce = secrets.token_hex(16)
        witness = secrets.token_hex(32)
        spawn_utc = datetime_utc()
        command = [self.config.python, "-I", "-B", str(self.config.worker), "--hai-worker", nonce, "--region", self.config.region]
        for flag, value in (
            ("--profile", self.config.profile),
            ("--test-endpoint", self.config.test_endpoint),
            ("--test-control-endpoint", self.config.test_control_endpoint),
        ):
            if value is not None:
                command.extend((flag, value))
        if one_shot:
            command.append("--one-shot")
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
        worker: Worker | None = None
        try:
            with self.ledger.spawn_guard(deadline):
                self.ledger.append_intent(
                    {"nonce": nonce, "spawn_utc": spawn_utc, "session": self.session, "lane": lane, "owner": self.session, "witness": witness},
                    deadline=deadline,
                )
                try:
                    handle = spawn_owned(
                        command,
                        self._environment(witness),
                        witness=witness,
                        nonce=nonce,
                        session=self.session,
                        lane=lane,
                        owner=self.session,
                        deadline=deadline,
                        spawn_utc=spawn_utc,
                    )
                except BaseException:
                    with contextlib.suppress(OSError, ValueError, TimeoutError):
                        self.ledger.close_intent(nonce, "spawn_failed", deadline=max(deadline, time.monotonic() + 1))
                    raise
                worker = Worker(handle, lane, pool_key, time.monotonic(), one_shot)
                self.workers.append(worker)
                record_leader_start(handle, deadline)
                self.ledger.append_spawn(handle.spawn_record(), deadline=deadline)
            handle.stderr_reader = StderrReader(
                handle.stderr,
                self.ledger.registry / f"worker_stderr_{nonce}.log",
                deadline=float("inf"),
                values=(witness,),
                withhold=secret_stream,
            )
            handle.stderr_reader.start()
            return worker
        except BaseException:
            if worker is not None:
                worker.status = "retiring"
                self.retire(worker, time.monotonic() + 5)
            raise
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)

    def acquire(self, operation: str, params: dict[str, Any], deadline: float, file_key: str | None = None) -> Worker:
        """Wait for the matching lane within the request's original absolute deadline."""
        lane = "control" if operation in CONTROL_OPERATIONS else "data"
        pool_key = "control" if lane == "control" else file_key
        one_shot = not self.config.pooled or secret_bearing(operation, params) or pool_key is None or operation == "s3_download"
        while True:
            expired: Worker | None = None
            with self.condition:
                if self.closing.is_set():
                    raise RuntimeError("shutdown")
                if time.monotonic() >= deadline:
                    raise TimeoutError("worker_admission_deadline")
                for worker in self.workers:
                    if worker.status != "idle" or worker.lane != lane:
                        continue
                    ready, _, _ = select.select([worker.handle.stdout], [], [], 0)
                    stale = worker.calls >= MAX_CALLS or time.monotonic() - worker.born >= MAX_AGE_SECONDS or bool(ready)
                    if stale or worker.pool_key != pool_key or one_shot:
                        worker.status = "retiring"
                        expired = worker
                        break
                    worker.status = "busy"
                    return worker
                if expired is None:
                    active = [worker for worker in self.workers if worker.status in {"busy", "idle", "retiring"}]
                    lane_active = sum(worker.lane == lane for worker in active)
                    live = sum(not worker.handle.confirmed and not (worker.owner_released and worker.handle.state == "read_only") for worker in self.workers)
                    if len(active) < MAX_ACTIVE and lane_active < (1 if lane == "control" else 3) and live < MAX_LIVE:
                        return self._spawn(lane, pool_key, one_shot, deadline, secret_stream=secret_bearing(operation, params))
                    self.condition.wait(timeout=min(0.05, max(0.0, deadline - time.monotonic())))
            if expired is not None:
                self.retire(expired, min(deadline, time.monotonic() + 5))

    def reserve_partial(self, worker: Worker, path: Path) -> None:
        """Reserve a unique missing partial path before its worker receives a download request."""
        with self.condition:
            if path in self.reserved_partials or path.exists() or path.is_symlink() or not path.is_absolute():
                raise ValueError("download_partial_path_not_unique")
            self.reserved_partials.add(path)
            worker.partials.add(path)

    def _cleanup_partials(self, worker: Worker) -> None:
        for path in tuple(worker.partials):
            path.unlink(missing_ok=True)
            self.reserved_partials.discard(path)
            worker.partials.remove(path)

    def release(self, worker: Worker, reply: dict[str, Any], deadline: float) -> StopResult | None:
        """Reuse only a normally completed, nonsecret call inside both recycle bounds."""
        with self.condition:
            worker.calls += 1
            kind = reply.get("error", {}).get("kind")
            reusable = (
                not worker.one_shot
                and kind not in {"deadline", "worker_died", "protocol", "shutdown", "network"}
                and worker.calls < MAX_CALLS
                and time.monotonic() - worker.born < MAX_AGE_SECONDS
                and not self.closing.is_set()
                and (worker.handle.stderr_reader is None or worker.handle.stderr_reader.failure is None)
            )
            if reusable:
                worker.status = "idle"
                self.condition.notify_all()
                return None
            worker.status = "retiring"
        return self.retire(worker, deadline)

    def retire(self, worker: Worker, deadline: float) -> StopResult:
        """Stop one exact handle, retaining survivors and their files without a second signal path."""
        with self.condition:
            while worker.stopping or (not self.closing.is_set() and sum(item.stopping for item in self.workers) >= MAX_STOPPING):
                if time.monotonic() >= deadline:
                    worker.status = "unproven"
                    worker.handle.last_stop = "survivor"
                    self.condition.notify_all()
                    return StopResult("survivor", worker.handle.pid, worker.handle.pgid, worker.handle.confirmed)
                self.condition.wait(timeout=min(0.05, max(0.0, deadline - time.monotonic())))
            if worker.handle.confirmed and worker.cleaned:
                return StopResult("confirmed", worker.handle.pid, worker.handle.pgid, True)
            worker.stopping = True
            worker.status = "stopping"
        try:
            result = StopResult("confirmed", worker.handle.pid, worker.handle.pgid, True) if worker.handle.confirmed else stop_owned(worker.handle, deadline)
            if worker.handle.stderr_reader is not None and not worker.handle.stderr_reader.finish(deadline=deadline):
                self.failed = True
                worker.handle.last_stop = "worker_unproven"
                return StopResult("worker_stderr_unconfirmed", worker.handle.pid, worker.handle.pgid, False)
            if result.confirmed and group_state(worker.handle.pgid) == "gone":
                evidence = {"group": "absent", "token": "not_seen", "members": [], "scan_utc": datetime_utc()}
                self.ledger.close(worker.handle.nonce, worker.handle.pgid, evidence, event="gone", deadline=deadline)
                with self.condition:
                    self._cleanup_partials(worker)
                    worker.cleaned = True
            return result
        except (OSError, ValueError, TimeoutError):
            self.failed = True
            worker.handle.last_stop = "worker_unproven"
            return StopResult("worker_unproven", worker.handle.pid, worker.handle.pgid, False)
        finally:
            with self.condition:
                worker.stopping = False
                worker.status = "closed" if worker.handle.confirmed and worker.cleaned else "unproven"
                self.condition.notify_all()

    def retry_survivors(self, deadline: float) -> list[StopResult]:
        """Retry only owned unreaped leaders. Read-only handles cannot signal anything."""
        with self.condition:
            retry = [
                worker
                for worker in self.workers
                if (worker.status == "unproven" or worker.owner_released)
                and (worker.handle.state != "read_only" or (not worker.cleaned and not worker.owner_released))
            ]
        return [self.retire(worker, deadline) for worker in retry if time.monotonic() < deadline]

    def reconcile_closures(self, closed: dict[str, str]) -> None:
        """Apply observed owner closures without claiming that their present groups are absent."""
        with self.condition:
            for worker in self.workers:
                event = closed.get(worker.handle.nonce)
                if event is None or worker.cleaned:
                    continue
                if event == "closed_by_owner":
                    worker.owner_released = True
                    worker.status = "owner_released"
                    worker.handle.last_stop = "released_by_owner"
                elif worker.handle.state == "read_only" and group_state(worker.handle.pgid) == "gone":
                    worker.handle.confirmed = True
                    worker.handle.last_stop = "confirmed"
                    self._cleanup_partials(worker)
                    worker.cleaned = True
                    worker.status = "closed"
            self.condition.notify_all()

    @property
    def dispatch_paused(self) -> bool:
        """Keep unresolved ownership visible until absence or an explicit owner closure."""
        return (
            self.failed
            or any(worker.handle.stderr_reader is not None and worker.handle.stderr_reader.failure is not None for worker in self.workers)
            or any(worker.handle.last_stop in {"survivor", "worker_unproven"} and not worker.owner_released for worker in self.workers)
        )

    def close(self, deadline: float) -> dict[str, Any]:
        """Reject new work first and stop groups in parallel inside one aggregate deadline."""
        previous_interrupts = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
        try:
            self.closing.set()
            with self.condition:
                workers = [
                    worker
                    for worker in self.workers
                    if (not worker.handle.confirmed or not worker.cleaned) and not (worker.owner_released and worker.handle.state == "read_only")
                ]
                self.condition.notify_all()
            for worker in workers:
                thread = threading.Thread(target=self._close_one, args=(worker, deadline), name=f"sdk_stop_{worker.handle.pid}")
                self.stop_threads.append(thread)
                thread.start()
            for thread in self.stop_threads:
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
            outstanding = [
                worker.handle
                for worker in self.workers
                if (not worker.handle.confirmed or not worker.cleaned or group_state(worker.handle.pgid) != "gone")
                and not (worker.owner_released and worker.handle.state == "read_only")
            ]
            return {
                "confirmed": not outstanding
                and all(worker.cleaned or (worker.owner_released and worker.handle.state == "read_only") for worker in self.workers)
                and not any(thread.is_alive() for thread in self.stop_threads),
                "released_by_owner": [worker.handle.pgid for worker in self.workers if worker.owner_released],
                "survivors": [
                    {"pid": handle.pid, "pgid": handle.pgid, "leader_start": handle.leader_start, "state": handle.state, "status": handle.last_stop}
                    for handle in outstanding
                ],
            }
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_interrupts)

    def _close_one(self, worker: Worker, deadline: float) -> None:
        with contextlib.suppress(OSError, ValueError, TimeoutError):
            self.retire(worker, deadline)


def datetime_utc() -> str:
    """Return an explicitly UTC event time for the private worker ledger."""
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat()
