"""Own worker groups while their leaders remain unreaped. Stored process numbers grant no signal authority."""

from __future__ import annotations

import contextlib
import importlib.util
import os
import re
import signal
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Literal

from orchestration.worker_stderr import StderrReader
from scripts.process import TimeoutExpired, find_program, run_command

WORKER_STOP_SECONDS = 5.0
POLL_SECONDS = 0.1
# How long one stop waits for a leader that the kernel already reports as dying (a zombie, or on macOS a process that carries the exiting flag E) to become
# reapable: at most this many seconds and at most EXIT_GRACE_SHARE of the time the stop has left, so a handle that never becomes reapable cannot use the
# deadline that the handles after it, and the recovery refresh after them, still need. The exit of a worker takes milliseconds, tens of milliseconds when
# memory or disk are busy, and a zombie leader with live threads is not reapable until they end. Only such a leader is waited for: a live handle, a
# reap-only handle and a listing that fails return at once.
EXIT_GRACE_SECONDS = 0.25
EXIT_GRACE_SHARE = 0.25
EXIT_POLL_SECONDS = 0.01
ALLOWED_PYTHON_EXECUTABLES = frozenset({Path(sys.executable).resolve(), (Path(__file__).resolve().parents[1] / ".venv/bin/python").resolve()})
HandleState = Literal["signalling", "reap_only", "read_only"]


def _load_portable() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "services/hai/deadline_run.py"
    spec = importlib.util.spec_from_file_location("hai_portable_group_rules", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("portable_group_helpers_unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_portable = _load_portable()


def group_state(group: int) -> str:
    """Return alive, gone or denied using the byte-preserved portable helper."""
    return str(_portable.group_state(group))


@dataclass(frozen=True)
class Member:
    """A sanitized process-table observation without arguments or environment text."""

    pid: int
    pgid: int
    state: str
    name: str
    witness: bool = False
    marker_nonce: str | None = None
    started: str = ""


@dataclass(frozen=True)
class Snapshot:
    """Process evidence with explicit missing-evidence status."""

    status: str
    members: tuple[Member, ...] = ()
    seen_nonces: frozenset[str] = frozenset()

    def group(self, pgid: int) -> tuple[Member, ...]:
        """Return observations for one group, without treating omissions as absence."""
        return tuple(member for member in self.members if member.pgid == pgid)


def process_snapshot(deadline: float, witness: str | None = None, witnesses: Mapping[str, tuple[int, str]] | None = None) -> Snapshot:
    """Read process states inside two seconds and discard all raw environment text."""
    remaining = min(2.0, deadline - time.monotonic())
    if remaining <= 0:
        return Snapshot("timed_out")
    args = ["-A", "-ww"]
    if witness is not None or witnesses is not None:
        args.append("-E" if sys_platform_is_mac() else "e")
    args += ["-o", "pid=,pgid=,stat=,lstart=,command="]
    try:
        result = run_command("ps", args, env={"PATH": os.defpath, "LC_ALL": "C"}, timeout=remaining)
    except TimeoutExpired:
        return Snapshot("timed_out")
    except OSError:
        return Snapshot("denied")
    if result.returncode:
        return Snapshot("denied")
    members = []
    seen_nonces = set()
    token = re.compile(r"(?:^|\s)HAI_WORKER_WITNESS=" + re.escape(witness) + r"(?:\s|$)") if witness else None
    for line in result.stdout.splitlines():
        fields = line.split(None, 8)
        if len(fields) != 9 or not fields[0].isdigit() or not fields[1].isdigit():
            continue
        command = fields[8]
        for nonce, (pgid, value) in (witnesses or {}).items():
            if pgid == int(fields[1]) and value and re.search(r"(?:^|\s)HAI_WORKER_WITNESS=" + re.escape(value) + r"(?:\s|$)", command):
                seen_nonces.add(nonce)
        marker = re.search(r"(?:^|\s)--hai-worker\s+(?:[0-9]+:)?([a-f0-9]{16,128})(?:\s|$)", command)
        members.append(
            Member(
                int(fields[0]),
                int(fields[1]),
                fields[2],
                Path(command.split()[0]).name,
                bool(token and token.search(command)),
                marker[1] if marker else None,
                " ".join(fields[3:8]),
            )
        )
    return Snapshot("seen", tuple(members), frozenset(seen_nonces))


def normalize_start(text: str) -> str:
    """Collapse the padding of a process-table start time so two reads of one process compare equal."""
    return " ".join(text.split())


def start_epoch(text: str) -> float:
    """Convert a process-table start time to seconds since the epoch, with the table's one-second resolution."""
    return time.mktime(time.strptime(normalize_start(text), "%a %b %d %H:%M:%S %Y"))


def sys_platform_is_mac() -> bool:
    """Select the platform's environment-listing syntax without reading credentials."""
    return os.uname().sysname == "Darwin"


def prepare_parent() -> None:
    """Restore default SIGCHLD before any worker starts, then verify the disposition."""
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    if signal.getsignal(signal.SIGCHLD) != signal.SIG_DFL:
        raise RuntimeError("worker_parent_requires_default_sigchld")


@dataclass
class OwnedProcess:
    """An in-memory authority tied to an unreaped leader and its private pipes."""

    pid: int
    pgid: int
    stdin: int
    stdout: int
    stderr: int
    witness: str = field(repr=False)
    nonce: str
    session: str
    lane: str
    owner: str
    leader_start: str
    spawn_utc: str
    state: HandleState = "signalling"
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    exit_observed: bool = False
    pipes_closed: bool = False
    confirmed: bool = False
    last_stop: str = "owned"
    stderr_reader: StderrReader | None = field(default=None, repr=False)

    def spawn_record(self) -> dict[str, object]:
        """Return the private spawn line. Only its witness field may persist the token."""
        return {
            "nonce": self.nonce,
            "pid": self.pid,
            "pgid": self.pgid,
            "leader_start": self.leader_start,
            "spawn_utc": self.spawn_utc,
            "lane": self.lane,
            "session": self.session,
            "witness": self.witness,
            "kind": "ledger",
            "owner": self.owner,
        }


@dataclass(frozen=True)
class StopResult:
    """A cleanup result that distinguishes refusal, a survivor and confirmed absence."""

    status: str
    pid: int
    pgid: int
    confirmed: bool


def spawn_owned(
    command: Sequence[str],
    environment: Mapping[str, str],
    *,
    witness: str,
    nonce: str,
    session: str,
    lane: str,
    owner: str,
    deadline: float,
    spawn_utc: str | None = None,
) -> OwnedProcess:
    """Create one group leader and its private pipes before the first request can be sent. A recorded intent passes its own spawn time."""
    if time.monotonic() >= deadline:
        raise TimeoutError("worker_spawn_deadline")
    prepare_parent()
    executable = find_program(command[0], environment)
    if executable is None:
        raise FileNotFoundError("owned_executable_unavailable")
    if Path(executable).resolve() not in ALLOWED_PYTHON_EXECUTABLES:
        raise ValueError("owned_executable_not_allowed")
    control_read, control_write = os.pipe()
    reply_read, reply_write = os.pipe()
    error_read, error_write = os.pipe()
    descriptors = (control_read, control_write, reply_read, reply_write, error_read, error_write)
    actions: list[tuple[int, int, int] | tuple[int, int]] = [
        (os.POSIX_SPAWN_DUP2, control_read, 0),
        (os.POSIX_SPAWN_DUP2, reply_write, 1),
        (os.POSIX_SPAWN_DUP2, error_write, 2),
    ]
    actions += [(os.POSIX_SPAWN_CLOSE, fd) for fd in descriptors if fd > 2]
    try:
        child = os.posix_spawn(
            executable,
            [executable, *command[1:]],
            dict(environment),
            file_actions=actions,
            setpgroup=0,
            setsigdef=(signal.SIGCHLD, signal.SIGINT, signal.SIGTERM),
        )
    except BaseException:
        for fd in descriptors:
            os.close(fd)
        raise
    for fd in (control_read, reply_write, error_write):
        os.close(fd)
    for fd in (control_write, reply_read, error_read):
        os.set_blocking(fd, False)
    return OwnedProcess(
        child,
        child,
        control_write,
        reply_read,
        error_read,
        witness,
        nonce,
        session,
        lane,
        owner,
        f"unreaped:{child}",
        spawn_utc or datetime.now(UTC).isoformat(),
    )


def record_leader_start(handle: OwnedProcess, deadline: float) -> None:
    """Record the leader's process-table start time before the first request is sent."""
    remaining = min(2.0, deadline - time.monotonic())
    if remaining <= 0:
        raise TimeoutError("worker_start_evidence_deadline")
    try:
        result = run_command("ps", ["-p", str(handle.pid), "-o", "lstart="], env={"PATH": os.defpath, "LC_ALL": "C"}, timeout=remaining)
    except (OSError, TimeoutExpired) as error:
        raise RuntimeError("worker_start_evidence_unavailable") from error
    if result.returncode or not result.stdout.strip():
        raise RuntimeError("worker_start_evidence_unavailable")
    handle.leader_start = normalize_start(result.stdout)


def _close_pipes(handle: OwnedProcess) -> None:
    if handle.pipes_closed:
        return
    for fd in (handle.stdin, handle.stdout, handle.stderr):
        with contextlib.suppress(OSError):
            os.close(fd)
    handle.pipes_closed = True


def _dying(state: str) -> bool:
    """Return True for a zombie (``Z``) and for a macOS process that carries the exiting flag (``E`` after the state letter, as in ``?E``).

    Its address space and environment are gone, so it can show no witness. A bare ``?`` only means that the task query failed, and Linux has no ``E`` flag,
    so neither is dying: they stay live members and keep the witness requirement.
    """
    return state.startswith("Z") or "E" in state[1:]


def stop_owned(handle: OwnedProcess, deadline: float) -> StopResult:
    """Signal and reap under one lock. Reaping irrevocably removes signal authority."""
    previous_interrupts = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
    try:
        deadline = min(deadline, time.monotonic() + WORKER_STOP_SECONDS)
        if not handle.lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
            return StopResult("survivor", handle.pid, handle.pgid, False)
        try:
            with handle.lock:
                if handle.state == "read_only":
                    return StopResult("refused_reaped", handle.pid, handle.pgid, handle.confirmed)
                if time.monotonic() >= deadline:
                    handle.last_stop = "worker_unproven" if handle.state == "reap_only" else "survivor"
                    return StopResult(handle.last_stop, handle.pid, handle.pgid, False)
                if handle.state == "signalling":
                    snapshot = process_snapshot(deadline, handle.witness)
                    members = snapshot.group(handle.pgid)
                    live = tuple(member for member in members if not _dying(member.state))
                    leader = next((member for member in members if member.pid == handle.pid), None)
                    if snapshot.status != "seen" or leader is None or (live and not any(member.witness for member in members)):
                        handle.state = "reap_only"
                    elif live:
                        if _portable.signal_group(handle.pgid, signal.SIGSTOP) == "gone":
                            handle.state = "read_only"
                            handle.last_stop = "worker_unproven"
                            _close_pipes(handle)
                            return StopResult(handle.last_stop, handle.pid, handle.pgid, False)
                        rescanned = process_snapshot(deadline, handle.witness)
                        if rescanned.status != "seen" or not any(member.witness for member in rescanned.group(handle.pgid)):
                            _portable.signal_group(handle.pgid, signal.SIGCONT)
                            handle.state = "reap_only"
                ready_to_reap = handle.state == "reap_only"
                dying_leader = False
                while not ready_to_reap and time.monotonic() < deadline:
                    snapshot = process_snapshot(deadline)
                    if snapshot.status != "seen":
                        break
                    members = snapshot.group(handle.pgid)
                    leader = next((member for member in members if member.pid == handle.pid), None)
                    if leader is None:
                        handle.state = "reap_only"
                        ready_to_reap = True
                        break
                    live = tuple(member for member in members if not _dying(member.state))
                    if not live and (handle.exit_observed or _dying(leader.state)):
                        ready_to_reap = True
                        dying_leader = _dying(leader.state)
                        break
                    if _portable.signal_group(handle.pgid, signal.SIGKILL) == "gone":
                        handle.state = "read_only"
                        handle.last_stop = "worker_unproven"
                        _close_pipes(handle)
                        return StopResult(handle.last_stop, handle.pid, handle.pgid, False)
                    time.sleep(min(POLL_SECONDS, max(0.0, deadline - time.monotonic())))
                now = time.monotonic()
                grace_end = now + min(EXIT_GRACE_SECONDS, EXIT_GRACE_SHARE * max(0.0, deadline - now))
                while ready_to_reap and time.monotonic() < deadline:
                    try:
                        reaped, _ = os.waitpid(handle.pid, os.WNOHANG)
                    except ChildProcessError:
                        reaped = handle.pid
                    if reaped:
                        handle.state = "read_only"
                        _close_pipes(handle)
                        handle.confirmed = _portable.wait_group_gone(handle.pgid, deadline, time.monotonic, time.sleep)
                        handle.last_stop = "confirmed" if handle.confirmed else "worker_unproven"
                        return StopResult(handle.last_stop, handle.pid, handle.pgid, handle.confirmed)
                    if not dying_leader or time.monotonic() >= grace_end:
                        break
                    time.sleep(min(EXIT_POLL_SECONDS, max(0.0, grace_end - time.monotonic()), max(0.0, deadline - time.monotonic())))
                handle.last_stop = "worker_unproven" if handle.state == "reap_only" else "survivor"
                status = "refused_unproven" if handle.state == "reap_only" else "survivor"
                return StopResult(status, handle.pid, handle.pgid, False)
        finally:
            handle.lock.release()
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_interrupts)
