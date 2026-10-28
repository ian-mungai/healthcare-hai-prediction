"""Supervise in-image SDK calls with an unreaped group leader and an absolute stop deadline."""

from __future__ import annotations

import contextlib
import importlib.util
import os
import secrets
import signal
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit


def _load(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("baked_supervisor_dependency_missing")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


FOLDER = Path(__file__).resolve().parent
_portable = _load("hai_task_group_rules", FOLDER / "deadline_run.py" if (FOLDER / "deadline_run.py").is_file() else FOLDER.parent / "deadline_run.py")
_protocol = _load(
    "hai_task_worker_protocol",
    FOLDER / "worker_protocol.py" if (FOLDER / "worker_protocol.py").is_file() else FOLDER.parents[2] / "orchestration/worker_protocol.py",
)


_stderr = _load(
    "hai_task_worker_stderr", FOLDER / "worker_stderr.py" if (FOLDER / "worker_stderr.py").is_file() else FOLDER.parents[2] / "orchestration/worker_stderr.py"
)


@dataclass
class Handle:
    """An in-task signal authority that ends at its only reap."""

    pid: int
    pgid: int
    stdin: int
    stdout: int
    stderr: int
    state: Literal["signalling", "reap_only", "read_only"] = "signalling"
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    confirmed: bool = False
    stderr_reader: Any = field(default=None, repr=False)


def _members(group: int, deadline: float) -> dict[int, str] | None:
    if not Path("/proc").is_dir():
        return None
    members = {}
    try:
        for entry in Path("/proc").iterdir():
            if time.monotonic() >= deadline:
                return None
            if not entry.name.isdigit():
                continue
            try:
                fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
                if int(fields[2]) == group:
                    members[int(entry.name)] = fields[0]
            except (FileNotFoundError, ProcessLookupError):
                continue
    except (OSError, ValueError, IndexError):
        return None
    return members


def stop_owned(handle: Handle, deadline: float) -> dict[str, Any]:
    """Signal and reap under one lock, keeping the leader until the last group signal."""
    previous_interrupts = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
    try:
        deadline = min(deadline, time.monotonic() + 5)
        if not handle.lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
            return {"status": "survivor", "confirmed": False}
        try:
            with handle.lock:
                if handle.state == "read_only":
                    return {"status": "refused_reaped", "confirmed": handle.confirmed}
                while time.monotonic() < deadline:
                    members = _members(handle.pgid, deadline)
                    if members is None:
                        return {"status": "survivor", "confirmed": False}
                    if handle.pid not in members:
                        handle.state = "read_only"
                        return {"status": "worker_unproven", "confirmed": False}
                    if all(state == "Z" for state in members.values()):
                        for fd in (handle.stdin, handle.stdout, handle.stderr):
                            with contextlib.suppress(OSError):
                                os.close(fd)
                        try:
                            reaped, _ = os.waitpid(handle.pid, os.WNOHANG)
                        except ChildProcessError:
                            reaped = handle.pid
                        if reaped:
                            handle.state = "read_only"
                            handle.confirmed = _portable.wait_group_gone(handle.pgid, deadline, time.monotonic, time.sleep)
                            return {"status": "confirmed" if handle.confirmed else "worker_unproven", "confirmed": handle.confirmed}
                    if handle.state == "reap_only":
                        return {"status": "refused_unproven", "confirmed": False}
                    if _portable.signal_group(handle.pgid, signal.SIGSTOP) == "gone":
                        handle.state = "read_only"
                        return {"status": "worker_unproven", "confirmed": False}
                    if _portable.signal_group(handle.pgid, signal.SIGKILL) == "gone":
                        handle.state = "read_only"
                        return {"status": "worker_unproven", "confirmed": False}
                    time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
                return {"status": "survivor", "confirmed": False}
        finally:
            handle.lock.release()
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_interrupts)


class Supervisor:
    """Run one transfer at a time without a recovery ledger, witness scan or host dependency."""

    def __init__(self, region: str, *, worker: Path | None = None, test_endpoint: str | None = None, stderr_folder: Path | None = None) -> None:
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        if signal.getsignal(signal.SIGCHLD) != signal.SIG_DFL:
            raise RuntimeError("supervisor_requires_default_sigchld")
        if test_endpoint is not None:
            parsed = urlsplit(test_endpoint)
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
                raise ValueError("supervisor_test_endpoint_must_be_loopback")
        self.test_endpoint = test_endpoint
        self.region = region
        self.worker = worker or FOLDER / "sdk_worker.py"
        self.handles: list[Handle] = []
        self.lock = threading.Lock()
        self.closing = False
        self._startup = threading.Lock()
        self.stderr_folder = stderr_folder

    def _spawn(self, deadline: float, *, withhold: bool) -> Handle:
        if not self._startup.acquire(timeout=max(0.0, deadline - time.monotonic())):
            raise TimeoutError("supervisor_startup_deadline")
        try:
            if self.closing or signal.getsignal(signal.SIGCHLD) != signal.SIG_DFL or time.monotonic() >= deadline:
                raise RuntimeError("supervisor_spawn_refused")
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
            environment = {name: os.environ[name] for name in ("PATH", "HOME", "TMPDIR", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") if name in os.environ}
            environment.update(PYTHONDONTWRITEBYTECODE="1", AWS_REGION=self.region, HAI_WORKER_WITNESS=secrets.token_hex(32))
            if self.test_endpoint:
                environment.update(AWS_CONFIG_FILE=os.devnull, AWS_SHARED_CREDENTIALS_FILE=os.devnull, AWS_EC2_METADATA_DISABLED="true")
            arguments = [
                sys.executable,
                "-I",
                "-B",
                str(self.worker),
                "--hai-worker",
                f"{os.getpid()}:{secrets.token_hex(16)}",
                "--region",
                self.region,
                "--one-shot",
            ]
            if self.test_endpoint is not None:
                arguments.extend(("--test-endpoint", self.test_endpoint))
            previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
            try:
                try:
                    pid = os.posix_spawn(
                        sys.executable, arguments, environment, file_actions=actions, setpgroup=0, setsigdef=(signal.SIGCHLD, signal.SIGTERM, signal.SIGINT)
                    )
                except BaseException:
                    for fd in descriptors:
                        os.close(fd)
                    raise
                handle = Handle(pid, pid, control_write, reply_read, error_read)
                self.handles.append(handle)
                for fd in (control_read, reply_write, error_write):
                    os.close(fd)
                for fd in (control_write, reply_read, error_read):
                    os.set_blocking(fd, False)
                if self.stderr_folder is None:
                    self.stderr_folder = Path(tempfile.mkdtemp(prefix="sdk_stderr_"))
                self.stderr_folder.mkdir(parents=True, exist_ok=True, mode=0o700)
                handle.stderr_reader = _stderr.StderrReader(
                    handle.stderr,
                    self.stderr_folder / f"worker_stderr_{pid}.log",
                    deadline=deadline + 5,
                    values=(environment["HAI_WORKER_WITNESS"],),
                    withhold=withhold,
                )
                handle.stderr_reader.start()
                return handle
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous)
        finally:
            self._startup.release()

    def _finish(self, handle: Handle, deadline: float) -> dict[str, Any]:
        cleanup = stop_owned(handle, deadline)
        if handle.stderr_reader is not None and not handle.stderr_reader.finish(deadline=deadline):
            return {"status": "worker_stderr_unconfirmed", "confirmed": False}
        return cleanup

    def call(self, operation: str, params: dict[str, Any], *, deadline: float) -> dict[str, Any]:
        """Perform one closed call, then confirm its whole group ended before returning success."""
        request_id = secrets.token_hex(16)
        reply: dict[str, Any]
        deadline = min(deadline, time.monotonic() + _protocol.cap_seconds(operation))
        if not self.lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
            reply = _protocol.error_reply(request_id, operation, "deadline", "supervisor_admission_deadline")
            return reply
        handle: Handle | None = None
        handles_before = len(self.handles)
        try:
            if self.closing or any(not entry.confirmed for entry in self.handles):
                reply = _protocol.error_reply(request_id, operation, "shutdown", "supervisor_cleanup_unconfirmed")
                return reply
            request = {"id": request_id, "op": operation, "params": params, "budget_s": deadline - time.monotonic()}
            _protocol.validate(request)
            handle = self._spawn(deadline, withhold=_protocol.secret_bearing(operation, params))
            request["budget_s"] = deadline - time.monotonic()
            _protocol.write_frame(handle.stdin, request, deadline)
            reply = _protocol.read_frame(handle.stdout, deadline)
            _protocol.validate(reply, operation, request_id)
        except TimeoutError:
            reply = _protocol.error_reply(request_id, operation, "deadline", "absolute_call_deadline")
        except EOFError:
            reply = _protocol.error_reply(request_id, operation, "worker_died", "worker_pipe_closed")
        except (OSError, ValueError):
            reply = _protocol.error_reply(request_id, operation, "protocol", "worker_frame_refused")
        except RuntimeError:
            reply = _protocol.error_reply(request_id, operation, "shutdown" if self.closing else "worker_died", "supervisor_spawn_refused")
        finally:
            cleanup = {"confirmed": True}
            cleanup_deadline = min(deadline + 5, time.monotonic() + 5)
            for owned in (handle,) if handle is not None else tuple(self.handles[handles_before:]):
                outcome = self._finish(owned, cleanup_deadline)
                if not outcome["confirmed"]:
                    cleanup = outcome
            self.lock.release()
        if not cleanup["confirmed"]:
            self.closing = True
            reply = _protocol.error_reply(request_id, operation, "worker_died", "supervisor_cleanup_unconfirmed")
            return reply
        return reply

    def close(self, *, deadline: float) -> bool:
        """Retain every unconfirmed handle and refuse future transfers."""
        previous_interrupts = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
        try:
            self.closing = True
            if not self._startup.acquire(timeout=max(0.0, deadline - time.monotonic())):
                return False
            try:
                outcomes = [self._finish(handle, deadline) for handle in self.handles]
                return all(outcome["confirmed"] for outcome in outcomes) and all(
                    handle.confirmed and _portable.group_state(handle.pgid) == "gone" for handle in self.handles
                )
            finally:
                self._startup.release()
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_interrupts)
