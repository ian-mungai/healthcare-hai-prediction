"""Stop private test launchers through their sockets and preserve unresolved recovery files."""

from __future__ import annotations

import atexit
import contextlib
import fcntl
import json
import os
import shlex
import signal
import socket
import sys
import threading
import time
from collections.abc import Callable, Iterator
from functools import wraps
from pathlib import Path
from typing import Any

from orchestration.launcher import PROTOCOL
from scripts.process import SubprocessError, run_command

PING_SECONDS = 3.0
SHUTDOWN_SECONDS = 5.0
END_SECONDS = 15.0
TERMINATION: list[int] = []


def record_termination(number: int) -> None:
    """Keep the first signal even if a runner catches SystemExit or Python ignores it in an exit callback."""
    if not TERMINATION:
        TERMINATION.append(number)


def exit_after_termination() -> None:
    """Preserve the signal exit status after all later-registered cleanup callbacks have finished."""
    if TERMINATION:
        for stream in (sys.stdout, sys.stderr):
            with contextlib.suppress(OSError, ValueError):
                stream.flush()
        os._exit(128 + TERMINATION[0])


atexit.register(exit_after_termination)


def lock_held(registry: Path) -> bool:
    """Return whether the launcher lock is held or cannot be inspected."""
    try:
        descriptor = os.open(registry / "launcher.lock", os.O_RDONLY)
    except FileNotFoundError:
        return False
    except OSError:
        return True
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return True
    finally:
        os.close(descriptor)
    return False


def ask(registry: Path, method: str, params: dict[str, Any], principal: dict[str, Any], seconds: float) -> dict[str, Any]:
    """Exchange one socket request within a total budget of seconds. Raise on timeout or refusal."""
    deadline = time.monotonic() + seconds
    # A registry outlives its launchers and a launcher replays the recorded result of a repeated request ID, so every request needs a fresh ID.
    message = {"v": PROTOCOL, "id": f"cleanup-{os.getpid()}-{method}-{os.urandom(6).hex()}", "method": method, "params": params, "principal": principal}

    def remaining() -> float:
        left = deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError(f"{method}: no complete reply within {seconds} s")
        return left

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(remaining())
        sock.connect(str(registry / "launcher.sock"))
        sock.settimeout(remaining())
        sock.sendall(json.dumps(message).encode() + b"\n")
        buffer = b""
        while b"\n" not in buffer:
            sock.settimeout(remaining())
            chunk = sock.recv(65536)
            if not chunk:
                raise OSError("the launcher closed the connection")
            buffer += chunk
    reply = json.loads(buffer.split(b"\n", 1)[0])
    if not reply.get("ok"):
        raise ValueError(str(reply.get("error")))
    return dict(reply["result"])


def launcher_running(registry: Path, seconds: float) -> bool:
    """Observe detached launcher processes and hosted launcher threads. Failed observation raises an error."""
    result = run_command("ps", ["-axww", "-o", "pid=,command="], timeout=seconds)
    if result.returncode or not result.stdout.strip():
        raise OSError("the process listing could not establish launcher completion")
    tail = ["-m", "orchestration.launcher", "--test-registry", str(registry.resolve()), "serve"]
    for line in result.stdout.splitlines():
        _, _, command = line.strip().partition(" ")
        try:
            arguments = shlex.split(command)
        except ValueError:
            if str(registry) in command:
                raise OSError("the launcher command could not be parsed") from None
            continue
        if arguments[-len(tail) :] == tail:
            return True
    return any(thread.name.startswith(("launcher-worker", "in-process-launcher", "hosted-launcher")) for thread in threading.enumerate())


@contextlib.contextmanager
def defer_termination() -> Iterator[list[int]]:
    """Finish cleanup after TERM or HUP and return the first deferred signal to the caller."""
    pending: list[int] = []
    if threading.current_thread() is not threading.main_thread():
        yield pending
        return
    previous = {number: signal.getsignal(number) for number in (signal.SIGTERM, signal.SIGHUP)}

    def remember(number: int, _frame: object) -> None:
        record_termination(number)
        if not pending:
            pending.append(number)

    try:
        for number in previous:
            signal.signal(number, remember)
        yield pending
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def finish_cleanup[**P, T](function: Callable[P, T]) -> Callable[P, T]:
    """Defer termination through this entire cleanup, then dispatch it to the restored outer handler."""

    @wraps(function)
    def finish(*args: P.args, **kwargs: P.kwargs) -> T:
        interrupted: list[int] = []
        try:
            with defer_termination() as interrupted:
                return function(*args, **kwargs)
        finally:
            if interrupted:
                signal.raise_signal(interrupted[0])

    return finish


@finish_cleanup
def end_launcher(registry: Path, nonce: str, seconds: float = END_SECONDS) -> str:
    """Request owned shutdown within one budget and require both a free lock and completed processes and workers.

    Return none, stopped, foreign or survived. A foreign nonce receives no shutdown. Uncertain completion returns survived.
    Nonce-free launchers rely on the caller owning the private registry and its starter tree. No process receives a signal.
    """
    deadline = time.monotonic() + seconds
    held = lock_held(registry)
    if held:
        try:
            left = deadline - time.monotonic()
            if left <= 0:
                return "survived"
            theirs = str(ask(registry, "ping", {}, {"kind": "any"}, min(PING_SECONDS, left)).get("launch_nonce") or "")
            if theirs and theirs != nonce:
                return "foreign"
            params: dict[str, Any] = {"owner": True, **({"launch_nonce": nonce} if theirs else {})}
            left = deadline - time.monotonic()
            if left > 0:
                ask(registry, "shutdown", params, {"kind": "any", "owner": True}, min(SHUTDOWN_SECONDS, left))
        except (OSError, ValueError, TimeoutError, KeyError):
            pass
    while (left := deadline - time.monotonic()) > 0:
        try:
            running = launcher_running(registry, left)
        except (OSError, SubprocessError):
            return "survived"
        if not running and not lock_held(registry):
            return "stopped" if held else "none"
        time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
    return "survived"


def leave_on_signal(number: int, _frame: object) -> None:
    """Raise SystemExit on TERM or HUP so the runner executes its cleanup."""
    record_termination(number)
    for other in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(other, signal.SIG_IGN)
    raise SystemExit(128 + number)


def exit_on_termination() -> None:
    """Install termination handlers in the main thread before starting a launcher."""
    if threading.current_thread() is threading.main_thread():
        for number in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(number, leave_on_signal)
