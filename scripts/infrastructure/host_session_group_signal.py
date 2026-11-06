"""A terminal Ctrl-C during the host-off request, with a real process and a real AWS command stand-in.

A terminal sends SIGINT to the whole foreground process group, not only to the session. This module forks a driver into its own process group, runs the
real ``Session.stop`` through ``AwsCli`` and the real runner, and replaces ``aws`` with a small Python program that applies a capacity change only
after a short in-flight window. The case sends SIGINT to the whole group during that window and checks that the host still reaches capacity zero and is
verified gone, within a bounded deadline.

The stand-in is ``host_session_fake_aws.py``; it imports nothing from the repository, so it runs with ``python -I``.
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

from scripts.acquisition.s3_store import AwsCli
from scripts.infrastructure import host_session_fake_aws as fake_aws
from scripts.infrastructure.host_session_fake_aws import GROUP, STATE_ENV
from scripts.process import SESSION_VARIABLE

DEADLINE_SECONDS = 30.0
POLL_SECONDS = 0.02
CLEANUP_SECONDS = 5.0
HUNG_DEADLINE_SECONDS = 1.5
HANG_SECONDS = 15.0


def drive_stop(module: ModuleType, root: Path, bin_directory: Path) -> int:
    """In the forked driver, which runs as a launched command (the launch variable is set, as in every CI group): put the stand-in first on PATH, run the
    real ``stop`` and write its report lines under ``root``. Returns the exit code."""
    os.setsid()
    os.environ["PATH"] = f"{bin_directory}{os.pathsep}{os.environ['PATH']}"
    os.environ[STATE_ENV] = str(root)
    os.environ[SESSION_VARIABLE] = "1"

    def report(line: str) -> None:
        with (root / "report.txt").open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    names = module.Names("group-signal-cluster", GROUP, "group-signal-provider", "group-signal-project", {"postgres": "group-signal-postgres"}, {})
    cli = AwsCli({"aws_profile": "group-signal-profile", "aws_region": "group-signal-region"}, module.tolerant_runner)
    try:
        module.Session(cli, names, module.Clock(), root / "marker", report).stop()
    except (module.HostSessionError, KeyboardInterrupt, module.TerminationRequested) as error:
        (root / "failure.txt").write_text(type(error).__name__, encoding="utf-8")
        return 1
    return 0


class Driver:
    """The forked driver: the one owner of its process group and of its reaping. The group is signaled only while the driver is not yet reaped."""

    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.reaped = False
        self.status: int | None = None

    def reap(self) -> bool:
        """Collect the driver's exit without waiting; true once it is reaped. After the reap the process identifier is not ours."""
        if not self.reaped:
            finished, raw = os.waitpid(self.pid, os.WNOHANG)
            if finished:
                self.reaped, self.status = True, os.waitstatus_to_exitcode(raw)
        return self.reaped

    def signal(self, number: int) -> bool:
        """Signal the driver's group, only while the unreaped driver still holds the group identifier; false when the driver is already reaped."""
        if self.reaped:
            return False
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(self.pid, number)
        return True

    def stop(self) -> None:
        """Bounded cleanup: kill the group while its leader is unreaped, then reap within the cleanup limit."""
        if self.reap():
            return
        self.signal(signal.SIGKILL)
        end = time.monotonic() + CLEANUP_SECONDS
        while not self.reap() and time.monotonic() < end:
            time.sleep(POLL_SECONDS)


def wait_for(path: Path, driver: Driver, deadline: float) -> bool:
    """True when ``path`` exists before ``deadline`` and the driver has not exited."""
    while time.monotonic() < deadline:
        if path.exists():
            return True
        if driver.reap():
            return False
        time.sleep(POLL_SECONDS)
    return False


class Seen:
    """What the observer saw: the reaped identifiers, every group signal sent as (identifier, name) and those sent after a reap.

    A plain class, not a dataclass: a reviewer's probe loads this file under another module name without registering it, which a dataclass cannot survive.
    """

    def __init__(self) -> None:
        self.reaped: set[int] = set()
        self.events: list[tuple[int, str]] = []
        self.stale: list[tuple[int, str]] = []

    def sent_to(self, pid: int) -> list[str]:
        """The names of the signals sent to one group, in order."""
        return [name for target, name in self.events if target == pid]


@contextlib.contextmanager
def observed_signals() -> Iterator[Seen]:
    """Record every reap and group signal made through ``os`` for as long as the block runs; the case runs its whole body, cleanup included, inside it."""
    seen = Seen()
    real_waitpid, real_killpg = os.waitpid, os.killpg

    def waitpid(pid: int, flags: int) -> tuple[int, int]:
        result = real_waitpid(pid, flags)
        if result[0]:
            seen.reaped.add(result[0])
        return result

    def killpg(pid: int, number: int) -> None:
        event = (pid, signal.Signals(number).name)
        seen.events.append(event)
        if pid in seen.reaped:
            seen.stale.append(event)
        real_killpg(pid, number)

    os.waitpid, os.killpg = waitpid, killpg
    try:
        yield seen
    finally:
        os.waitpid, os.killpg = real_waitpid, real_killpg


def supervise(child: Callable[[], int], requested: Path, seconds: float) -> Driver:
    """Fork ``child`` into its own group, send SIGINT to the group once ``requested`` exists and clean up within the deadline."""
    deadline = time.monotonic() + seconds
    pid = os.fork()
    if pid == 0:
        os._exit(child())
    driver = Driver(pid)
    try:
        if wait_for(requested, driver, deadline):
            driver.signal(signal.SIGINT)
        while not driver.reap() and time.monotonic() < deadline:
            time.sleep(POLL_SECONDS)
    finally:
        driver.stop()
    return driver


def hung_child(requested: Path) -> Callable[[], int]:
    """A driver that asks for the signal, ignores SIGINT and outlives the deadline."""

    def child() -> int:
        os.setsid()
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        requested.write_text("requested", encoding="utf-8")
        time.sleep(HANG_SECONDS)
        return 0

    return child


def early_child() -> int:
    """A driver that leads its own group and exits before it asks for any signal."""
    os.setsid()
    return 0


def execute(module: ModuleType) -> dict[str, Any]:
    """The whole case, from the first driver start to the removal of its folder: the facts that ``run`` checks. Nothing here is outside the observer."""
    with tempfile.TemporaryDirectory(prefix="host_session_group_") as directory:
        root = Path(directory)
        bin_directory = root / "bin"
        bin_directory.mkdir()
        shim = bin_directory / "aws"
        shim.write_text(f'#!/bin/sh\nexec "{sys.executable}" -I "{Path(fake_aws.__file__).resolve()}" "$@"\n', encoding="utf-8")
        shim.chmod(0o755)
        (root / "group.json").write_text(json.dumps({"desired": 1, "instances": ["i-stand-in"]}), encoding="utf-8")
        driver = supervise(lambda: drive_stop(module, root, bin_directory), root / "requested", DEADLINE_SECONDS)
        status = driver.status
        state = json.loads((root / "group.json").read_text(encoding="utf-8"))
        session_file = root / "session.json"
        session = json.loads(session_file.read_text(encoding="utf-8")) if session_file.exists() else {}
        lines = (root / "report.txt").read_text(encoding="utf-8").splitlines() if (root / "report.txt").exists() else []
        started = time.monotonic()
        hung = supervise(hung_child(root / "hung_requested"), root / "hung_requested", HUNG_DEADLINE_SECONDS)
        hung_seconds = time.monotonic() - started
        early = supervise(early_child, root / "never", DEADLINE_SECONDS)
    facts: dict[str, Any] = {
        "pid": driver.pid,
        "status": status,
        "state": state,
        "session": session,
        "lines": lines,
        "hung_pid": hung.pid,
        "hung_status": hung.status,
        "hung_seconds": hung_seconds,
        "early_pid": early.pid,
        "early_status": early.status,
    }
    return facts


def run(module: ModuleType) -> dict[str, bool]:
    """Send SIGINT to the whole process group of a running ``stop`` during its capacity-zero request and check the outcome.

    The observer wraps the complete case, so a signal in the cleanup of the caller or at the end of the case is seen like any other.
    """
    with observed_signals() as seen:
        facts = execute(module)
    state, lines, session = facts["state"], facts["lines"], facts["session"]
    return {
        "a SIGINT to the whole process group during the host-off request ends the stop within the deadline": facts["status"] is not None,
        "the stop does not fail after a group SIGINT": facts["status"] == 0,
        "the host reaches capacity zero after a group SIGINT": state["desired"] == 0,
        "the host-off request leads a session of its own inside a launched command": bool(session) and session["sid"] == session["pid"],
        "no instance is left after a group SIGINT": not state["instances"],
        "the stop reports the host verified gone after a group SIGINT": any(line.startswith("host_off_verified") for line in lines),
        "the group SIGINT is reported as deferred": any("signal_deferred" in line and "SIGINT" in line for line in lines),
        "every driver is reaped through the observed wait": {facts["pid"], facts["hung_pid"], facts["early_pid"]} <= seen.reaped,
        "no signal is sent to any group after its driver is reaped, anywhere in the case": not seen.stale,
        "the group receives the one SIGINT and nothing else": seen.sent_to(facts["pid"]) == ["SIGINT"],
        "a driver that outlives the deadline is killed through its group and reaped within the cleanup limit": facts["hung_status"] == -signal.SIGKILL
        and facts["hung_seconds"] < HUNG_DEADLINE_SECONDS + CLEANUP_SECONDS,
        "the group of a driver that outlives the deadline gets SIGINT, then SIGKILL, both before the reap": seen.sent_to(facts["hung_pid"])
        == ["SIGINT", "SIGKILL"],
        "a driver that exits early is reaped and its group receives no signal": facts["early_status"] == 0 and seen.sent_to(facts["early_pid"]) == [],
    }
