#!/usr/bin/env python3
"""The in-image deadline entrypoint of a finite job (installed as /opt/hai/deadline_run).

    HAI_DEADLINE_UTC=<epoch seconds> /opt/hai/deadline_run <command> [argument ...]

The launcher fixes the absolute deadline in the container's create specification and runs this program in front of the
template's command under ``--init``. The program is its own watchdog, armed before the command exists (D1 section 5
item 1). It uses only the Python 3 standard library.

1. A bracketed sample: monotonic ``M1``, wall ``W0``, monotonic ``M2``. A bracket wider than 50 ms is retried up to 10
   times and then the program exits 124 without starting the command. ``E = M1 + (D - S - W0)`` is the single absolute
   monotonic expiry; ``S`` (2 s) is the admitted host-to-VM clock skew.
2. ``B = E - S - 1`` is the command-start boundary. The command never starts when the monotonic clock is not before ``B``.
3. The program forks; the child puts itself in its own process group and, immediately before ``exec``, rechecks that the
   monotonic clock is before ``B`` (exit 124 without running the command if not). At ``E`` the group gets TERM; at
   ``E + K`` (``K`` = 55 s) it gets KILL; the program then exits 124. Only ESRCH proves a group ended: a denied signal
   (EPERM) is retried for 5 s and then reported on stderr with exit code 123 (group not proved ended; the launcher's
   own enforcement and the removal of the container are the backstop).

Exit codes: the command's own code (128 + signal when it was killed); 124 on a missed boundary or a deadline stop;
125 when the deadline variable or the command is missing or malformed or the relay child fails before the command ends (it names the
exception on stderr); 123 when a process group is not proved ended.
"""

from __future__ import annotations

import contextlib
import os
import signal
import sys
import time
from collections.abc import Callable

SKEW = 2.0
KILL_GRACE = 55.0
BRACKET_LIMIT = 0.050
BRACKET_TRIES = 10
START_MARGIN = 1.0
POLL = 0.02
TIMEOUT_EXIT = 124
USAGE_EXIT = 125
UNCONFIRMED_EXIT = 123
NOT_FOUND_EXIT = 127
REAP_WAIT = 5.0
FAULT_TEXT_LIMIT = 200


def bracketed_sample(wall: Callable[[], float], mono: Callable[[], float]) -> tuple[float, float] | None:
    """Return ``(M1, W0)`` from a sample whose monotonic bracket is at most 50 ms, or None after 10 wide brackets."""
    for _ in range(BRACKET_TRIES):
        first = mono()
        wall_now = wall()
        second = mono()
        if second - first <= BRACKET_LIMIT:
            return first, wall_now
    return None


def expiry(deadline_utc: float, first: float, wall_now: float) -> tuple[float, float]:
    """Return the absolute monotonic expiry ``E`` and the command-start boundary ``B``."""
    absolute = first + (deadline_utc - SKEW - wall_now)
    return absolute, absolute - SKEW - START_MARGIN


def group_state(group: int) -> str:
    """Probe a process group with signal 0: ``gone`` (ESRCH proves it ended), ``alive`` or ``denied`` (EPERM)."""
    try:
        os.killpg(group, 0)
    except ProcessLookupError:
        return "gone"
    except PermissionError:
        return "denied"
    return "alive"


def signal_group(group: int, number: int) -> str:
    """Send a signal to the group and return ``sent``, ``gone`` (ESRCH) or ``denied`` (EPERM).

    EPERM never proves that the group ended: macOS (where the acceptance test runs) refuses the signal for a group whose
    members have exited but are not reaped yet, and any system refuses it for a live member it may not signal. A denied
    signal stays visible to the caller, which treats the group as ended only after ESRCH.
    """
    try:
        os.killpg(group, number)
    except ProcessLookupError:
        return "gone"
    except PermissionError:
        return "denied"
    return "sent"


def wait_group_gone(group: int, until: float, mono: Callable[[], float], sleep: Callable[[float], None]) -> bool:
    """Wait until ``group`` is proved gone (ESRCH) or the monotonic clock reaches ``until``."""
    while True:
        if group_state(group) == "gone":
            return True
        if mono() >= until:
            return False
        sleep(POLL)


def exit_code(status: int) -> int:
    """The shell-style exit code of a wait status."""
    if os.WIFSIGNALED(status):
        return 128 + os.WTERMSIG(status)
    return os.WEXITSTATUS(status)


def unconfirmed(group: int, what: str) -> int:
    """Report a group that is not proved ended and return the dedicated exit code."""
    sys.stderr.write(f"deadline_run: process group {group} not proved ended after {what}; the launcher removes the container\n")
    return UNCONFIRMED_EXIT


def spawn(command: list[str], boundary: float, mono: Callable[[], float]) -> int:
    """Fork the relay of the command and return its process ID (D1 section 5 item 1 steps 3 and 4).

    The child puts itself in a new process group, rechecks that the monotonic clock is before ``B`` immediately before it
    starts the command and exits 124 without running the command otherwise. The command joins the child's group, so the
    group's leader is the relay: it ignores TERM and INT (the command gets them through the group with default handling)
    and returns the command's exit code, which keeps the leader alive until the command and its group are stopped.
    """
    child = os.fork()
    if child:
        return child
    try:
        try:
            os.setpgid(0, 0)
        except PermissionError:
            # macOS can refuse this call once the parent's setpgid has already made the child its group leader; only that case may go on
            if os.getpgrp() != os.getpid():
                raise
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        if mono() >= boundary:
            os._exit(TIMEOUT_EXIT)
        command_pid = os.posix_spawnp(command[0], command, os.environ, setpgroup=os.getpgrp(), setsigdef=(signal.SIGTERM, signal.SIGINT))
        _, raw = os.waitpid(command_pid, 0)
        os._exit(exit_code(raw))
    except FileNotFoundError:
        sys.stderr.write(f"deadline_run: {command[0]} not found\n")
        os._exit(NOT_FOUND_EXIT)
    except BaseException as exc:
        with contextlib.suppress(BaseException):
            text = " ".join(f"{type(exc).__name__}: {exc}".split())
            sys.stderr.write(f"deadline_run: relay child failed: {text[:FAULT_TEXT_LIMIT]}\n")
            sys.stderr.flush()
        os._exit(USAGE_EXIT)


def run(
    command: list[str],
    deadline_utc: float,
    wall: Callable[[], float] = time.time,
    mono: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Run ``command`` under the deadline and return the exit code."""
    sample = bracketed_sample(wall, mono)
    if sample is None:
        return TIMEOUT_EXIT
    stop_at, boundary = expiry(deadline_utc, *sample)
    if mono() >= boundary:
        return TIMEOUT_EXIT
    child = spawn(command, boundary, mono)
    with contextlib.suppress(OSError):
        os.setpgid(child, child)  # the child does the same; whichever runs second finds it done or the child gone
    forwarded: list[int] = []

    def forward(number: int, _frame: object) -> None:
        forwarded.append(number)
        signal_group(child, signal.SIGTERM)

    signal.signal(signal.SIGTERM, forward)
    signal.signal(signal.SIGINT, forward)
    status: int | None = None
    termed = killed = False
    while True:
        done, raw = os.waitpid(child, os.WNOHANG)
        if done:
            status = raw
            break
        now = mono()
        if not termed and now >= stop_at:
            signal_group(child, signal.SIGTERM)
            termed = True
        if not killed and now >= stop_at + KILL_GRACE:
            signal_group(child, signal.SIGKILL)
            killed = True
            # Reap the leader within a bound; a denied KILL must not block the program past the deadline window.
            limit = stop_at + KILL_GRACE + REAP_WAIT
            while mono() < limit:
                done, raw = os.waitpid(child, os.WNOHANG)
                if done:
                    status = raw
                    break
                sleep(POLL)
            if status is None:
                return unconfirmed(child, "KILL (the group leader was not reaped)")
            break
        sleep(POLL)
    # Whatever the command left in its group is stopped with it; only ESRCH proves the group ended (EPERM does not).
    outcome = signal_group(child, signal.SIGKILL)
    if outcome != "gone" and not wait_group_gone(child, mono() + REAP_WAIT, mono, sleep):
        return unconfirmed(child, "the final KILL")
    if termed:
        return TIMEOUT_EXIT
    return exit_code(status)


def main(argv: list[str]) -> int:
    """Read the deadline from the environment and run the command."""
    command = argv[1:]
    if command[:1] == ["--"]:
        command = command[1:]
    try:
        deadline = float(os.environ["HAI_DEADLINE_UTC"])
    except (KeyError, ValueError):
        sys.stderr.write("deadline_run: HAI_DEADLINE_UTC must be epoch seconds\n")
        return USAGE_EXIT
    if not command:
        sys.stderr.write("deadline_run: no command\n")
        return USAGE_EXIT
    return run(command, deadline)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
