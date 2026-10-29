"""The one place this repository's scripts start another program.

Every call resolves the program to a full path, passes a list of arguments (never a shell string), closes stdin and
applies a timeout. A top-level call starts the program in a new session (``setsid``), which it leads; the call names it to its descendants with
``PROCESS_LAUNCH_SESSION=1``. At the deadline, or when the caller is interrupted, every member of that session stops: the call lists the live
processes (``ps -A -o pid=,ppid=,stat=``; macOS ``pgrep`` has no session option and its ``ps`` shows the session as 0) and signals those whose
``os.getsid`` is the session: TERM, then KILL after a grace period, then a check that no member is left. A call made inside such a session (a nested
launch, whose environment carries the variable) stays in that session: it starts its program in a new process group and stops only that group at its
own deadline; the outer stop reaches it as a session member. A member that starts its own session (``setsid``) without this launcher is outside the
session and is not stopped. The leader is reaped only after the stop, so its pid, which is the session id, cannot be reused while the stop runs. If
the session cannot be listed (the lister cannot start, fails or stalls: its own group is killed and reaped within a bound), the stop still signals
the leader's group and the call fails as if a member survived. Each member's session is read again just before its signal. SIGINT is held while the
program starts and while it is stopped, and is raised once the stop is confirmed. Python delivers SIGINT to the main thread only, so a caller that
runs jobs in threads uses ``LaunchPool`` instead of ``ThreadPoolExecutor``: the first SIGINT sets the pool's cancellation, each job's ``run_command``
(it waits in short slices) stops its own session and raises ``LaunchCancelled``, the pool waits for every job within ``CANCEL_WAIT_SECONDS``, and only
then forwards the interrupt (``LaunchesNotStopped`` when a stop is not confirmed or a job does not end). A thread outside a ``LaunchPool`` is unchanged.
Every stop returns a ``StopOutcome`` (truthy when the stop is NOT confirmed; it names the survivors). The outcome is recorded on every path of
``run_command``: a normal end finalizes the launch's session and raises ``LaunchesNotStopped`` when a member survives; a deadline raises
``ProcessGroupSurvived``; a cancellation or any other exception (an interrupt, an alarm, a
handler that raises) raises ``LaunchesNotStopped`` chained from it; and a stop that raises counts as unconfirmed. An unconfirmed stop is also kept in
memory in the process (``record_unconfirmed``) and printed to standard error when it happens, so a caller that catches the exception and carries on
(``except TimeoutExpired``) cannot lose it: the interpreter then exits with status ``UNCONFIRMED_EXIT_STATUS`` (70), unless the caller took the record
with ``take_unconfirmed`` to report it itself. A ``LaunchPool`` counts a job's end as no cleanup: it raises ``LaunchesNotStopped`` when it is left with
any unconfirmed stop of its jobs, with or without an interrupt. The lister of the session listing is killed and reaped on every way out of the listing
(its own deadline, an alarm, an interrupt).
A top-level launch also finalizes its session when the program ends normally (``finalize_session``): a member the program left behind (a child that outlives
a command that exits at its own deadline) is stopped, and one that survives KILL is recorded and raised as ``LaunchesNotStopped``; a nested launch is not
finalized (its members are in the outer session, which is finalized when the outer command ends). A stop that is cut short by any ``BaseException`` (an alarm
handler that raises ``SystemExit``) is recorded as unconfirmed, the leader's group gets a last KILL, and the call raises the named failure chained from it.
The fallback of a session that cannot be listed never signals the caller's own group (the CI worker's confirming leader): it reports the failure and returns
the unconfirmed outcome.
Other scripts import
``run_command`` from here instead of ``subprocess``, so the lint exception for starting a process exists on one line. Run scripts that import it as
modules from the repository root, for example ``.venv/bin/python -m scripts.quality.scan_secrets``.
"""

from __future__ import annotations

import atexit
import contextlib
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from subprocess import CalledProcessError, CompletedProcess, SubprocessError, TimeoutExpired
from types import FrameType, TracebackType
from typing import Any, ParamSpec, Self, TypeVar

__all__ = [
    "CompletedProcess",
    "LaunchCancelled",
    "LaunchPool",
    "LaunchesNotStopped",
    "ProcessGroupSurvived",
    "SessionListingFailed",
    "StopOutcome",
    "SubprocessError",
    "TimeoutExpired",
    "UNCONFIRMED_EXIT_STATUS",
    "clear_git_environment",
    "find_program",
    "record_unconfirmed",
    "run_command",
    "stop_launch",
    "stop_process_group",
    "stop_sessions",
    "take_unconfirmed",
]

DEFAULT_TIMEOUT_SECONDS = 300
# Seconds a process group or session gets to end after TERM, and to disappear after KILL, before the stop is reported as failed.
STOP_GRACE_SECONDS = 5.0
CONFIRM_SECONDS = 5.0
POLL_SECONDS = 0.05
# The environment variable that marks a process inside a session that a launch started; a launch made there is nested.
SESSION_VARIABLE = "PROCESS_LAUNCH_SESSION"
# The command that lists every live process with its parent and state, and the seconds it may take.
SESSION_LISTING = ("ps", "-A", "-o", "pid=,ppid=,stat=")
LISTING_SECONDS = 5.0
# Seconds the lister gets to be reaped after its group got KILL at the listing deadline.
REAP_SECONDS = 1.0
# A job of a ``LaunchPool`` waits for its program in slices of this many seconds and looks at the pool's cancellation between them.
SLICE_SECONDS = 0.1
# Seconds a ``LaunchPool`` waits for its jobs to stop their sessions after an interrupt: two stops (TERM and KILL phases) that each end late by one
# listing and one reap, plus a confirmation.
CANCEL_WAIT_SECONDS = 2 * (STOP_GRACE_SECONDS + CONFIRM_SECONDS + LISTING_SECONDS + REAP_SECONDS) + CONFIRM_SECONDS

# The exit status of a process that ends with an unconfirmed stop on record (EX_SOFTWARE); the record is printed to standard error as well.
UNCONFIRMED_EXIT_STATUS = 70

P = ParamSpec("P")
T = TypeVar("T")


class ProcessGroupSurvived(TimeoutExpired):
    """The deadline passed and a member of the program's session or process group was still alive after KILL, or the session could not be listed.

    It is a ``TimeoutExpired``, so callers that catch the timeout see it too. ``process_group`` is the leader's pid: the id of its group and session.
    """

    def __init__(
        self, cmd: Any, timeout: float, process_group: int, output: str | bytes | None = None, stderr: str | bytes | None = None, detail: str = ""
    ) -> None:
        super().__init__(cmd, timeout, output, stderr)
        self.process_group = process_group
        self.detail = detail

    def __str__(self) -> str:
        return (
            f"{super().__str__()}; the session or process group {self.process_group} still had members after KILL or could not be listed, "
            f"so the stop is not confirmed{f' ({self.detail})' if self.detail else ''}"
        )


class LaunchCancelled(RuntimeError):
    """The pool a job belongs to was interrupted: the job's program (if it started) has been stopped and no new program starts."""


class LaunchesNotStopped(RuntimeError):
    """A stop was not confirmed after an interrupt or an error, or a pool was left with an unconfirmed stop or a job that did not end in time.

    ``survivors`` holds the pids that were still alive when they are known (the ids of the sessions or groups otherwise).
    """

    def __init__(self, message: str, survivors: Collection[int] = ()) -> None:
        super().__init__(message)
        self.survivors = frozenset(survivors)


class SessionListingFailed(OSError):
    """The live processes could not be listed or their sessions could not be read, so a session stop cannot be confirmed."""


@dataclass(frozen=True)
class StopOutcome:
    """What a stop found: confirmed (no member left), or the survivors. Truthy when the stop is NOT confirmed, as the former bool result was.

    ``survivors`` are the pids still alive after KILL when known (the id of the session or group otherwise); ``reason`` says why the stop is unconfirmed.
    """

    unconfirmed: bool = False
    survivors: frozenset[int] = frozenset()
    reason: str = ""

    @property
    def confirmed(self) -> bool:
        return not self.unconfirmed

    def __bool__(self) -> bool:
        return self.unconfirmed

    def describe(self) -> str:
        found = f"alive: {', '.join(str(pid) for pid in sorted(self.survivors))}" if self.survivors else ""
        return "; ".join(part for part in (self.reason, found) if part)


def as_outcome(result: object, session: int) -> StopOutcome:
    """Read a stop's result: a ``StopOutcome``, or a bool of the former kind (True: not confirmed)."""
    if isinstance(result, StopOutcome):
        return result
    return StopOutcome(True, frozenset({session}), "the stop reported that a member survived") if result else StopOutcome()


# The unconfirmed stops of this process as (pid of the process, text); kept in memory only. A forked child inherits the list but reports only its own.
UNCONFIRMED: list[tuple[int, str]] = []
UNCONFIRMED_LOCK = threading.Lock()


def note_unconfirmed(text: str) -> None:
    """Keep an unconfirmed stop for the process (see ``take_unconfirmed``) and print it to standard error at once."""
    with UNCONFIRMED_LOCK:
        UNCONFIRMED.append((os.getpid(), text))
    with contextlib.suppress(OSError):
        os.write(2, f"process launcher: {text}\n".encode(errors="replace"))


def program_name(command: Any) -> str:
    """The name of the program in a command (the list of a launch, or its text), for messages."""
    return os.path.basename(str(command[0])) if isinstance(command, Sequence) and command and not isinstance(command, str) else str(command)


def record_unconfirmed(command: Any, session: int, outcome: StopOutcome, cancellation: Cancellation | None = None) -> str:
    """Record an unconfirmed stop of a launch: for the process, and for the pool whose job made it. Returns the text."""
    program = program_name(command)
    text = f"the stop of {program} (session or process group {session}) is not confirmed ({outcome.describe()})"
    if cancellation is not None:
        cancellation.unconfirmed.append((session, text))
    note_unconfirmed(text)
    return text


def take_unconfirmed() -> list[str]:
    """Return this process's unconfirmed stops and clear the record: the caller reports them itself, so the exit status is not changed."""
    with UNCONFIRMED_LOCK:
        mine = [text for pid, text in UNCONFIRMED if pid == os.getpid()]
        UNCONFIRMED.clear()
    return mine


def exit_with_unconfirmed() -> None:
    """At interpreter exit: a process that still has an unconfirmed stop on record ends with ``UNCONFIRMED_EXIT_STATUS``, whatever the caller did."""
    with UNCONFIRMED_LOCK:
        mine = [text for pid, text in UNCONFIRMED if pid == os.getpid()]
    if not mine:
        return
    for stream in (sys.stdout, sys.stderr):
        with contextlib.suppress(OSError, ValueError):
            stream.flush()
    with contextlib.suppress(OSError):
        os.write(2, f"process launcher: exiting with status {UNCONFIRMED_EXIT_STATUS}: {len(mine)} stop(s) were not confirmed\n".encode())
    os._exit(UNCONFIRMED_EXIT_STATUS)


atexit.register(exit_with_unconfirmed)


def named_failure(outcome: StopOutcome, command: Any, cause: BaseException) -> BaseException:
    """The exception for an unconfirmed stop after ``cause`` (an interrupt or an error): it names the stop and keeps the survivors."""
    program = program_name(command)
    return LaunchesNotStopped(
        f"the stop of {program} is not confirmed after {type(cause).__name__} ({outcome.describe()})",
        outcome.survivors,
    )


def caller_group() -> int:
    """The process group of the caller: the fallback of a failed session listing never signals it (the caller is the one that must report)."""
    return os.getpgrp()


def group_alive(process_group: int) -> bool:
    """Return whether any process, a zombie included, is still in the process group."""
    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def close_pipes(process: subprocess.Popen[str]) -> None:
    """Close the pipes of a stopped program without waiting for it: a context manager would wait for a leader that survived KILL."""
    for pipe in (process.stdout, process.stderr):
        if pipe is not None:
            pipe.close()


def start(argv: Sequence[str], cwd: Path | None, env: Mapping[str, str] | None, new_session: bool) -> subprocess.Popen[str]:
    """Start a resolved program with closed stdin and captured text output, in a new session or in a new process group."""
    return subprocess.Popen(  # noqa: S603 - the only process launcher: full path, list arguments, no shell, stdin closed, own session or group
        list(argv),
        cwd=cwd,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=new_session,
        process_group=None if new_session else 0,
    )


def end_lister(lister: subprocess.Popen[str]) -> None:
    """Stop a lister that is not wanted any more. It leads its own group: KILL reaches a process it started that holds its pipes, and the reap is bounded."""
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(lister.pid, signal.SIGKILL)
    close_pipes(lister)
    with contextlib.suppress(TimeoutExpired):
        lister.wait(timeout=REAP_SECONDS)


def list_processes() -> list[tuple[int, int, str]]:
    """Every live process as (pid, parent pid, state), from ``SESSION_LISTING``. Raises ``SessionListingFailed`` on any failure."""
    executable = find_program(SESSION_LISTING[0])
    if executable is None:
        raise SessionListingFailed(f"{SESSION_LISTING[0]} is not on PATH")
    try:
        lister = start([executable, *SESSION_LISTING[1:]], None, None, new_session=False)
    except OSError as failure:
        raise SessionListingFailed(f"{executable} could not start: {failure}") from None
    try:
        output, error = lister.communicate(timeout=LISTING_SECONDS)
    except TimeoutExpired:
        end_lister(lister)
        raise SessionListingFailed(f"{executable} did not finish within {LISTING_SECONDS} s") from None
    except BaseException:
        # An alarm or an interrupt ends the listing: the lister and the process that holds its pipes must not outlive it.
        end_lister(lister)
        raise
    if lister.returncode:
        raise SessionListingFailed(f"{executable} exited {lister.returncode}: {error.strip()}")
    rows: list[tuple[int, int, str]] = []
    for line in output.splitlines():
        fields = line.split()
        if not fields:
            continue
        if len(fields) < 3 or not fields[0].isdigit() or not fields[1].isdigit():
            raise SessionListingFailed(f"{executable} printed a line that is not a pid, a parent pid and a state: {line!r}")
        rows.append((int(fields[0]), int(fields[1]), fields[2]))
    return rows


def session_members(sessions: Collection[int], parent: int | None) -> set[int]:
    """The live members of the sessions, without zombies and without the caller.

    A session's leader counts while it is ``parent``'s child even before it has made its session (a worker whose ``setsid`` has not run yet). ``parent``
    is None when the leader has ended and been reaped: its pid is then only a session id, and a new child with that pid is not a member.
    """
    members: set[int] = set()
    own = os.getpid()
    for pid, parent_pid, state in list_processes():
        if pid == own or state.startswith("Z"):
            continue
        if parent is not None and pid in sessions and parent_pid == parent:
            members.add(pid)
            continue
        try:
            if os.getsid(pid) in sessions:
                members.add(pid)
        except ProcessLookupError:
            continue
        except PermissionError as error:
            raise SessionListingFailed(f"the session of process {pid} cannot be read: {error}") from None
    return members


def still_member(pid: int, sessions: Collection[int]) -> bool:
    """Read the session of a listed member again just before its signal: a member that has left the sessions (``setsid``) or ended is skipped.

    A leader that has not made its session yet (``pid`` is a session id) stays a member. A pid that ends and is reused between this check and the
    signal is the remaining race (microseconds).
    """
    if pid in sessions:
        return True
    try:
        return os.getsid(pid) in sessions
    except ProcessLookupError:
        return False
    except PermissionError as error:
        raise SessionListingFailed(f"the session of process {pid} cannot be read: {error}") from None


def session_left(members: set[int]) -> bool:
    """Return whether a listed session still has a member."""
    return bool(members)


def stop_process_group(process_group: int, leader: subprocess.Popen[str] | None = None, grace_seconds: float | None = None) -> StopOutcome:
    """Stop every member of a process group: TERM, then KILL after the grace period, and confirm none is left.

    ``leader`` is the child that started the group. It is reaped here, because an unreaped zombie keeps the group alive.
    ``grace_seconds`` is the time between TERM and KILL; None means ``STOP_GRACE_SECONDS``. Returns the outcome: unconfirmed when a member survived KILL
    for ``CONFIRM_SECONDS`` (``survivors`` then holds the group id).
    """
    grace = STOP_GRACE_SECONDS if grace_seconds is None else grace_seconds
    for signal_number, wait_seconds in ((signal.SIGTERM, grace), (signal.SIGKILL, CONFIRM_SECONDS)):
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process_group, signal_number)
        end = time.monotonic() + wait_seconds
        while True:
            if leader is not None:
                leader.poll()
            if not group_alive(process_group):
                return StopOutcome()
            if time.monotonic() >= end:
                break
            time.sleep(POLL_SECONDS)
    return StopOutcome(True, frozenset({process_group}), f"the process group {process_group} still had members after KILL")


def stop_sessions(
    sessions: Collection[int],
    grace_seconds: float | None = None,
    parent: int | None = None,
    leader: subprocess.Popen[str] | None = None,
    allow_own: bool = False,
) -> StopOutcome:
    """Stop every member of the sessions: TERM, then KILL after the grace period, and confirm none is left.

    ``sessions`` are session ids (each its leader's pid); ``parent`` is the leaders' parent, the caller when None. The sessions are listed again on
    every poll, so a member started during the stop is signalled too. Returns the outcome, unconfirmed (and truthy) when a member survived KILL for
    ``CONFIRM_SECONDS`` (``survivors`` holds those pids) or when the sessions could not be listed (``reason`` says why); then each leader's process group
    (its id is the session id) still gets TERM and KILL and the stop waits up to ``CONFIRM_SECONDS`` for those groups to end (``leader`` is reaped there
    only, because a zombie keeps its group alive).
    Refuses (``ValueError``) the caller's own session unless ``allow_own`` is set (a CI worker's leader that stops its own session when the dispatcher
    has died; the caller is never signalled, by the listing stop or by the fallback group stop, which skips the caller's own group and reports instead).
    """
    targets = set(sessions)
    if os.getsid(0) in targets and not allow_own:
        raise ValueError(f"refusing to stop session {os.getsid(0)}: it is the caller's own session")
    grace = STOP_GRACE_SECONDS if grace_seconds is None else grace_seconds
    owner = None if leader is not None and leader.returncode is not None else (os.getpid() if parent is None else parent)
    members: set[int] = set()
    try:
        for signal_number, wait_seconds in ((signal.SIGTERM, grace), (signal.SIGKILL, CONFIRM_SECONDS)):
            signalled: set[int] = set()
            end = time.monotonic() + wait_seconds
            while True:
                members = session_members(targets, owner)
                for pid in members - signalled:
                    if not still_member(pid, targets):
                        continue
                    with contextlib.suppress(ProcessLookupError):
                        os.kill(pid, signal_number)
                signalled |= members
                if not session_left(members):
                    return StopOutcome()
                if time.monotonic() >= end:
                    break
                time.sleep(POLL_SECONDS)
    except SessionListingFailed as listing_failure:
        reason = f"the sessions could not be listed: {listing_failure}"
        # The failure is on record and on standard error before any signal: the fallback may be the last thing this process does.
        note_unconfirmed(f"{reason}; the leaders' groups of {sorted(targets)} are stopped without a confirmation of the sessions")
        own_group = caller_group()
        groups = [session for session in targets if session != own_group]
        for signal_number in (signal.SIGTERM, signal.SIGKILL):
            for session in groups:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(session, signal_number)
        end = time.monotonic() + CONFIRM_SECONDS
        while any(group_alive(session) for session in groups) and time.monotonic() < end:
            if leader is not None:
                leader.poll()
            time.sleep(POLL_SECONDS)
        left = frozenset(session for session in targets if session == own_group or group_alive(session))
        if own_group in targets:
            reason += f"; the caller's own group {own_group} was not signalled and its members are not confirmed stopped"
        return StopOutcome(True, left or frozenset(targets), reason)
    return StopOutcome(True, frozenset(members), "members of the sessions were still alive after KILL")


def stop_launch(leader_pid: int, leader: subprocess.Popen[str] | None = None, grace_seconds: float | None = None, session: bool = False) -> StopOutcome:
    """Stop what a launch started: its session (a top-level launch) or its process group (a nested launch). Truthy when not confirmed.

    The leader of a session is reaped only after the stop, so its pid (the session id) stays reserved while the stop runs.
    """
    if not session:
        return stop_process_group(leader_pid, leader, grace_seconds)
    outcome = stop_sessions({leader_pid}, grace_seconds, leader=leader)
    if leader is not None:
        leader.poll()
    return outcome


@contextlib.contextmanager
def interrupts_deferred() -> Iterator[None]:
    """Hold SIGINT while a launch is created or stopped, and deliver it once that work is done, so a second Ctrl-C cannot cut a stop short.

    Only the main thread receives signals in Python, so in another thread this does nothing.
    """
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    received: list[int] = []
    previous = signal.signal(signal.SIGINT, lambda number, _frame: received.append(number))
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, signal.SIG_DFL if previous is None else previous)
        if received:
            signal.raise_signal(signal.SIGINT)


class Cancellation:
    """The cancellation of one ``LaunchPool``: set once by the pool's first interrupt; ``unconfirmed`` lists the launches of its jobs whose stop was not
    confirmed, on every path (a deadline, a cancellation, an error), as (session or group, text)."""

    def __init__(self) -> None:
        self.event = threading.Event()
        self.unconfirmed: list[tuple[int, str]] = []

    def cancelled(self) -> bool:
        return self.event.is_set()


# The cancellation of the pool whose job runs in this thread; absent in every other thread.
JOB = threading.local()


def stop_recorded(
    process: subprocess.Popen[str], grace_seconds: float | None, nested: bool, cancellation: Cancellation | None
) -> tuple[StopOutcome, BaseException | None]:
    """Stop what a launch started, close its pipes and record an unconfirmed outcome (for the process and for the pool of the job), on every path.

    SIGINT is held meanwhile; one that arrived is returned (not raised), so the caller can name the failure and chain it. A stop that raises anything,
    ``SystemExit`` of an alarm handler included, counts as unconfirmed, with the error as its reason; it is returned as well, and the leader's group
    gets a last KILL because the stop did not finish: the call must never end with only the exception of the stop, or with none.
    """
    late: BaseException | None = None
    outcome = StopOutcome()
    try:
        with interrupts_deferred():
            try:
                outcome = as_outcome(stop_launch(process.pid, process, grace_seconds, session=not nested), process.pid)
            except BaseException as failure:
                late = failure
                outcome = StopOutcome(True, frozenset({process.pid}), f"the stop failed with {type(failure).__name__}: {failure}")
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(process.pid, signal.SIGKILL)
                with contextlib.suppress(TimeoutExpired):
                    process.wait(timeout=REAP_SECONDS)
            close_pipes(process)
            if outcome.unconfirmed:
                record_unconfirmed(process.args, process.pid, outcome, cancellation)
    except KeyboardInterrupt as interrupt:
        late = late or interrupt
    return outcome, late


def finalize_session(
    process: subprocess.Popen[str], grace_seconds: float | None, cancellation: Cancellation | None
) -> tuple[StopOutcome, BaseException | None]:
    """Stop what a top-level program left in its session after it ended normally, and record a stop that is not confirmed.

    The leader has ended and been reaped, so its pid is only the session id. The usual case is an empty session: one listing, no signal.
    """
    return stop_recorded(process, grace_seconds, False, cancellation)


def communicate(process: subprocess.Popen[str], timeout: float, cancellation: Cancellation | None) -> tuple[str, str]:
    """Wait for the program's output. A pool job waits in slices and raises ``LaunchCancelled`` as soon as its pool is cancelled."""
    if cancellation is None:
        return process.communicate(timeout=timeout)
    end = time.monotonic() + timeout
    while True:
        if cancellation.cancelled():
            raise LaunchCancelled(f"{process.args!r} was cancelled by an interrupt")
        remaining = end - time.monotonic()
        try:
            return process.communicate(timeout=min(SLICE_SECONDS, max(remaining, 0.0)))
        except TimeoutExpired as expired:
            if remaining <= SLICE_SECONDS:
                raise TimeoutExpired(process.args, timeout, expired.stdout, expired.stderr) from None


class LaunchPool(ThreadPoolExecutor):
    """A thread pool for jobs that start programs through ``run_command``, which stops their sessions when the caller is interrupted.

    Use it as ``with LaunchPool(max_workers=n) as pool:`` in the main thread instead of ``ThreadPoolExecutor``. Inside the block the first SIGINT sets
    the pool's cancellation and raises ``KeyboardInterrupt``; later ones are held until the pool is left. Leaving the block after an interrupt drops the
    queued jobs, waits for every running job (each stops its own session within its ``run_command``) and only then forwards the interrupt. A job that
    ends in a stop that is not confirmed, or a job that does not end within ``CANCEL_WAIT_SECONDS``, raises ``LaunchesNotStopped`` instead, chained
    from the interrupt. In another thread no handler is installed and the pool behaves as a plain one. SIGTERM is not handled.
    """

    def __init__(self, max_workers: int | None = None) -> None:
        super().__init__(max_workers=max_workers)
        self.cancellation = Cancellation()
        self.jobs: list[Future[Any]] = []
        self.closed = False
        self.interrupts = 0
        self.holding = False
        self.previous: Any = None

    def __enter__(self) -> Self:
        super().__enter__()
        if threading.current_thread() is threading.main_thread():
            self.previous = signal.signal(signal.SIGINT, self.interrupted)
            self.holding = True
        return self

    def __exit__(self, _exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None) -> None:
        interrupt = exc_value if isinstance(exc_value, KeyboardInterrupt) else None
        problems: list[str] = []
        try:
            if interrupt is None:
                try:
                    if not self.closed:
                        self.shutdown(wait=True)
                except KeyboardInterrupt as error:
                    interrupt = error
            # A job that ended is no proof that its program was stopped: the stops that were not confirmed count, with or without an interrupt.
            problems = self.unconfirmed_problems()
            if interrupt is not None or self.cancellation.cancelled():
                problems = self.stop_jobs()
        finally:
            self.release_interrupts()
        if problems:
            raise LaunchesNotStopped("; ".join(problems), [pid for pid, _text in self.cancellation.unconfirmed]) from (interrupt or exc_value)
        if interrupt is not None and interrupt is not exc_value:
            raise interrupt

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        self.closed = True
        super().shutdown(wait=wait, cancel_futures=cancel_futures)

    def submit(self, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        future = super().submit(self.run_job, fn, args, kwargs)
        self.jobs.append(future)
        return future

    def run_job(self, function: Callable[..., T], arguments: tuple[Any, ...], keywords: dict[str, Any]) -> T:
        """Run a job in a pool thread with the pool's cancellation visible to its ``run_command`` calls; a cancelled pool starts no job."""
        if self.cancellation.cancelled():
            raise LaunchCancelled("the pool was cancelled by an interrupt before this job started")
        JOB.cancellation = self.cancellation
        try:
            return function(*arguments, **keywords)
        finally:
            del JOB.cancellation

    def interrupted(self, _number: int, _frame: FrameType | None) -> None:
        """The pool's SIGINT handler: the first interrupt cancels the pool and reaches the caller; the others are held."""
        self.interrupts += 1
        if self.interrupts == 1:
            self.cancellation.event.set()
            raise KeyboardInterrupt
        if self.interrupts == 2:
            os.write(2, b"interrupt: the running programs are being stopped; further interrupts are held until they are gone\n")

    def release_interrupts(self) -> None:
        if self.holding:
            self.holding = False
            signal.signal(signal.SIGINT, signal.SIG_DFL if self.previous is None else self.previous)

    def stop_jobs(self) -> list[str]:
        """Cancel the pool and wait for its jobs. Returns what is wrong: stops that were not confirmed and jobs that did not end."""
        self.cancellation.event.set()
        self.shutdown(wait=False, cancel_futures=True)
        problems = self.wait_for_jobs()
        problems.extend(self.unconfirmed_problems())
        return problems

    def unconfirmed_problems(self) -> list[str]:
        """The stops of this pool's jobs that were not confirmed, as text (empty when every stop was confirmed)."""
        return [text for _pid, text in self.cancellation.unconfirmed]

    def live_jobs(self) -> list[Future[Any]]:
        """The jobs that can still run. A queued job that ``shutdown(cancel_futures=True)`` cancelled never runs, and ``wait`` does not count it as done."""
        return [job for job in self.jobs if not job.cancelled()]

    def wait_for_jobs(self) -> list[str]:
        """Wait up to ``CANCEL_WAIT_SECONDS`` for every job that can run; return a description of the jobs still running (also kept for the process)."""
        _done, running = wait(self.live_jobs(), timeout=CANCEL_WAIT_SECONDS)
        if not running:
            return []
        text = f"{len(running)} job(s) were still running {CANCEL_WAIT_SECONDS:g} s after the interrupt, so their stops are not confirmed"
        note_unconfirmed(text)
        return [text]


def clear_git_environment() -> None:
    """Detach a synthetic test runner from an invoking Git hook before it operates on scratch repositories.

    Only test entry points call this; production checks keep the hook's staged-index context. It changes this process
    and its future children only, never the parent process or machine configuration.
    """
    variables = run_command("git", ["rev-parse", "--local-env-vars"], check=True).stdout.splitlines()
    variables.extend(f"GIT_{role}_{field}" for role in ("AUTHOR", "COMMITTER") for field in ("NAME", "EMAIL", "DATE"))
    variables.extend(("PRE_COMMIT_FROM_REF", "PRE_COMMIT_TO_REF"))
    for variable in variables:
        os.environ.pop(variable, None)


def find_program(program: str, env: Mapping[str, str] | None = None) -> str | None:
    """Return the full path of ``program`` on the child's PATH, or None when it is not installed."""
    return shutil.which(program, path=(os.environ if env is None else env).get("PATH"))


def run_command(
    program: str,
    args: Sequence[str],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    check: bool = False,
    grace_seconds: float | None = None,
) -> CompletedProcess[str]:
    """Run ``program`` with ``args`` and return its exit code and captured text output.

    Parameters
    ----------
    program : str
        Program name or path, resolved to a full path on the child's PATH.
    args : Sequence[str]
        Arguments passed as a list; no shell is involved.
    cwd : Path, optional
        Working directory for the program.
    env : Mapping[str, str], optional
        Complete environment for the program; the caller's environment when omitted.
    timeout : float
        Seconds before the program is stopped.
    check : bool
        Raise when the program exits non-zero.
    grace_seconds : float, optional
        Seconds between TERM and KILL when the group is stopped; ``STOP_GRACE_SECONDS`` when omitted. A caller whose own deadline must hold
        within a bound shorter than that passes a smaller value.

    Returns
    -------
    CompletedProcess[str]
        Exit code, stdout and stderr.

    Raises
    ------
    FileNotFoundError
        ``program`` is not on PATH.
    subprocess.CalledProcessError
        ``check`` is true and the program exits non-zero.
    TimeoutExpired
        The program ran longer than ``timeout`` seconds.
    """
    executable = find_program(program, env)
    if executable is None:
        raise FileNotFoundError(f"{program} is not on PATH")
    nested = os.environ.get(SESSION_VARIABLE) == "1"
    child_environment = dict(os.environ if env is None else env)
    child_environment[SESSION_VARIABLE] = "1"
    # SIGINT is held while the program starts and while it is stopped; one that arrived then is raised once the stop is confirmed.
    cancellation: Cancellation | None = getattr(JOB, "cancellation", None)
    if cancellation is not None and cancellation.cancelled():
        raise LaunchCancelled(f"{program} was not started: the pool was cancelled by an interrupt")
    process: subprocess.Popen[str] | None = None
    try:
        with interrupts_deferred():
            process = start([executable, *args], cwd, child_environment, new_session=not nested)
        stdout, stderr = communicate(process, timeout, cancellation)
    except LaunchCancelled:
        if process is None or cancellation is None:
            raise
        outcome, late = stop_recorded(process, grace_seconds, nested, cancellation)
        if outcome.unconfirmed:
            raise ProcessGroupSurvived(process.args, timeout, process.pid, detail=outcome.describe()) from late
        if late is not None:
            raise late from None
        raise
    except TimeoutExpired as expired:
        if process is None:
            raise
        outcome, late = stop_recorded(process, grace_seconds, nested, cancellation)
        if outcome.unconfirmed:
            raise ProcessGroupSurvived(process.args, timeout, process.pid, expired.stdout, expired.stderr, detail=outcome.describe()) from late
        if late is not None:
            raise late from expired
        raise
    except BaseException as failure:
        if process is not None:
            outcome, late = stop_recorded(process, grace_seconds, nested, cancellation)
            if outcome.unconfirmed:
                raise named_failure(outcome, process.args, failure) from failure
            if late is not None:
                raise late from failure
        raise
    if not nested:
        # A normal end is no proof that the session is empty: a member that the program left (a child of a command that exited at its own deadline) is
        # stopped here, and one that survives is named, recorded and never a success.
        outcome, late = finalize_session(process, grace_seconds, cancellation)
        if outcome.unconfirmed:
            program = program_name(process.args)
            raise LaunchesNotStopped(
                f"{program} ended, but the stop of the members it left in its session is not confirmed ({outcome.describe()})", outcome.survivors
            ) from late
        if late is not None:
            raise late
    if check and process.returncode:
        raise CalledProcessError(process.returncode, process.args, output=stdout, stderr=stderr)
    return CompletedProcess(process.args, process.returncode, stdout, stderr)
