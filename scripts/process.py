"""The one place this repository's scripts start another program.

Every call resolves the program to a full path, passes a list of arguments (never a shell string), closes stdin and
applies a timeout. A top-level call starts the program in a new session (``setsid``), which it leads; the call names it to its descendants with
``PROCESS_LAUNCH_SESSION=1``. At the deadline, or when the caller is interrupted, every member of that session stops: the call lists the live
processes (``ps -A -o pid=,ppid=,stat=``; macOS ``pgrep`` has no session option and its ``ps`` shows the session as 0) and signals those whose
``os.getsid`` is the session: TERM, then KILL after a grace period, then a check that no member is left. A call made inside such a session (a nested
launch, whose environment carries the variable) stays in that session: it starts its program in a new process group and stops only that group at its
own deadline; the outer stop reaches it as a session member. A member that starts its own session (``setsid``) without this launcher is outside the
session and is not stopped. The leader is reaped only after the stop, so its pid, which is the session id, cannot be reused while the stop runs. If
the session cannot be listed, the stop still signals the leader's group and the call fails as if a member survived. Other scripts import
``run_command`` from here instead of ``subprocess``, so the lint exception for starting a process exists on one line. Run scripts that import it as
modules from the repository root, for example ``.venv/bin/python -m scripts.quality.scan_secrets``.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import signal
import subprocess
import time
from collections.abc import Collection, Mapping, Sequence
from pathlib import Path
from subprocess import CalledProcessError, CompletedProcess, SubprocessError, TimeoutExpired
from typing import Any

__all__ = [
    "CompletedProcess",
    "ProcessGroupSurvived",
    "SessionListingFailed",
    "SubprocessError",
    "TimeoutExpired",
    "clear_git_environment",
    "find_program",
    "run_command",
    "stop_launch",
    "stop_process_group",
    "stop_sessions",
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


class ProcessGroupSurvived(TimeoutExpired):
    """The deadline passed and a member of the program's session or process group was still alive after KILL, or the session could not be listed.

    It is a ``TimeoutExpired``, so callers that catch the timeout see it too. ``process_group`` is the leader's pid: the id of its group and session.
    """

    def __init__(self, cmd: Any, timeout: float, process_group: int, output: str | bytes | None = None, stderr: str | bytes | None = None) -> None:
        super().__init__(cmd, timeout, output, stderr)
        self.process_group = process_group

    def __str__(self) -> str:
        return (
            f"{super().__str__()}; the session or process group {self.process_group} still had members after KILL or could not be listed, "
            "so the stop is not confirmed"
        )


class SessionListingFailed(OSError):
    """The live processes could not be listed or their sessions could not be read, so a session stop cannot be confirmed."""


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


def list_processes() -> list[tuple[int, int, str]]:
    """Every live process as (pid, parent pid, state), from ``SESSION_LISTING``. Raises ``SessionListingFailed`` on any failure."""
    executable = find_program(SESSION_LISTING[0])
    if executable is None:
        raise SessionListingFailed(f"{SESSION_LISTING[0]} is not on PATH")
    lister = start([executable, *SESSION_LISTING[1:]], None, None, new_session=False)
    try:
        output, error = lister.communicate(timeout=LISTING_SECONDS)
    except TimeoutExpired:
        lister.kill()
        lister.communicate()
        raise SessionListingFailed(f"{executable} did not finish within {LISTING_SECONDS} s") from None
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


def session_members(sessions: Collection[int], parent: int) -> set[int]:
    """The live members of the sessions, without zombies and without the caller.

    A session's leader counts while it is ``parent``'s child even before it has made its session (a worker whose ``setsid`` has not run yet).
    """
    members: set[int] = set()
    own = os.getpid()
    for pid, parent_pid, state in list_processes():
        if pid == own or state.startswith("Z"):
            continue
        if pid in sessions and parent_pid == parent:
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


def session_left(members: set[int]) -> bool:
    """Return whether a listed session still has a member."""
    return bool(members)


def stop_process_group(process_group: int, leader: subprocess.Popen[str] | None = None, grace_seconds: float | None = None) -> bool:
    """Stop every member of a process group: TERM, then KILL after the grace period, and confirm none is left.

    ``leader`` is the child that started the group. It is reaped here, because an unreaped zombie keeps the group alive.
    ``grace_seconds`` is the time between TERM and KILL; None means ``STOP_GRACE_SECONDS``. Returns True when a member survived KILL for ``CONFIRM_SECONDS``.
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
                return False
            if time.monotonic() >= end:
                break
            time.sleep(POLL_SECONDS)
    return True


def stop_sessions(
    sessions: Collection[int], grace_seconds: float | None = None, parent: int | None = None, leader: subprocess.Popen[str] | None = None
) -> bool:
    """Stop every member of the sessions: TERM, then KILL after the grace period, and confirm none is left.

    ``sessions`` are session ids (each its leader's pid); ``parent`` is the leaders' parent, the caller when None. The sessions are listed again on
    every poll, so a member started during the stop is signalled too. Returns True when a member survived KILL for ``CONFIRM_SECONDS`` or when the
    sessions could not be listed; then each leader's process group (its id is the session id) still gets TERM and KILL and the stop waits up to
    ``CONFIRM_SECONDS`` for those groups to end (``leader`` is reaped there only, because a zombie keeps its group alive).
    Refuses (``ValueError``) the caller's own session.
    """
    targets = set(sessions)
    if os.getsid(0) in targets:
        raise ValueError(f"refusing to stop session {os.getsid(0)}: it is the caller's own session")
    grace = STOP_GRACE_SECONDS if grace_seconds is None else grace_seconds
    owner = os.getpid() if parent is None else parent
    try:
        for signal_number, wait_seconds in ((signal.SIGTERM, grace), (signal.SIGKILL, CONFIRM_SECONDS)):
            signalled: set[int] = set()
            end = time.monotonic() + wait_seconds
            while True:
                members = session_members(targets, owner)
                for pid in members - signalled:
                    with contextlib.suppress(ProcessLookupError):
                        os.kill(pid, signal_number)
                signalled |= members
                if not session_left(members):
                    return False
                if time.monotonic() >= end:
                    break
                time.sleep(POLL_SECONDS)
    except SessionListingFailed:
        for signal_number in (signal.SIGTERM, signal.SIGKILL):
            for session in targets:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(session, signal_number)
        end = time.monotonic() + CONFIRM_SECONDS
        while any(group_alive(session) for session in targets) and time.monotonic() < end:
            if leader is not None:
                leader.poll()
            time.sleep(POLL_SECONDS)
        return True
    return True


def stop_launch(leader_pid: int, leader: subprocess.Popen[str] | None = None, grace_seconds: float | None = None, session: bool = False) -> bool:
    """Stop what a launch started: its session (a top-level launch) or its process group (a nested launch). True when the stop is not confirmed.

    The leader of a session is reaped only after the stop, so its pid (the session id) stays reserved while the stop runs.
    """
    if not session:
        return stop_process_group(leader_pid, leader, grace_seconds)
    survived = stop_sessions({leader_pid}, grace_seconds, leader=leader)
    if leader is not None:
        leader.poll()
    return survived


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
    process = start([executable, *args], cwd, child_environment, new_session=not nested)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except TimeoutExpired as expired:
        survived = stop_launch(process.pid, process, grace_seconds, session=not nested)
        close_pipes(process)
        if survived:
            raise ProcessGroupSurvived(process.args, timeout, process.pid, expired.stdout, expired.stderr) from None
        raise
    except BaseException:
        stop_launch(process.pid, process, grace_seconds, session=not nested)
        close_pipes(process)
        raise
    if check and process.returncode:
        raise CalledProcessError(process.returncode, process.args, output=stdout, stderr=stderr)
    return CompletedProcess(process.args, process.returncode, stdout, stderr)
