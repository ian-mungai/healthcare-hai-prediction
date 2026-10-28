"""The one place this repository's scripts start another program.

Every call resolves the program to a full path, passes a list of arguments (never a shell string), closes stdin and
applies a timeout. The program starts in its own process group. At the deadline, or when the caller is interrupted, the whole group
stops: TERM, then KILL after a grace period, then a check that no member is left. A member that starts its own session or group
(``setsid``) is outside the group and is not stopped. Other scripts import ``run_command`` from here instead of ``subprocess``, so the lint exception for
starting a process exists on one line. Run scripts that import it as modules from the repository root, for example
``.venv/bin/python -m scripts.quality.scan_secrets``.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import signal
import subprocess
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from subprocess import CalledProcessError, CompletedProcess, SubprocessError, TimeoutExpired
from typing import Any

__all__ = [
    "CompletedProcess",
    "ProcessGroupSurvived",
    "SubprocessError",
    "TimeoutExpired",
    "clear_git_environment",
    "find_program",
    "run_command",
    "stop_process_group",
]

DEFAULT_TIMEOUT_SECONDS = 300
# Seconds a process group gets to end after TERM, and to disappear after KILL, before the stop is reported as failed.
STOP_GRACE_SECONDS = 5.0
CONFIRM_SECONDS = 5.0
POLL_SECONDS = 0.05


class ProcessGroupSurvived(TimeoutExpired):
    """The deadline passed and a member of the program's process group was still alive after KILL.

    It is a ``TimeoutExpired``, so callers that catch the timeout see it too. ``process_group`` is the group id.
    """

    def __init__(self, cmd: Any, timeout: float, process_group: int, output: str | bytes | None = None, stderr: str | bytes | None = None) -> None:
        super().__init__(cmd, timeout, output, stderr)
        self.process_group = process_group

    def __str__(self) -> str:
        return f"{super().__str__()}; process group {self.process_group} still had members after KILL"


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


def stop_process_group(process_group: int, leader: subprocess.Popen[str] | None = None, grace_seconds: float | None = None) -> bool:
    """Stop every member of a process group: TERM, then KILL after the grace period, and confirm none is left.

    ``leader`` is the child that started the group. It is reaped here, because an unreaped zombie keeps the group alive.
    ``grace_seconds`` is the time between TERM and KILL; None means ``STOP_GRACE_SECONDS``.
    Returns True when a member survived KILL for ``CONFIRM_SECONDS``.
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
    process = subprocess.Popen(  # noqa: S603 - the only process launcher: full path, list arguments, no shell, stdin closed, own process group
        [executable, *args],
        cwd=cwd,
        env=None if env is None else dict(env),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        process_group=0,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except TimeoutExpired as expired:
        survived = stop_process_group(process.pid, process, grace_seconds)
        close_pipes(process)
        if survived:
            raise ProcessGroupSurvived(process.args, timeout, process.pid, expired.stdout, expired.stderr) from None
        raise
    except BaseException:
        stop_process_group(process.pid, process, grace_seconds)
        close_pipes(process)
        raise
    if check and process.returncode:
        raise CalledProcessError(process.returncode, process.args, output=stdout, stderr=stderr)
    return CompletedProcess(process.args, process.returncode, stdout, stderr)
