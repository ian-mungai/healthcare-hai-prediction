"""The one place this repository's scripts start another program.

Every call resolves the program to a full path, passes a list of arguments (never a shell string), closes stdin and
applies a timeout. The program starts in its own process group. At the deadline, or when the caller is interrupted, the whole group
stops: TERM, then KILL after a grace period, then a check that no member is left. A descendant that starts a program through this launcher (a
nested launch) puts that program in a new group, so each call keeps a private registry of owned groups: a file with mode 600 in a folder with mode 700,
named to its child in ``PROCESS_GROUP_REGISTRIES`` together with the registries of its own ancestors (paths separated by ``:``). Every launch appends its
new group, one line, to each registry it inherited, so a registry lists every group that any descendant started. The stop covers the own group and every
recorded group, and the registry is removed when the call returns or raises. A nested registry folder is made inside its parent's, so a nested launcher
killed before its own cleanup leaves nothing behind. A member that starts its own session or group (``setsid``) without this launcher is outside every
group and is not stopped. Other scripts import ``run_command`` from here instead of ``subprocess``, so the lint exception for
starting a process exists on one line. Run scripts that import it as modules from the repository root, for example
``.venv/bin/python -m scripts.quality.scan_secrets``.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import tempfile
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
# The environment variable that names the registries of owned process groups (the caller's ancestors' and the caller's own), separated by ":".
REGISTRY_VARIABLE = "PROCESS_GROUP_REGISTRIES"
REGISTRY_FILE = "groups"


class ProcessGroupSurvived(TimeoutExpired):
    """The deadline passed and a member of the program's process group was still alive after KILL.

    It is a ``TimeoutExpired``, so callers that catch the timeout see it too. ``process_group`` is the group id.
    """

    def __init__(self, cmd: Any, timeout: float, process_group: int, output: str | bytes | None = None, stderr: str | bytes | None = None) -> None:
        super().__init__(cmd, timeout, output, stderr)
        self.process_group = process_group

    def __str__(self) -> str:
        return f"{super().__str__()}; process group {self.process_group} or a group its descendants started still had members after KILL"


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


def inherited_registries() -> list[str]:
    """The registries of this process's ancestors, from its own environment."""
    return [path for path in os.environ.get(REGISTRY_VARIABLE, "").split(":") if path]


def recorded_groups(registry: Path | None) -> set[int]:
    """The groups recorded in a registry. A line that is not a group id above 1, or that names the caller's own group, is ignored."""
    if registry is None:
        return set()
    try:
        lines = registry.read_text(encoding="utf-8").split()
    except FileNotFoundError:
        return set()
    own = os.getpgrp()
    return {int(line) for line in lines if line.isdigit() and int(line) > 1 and int(line) != own}


def record_group(process_group: int, registries: Sequence[str]) -> None:
    """Append a new group to every inherited registry. A registry whose owner has already returned (and removed it) is skipped, never created again."""
    line = f"{process_group}\n".encode()
    for path in registries:
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_APPEND)
        except (FileNotFoundError, NotADirectoryError):
            continue
        try:
            os.write(descriptor, line)
        finally:
            os.close(descriptor)


def create_registry(registries: Sequence[str]) -> Path:
    """Make this call's private registry: an empty file with mode 600 in a new folder with mode 700.

    The folder is made inside the nearest ancestor's registry folder when that folder exists and is writable, so the ancestor's removal takes it too.
    """
    parent = os.path.dirname(registries[-1]) if registries else None
    try:
        folder = tempfile.mkdtemp(prefix="process_groups.", dir=parent)
    except OSError:
        folder = tempfile.mkdtemp(prefix="process_groups.")
    registry = Path(folder) / REGISTRY_FILE
    os.close(os.open(registry, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600))
    return registry


def stop_process_group(
    process_group: int, leader: subprocess.Popen[str] | None = None, grace_seconds: float | None = None, registry: Path | None = None
) -> bool:
    """Stop every member of a process group and of every group recorded in ``registry``: TERM, then KILL after the grace period, and confirm none is left.

    ``leader`` is the child that started the group. It is reaped here, because an unreaped zombie keeps the group alive.
    ``grace_seconds`` is the time between TERM and KILL; None means ``STOP_GRACE_SECONDS``. The registry is read again while the stop waits, so a
    group recorded during the stop is stopped too. A group that has already ended counts as stopped, and so does a recorded group that this user may
    not signal (its id now belongs to another user's program, so it is no descendant of this call).
    Returns True when a member survived KILL for ``CONFIRM_SECONDS``.
    """
    grace = STOP_GRACE_SECONDS if grace_seconds is None else grace_seconds
    groups: set[int] = set()
    foreign: set[int] = set()
    for signal_number, wait_seconds in ((signal.SIGTERM, grace), (signal.SIGKILL, CONFIRM_SECONDS)):
        groups.add(process_group)
        signalled: set[int] = set()
        end = time.monotonic() + wait_seconds
        while True:
            groups |= recorded_groups(registry) - foreign
            for group in groups - signalled:
                try:
                    os.killpg(group, signal_number)
                except ProcessLookupError:
                    pass
                except PermissionError:
                    if group == process_group:
                        raise
                    foreign.add(group)
            groups -= foreign
            signalled |= groups
            if leader is not None:
                leader.poll()
            if not any(group_alive(group) for group in groups):
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
    ancestors = inherited_registries()
    registry = create_registry(ancestors)
    try:
        child_environment = dict(os.environ if env is None else env)
        child_environment[REGISTRY_VARIABLE] = ":".join([*ancestors, str(registry)])
        process = subprocess.Popen(  # noqa: S603 - the only process launcher: full path, list arguments, no shell, stdin closed, own process group
            [executable, *args],
            cwd=cwd,
            env=child_environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            process_group=0,
        )
        try:
            record_group(process.pid, ancestors)
            stdout, stderr = process.communicate(timeout=timeout)
        except TimeoutExpired as expired:
            survived = stop_process_group(process.pid, process, grace_seconds, registry)
            close_pipes(process)
            if survived:
                raise ProcessGroupSurvived(process.args, timeout, process.pid, expired.stdout, expired.stderr) from None
            raise
        except BaseException:
            stop_process_group(process.pid, process, grace_seconds, registry)
            close_pipes(process)
            raise
    finally:
        shutil.rmtree(registry.parent, ignore_errors=True)
    if check and process.returncode:
        raise CalledProcessError(process.returncode, process.args, output=stdout, stderr=stderr)
    return CompletedProcess(process.args, process.returncode, stdout, stderr)
