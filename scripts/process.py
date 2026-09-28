"""The one place this repository's scripts start another program.

Every call resolves the program to a full path, passes a list of arguments (never a shell string), closes stdin and
applies a timeout. Other scripts import ``run_command`` from here instead of ``subprocess``, so the lint exception for
starting a process exists on one line. Run scripts that import it as modules from the repository root, for example
``.venv/bin/python -m scripts.quality.scan_secrets``.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from subprocess import CompletedProcess, TimeoutExpired

__all__ = ["CompletedProcess", "TimeoutExpired", "find_program", "run_command"]

DEFAULT_TIMEOUT_SECONDS = 300


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
    return subprocess.run(  # noqa: S603 - the only process launcher: full path, list arguments, no shell, stdin closed, timeout
        [executable, *args],
        cwd=cwd,
        env=None if env is None else dict(env),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=check,
    )
