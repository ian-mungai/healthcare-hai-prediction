"""Resolve installed acquisition command-line tools without invoking a shell."""

import shutil
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal

from scripts.process import CompletedProcess, run_command


def executable(name: Literal["aws", "terraform", "xattr"]) -> str:
    """Return the installed tool's absolute path or fail before starting a process."""
    path = shutil.which(name)
    if path is None:
        raise FileNotFoundError(f"Required acquisition tool is unavailable: {name}")
    return path


def run_argv(
    argv: Sequence[str],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    timeout: float = 300,
    check: bool = False,
    capture_output: bool = True,
    text: bool = True,
) -> CompletedProcess[str]:
    """Adapt existing injected collector runners to the single captured-text launcher."""
    if not argv or not capture_output or not text:
        raise ValueError("Collector commands require nonempty argv and captured text output")
    return run_command(argv[0], argv[1:], cwd=cwd, env=env, timeout=timeout, check=check)
