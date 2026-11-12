"""The job-entry boundary of the startup check: a failure ends as fixed text and a status, and a parent's diagnostic hook never sees a raw request value.

Each case runs the real check in a real parent process with a hook that prints the locals of every frame of an uncaught error. The result file cases replace
the file at the moment the parent opens it. Every path, marker and file here is synthetic.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from scripts.process import TimeoutExpired, run_command
from tests.startup_check_parent_probe import MARKER
from tests.startup_check_support import host_environment
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
PROBE = "tests.startup_check_parent_probe"
BOUND_SECONDS = 40


def run_probe(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, scenario: str, **extra: str) -> tuple[int, str]:
    """Run one scenario in its own process, in a task-like environment, and return its exit status and everything it wrote."""
    host_environment(monkeypatch)
    environment = {**os.environ, "TMPDIR": str(tmp_path), "OM_API": "", **extra}
    try:
        done = run_command(sys.executable, ["-m", PROBE, scenario], cwd=ROOT, env=environment, timeout=BOUND_SECONDS)
    except TimeoutExpired:
        pytest.fail(f"{scenario}: the parent did not end within {BOUND_SECONDS} seconds")
    return done.returncode, done.stdout + done.stderr


@pytest.mark.parametrize(
    ("scenario", "extra"),
    [("expected", {}), ("internal", {}), ("timeout", {"STARTUP_CHECK_TEST_FAULT": "hang"})],
    ids=["expected failure", "internal error", "timeout"],
)
def test_a_startup_failure_ends_as_fixed_text_and_never_shows_a_raw_value_to_a_diagnostic_hook(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, scenario: str, extra: dict[str, str]
) -> None:
    status, text = run_probe(monkeypatch, tmp_path, scenario, **extra)
    check(status != 0, f"{scenario}: the job ends with a failure status: {status}")
    check("startup_check: FAILED" in text, f"{scenario}: the fixed diagnostic is printed: {text[:600]}")
    check(MARKER not in text, f"{scenario}: a raw request value reached the hook: {text[:1500]}")


@pytest.mark.parametrize("replacement", ["fifo", "symlink"])
def test_a_result_file_replaced_before_it_is_opened_is_refused_without_blocking(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, replacement: str) -> None:
    """A descendant of the child replaces the result file with a FIFO that has no writer, or with a link, after the child has finished."""
    status, text = run_probe(monkeypatch, tmp_path, replacement)
    check(
        status != 0 and "startup_check: FAILED startup_check_internal_error" in text,
        f"{replacement}: the replacement ends the check as the internal error: {text[:600]}",
    )
