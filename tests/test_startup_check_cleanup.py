"""The private request folder of a startup check is removed on every path: after a timeout, after a malformed result and after a success.

The child is the real isolated Python process, or the synthetic fault child. The deadline is short for the timeout case, so the child's own bound and the
removal share one budget, as they do in production with the long one.
"""

from __future__ import annotations

import tempfile
import time
from pathlib import Path
from typing import Any

import pytest

from scripts.infrastructure import startup_check
from scripts.process import SESSION_VARIABLE
from tests.startup_check_support import host_environment
from tests.support import check

FAULT_CHILD = "tests.startup_check_child_faults"
MARKER = "SYNTHETIC_PRIVATE_QUEUE"


@pytest.fixture
def private_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Task-like environment, the real child process and a temporary root of its own, so a folder that is left behind is seen."""
    host_environment(monkeypatch)
    monkeypatch.setattr(startup_check, "ISOLATED", True)
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    return tmp_path


def use_fault(monkeypatch: pytest.MonkeyPatch, fault: str) -> None:
    monkeypatch.setattr(startup_check, "CHILD_MODULE", FAULT_CHILD)
    monkeypatch.setattr(startup_check, "CHILD_ENV_EXTRA", ("STARTUP_CHECK_TEST_FAULT",))
    monkeypatch.setenv("STARTUP_CHECK_TEST_FAULT", fault)


def left_behind(root: Path) -> list[str]:
    return sorted(str(path.relative_to(root)) for path in root.glob("startup_check_*/**/*")) + sorted(path.name for path in root.glob("startup_check_*"))


def test_the_private_folder_is_removed_after_a_timeout(monkeypatch: pytest.MonkeyPatch, private_root: Path) -> None:
    """The child hangs, the deadline is short, and the request with its marker-bearing queue path must still be removed."""
    use_fault(monkeypatch, "hang")
    monkeypatch.setattr(startup_check, "CHILD_SECONDS", 1.0)
    with pytest.raises(startup_check.StartupCheckError, match="startup_check_internal_error"):
        startup_check.check_queue_files("publisher", Path(f"{MARKER}.json"), controls=None, keys=False)
    check(not left_behind(private_root), f"the private folder is gone after a timeout: {left_behind(private_root)}")


def test_the_private_folder_is_removed_after_a_malformed_result(monkeypatch: pytest.MonkeyPatch, private_root: Path) -> None:
    use_fault(monkeypatch, "garbage_line")
    with pytest.raises(startup_check.StartupCheckError, match="startup_check_internal_error"):
        startup_check.check_queue_files("publisher", Path(f"{MARKER}.json"), controls=None, keys=False)
    check(not left_behind(private_root), f"the private folder is gone after a malformed result: {left_behind(private_root)}")


def test_the_private_folder_is_removed_after_a_success(monkeypatch: pytest.MonkeyPatch, private_root: Path) -> None:
    startup_check.check_files("publisher", [])
    check(not left_behind(private_root), f"the private folder is gone after a success: {left_behind(private_root)}")


def test_the_child_and_the_read_stop_before_the_overall_deadline_so_that_removal_has_time(monkeypatch: pytest.MonkeyPatch, private_root: Path) -> None:
    """The time given to the child is the overall bound less the time reserved for removal."""
    use_fault(monkeypatch, "hang")
    monkeypatch.setattr(startup_check, "CHILD_SECONDS", 2.0)
    given: list[float] = []
    real = startup_check.run_command

    def watch(program: str, arguments: list[str], **keywords: Any) -> Any:
        given.append(keywords["timeout"])
        return real(program, arguments, **keywords)

    monkeypatch.setattr(startup_check, "run_command", watch)
    with pytest.raises(startup_check.StartupCheckError):
        startup_check.check_queue_files("publisher", Path(f"{MARKER}.json"), controls=None, keys=False)
    check(given and given[0] < 2.0 - 0.1, f"the child's bound leaves time for the removal: {given}")


def stop_a_child_that_ignores_term(monkeypatch: pytest.MonkeyPatch, private_root: Path, seconds: float) -> float:
    """Run the check against a nested-launch child that ignores TERM and sleeps. Return the time to the fixed error, with the folder checked gone."""
    use_fault(monkeypatch, "ignore_term")
    monkeypatch.setenv(SESSION_VARIABLE, "1")
    monkeypatch.setattr(startup_check, "CHILD_SECONDS", seconds)
    started = time.monotonic()
    with pytest.raises(startup_check.StartupCheckError, match="startup_check_internal_error"):
        startup_check.check_queue_files("publisher", Path(f"{MARKER}.json"), controls=None, keys=False)
    elapsed = time.monotonic() - started
    check(not left_behind(private_root), f"the private folder is gone although the child ignored TERM: {left_behind(private_root)}")
    return elapsed


def test_a_nested_child_that_ignores_term_is_stopped_and_its_folder_removed_within_a_short_deadline(
    monkeypatch: pytest.MonkeyPatch, private_root: Path
) -> None:
    """The same budget split as production, scaled: the stop grace, the confirmation and the deletion all fit inside the one deadline."""
    elapsed = stop_a_child_that_ignores_term(monkeypatch, private_root, 6.0)
    check(elapsed < 6.0, f"the fixed error comes within the deadline: {elapsed:.2f} s")
