"""The slow case of the startup check's shutdown budget: the production bound, with a nested-launch child that ignores TERM and sleeps.

Each case waits most of the 60 second bound, so they are opt-in: without ``HAI_RUN_SLOW_TESTS=1`` they are skipped, and a plain ``pytest tests`` run or
the configuration CI command does not wait for them. Run them by name:

    HAI_RUN_SLOW_TESTS=1 .venv/bin/python -m pytest tests/test_startup_check_cleanup_slow.py
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

import pytest

from scripts import process
from scripts.infrastructure import startup_check
from scripts.process import SESSION_VARIABLE
from tests.startup_check_support import host_environment
from tests.support import check
from tests.test_startup_check_cleanup import FAULT_CHILD, MARKER, SLOW_FLAG, delay_confirmation, left_behind

PRODUCTION_CHILD_SECONDS = 60.0
pytestmark = pytest.mark.skipif(os.environ.get(SLOW_FLAG) != "1", reason=f"a production-bound case of about a minute; set {SLOW_FLAG}=1 to run it")


def run_at_the_production_bound(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A nested-launch child that ignores TERM and sleeps, at the production bound. The folder must be gone and the fixed error must come in time."""
    host_environment(monkeypatch)
    monkeypatch.setattr(startup_check, "ISOLATED", True)
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(startup_check, "CHILD_MODULE", FAULT_CHILD)
    monkeypatch.setattr(startup_check, "CHILD_ENV_EXTRA", ("STARTUP_CHECK_TEST_FAULT",))
    monkeypatch.setenv("STARTUP_CHECK_TEST_FAULT", "ignore_term")
    monkeypatch.setenv(SESSION_VARIABLE, "1")
    monkeypatch.setattr(startup_check, "CHILD_SECONDS", PRODUCTION_CHILD_SECONDS)
    started = time.monotonic()
    with pytest.raises(startup_check.StartupCheckError, match="startup_check_internal_error"):
        startup_check.check_queue_files("publisher", Path(f"{MARKER}.json"), controls=None, keys=False)
    elapsed = time.monotonic() - started
    check(not left_behind(tmp_path), f"the private folder is gone: {left_behind(tmp_path)}")
    check(elapsed <= PRODUCTION_CHILD_SECONDS, f"the fixed error comes within the overall deadline: {elapsed:.2f} s")


def test_a_nested_child_that_ignores_term_is_stopped_and_its_folder_removed_at_the_production_bound(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    run_at_the_production_bound(monkeypatch, tmp_path)


def test_a_confirmation_that_takes_nearly_the_launchers_whole_bound_still_leaves_time_to_remove_the_folder_at_the_production_bound(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The launcher's own bound is 5 seconds after KILL. The confirmation here takes 4.9 seconds."""
    check(process.CONFIRM_SECONDS == 5.0, "the launcher keeps its 5 second confirmation bound")
    delay_confirmation(monkeypatch, 4.9)
    run_at_the_production_bound(monkeypatch, tmp_path)
