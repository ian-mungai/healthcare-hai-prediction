"""The slow case of the startup check's shutdown budget: the production bound, with a nested-launch child that ignores TERM and sleeps.

It waits the whole execution bound (about 55 seconds), so it lives in its own file. Run it by name: ``pytest tests/test_startup_check_cleanup_slow.py``.
"""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import pytest

from scripts.infrastructure import startup_check
from scripts.process import SESSION_VARIABLE
from tests.startup_check_support import host_environment
from tests.support import check
from tests.test_startup_check_cleanup import FAULT_CHILD, MARKER, left_behind

PRODUCTION_CHILD_SECONDS = 60.0
PRODUCTION_CLEANUP_SECONDS = 5.0


def test_a_nested_child_that_ignores_term_is_stopped_and_its_folder_removed_at_the_production_bound(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    host_environment(monkeypatch)
    monkeypatch.setattr(startup_check, "ISOLATED", True)
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(startup_check, "CHILD_MODULE", FAULT_CHILD)
    monkeypatch.setattr(startup_check, "CHILD_ENV_EXTRA", ("STARTUP_CHECK_TEST_FAULT",))
    monkeypatch.setenv("STARTUP_CHECK_TEST_FAULT", "ignore_term")
    monkeypatch.setenv(SESSION_VARIABLE, "1")
    monkeypatch.setattr(startup_check, "CHILD_SECONDS", PRODUCTION_CHILD_SECONDS)
    monkeypatch.setattr(startup_check, "CLEANUP_SECONDS", PRODUCTION_CLEANUP_SECONDS)
    started = time.monotonic()
    with pytest.raises(startup_check.StartupCheckError, match="startup_check_internal_error"):
        startup_check.check_queue_files("publisher", Path(f"{MARKER}.json"), controls=None, keys=False)
    elapsed = time.monotonic() - started
    check(not left_behind(tmp_path), f"the private folder is gone: {left_behind(tmp_path)}")
    check(elapsed <= PRODUCTION_CHILD_SECONDS, f"the fixed error comes within the overall deadline: {elapsed:.2f} s")
