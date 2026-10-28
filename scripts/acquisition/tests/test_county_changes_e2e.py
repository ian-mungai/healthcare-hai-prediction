"""Run the offline county-change fetch and parse E2E twice in the acquisition gate and keep each artifact."""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime

import pytest

from scripts.acquisition import run_county_changes_e2e as suite
from scripts.acquisition.source_registry import REPO_ROOT
from tests.support import check


@pytest.mark.parametrize("iteration", [1, 2])
def test_county_changes_workflow(iteration: int, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the tool's real entry points against synthetic pages and require every scenario to pass."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    folder = REPO_ROOT / "data/e2e/acquisition_checks"
    folder.mkdir(parents=True, exist_ok=True)
    artifact = folder / f"county_changes_{stamp}_{os.getpid()}_{iteration}.json"
    monkeypatch.setattr(sys, "argv", [suite.__name__, "--output", str(artifact)])
    with pytest.raises(SystemExit) as raised:
        suite.main()
    report = json.loads(artifact.read_text())
    failed = [item["name"] for item in report["scenarios"] if not item["passed"]]
    check(raised.value.code == 0 and report["status"] == "passed" and not failed, f"County-change scenarios failed: {failed}; inspect {artifact}")
