"""Run the offline reference-document E2E twice in the acquisition gate and keep each artifact."""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from scripts.acquisition import run_reference_documents_e2e as suite
from scripts.acquisition.source_registry import REPO_ROOT
from tests.support import check


@pytest.mark.parametrize("iteration", [1, 2])
def test_reference_documents_workflow(iteration: int, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the fetch scripts' real entry points against the saved fixtures and require every scenario to pass."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    folder = REPO_ROOT / "data/e2e/acquisition_checks"
    folder.mkdir(parents=True, exist_ok=True)
    artifact = folder / f"reference_documents_{stamp}_{os.getpid()}_{iteration}.json"
    monkeypatch.setattr(sys, "argv", [suite.__name__, "--output", str(artifact)])
    with pytest.raises(SystemExit) as raised:
        suite.main()
    report = json.loads(artifact.read_text())
    failed = [item["name"] for item in report["scenarios"] if not item["passed"]]
    check(raised.value.code == 0 and report["status"] == "passed" and not failed, f"Reference-document scenarios failed: {failed}; inspect {artifact}")


SIX = suite.SIX_CODES
MUTATED_CODES = {
    **{f"without_{code}": tuple(other for other in SIX if other != code) for code in SIX},
    "one_wrong_code": ("02041", *SIX[1:]),
    "one_extra_code": (*SIX, "02999"),
}


@pytest.mark.parametrize("mutated", MUTATED_CODES.values(), ids=MUTATED_CODES.keys())
def test_six_code_scenario_fails_when_a_code_is_missing_or_wrong(mutated: tuple[str, ...], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Mutate the checked codes in the subject and require the independently written expectation to notice."""
    monkeypatch.setattr(suite.alaska, "CODES", mutated)
    workspace = suite.Workspace(tmp_path, json.loads(suite.FIXTURES.read_text()))
    suite.crosswalk_scenarios(workspace)
    six_codes = next(item for item in workspace.scenarios if item["name"] == suite.SIX_CODE_SCENARIO)
    check(not six_codes["passed"], f"The six-code scenario passed with codes {mutated}")
