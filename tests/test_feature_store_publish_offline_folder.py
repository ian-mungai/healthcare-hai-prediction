"""The feature store publish_offline mode refuses a report folder that the Spark suite cannot see, before it builds anything or starts a session."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from scripts.features import run_feature_store_e2e as runner
from tests.support import check


def test_a_report_folder_outside_the_report_root_is_refused_before_anything_is_built(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    folder = tmp_path / "publish_offline"
    monkeypatch.setattr(sys, "argv", ["run_feature_store_e2e", "--mode", "publish_offline", "--output-directory", str(folder)])
    code = runner.main()
    report = json.loads((folder / "report.json").read_text())
    check(code == 1 and report["status"] == "fail", f"the run did not fail: {code} {report['status']}")
    check("directly under data/e2e/feature_store" in report["error"], f"the error does not name the report root: {report['error'][:200]}")
    check(not (folder / "inputs.json").exists() and not (folder / "exports").exists(), "a refused folder received exports")


def test_a_new_folder_under_the_report_root_is_accepted() -> None:
    runner.require_report_root(runner.OUT / "20261114T000000Z")
