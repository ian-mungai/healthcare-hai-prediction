"""Include the registry-free durable-state workflow in the acquisition coverage gate."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime

import pytest

from scripts.acquisition import run_collector_state_e2e
from scripts.acquisition.source_registry import REPO_ROOT
from tests.support import check


def test_collector_state_workflow(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run the complete synthetic workflow and keep its repeatable artifact."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    folder = REPO_ROOT / "data/e2e/collector_state" / stamp
    output = folder / "artifact.json"
    monkeypatch.setattr(sys, "argv", [run_collector_state_e2e.__name__, "--output", str(output)])
    with pytest.raises(SystemExit) as result:
        run_collector_state_e2e.main()
    check(result.value.code == 0, f"Collector state workflow failed: {output}")
    artifact = json.loads(output.read_bytes())
    check(artifact["status"] == "passed" and artifact["code_unchanged_during_run"], f"Collector state artifact is not accepted: {output}")
    (folder / "retention.json").write_text(
        json.dumps(
            {
                "class": 5,
                "purpose": "Synthetic collector state regression baseline",
                "created": stamp,
                "end_condition": "A later passing run replaces this baseline",
            }
        )
        + "\n"
    )
