"""Every committed collector plan loads through its contract, so a stale lock or plan chain fails the gate (failure mode 261).

Several contracts read the collected captures the plans name, which live in the ignored data/ folder. A clean
checkout, as in GitHub CI, has none, so the test is skipped there and runs in the local gate.
"""

from __future__ import annotations

import importlib

import pytest

from scripts.acquisition.redownload import PLANNED
from scripts.acquisition.source_registry import REPO_ROOT

COLLECTED = REPO_ROOT / "data/datasets"


@pytest.mark.skipif(not COLLECTED.is_dir(), reason="the committed plans read collected captures under data/, absent in a clean checkout")
@pytest.mark.parametrize("mode", sorted(PLANNED))
def test_committed_plan_loads(mode: str) -> None:
    """Load the committed plan the redownload preflight reads for this collector."""
    module_name, loader = PLANNED[mode][:2]
    getattr(importlib.import_module(f"scripts.acquisition.{module_name}"), loader)()
