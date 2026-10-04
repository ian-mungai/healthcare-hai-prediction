"""Every committed collector plan loads through its contract, so a stale lock or plan chain fails the gate (failure mode 261)."""

from __future__ import annotations

import importlib

import pytest

from scripts.acquisition.redownload import PLANNED


@pytest.mark.parametrize("mode", sorted(PLANNED))
def test_committed_plan_loads(mode: str) -> None:
    """Load the committed plan the redownload preflight reads for this collector."""
    module_name, loader = PLANNED[mode][:2]
    getattr(importlib.import_module(f"scripts.acquisition.{module_name}"), loader)()
