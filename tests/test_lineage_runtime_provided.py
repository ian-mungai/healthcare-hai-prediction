"""The orchestration runtime lists the mounted ``hai_lineage`` package as provided, so the dependency check does not look for an installed package."""

from __future__ import annotations

import json
from pathlib import Path

from tests.support import check

ROOT = Path(__file__).resolve().parents[1]


def test_the_lineage_runtime_declares_the_mounted_package_it_imports() -> None:
    runtimes = json.loads((ROOT / "config/quality/dependency_runtimes.json").read_text())["runtimes"]
    check("hai_lineage" in runtimes["orchestration"]["provided"], "the orchestration runtime does not list the mounted hai_lineage package as provided")
