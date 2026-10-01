"""Run existing synthetic integrated workflows in the standard acquisition gate."""

from __future__ import annotations

import importlib
import os
import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from scripts.acquisition import collection_layout, s3_store
from scripts.acquisition.source_registry import REPO_ROOT, read_json
from tests.support import check

SUITES = [
    "bls_api",
    "census_acs_api",
    "census_acs_detailed",
    "hud_api",
    "wonder_export",
    "cms_owners",
    "hcai_util",
    "onc_mu",
    "privacy",
    "code_versions",
    "collection_policy",
    "scope_cache",
    "history",
    "redownload",
    "registry_additions",
    # Off macOS, the HUD and WONDER suites substitute only the browser-download metadata reads (see run_hud_xlsx_e2e).
    "hud_xlsx",
]
# These suites write a folder whose artifact.json holds the outcome.
DIRECTORY_SUITES = {"privacy", "registry_additions"}
# Its folder also holds a copy of the 17 MB registry per case (about 300 MB); retain only the artifact.
ARTIFACT_ONLY_SUITES = {"registry_additions"}


@pytest.fixture(scope="session")
def offline_artifact_root() -> Path:
    """Keep suite outcomes after temporary synthetic source files are removed."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    root = REPO_ROOT / "data/e2e/acquisition_checks" / f"{stamp}_{os.getpid()}"
    root.mkdir(parents=True, exist_ok=False)
    return root


@pytest.mark.parametrize("iteration", [1, 2])
@pytest.mark.parametrize("suite", SUITES)
def test_offline_workflow(suite: str, iteration: int, offline_artifact_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise production orchestration and persist its own aggregate outcome."""
    retained = offline_artifact_root / f"{suite}_{iteration}{'' if suite in DIRECTORY_SUITES else '.json'}"
    scratch = Path(tempfile.mkdtemp(prefix=f"{suite}_e2e_")) if suite in ARTIFACT_ONLY_SUITES else None
    output = scratch / "output" if scratch else retained
    monkeypatch.setattr(s3_store, "load_routes", collection_layout.load_routes)
    module = importlib.import_module(f"scripts.acquisition.run_{suite}_e2e")
    monkeypatch.setattr(sys, "argv", [module.__name__, "--output", str(output)])
    try:
        try:
            module.main()
        except SystemExit as exc:
            check(exc.code in (None, 0), f"Integrated {suite} exited with {exc.code}")
        finally:
            if scratch and (output / "artifact.json").exists():
                retained.mkdir(parents=True, exist_ok=False)
                shutil.copy2(output / "artifact.json", retained / "artifact.json")
    finally:
        if scratch:
            shutil.rmtree(scratch)
    result = read_json(retained / "artifact.json" if suite in DIRECTORY_SUITES else retained)
    if "status" in result:
        passed = result["status"] == "passed"
    elif isinstance(result.get("passed"), bool):
        passed = result["passed"]
    else:
        passed = result.get("total") is not None and result.get("passed") == result["total"]
    check(passed, f"Integrated {suite} workflow failed; inspect {retained}")
