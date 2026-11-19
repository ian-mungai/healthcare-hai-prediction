"""Run existing synthetic integrated workflows in the standard acquisition gate."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import shutil
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
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
    "census_acs_tables",
    "hud_api",
    "wonder_export",
    "reference_download",
    "ahrq_compendium",
    "cms_owners",
    "nber_hc",
    "hcai_util",
    "onc_mu",
    "pseudonymize",
    "code_versions",
    "collection_policy",
    "scope_cache",
    "history",
    "redownload",
    "registry_additions",
    "manifest_correction",
    "single_copy_index",
    # Off macOS, the HUD and WONDER suites substitute only the browser-download metadata reads (see run_hud_xlsx_e2e).
    "hud_xlsx",
    "hud_xlsx_direct",
]
DIRECTORY_SUITES = {"pseudonymize", "registry_additions"}
ARTIFACT_ONLY_SUITES = {"registry_additions"}
MODE = os.environ.get("ACQUISITION_E2E_MODE", "full")
SNAPSHOT_ROOTS = ("scripts/acquisition", "config/acquisition")
SECONDS: dict[str, float] = {}


def code_sha256() -> str:
    """Hash every file under the snapshot roots, caches excluded, by path and content."""
    digest = hashlib.sha256()
    for root in SNAPSHOT_ROOTS:
        for path in sorted((REPO_ROOT / root).rglob("*")):
            if path.is_file() and "__pycache__" not in path.parts:
                digest.update(path.relative_to(REPO_ROOT).as_posix().encode() + b"\0" + hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


@pytest.fixture(scope="session")
def offline_artifact_root() -> Iterator[Path]:
    """Keep suite outcomes after temporary synthetic source files are removed; record the run in run.json."""
    check(MODE in ("full", "iteration"), f"ACQUISITION_E2E_MODE must be full or iteration, not {MODE!r}")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    root = REPO_ROOT / "data/e2e/acquisition_checks" / f"{stamp}_{os.getpid()}"
    root.mkdir(parents=True, exist_ok=False)
    started = code_sha256()
    yield root
    unchanged = code_sha256() == started
    record = {
        "mode": MODE,
        "evidence": "commit evidence" if MODE == "full" else "iteration run: not commit evidence",
        "skipped_checks": [f"{suite}_2" for suite in SUITES] if MODE == "iteration" else [],
        "code_sha256": started,
        "code_unchanged_during_run": unchanged,
        "parallel": "not applicable: suites share process state",
        "suite_seconds": [
            {"suite": suite, "run": run, "seconds": SECONDS[f"{suite}_{run}"]} for suite in SUITES for run in (1, 2) if f"{suite}_{run}" in SECONDS
        ],
    }
    (root / "run.json").write_text(json.dumps(record, indent=2) + "\n")
    check(unchanged, f"Code under test changed during the run; rerun on one version ({root / 'run.json'})")


@pytest.mark.parametrize("iteration", [1, 2])
@pytest.mark.parametrize("suite", SUITES)
def test_offline_workflow(suite: str, iteration: int, offline_artifact_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise production orchestration and persist its own aggregate outcome."""
    if MODE == "iteration" and iteration == 2:
        pytest.skip("iteration run: the repeatability run is skipped; not commit evidence")
    began = time.monotonic()
    retained = offline_artifact_root / f"{suite}_{iteration}{'' if suite in DIRECTORY_SUITES else '.json'}"
    scratch = Path(tempfile.mkdtemp(prefix=f"{suite}_e2e_")) if suite in ARTIFACT_ONLY_SUITES else None
    output = scratch / "output" if scratch else retained
    monkeypatch.setattr(s3_store, "load_routes", collection_layout.load_routes)
    module = importlib.import_module(f"scripts.acquisition.run_{suite}_e2e")
    monkeypatch.setattr(sys, "argv", [module.__name__, "--output", str(output)])
    exit_code: object = None
    try:
        try:
            module.main()
        except SystemExit as exc:
            # Keep the status: the artifact, which holds the error and the failed cases, is read below on every path.
            exit_code = exc.code
        finally:
            if scratch and (output / "artifact.json").exists():
                retained.mkdir(parents=True, exist_ok=False)
                shutil.copy2(output / "artifact.json", retained / "artifact.json")
    finally:
        if scratch:
            shutil.rmtree(scratch)
    exited_cleanly = exit_code in (None, 0)
    artifact = retained / "artifact.json" if suite in DIRECTORY_SUITES else retained
    SECONDS[f"{suite}_{iteration}"] = round(time.monotonic() - began, 1)
    if not artifact.is_file():
        check(False, f"Integrated {suite} exited with {exit_code!r} and left no artifact at {artifact}")
    result = read_json(artifact)
    if "status" in result:
        passed = result["status"] == "passed"
    elif isinstance(result.get("passed"), bool):
        passed = result["passed"]
    else:
        passed = result.get("total") is not None and result.get("passed") == result["total"]
    check(passed and exited_cleanly, failure_message(suite, retained, result, exit_code))


def failure_message(suite: str, retained: Path, result: dict, exit_code: object = None) -> str:
    """The pytest failure text of a suite that did not pass: its exit status, the recorded error, the failed cases and where the evidence is."""
    detail = [] if exit_code in (None, 0) else [f"exit status {exit_code}"]
    detail += [f"error: {result['error']}"] if result.get("error") else []
    cases = result.get("cases")
    if isinstance(cases, list):
        detail.append(f"{len(cases)} cases ran")
        detail += [f"case not passed: {case.get('name', case.get('case', '?'))}" for case in cases if isinstance(case, dict) and case.get("passed") is False]
    return f"Integrated {suite} workflow failed ({'; '.join(detail) or 'no error recorded'}); inspect {retained}"


def run_planted_suite(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, planted: Callable[[Path], list[dict]]) -> str:
    """Run ``test_offline_workflow`` itself on the registry additions suite with a planted ``run_cases`` and return the failure message it raises."""
    module = importlib.import_module("scripts.acquisition.run_registry_additions_e2e")
    monkeypatch.setattr(module, "run_cases", planted)
    with pytest.raises(AssertionError) as raised:
        test_offline_workflow("registry_additions", 1, tmp_path, monkeypatch)
    return str(raised.value)


def test_blocked_registry_additions_error_reaches_the_failure_message(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A run that stops before any case (status blocked, 0 cases, exit 1) keeps its error and its exit status in the failure message, as under the gate."""

    def stop(_root: Path) -> list[dict]:
        raise OSError("No space left on device: planted by the case")

    message = run_planted_suite(tmp_path, monkeypatch, stop)
    check("No space left on device: planted by the case" in message, f"the blocked error is missing from the failure message: {message}")
    check("OSError" in message and "stop" in message, f"the error type and the place it was raised are missing from the failure message: {message}")
    check("exit status 1" in message, f"the exit status is missing from the failure message: {message}")


def test_failed_case_names_reach_the_failure_message(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A run in which a case fails (status failed, exit 1) names that case in the failure message."""

    def one_failed_case(_root: Path) -> list[dict]:
        return [{"case": "synthetic_passed_case", "passed": True}, {"case": "synthetic_failed_case", "passed": False}]

    message = run_planted_suite(tmp_path, monkeypatch, one_failed_case)
    check("case not passed: synthetic_failed_case" in message, f"the failed case is missing from the failure message: {message}")
    check("synthetic_passed_case" not in message, f"a passing case is named as failed: {message}")
    check("2 cases ran" in message and "exit status 1" in message, f"the case count or the exit status is missing: {message}")


def test_nonzero_exit_without_an_artifact_is_reported_with_its_status(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A suite that exits nonzero before it writes its artifact still fails with a message that names the status and the missing artifact."""
    module = importlib.import_module("scripts.acquisition.run_registry_additions_e2e")

    def exit_early() -> None:
        raise SystemExit(3)

    monkeypatch.setattr(module, "main", exit_early)
    with pytest.raises(AssertionError) as raised:
        test_offline_workflow("registry_additions", 1, tmp_path, monkeypatch)
    message = str(raised.value)
    check("exited with 3" in message and "no artifact" in message, f"the exit status or the missing artifact is not named: {message}")
