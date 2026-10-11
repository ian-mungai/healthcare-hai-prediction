"""Check the package fixture kit end to end: a credential-free prepare in a fresh worktree and every check outcome.

Run ``.venv/bin/python -m scripts.lakehouse.run_package_fixture_e2e`` with Docker running and the analytics image built.
Reports stay in data/e2e/package_fixture/. It uses synthetic selectors for a test package (W0 to W2) on the beab859
fixture; real package selectors are frozen in plans/parallel_work_20261010/selectors/. It needs the beab859 fixture base
build and baseline in the main checkout (data/analytics/dbt/e2e/base/, data/wave0/baselines/fixture_beab859/). It takes
about 20 minutes and is not part of the local CI script. Failure modes 737, 888 and 889 are in
plans/wave0_20261010/failure_modes.md.
"""

from __future__ import annotations

import difflib
import hashlib
import json
import shutil
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import CompletedProcess, run_command

ROOT = Path(__file__).resolve().parents[2]
MODEL = "dbt/models/silver/intermediate/int_occmix_survey_rows.sql"
TEST = "dbt/tests/assert_occmix_values_cast.sql"
BASELINE = "data/wave0/baselines/fixture_beab859/baseline.json"


def patch(folder: Path, name: str, path: str, change: Callable[[str], str]) -> Path:
    """Write a unified diff of one file of dbt/ with paths a/<path> and b/<path>."""
    before = (ROOT / path).read_text()
    lines = difflib.unified_diff(before.splitlines(keepends=True), change(before).splitlines(keepends=True), f"a/{path}", f"b/{path}")
    target = folder / f"{name}.patch"
    target.write_text("".join(lines))
    return target


def selector(folder: Path, package: str, tests: list[str]) -> Path:
    """A synthetic selector contract in the selectors' format, owning the occupational-mix survey model."""
    document = {
        "package": package,
        "owned_models": ["int_occmix_survey_rows"],
        "owned_seeds": [],
        "required_tests": tests,
        "required_test_count": len(tests),
        "retired_tests": [],
        "documented_changes": [],
        "compare_unchanged_with_baseline": ["int_occmix_survey_rows"],
        "baseline": BASELINE,
        "rule": "test selector for the kit's E2E",
        "note": "synthetic",
    }
    path = folder / f"{package.lower()}.yml"
    path.write_text(json.dumps(document, indent=2))  # JSON is valid YAML
    return path


CALLS: list[dict[str, Any]] = []


def kit(*args: str, cwd: Path = ROOT) -> CompletedProcess[str]:
    """Run the kit's CLI and keep its exit code and the tail of its error output for the report."""
    result = run_command(sys.executable, ["-m", "scripts.lakehouse.package_fixture", *args], cwd=cwd, timeout=3600)
    CALLS.append({"args": [arg.replace(str(ROOT), ".") for arg in args], "exit": result.returncode, "stderr": result.stderr[-600:]})
    return result


def main() -> int:
    """Run every scenario, write the report and return 1 when any check fails."""
    started = datetime.now(UTC)
    folder = ROOT / "data" / "e2e" / "package_fixture" / started.strftime("%Y%m%dT%H%M%SZ")
    folder.mkdir(parents=True)
    checks: dict[str, bool] = {}
    evidence: dict[str, Any] = {}

    # 888: prepare in a fresh linked worktree that has no data/ at all (no secrets, no packages, no builds).
    worktree = folder / "worktree"
    run_command("git", ["worktree", "add", "--detach", "-q", str(worktree), "HEAD"], cwd=ROOT, check=True)
    for name in ("scripts/lakehouse/package_fixture.py", "scripts/lakehouse/run_lock.py", "scripts/lakehouse/memory_budget.py"):
        shutil.copy2(ROOT / name, worktree / name)  # the kit under test may be uncommitted
    prepared = kit("prepare", cwd=worktree)
    record = worktree / "data/package_fixture/prepare.json"
    evidence["prepare"] = json.loads(record.read_text()) if record.exists() else prepared.stderr[-800:]
    checks["prepare builds in a worktree with no secrets"] = (
        prepared.returncode == 0 and not (worktree / "data/lakehouse/secrets").exists() and json.loads(record.read_text())["failed_nodes"] == []
    )
    run_command("git", ["worktree", "remove", "--force", str(worktree)], cwd=ROOT, check=True)

    # 889 and 737: record, verify, tampering, a missing required test and a changed kept model.
    w0 = selector(folder, "W0", ["assert_occmix_values_cast"])
    artifact = ROOT / "data/package_fixture/records/w0/effective_selection.json"
    artifact.unlink(missing_ok=True)
    recorded = kit("check", "--package", "W0", "--selector", str(w0), "--record")
    approved = folder / "approved_w0.json"
    if artifact.exists():
        shutil.copy2(artifact, approved)
    evidence["recorded"] = json.loads(artifact.read_text()) if artifact.exists() else recorded.stderr[-800:]
    checks["record writes the effective selection with both hashes"] = recorded.returncode == 0 and approved.exists()
    again = kit("check", "--package", "W0", "--selector", str(w0), "--record")
    checks["a second record is refused"] = again.returncode == 1 and "is recorded once" in again.stderr
    verified = kit("check", "--package", "W0", "--selector", str(w0), "--approved", str(approved))
    checks["check passes against the approved selection"] = verified.returncode == 0
    report = json.loads((ROOT / "data/package_fixture/records/w0/check.json").read_text())
    checks["unchanged model equals the beab859 baseline"] = report["comparisons"].get("int_occmix_survey_rows") == "equal"
    tampered = folder / "tampered_w0.json"
    content = json.loads(approved.read_text()) if approved.exists() else {"nodes": []}
    content["nodes"] = content["nodes"][1:]
    tampered.write_text(json.dumps(content))
    refused = kit("check", "--package", "W0", "--selector", str(w0), "--approved", str(tampered))
    checks["a selection that differs from the approved one fails"] = refused.returncode == 1 and "nodes differs from the approved selection" in refused.stderr
    w1 = selector(folder, "W1", ["assert_occmix_values_cast", "assert_test_that_does_not_exist"])
    (ROOT / "data/package_fixture/records/w1/effective_selection.json").unlink(missing_ok=True)
    missing = kit("check", "--package", "W1", "--selector", str(w1), "--record")
    checks["a missing required test fails"] = missing.returncode == 1 and "required test missing: assert_test_that_does_not_exist" in missing.stderr
    emptied = patch(folder, "empty_model", MODEL, lambda text: text.rstrip("\n") + "\nwhere false\n")
    changed = kit("check", "--package", "W0", "--selector", str(w0), "--patch", str(emptied), "--approved", str(approved))
    checks["a changed kept model fails the baseline comparison"] = changed.returncode == 1 and "int_occmix_survey_rows differs" in changed.stderr

    # 889: an expected failure passes only when exactly its named test fails.
    broken = patch(folder, "break_cast_test", TEST, lambda _text: "select 1 as failure\n")
    exact = kit(
        "check", "--package", "W0", "--selector", str(w0), "--approved", str(approved), "--expected-failure", f"cast:{broken}:assert_occmix_values_cast"
    )
    checks["an expected failure of its named test passes"] = exact.returncode == 0
    wrong = kit(
        "check",
        "--package",
        "W0",
        "--selector",
        str(w0),
        "--approved",
        str(approved),
        "--expected-failure",
        f"cast:{broken}:assert_occmix_holds_exclude_values",
    )
    checks["an expected failure of another test fails"] = wrong.returncode == 1 and "wanted only assert_occmix_holds_exclude_values to fail" in wrong.stderr

    passed = all(checks.values())
    code = {
        name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
        for name in ("scripts/lakehouse/package_fixture.py", "scripts/lakehouse/run_package_fixture_e2e.py")
    }
    result = {
        "feature": "package fixture kit (wave 0 item 9)",
        "started_utc": started.isoformat(),
        "finished_utc": datetime.now(UTC).isoformat(),
        "status": "pass" if passed else "fail",
        "commit": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
        "code_sha256": code,
        "command": ".venv/bin/python -m scripts.lakehouse.run_package_fixture_e2e",
        "checks": checks,
        "evidence": evidence,
        "calls": CALLS,
        "limits": ["One synthetic package selector on the occupational-mix survey model; real selectors are checked when packages hand back work."],
    }
    (folder / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    for name, ok in checks.items():
        sys.stdout.write(f"{'ok' if ok else 'FAIL':<5} {name}\n")
    sys.stdout.write(f"{result['status']}: {folder.relative_to(ROOT)}/report.json\n")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
