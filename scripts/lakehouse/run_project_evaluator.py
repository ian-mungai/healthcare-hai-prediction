"""Run dbt-project-evaluator on request and check that it stays out of every other dbt run.

Run from the repository root after ``scripts/quality/install_sqlfluff.py`` (it installs the pinned dbt and the dbt
packages):

    .venv/bin/python -m scripts.lakehouse.run_project_evaluator

It uses the dbt-core and dbt-duckdb versions of the analytics image from ``.tools/sqlfluff`` and the in-memory ``lint``
target: the evaluator reads only the dbt graph, so no catalog or credentials are needed. Checks (failure modes 32 to 38
in data/conformance/20261004/cleanup_checks_failure_modes.md): the packages install from the committed lock file without
changing it, a default selection holds no evaluator node, and the evaluator's findings are warnings, not errors. The
report, with the warning count of each evaluator rule, goes to data/e2e/dbt_evaluator/.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
DBT = ROOT / ".tools/sqlfluff/bin/dbt"
PROJECT = ROOT / "dbt"
LOCK = PROJECT / "package-lock.yml"
WORK = ROOT / "data/analytics/dbt/evaluator"
PACKAGES = ROOT / "data/analytics/dbt/dbt_packages"
REPORT_ROOT = ROOT / "data/e2e/dbt_evaluator"
PACKAGE = "dbt_project_evaluator"
ENABLE = '{"run_project_evaluator": true}'
TIMEOUT = 900


def dbt(args: list[str]) -> tuple[int, str]:
    """Run the pinned dbt on the lint target with its output outside the project; return the exit code and output."""
    env = {
        **os.environ,
        "DBT_PROFILES_DIR": str(PROJECT),
        "DBT_TARGET_PATH": str(WORK / "target"),
        "DBT_LOG_PATH": str(WORK / "logs"),
        "DBT_PACKAGES_INSTALL_PATH": str(PACKAGES),
    }
    common = ["--project-dir", str(PROJECT)] if args[0] == "deps" else ["--project-dir", str(PROJECT), "--target", "lint", "--no-partial-parse"]
    result = run_command(str(DBT), [*args, *common], cwd=ROOT, env=env, timeout=TIMEOUT)
    return result.returncode, result.stdout + result.stderr


def evaluator_nodes(extra: list[str]) -> list[str]:
    """Return the evaluator nodes a selection holds."""
    code, output = dbt(["ls", "--select", f"package:{PACKAGE}", "--output", "name", "--quiet", *extra])
    if code:
        raise RuntimeError(f"dbt ls failed: {output[-1500:]}")
    return [line.strip() for line in output.splitlines() if line.strip() and " " not in line.strip()]


def main() -> int:
    """Run the checks and the evaluator, write the report and return 0 only when every check passed."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, help="report path; default data/e2e/dbt_evaluator/report_<UTC>.json")
    args = parser.parse_args()
    checks: dict[str, bool] = {}
    warnings: dict[str, int] = {}
    try:
        if not DBT.exists():
            raise RuntimeError("dbt is missing: run .venv/bin/python -m scripts.quality.install_sqlfluff")
        before = hashlib.sha256(LOCK.read_bytes()).hexdigest() if LOCK.exists() else None
        code, output = dbt(["deps"])
        checks["packages_install_from_lock"] = code == 0 and before is not None
        checks["lock_file_unchanged"] = before is not None and LOCK.exists() and hashlib.sha256(LOCK.read_bytes()).hexdigest() == before
        if not checks["packages_install_from_lock"]:
            raise RuntimeError(f"dbt deps failed or the lock file is missing: {output[-1500:]}")
        checks["off_by_default"] = evaluator_nodes([]) == []
        checks["on_by_request"] = len(evaluator_nodes(["--vars", ENABLE])) > 0
        code, output = dbt(["build", "--select", f"package:{PACKAGE}", "--vars", ENABLE])
        results = json.loads((WORK / "target/run_results.json").read_text())["results"] if (WORK / "target/run_results.json").exists() else []
        statuses = [result["status"] for result in results]
        checks["build_ran"] = bool(results)
        checks["findings_are_warnings"] = code == 0 and "error" not in statuses and "fail" not in statuses
        warnings = {result["unique_id"].split(".")[2]: int(result.get("failures") or 0) for result in results if result["status"] == "warn"}
    except (RuntimeError, OSError, KeyError, ValueError) as error:
        checks["stopped_without_error"] = False
        sys.stdout.write(f"stopped: {type(error).__name__}: {error}\n")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report = {"run_at_utc": stamp, "target": "lint (in memory)", "checks": checks, "warnings_by_rule": warnings, "passed": all(checks.values())}
    path = args.output or REPORT_ROOT / f"report_{stamp}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    for name, passed in checks.items():
        sys.stdout.write(f"{'PASS' if passed else 'FAIL'} {name}\n")
    sys.stdout.write(
        f"{len(warnings)} evaluator rules with findings; report {path.relative_to(ROOT) if path.is_absolute() and path.is_relative_to(ROOT) else path}\n"
    )
    return 0 if checks and all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
