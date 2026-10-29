"""Run synthetic catalog migration and recovery cases and retain repeatable local evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse.catalog_migration_cases import CASES, MIGRATION_CASES, Scenario
from scripts.process import TimeoutExpired, run_command

ROOT = Path(__file__).resolve().parents[2]
SUBJECTS = (
    "scripts/lakehouse/migrate_aws_catalog.py",
    "scripts/lakehouse/catalog_migration_cases.py",
    "scripts/lakehouse/catalog_journal.py",
    "scripts/lakehouse/register_aws_catalog.py",
    "scripts/lakehouse/run_catalog_migration_e2e.py",
    "scripts/lakehouse/run_catalog_migration_defects.py",
    "scripts/process.py",
)


def fingerprints() -> dict[str, str | None]:
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() if (ROOT / name).exists() else None for name in SUBJECTS}


def main() -> int:
    """Run the selected stand-in cases within one 120-second budget and retain failures as evidence."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", choices=list(CASES) + list(MIGRATION_CASES), action="append")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    before = fingerprints()
    started = time.monotonic()
    results: list[dict[str, Any]] = []
    cases = CASES | MIGRATION_CASES
    head = run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, timeout=10).stdout.strip()
    status = run_command("git", ["status", "--porcelain", "--", *SUBJECTS], cwd=ROOT, timeout=10)
    selected = list(args.case or cases)
    try:
        for name in selected:
            case = Scenario(args.output / name, ROOT, min(started + 120, time.monotonic() + 30))
            error = None
            try:
                if time.monotonic() >= started + 120:
                    raise TimeoutError("migration suite exceeded its 120-second deadline")
                cases[name](case)
            except (OSError, RuntimeError, AssertionError, TimeoutExpired) as failure:
                error = f"{type(failure).__name__}: {failure}"
            finally:
                if error is None and sys.exception() is not None:
                    error = f"unhandled {type(sys.exception()).__name__}"
                results.append(
                    {
                        "case": name,
                        "passed": error is None and bool(case.assertions) and all(item["passed"] for item in case.assertions),
                        "assertions": case.assertions,
                        "commands": case.commands,
                        "error": error,
                    }
                )
    finally:
        unchanged = before == fingerprints()
        report = {
            "feature": "synthetic catalog migration and recovery",
            "timestamp": datetime.now(UTC).isoformat(),
            "head": head,
            "worktree_changes": status.stdout if status.returncode == 0 else "unavailable",
            "environment": sys.version,
            "subject_hashes": before,
            "subject_unchanged": unchanged,
            "mode": "selected" if args.case else "full",
            "command": [sys.executable, "-m", "scripts.lakehouse.run_catalog_migration_e2e", *sys.argv[1:]],
            "seed": "2468abcd",
            "cases": results,
            "passed": unchanged and len(results) == len(selected) and all(item["passed"] for item in results),
            "elapsed_seconds": time.monotonic() - started,
            "reset": "Use a new --output directory. Inputs and object versions are synthetic and recreated for each case.",
            "limits": [
                "No network, AWS, Docker or native Polaris",
                "Injected authenticated REST and versioned object stand-ins",
                "Authority, live fencing and native quiescence are not proven",
            ],
            "cleanup": "done: synchronous children finished and no services started",
        }
        (args.output / "retention.json").write_text(
            json.dumps(
                {
                    "class": 5,
                    "purpose": "catalog migration regression evidence",
                    "created": report["timestamp"],
                    "end_condition": "A newer accepted run replaces it",
                }
            )
            + "\n",
            encoding="utf-8",
        )
        (args.output / "report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        sys.stdout.write(json.dumps({"passed": report["passed"], "cases": len(results), "report": str(args.output / "report.json")}) + "\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
