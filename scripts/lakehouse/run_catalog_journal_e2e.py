"""Save repeatable evidence for three file-backed catalog recovery cases, without services."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse.catalog_migration_cases import CASES, Scenario
from scripts.process import TimeoutExpired, run_command

REPO = Path(__file__).resolve().parents[2]


def fingerprints(subject: Path) -> dict[str, str | None]:
    """Bind the result to the subject and the independently authored scenarios."""
    paths = [
        subject / "scripts/lakehouse/catalog_journal.py",
        Path(__file__),
        Path(__file__).with_name("catalog_migration_cases.py"),
        REPO / "scripts/process.py",
    ]
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None for path in paths}


def main() -> int:
    """Run the bounded stand-in cases and save a report for both passing and failing runs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--subject-root", type=Path, default=REPO)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    before = fingerprints(args.subject_root)
    head = run_command("git", ["rev-parse", "HEAD"], cwd=REPO, timeout=10).stdout.strip()
    subject_status = run_command(
        "git",
        [
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--ignored=matching",
            "--",
            str(args.subject_root.resolve() / "scripts/lakehouse/catalog_journal.py"),
        ],
        cwd=REPO,
        timeout=10,
    )
    results: list[dict[str, Any]] = []
    started = time.monotonic()
    for name, scenario in CASES.items():
        case = Scenario(output / name, args.subject_root, min(started + 60, time.monotonic() + 20))
        error = None
        try:
            scenario(case)
        except (OSError, RuntimeError, ValueError, KeyError, TimeoutExpired) as failure:
            error = f"{type(failure).__name__}: {failure}"
        results.append(
            {
                "case": name,
                "passed": error is None and all(item["passed"] for item in case.assertions),
                "assertions": case.assertions,
                "commands": case.commands,
                "error": error,
            }
        )
    unchanged = before == fingerprints(args.subject_root)
    report = {
        "feature": "catalog recovery stand-in",
        "timestamp": datetime.now(UTC).isoformat(),
        "head": head,
        "environment": sys.version,
        "subject_hashes": before,
        "subject_unchanged": unchanged,
        "uncommitted_subject": bool(subject_status.stdout) if subject_status.returncode == 0 else None,
        "subject_git_status": subject_status.stdout if subject_status.returncode == 0 else None,
        "seed": "2468abcd",
        "mode": "full",
        "command": [sys.executable, "-m", "scripts.lakehouse.run_catalog_journal_e2e", *sys.argv[1:]],
        "cases": results,
        "elapsed_seconds": time.monotonic() - started,
        "passed": unchanged and all(case["passed"] for case in results),
        "limits": [
            "File-backed catalog and local immutable journal only",
            "No Polaris REST, AWS, Docker, launcher hooks or native writer proof",
            "Catalog provenance and quiescence are supplied by the stand-in adapter",
        ],
        "reset": "Use a new --output directory. Each seed resets only its owned synthetic driver directory.",
        "cleanup": "done: synchronous commands finished; the killed driver has no descendants; evidence retained",
    }
    (output / "retention.json").write_text(
        json.dumps(
            {"class": 5, "purpose": "catalog recovery regression baseline", "created": report["timestamp"], "end_condition": "A newer accepted run replaces it"}
        )
        + "\n",
        encoding="utf-8",
    )
    (output / "report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    sys.stdout.write(json.dumps({"passed": report["passed"], "cases": len(results), "report": str(output / "report.json")}) + "\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
