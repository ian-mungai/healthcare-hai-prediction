"""Record explicit static or in-process placement proof without AWS or Docker access.

Run with --proof S for pure contracts or --proof D for real broker and launcher handlers with synthetic transports.
Native worker, Terraform and actual AWS proofs remain separate lead-run gates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.orchestration.ecs_target_cases import contract_cases, selector_cases, sizing_cases

ROOT = Path(__file__).resolve().parents[2]
SOURCES = (
    "orchestration/ecs_target.py",
    "orchestration/launcher.py",
    "orchestration/interface.py",
    "orchestration/client.py",
    "orchestration/fake.py",
    "scripts/orchestration/broker.py",
    "scripts/orchestration/ecs_target_cases.py",
    "scripts/orchestration/ecs_submission_cases.py",
    "scripts/orchestration/ecs_fake_api.py",
    "scripts/orchestration/run_ecs_target_e2e.py",
    "scripts/orchestration/run_contracts_e2e.py",
    "config/orchestration/ecs_jobs.json",
    "config/orchestration/ecs_jobs.schema.json",
    "data_contracts/orchestration/execution.schema.json",
    "data_contracts/orchestration/submission_request.schema.json",
    "data_contracts/orchestration/ledger_line.schema.json",
    "data_contracts/orchestration/dbt_lineage_record.schema.json",
    "data_contracts/orchestration/snapshot_record.schema.json",
    "data_contracts/orchestration/input_snapshot_record.schema.json",
    "data_contracts/orchestration/examples/submission_requests.json",
    "data_contracts/orchestration/examples/ledger_states.json",
    "config/quality/docker_calls_reviewed.json",
)


def hashes() -> dict[str, str]:
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES}


def main() -> int:
    """Run the requested proof level and save failures with source identity and cleanup status."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--proof", choices=("S", "D", "T", "A"), required=True)
    parser.add_argument("--lane", choices=("A",), default="A")
    parser.add_argument("--case", action="append", choices=("selector", "sizing", "contracts", "submission"))
    args = parser.parse_args()
    if args.proof not in ("S", "D"):
        parser.error("this increment supplies S and in-process D only. T and A require the coordinating agent's integrated runtime and cloud inputs")
    selected = args.case or (["selector", "sizing", "contracts"] if args.proof == "S" else ["submission"])
    if (args.proof == "S" and "submission" in selected) or (args.proof == "D" and selected != ["submission"]):
        parser.error("the case selection does not match the requested proof level")
    started = datetime.now(UTC)
    folder = ROOT / "data/e2e/ecs_target" / started.strftime("%Y%m%dT%H%M%S_%fZ")
    folder.mkdir(parents=True)
    before = hashes()
    checks: dict[str, Any] = {}
    try:
        for name in selected:
            if name == "selector":
                checks[name] = selector_cases()
            elif name == "sizing":
                checks[name] = sizing_cases()
            elif name == "contracts":
                checks[name] = contract_cases(ROOT)
            else:
                from scripts.orchestration.ecs_submission_cases import submission_cases

                checks[name] = submission_cases(ROOT, folder / "submission")
    except Exception as error:
        checks["runner"] = {"completed": False, "exception_type": type(error).__name__}
    failed = [f"{group}.{name}" for group, values in checks.items() for name, value in values.items() if value is False]
    if before != hashes():
        failed.append("source_changed")
    report = {
        "started_utc": started.isoformat(),
        "proof": args.proof,
        "scope": "pure contracts" if args.proof == "S" else "in-process broker and launcher handlers",
        "command": [".venv/bin/python", "-m", "scripts.orchestration.run_ecs_target_e2e", *sys.argv[1:]],
        "source_sha256": before,
        "classification": "Synthetic",
        "checks": checks,
        "failed": failed,
        "cleanup": "unproven" if "runner" in checks else "done",
        "limits": ["No AWS, Docker, native worker or real data execution", "The cloud rollout switch remains disabled"],
    }
    (folder / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    (folder / "retention.json").write_text(
        json.dumps(
            {"class": 4, "purpose": report["scope"], "created": started.isoformat(), "end_condition": "Review accepted and no record cites this run"}, indent=2
        )
        + "\n"
    )
    sys.stdout.write(json.dumps({"report": str(folder / "report.json"), "failed": failed}) + "\n")
    return int(bool(failed))


if __name__ == "__main__":
    sys.exit(main())
