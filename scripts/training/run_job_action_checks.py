"""Run the synthetic E2E cases of the training job's data actions and save a repeatable local report."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from scripts.training import job_action_cases


def main() -> int:
    """Write the report, name every failed case and fail the process when one fails."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    started = datetime.now(UTC).isoformat()
    (args.output / "retention.json").write_text(
        json.dumps(
            {"class": 4, "purpose": "Synthetic training job action verification", "created": started, "end_condition": "No review or handoff cites this report"}
        )
        + "\n"
    )
    results = job_action_cases.run(args.case)
    report = {
        "schema": 1,
        "started": started,
        "classification": "Synthetic",
        "mode": "standins",
        "cases": results,
        "limits": (
            "Native statistics, MLflow server behavior, DVC remotes and container mounts are unverified. "
            "Parquet, model libraries and the tracking client are explicit stand-ins."
        ),
        "cleanup": "done",
    }
    (args.output / "report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    failed = [result for result in results if not result["passed"]]
    for result in failed:
        sys.stderr.write(f"FAIL {result['name']}: {result['detail']}\n")
    sys.stdout.write(json.dumps({"passed": len(results) - len(failed), "failed": len(failed)}) + "\n")
    return int(bool(failed))


if __name__ == "__main__":
    raise SystemExit(main())
