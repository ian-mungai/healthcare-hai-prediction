"""Run the synthetic E2E cases of the model API and save a repeatable local report."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from scripts.features.feast_harness import run_cases
from scripts.serving import serving_cases


def main() -> int:
    """Write the report, name every failed check and fail the process when one fails."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", action="append", default=[])
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    started = datetime.now(UTC).isoformat()
    (args.output / "retention.json").write_text(
        json.dumps({"class": 4, "purpose": "Synthetic model API verification", "created": started, "end_condition": "No review or handoff cites this report"})
        + "\n"
    )
    report = run_cases(serving_cases.CASES, args.case)
    report |= {
        "schema": 1,
        "started": started,
        "classification": "Synthetic",
        "mode": "standins",
        "limits": (
            "FastAPI, the MLflow model loader, the PyIceberg sink and the S3 manifest store run only in the serving image and are not exercised. "
            "The model, the registry and the sink are stand-ins. Canary proof is replayed requests, not live traffic."
        ),
    }
    (args.output / "report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    for result in report["cases"]:
        for name in result["failed"]:
            sys.stderr.write(f"FAIL {result['case']}: {name}\n")
    passed = sum(item["verdict"] == "pass" for item in report["cases"])
    sys.stdout.write(json.dumps({"passed": passed, "failed": len(report["cases"]) - passed}) + "\n")
    return int(report["verdict"] != "pass")


if __name__ == "__main__":
    raise SystemExit(main())
