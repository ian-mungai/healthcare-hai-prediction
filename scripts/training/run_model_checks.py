"""Run bounded synthetic model-caller cases with explicit native-library substitutions."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.training.model_cases import scenarios, source_paths


def main() -> int:
    """Write a repeatable local report and fail visibly on a case or source-identity change."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    args.output.mkdir(parents=True, exist_ok=True)
    started = datetime.now(UTC).isoformat()
    (args.output / "retention.json").write_text(
        json.dumps(
            {"class": 4, "purpose": "Synthetic model-caller verification", "created": started, "end_condition": "No review or handoff cites this report"}
        )
        + "\n"
    )
    hashes = {path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in source_paths(root) if path.is_file()}
    results: list[dict[str, Any]] = []
    try:
        cases = scenarios()
    except ModuleNotFoundError as error:
        cases = []
        results.append({"name": "model_implementation_present", "verdict": "fail", "error": f"missing implementation module: {error.name}"})
    if args.case:
        cases = [(name, call) for name, call in cases if name == args.case]
        if not cases:
            results.append({"name": args.case, "verdict": "fail", "error": "named case is unavailable"})
    if not cases and not results:
        results.append({"name": "case_inventory", "verdict": "fail", "error": "model caller case inventory is empty"})
    for name, call in cases:
        start = time.monotonic()
        try:
            call()
        except (AssertionError, ValueError, RuntimeError, ImportError, ArithmeticError, KeyError, TypeError, AttributeError) as error:
            results.append({"name": name, "verdict": "fail", "error": f"{type(error).__name__}: {error}"})
        else:
            results.append({"name": name, "verdict": "pass"})
        results[-1]["seconds"] = time.monotonic() - start
    stable = hashes == {path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in source_paths(root) if path.is_file()}
    report = {
        "schema": 1,
        "started": started,
        "classification": "Synthetic",
        "mode": "standins",
        "source_sha256": hashes,
        "source_unchanged": stable,
        "cases": results,
        "limits": "Native statistics, convergence, SHAP, serialization and launcher integration are unverified. External libraries are explicit stand-ins.",
        "cleanup": "done",
    }
    (args.output / "report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    failed = sum(row["verdict"] != "pass" for row in results)
    sys.stdout.write(json.dumps({"passed": len(results) - failed, "failed": failed, "source_unchanged": stable}) + "\n")
    return int(bool(failed) or not stable)


if __name__ == "__main__":
    raise SystemExit(main())
