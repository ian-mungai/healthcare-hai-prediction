"""Run the synthetic E2E cases of the Streamlit page and save a repeatable local report.

The client cases always run. The page cases run when Streamlit's own test harness is installed. Without it the page is unchecked: the report's verdict is
``incomplete`` instead of ``pass`` and the runner says so on both output streams. It still exits zero so that a host without Streamlit can run the client
cases, and ``--require-app`` turns the missing harness into a failure, which is how the heavy run in the page image must call it.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from scripts.features.feast_harness import run_cases
from scripts.streamlit_app import client_cases


def harness_available() -> bool:
    try:
        return importlib.util.find_spec("streamlit.testing.v1") is not None
    except ModuleNotFoundError:
        return False


def main() -> int:
    """Write the report, name every failed check and fail the process when one fails."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", action="append", default=[])
    parser.add_argument(
        "--require-app", action="store_true", help="fail when Streamlit's test harness is missing instead of reporting the page cases as blocked"
    )
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    started = datetime.now(UTC).isoformat()
    (args.output / "retention.json").write_text(
        json.dumps(
            {"class": 4, "purpose": "Synthetic Streamlit page verification", "created": started, "end_condition": "No review or handoff cites this report"}
        )
        + "\n"
    )
    cases = dict(client_cases.CASES)
    app_state = "blocked: streamlit is not installed in this environment"
    if harness_available():
        from scripts.streamlit_app import app_cases

        cases |= app_cases.CASES
        app_state = "ran"
    report = run_cases(cases, args.case)
    page_run = app_state == "ran"
    report |= {
        "schema": 1,
        "started": started,
        "classification": "Synthetic",
        "mode": "standins",
        "page_cases": "ran" if page_run else "NOT RUN",
        "page_cases_reason": "" if page_run else app_state,
        "limits": (
            "The stand-in API speaks real HTTP on the loopback address but is not the model API. "
            "The page cases need Streamlit's test harness, which runs a script in the process and opens no browser. No real API, key or AWS call is used."
        ),
    }
    if not page_run:
        report["verdict"] = "fail" if args.require_app else ("incomplete" if report["verdict"] == "pass" else report["verdict"])
    (args.output / "report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    for result in report["cases"]:
        for name in result["failed"]:
            sys.stderr.write(f"FAIL {result['case']}: {name}\n")
    passed = sum(item["verdict"] == "pass" for item in report["cases"])
    if not page_run:
        sys.stderr.write(
            "PAGE CASES NOT RUN: Streamlit is not installed here, so the page itself is unchecked. Run this with --require-app in the page image.\n"
        )
    sys.stdout.write(
        json.dumps({"passed": passed, "failed": len(report["cases"]) - passed, "page_cases": report["page_cases"], "verdict": report["verdict"]}) + "\n"
    )
    return int(report["verdict"] not in ("pass", "incomplete"))


if __name__ == "__main__":
    raise SystemExit(main())
