"""Check that the lineage completion records still build their events and that no lineage spool code remains, without services or native provider packages."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.util
import inspect
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
RUN = "20261017T010000Z-1234abcd"
IDENTITY = {"dag_id": "silver_transform_dag", "task_id": "silver_build", "dag_run_id": "manual__2026-10-17T01:00:00+00:00"}
ENVELOPE = {
    "version": 1,
    "inputs": [{"namespace": "file", "name": "fixture/bronze.duckdb"}],
    "outputs": [{"namespace": "file", "name": "fixture/silver.duckdb"}],
}


def require(value: Any, message: str) -> None:
    if not value:
        raise AssertionError(message)


def record_fixture() -> dict[str, Any]:
    return {
        "request_version": 2,
        "submission_id": f"{RUN}:silver_build:fixture:1",
        "template": "silver_build",
        "run_id": RUN,
        "mode": "fixture",
        "attempt": 1,
        **IDENTITY,
        "lineage": copy.deepcopy(ENVELOPE),
        "exit_code": 0,
        "written_at": "2026-10-17T01:02:00+00:00",
        "launcher": {"id": "at-synthetic", "n": 1, "ended_at": "2026-10-17T01:01:00+00:00"},
        "ledger": [
            {
                "event": "intent",
                "at": "2026-10-17T01:00:00+00:00",
                "request_version": 2,
                "template": "silver_build",
                "run_id": RUN,
                "mode": "fixture",
                "attempt": 1,
                **IDENTITY,
                "lineage": copy.deepcopy(ENVELOPE),
            }
        ],
    }


def no_spool() -> None:
    for name in ("provider_events", "provider_transport"):
        require(importlib.util.find_spec(f"services.lineage.{name}") is None, f"the lineage spool module {name} is still present")
    from orchestration import lineage

    require(list(inspect.signature(lineage.build_events).parameters) == ["record"], "build_events still takes a native provider run")
    require(not hasattr(lineage.Outbox, "provider_run") and not hasattr(lineage.Outbox, "record_cycle"), "the outbox still resolves native provider runs")
    require("require_provider" not in inspect.signature(lineage.sweep).parameters, "sweep still defers records for native provider callbacks")


def record_events() -> None:
    from orchestration.lineage import build_events

    events = build_events(record_fixture())
    require(
        len(events) == 2 and {event["eventType"] for event in events} == {"START", "COMPLETE"},
        "a valid completion record did not yield its START and COMPLETE events",
    )


CASES = {"no_spool": no_spool, "record_events": record_events}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=sorted(CASES))
    args = parser.parse_args()
    output = ROOT / "data/e2e/lineage_completion" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    output.mkdir(parents=True)
    sources = [ROOT / path for path in ("scripts/orchestration/run_lineage_completion_e2e.py", "orchestration/lineage.py")]
    before = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    results = []
    for name in args.case or CASES:
        try:
            CASES[name]()
        except (AssertionError, OSError, ValueError, RuntimeError, AttributeError, TypeError) as error:
            results.append({"case": name, "pass": False, "error": f"{type(error).__name__}: {error}"})
        else:
            results.append({"case": name, "pass": True})
    require(before == {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}, "source changed during check")
    result = {
        "created_at": datetime.now(UTC).isoformat(),
        "command": [sys.executable, "-m", "scripts.orchestration.run_lineage_completion_e2e", *sys.argv[1:]],
        "environment": {"python": sys.version.split()[0], "platform": sys.platform},
        "boundary": "the repository source and synthetic completion records; no native provider or daemon",
        "cases": results,
        "pass": all(row["pass"] for row in results),
        "source_sha256": before,
        "cleanup": "no services started",
    }
    (output / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    (output / "retention.json").write_text(
        json.dumps(
            {"class": 4, "purpose": "lineage completion record evidence", "created": datetime.now(UTC).isoformat(), "end_condition": "no record cites this run"}
        )
        + "\n"
    )
    sys.stdout.write(json.dumps({"pass": result["pass"], "report": str(output / "report.json"), "cases": results}) + "\n")
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
