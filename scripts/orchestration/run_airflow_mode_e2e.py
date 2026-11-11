"""Run the Airflow launch mode cases offline: a DAG step reaches the direct ECS launch library, chosen by an Airflow Variable.

No AWS call, no Docker and no credential is used. Each run writes ``data/e2e/airflow_launch_mode/<timestamp>.json`` (ignored by Git) with the revision,
the changed files, the substitutions and one line per case. Run from the repository root:
``.venv/bin/python -m scripts.orchestration.run_airflow_mode_e2e [--case NAME]``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path

from scripts.orchestration import airflow_mode_cases
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SOURCES = (
    "orchestration/airflow_steps.py",
    "dags/launch_settle_dag.py",
    "orchestration/dag_runtime.py",
    "orchestration/ecs_preflight.py",
    "config/orchestration/host_jobs.json",
    "infra/jobs/tests/host_definitions.json",
    "infra/jobs/tests/publisher_host_definition.json",
    "infra/services/airflow.tf",
    "scripts/orchestration/airflow_mode_cases.py",
    "scripts/orchestration/run_airflow_mode_e2e.py",
)
SUBSTITUTIONS = [
    "fake ECS, Logs and write-once S3 clients, and an in-memory bucket for the staged inputs",
    "virtual clock (waits of minutes take no real time)",
    "a stand-in of the Airflow SDK and of the Amazon provider 9.36.0 operator, not the real packages (rerun the cases in the Airflow image)",
    "the shipped DAG files run task by task in order, not under a scheduler",
]


def git(*arguments: str) -> str:
    return run_command("git", list(arguments), cwd=ROOT, timeout=30).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case")
    args = parser.parse_args()
    results = []
    for name, case in airflow_mode_cases.CASES.items():
        if args.case and args.case != name:
            continue
        try:
            case()
            results.append({"case": name, "outcome": "pass"})
        except Exception as error:
            detail = "".join(traceback.format_exception_only(type(error), error)).strip()
            results.append({"case": name, "outcome": "fail", "detail": detail})
    failed = [item for item in results if item["outcome"] == "fail"]
    started = datetime.now(UTC)
    artifact = {
        "feature": "Airflow launch mode",
        "at": started.isoformat(),
        "revision": git("rev-parse", "HEAD") or "unknown",
        "uncommitted": git("status", "--short").splitlines(),
        "mode": "iteration" if args.case else "full",
        "command": " ".join(["python", "-m", "scripts.orchestration.run_airflow_mode_e2e", *sys.argv[1:]]),
        "substitutions": SUBSTITUTIONS,
        "sources": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES if (ROOT / name).exists()},
        "results": results,
    }
    folder = ROOT / "data" / "e2e" / "airflow_launch_mode"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{started.strftime('%Y%m%dT%H%M%SZ')}.json").write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    for item in failed:
        sys.stdout.write(f"FAIL {item['case']}: {item['detail']}\n")
    sys.stdout.write(f"{len(results) - len(failed)} passed, {len(failed)} failed\n")
    return 1 if failed or not results else 0


if __name__ == "__main__":
    sys.exit(main())
