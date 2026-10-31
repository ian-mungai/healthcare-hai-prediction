"""Run the lean broker target offline: the ECS adapter cases, then a DAG task through the HTTP broker to the fake ECS client.

No AWS call, no Docker and no credential is used. The artifact goes to ``data/e2e/ecs_broker/<timestamp>.json`` (ignored by Git).
Run from the repository root: ``.venv/bin/python -m scripts.orchestration.run_ecs_broker_e2e [--only adapter|broker|chain] [--case NAME]``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import traceback
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from scripts.orchestration import airflow_mount_cases, ecs_adapter_cases, ecs_broker_cases, ecs_chain_cases, layer_walk_cases, override_docs_cases
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SOURCES = (
    "orchestration/ecs_adapter.py",
    "dags/bronze_chain_dag.py",
    "scripts/orchestration/ecs_chain_cases.py",
    "scripts/orchestration/ecs_broker.py",
    "config/orchestration/ecs_broker_jobs.json",
    "config/orchestration/ecs_broker_jobs.schema.json",
    "data_contracts/orchestration/ecs_ledger_line.schema.json",
    "scripts/orchestration/ecs_broker_fake.py",
    "scripts/orchestration/ecs_adapter_cases.py",
    "scripts/orchestration/airflow_mount_cases.py",
    "scripts/orchestration/override_docs_cases.py",
    "scripts/orchestration/layer_walk_cases.py",
    "scripts/orchestration/job_overrides.py",
    "scripts/orchestration/airflow_import_probe.py",
    "infra/services/airflow.tf",
    "scripts/orchestration/run_ecs_broker_e2e.py",
)


def suites() -> dict[str, dict[str, Callable[[], None]]]:
    """Every suite, imported without a guard: a suite that cannot be imported stops the run with its error."""
    return {
        "adapter": ecs_adapter_cases.CASES,
        "broker": ecs_broker_cases.CASES,
        "chain": ecs_chain_cases.CASES,
        "airflow": airflow_mount_cases.CASES,
        "overrides": override_docs_cases.CASES,
        "walk": layer_walk_cases.CASES,
    }


def revision() -> str:
    return run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, timeout=10).stdout.strip() or "unknown"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--only", choices=("adapter", "broker", "chain", "airflow", "overrides", "walk"))
    parser.add_argument("--case")
    args = parser.parse_args()
    results = []
    for suite, cases in suites().items():
        if args.only and args.only != suite:
            continue
        for name, case in cases.items():
            if args.case and args.case != name:
                continue
            try:
                case()
                results.append({"suite": suite, "case": name, "outcome": "pass"})
            except Exception as error:
                detail = "".join(traceback.format_exception_only(type(error), error)).strip()
                results.append({"suite": suite, "case": name, "outcome": "fail", "detail": detail})
    failed = [item for item in results if item["outcome"] == "fail"]
    started = datetime.now(UTC)
    artifact = {
        "feature": "lean broker ECS target",
        "at": started.isoformat(),
        "revision": revision(),
        "mode": "full" if not (args.only or args.case) else "iteration",
        "substitutions": ["fake ECS, Logs and S3 clients", "airflow.sdk stub", "virtual cluster time"],
        "sources": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES if (ROOT / name).exists()},
        "results": results,
    }
    folder = ROOT / "data" / "e2e" / "ecs_broker"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{started.strftime('%Y%m%dT%H%M%SZ')}.json").write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    for item in failed:
        sys.stdout.write(f"FAIL {item['suite']}/{item['case']}: {item['detail']}\n")
    sys.stdout.write(f"{len(results) - len(failed)} passed, {len(failed)} failed\n")
    return 1 if failed or not results else 0


if __name__ == "__main__":
    sys.exit(main())
