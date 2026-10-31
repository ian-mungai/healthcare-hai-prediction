"""Write the container overrides for a hand-started job task from the broker job table, so a runbook never carries its own list of variables.

    .venv/bin/python -m scripts.orchestration.job_overrides --job silver --run <RUN_ID> --code-run <RUN_ID> \\
        --code-sha256 <SHA256> --polaris-uri <URI> [--dry-run]

``config/orchestration/ecs_broker_jobs.json`` names the containers of each job and the empty variables that each one needs. This tool fills every one of them
from an argument and refuses by name when a needed value is missing, so a task never starts with a variable empty. The broker uses the same table. Terraform
tests compare the table with the registered task definitions, so the output also equals what the task definition leaves empty.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
TABLE = ROOT / "config/orchestration/ecs_broker_jobs.json"
DRY_RUN_VARIABLE = "LAYER_DRY_RUN"
LAYERS = ("silver", "gold", "features")


class OverrideError(ValueError):
    """A variable that the job needs has no value."""


def build(job: str, values: dict[str, str], dry_run: bool = False, table: Path = TABLE) -> dict[str, Any]:
    """The ``containerOverrides`` document for one job; ``values`` maps variable names to values."""
    jobs = json.loads(table.read_text())["jobs"]
    if job not in jobs:
        raise OverrideError(f"{job} is not a job of the broker table: {sorted(jobs)}")
    if dry_run and job not in LAYERS:
        raise OverrideError("only the silver, gold and features jobs have a dry run")
    overrides = []
    for container, names in jobs[job]["containers"].items():
        environment = []
        for name in names:
            if not values.get(name):
                raise OverrideError(f"{name} has no value: the {container} container of the {job} job needs it")
            environment.append({"name": name, "value": values[name]})
        if dry_run:
            environment.append({"name": DRY_RUN_VARIABLE, "value": "1"})
        overrides.append({"name": container, "environment": environment})
    return {"containerOverrides": overrides}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--job", required=True)
    parser.add_argument("--run", default="", help="RUN_ID: the run that this task's output belongs to")
    parser.add_argument("--code-run", default="", help="CODE_RUN_ID: the run whose staged code the task fetches (the run itself for a standalone run)")
    parser.add_argument("--code-sha256", default="", help="CODE_SHA256: the digest in code.sha256 beside the staged archive")
    parser.add_argument("--polaris-uri", default="", help="POLARIS_URI: http://<host private address>:8181/api/catalog")
    parser.add_argument("--input-run", default="", help="INPUT_RUN_ID: the committed silver run for gold, the committed gold run for features")
    parser.add_argument("--mlflow-uri", default="", help="MLFLOW_TRACKING_URI: http://<host private address>:5100")
    parser.add_argument("--training-parameters", default="", help="TRAINING_PARAMETERS: the parameters of the run as one JSON string")
    parser.add_argument("--dry-run", action="store_true", help="set LAYER_DRY_RUN=1 on every container of a layer job")
    args = parser.parse_args(argv)
    values = {
        "RUN_ID": args.run,
        "CODE_RUN_ID": args.code_run,
        "CODE_SHA256": args.code_sha256,
        "POLARIS_URI": args.polaris_uri,
        "INPUT_RUN_ID": args.input_run,
        "MLFLOW_TRACKING_URI": args.mlflow_uri,
        "TRAINING_PARAMETERS": args.training_parameters,
    }
    try:
        document = build(args.job, values, args.dry_run)
    except OverrideError as error:
        sys.stderr.write(f"job_overrides: {error}\n")
        return 2
    sys.stdout.write(json.dumps(document, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
