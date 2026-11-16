"""Write the container overrides for a hand-started job task from the job catalog, so a runbook never carries its own list of variables.

    .venv/bin/python -m scripts.orchestration.job_overrides --job silver --run <RUN_ID> --code-run <RUN_ID> \\
        --code-sha256 <SHA256> --polaris-uri <URI> [--dry-run]

``config/orchestration/ecs_broker_jobs.json`` names the containers of each job and the empty variables that each one needs. This tool fills every one of them
from an argument and refuses by name when a needed value is missing, so a task never starts with a variable empty. The preflight reads the same catalog.
Terraform tests compare the table with the registered task definitions, so the output also equals what the task definition leaves empty.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from scripts.features import feature_sets

ROOT = Path(__file__).resolve().parents[2]
TABLE = ROOT / "config/orchestration/ecs_broker_jobs.json"
DRY_RUN_VARIABLE = "LAYER_DRY_RUN"
LAYERS = ("silver", "gold", "features")
# A features task holds its release variables empty unless the launch is a release (scripts/lakehouse/layer_run.py): a build names no phase, no model set and
# no run to release. A release names all three. A reload (host_session start) names the phase and the model set, never a run: the reload picks its own run.
# The metadata job reads FEATURES_RUN too and always needs it.
RELEASE_VARIABLES = ("LAYER_PHASE", "RELEASE_MODEL", "FEATURES_RUN")
RELEASE_PHASE = "release"
RELOAD_PHASE = "reload"


class OverrideError(ValueError):
    """A variable that the job needs has no value."""


def build(job: str, values: dict[str, str], dry_run: bool = False, table: Path = TABLE) -> dict[str, Any]:
    """The ``containerOverrides`` document for one job; ``values`` maps variable names to values."""
    jobs = json.loads(table.read_text())["jobs"]
    if job not in jobs:
        raise OverrideError(f"{job} is not a job of the job catalog: {sorted(jobs)}")
    if jobs[job].get("needs_serving_parameters"):
        raise OverrideError(
            f"the {job} job starts only through the direct launch of the promotion chain, which derives its request: no owner command builds it"
        )
    if dry_run and job not in LAYERS:
        raise OverrideError("only the silver, gold and features jobs have a dry run")
    values = {**values, feature_sets.VARIABLE: values.get(feature_sets.VARIABLE) or feature_sets.SINGLE_PATH}  # no set given is the single path
    chosen = values[feature_sets.VARIABLE]
    if chosen != feature_sets.SINGLE_PATH and chosen not in feature_sets.SETS:
        raise OverrideError(f"{feature_sets.VARIABLE} {chosen!r} is not one of {feature_sets.SINGLE_PATH}, {', '.join(feature_sets.SETS)}")
    phase = values.get("LAYER_PHASE") if job == "features" else None
    needed = {RELEASE_PHASE: RELEASE_VARIABLES, RELOAD_PHASE: ("LAYER_PHASE", "RELEASE_MODEL")}.get(phase or "", ())
    if job == "features" and values.get("LAYER_PHASE") not in (None, "", RELEASE_PHASE, RELOAD_PHASE, *jobs[job]["containers"]):
        raise OverrideError(f"LAYER_PHASE {values['LAYER_PHASE']!r} is not a phase of the features job")
    overrides = []
    for container, names in jobs[job]["containers"].items():
        environment = []
        for name in names:
            if not values.get(name) and not (job == "features" and name in RELEASE_VARIABLES and name not in needed):
                raise OverrideError(f"{name} has no value: the {container} container of the {job} job needs it")
            environment.append({"name": name, "value": values.get(name, "")})
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
    parser.add_argument("--features-run", default="", help="FEATURES_RUN: the features run whose chain the metadata job ingests (a run ID, never latest)")
    parser.add_argument(
        "--layer-phase",
        default="",
        help="LAYER_PHASE: release or reload runs only the feast container of the features job, as a release or reload; empty runs the build phases",
    )
    parser.add_argument("--release-model", default="", help="RELEASE_MODEL: the model set whose held online load the release releases (m1, m2 or m4)")
    parser.add_argument("--feature-set", default="", help="FEATURE_SET: all (the single path, the default), base or m1 to m6 for the features job")
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
        "FEATURES_RUN": args.features_run,
        "FEATURE_SET": args.feature_set,
        "LAYER_PHASE": args.layer_phase,
        "RELEASE_MODEL": args.release_model,
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
