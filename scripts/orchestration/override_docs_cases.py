"""Check the override generator against the variables that the real tasks need.

A hand-started task leaves its variables empty until the run-task call fills them. These cases build the overrides of every job of the broker job table with
sample values and require that they fill exactly the variables that the table gives each container, and that the generator refuses a missing value by name.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from pathlib import Path

from scripts.orchestration import job_overrides
from scripts.orchestration.ecs_adapter_cases import require
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
RUN = "20261030T100000Z-abcdef01"
VALUES = {
    "RUN_ID": RUN,
    "CODE_RUN_ID": RUN,
    "INPUT_RUN_ID": "20261030T090000Z-0badcafe",
    "CODE_SHA256": "ab" * 32,
    "POLARIS_URI": "http://10.0.0.5:8181/api/catalog",
    "MLFLOW_TRACKING_URI": "http://10.0.0.5:5100",
    "TRAINING_PARAMETERS": '{"mlflow": "aws"}',
    "SERVING_REQUEST": '{"request": "synthetic"}',
    "BRONZE_DRY_RUN": "1",
    "BRONZE_INVENTORY_SHA256": "cd" * 32,
    "BRONZE_REQUEST_SHA256": "ef" * 32,
    "BRONZE_TABLES": '["synthetic_table"]',
    "BRONZE_TARGET": "bronze_e2e",
    "HAI_DEADLINE_UTC": "2026-10-30T12:00:00Z",
}


def job_table() -> dict:
    return json.loads((ROOT / "config/orchestration/ecs_broker_jobs.json").read_text())["jobs"]


def the_generator_fills_every_variable_of_every_job() -> None:
    check_overrides(job_table())


def a_job_variable_without_a_sample_fails_the_check() -> None:
    table = job_table()
    container = next(iter(table["silver"]["containers"]))
    table["silver"]["containers"][container] = [*table["silver"]["containers"][container], "REQUIRED_NEW_VALUE"]
    try:
        check_overrides(table)
    except AssertionError as error:
        require("REQUIRED_NEW_VALUE" in str(error), f"the refusal does not name the variable: {error}")
        return
    require(False, "a job that needs a variable without a sample value was skipped instead of failing the check")


def check_overrides(table: dict) -> None:
    for job, row in table.items():
        needed = {variable for variables in row["containers"].values() for variable in variables}
        unknown = needed - set(VALUES) - {job_overrides.DRY_RUN_VARIABLE}
        require(not unknown, f"the {job} job needs {sorted(unknown)}, which have no sample value here: add one so its overrides are checked")
        for dry_run in (False, True) if job in job_overrides.LAYERS else (False,):
            document = job_overrides.build(job, {name: VALUES[name] for name in needed if name in VALUES}, dry_run)
            built = {item["name"]: [pair["name"] for pair in item["environment"]] for item in document["containerOverrides"]}
            wanted = {container: [*variables, *([job_overrides.DRY_RUN_VARIABLE] if dry_run else [])] for container, variables in row["containers"].items()}
            require(built == wanted, f"the {job} overrides fill {built}, the task needs {wanted}")


def the_generator_refuses_a_missing_value_by_name() -> None:
    done = run_command(
        sys.executable,
        [
            "-m",
            "scripts.orchestration.job_overrides",
            "--job",
            "silver",
            "--run",
            RUN,
            "--code-sha256",
            "ab" * 32,
            "--polaris-uri",
            "http://10.0.0.5:8181/api/catalog",
        ],
        cwd=ROOT,
        timeout=60,
    )
    require(done.returncode == 2 and "CODE_RUN_ID" in done.stderr, f"{done.returncode}: {done.stderr.strip()[-200:]}")
    done = run_command(sys.executable, ["-m", "scripts.orchestration.job_overrides", "--job", "gold", "--run", RUN, "--dry-run"], cwd=ROOT, timeout=60)
    require(done.returncode == 2, "a gold dry run without its input run was accepted")


CASES: dict[str, Callable[[], None]] = {
    "the_generator_fills_every_variable_of_every_job": the_generator_fills_every_variable_of_every_job,
    "the_generator_refuses_a_missing_value_by_name": the_generator_refuses_a_missing_value_by_name,
    "a_job_variable_without_a_sample_fails_the_check": a_job_variable_without_a_sample_fails_the_check,
}
