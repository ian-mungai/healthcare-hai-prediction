"""Check the runbooks' override lists against the variables that the real tasks need.

A hand-started task leaves its variables empty until the run-task call fills them. A runbook that carries its own list can omit one, and then the task starts
with an empty value. These cases read the runbooks: no document may hold a hand-written override list, and every documented ``job_overrides`` command, with
sample values for its placeholders, must fill exactly the variables that the broker job table gives each container of its job.
"""

from __future__ import annotations

import json
import re
import shlex
import sys
from collections.abc import Callable
from pathlib import Path

from scripts.orchestration import job_overrides
from scripts.orchestration.ecs_adapter_cases import require
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
COMMAND = re.compile(r"(?m)^\s*\.venv/bin/python -m scripts\.orchestration\.job_overrides((?:\\\n|[^\n])*)")
RUN = "20261030T100000Z-abcdef01"
SAMPLES = {
    "--run": RUN,
    "--code-run": RUN,
    "--input-run": "20261030T090000Z-0badcafe",
    "--code-sha256": "ab" * 32,
    "--polaris-uri": "http://10.0.0.5:8181/api/catalog",
    "--mlflow-uri": "http://10.0.0.5:5100",
    "--training-parameters": '{"mlflow": "aws"}',
    "--job": "",
}


def documents() -> dict[str, str]:
    return {path.name: path.read_text() for path in sorted((ROOT / "docs").glob("*.md"))}


def documented_commands() -> list[tuple[str, list[str]]]:
    """Every ``job_overrides`` command in the runbooks as (document, arguments after the module name)."""
    found = []
    for name, text in documents().items():
        for match in COMMAND.finditer(text):
            found.append((name, shlex.split(match.group(1).replace("\\\n", " ").split(" > ")[0], comments=False)))
    return found


def filled(arguments: list[str]) -> tuple[str, dict[str, str], bool]:
    """The job, the values and the dry run flag of a documented command, with a sample for each placeholder."""
    job, values, dry_run = "", {}, False
    flags = {
        "--run": "RUN_ID",
        "--code-run": "CODE_RUN_ID",
        "--code-sha256": "CODE_SHA256",
        "--polaris-uri": "POLARIS_URI",
        "--input-run": "INPUT_RUN_ID",
        "--mlflow-uri": "MLFLOW_TRACKING_URI",
        "--training-parameters": "TRAINING_PARAMETERS",
    }
    index = 0
    while index < len(arguments):
        flag = arguments[index]
        if flag == "--dry-run":
            dry_run = True
            index += 1
            continue
        require(flag in SAMPLES, f"the runbook command uses the unknown option {flag}")
        require(index + 1 < len(arguments), f"{flag} has no value in the runbook")
        if flag == "--job":
            job = arguments[index + 1]
        else:
            values[flags[flag]] = SAMPLES[flag]
        index += 2
    return job, values, dry_run


def no_runbook_carries_its_own_override_list() -> None:
    for name, text in documents().items():
        require("containerOverrides" not in text, f"{name} holds a hand-written override list: use scripts.orchestration.job_overrides")
        if "--overrides" in text:
            require("job_overrides" in text, f"{name} passes --overrides without the generator")


def documented_commands_fill_every_variable_of_their_job() -> None:
    table = json.loads((ROOT / "config/orchestration/ecs_broker_jobs.json").read_text())["jobs"]
    commands = documented_commands()
    jobs = {filled(arguments)[0] for _, arguments in commands}
    require({"silver", "gold", "features", "train"} <= jobs, f"the runbooks document the override command for {sorted(jobs)} only")
    for name, arguments in commands:
        job, values, dry_run = filled(arguments)
        document = job_overrides.build(job, values, dry_run)
        built = {item["name"]: [pair["name"] for pair in item["environment"]] for item in document["containerOverrides"]}
        wanted = {container: [*variables, *([job_overrides.DRY_RUN_VARIABLE] if dry_run else [])] for container, variables in table[job]["containers"].items()}
        require(built == wanted, f"{name}: the documented {job} command fills {built}, the task needs {wanted}")
        for container, variables in built.items():
            require("CODE_RUN_ID" in variables or container not in ("fetch", "pin"), f"{name}: the {container} container has no code run")


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
    "no_runbook_carries_its_own_override_list": no_runbook_carries_its_own_override_list,
    "documented_commands_fill_every_variable_of_their_job": documented_commands_fill_every_variable_of_their_job,
    "the_generator_refuses_a_missing_value_by_name": the_generator_refuses_a_missing_value_by_name,
}
