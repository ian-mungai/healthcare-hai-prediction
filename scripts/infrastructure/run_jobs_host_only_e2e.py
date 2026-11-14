"""E2E of the rule that every job runs as an ECS task on the host: the whole plan of the jobs stack holds no task definition that is not an EC2 bridge task.

    .venv/bin/python -m scripts.infrastructure.run_jobs_host_only_e2e --out <new folder> [--plugin-dir <providers folder>]

The check reads the plan that ``terraform test -json -verbose`` prints for ``infra/jobs/tests/host_only.tftest.hcl``. It sees every planned ECS task
definition whatever wrote it: a root file, a child module, a variable default, a data source, Terraform JSON or a JSON input file. It then plants a task
definition through each of those ways and one privileged container in a host definition, and each plant must fail its check. Nothing calls AWS or Docker:
the provider is a test mock."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_TEST = "tests/host_only.tftest.hcl"
HOST_TEST = "tests/host.tftest.hcl"
SHARED_FILES = (
    "config/orchestration/ecs_broker_jobs.json",
    "config/orchestration/ecs_broker_jobs.schema.json",
    "config/orchestration/host_jobs.json",
    "scripts/orchestration/task_guard.py",
)
FARGATE_TASK = """
resource "aws_ecs_task_definition" "planted" {
  family                   = "planted"
  requires_compatibilities = [%s]
  network_mode             = %s
  cpu                      = "1024"
  memory                   = "2048"
  container_definitions    = jsonencode([{ name = "planted", image = "planted", essential = true }])
}
"""
LITERAL = ('"FARGATE"', '"awsvpc"')


def prepare_workspace(folder: Path, plugin_dir: Path | None) -> Path:
    """A scratch copy of the jobs stack with the files it reads above its own folder, initialized without a backend."""
    jobs = folder / "infra/jobs"
    shutil.copytree(ROOT / "infra/jobs", jobs, ignore=shutil.ignore_patterns(".terraform", "*.tfvars.json"))
    flow = folder / "infra/services/tests/flow"
    shutil.copytree(ROOT / "infra/services/tests/flow", flow, ignore=shutil.ignore_patterns(".terraform"))
    for name in SHARED_FILES:
        (folder / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, folder / name)
    initialize(jobs, plugin_dir)
    return jobs


def initialize(jobs: Path, plugin_dir: Path | None) -> None:
    arguments = ["-chdir=" + str(jobs), "init", "-backend=false", "-input=false"]
    if plugin_dir is not None:
        arguments.append("-plugin-dir=" + str(plugin_dir))
    result = run_command("terraform", arguments, cwd=jobs, timeout=600)
    if result.returncode != 0:
        raise RuntimeError("terraform init failed: " + (result.stdout + result.stderr)[-600:])


def run_test(jobs: Path, test_file: str) -> tuple[int, list[dict[str, Any]]]:
    result = run_command("terraform", ["-chdir=" + str(jobs), "test", "-json", "-verbose", "-filter=" + test_file], cwd=jobs, timeout=900)
    events = []
    for line in result.stdout.splitlines():
        try:
            events.append(json.loads(line))
        except ValueError:
            continue
    return result.returncode, events


def task_definitions(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Every ECS task definition of every planned module: its address and the compatibilities and network mode it resolves to."""
    found = []
    for event in events:
        plan = event.get("test_plan")
        if not isinstance(plan, dict):
            continue
        for change in plan.get("resource_changes", []):
            if change.get("type") != "aws_ecs_task_definition":
                continue
            after = (change.get("change") or {}).get("after") or {}
            found.append(
                {
                    "address": change["address"],
                    "requires_compatibilities": sorted(after.get("requires_compatibilities") or []),
                    "network_mode": after.get("network_mode"),
                }
            )
    return found


def foreign_definitions(definitions: list[dict[str, Any]]) -> list[str]:
    return [item["address"] for item in definitions if item["requires_compatibilities"] != ["EC2"] or item["network_mode"] != "bridge"]


def plant_module(jobs: Path) -> None:
    (jobs / "planted_child").mkdir()
    (jobs / "planted_child/main.tf").write_text(FARGATE_TASK % LITERAL)
    (jobs / "planted_module.tf").write_text('module "planted" {\n  source = "./planted_child"\n}\n')


VARIABLE_DEFAULT = 'variable "planted_compatibility" {\n  type    = string\n  default = "FAR\\u0047ATE"\n}\n'


def plant_variable_default(jobs: Path) -> None:
    (jobs / "planted_variable.tf").write_text(VARIABLE_DEFAULT + FARGATE_TASK % ("var.planted_compatibility", 'join("", ["aws", "vpc"])'))


def plant_data_source(jobs: Path) -> None:
    data = 'data "aws_ecs_task_definition" "planted" {\n  task_definition = "planted"\n}\n'
    (jobs / "planted_data.tf").write_text(
        VARIABLE_DEFAULT + data + FARGATE_TASK % ("var.planted_compatibility", "data.aws_ecs_task_definition.planted.network_mode")
    )
    test = (jobs / FIXTURE_TEST).read_text()
    mock = 'mock_provider "aws" {\n  mock_data "aws_ecs_task_definition" {\n    defaults = { network_mode = "awsvpc" }\n  }\n}'
    (jobs / FIXTURE_TEST).write_text(test.replace('mock_provider "aws" {}', mock, 1))


def plant_terraform_json(jobs: Path) -> None:
    document = {
        "resource": {
            "aws_ecs_task_definition": {
                "planted": {
                    "family": "planted",
                    "requires_compatibilities": ["FARGATE"],
                    "network_mode": "awsvpc",
                    "cpu": "1024",
                    "memory": "2048",
                    "container_definitions": '[{"name":"planted","image":"planted","essential":true}]',
                }
            }
        }
    }
    (jobs / "planted.tf.json").write_text(json.dumps(document))


def plant_json_input(jobs: Path) -> None:
    (jobs / "planted_settings.json").write_text(json.dumps({"compatibilities": ["FARGATE"], "network_mode": "awsvpc"}))
    settings = 'locals {\n  planted_settings = jsondecode(file("${path.module}/planted_settings.json"))\n}\n'
    (jobs / "planted_file.tf").write_text(settings + FARGATE_TASK % ("local.planted_settings.compatibilities[0]", "local.planted_settings.network_mode"))


def plant_privileged_container(jobs: Path) -> None:
    path = jobs / "promote.tf"
    text = path.read_text()
    path.write_text(text.replace("privileged             = false", "privileged             = true"))


PLANTS: dict[str, Callable[[Path], None]] = {
    "a task definition in a child module": plant_module,
    "a task definition with a variable default for its settings": plant_variable_default,
    "a task definition that takes its network mode from a data source": plant_data_source,
    "a task definition in a Terraform JSON file": plant_terraform_json,
    "a task definition that reads its settings from a JSON input file": plant_json_input,
}


def plant_checks(folder: Path, plugin_dir: Path | None) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for label, plant in PLANTS.items():
        jobs = prepare_workspace(folder / ("plant_" + str(len(checks))), plugin_dir)
        plant(jobs)
        initialize(jobs, plugin_dir)
        code, events = run_test(jobs, FIXTURE_TEST)
        planted = foreign_definitions(task_definitions(events))
        checks[f"the check catches {label}"] = code == 0 and any("planted" in address for address in planted)
    jobs = prepare_workspace(folder / "plant_privileged", plugin_dir)
    plant_privileged_container(jobs)
    code, _ = run_test(jobs, HOST_TEST)
    checks["the host tests catch a privileged container in a host definition"] = code != 0
    return checks


# The only Terraform roots that may declare an ECS task definition: the jobs stack, whose plan the check above reads, and the services stack with its own tests.
TASK_DEFINITION_ROOTS = ("infra/jobs", "infra/services")


def declaration_checks() -> dict[str, bool]:
    """No other Terraform root or module declares an ECS task definition, so no job can be registered outside the checked plan."""
    declaring: dict[str, list[str]] = {}
    for pattern in ("*.tf", "*.tf.json"):
        for path in sorted((ROOT / "infra").rglob(pattern)):
            if ".terraform" in path.parts or "aws_ecs_task_definition" not in path.read_text():
                continue
            declaring.setdefault(str(path.parent.relative_to(ROOT)), []).append(path.name)
    foreign = sorted(folder for folder in declaring if folder not in TASK_DEFINITION_ROOTS)
    return {f"only the jobs and services stacks declare a task definition ({foreign})": not foreign}


def run(output: Path, plugin_dir: Path | None) -> dict[str, dict[str, bool]]:
    with tempfile.TemporaryDirectory(prefix="jobs_host_only_") as scratch:
        folder = Path(scratch)
        jobs = prepare_workspace(folder / "stack", plugin_dir)
        code, events = run_test(jobs, FIXTURE_TEST)
        definitions = task_definitions(events)
        stack = {
            "the fixture plan passes its own test": code == 0,
            "the plan holds the fifteen host definitions and the publisher": len(definitions) == 16,
            "every planned task definition of every module is an EC2 bridge task": bool(definitions) and not foreign_definitions(definitions),
        }
        planted = plant_checks(folder / "plants", plugin_dir)
    output.mkdir(parents=True)
    results = {"declarations": declaration_checks(), "stack": stack, "plants": planted}
    (output / "report.json").write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--out", type=Path, required=True, help="a new folder for the report")
    parser.add_argument("--plugin-dir", type=Path, default=None, help="a folder with the provider packages, for a machine without a network")
    arguments = parser.parse_args()
    results = run(arguments.out, arguments.plugin_dir)
    failed = [f"{group}: {name}" for group, checks in results.items() for name, ok in checks.items() if not ok]
    total = sum(len(checks) for checks in results.values())
    for line in failed:
        sys.stdout.write(f"FAIL {line}\n")
    sys.stdout.write(f"jobs host-only E2E: {total - len(failed)} of {total} checks passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
