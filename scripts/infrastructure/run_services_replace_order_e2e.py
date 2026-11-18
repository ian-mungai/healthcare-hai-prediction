"""E2E of the rule that a services apply registers the new revision of a task definition before it retires the old one.

    .venv/bin/python -m scripts.infrastructure.run_services_replace_order_e2e --out <new folder> [--plugin-dir <providers folder>]

On Nov 16 2026 ECS refused the seven new features job definitions ("Too many containers") after Terraform had already destroyed the seven old ones in
the state, so the family had no definition in the state until the owner restored it by hand. The services stack replaced its task definitions in the
same order. A plan cannot run the ECS limits, so the check reads the order of the actions: a replacement that creates first is planned as
``["create", "delete"]`` and one that destroys first as ``["delete", "create"]``. With the create first, a registration that fails leaves the old
revision in the state. The check reads the plan that ``terraform test -json -verbose`` prints for ``infra/services/tests/replace_order.tftest.hcl``:
the stack with every optional service on is applied to a mock provider, a plan then replaces the twelve service definitions and the four one-shot
definitions by address, and that plan must create each new definition first, keep each old revision (``skip_destroy`` on, because the project user
may not deregister a revision) and leave every ECS service in place: each fixed and catalog service updates to the new revision and the two serving
services, which GitHub releases, keep theirs. It then plants each defect and each must fail its own check. Nothing calls AWS: the provider is a test
mock, so the check proves the plan the provider receives and not the provider's own create and delete calls."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_TEST = "tests/replace_order.tftest.hcl"
REPLACE_RUN = "replace_every_definition"
TASK_TYPE = "aws_ecs_task_definition"
SERVICE_TYPE = "aws_ecs_service"
DEFINITION_ADDRESS = re.compile(r'^aws_ecs_task_definition\.(service|one_shot)\["[a-z0-9_]+"\]$')
SERVING_ADDRESS = re.compile(r'^aws_ecs_service\.serving\["[a-z0-9_]+"\]$')
EXPECTED_DEFINITIONS = 16
EXPECTED_SERVICES = 12
EXPECTED_SERVING = 2
CREATE_FIRST = ["create", "delete"]
CREATE_BEFORE_DESTROY = re.compile(r"^(\s*)create_before_destroy(\s*)=(\s*)true\s*\n", re.MULTILINE)
PLANTED_DEFINITION = """
resource "aws_ecs_task_definition" "planted" {
  family                   = "planted"
  requires_compatibilities = ["EC2"]
  network_mode             = "bridge"
  cpu                      = "1024"
  memory                   = "2048"
  container_definitions    = jsonencode([{ name = "planted", image = "planted", essential = true }])
  skip_destroy             = true
}
"""
SERVICE_BLOCK = 'resource "aws_ecs_service" "service" {'
ORDER_CHECK = "every replaced task definition is created before the old revision is retired"
KEEP_CHECK = "every replaced definition keeps its old revision: the state before and the plan after have skip_destroy on"
COUNT_CHECK = "the plan replaces the twelve service definitions and the four one-shot definitions, and no other task definition"
PLACE_CHECK = "no ECS service is destroyed or replaced by the replacement of its definition"
FOLLOW_CHECK = "every fixed and catalog service updates to the new revision and the two serving services keep theirs"


def plan_changes(events: list[dict[str, Any]], run_name: str, resource_type: str) -> list[dict[str, Any]]:
    """Every change of one resource type in the plan of one run: address, actions, skip_destroy before and after, and whether the revision is unknown."""
    found = []
    for event in events:
        plan = event.get("test_plan")
        if event.get("@testrun") != run_name or not isinstance(plan, dict):
            continue
        for change in plan.get("resource_changes", []):
            if change.get("type") != resource_type:
                continue
            body = change.get("change") or {}
            found.append(
                {
                    "address": str(change["address"]),
                    "actions": list(body.get("actions") or []),
                    "kept_before": (body.get("before") or {}).get("skip_destroy") is True,
                    "kept_after": (body.get("after") or {}).get("skip_destroy") is True,
                    "new_revision": bool((body.get("after_unknown") or {}).get("task_definition")),
                }
            )
    return found


def replaced(changes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The changes that replace a definition, in either order."""
    return [item for item in changes if sorted(item["actions"]) == sorted(CREATE_FIRST)]


def services_follow(services: list[dict[str, Any]]) -> bool:
    """Each fixed and catalog service updates in place to a revision that the plan does not know yet, and each serving service has no change."""
    serving = [item for item in services if SERVING_ADDRESS.match(item["address"])]
    following = [item for item in services if not SERVING_ADDRESS.match(item["address"])]
    return (
        len(services) == EXPECTED_SERVICES
        and len(serving) == EXPECTED_SERVING
        and all(item["actions"] == ["no-op"] for item in serving)
        and all(item["actions"] == ["update"] and item["new_revision"] for item in following)
    )


def evaluate(code: int, events: list[dict[str, Any]]) -> dict[str, bool]:
    changes = plan_changes(events, REPLACE_RUN, TASK_TYPE)
    swaps = replaced(changes)
    services = plan_changes(events, REPLACE_RUN, SERVICE_TYPE)
    return {
        "the replacement test passes": code == 0,
        COUNT_CHECK: len(swaps) == EXPECTED_DEFINITIONS
        and len(changes) == EXPECTED_DEFINITIONS
        and all(DEFINITION_ADDRESS.match(item["address"]) for item in swaps),
        ORDER_CHECK: bool(swaps) and all(item["actions"] == CREATE_FIRST for item in swaps),
        KEEP_CHECK: bool(swaps) and all(item["kept_before"] and item["kept_after"] for item in swaps),
        PLACE_CHECK: bool(services) and not any("delete" in item["actions"] or "create" in item["actions"] for item in services),
        FOLLOW_CHECK: services_follow(services),
    }


def edit(name: str, change: Callable[[str], str]) -> Callable[[Path], None]:
    def plant(stack: Path) -> None:
        path = stack / name
        text = path.read_text()
        planted = change(text)
        if planted == text:
            raise RuntimeError(f"The planted change did not alter {name}.")
        path.write_text(planted)

    return plant


def without_create_before_destroy(block_start: str) -> Callable[[str], str]:
    """Remove the create_before_destroy line of the first definition block that follows ``block_start``."""

    def change(text: str) -> str:
        start = text.index(block_start)
        return text[:start] + CREATE_BEFORE_DESTROY.sub("", text[start:], count=1)

    return change


def skip_destroy_off(text: str) -> str:
    return re.sub(r"(skip_destroy\s*=\s*)true", r"\1false", text, count=1)


def replace_nothing(text: str) -> str:
    """Drop the replace list of the replacement run, so the plan changes nothing."""
    return re.sub(r"\n  plan_options \{.*?\n  \}\n", "\n", text, flags=re.DOTALL)


def with_service_lifecycle(body: str) -> Callable[[str], str]:
    """Add a lifecycle block to the fixed services, which declare none."""

    def change(text: str) -> str:
        start = text.index(SERVICE_BLOCK)
        marker = text.index("  deployment_circuit_breaker {", start)
        return text[:marker] + f"  lifecycle {{\n    {body}\n  }}\n\n" + text[marker:]

    return change


def plant_unprotected_definition(stack: Path) -> None:
    (stack / "planted_definition.tf").write_text(PLANTED_DEFINITION)


# Each plant is a defect and the check that must fail because of it.
PLANTS: dict[str, tuple[Callable[[Path], None], str]] = {
    "the service definitions are replaced in the old order": (
        edit("main.tf", without_create_before_destroy('resource "aws_ecs_task_definition" "service"')),
        ORDER_CHECK,
    ),
    "the one-shot definitions are replaced in the old order": (
        edit("one_shot.tf", without_create_before_destroy('resource "aws_ecs_task_definition" "one_shot"')),
        ORDER_CHECK,
    ),
    "a new task definition is declared that the replacement run does not name": (plant_unprotected_definition, COUNT_CHECK),
    "a service definition retires its old revision when it is replaced": (edit("main.tf", skip_destroy_off), KEEP_CHECK),
    "a one-shot definition retires its old revision when it is replaced": (edit("one_shot.tf", skip_destroy_off), KEEP_CHECK),
    "the replacement run changes nothing, so the order check would pass on an empty plan": (
        edit(FIXTURE_TEST, replace_nothing),
        COUNT_CHECK,
    ),
    "a fixed service is replaced together with its definition": (
        edit("main.tf", with_service_lifecycle("replace_triggered_by = [aws_ecs_task_definition.service[each.key]]")),
        PLACE_CHECK,
    ),
    "a fixed service keeps the old revision when its definition is replaced": (
        edit("main.tf", with_service_lifecycle("ignore_changes = [task_definition]")),
        FOLLOW_CHECK,
    ),
}


def prepare_workspace(folder: Path, plugin_dir: Path | None) -> Path:
    """A scratch copy of the services stack, initialized without a backend. It reads nothing above its own folder."""
    stack = folder / "infra/services"
    shutil.copytree(ROOT / "infra/services", stack, ignore=shutil.ignore_patterns(".terraform", "*.tfvars.json", "*.tfstate*", "*.tfplan"))
    initialize(stack, plugin_dir)
    return stack


def initialize(stack: Path, plugin_dir: Path | None) -> None:
    arguments = ["-chdir=" + str(stack), "init", "-backend=false", "-input=false"]
    if plugin_dir is not None:
        arguments.append("-plugin-dir=" + str(plugin_dir))
    result = run_command("terraform", arguments, cwd=stack, timeout=600)
    if result.returncode != 0:
        raise RuntimeError("terraform init failed: " + (result.stdout + result.stderr)[-600:])


def run_test(stack: Path, test_file: str) -> tuple[int, list[dict[str, Any]]]:
    result = run_command("terraform", ["-chdir=" + str(stack), "test", "-json", "-verbose", "-filter=" + test_file], cwd=stack, timeout=900)
    events = []
    for line in result.stdout.splitlines():
        try:
            events.append(json.loads(line))
        except ValueError:
            continue
    return result.returncode, events


def plant_checks(folder: Path, plugin_dir: Path | None) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for label, (plant, failing_check) in PLANTS.items():
        stack = prepare_workspace(folder / f"plant_{len(checks)}", plugin_dir)
        plant(stack)
        initialize(stack, plugin_dir)
        code, events = run_test(stack, FIXTURE_TEST)
        checks[f"the check catches: {label}"] = evaluate(code, events).get(failing_check, True) is False
    return checks


def run(output: Path, plugin_dir: Path | None) -> dict[str, dict[str, bool]]:
    with tempfile.TemporaryDirectory(prefix="services_replace_order_") as scratch:
        folder = Path(scratch)
        stack_folder = prepare_workspace(folder / "stack", plugin_dir)
        code, events = run_test(stack_folder, FIXTURE_TEST)
        stack = evaluate(code, events)
        planted = plant_checks(folder / "plants", plugin_dir)
    output.mkdir(parents=True)
    results = {"stack": stack, "plants": planted}
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
    sys.stdout.write(f"services replace order E2E: {total - len(failed)} of {total} checks passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
