"""E2E of the rule that a jobs apply registers the new revision of a task definition before it retires the old one.

    .venv/bin/python -m scripts.infrastructure.run_jobs_replace_order_e2e --out <new folder> [--plugin-dir <providers folder>]

On Nov 16 2026 ECS refused the seven new features definitions ("Too many containers") after Terraform had already destroyed the seven old ones in
the state, so the family had no definition in the state until the owner restored it by hand. A plan cannot run the ECS limits, so the check reads
the order of the actions: a replacement that creates first is planned as ``["create", "delete"]`` and one that destroys first as ``["delete",
"create"]``. With the create first, a registration that fails leaves the old revision in the state. The check reads the plan that ``terraform
test -json -verbose`` prints for ``infra/jobs/tests/replace_order.tftest.hcl``: the stack is applied to a mock provider, a plan then replaces
every host definition and the publisher by address, and the plan of that replacement must create each new definition first and keep each old
revision (``skip_destroy`` on), because the project user may not deregister a revision. It then plants each defect and each must fail its own
check. Nothing calls AWS: the provider is a test mock, so the check proves the plan the provider receives and not the provider's own create and
delete calls."""

from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.infrastructure.run_jobs_host_only_e2e import initialize, prepare_workspace, run_test

FIXTURE_TEST = "tests/replace_order.tftest.hcl"
REPLACE_RUN = "replace_every_definition"
TASK_TYPE = "aws_ecs_task_definition"
HOST_ADDRESS = re.compile(r'^aws_ecs_task_definition\.(host\["[a-z0-9_]+"\]|publisher_host\[0\])$')
EXPECTED_DEFINITIONS = 16
CREATE_FIRST, DESTROY_FIRST = ["create", "delete"], ["delete", "create"]
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
ORDER_CHECK = "every replaced task definition is created before the old revision is retired"
KEEP_CHECK = "every replaced definition keeps its old revision: the state before and the plan after have skip_destroy on"
COUNT_CHECK = "the plan replaces the fifteen host definitions and the publisher, and no other task definition"


def plan_changes(events: list[dict[str, Any]], run_name: str) -> list[dict[str, Any]]:
    """Every task definition change of the plan of one run: address, actions, and skip_destroy before and after."""
    found = []
    for event in events:
        plan = event.get("test_plan")
        if event.get("@testrun") != run_name or not isinstance(plan, dict):
            continue
        for change in plan.get("resource_changes", []):
            if change.get("type") != TASK_TYPE:
                continue
            body = change.get("change") or {}
            found.append(
                {
                    "address": str(change["address"]),
                    "actions": list(body.get("actions") or []),
                    "kept_before": (body.get("before") or {}).get("skip_destroy") is True,
                    "kept_after": (body.get("after") or {}).get("skip_destroy") is True,
                }
            )
    return found


def replaced(changes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The changes that replace a definition, in either order."""
    return [item for item in changes if sorted(item["actions"]) == sorted(CREATE_FIRST)]


def evaluate(code: int, events: list[dict[str, Any]]) -> dict[str, bool]:
    changes = plan_changes(events, REPLACE_RUN)
    swaps = replaced(changes)
    return {
        "the replacement test passes": code == 0,
        COUNT_CHECK: len(swaps) == EXPECTED_DEFINITIONS and len(changes) == EXPECTED_DEFINITIONS and all(HOST_ADDRESS.match(item["address"]) for item in swaps),
        ORDER_CHECK: bool(swaps) and all(item["actions"] == CREATE_FIRST for item in swaps),
        KEEP_CHECK: bool(swaps) and all(item["kept_before"] and item["kept_after"] for item in swaps),
    }


def edit(name: str, change: Callable[[str], str]) -> Callable[[Path], None]:
    def plant(jobs: Path) -> None:
        path = jobs / name
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


def plant_unprotected_definition(jobs: Path) -> None:
    (jobs / "planted_definition.tf").write_text(PLANTED_DEFINITION)


# Each plant is a defect and the check that must fail because of it.
PLANTS: dict[str, tuple[Callable[[Path], None], str]] = {
    "the host definitions are replaced in the old order": (
        edit("host.tf", without_create_before_destroy('resource "aws_ecs_task_definition" "host"')),
        ORDER_CHECK,
    ),
    "the publisher definition is replaced in the old order": (
        edit("publisher.tf", without_create_before_destroy('resource "aws_ecs_task_definition" "publisher_host"')),
        ORDER_CHECK,
    ),
    "a new task definition is declared that the replacement run does not name": (plant_unprotected_definition, COUNT_CHECK),
    "a host definition retires its old revision when it is replaced": (edit("host.tf", skip_destroy_off), KEEP_CHECK),
    "the publisher definition retires its old revision when it is replaced": (
        edit("publisher.tf", skip_destroy_off),
        KEEP_CHECK,
    ),
    "the replacement run changes nothing, so the order check would pass on an empty plan": (
        edit(FIXTURE_TEST, replace_nothing),
        COUNT_CHECK,
    ),
}


def plant_checks(folder: Path, plugin_dir: Path | None) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for label, (plant, failing_check) in PLANTS.items():
        jobs = prepare_workspace(folder / f"plant_{len(checks)}", plugin_dir)
        plant(jobs)
        initialize(jobs, plugin_dir)
        code, events = run_test(jobs, FIXTURE_TEST)
        checks[f"the check catches: {label}"] = evaluate(code, events).get(failing_check, True) is False
    return checks


def run(output: Path, plugin_dir: Path | None) -> dict[str, dict[str, bool]]:
    with tempfile.TemporaryDirectory(prefix="jobs_replace_order_") as scratch:
        folder = Path(scratch)
        jobs = prepare_workspace(folder / "stack", plugin_dir)
        code, events = run_test(jobs, FIXTURE_TEST)
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
    sys.stdout.write(f"jobs replace order E2E: {total - len(failed)} of {total} checks passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
