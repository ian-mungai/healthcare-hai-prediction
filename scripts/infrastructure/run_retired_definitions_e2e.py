"""E2E of the rule that the apply which removes the retired Fargate job definitions only forgets them and leaves every host definition alone.

    .venv/bin/python -m scripts.infrastructure.run_retired_definitions_e2e --out <new folder> [--plugin-dir <providers folder>]

The project user holds no grant to deregister a task definition, and the owner deregistered every ACTIVE revision of the retired families by hand on
Nov 13 2026. The AWS provider calls that API when it destroys a definition whose skip_destroy is off, and it only forgets a definition whose skip_destroy is
on. The jobs stack declares no Fargate definition any more, so the check builds the state it had before:
``infra/jobs/retired_history/fargate_history.tf.txt`` declares the 19 retired definitions at their old addresses with skip_destroy on, and
``infra/jobs/retired_history/retired_definitions.tftest.hcl`` applies the stack with them and the host definitions, applies the final configuration with the
same host inputs, and plans again. This script copies both into a scratch copy of the stack and reads the plans from ``terraform test -json -verbose``. The
removal plan must forget exactly the 19 retired addresses, each as a plain delete of a state that keeps its revisions, and every other resource change must
be a no-op, so a host definition that is forgotten, replaced or updated fails. The removal plan creates, updates and replaces nothing, so no retired family
is registered again. The plan after the removal must be empty. It then plants each of those defects and each must fail its own check. Nothing calls AWS:
the provider is a test mock, so the check proves the state the provider reads and not the provider's own delete call."""

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

from scripts.infrastructure.run_jobs_host_only_e2e import ROOT, initialize, prepare_workspace, run_test

HISTORY_FOLDER = ROOT / "infra/jobs/retired_history"
FIXTURE_SOURCE = HISTORY_FOLDER / "fargate_history.tf.txt"
TEST_SOURCE = HISTORY_FOLDER / "retired_definitions.tftest.hcl"
FIXTURE_NAME = "retired_fargate_history.tf"
FIXTURE_TEST = "tests/retired_definitions.tftest.hcl"
REMOVAL_RUN, FINAL_RUN = "remove_retired_only", "plan_after_removal"
FAMILY_PREFIX = "example_project_dev_job_"
TASK_TYPE = "aws_ecs_task_definition"
HOST_ADDRESS = re.compile(r'^aws_ecs_task_definition\.(host\["[a-z0-9_]+"\]|publisher_host\[0\])$')
SET_LOCAL = re.compile(r"retired_history_(\w+)\s*=\s*var\.retired_history_present \? toset\(\[([^\]]*)\]\) : toset\(\[\]\)")
SINGLE_BLOCK = re.compile(r'resource "aws_ecs_task_definition" "(\w+)" \{\s*count\s*=[^\n]*\n.*?_job_(\w+)"', re.DOTALL)


def retired_addresses(fixture: str) -> dict[str, str]:
    """The old address of each retired definition and the family it registered, read from the fixture that declares them."""
    found = {
        f'{TASK_TYPE}.{block}["{name}"]': name for block, names in SET_LOCAL.findall(fixture) if block != "single" for name in re.findall(r'"(\w+)"', names)
    }
    found |= {f"{TASK_TYPE}.{block}[0]": family for block, family in SINGLE_BLOCK.findall(fixture)}
    return found


# The old address of each retired definition and the family it registered.
RETIRED_ADDRESSES = retired_addresses(FIXTURE_SOURCE.read_text(encoding="utf-8"))
SKIP_DESTROY_LINE = re.compile(r"^(\s*)skip_destroy(\s*)=(\s*)true\s*$", re.MULTILINE)
PLANTED_REREGISTRATION = "\n".join(
    [
        'resource "aws_ecs_task_definition" "again" {',
        "  count                 = var.retired_history_present ? 0 : 1",
        '  family                = "example_project_dev_job_silver"',
        "  container_definitions = jsonencode([])",
        "}",
        "",
    ]
)
PLANTED_RESOURCE = 'resource "terraform_data" "planted" {\n  input = var.retired_history_present\n}\n'


def workspace(folder: Path, plugin_dir: Path | None) -> Path:
    """A scratch copy of the jobs stack with the historical fixture beside it and the transition test among its tests."""
    jobs = prepare_workspace(folder, plugin_dir)
    shutil.copy2(FIXTURE_SOURCE, jobs / FIXTURE_NAME)
    shutil.copy2(TEST_SOURCE, jobs / FIXTURE_TEST)
    return jobs


def plan_changes(events: list[dict[str, Any]], run_name: str) -> list[dict[str, Any]]:
    """Every resource change of the plan of one run: address, type, actions, and the family and skip_destroy value of the prior state."""
    found = []
    for event in events:
        plan = event.get("test_plan")
        if event.get("@testrun") != run_name or not isinstance(plan, dict):
            continue
        for change in plan.get("resource_changes", []):
            body = change.get("change") or {}
            before = body.get("before") or {}
            found.append(
                {
                    "address": str(change["address"]),
                    "type": str(change.get("type", "")),
                    "actions": list(body.get("actions") or []),
                    "family": str(before.get("family", "")),
                    "keeps_revisions": before.get("skip_destroy") is True,
                }
            )
    return found


def forgotten(changes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The changes at a retired address that delete it. A delete at any other address, a host definition included, is not among them."""
    return [item for item in changes if item["address"] in RETIRED_ADDRESSES and "delete" in item["actions"]]


def evaluate_removal(changes: list[dict[str, Any]]) -> dict[str, bool]:
    gone = forgotten(changes)
    others = [item for item in changes if item not in gone]
    deleted = {item["address"] for item in changes if "delete" in item["actions"]}
    hosts = [item for item in changes if item["type"] == TASK_TYPE and HOST_ADDRESS.match(item["address"])]
    return {
        "the removal plan forgets exactly the 19 retired definitions at their old addresses": len(RETIRED_ADDRESSES) == 19
        and deleted == set(RETIRED_ADDRESSES)
        and len(gone) == len(RETIRED_ADDRESSES),
        "the removal plan forgets exactly the families that were registered": {
            item["family"].removeprefix(FAMILY_PREFIX) for item in gone if item["family"].startswith(FAMILY_PREFIX)
        }
        == set(RETIRED_ADDRESSES.values())
        and all(item["family"].removeprefix(FAMILY_PREFIX) == RETIRED_ADDRESSES.get(item["address"]) for item in gone),
        "no retired family is registered again: the removal plan holds only deletes and no-ops, no create, update or replace": all(
            item["actions"] in (["delete"], ["no-op"]) for item in changes
        ),
        "every retired definition is a plain delete of a state that keeps its revisions": bool(gone)
        and all(item["actions"] == ["delete"] and item["keeps_revisions"] for item in gone),
        "every other resource of the plan is unchanged: no host definition or other resource is forgotten, replaced, updated or created": bool(others)
        and all(item["actions"] == ["no-op"] for item in others),
        "the plan holds the fifteen host definitions and the publisher, all unchanged": len(hosts) == 16
        and all(item["actions"] == ["no-op"] for item in hosts),
    }


def evaluate_final(changes: list[dict[str, Any]]) -> dict[str, bool]:
    return {
        "the plan after the removal is empty and still holds the fifteen host definitions and the publisher": bool(changes)
        and all(item["actions"] == ["no-op"] for item in changes)
        and sum(1 for item in changes if item["type"] == TASK_TYPE and HOST_ADDRESS.match(item["address"])) == 16,
    }


def evaluate(code: int, events: list[dict[str, Any]]) -> dict[str, bool]:
    return {"the transition test passes": code == 0} | evaluate_removal(plan_changes(events, REMOVAL_RUN)) | evaluate_final(plan_changes(events, FINAL_RUN))


def edit(name: str, change: Callable[[str], str]) -> Callable[[Path], None]:
    def plant(jobs: Path) -> None:
        path = jobs / name
        text = change(path.read_text())
        if text == path.read_text():
            raise RuntimeError(f"The planted change did not alter {name}.")
        path.write_text(text)

    return plant


def in_removal_runs(change: Callable[[str], str]) -> Callable[[str], str]:
    """Apply a change to the text of the three runs that follow the historical state."""

    def apply(text: str) -> str:
        head, marker, tail = text.partition(f'run "{REMOVAL_RUN}"')
        return head + marker + change(tail)

    return apply


def forget_every_host_definition(tail: str) -> str:
    return re.sub(r"host_jobs_enabled(\s*)=(\s*)true", r"host_jobs_enabled\1=\2false", tail)


def forget_one_host_definition(tail: str) -> str:
    listed = json.dumps(["bronze", "drift", "features", "gold", "metadata", "promote", "silver", "train"])
    return re.sub(r"(\n\s*)(retired_history_present\s*=\s*false)", rf"\1\2\1host_jobs = {listed}", tail)


def replace_host_definitions(tail: str) -> str:
    """Size the host jobs one MiB larger, which the host capacity binding allows, so every host definition gets new containers."""
    binding = "job_memory_mib=23553 usable_memory_mib=30721 system_reserve_mib=2048 service_reservations_mib=5120"
    return re.sub(r"(\n\s*)(retired_history_present\s*=\s*false)", rf'\1\2\1host_job_memory_mib = 23553\1host_capacity_binding = "{binding}"', tail)


def skip_destroy_off_in_first_block(text: str) -> str:
    return SKIP_DESTROY_LINE.sub(r"\1skip_destroy\2=\3false", text, count=1)


def skip_destroy_off_in_last_block(text: str) -> str:
    matches = list(SKIP_DESTROY_LINE.finditer(text))
    last = matches[-1]
    return text[: last.start()] + SKIP_DESTROY_LINE.sub(r"\1skip_destroy\2=\3false", text[last.start() : last.end()]) + text[last.end() :]


def skip_destroy_omitted_in_train(text: str) -> str:
    block = text.index('resource "aws_ecs_task_definition" "train"')
    return text[:block] + SKIP_DESTROY_LINE.sub("", text[block:], count=1)


def keep_one_layer_definition(text: str) -> str:
    return text.replace('toset(["silver", "gold", "features"]) : toset([])', 'toset(["silver", "gold", "features"]) : toset(["silver"])')


def plant_unrelated_resource(jobs: Path) -> None:
    (jobs / "planted.tf").write_text(PLANTED_RESOURCE)


def plant_reregistration(jobs: Path) -> None:
    (jobs / "planted_again.tf").write_text(PLANTED_REREGISTRATION)


# Each plant is a defect and the check that must fail because of it.
PLANTS: dict[str, tuple[Callable[[Path], None], str]] = {
    "every host definition is forgotten with the retired ones": (
        edit(FIXTURE_TEST, in_removal_runs(forget_every_host_definition)),
        "every other resource of the plan is unchanged: no host definition or other resource is forgotten, replaced, updated or created",
    ),
    "one host definition is forgotten with the retired ones": (
        edit(FIXTURE_TEST, in_removal_runs(forget_one_host_definition)),
        "every other resource of the plan is unchanged: no host definition or other resource is forgotten, replaced, updated or created",
    ),
    "the host definitions are changed by the removing apply": (
        edit(FIXTURE_TEST, in_removal_runs(replace_host_definitions)),
        "every other resource of the plan is unchanged: no host definition or other resource is forgotten, replaced, updated or created",
    ),
    "an unrelated resource changes in the removing apply": (
        plant_unrelated_resource,
        "every other resource of the plan is unchanged: no host definition or other resource is forgotten, replaced, updated or created",
    ),
    "a retired family is registered again under a new address": (
        plant_reregistration,
        "no retired family is registered again: the removal plan holds only deletes and no-ops, no create, update or replace",
    ),
    "a retired definition stays declared": (
        edit(FIXTURE_NAME, keep_one_layer_definition),
        "the removal plan forgets exactly the 19 retired definitions at their old addresses",
    ),
    "a retired definition is recorded with skip_destroy off (first block)": (
        edit(FIXTURE_NAME, skip_destroy_off_in_first_block),
        "every retired definition is a plain delete of a state that keeps its revisions",
    ),
    "a retired definition is recorded with skip_destroy off (last block)": (
        edit(FIXTURE_NAME, skip_destroy_off_in_last_block),
        "every retired definition is a plain delete of a state that keeps its revisions",
    ),
    "a retired definition is recorded with no skip_destroy": (
        edit(FIXTURE_NAME, skip_destroy_omitted_in_train),
        "every retired definition is a plain delete of a state that keeps its revisions",
    ),
}


def plant_checks(folder: Path, plugin_dir: Path | None) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for label, (plant, failing_check) in PLANTS.items():
        jobs = workspace(folder / f"plant_{len(checks)}", plugin_dir)
        plant(jobs)
        initialize(jobs, plugin_dir)
        code, events = run_test(jobs, FIXTURE_TEST)
        planned = bool(plan_changes(events, REMOVAL_RUN))
        checks[f"the check catches: {label}"] = planned and evaluate(code, events).get(failing_check, True) is False
    return checks


def run(output: Path, plugin_dir: Path | None) -> dict[str, dict[str, bool]]:
    with tempfile.TemporaryDirectory(prefix="retired_definitions_") as scratch:
        folder = Path(scratch)
        jobs = workspace(folder / "stack", plugin_dir)
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
    sys.stdout.write(f"retired definitions E2E: {total - len(failed)} of {total} checks passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
