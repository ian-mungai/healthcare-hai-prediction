"""Prove that the first owner apply after the move leaves the model API and the dashboard on a revision the release check accepts (owner decision D18).

The two serving services ignore ``task_definition``, so an owner apply cannot roll back a release by GitHub. A service that exists before this change runs a
revision that names the shared task execution role and, for the model API, the shared Redis password. GitHub renders its next revision from the running one, and
the release check refuses that password, so the first release would fail before it registers anything. The stack therefore replaces each serving service once:
the service watches the execution role of its task definition (``replace_triggered_by``), which changes from the shared role to the serving execution role in
that apply only.

The run plans the services stack with the real AWS provider and no AWS call (as ``run_services_move_e2e`` does). Nothing is applied and no credential is read.

1. First apply: a state that holds the services at their old addresses with a base revision (:99, shared role and shared Redis password). The plan must replace
   both services because of the trigger, keep the old task definition revision registered, and start the new services on the revision of the stack (serving
   execution role, the Redis user of the model API).
2. The release step of ``.github/workflows/deploy_model_api.yml`` runs as written, with a stand-in for the AWS command. On the base revision the model API step
   stops at the release check and registers nothing (the control). On the revision that the plan gives the new service the step registers the next revision.
3. Later apply: a state with the services at the new addresses on a revision that GitHub registered (:100) and a task definition that the stack changes (a new
   image digest). The task definition is replaced, and the services must stay as they are.

Then each planted defect (no trigger, a trigger on the whole task definition or on an attribute that never changes, no ignore_changes, the shared execution
role, the shared Redis password) must make one of these checks fail.

Run: ``.venv/bin/python -m scripts.infrastructure.run_serving_transition_e2e --output-directory data/e2e/<new folder>``
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import shutil
import stat
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.infrastructure import run_services_move_e2e as stack
from scripts.infrastructure import run_serving_release_real_e2e as release
from scripts.process import run_command

REPO_ROOT = stack.REPO_ROOT
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "deploy_model_api.yml"
STEP_NAME = "Register the next revision from the running one and check it"
BASE_REVISION = 99
LATER_REVISION = 100
NEW_DIGEST = "sha256:" + "5" * 64
CHANGED_DIGEST = "sha256:" + "6" * 64
# The release step starts with literal arguments only (the docker-calls guard proves them), so the step, the stand-in and the definitions go to fixed paths.
STEP = Path("data/e2e/.transition_step.sh")
BIN = Path("data/e2e/.transition_bin")
FIXTURE = Path("data/e2e/.transition_fixture")
STAND_IN = """#!/usr/bin/env bash
# Stand-in for the AWS command: it answers the four calls of the release step from files and refuses every other call.
set -euo pipefail
fixture="$TRANSITION_FIXTURE"
group="$1 $2"
shift 2
task_definition=""
input=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --task-definition) task_definition="$2"; shift 2 ;;
    --cli-input-json) input="${2#file://}"; shift 2 ;;
    *) shift ;;
  esac
done
case "$group" in
  "sts get-caller-identity") cat "$fixture/account" ;;
  "ecs describe-services") cat "$fixture/running_arn" ;;
  "ecs describe-task-definition") cat "$fixture/definitions/$(printf '%s' "$task_definition" | tr '/:' '__').json" ;;
  "ecs register-task-definition")
    cp "$input" "$fixture/registered.json"
    arn="$(cat "$fixture/registered_arn")"
    cp "$input" "$fixture/definitions/$(printf '%s' "$arn" | tr '/:' '__').json"
    printf '%s\\n' "$arn"
    ;;
  *) echo "unexpected aws call: $group" >&2; exit 64 ;;
esac
"""


def release_step() -> str:
    """The script of the release step as the workflow file has it: the lines under ``run: |`` of the step with that name, less their indentation."""
    lines = WORKFLOW.read_text(encoding="utf-8").splitlines()
    start = next((index for index, line in enumerate(lines) if line.strip() == f"- name: {STEP_NAME}"), None)
    if start is None:
        raise RuntimeError(f"the workflow has no step named {STEP_NAME}")
    run = next(index for index in range(start, len(lines)) if lines[index].strip() == "run: |")
    indent = len(lines[run + 1]) - len(lines[run + 1].lstrip())
    body = []
    for line in lines[run + 1 :]:
        if line.strip() and len(line) - len(line.lstrip()) < indent:
            break
        body.append(line[indent:])
    return "\n".join(body).rstrip() + "\n"


def containers_of(change_after: dict[str, Any]) -> list[dict[str, Any]]:
    return json.loads(change_after["container_definitions"])


def task_definition_changes(plan: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {change["index"]: change for change in plan["resource_changes"] if change["type"] == "aws_ecs_task_definition"}


def shared_redis_secret(plan: dict[str, Any]) -> str:
    """The ARN of the shared Redis password, which the Redis container of the stack reads."""
    after = task_definition_changes(plan)["redis"]["change"]["after"]
    for container in containers_of(after):
        for secret in container.get("secrets", []):
            if secret["name"] == "REDIS_PASSWORD":
                return str(secret["valueFrom"])
    raise RuntimeError("the Redis container has no REDIS_PASSWORD secret")


def as_base(containers: list[dict[str, Any]], shared_secret: str) -> list[dict[str, Any]]:
    """The containers as the stack rendered them before this change: the shared Redis password and no Redis user name."""
    base = copy.deepcopy(containers)
    for container in base:
        for secret in container.get("secrets", []):
            if secret["name"] == "REDIS_PASSWORD":
                secret["valueFrom"] = shared_secret
        if "environment" in container:
            container["environment"] = [item for item in container["environment"] if item["name"] != "REDIS_USERNAME"]
    return base


def encode(containers: list[dict[str, Any]]) -> str:
    return json.dumps(containers, sort_keys=True, separators=(",", ":"))


def revision_arn(key: str, revision: int) -> str:
    return f"arn:aws:ecs:{release.REGION}:{stack.ACCOUNT}:task-definition/example_project_dev_{key}:{revision}"


def task_definition_state(plan: dict[str, Any], key: str, revision: int, role: str, containers: list[dict[str, Any]]) -> dict[str, Any]:
    """A task definition instance of the state: the known values of the planned one, a revision, a role and the containers given."""
    change = task_definition_changes(plan)[key]["change"]
    attributes = {name: value for name, value in change["after"].items() if stack.fully_known(change["after_unknown"].get(name))}
    arn = revision_arn(key, revision)
    attributes.update(
        arn=arn,
        arn_without_revision=arn.rsplit(":", 1)[0],
        id=arn,
        revision=revision,
        execution_role_arn=role,
        container_definitions=encode(containers),
        skip_destroy=True,
    )
    return {"index_key": key, "schema_version": 1, "attributes": attributes, "sensitive_attributes": []}


def state_of(plan: dict[str, Any], *, later: bool) -> dict[str, Any]:
    """The state before the first apply (``later`` false: old addresses, base revisions) or after it (new addresses, a revision that GitHub registered)."""
    state = stack.old_state(plan)
    service_resource = state["resources"][0]
    shared = shared_redis_secret(plan)
    definitions = []
    for key in stack.SERVING:
        after = task_definition_changes(plan)[key]["change"]["after"]
        if later:
            containers = json.loads(encode(containers_of(after)).replace(release_digest(plan, key), CHANGED_DIGEST))
            definitions.append(task_definition_state(plan, key, LATER_REVISION, release.role("serving_execution"), containers))
        else:
            definitions.append(task_definition_state(plan, key, BASE_REVISION, release.role("task_execution"), as_base(containers_of(after), shared)))
    for instance in service_resource["instances"]:
        key = instance["index_key"]
        instance["attributes"]["task_definition"] = revision_arn(key, LATER_REVISION if later else BASE_REVISION)
    if later:
        service_resource["name"] = "serving"
    state["resources"].append(
        {"mode": "managed", "type": "aws_ecs_task_definition", "name": "service", "provider": service_resource["provider"], "instances": definitions}
    )
    return state


def release_digest(plan: dict[str, Any], key: str) -> str:
    """The image digest that the stack puts in the first container of a serving task definition."""
    image = containers_of(task_definition_changes(plan)[key]["change"]["after"])[0]["image"]
    return str(image.rsplit("@", 1)[-1])


def service_changes(plan: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {change["index"]: change for change in plan["resource_changes"] if change["type"] == "aws_ecs_service" and change.get("index") in stack.SERVING}


def first_apply_problems(plan: dict[str, Any] | None, text: str) -> list[str]:
    """What is wrong with the plan of the first apply. Empty means each service is replaced once and starts on the revision of the stack."""
    if plan is None:
        return [f"the plan failed: {text.strip().splitlines()[-1][:160] if text.strip() else 'no output'}"]
    found = []
    for key, change in service_changes(plan).items():
        if change["change"]["actions"] != ["delete", "create"] or change.get("action_reason") != "replace_by_triggers":
            found.append(
                f"{change['address']} is planned as {'+'.join(change['change']['actions'])} ({change.get('action_reason')}), not replaced by the trigger"
            )
        if change.get("previous_address") != f'aws_ecs_service.service["{key}"]':
            found.append(f"{change['address']} does not come from the old address ({change.get('previous_address')})")
        if not change["change"]["after_unknown"].get("task_definition"):
            found.append(f"{change['address']} does not start on the task definition of the stack")
    for key, change in task_definition_changes(plan).items():
        if key in stack.SERVING:
            if change["change"]["actions"] != ["delete", "create"]:
                found.append(f"{change['address']} is planned as {'+'.join(change['change']['actions'])}, not as a new revision")
            if not change["change"]["after"].get("skip_destroy"):
                found.append(f"{change['address']} would deregister the old revision")
    for change in plan["resource_changes"]:
        if "delete" in change["change"]["actions"] and change["type"] not in ("aws_ecs_service", "aws_ecs_task_definition"):
            found.append(f"{change['address']} is destroyed or replaced")
    return found


def later_apply_problems(plan: dict[str, Any] | None, text: str) -> list[str]:
    """What is wrong with the plan of a later apply. Empty means the stack changes a task definition and the services stay on their released revision."""
    if plan is None:
        return [f"the plan failed: {text.strip().splitlines()[-1][:160] if text.strip() else 'no output'}"]
    found = []
    for change in service_changes(plan).values():
        if change["change"]["actions"] != ["no-op"]:
            found.append(f"{change['address']} is planned as {'+'.join(change['change']['actions'])}, so an owner apply would roll the release back")
        if change.get("previous_address"):
            found.append(f"{change['address']} still moves from {change['previous_address']}")
    for key, change in task_definition_changes(plan).items():
        if key in stack.SERVING and change["change"]["actions"] == ["no-op"]:
            found.append(f"{change['address']} did not change, so this case proves nothing about the trigger")
    return found


def environment() -> dict[str, str]:
    return {
        **release.environment(),
        "PATH": f"{REPO_ROOT / BIN}:/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin",
        "TRANSITION_FIXTURE": str(REPO_ROOT / FIXTURE),
        "CLUSTER": "example-project-dev",
        "RUNNER_TEMP": str(REPO_ROOT / FIXTURE / "tmp"),
        "GITHUB_OUTPUT": str(REPO_ROOT / FIXTURE / "github_output"),
    }


def run_step(service: str, running: dict[str, Any], revision: int) -> dict[str, Any]:
    """Run the release step on a running definition. Return its exit code, its last lines and the definition that it registered (or None)."""
    fixture = REPO_ROOT / FIXTURE
    if fixture.exists():
        shutil.rmtree(fixture)
    (fixture / "definitions").mkdir(parents=True)
    (fixture / "tmp").mkdir()
    arn = revision_arn(service, revision)
    release.write(FIXTURE / "account", stack.ACCOUNT)
    release.write(FIXTURE / "running_arn", arn)
    release.write(FIXTURE / "registered_arn", revision_arn(service, revision + 1))
    release.write(FIXTURE / "definitions" / (arn.replace("/", "_").replace(":", "_") + ".json"), json.dumps(running))
    release.write(BIN / "aws", STAND_IN)
    os.chmod(REPO_ROOT / BIN / "aws", stat.S_IRWXU)
    release.write(STEP, "set -euo pipefail\n" + release_step())
    env = {**environment(), "SERVING_SERVICE": service, "IMAGE_DIGEST": NEW_DIGEST}
    result = run_command("bash", [str(STEP)], cwd=REPO_ROOT, env=env, timeout=120.0)
    registered = fixture / "registered.json"
    return {
        "exit": result.returncode,
        "text": release.text_of(result)[-300:],
        "registered": json.loads(registered.read_text(encoding="utf-8")) if registered.is_file() else None,
    }


def chain_problems(plan: dict[str, Any] | None) -> list[str]:
    """Run the release step on what each service runs after the first apply. Empty means the base revision is refused and the service runs one the step accepts.

    A service that the plan leaves alone keeps its base revision. A service that the plan replaces or updates starts on the task definition of the stack.
    """
    if plan is None:
        return ["no plan to take the new revision from"]
    found = []
    shared = shared_redis_secret(plan)
    changes = service_changes(plan)
    for key in stack.SERVING:
        new = release.planted_definition(plan, key)
        base = copy.deepcopy(new)
        base["containerDefinitions"] = as_base(new["containerDefinitions"], shared)
        base["executionRoleArn"] = release.role("task_execution")
        control = run_step(key, base, BASE_REVISION)
        if key == "model_api" and (control["exit"] != 1 or control["registered"] is not None):
            found.append(f"the release of the base {key} revision should stop at the release check (exit {control['exit']})")
        moved_on = changes[key]["change"]["actions"] != ["no-op"]
        after = run_step(key, new if moved_on else base, LATER_REVISION if moved_on else BASE_REVISION)
        if after["exit"] != 0 or after["registered"] is None:
            found.append(f"the first release of {key} after the apply fails (exit {after['exit']}): {after['text'][-160:]}")
        elif after["registered"]["executionRoleArn"] != release.role("serving_execution"):
            found.append(f"the release of {key} does not register the serving execution role")
    return found


def replace_once(text: str, old: str, new: str) -> str:
    if text.count(old) != 1:
        raise RuntimeError(f"the planted text must occur once: {old[:60]}")
    return text.replace(old, new, 1)


TRIGGER = "    replace_triggered_by = [aws_ecs_task_definition.service[each.key].execution_role_arn]\n"
IGNORE = "    ignore_changes       = [task_definition]\n"
# (name, file, change of the text)
DEFECTS: list[tuple[str, str, Callable[[str], str]]] = [
    ("the services have no trigger", "main.tf", lambda text: replace_once(text, TRIGGER, "")),
    (
        "the trigger watches the whole task definition",
        "main.tf",
        lambda text: replace_once(text, "service[each.key].execution_role_arn]", "service[each.key]]"),
    ),
    (
        "the trigger watches an attribute that never changes",
        "main.tf",
        lambda text: replace_once(text, "service[each.key].execution_role_arn]", "service[each.key].family]"),
    ),
    ("the services no longer ignore their task definition", "main.tf", lambda text: replace_once(text, IGNORE, "    ignore_changes       = []\n")),
    (
        "the serving task definitions keep the shared execution role",
        "main.tf",
        lambda text: replace_once(
            text, "contains(local.serving_keys, each.key) ? var.serving_execution_role_arn : var.execution_role_arn", "var.execution_role_arn"
        ),
    ),
    (
        "the model API task definition keeps the shared Redis password",
        "model_api.tf",
        lambda text: replace_once(text, 'valueFrom = local.secret_arn["model_api_redis_password"] },', 'valueFrom = local.secret_arn["redis_password"] },'),
    ),
]


def checks(directory: Path, output: Path, label: str) -> dict[str, list[str]]:
    """Plan the stack in ``directory`` against the state before and after the first apply and run the three kinds of check."""
    first, first_text = stack.plan_json(directory, None)
    if first is None:
        return {"plan": [f"the plan without a state failed: {first_text.strip().splitlines()[-1][:160]}"]}
    result: dict[str, list[str]] = {}
    for name, later in (("first_apply", False), ("later_apply", True)):
        state = output / f"{label}_{name}.tfstate"
        state.write_text(json.dumps(state_of(first, later=later), indent=2) + "\n", encoding="utf-8")
        work = output / f"{label}_work.tfstate"
        shutil.copyfile(state, work)
        plan, text = stack.plan_json(directory, work)
        (output / f"{label}_{name}_plan.txt").write_text(text, encoding="utf-8")
        result[name] = first_apply_problems(plan, text) if not later else later_apply_problems(plan, text)
        if not later:
            result["release_step"] = chain_problems(plan)
    return result


def run(output: Path, plugin_directory: Path | None, source: Path = stack.STACK) -> dict[str, Any]:
    """Prove the transition on the stack as it is, then plant each defect in a copy and require one check to fail."""
    output = output.absolute()
    output.mkdir(parents=True, exist_ok=False)
    base = output / "stack"
    stack.prepare(source, base, plugin_directory)
    release.write_variables(base)
    report: dict[str, Any] = {"checks": {}, "defects": []}
    try:
        report["checks"] = proof = checks(base, output, "proof")
        proved = all(not found for found in proof.values()) and bool(proof)
        for name, file, mutate in DEFECTS if proved else []:
            planted = output / f"defect_{len(report['defects'])}"
            shutil.copytree(base, planted, ignore=shutil.ignore_patterns("plan.bin"))
            (planted / file).write_text(mutate((planted / file).read_text(encoding="utf-8")), encoding="utf-8")
            found = checks(planted, output, f"defect_{len(report['defects'])}")
            caught = {kind: items[:2] for kind, items in found.items() if items}
            report["defects"].append({"defect": name, "caught": bool(caught), "caught_by": caught})
    finally:
        # The step, the stand-in and the definitions are scratch files of this run.
        shutil.rmtree(REPO_ROOT / FIXTURE, ignore_errors=True)
        shutil.rmtree(REPO_ROOT / BIN, ignore_errors=True)
        (REPO_ROOT / STEP).unlink(missing_ok=True)
    report["passed"] = proved and len(report["defects"]) == len(DEFECTS) and all(item["caught"] for item in report["defects"])
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output-directory", type=Path, required=True, help="A new folder for the plans and the report; it must not exist.")
    parser.add_argument("--plugin-directory", type=Path, default=stack.PROVIDERS if stack.PROVIDERS.is_dir() else None, help="A folder with the AWS provider.")
    parser.add_argument("--stack-directory", type=Path, default=stack.STACK, help="The services stack to plan; defaults to infra/services.")
    arguments = parser.parse_args()
    output = arguments.output_directory.expanduser().absolute()
    report = run(output, arguments.plugin_directory, arguments.stack_directory.expanduser().absolute())
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    sys.stdout.write(f"event=serving_transition_e2e passed={report['passed']}\n")
    for kind, items in report["checks"].items():
        for item in items:
            sys.stdout.write(f"problem in {kind}: {item}\n")
    for item in report["defects"]:
        sys.stdout.write(f"defect caught={item['caught']}: {item['defect']}\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
