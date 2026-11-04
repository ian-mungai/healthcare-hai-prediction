"""Prove that the model API and dashboard services move to their own resource without a destroy or a replacement (owner decision D18).

GitHub registers new task definition revisions of the two serving services and rolls the services to them. So the services stack keeps them in a resource of
their own, ``aws_ecs_service.serving``, with ``ignore_changes = [task_definition]``. They were services of ``aws_ecs_service.service``; ``moved`` blocks carry
the state over.

The run plans the services stack with the real AWS provider and no AWS call: the provider takes fake keys and skips every check, and ``-refresh=false`` keeps
it off the network. The first plan has no state. A state is then built with the two services at their old addresses and a task definition revision that GitHub
registered (:99). The second plan must show both services at the new addresses with that revision untouched and nothing destroyed or replaced. Then each
planted defect (a missing moved block, a dropped ignore_changes, a renamed service) must make the same checks fail. Nothing is applied; no credential is read.

Run: ``.venv/bin/python -m scripts.infrastructure.run_services_move_e2e --output-directory data/e2e/<new folder>``
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from scripts.process import run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
STACK = REPO_ROOT / "infra" / "services"
PROVIDERS = REPO_ROOT / "infra" / ".terraform" / "providers"
ACCOUNT = "111" * 4
RELEASED_REVISION = 99
TIMEOUT_SECONDS = 900.0
SERVING = ("model_api", "streamlit")
OFFLINE_PROVIDER = """provider "aws" {
  profile                     = null
  allowed_account_ids         = null
  region                      = "us-west-2"
  access_key                  = "synthetic"
  secret_key                  = "synthetic"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  skip_region_validation      = true
}
"""
IMAGES = ("postgres", "redis", "polaris", "polaris_admin", "airflow", "marquez", "marquez_web", "mlflow", "model_api", "streamlit")


def variables() -> dict[str, Any]:
    """The inputs of a plan with the model API and the dashboard on, with synthetic names."""
    role = f"arn:aws:iam::{ACCOUNT}:role/example"
    return {
        "aws_profile": "example_project_dev",
        "environment": "dev",
        "project_name": "example_project",
        "project_tag": "example_project",
        "aws_region": "us-west-2",
        "expected_account_id": ACCOUNT,
        "cluster_name": "example-project-dev",
        "ec2_capacity_provider_name": "example-project-dev-host",
        "execution_role_arn": f"{role}-execution",
        "serving_execution_role_arn": f"{role}-serving-execution",
        "polaris_task_role_arn": f"{role}-polaris",
        "control_plane_task_role_arn": f"{role}-control",
        "model_api_task_role_arn": f"{role}-model-api",
        "streamlit_task_role_arn": f"{role}-streamlit",
        "data_mount_path": "/mnt/example-data",
        "marquez_enabled": False,
        "host_memory_mib": 32768,
        "model_api_enabled": True,
        "streamlit_enabled": True,
        "model_api_manifest_uri": "s3://example-data/serving/deployments",
        "model_api_registry_uri": "s3://example-data/lakehouse/feast/registry.pb",
        "model_api_mlflow_uri": "http://127.0.0.1:5100",
        "log_group_names": {name: f"/example/{name}" for name in ("postgres", "redis", "polaris", "airflow", "marquez", "mlflow")},
        "ecr_repository_urls": {name: f"{ACCOUNT}.dkr.ecr.us-west-2.amazonaws.com/example_project/{name}" for name in IMAGES},
        "image_digests": {name: "sha256:" + digit * 64 for name, digit in zip(IMAGES, "abcdef0123", strict=True)},
    }


def terraform(arguments: list[str], cwd: Path, extra_environment: Mapping[str, str] | None = None) -> tuple[int, str]:
    """Run terraform in ``cwd`` with no AWS profile or credential of the caller, and return its exit code and combined text."""
    environment = {
        "PATH": str(Path(sys.executable).parent) + ":/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin",
        "TF_IN_AUTOMATION": "1",
        "TF_INPUT": "0",
        **(extra_environment or {}),
    }
    result = run_command("terraform", arguments, cwd=cwd, env=environment, timeout=TIMEOUT_SECONDS)
    return result.returncode, result.stdout + result.stderr


def prepare(source: Path, destination: Path, plugin_directory: Path | None) -> None:
    """Copy the stack without its tests or local state, add the offline provider and variables, and initialize it."""
    shutil.copytree(source, destination, ignore=shutil.ignore_patterns(".terraform", "tests", "*.tfstate*", "*.tfplan", "ci.tfvars.json"))
    (destination / "provider_override.tf").write_text(OFFLINE_PROVIDER, encoding="utf-8")
    (destination / "inputs.auto.tfvars.json").write_text(json.dumps(variables(), indent=2) + "\n", encoding="utf-8")
    arguments = ["init", "-backend=false", "-input=false", "-no-color"]
    if plugin_directory is not None:
        arguments.append(f"-plugin-dir={plugin_directory}")
    code, text = terraform(arguments, destination)
    if code != 0:
        raise RuntimeError(f"terraform init failed:\n{text[-1500:]}")


def plan_json(stack: Path, state: Path | None) -> tuple[dict[str, Any] | None, str]:
    """Plan without a refresh and return the plan as JSON, or None with the text when the plan fails."""
    arguments = ["plan", "-refresh=false", "-input=false", "-no-color", "-out=plan.bin"]
    if state is not None:
        arguments.append(f"-state={state}")
    code, text = terraform(arguments, stack)
    if code != 0:
        return None, text
    code, shown = terraform(["show", "-json", "plan.bin"], stack)
    if code != 0:
        return None, shown
    return json.loads(shown), text


def fully_known(unknown: Any) -> bool:
    """Whether a value of ``after_unknown`` marks nothing unknown, however deep: a nested block with only known values has a dict or list of false flags."""
    if isinstance(unknown, dict):
        return all(fully_known(value) for value in unknown.values())
    if isinstance(unknown, list):
        return all(fully_known(value) for value in unknown)
    return not unknown


def old_state(first_plan: dict[str, Any]) -> dict[str, Any]:
    """A state with the serving services at their old addresses, built from what the first plan would create, running a revision that GitHub registered."""
    instances = []
    for change in first_plan["resource_changes"]:
        if change["type"] == "aws_ecs_service" and change.get("index") in SERVING:
            key = change["index"]
            known = {name: value for name, value in change["change"]["after"].items() if fully_known(change["change"]["after_unknown"].get(name))}
            known["id"] = f"arn:aws:ecs:us-west-2:{ACCOUNT}:service/example-project-dev/example_project_dev_{key}"
            known["triggers"] = {}
            known["task_definition"] = f"arn:aws:ecs:us-west-2:{ACCOUNT}:task-definition/example_project_dev_{key}:{RELEASED_REVISION}"
            instances.append({"index_key": key, "schema_version": 0, "attributes": known, "sensitive_attributes": []})
    return {
        "version": 4,
        "terraform_version": "1.16.1",
        "serial": 1,
        "lineage": "00000000-0000-0000-0000-000000000000",
        "outputs": {},
        "resources": [
            {
                "mode": "managed",
                "type": "aws_ecs_service",
                "name": "service",
                "provider": 'provider["registry.terraform.io/hashicorp/aws"]',
                "instances": instances,
            }
        ],
    }


def problems(plan: dict[str, Any] | None, text: str) -> list[str]:
    """What is wrong with the second plan. Empty means the services moved, nothing is destroyed or replaced and the released revision stays."""
    if plan is None:
        return [f"the plan failed: {text.strip().splitlines()[-1][:160] if text.strip() else 'no output'}"]
    found = []
    for change in plan["resource_changes"]:
        actions = change["change"]["actions"]
        if "delete" in actions:
            found.append(f"{change['address']} is destroyed or replaced ({'+'.join(actions)})")
        if change["type"] == "aws_ecs_service" and change.get("index") in SERVING:
            if actions != ["no-op"]:
                found.append(f"{change['address']} is planned as {'+'.join(actions)}, not left alone")
            if change.get("previous_address") != f'aws_ecs_service.service["{change["index"]}"]':
                found.append(f"{change['address']} does not come from the old address ({change.get('previous_address')})")
    prior = {
        resource["address"]: resource["values"].get("task_definition")
        for resource in plan["prior_state"].get("values", {}).get("root_module", {}).get("resources", [])
        if resource["type"] == "aws_ecs_service"
    }
    for key in SERVING:
        address = f'aws_ecs_service.serving["{key}"]'
        expected = f"arn:aws:ecs:us-west-2:{ACCOUNT}:task-definition/example_project_dev_{key}:{RELEASED_REVISION}"
        if prior.get(address) != expected:
            found.append(f"{address} did not take over the state of the old address with revision :{RELEASED_REVISION}")
    return found


def replace_once(text: str, old: str, new: str) -> str:
    if text.count(old) != 1:
        raise RuntimeError(f"the planted text must occur once: {old[:60]}")
    return text.replace(old, new, 1)


MOVED_MODEL_API = 'moved {\n  from = aws_ecs_service.service["model_api"]\n  to   = aws_ecs_service.serving["model_api"]\n}\n'
MOVED_STREAMLIT = 'moved {\n  from = aws_ecs_service.service["streamlit"]\n  to   = aws_ecs_service.serving["streamlit"]\n}\n'


def rename_serving(text: str) -> str:
    """Change the service name argument of the serving resource only, which forces a replacement."""
    start = text.index('resource "aws_ecs_service" "serving"')
    head, tail = text[:start], text[start:]
    return head + replace_once(
        tail,
        'name                               = "${local.name_prefix}_${each.key}"',
        'name                               = "${local.name_prefix}_${each.key}_v2"',
    )


DEFECTS: dict[str, Callable[[str], str]] = {
    "the moved block of the model API is missing": lambda text: replace_once(text, MOVED_MODEL_API, ""),
    "the moved block of the dashboard is missing": lambda text: replace_once(text, MOVED_STREAMLIT, ""),
    "both moved blocks are missing": lambda text: replace_once(replace_once(text, MOVED_MODEL_API, ""), MOVED_STREAMLIT, ""),
    "the serving services no longer ignore their task definition": lambda text: replace_once(
        text, "    ignore_changes = [task_definition]\n", "    ignore_changes = []\n"
    ),
    "the serving services ignore the desired count and not the task definition": lambda text: replace_once(
        text, "    ignore_changes = [task_definition]\n", "    ignore_changes = [desired_count]\n"
    ),
    "the serving services are renamed": rename_serving,
}


def run(output: Path, plugin_directory: Path | None, stack: Path = STACK) -> dict[str, Any]:
    """Prove the move, then plant each defect and check that the same checks fail."""
    output.mkdir(parents=True, exist_ok=False)
    base = output / "stack"
    prepare(stack, base, plugin_directory)
    first, text = plan_json(base, None)
    if first is None:
        raise RuntimeError(f"the first plan failed:\n{text[-1500:]}")
    state_path = output / "old.tfstate"
    state_path.write_text(json.dumps(old_state(first), indent=2) + "\n", encoding="utf-8")
    shutil.copyfile(state_path, output / "work.tfstate")
    second, second_text = plan_json(base, output / "work.tfstate")
    moved = problems(second, second_text)
    (output / "second_plan.txt").write_text(second_text, encoding="utf-8")
    report: dict[str, Any] = {"moved_without_destroy": not moved, "problems": moved, "defects": []}
    for name, mutate in DEFECTS.items() if not moved else {}.items():
        planted = output / f"defect_{len(report['defects'])}"
        shutil.copytree(base, planted, ignore=shutil.ignore_patterns("plan.bin"))
        main = planted / "main.tf"
        main.write_text(mutate(main.read_text(encoding="utf-8")), encoding="utf-8")
        shutil.copyfile(state_path, output / "work.tfstate")
        defect_plan, defect_text = plan_json(planted, output / "work.tfstate")
        found = problems(defect_plan, defect_text)
        report["defects"].append({"defect": name, "caught": bool(found), "problems": found[:3]})
    report["passed"] = report["moved_without_destroy"] and len(report["defects"]) == len(DEFECTS) and all(item["caught"] for item in report["defects"])
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output-directory", type=Path, required=True, help="A new folder for the plans and the report; it must not exist.")
    parser.add_argument(
        "--plugin-directory",
        type=Path,
        default=PROVIDERS if PROVIDERS.is_dir() else None,
        help="A folder with the AWS provider; defaults to infra/.terraform/providers.",
    )
    parser.add_argument("--stack-directory", type=Path, default=STACK, help="The services stack to plan; defaults to infra/services.")
    arguments = parser.parse_args()
    report = run(arguments.output_directory.expanduser().absolute(), arguments.plugin_directory, arguments.stack_directory.expanduser().absolute())
    (arguments.output_directory.expanduser().absolute() / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    sys.stdout.write(f"event=services_move_e2e passed={report['passed']} moved_without_destroy={report['moved_without_destroy']}\n")
    for item in report["defects"]:
        sys.stdout.write(f"defect caught={item['caught']}: {item['defect']}\n")
    for line in report["problems"]:
        sys.stdout.write(f"problem: {line}\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
