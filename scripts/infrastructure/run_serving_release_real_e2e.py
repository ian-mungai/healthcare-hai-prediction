"""Run the serving release check on the task definitions that the services stack really plans (owner decision D18).

A hand-written fixture can agree with the check while the stack disagrees with both. So this plans the services stack with the real AWS provider and no AWS
call (fake keys, every check skipped), takes the planned model API and dashboard task definitions as the stack renders them, and feeds them to
``.github/scripts/serving_release.sh``:

1. The definition of each service passes the check, and so does the revision that ``render`` builds from it with a new image digest.
2. Each planted defect in the stack makes the check exit 1: the shared Redis password put back, a control-plane secret, the shared execution role, a foreign
   image repository, and a task role of another service.

Nothing is applied and no credential is read.
Run: ``.venv/bin/python -m scripts.infrastructure.run_serving_release_real_e2e --output-directory data/e2e/<folder>``
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from scripts.infrastructure import proof_copies
from scripts.process import run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
PROJECT = "example_project"
ENVIRONMENT = "dev"
ACCOUNT = "111" * 4
REGION = "us-west-2"
SERVICES = ("model_api", "streamlit")
STACK = REPO_ROOT / "infra" / "services"
PROVIDERS = REPO_ROOT / "infra" / ".terraform" / "providers"
TIMEOUT_SECONDS = 900.0
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
        "mlflow_enabled": False,
        "monitoring_enabled": False,
        "openmetadata_persistent_enabled": False,
        "airflow_lineage_http_enabled": False,
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
    # Only what terraform needs: the caller's PATH (CI puts the pinned terraform there) and the provider cache. No AWS profile, key or token passes.
    kept = {name: os.environ[name] for name in ("PATH", "HOME", "TMPDIR", "TF_PLUGIN_CACHE_DIR") if name in os.environ}
    environment = {**kept, "TF_IN_AUTOMATION": "1", "TF_INPUT": "0", **(extra_environment or {})}
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
    elif (destination / ".terraform.lock.hcl").is_file():
        arguments.append("-lockfile=readonly")
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


def role(name: str) -> str:
    return f"arn:aws:iam::{ACCOUNT}:role/{PROJECT}_{name}_{ENVIRONMENT}"


def release_variables() -> dict[str, Any]:
    """The stack inputs with the names the release check expects: the serving roles, the serving execution role and one repository per image."""
    values = variables()
    values["execution_role_arn"] = role("task_execution")
    values["serving_execution_role_arn"] = role("serving_execution")
    values["model_api_task_role_arn"] = role("model_api_task")
    values["streamlit_task_role_arn"] = role("streamlit_task")
    for name in SERVICES:
        values["ecr_repository_urls"][name] = f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/{PROJECT}_{name}_{ENVIRONMENT}"
    return values


def planted_definition(plan: dict[str, Any], service: str) -> dict[str, Any]:
    """The planned task definition of a service in the shape of ``aws ecs describe-task-definition --query taskDefinition``."""
    for change in plan["resource_changes"]:
        if change["address"] == f'aws_ecs_task_definition.service["{service}"]':
            after = change["change"]["after"]
            return {
                "family": after["family"],
                "taskRoleArn": after["task_role_arn"],
                "executionRoleArn": after["execution_role_arn"],
                "networkMode": after["network_mode"],
                "requiresCompatibilities": after["requires_compatibilities"],
                "volumes": [],
                "containerDefinitions": json.loads(after["container_definitions"]),
            }
    raise RuntimeError(f"the plan holds no task definition for {service}")


# The check starts with literal arguments only (the docker-calls guard proves them), so the definition goes to fixed files that the runner writes first.
SCRIPT = Path(".github/scripts/serving_release.sh")
INPUT = Path("data/e2e/.serving_release_input.json")
RENDERED = Path("data/e2e/.serving_release_rendered.json")


def environment() -> dict[str, str]:
    return {
        "PATH": "/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin",
        "PROJECT_NAME": PROJECT,
        "ENVIRONMENT": ENVIRONMENT,
        "AWS_ACCOUNT_ID": ACCOUNT,
        "AWS_REGION": REGION,
    }


def write(path: Path, text: str) -> None:
    (REPO_ROOT / path).parent.mkdir(parents=True, exist_ok=True)
    (REPO_ROOT / path).write_text(text, encoding="utf-8")


def text_of(result: Any) -> str:
    return (result.stdout + result.stderr).strip()


def check(definition: dict[str, Any], service: str) -> tuple[int, str]:
    """Run the check on a definition with the names of the synthetic stack; return its exit code and text."""
    write(INPUT, json.dumps(definition))
    if service == "model_api":
        result = run_command(
            "bash", [str(SCRIPT), "check", "model_api", "data/e2e/.serving_release_input.json"], cwd=REPO_ROOT, env=environment(), timeout=60.0
        )
    else:
        result = run_command(
            "bash", [str(SCRIPT), "check", "streamlit", "data/e2e/.serving_release_input.json"], cwd=REPO_ROOT, env=environment(), timeout=60.0
        )
    write(INPUT, "")
    return result.returncode, text_of(result)


def render_and_check(definition: dict[str, Any]) -> tuple[int, str]:
    """Render the next revision of the model API from a definition with a new digest, then check what comes out."""
    write(INPUT, json.dumps(definition))
    rendered = run_command(
        "bash",
        [str(SCRIPT), "render", "model_api", "sha256:5555555555555555555555555555555555555555555555555555555555555555", "data/e2e/.serving_release_input.json"],
        cwd=REPO_ROOT,
        env=environment(),
        timeout=60.0,
    )
    write(INPUT, "")
    if rendered.returncode != 0:
        return rendered.returncode, text_of(rendered)
    write(RENDERED, rendered.stdout)
    result = run_command("bash", [str(SCRIPT), "check", "model_api", "data/e2e/.serving_release_rendered.json"], cwd=REPO_ROOT, env=environment(), timeout=60.0)
    write(RENDERED, "")
    return result.returncode, text_of(result)


def plan_definitions(directory: Path) -> dict[str, dict[str, Any]] | str:
    """The planned definitions of the two services, or the plan error text."""
    plan, text = plan_json(directory, None)
    if plan is None:
        return text
    return {service: planted_definition(plan, service) for service in SERVICES}


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    if text.count(old) != 1:
        raise RuntimeError(f"the planted text must occur once in {path.name}: {old[:60]}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def write_variables(directory: Path, change: Callable[[dict[str, Any]], None] | None = None) -> None:
    values = release_variables()
    if change is not None:
        change(values)
    (directory / "inputs.auto.tfvars.json").write_text(json.dumps(values, indent=2) + "\n", encoding="utf-8")


def foreign_image(values: dict[str, Any]) -> None:
    values["ecr_repository_urls"]["model_api"] = f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/{PROJECT}_airflow_{ENVIRONMENT}"


def host_role(values: dict[str, Any]) -> None:
    values["model_api_task_role_arn"] = role("ecs_host")


# (name, file to change, text, replacement, change of the inputs)
DEFECTS: list[tuple[str, str, str, str, Callable[[dict[str, Any]], None] | None]] = [
    (
        "the shared Redis password is put back into the model API",
        "model_api.tf",
        'valueFrom = local.secret_arn["model_api_redis_password"] },',
        'valueFrom = local.secret_arn["redis_password"] },',
        None,
    ),
    (
        "the model API also injects the PostgreSQL password",
        "model_api.tf",
        '        { name = "MODEL_API_KEY_HASHES"',
        '        { name = "PG_PASSWORD", valueFrom = local.secret_arn["postgres_password"] },\n        { name = "MODEL_API_KEY_HASHES"',
        None,
    ),
    (
        "the serving tasks name the shared execution role",
        "main.tf",
        "contains(local.serving_keys, each.key) ? var.serving_execution_role_arn : var.execution_role_arn",
        "var.execution_role_arn",
        None,
    ),
    ("the model API image comes from another repository", "main.tf", "  all_containers      = concat(", "  all_containers      = concat(", foreign_image),
    ("the model API runs under the host role", "main.tf", "  all_containers      = concat(", "  all_containers      = concat(", host_role),
]


def run(output: Path, plugin_directory: Path | None, source: Path = STACK) -> dict[str, Any]:
    """Check the real definitions, then plant each defect in a copy of the stack and require the check to refuse the result."""
    output = output.absolute()
    output.mkdir(parents=True, exist_ok=False)
    base = output / "stack"
    prepare(source, base, plugin_directory)
    write_variables(base)
    planned = plan_definitions(base)
    report: dict[str, Any] = {"cases": [], "defects": []}
    if isinstance(planned, str):
        report["cases"].append({"case": "the stack plans", "passed": False, "detail": planned.strip().splitlines()[-1][:160] if planned.strip() else ""})
    else:
        for service, definition in planned.items():
            code, text = check(definition, service)
            report["cases"].append({"case": f"the planned {service} definition passes the check", "passed": code == 0, "detail": text[-160:]})
        code, rendered = render_and_check(planned["model_api"])
        report["cases"].append(
            {"case": "the revision rendered from the planned model API definition passes the check", "passed": code == 0, "detail": rendered[-160:]}
        )
    cases_pass = bool(report["cases"]) and all(item["passed"] for item in report["cases"])
    for name, file, old, new, change in DEFECTS if cases_pass else []:
        planted = output / f"defect_{len(report['defects'])}"
        shutil.copytree(base, planted, ignore=shutil.ignore_patterns("plan.bin"))
        if old != new:
            replace_once(planted / file, old, new)
        write_variables(planted, change)
        definitions = plan_definitions(planted)
        if isinstance(definitions, str):
            report["defects"].append({"defect": name, "caught": True, "detail": "the stack no longer plans"})
            proof_copies.discard_copy(planted, keep=False)
            continue
        code, text = check(definitions["model_api"], "model_api")
        report["defects"].append({"defect": name, "caught": code == 1, "detail": text[-160:]})
        proof_copies.discard_copy(planted, keep=code != 1)
    report["passed"] = (
        bool(report["cases"])
        and all(item["passed"] for item in report["cases"])
        and len(report["defects"]) == len(DEFECTS)
        and all(item["caught"] for item in report["defects"])
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output-directory", type=Path, required=True, help="A new folder for the plans and the report; it must not exist.")
    parser.add_argument("--plugin-directory", type=Path, default=PROVIDERS if PROVIDERS.is_dir() else None, help="A folder with the AWS provider.")
    parser.add_argument("--stack-directory", type=Path, default=STACK, help="The services stack to plan; defaults to infra/services.")
    arguments = parser.parse_args()
    output = arguments.output_directory.expanduser().absolute()
    report = run(output, arguments.plugin_directory, arguments.stack_directory.expanduser().absolute())
    if (output / "stack").is_dir():
        proof_copies.discard_copy(output / "stack", keep=not report["passed"])
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    sys.stdout.write(f"event=serving_release_real_e2e passed={report['passed']}\n")
    for item in report["cases"]:
        sys.stdout.write(f"case passed={item['passed']}: {item['case']} {item['detail'] if not item['passed'] else ''}\n")
    for item in report["defects"]:
        sys.stdout.write(f"defect caught={item['caught']}: {item['defect']}\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
