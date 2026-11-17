"""Render platform, services or alerts Terraform inputs and check saved plans before apply."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from scripts.infrastructure.capture_reader import CaptureError, read_capture_with_digest
from scripts.infrastructure.render_project_config import (
    REPO_ROOT,
    ConfigurationError,
    load_alert_email,
    load_configuration,
    verify_project_identity,
    write_or_check,
)
from scripts.infrastructure.stack_settings import INFRA, project_parameter_values, require_local_override
from scripts.process import TimeoutExpired, run_command

PLATFORM_DIRECTORY = REPO_ROOT / "infra" / "platform"
PREPARATION_SCHEMAS = {
    "platform": REPO_ROOT / "data_contracts/infrastructure/platform_preparation.schema.json",
    "services": REPO_ROOT / "data_contracts/infrastructure/services_preparation.schema.json",
}
TERRAFORM_SECONDS = 120
# Inputs that Terraform declares as sets: its saved plan lists their elements sorted, so the rendered list is sorted too.
SET_INPUTS = frozenset({"ecr_repositories", "log_groups"})
PLATFORM_KEYS = ("aws_profile", "aws_region", "environment", "expected_account_id", "project_name")
SERVICES_PLATFORM_KEYS = (
    "project_tag",
    "cluster_name",
    "ec2_capacity_provider_name",
    "execution_role_arn",
    "polaris_task_role_arn",
    "control_plane_task_role_arn",
    "log_group_names",
    "ecr_repository_urls",
    "data_mount_path",
    "host_private_ip",
)
# The services inputs that hold an account, the project, the environment or the data bucket, so they are built here and never committed. The other services
# settings (the switches, the cap and the loopback addresses) are the committed Terraform defaults.
SERVICES_ROLE_NAMES = {
    "airflow_launch_task_role_arn": "airflow_launch_task",
    "code_fetch_task_role_arn": "host_code_fetch_task",
    "mlflow_task_role_arn": "mlflow_task",
    "model_api_task_role_arn": "model_api_task",
    "openmetadata_task_role_arn": "openmetadata_host_task",
    "serving_execution_role_arn": "serving_execution",
    "streamlit_task_role_arn": "streamlit_task",
}
SERVICES_BUCKET_INPUTS = ("airflow_launch_bucket_name", "code_bucket_name", "mlflow_artifact_bucket")
SERVING_MANIFEST_KEY = "serving/deployments"
FEATURE_REGISTRY_KEY = "lakehouse/feast/registry.pb"


def platform_values(storage: dict[str, str]) -> dict[str, str]:
    """Return the .env-derived inputs the platform stack declares; the data bucket belongs to other stacks."""
    return {key: storage[key] for key in PLATFORM_KEYS}


def terraform_document(directory: Path, arguments: list[str], storage: dict[str, str], failure: str, timeout: float = TERRAFORM_SECONDS) -> dict[str, Any]:
    """Read Terraform JSON with the project profile and report failures without printing subprocess output.

    ``timeout`` is the seconds the lookup may take, at most the 120-second limit. A caller under a command deadline passes the time that remains; when none
    remains, no process starts.
    """
    if timeout <= 0:
        raise ConfigurationError(f"{failure} The command deadline passed before Terraform started.")
    timeout = min(timeout, TERRAFORM_SECONDS)
    environment = os.environ.copy()
    for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN"):
        environment.pop(name, None)
    environment.update(AWS_PROFILE=storage["aws_profile"], AWS_REGION=storage["aws_region"], AWS_DEFAULT_REGION=storage["aws_region"])
    try:
        result = run_command("terraform", [f"-chdir={directory}", *arguments], env=environment, timeout=timeout)
    except TimeoutExpired:
        raise ConfigurationError(f"{failure} Terraform exceeded {timeout:g} seconds.") from None
    except OSError:
        raise ConfigurationError(failure) from None
    if result.returncode:
        raise ConfigurationError(failure)
    try:
        document = json.loads(result.stdout)
    except ValueError:
        raise ConfigurationError(f"{failure} Terraform returned invalid JSON.") from None
    if not isinstance(document, dict):
        raise ConfigurationError(f"{failure} Terraform must return a JSON object.")
    return document


def platform_outputs(storage: dict[str, str]) -> dict[str, Any]:
    """The outputs of the applied platform stack, as ``terraform output -json`` prints them."""
    return terraform_document(PLATFORM_DIRECTORY, ["output", "-json"], storage, "The platform outputs could not be read. Apply the platform stack first.")


def alerts_values(storage: dict[str, str], env_file: Path) -> dict[str, Any]:
    """The inputs of the alerts root: the project keys, the alerts topic from the platform output and the optional address from .env."""
    topic = platform_outputs(storage).get("alerts_topic_arn")
    if not isinstance(topic, dict) or not isinstance(topic.get("value"), str) or not topic["value"]:
        raise ConfigurationError("Missing platform output: alerts_topic_arn. Apply the platform stack before rendering the alerts inputs.")
    return {**platform_values(storage), "alerts_topic_arn": topic["value"], **load_alert_email(env_file)}


def services_environment_values(storage: dict[str, str]) -> dict[str, Any]:
    """The services inputs built from .env: the task role ARNs of the project, the data bucket, the model API addresses inside it and every parameter name."""
    account, project, environment, bucket = (storage[name] for name in ("expected_account_id", "project_name", "environment", "data_bucket_name"))
    roles = {name: f"arn:aws:iam::{account}:role/{project}_{role}_{environment}" for name, role in SERVICES_ROLE_NAMES.items()}
    buckets = dict.fromkeys(SERVICES_BUCKET_INPUTS, bucket)
    return {
        **roles,
        **buckets,
        **project_parameter_values(INFRA / "services", project),
        "model_api_manifest_uri": f"s3://{bucket}/{SERVING_MANIFEST_KEY}",
        "model_api_registry_uri": f"s3://{bucket}/{FEATURE_REGISTRY_KEY}",
    }


def services_values(storage: dict[str, str]) -> dict[str, Any]:
    """Combine project inputs with the ten required platform outputs, preserving map values; the host address is one of them, never typed by hand.

    The task roles, the data bucket and the model API addresses are built from .env, so no reviewed file carries them.
    """
    outputs = platform_outputs(storage)
    values: dict[str, Any] = {**platform_values(storage), **services_environment_values(storage)}
    for name in SERVICES_PLATFORM_KEYS:
        item = outputs.get(name)
        if not isinstance(item, dict) or item.get("value") is None:
            raise ConfigurationError(f"Missing platform output: {name}. Apply the platform stack before rendering services inputs.")
        expected_type = dict if name in ("log_group_names", "ecr_repository_urls") else str
        if not isinstance(item["value"], expected_type):
            raise ConfigurationError(f"Invalid platform output type: {name}. Supply the type declared by the services stack.")
        if expected_type is str and not item["value"]:
            raise ConfigurationError(f"Invalid platform output type: {name}. Supply a nonempty string.")
        if isinstance(item["value"], dict) and not all(isinstance(value, str) for value in item["value"].values()):
            raise ConfigurationError(f"Invalid platform output type: {name}. Supply an object with string values.")
        values[name] = item["value"]
    return values


def stack_preparation(path: Path | None, sha256: str | None, stack: str) -> dict[str, Any]:
    """Read the nonsecret reviewed inputs of the platform or services stack only when their exact bytes match the reviewed digest."""
    if path is None and sha256 is None:
        return {}
    if stack not in PREPARATION_SCHEMAS:
        raise ConfigurationError(f"The {stack} root takes no preparation inputs. No Terraform action was run.")
    if path is None or sha256 is None:
        raise ConfigurationError("Supply both the preparation input path and preparation SHA-256.")
    require_local_override(path, REPO_ROOT)
    try:
        document, actual = read_capture_with_digest(path)
    except CaptureError as error:
        raise ConfigurationError(str(error)) from None
    if sha256 != actual:
        raise ConfigurationError("The preparation SHA-256 differs from the reviewed bytes. No Terraform action was run.")
    schema = json.loads(PREPARATION_SCHEMAS[stack].read_text(encoding="utf-8"))
    if next(Draft202012Validator(schema).iter_errors(document), None) is not None:
        raise ConfigurationError(f"The preparation contract is invalid for the {stack} root. No Terraform action was run.")
    values = dict(document["variables"])
    for name in SET_INPUTS & values.keys():
        values[name] = sorted(values[name])
    return values


def services_preparation(path: Path | None, sha256: str | None) -> dict[str, Any]:
    """The optional one-off overrides of the services inputs, read from the file the command names (the committed defaults and .env hold every other value)."""
    return stack_preparation(path, sha256, "services")


def services_image_values(directory: Path) -> dict[str, object]:
    """Read the separately recorded image digests for saved-plan comparison without rewriting the record."""
    try:
        images = json.loads((directory / "images.auto.tfvars.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise ConfigurationError("Record pushed image digests with push_images record before checking a services plan.") from None
    if not isinstance(images, dict) or set(images) != {"image_digests"} or not isinstance(images["image_digests"], dict):
        raise ConfigurationError("The services image record must contain the image_digests map written by push_images record.")
    return {"image_digests": images["image_digests"]}


def verify_platform_plan(plan: dict, expected: Mapping[str, object], stack: str = "platform") -> None:
    """Accept a complete plan whose rendered inputs match and whose other variables keep their committed defaults."""
    if plan.get("errored") or not plan.get("complete"):
        raise ConfigurationError("Only a complete, successful saved plan may be applied.")
    declared = plan.get("configuration", {}).get("root_module", {}).get("variables", {})
    actual = {name: item.get("value") for name, item in plan.get("variables", {}).items()}
    if set(actual) != set(declared) or not set(expected) <= set(actual):
        raise ConfigurationError(f"Saved plan variables differ from the {stack} stack's declared variables; create and review a fresh plan.")
    for name, value in actual.items():
        if name not in expected and "default" not in declared[name]:
            source = ".env" if stack == "platform" else "its input sources"
            raise ConfigurationError(f"A {stack} variable without a default is not rendered from {source}; create and review a fresh plan.")
        if value != (expected[name] if name in expected else declared[name]["default"]):
            sources = {
                "platform": ".env or the committed defaults",
                "alerts": ".env, platform outputs or the committed defaults",
            }.get(stack, ".env, platform outputs, image digests or the committed defaults")
            raise ConfigurationError(f"Saved plan inputs differ from {sources}; create and review a fresh plan.")


def main() -> None:
    """Render or check platform or services inputs without printing private values."""
    parser = argparse.ArgumentParser(description="Render ignored platform, services or alerts Terraform inputs without printing values.")
    parser.add_argument("--stack", choices=("platform", "services", "alerts"), default="platform")
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--preparation-inputs",
        type=Path,
        help="a named, ignored file of one-off nonsecret overrides of the platform or services stack, never committed to main",
    )
    parser.add_argument("--preparation-sha256", help="the SHA-256 of the overrides file")
    parser.add_argument("--check-plan", type=Path)
    parser.add_argument("--verify-identity", action="store_true", help="Verify that the project profile authenticates as the derived policy user.")
    arguments = parser.parse_args()
    stack = arguments.stack
    directory = REPO_ROOT / "infra" / stack
    try:
        storage, _ = load_configuration(arguments.env_file)
        # The alert address is an input of the alerts root only: no CI plan may hold it, so the platform and services stacks never receive it.
        if stack == "services":
            values: dict[str, Any] = services_values(storage)
        elif stack == "alerts":
            values = alerts_values(storage, arguments.env_file)
        else:
            values = platform_values(storage)
        values.update(stack_preparation(arguments.preparation_inputs, arguments.preparation_sha256, stack))
        if arguments.check_plan:
            if stack == "platform":
                # Platform plan inspection keeps its original environment and error messages.
                try:
                    result = run_command("terraform", [f"-chdir={PLATFORM_DIRECTORY}", "show", "-json", str(arguments.check_plan.resolve())], timeout=120)
                except TimeoutExpired:
                    raise ConfigurationError("The saved plan could not be inspected in time; no apply was run.") from None
                if result.returncode:
                    raise ConfigurationError("The saved plan could not be inspected; no apply was run.")
                plan = json.loads(result.stdout)
            else:
                plan = terraform_document(
                    directory,
                    ["show", "-json", str(arguments.check_plan.resolve())],
                    storage,
                    "The saved plan could not be inspected. No apply was run.",
                )
            expected = {**values, **(services_image_values(directory) if stack == "services" else {})}
            verify_platform_plan(plan, expected, stack)
        if arguments.verify_identity:
            verify_project_identity(storage)
        write_or_check(directory / "deployment.auto.tfvars.json", values, arguments.check)
    except (ConfigurationError, OSError, ValueError) as error:
        parser.error(str(error))
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(f"event={stack}_configuration %(message)s"))
    logger.addHandler(handler)
    try:
        if arguments.verify_identity:
            logger.info("Verified that the project profile authenticates as the derived policy user in the configured account.")
        if arguments.check:
            logger.info("Verified .env-derived %s inputs.", stack)
        else:
            logger.info("Rendered private %s inputs from .env%s.", stack, " and platform outputs" if stack in ("services", "alerts") else "")
    finally:
        logger.removeHandler(handler)


if __name__ == "__main__":
    main()
