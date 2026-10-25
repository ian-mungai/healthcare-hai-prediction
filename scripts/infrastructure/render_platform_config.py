"""Render platform or services Terraform inputs and check saved plans before apply."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from scripts.infrastructure.render_project_config import REPO_ROOT, ConfigurationError, load_configuration, verify_project_identity, write_or_check
from scripts.process import TimeoutExpired, run_command

PLATFORM_DIRECTORY = REPO_ROOT / "infra" / "platform"
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
)


def platform_values(storage: dict[str, str]) -> dict[str, str]:
    """Return the .env-derived inputs the platform stack declares; the data bucket belongs to other stacks."""
    return {key: storage[key] for key in PLATFORM_KEYS}


def terraform_document(directory: Path, arguments: list[str], storage: dict[str, str], failure: str) -> dict[str, Any]:
    """Read Terraform JSON with the project profile and report failures without printing subprocess output."""
    environment = os.environ.copy()
    for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN"):
        environment.pop(name, None)
    environment.update(AWS_PROFILE=storage["aws_profile"], AWS_REGION=storage["aws_region"], AWS_DEFAULT_REGION=storage["aws_region"])
    try:
        result = run_command("terraform", [f"-chdir={directory}", *arguments], env=environment, timeout=120)
    except TimeoutExpired:
        raise ConfigurationError(f"{failure} Terraform exceeded 120 seconds.") from None
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


def services_values(storage: dict[str, str]) -> dict[str, Any]:
    """Combine project inputs with the nine required platform outputs, preserving map values."""
    outputs = terraform_document(PLATFORM_DIRECTORY, ["output", "-json"], storage, "The platform outputs could not be read. Apply the platform stack first.")
    values: dict[str, Any] = dict(platform_values(storage))
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
            sources = ".env or the committed defaults" if stack == "platform" else ".env, platform outputs, image digests or the committed defaults"
            raise ConfigurationError(f"Saved plan inputs differ from {sources}; create and review a fresh plan.")


def main() -> None:
    """Render or check platform or services inputs without printing private values."""
    parser = argparse.ArgumentParser(description="Render ignored platform or services Terraform inputs without printing values.")
    parser.add_argument("--stack", choices=("platform", "services"), default="platform")
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--check-plan", type=Path)
    parser.add_argument("--verify-identity", action="store_true", help="Verify that the project profile authenticates as the derived policy user.")
    arguments = parser.parse_args()
    stack = arguments.stack
    directory = REPO_ROOT / "infra" / stack
    try:
        storage, _ = load_configuration(arguments.env_file)
        values: dict[str, Any] = services_values(storage) if stack == "services" else platform_values(storage)
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
            logger.info("Rendered private %s inputs from .env%s.", stack, " and platform outputs" if stack == "services" else "")
    finally:
        logger.removeHandler(handler)


if __name__ == "__main__":
    main()
