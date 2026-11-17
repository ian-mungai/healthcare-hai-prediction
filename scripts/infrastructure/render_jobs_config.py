"""Render jobs Terraform base inputs and reject an unreviewed saved-plan configuration."""

from __future__ import annotations

import argparse
import ipaddress
import json
import re
import sys
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from scripts.infrastructure.capture_reader import CaptureError, read_capture_with_digest
from scripts.infrastructure.render_project_config import (
    REPO_ROOT,
    ConfigurationError,
    load_configuration,
    verify_plan_resources,
    verify_plan_variables,
    verify_project_identity,
    write_or_check,
)
from scripts.process import TimeoutExpired, run_command

JOBS_DIRECTORY = REPO_ROOT / "infra/jobs"
SERVICES_DIRECTORY = REPO_ROOT / "infra/services"
PLATFORM_DIRECTORY = REPO_ROOT / "infra/platform"
PRIVATE_NETWORKS = tuple(ipaddress.IPv4Network(block) for block in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"))
JOB_ROOM_FLOOR_MIB = 4096
# The services output host_capacity_binding: the job memory (the host job cap) and the usable-memory inputs and reservations it must fit. The jobs stack
# labels the init container of every host revision with it, and the launch preflight runs a host job only from a revision bound to the current inputs.
BINDING_FORM = re.compile(
    r"job_memory_mib=(?P<job>[1-9][0-9]*) usable_memory_mib=(?P<usable>[1-9][0-9]*) system_reserve_mib=(?P<reserve>[0-9]+) "
    r"service_reservations_mib=(?P<services>[0-9]+)"
)
PREPARATION_SCHEMA = REPO_ROOT / "data_contracts/infrastructure/jobs_preparation.schema.json"
# The pushed image digests that push_images record keeps for the services stack: the one record of what ECR holds.
IMAGE_RECORD = SERVICES_DIRECTORY / "images.auto.tfvars.json"
DIGEST_FORM = re.compile(r"sha256:[0-9a-f]{64}")
# The jobs inputs that hold an account, the project or the environment, so they are built here and never committed. The job switches, the host job families,
# the host repository path and the serve prefix are the committed Terraform defaults.
JOB_ROLES = {
    "job_role_arns": ("execution", "publisher_task", "write_silver_task", "write_gold_task", "write_features_task"),
    "train_roles": ("execution", "train_task"),
    "drift_roles": ("execution", "drift_task"),
    "serve_roles": ("serve_task",),
    "openmetadata_roles": ("execution", "openmetadata_task"),
}
WRITER_LAYERS = ("silver", "gold", "features")
# Each image input maps its keys to the repository, named <project>_<repository>_<environment>, whose digest the record holds.
JOB_IMAGES = {
    "images": {"analytics": "analytics", "quality": "quality", "spark": "spark"},
    "train_images": {"training": "training", "feature_store": "feature_store"},
    "drift_images": {"training": "training", "feature_store": "feature_store", "drift": "drift"},
    "openmetadata_images": {"ingestion": "openmetadata_ingestion"},
}


def jobs_values(storage: dict[str, str]) -> dict[str, str]:
    """Return the six environment-derived base inputs for the jobs root."""
    return {key: storage[key] for key in ("aws_profile", "aws_region", "data_bucket_name", "environment", "expected_account_id", "project_name")}


def image_digests(path: Path | None = None) -> dict[str, str]:
    """The recorded image digests by repository name, or refuse; the record is the one push_images writes after a push."""
    refusal = ConfigurationError("The image digests could not be read. Record them with push_images record, then render again. No Terraform action was run.")
    try:
        record = json.loads((path or IMAGE_RECORD).read_text(encoding="utf-8"))["image_digests"]
    except (OSError, ValueError, KeyError, TypeError):
        raise refusal from None
    if not isinstance(record, dict):
        raise refusal
    return {str(name): str(digest) for name, digest in record.items()}


def job_images(storage: dict[str, str], digests: dict[str, str]) -> dict[str, dict[str, str]]:
    """The image inputs of the jobs stack: each repository of the project in the account, pinned by its recorded digest."""
    registry = f"{storage['expected_account_id']}.dkr.ecr.{storage['aws_region']}.amazonaws.com/{storage['project_name']}"
    inputs: dict[str, dict[str, str]] = {}
    for group, members in JOB_IMAGES.items():
        inputs[group] = {}
        for key, repository in members.items():
            digest = digests.get(repository)
            if digest is None or not DIGEST_FORM.fullmatch(digest):
                raise ConfigurationError(
                    f"No valid digest is recorded for the {repository} image. Record it with push_images record, then render again. No Terraform action ran."
                )
            inputs[group][key] = f"{registry}_{repository}_{storage['environment']}@{digest}"
    return inputs


def environment_values(storage: dict[str, str], digests: dict[str, str]) -> dict[str, Any]:
    """The jobs inputs built from .env and the recorded digests: the roles, the log group, the writer parameter names and the images."""
    account, project, environment = storage["expected_account_id"], storage["project_name"], storage["environment"]
    values: dict[str, Any] = {
        group: {name: f"arn:aws:iam::{account}:role/{project}_job_{name}_{environment}" for name in names} for group, names in JOB_ROLES.items()
    }
    values["log_group_name"] = f"{project}_jobs_{environment}"
    values["writer_parameter_names"] = {
        f"write_{layer}_task": [f"/{project}/aws_polaris_{layer}_client_{part}" for part in ("id", "secret")] for layer in WRITER_LAYERS
    }
    return values | job_images(storage, digests)


def host_job_room(directory: Path = SERVICES_DIRECTORY) -> tuple[int, str]:
    """Return the memory MiB one host job (the publisher too) gets and the binding it comes from, read from the services stack output, or refuse."""
    try:
        result = run_command("terraform", [f"-chdir={directory}", "output", "-json", "host_capacity_binding"], timeout=120)
    except (OSError, TimeoutExpired):
        raise ConfigurationError("The services job room could not be read within 120 seconds. No Terraform action was run.") from None
    try:
        binding = json.loads(result.stdout) if result.returncode == 0 else None
    except ValueError:
        binding = None
    form = BINDING_FORM.fullmatch(binding) if isinstance(binding, str) else None
    room = int(form["job"]) if form is not None else 0
    if form is None or room > int(form["usable"]) - int(form["reserve"]) - int(form["services"]) or room < JOB_ROOM_FLOOR_MIB:
        raise ConfigurationError(
            "Host jobs need the job room from the services stack output host_capacity_binding (job_memory_mib of at least 4096 MiB, at most "
            "usable_memory_mib less system_reserve_mib and service_reservations_mib). "
            "Apply the services stack, then render again. No Terraform action was run."
        )
    return room, str(binding)


def host_private_ip(directory: Path = PLATFORM_DIRECTORY) -> str:
    """Return the host's private address from the platform stack output host_private_ip, or refuse; no file carries it."""
    refusal = ConfigurationError(
        "The metadata host job needs the host's private address from the platform stack output host_private_ip "
        "(a private IPv4 address). Apply the platform stack, then render again. No Terraform action was run."
    )
    try:
        result = run_command("terraform", [f"-chdir={directory}", "output", "-json", "host_private_ip"], timeout=120)
    except (OSError, TimeoutExpired):
        raise refusal from None
    try:
        address = json.loads(result.stdout) if result.returncode == 0 else None
    except ValueError:
        address = None
    try:
        parsed = ipaddress.IPv4Address(address) if isinstance(address, str) else None
    except ValueError:
        parsed = None
    if parsed is None or str(parsed) != address or not any(parsed in network for network in PRIVATE_NETWORKS):
        raise refusal
    return str(address)


def alerts_topic_arn(storage: dict[str, str], directory: Path = PLATFORM_DIRECTORY) -> str:
    """Return the platform alerts topic from the platform stack output alerts_topic_arn, or refuse; no file carries it.

    Only this project's topic of the environment, region and account is accepted, the same one the alerts and IAM stacks accept.
    """
    refusal = ConfigurationError(
        "The promote host job needs the alerts topic of this project, environment, region and account from the platform stack output "
        "alerts_topic_arn. Apply the platform stack, then render again. No Terraform action was run."
    )
    try:
        result = run_command("terraform", [f"-chdir={directory}", "output", "-json", "alerts_topic_arn"], timeout=120)
    except (OSError, TimeoutExpired):
        raise refusal from None
    try:
        topic = json.loads(result.stdout) if result.returncode == 0 else None
    except ValueError:
        topic = None
    expected = f"arn:aws:sns:{storage['aws_region']}:{storage['expected_account_id']}:{storage['project_name']}_alerts_{storage['environment']}"
    if topic != expected:
        raise refusal
    return expected


def verify_jobs_plan(plan: dict[str, Any], expected: dict[str, Any]) -> None:
    """Reject secret resources, incomplete variable sets and values outside the rendered inputs or committed defaults."""
    try:
        verify_plan_resources(plan)
        verify_plan_variables(plan, expected)
        declared = plan.get("configuration", {}).get("root_module", {}).get("variables", {})
        if set(plan.get("variables", {})) != set(declared) or not set(expected) <= set(declared):
            raise ConfigurationError("The declared variables differ.")
    except (ConfigurationError, TypeError, AttributeError, KeyError):
        raise ConfigurationError("The saved jobs plan is incomplete, changes approved inputs or declares a secret. Create and review a fresh plan.") from None


def preparation_values(path: Path | None, sha256: str | None, root: str) -> dict[str, Any]:
    """Read the one-off nonsecret overrides of the named file only when its exact bytes match the given digest."""
    if path is None and sha256 is None:
        return {}
    if path is None or sha256 is None:
        raise ConfigurationError("Supply both the preparation input path and preparation SHA-256.")
    document, actual_sha256 = read_capture_with_digest(path)
    if sha256 != actual_sha256:
        raise ConfigurationError("The preparation SHA-256 differs from the reviewed bytes. No Terraform action was run.")
    schema = json.loads(PREPARATION_SCHEMA.read_text(encoding="utf-8"))
    if document.get("root") != root or next(Draft202012Validator(schema).iter_errors(document), None) is not None:
        raise ConfigurationError("The preparation contract is invalid for the selected jobs root. No Terraform action was run.")
    values = dict(document["variables"])
    return values


def main() -> int:
    """Render private base inputs or inspect the exact saved plan before wrapper dispatch."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--preparation-inputs", type=Path, help="a named, ignored file of one-off nonsecret overrides, never committed to main")
    parser.add_argument("--preparation-sha256", help="the SHA-256 of the overrides file")
    parser.add_argument("--check-plan", type=Path)
    parser.add_argument("--verify-identity", action="store_true")
    arguments = parser.parse_args()
    try:
        root = "jobs"
        prepared = preparation_values(arguments.preparation_inputs, arguments.preparation_sha256, root)
        storage, _ = load_configuration(arguments.env_file)
        values: dict[str, Any] = jobs_values(storage)
        directory = JOBS_DIRECTORY
        values.update(environment_values(storage, image_digests()))
        values["host_job_memory_mib"], values["host_capacity_binding"] = host_job_room()
        values["host_private_ip"] = host_private_ip()
        values["alerts_topic_arn"] = alerts_topic_arn(storage)
        values.update(prepared)
        if arguments.check_plan:
            try:
                result = run_command("terraform", [f"-chdir={directory}", "show", "-json", str(arguments.check_plan.resolve())], timeout=120)
            except (OSError, TimeoutExpired):
                raise ConfigurationError("The saved jobs plan could not be inspected within 120 seconds. No apply was run.") from None
            if result.returncode:
                raise ConfigurationError("The saved jobs plan could not be inspected. No apply was run.")
            plan = json.loads(result.stdout)
            if not isinstance(plan, dict):
                raise ConfigurationError("The saved jobs plan must contain one plan object.")
            verify_jobs_plan(plan, values)
        if arguments.verify_identity:
            verify_project_identity(storage)
        write_or_check(directory / "deployment.auto.tfvars.json", values, arguments.check)
    except (ConfigurationError, CaptureError) as error:
        parser.error(str(error))
    except (OSError, ValueError, TypeError, AttributeError, KeyError, RecursionError):
        parser.error("The jobs configuration or saved jobs plan is invalid. No Terraform action was run.")
    sys.stdout.write("Verified private jobs inputs.\n" if arguments.check else "Rendered private jobs inputs.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
