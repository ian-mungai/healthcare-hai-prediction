"""Render private Terraform inputs and verify project identity and saved-plan inputs."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dotenv import dotenv_values

from scripts.process import TimeoutExpired, run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
SECRET_RESOURCE_TYPE = re.compile(r"^aws_(secretsmanager_|ssm_parameter)")
PUBLIC_PARAMETER_PREFIX = "/aws/service/"
PREPARATION_PATH = REPO_ROOT / "config" / "aws" / "iam_preparation.json"
BRONZE_TABLES_PATH = REPO_ROOT / "config" / "lakehouse" / "bronze_tables.json"
PREPARATION_NAMES = frozenset({"bronze_live_locations", "bronze_qualification_collection", "bronze_writer_prepared", "job_writer_e2e_prepared"})
PREPARATION_FLAGS = ("bronze_writer_prepared", "job_writer_e2e_prepared")
COLLECTION_PATTERN = re.compile(r"[a-z0-9_]+/[a-z0-9_]+")
DVC_LOCAL_CONFIG = REPO_ROOT / ".dvc" / "config.local"
DVC_REMOTE_NAME = "data"


class ConfigurationError(ValueError):
    """Report an invalid configuration without echoing private input values."""


def build_configuration(environment: dict[str, str | None], admin_profile: str | None = None) -> tuple[dict[str, str], dict[str, str]]:
    """Validate the documented environment keys and return storage and IAM inputs."""
    rules = {
        "AWS_ACCOUNT_ID": r"[0-9]{12}",
        "AWS_REGION": r"[a-z]{2}-[a-z]+-[1-9]",
        "ENVIRONMENT": r"dev|prod",
        "PROJECT_NAME": r"[a-z][a-z0-9_]{2,79}",
        "S3_BUCKET": r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]",
    }
    if environment.keys() - (rules.keys() | {"AWS_PROFILE"}):
        raise ConfigurationError("Unsupported .env settings; keep only the keys documented in .env.example.")
    for name, pattern in rules.items():
        value = environment.get(name)
        if not isinstance(value, str) or not re.fullmatch(pattern, value) or value.startswith("your-"):
            raise ConfigurationError(f"Set a valid {name} in .env.")
    profile = environment.get("AWS_PROFILE")
    # The project profile and its same-named IAM user are <project>_<environment>, for example example_project_dev.
    if not isinstance(profile, str) or profile != f"{environment['PROJECT_NAME']}_{environment['ENVIRONMENT']}" or len(profile) > 64:
        raise ConfigurationError("Set a valid AWS_PROFILE matching PROJECT_NAME, an underscore and ENVIRONMENT; administrator selection is separate.")
    common = {
        "aws_region": str(environment["AWS_REGION"]),
        "expected_account_id": str(environment["AWS_ACCOUNT_ID"]),
        "data_bucket_name": str(environment["S3_BUCKET"]),
        "project_name": str(environment["PROJECT_NAME"]),
        "environment": str(environment["ENVIRONMENT"]),
    }
    storage = {**common, "aws_profile": profile}
    iam = dict(storage)
    if admin_profile is not None:
        if not admin_profile.strip() or admin_profile == profile:
            raise ConfigurationError("IAM permission updates require an explicitly selected administrator profile.")
        iam["admin_profile"] = admin_profile
    return storage, iam


def load_configuration(path: Path, admin_profile: str | None = None) -> tuple[dict[str, str], dict[str, str]]:
    """Read literal dotenv values without shell expansion and validate them."""
    if not path.is_file():
        raise ConfigurationError("The selected .env file does not exist.")
    return build_configuration(dict(dotenv_values(path, interpolate=False)), admin_profile)


def unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Build a JSON object and reject a repeated key, which would otherwise let the last value win silently."""
    if len({key for key, _ in pairs}) != len(pairs):
        raise ValueError("duplicate key")
    return dict(pairs)


def mapped_collections(path: Path) -> set[object]:
    """The collections the committed bronze table map names."""
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))["tables"]
        return {row["collection"] for row in rows}
    except (OSError, ValueError, KeyError, TypeError):
        raise ConfigurationError("The bronze table map could not be read: config/lakehouse/bronze_tables.json.") from None


def load_preparation(bucket: str, path: Path = PREPARATION_PATH, tables: Path = BRONZE_TABLES_PATH) -> dict[str, object]:
    """Read the committed IAM preparation inputs and refuse anything but the four documented values; no rejected value is echoed."""
    if not path.is_file():
        raise ConfigurationError("The IAM preparation file is missing: config/aws/iam_preparation.json.")
    try:
        document = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_pairs)
    except ValueError:
        document = None
    if not isinstance(document, dict):
        raise ConfigurationError("The IAM preparation file must be one JSON object with unique keys.")
    if document.keys() != PREPARATION_NAMES:
        raise ConfigurationError(f"The IAM preparation file must hold exactly the four inputs: {', '.join(sorted(PREPARATION_NAMES))}.")
    for name in PREPARATION_FLAGS:
        if not isinstance(document[name], bool):
            raise ConfigurationError(f"Set {name} to true or false in the IAM preparation file.")
    collection = document["bronze_qualification_collection"]
    if not isinstance(collection, str) or (collection and not COLLECTION_PATTERN.fullmatch(collection)):
        raise ConfigurationError("Set bronze_qualification_collection to empty or a publisher/collection name in the IAM preparation file.")
    if collection and collection not in mapped_collections(tables):
        raise ConfigurationError(
            "Set bronze_qualification_collection to a mapped collection; that value is not a mapped collection in config/lakehouse/bronze_tables.json."
        )
    locations = {"bronze": f"s3://{bucket}/lakehouse/bronze/", "bronze_dictionary": f"s3://{bucket}/lakehouse/bronze_dictionary/"}
    if document["bronze_live_locations"] not in ({}, locations):
        raise ConfigurationError("Set bronze_live_locations to empty or exactly the two namespace locations of the data bucket in the IAM preparation file.")
    return document


def verify_project_identity(storage: dict[str, str]) -> None:
    """Reject identities outside the configured account and same-named project user."""
    environment = os.environ.copy()
    for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN"):
        environment.pop(name, None)
    command = [
        "aws",
        "sts",
        "get-caller-identity",
        "--profile",
        storage["aws_profile"],
        "--region",
        storage["aws_region"],
        "--output",
        "json",
        "--no-cli-pager",
    ]
    try:
        result = run_command(command[0], command[1:], env=environment, timeout=30)
    except (OSError, TimeoutExpired):
        raise ConfigurationError("Could not verify the project AWS identity; check AWS CLI access and local profile credentials.") from None
    if result.returncode:
        raise ConfigurationError("Could not verify the project AWS identity; check local profile credentials. No plan or apply was run.")
    try:
        identity = json.loads(result.stdout)
    except ValueError:
        raise ConfigurationError("AWS identity verification returned an invalid response.") from None
    if not isinstance(identity, dict):
        raise ConfigurationError("AWS identity verification returned an invalid response.")
    account = storage["expected_account_id"]
    arn = identity.get("Arn")
    matches_user = isinstance(arn, str) and arn.startswith(f"arn:aws:iam::{account}:user/") and arn.rsplit("/", 1)[-1] == storage["aws_profile"]
    if identity.get("Account") != account or not matches_user:
        raise ConfigurationError("The project profile must authenticate as its same-named IAM user in the configured account. No plan or apply was run.")


def dvc_local_config(storage: Mapping[str, str]) -> str:
    """The ignored DVC configuration that names the remote: the ``dvc/`` prefix of the data bucket (decision 20)."""
    return (
        f"[core]\n    remote = {DVC_REMOTE_NAME}\n"
        f"['remote \"{DVC_REMOTE_NAME}\"']\n    url = s3://{storage['data_bucket_name']}/dvc/\n    region = {storage['aws_region']}\n"
    )


def write_or_check(path: Path, values: Mapping[str, object], check: bool = False) -> None:
    """Atomically write owner-only JSON inputs or reject stale generated bytes."""
    write_text_or_check(path, json.dumps(values, indent=2, sort_keys=True) + "\n", check)


def write_text_or_check(path: Path, content: str, check: bool = False) -> None:
    """Atomically write an owner-only file or reject stale generated bytes."""
    if check:
        if not path.is_file() or path.read_text(encoding="utf-8") != content:
            raise ConfigurationError(f"Generated configuration is stale: {path.name}; render it from .env.")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    # Atomic private writes keep account-specific inputs out of partially written files.
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix=".config_", delete=False, encoding="utf-8") as handle:
        temporary = Path(handle.name)
        try:
            os.chmod(temporary, 0o600)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


def verify_plan_variables(plan: dict[str, Any], expected: dict[str, str], preparation: Mapping[str, object] | None = None) -> None:
    """Reject incomplete saved plans, any mismatch in the written inputs or IAM preparation inputs and any other input that is not its committed default."""
    if plan.get("errored") or not plan.get("complete"):
        raise ConfigurationError("Only a complete, successful saved plan may be applied.")
    actual = {name: item.get("value") for name, item in plan.get("variables", {}).items()}
    if {name: value for name, value in actual.items() if name in expected} != expected:
        raise ConfigurationError("Saved plan inputs differ from .env or the selected profile; create and review a fresh plan.")
    wanted = preparation or {}
    if {name: value for name, value in actual.items() if name in wanted} != wanted:
        raise ConfigurationError("Saved plan IAM preparation inputs differ from config/aws/iam_preparation.json; create and review a fresh plan.")
    declared = plan.get("configuration", {}).get("root_module", {}).get("variables", {})
    for name in sorted(actual.keys() - expected.keys() - wanted.keys()):
        if "default" not in declared.get(name, {}) or actual[name] != declared[name]["default"]:
            raise ConfigurationError(f"Saved plan input {name} is not its committed default; create and review a fresh plan.")


def parameter_name(node: dict[str, Any]) -> str | None:
    """The parameter name a plan node declares: its read values, its configuration expression or the planned change, whichever the section has."""
    candidates = (
        (node.get("values") or {}).get("name"),
        ((node.get("expressions") or {}).get("name") or {}).get("constant_value"),
        ((node.get("change") or {}).get("after") or {}).get("name"),
    )
    return next((name for name in candidates if isinstance(name, str)), None)


def public_parameter(node: dict[str, Any]) -> bool:
    """True for a data source that reads a public AWS-published parameter (a name under ``/aws/service/``, such as the ECS image id): it holds no secret."""
    kind, name = node.get("type"), parameter_name(node)
    return node.get("mode") == "data" and kind == "aws_ssm_parameter" and isinstance(name, str) and name.startswith(PUBLIC_PARAMETER_PREFIX)


def secret_resources(node: object) -> list[str]:
    """The address of every Secrets Manager or Parameter Store resource, data source or ephemeral resource anywhere in a saved plan's JSON.

    A data source of a public AWS parameter (``/aws/service/...``) is not listed.
    """
    found: list[str] = []
    if isinstance(node, dict):
        kind = node.get("type")
        if isinstance(kind, str) and SECRET_RESOURCE_TYPE.match(kind) and ("mode" in node or "address" in node) and not public_parameter(node):
            found.append(str(node.get("address") or kind))
        for child in node.values():
            found += secret_resources(child)
    elif isinstance(node, list):
        for child in node:
            found += secret_resources(child)
    return found


def verify_plan_resources(plan: dict[str, Any]) -> None:
    """Reject a saved plan that creates, reads or declares a Secrets Manager secret or a Parameter Store parameter: no secret value enters Terraform."""
    found = sorted(set(secret_resources(plan)))
    if found:
        raise ConfigurationError(
            f"Saved plan declares a secret or parameter resource or data source ({', '.join(found)}); no secret may be managed by Terraform."
        )


def main() -> None:
    """Render or check configuration without disclosing private values."""
    parser = argparse.ArgumentParser(description="Render ignored Terraform inputs from .env without printing values.")
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    parser.add_argument("--stack", choices=("storage", "iam", "both", "dvc"), default="both", help="dvc renders only the ignored .dvc/config.local.")
    parser.add_argument("--dvc-config", type=Path, default=DVC_LOCAL_CONFIG, help="Where --stack dvc writes the DVC remote.")
    parser.add_argument("--admin-profile")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--check-plan", type=Path)
    parser.add_argument("--verify-identity", action="store_true", help="Verify that the project profile authenticates as the derived policy user.")
    arguments = parser.parse_args()
    try:
        storage, iam = load_configuration(arguments.env_file, arguments.admin_profile)
        preparation = load_preparation(storage["data_bucket_name"]) if arguments.stack in ("iam", "both") else None
        selected = {"storage": (REPO_ROOT / "infra", storage), "iam": (REPO_ROOT / "infra" / "iam", iam)}
        if arguments.check_plan:
            if arguments.stack in ("both", "dvc"):
                raise ConfigurationError("Select one Terraform stack when checking a saved plan.")
            directory, values = selected[arguments.stack]
            # Resolve the existing Terraform CLI through the project operator's PATH; never invoke a shell.
            command = ["terraform", f"-chdir={directory}", "show", "-json", str(arguments.check_plan.resolve())]
            try:
                result = run_command(command[0], command[1:], timeout=120)
            except TimeoutExpired:
                raise ConfigurationError("The saved plan could not be inspected in time; no apply was run.") from None
            if result.returncode:
                raise ConfigurationError("The saved plan could not be inspected; no apply was run.")
            plan = json.loads(result.stdout)
            verify_plan_resources(plan)
            verify_plan_variables(plan, values, preparation)
        if arguments.verify_identity:
            verify_project_identity(storage)
        if arguments.stack == "dvc":
            write_text_or_check(arguments.dvc_config, dvc_local_config(storage), arguments.check)
        for name, (directory, values) in selected.items():
            if arguments.stack in (name, "both"):
                stored_values = {key: value for key, value in values.items() if key != "admin_profile"}
                write_or_check(directory / "deployment.auto.tfvars.json", stored_values, arguments.check)
                if name == "iam" and preparation is not None:
                    write_or_check(directory / "preparation.auto.tfvars.json", preparation, arguments.check)
    except (ConfigurationError, OSError, ValueError) as error:
        parser.error(str(error))
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("event=project_configuration %(message)s"))
    logger.addHandler(handler)
    try:
        if arguments.verify_identity:
            logger.info("Verified that the project profile authenticates as the derived policy user in the configured account.")
        subject = "DVC remote configuration" if arguments.stack == "dvc" else "Terraform inputs"
        logger.info(f"Verified .env-derived {subject}." if arguments.check else f"Rendered private {subject} from .env.")
    finally:
        logger.removeHandler(handler)


if __name__ == "__main__":
    main()
