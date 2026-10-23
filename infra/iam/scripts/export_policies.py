"""Export the project's customer-managed policies from IAM into policy templates with placeholders. Read-only against AWS.

Run it to see whether IAM drifted from the tracked templates: the exported files replace the account, region, bucket, project and environment values with
``${AWS_ACCOUNT_ID}``, ``${AWS_REGION}``, ``${DATA_BUCKET_NAME}``, ``${PROJECT_NAME}`` and ``${ENVIRONMENT}``. Export to a scratch directory and compare.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

from infra.iam.scripts.manage_policies import GROUP_SERVICES, Names, SessionFactory, connect, local_policies, stored_document
from infra.iam.scripts.render_policy import POLICIES_DIRECTORY, TEMPLATE_PREFIX, PolicyRenderError, placeholder_values
from scripts.infrastructure.render_project_config import REPO_ROOT, ConfigurationError, load_configuration


def sanitize_string(value: str, values: Mapping[str, str]) -> str:
    """Replace the deployment values in one string with their placeholders, longest and most specific first."""
    for name in ("DATA_BUCKET_NAME", "AWS_ACCOUNT_ID", "AWS_REGION", "PROJECT_NAME"):
        value = value.replace(values[name], f"${{{name}}}")
    return re.sub(rf"(?<=[_}}])({re.escape(values['ENVIRONMENT'])})(?![A-Za-z0-9])", "${ENVIRONMENT}", value)


def sanitize(value: Any, values: Mapping[str, str]) -> Any:
    if isinstance(value, dict):
        return {key: sanitize(item, values) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize(item, values) for item in value]
    if isinstance(value, str):
        return sanitize_string(value, values)
    return value


def export_policies(iam: Any, names: Names, values: Mapping[str, str], output: Path) -> list[Path]:
    """Write one template per service; a missing policy stops the export."""
    existing = local_policies(iam)
    written = []
    for service in sorted(service for services in GROUP_SERVICES.values() for service in services):
        name = names.policy(service)
        if name not in existing:
            raise RuntimeError(f"IAM holds no customer-managed policy named {name}")
        target = output / f"{TEMPLATE_PREFIX}{service}_policy.json"
        target.write_text(json.dumps(sanitize(stored_document(iam, existing[name]), values), indent=2) + "\n", encoding="utf-8")
        written.append(target)
    return written


def run(arguments: argparse.Namespace, session_factory: SessionFactory = boto3.Session) -> list[Path]:
    storage, _ = load_configuration(arguments.env_file)
    if arguments.admin_profile == storage["aws_profile"] or not arguments.admin_profile.strip():
        raise ConfigurationError("Pass an administrator profile that is not the project profile.")
    names = Names(storage["project_name"], storage["environment"])
    iam, _ = connect(session_factory, arguments.admin_profile, storage["aws_region"], storage["expected_account_id"], names)
    arguments.output_directory.mkdir(parents=True, exist_ok=True)
    return export_policies(iam, names, placeholder_values(storage), arguments.output_directory)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admin-profile", required=True, help="The administrator AWS profile; it is never read from .env.")
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    parser.add_argument("--output-directory", type=Path, default=POLICIES_DIRECTORY, help="Where to write the templates; use a scratch directory to compare.")
    arguments = parser.parse_args()
    try:
        for path in run(arguments):
            sys.stdout.write(f"Exported {path.name}\n")
    except (ConfigurationError, PolicyRenderError, RuntimeError, ClientError, OSError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
