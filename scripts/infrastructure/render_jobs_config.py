"""Render jobs Terraform base inputs and reject an unreviewed saved-plan configuration."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from scripts.infrastructure.render_ecs_target import TargetError, read_capture_with_digest
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
PREPARATION_SCHEMA = REPO_ROOT / "data_contracts/infrastructure/jobs_preparation.schema.json"


def jobs_values(storage: dict[str, str]) -> dict[str, str]:
    """Return the six environment-derived base inputs for the jobs root."""
    return {key: storage[key] for key in ("aws_profile", "aws_region", "data_bucket_name", "environment", "expected_account_id", "project_name")}


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
    """Read nonsecret preparation inputs only when their exact bytes match the reviewed digest."""
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
    if root == "jobs" and "enabled_definitions" in values:
        values["enabled_definitions"] = sorted(values["enabled_definitions"])
    return values


def main() -> int:
    """Render private base inputs or inspect the exact saved plan before wrapper dispatch."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--identity-probe", action="store_true")
    parser.add_argument("--preparation-inputs", type=Path)
    parser.add_argument("--preparation-sha256")
    parser.add_argument("--check-plan", type=Path)
    parser.add_argument("--verify-identity", action="store_true")
    arguments = parser.parse_args()
    try:
        root = "identity_probe" if arguments.identity_probe else "jobs"
        prepared = preparation_values(arguments.preparation_inputs, arguments.preparation_sha256, root)
        storage, _ = load_configuration(arguments.env_file)
        values: dict[str, Any] = jobs_values(storage)
        directory = JOBS_DIRECTORY
        if arguments.identity_probe:
            values.pop("data_bucket_name")
            directory /= "identity_probe"
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
    except (ConfigurationError, TargetError) as error:
        parser.error(str(error))
    except (OSError, ValueError, TypeError, AttributeError, KeyError, RecursionError):
        parser.error("The jobs configuration or saved jobs plan is invalid. No Terraform action was run.")
    sys.stdout.write("Verified private jobs inputs.\n" if arguments.check else "Rendered private jobs inputs.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
