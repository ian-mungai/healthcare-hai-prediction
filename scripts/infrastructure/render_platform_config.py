"""Render the platform stack's Terraform inputs from .env and check saved platform plans before apply."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from scripts.infrastructure.render_project_config import REPO_ROOT, ConfigurationError, load_configuration, verify_project_identity, write_or_check
from scripts.process import TimeoutExpired, run_command

PLATFORM_DIRECTORY = REPO_ROOT / "infra" / "platform"
PLATFORM_KEYS = ("aws_profile", "aws_region", "environment", "expected_account_id", "project_name")


def platform_values(storage: dict[str, str]) -> dict[str, str]:
    """Return the .env-derived inputs the platform stack declares; the data bucket belongs to other stacks."""
    return {key: storage[key] for key in PLATFORM_KEYS}


def verify_platform_plan(plan: dict, expected: dict[str, str]) -> None:
    """Accept a complete plan whose .env inputs match and whose other variables keep their committed defaults."""
    if plan.get("errored") or not plan.get("complete"):
        raise ConfigurationError("Only a complete, successful saved plan may be applied.")
    declared = plan.get("configuration", {}).get("root_module", {}).get("variables", {})
    actual = {name: item.get("value") for name, item in plan.get("variables", {}).items()}
    if set(actual) != set(declared) or not set(expected) <= set(actual):
        raise ConfigurationError("Saved plan variables differ from the platform stack's declared variables; create and review a fresh plan.")
    for name, value in actual.items():
        if name not in expected and "default" not in declared[name]:
            raise ConfigurationError("A platform variable without a default is not rendered from .env; create and review a fresh plan.")
        if value != (expected[name] if name in expected else declared[name]["default"]):
            raise ConfigurationError("Saved plan inputs differ from .env or the committed defaults; create and review a fresh plan.")


def main() -> None:
    """Render or check platform inputs without printing private values."""
    parser = argparse.ArgumentParser(description="Render ignored platform Terraform inputs from .env without printing values.")
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--check-plan", type=Path)
    parser.add_argument("--verify-identity", action="store_true", help="Verify that the project profile authenticates as the derived policy user.")
    arguments = parser.parse_args()
    try:
        storage, _ = load_configuration(arguments.env_file)
        values = platform_values(storage)
        if arguments.check_plan:
            # Resolve the existing Terraform CLI through the project operator's PATH; never invoke a shell.
            try:
                result = run_command("terraform", [f"-chdir={PLATFORM_DIRECTORY}", "show", "-json", str(arguments.check_plan.resolve())], timeout=120)
            except TimeoutExpired:
                raise ConfigurationError("The saved plan could not be inspected in time; no apply was run.") from None
            if result.returncode:
                raise ConfigurationError("The saved plan could not be inspected; no apply was run.")
            verify_platform_plan(json.loads(result.stdout), values)
        if arguments.verify_identity:
            verify_project_identity(storage)
        write_or_check(PLATFORM_DIRECTORY / "deployment.auto.tfvars.json", values, arguments.check)
    except (ConfigurationError, OSError, ValueError) as error:
        parser.error(str(error))
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("event=platform_configuration %(message)s"))
    logger.addHandler(handler)
    try:
        if arguments.verify_identity:
            logger.info("Verified that the project profile authenticates as the derived policy user in the configured account.")
        logger.info("Verified .env-derived platform inputs." if arguments.check else "Rendered private platform inputs from .env.")
    finally:
        logger.removeHandler(handler)


if __name__ == "__main__":
    main()
