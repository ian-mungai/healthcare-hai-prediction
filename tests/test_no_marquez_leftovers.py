"""No Marquez service, image, repository, log group, switch, configuration or code path is left.

OpenMetadata alone holds lineage. These checks fail when a Marquez piece comes back: the Terraform stacks, the launcher, the lineage code, the metadata
ingestion configuration, the images list, the contracts and the guard-less scripts name no Marquez, and the files that only served it stay deleted.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from scripts.infrastructure import openmetadata_persistent_bootstrap as bootstrap
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
ROOTS = (
    ".env.example",
    ".github",
    "README.md",
    "config",
    "dags",
    "data_contracts",
    "dbt",
    "docker-compose.yaml",
    "feature_store",
    "features",
    "infra",
    "monitoring",
    "orchestration",
    "pyproject.toml",
    "scripts",
    "services",
    "tests",
)
SKIPPED_PARTS = frozenset({".terraform", "__pycache__", "node_modules"})
SKIPPED_MARKERS = ("tfstate", "tfplan")
# Files that say a Marquez piece is absent. Everything else names no Marquez at all.
NEGATIVE_FILES = frozenset({"tests/test_no_marquez_leftovers.py", "infra/platform/tests/platform.tftest.hcl"})
GONE = (
    "infra/services/marquez.tf",
    "config/metadata/ingestion/marquez.json",
    "scripts/infrastructure/openmetadata_marquez.py",
    "services/lineage/marquez.yaml.tmpl",
    "services/lineage/db_dump.py",
    "services/lineage/docker-compose.lineage.yaml",
    "services/lineage/docker-compose.checks.yaml",
    "orchestration/lineage_services.py",
    "scripts/orchestration/run_lineage_service_cases.py",
    "scripts/orchestration/run_lineage_services_e2e.py",
    "scripts/orchestration/run_lineage_stack_e2e.py",
)
STACKS = ("infra/platform", "infra/iam", "infra/services")


def tracked_text_files() -> list[Path]:
    found: list[Path] = []
    for entry in ROOTS:
        base = ROOT / entry
        candidates = [base] if base.is_file() else sorted(base.rglob("*")) if base.is_dir() else []
        for path in candidates:
            if not path.is_file() or SKIPPED_PARTS & set(path.parts) or any(marker in path.name for marker in SKIPPED_MARKERS):
                continue
            found.append(path)
    return found


def relative(path: Path) -> str:
    return str(path.relative_to(ROOT))


def test_no_file_or_path_names_marquez() -> None:
    named = [relative(path) for path in tracked_text_files() if "marquez" in relative(path).lower() and relative(path) not in NEGATIVE_FILES]
    check(not named, f"these paths still name Marquez: {named}")


def test_no_file_content_names_marquez() -> None:
    named = []
    for path in tracked_text_files():
        if relative(path) in NEGATIVE_FILES:
            continue
        try:
            text = path.read_text()
        except UnicodeDecodeError:
            continue
        if "marquez" in text.lower():
            named.append(relative(path))
    check(not named, f"these files still name Marquez: {named}")


@pytest.mark.parametrize("name", GONE)
def test_the_files_that_only_served_marquez_stay_deleted(name: str) -> None:
    check(not (ROOT / name).exists(), f"{name} is back")


def test_the_platform_stack_declares_no_marquez_repository_or_log_group() -> None:
    text = "".join(path.read_text() for folder in ("infra/platform",) for path in (ROOT / folder).glob("*.tf"))
    check(not re.search(r"marquez", text, re.IGNORECASE), "the platform stack still declares a Marquez repository, log group or output")
    tests = (ROOT / "infra/platform/tests/platform.tftest.hcl").read_text()
    check("marquez" in tests.lower(), "the platform tests no longer assert that the Marquez repositories and log group are absent")


@pytest.mark.parametrize("stack", STACKS)
def test_no_stack_takes_a_marquez_switch_image_or_secret(stack: str) -> None:
    text = "".join(path.read_text() for path in (ROOT / stack).glob("*.tf"))
    check(not re.search(r"marquez_enabled|marquez_db_password|marquez_web|marquez_api", text, re.IGNORECASE), f"{stack} still takes a Marquez input")


COMPARISON_BOT_NAMES = ("openmetadata_bot_comparison", "comparison-bot", "DataConsumer")
COMPARISON_BOT_REFUSALS = frozenset(
    {
        "infra/iam/tests/openmetadata_persistent.tftest.hcl",
        "infra/services/tests/openmetadata.tftest.hcl",
        "scripts/infrastructure/run_openmetadata_persistent_e2e.py",
        "tests/test_no_marquez_leftovers.py",
    }
)


def test_the_openmetadata_comparison_bot_that_only_the_marquez_comparison_used_is_gone() -> None:
    named = []
    for path in tracked_text_files():
        if relative(path) in COMPARISON_BOT_REFUSALS:
            continue
        try:
            text = path.read_text()
        except UnicodeDecodeError:
            continue
        if any(name in text for name in COMPARISON_BOT_NAMES):
            named.append(relative(path))
    check(not named, f"these files still name the comparison bot, its token parameter or its role: {named}")
    check("comparison" not in bootstrap.CALLER_ROLES, "the bootstrap still creates a comparison caller")
    check(bootstrap.RETIRED_CALLERS == ("comparison",), "the bootstrap no longer retires the comparison bot that earlier releases created")
    for stack in ("infra/iam/openmetadata_persistent.tf", "infra/services/openmetadata_variables.tf"):
        check('"comparison"' not in (ROOT / stack).read_text(), f"{stack} still names a comparison caller")
