"""No Marquez service, image, repository, log group, switch, configuration or code path is left.

OpenMetadata alone holds lineage. These checks fail when a Marquez piece comes back: no file Git tracks or would add names Marquez in its path or its
text, the files that only served it stay deleted, and the stacks take no Marquez input. The scan reads the list of files from Git, so a file is skipped only
when Git ignores it or when its extension is a saved Terraform state or plan; its name is never a reason to skip it. Git ignores only exact artifact
names (see .gitignore), so no source file is hidden; the owner's local inputs (*.tfvars and *.tfvars.json) stay ignored, cannot be
committed and are not judged here.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from pathlib import Path

import pytest

from scripts.infrastructure import openmetadata_persistent_bootstrap as bootstrap
from scripts.process import run_command
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
# Saved Terraform state and plan files are artifacts, matched by their final extension only. A tracked one is refused by the credential-files hook.
ARTIFACT_EXTENSIONS = (".tfstate", ".tfplan", ".tfstate.backup")
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
COMPARISON_BOT_NAMES = ("openmetadata_bot_comparison", "comparison-bot", "DataConsumer")
# The files that refuse the comparison bot or retire it: the two Terraform refusal tests, the bootstrap E2E that seeds the old bot and this guard.
COMPARISON_BOT_REFUSALS = frozenset(
    {
        "infra/iam/tests/openmetadata_persistent.tftest.hcl",
        "infra/services/tests/openmetadata.tftest.hcl",
        "scripts/infrastructure/run_openmetadata_persistent_e2e.py",
        "tests/test_no_marquez_leftovers.py",
    }
)


def listed_files(root: Path) -> list[str]:
    """Every file Git tracks or would add (not ignored) under ``root``, as paths relative to it. A deleted tracked file is not a file."""
    result = run_command("git", ["ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root, timeout=60, check=True)
    names = {name for name in result.stdout.split("\0") if name}
    return sorted(name for name in names if (root / name).is_file() and not name.endswith(ARTIFACT_EXTENSIONS))


def named_files(root: Path, matches: Callable[[str], bool], allowed: Iterable[str]) -> list[str]:
    """The listed files whose path or text satisfies ``matches``, except the allowed ones. A file that is not text is judged by its path only."""
    skipped = frozenset(allowed)
    found = []
    for name in listed_files(root):
        if name in skipped:
            continue
        if matches(name):
            found.append(name)
            continue
        try:
            text = (root / name).read_text()
        except UnicodeDecodeError:
            continue
        if matches(text):
            found.append(name)
    return found


def names_marquez(text: str) -> bool:
    return "marquez" in text.lower()


def names_comparison_bot(text: str) -> bool:
    lowered = text.lower()
    return any(name.lower() in lowered for name in COMPARISON_BOT_NAMES)


def test_no_file_or_path_names_marquez() -> None:
    named = named_files(ROOT, names_marquez, NEGATIVE_FILES)
    check(not named, f"these files or paths still name Marquez: {named}")


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


def test_the_openmetadata_comparison_bot_that_only_the_marquez_comparison_used_is_gone() -> None:
    named = named_files(ROOT, names_comparison_bot, COMPARISON_BOT_REFUSALS)
    check(not named, f"these files still name the comparison bot, its token parameter or its role: {named}")
    check("comparison" not in bootstrap.CALLER_ROLES, "the bootstrap still creates a comparison caller")
    check(bootstrap.RETIRED_CALLERS == ("comparison",), "the bootstrap no longer retires the comparison bot that earlier releases created")
    for stack in ("infra/iam/openmetadata_persistent.tf", "infra/services/openmetadata_variables.tf"):
        check('"comparison"' not in (ROOT / stack).read_text(), f"{stack} still names a comparison caller")


def scratch_repository(folder: Path, ignore_rules: str = "ignored.tfstate\nbuild/\n") -> Path:
    root = folder / "repository"
    root.mkdir()
    run_command("git", ["init", "-q"], cwd=root, timeout=60, check=True)
    (root / ".gitignore").write_text(ignore_rules)
    return root


@pytest.mark.parametrize(
    ("path", "text"),
    [
        ("scripts/runtime_tfplan.py", "URL = 'http://Marquez:5000'\n"),
        ("config/generated_tfstate_config.json", '{"url": "MARQUEZ"}\n'),
        ("legacy/Marquez.py", "URL = 'http://marquez:5000'\n"),
        ("legacy/other.py", "URL = 'http://marquez:5000'\n"),
        ("handbook/notes.md", "The lineage store was Marquez.\n"),
    ],
)
def test_the_scan_finds_marquez_whatever_the_file_name_or_folder(tmp_path: Path, path: str, text: str) -> None:
    root = scratch_repository(tmp_path)
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text)
    check(named_files(root, names_marquez, ()) == [path], f"the scan missed {path}")


@pytest.mark.parametrize(
    ("path", "text"),
    [
        ("scripts/runtime_tfplan.py", "COMPARISON_TOKEN = '/p/openmetadata_bot_comparison'\n"),
        ("config/generated_tfstate_config.json", '{"bot": "comparison-bot"}\n'),
        ("legacy/Comparison_Bot.py", "ROLE = 'DataConsumer'\n"),
    ],
)
def test_the_comparison_bot_scan_finds_the_bot_whatever_the_file_name_or_folder(tmp_path: Path, path: str, text: str) -> None:
    root = scratch_repository(tmp_path)
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text)
    check(named_files(root, names_comparison_bot, ()) == [path], f"the scan missed {path}")


def test_the_scan_skips_only_ignored_files_saved_state_and_plans_and_the_allowed_paths(tmp_path: Path) -> None:
    root = scratch_repository(tmp_path)
    (root / "build").mkdir()
    for path in ("ignored.tfstate", "build/marquez.py", "saved.tfplan", "terraform.tfstate", "backup.tfstate.backup"):
        (root / path).write_text("marquez\n")
    (root / "allowed.md").write_text("marquez\n")
    (root / "kept_tfstate.md").write_text("marquez\n")
    check(named_files(root, names_marquez, ["allowed.md"]) == ["kept_tfstate.md"], "only a name that merely contains tfstate must still be scanned")


# The cases above use minimal ignore rules. These use this repository's own, so a rule that hides source files cannot pass unseen.
@pytest.mark.parametrize(
    ("path", "text"),
    [
        ("scripts/restored.tfstate.py", "URL = 'http://Marquez:5000'\n"),
        ("infra/services/restored.tfstate.tf", "# marquez\n"),
        ("config/restored.tfstate.json", '{"url": "MARQUEZ"}\n'),
        ("scripts/restored.tfstate.1700000000.py", "URL = 'http://marquez:5000'\n"),
    ],
)
def test_the_ignore_rules_of_this_repository_hide_no_source_file_from_the_scan(tmp_path: Path, path: str, text: str) -> None:
    root = scratch_repository(tmp_path, (ROOT / ".gitignore").read_text())
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text)
    check(named_files(root, names_marquez, ()) == [path], f"the ignore rules of this repository hide {path} from the scan")


def test_the_ignore_rules_of_this_repository_still_hide_the_state_and_plan_files(tmp_path: Path) -> None:
    root = scratch_repository(tmp_path, (ROOT / ".gitignore").read_text())
    for path in ("terraform.tfstate", "terraform.tfstate.backup", "terraform.tfstate.1700000000.backup", "saved.tfplan", "reviewed.tfplan.json"):
        (root / path).write_text("marquez\n")
    check(listed_files(root) == [".gitignore"], f"a state or plan file is not ignored: {listed_files(root)}")


def test_a_plan_export_stays_ignored_while_the_look_alike_source_files_stay_visible_to_both_guards(tmp_path: Path) -> None:
    root = scratch_repository(tmp_path, (ROOT / ".gitignore").read_text())
    text = "marquez comparison-bot\n"
    sources = ("scripts/restored.tfstate.py", "infra/services/restored.tfstate.tf")
    for path in ("reviewed.tfplan.json", *sources):
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
    check(listed_files(root) == [".gitignore", *sorted(sources)], f"the listed files are not the two sources and the ignore file: {listed_files(root)}")
    check(named_files(root, names_marquez, ()) == sorted(sources), "the Marquez scan must see both source files and not the plan export")
    check(named_files(root, names_comparison_bot, ()) == sorted(sources), "the comparison bot scan must see both source files and not the plan export")
