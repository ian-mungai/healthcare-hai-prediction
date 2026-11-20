"""The repository's ignore rules hide Terraform state, backups, saved plans and plan exports, and hide no source file whose name merely contains ``tfstate``.

Each case copies the repository's own ``.gitignore`` into a scratch Git repository and reads Git's list of tracked or not ignored files. A rule that is too
wide hides a source file from every Git-based scan and from review. A rule that is missing lets a state file or a saved plan reach a commit.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.process import run_command
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
# The artifacts Terraform writes. Every one of them must stay ignored.
ARTIFACTS = ("terraform.tfstate", "terraform.tfstate.backup", "terraform.tfstate.1700000000.backup", "saved.tfplan", "reviewed.tfplan.json")
# Source files whose names contain the state extension in the middle. Every one of them must stay visible to Git.
SOURCES = ("scripts/restored.tfstate.py", "infra/services/restored.tfstate.tf", "config/restored.tfstate.json", "scripts/restored.tfstate.1700000000.py")
IGNORE_FILE = ".gitignore"


def scratch_repository(folder: Path) -> Path:
    """An empty Git repository that holds a copy of this repository's ignore rules."""
    root = folder / "repository"
    root.mkdir()
    run_command("git", ["init", "-q"], cwd=root, timeout=60, check=True)
    (root / IGNORE_FILE).write_text((ROOT / IGNORE_FILE).read_text())
    return root


def listed_files(root: Path) -> list[str]:
    """Every file Git tracks or would add (not ignored) under ``root``, as sorted paths relative to it."""
    result = run_command("git", ["ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root, timeout=60, check=True)
    return sorted(name for name in result.stdout.split("\0") if name and (root / name).is_file())


def write(root: Path, paths: tuple[str, ...]) -> None:
    for path in paths:
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("x\n")


@pytest.mark.parametrize("path", SOURCES)
def test_a_source_file_whose_name_contains_the_state_extension_stays_visible(tmp_path: Path, path: str) -> None:
    root = scratch_repository(tmp_path)
    write(root, (path,))
    check(listed_files(root) == [IGNORE_FILE, path], f"the ignore rules hide the source file {path}: {listed_files(root)}")


@pytest.mark.parametrize("path", ARTIFACTS)
def test_a_state_backup_saved_plan_or_plan_export_stays_ignored(tmp_path: Path, path: str) -> None:
    root = scratch_repository(tmp_path)
    write(root, (path,))
    check(listed_files(root) == [IGNORE_FILE], f"the ignore rules do not hide the Terraform artifact {path}: {listed_files(root)}")


def test_the_artifacts_stay_ignored_while_the_look_alike_sources_stay_visible_in_one_listing(tmp_path: Path) -> None:
    root = scratch_repository(tmp_path)
    write(root, (*ARTIFACTS, *SOURCES))
    check(listed_files(root) == sorted([IGNORE_FILE, *SOURCES]), f"the listing is not the ignore file and the source files alone: {listed_files(root)}")
