"""No live code reads a private original folder.

Bronze pseudonymizes, so every capture stores the whole file under raw/ and no code keeps an owner-only original. The only place the folder name may
still appear is a line that asserts the folder was not made.
"""

from __future__ import annotations

import re
from pathlib import Path

from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
FOLDERS = ("scripts", "orchestration", "dags", "tests", "infra", ".github")
SUFFIXES = {".py", ".sh", ".tf", ".hcl", ".yml", ".yaml"}
NAME = "private_" + "original"
ASSERTS_ABSENCE = re.compile(r"\bnot\b[^\n]*" + NAME)


def code_files() -> list[Path]:
    return sorted(
        path
        for folder in FOLDERS
        for path in (ROOT / folder).rglob("*")
        if path.suffix in SUFFIXES and path.is_file() and ".terraform" not in path.parts and path != Path(__file__).resolve()
    )


def test_no_live_code_names_a_private_original_folder_except_to_assert_it_was_not_made() -> None:
    found = [
        f"{path.relative_to(ROOT)}:{number}"
        for path in code_files()
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if NAME in line and not ASSERTS_ABSENCE.search(line)
    ]
    check(not found, f"these lines still use a private original folder: {found}")


def test_the_scan_sees_the_code_it_guards() -> None:
    names = {path.name for path in code_files()}
    check({"stage_publisher.py", "stored_mirror.py", "collector_state.py", "model_metadata.py"} <= names, "the scan must reach the guarded modules")
