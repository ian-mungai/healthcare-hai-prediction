"""Shared set-up for the registry check tests: a copy of the committed inputs in a temporary folder, edited one byte at a time.

A copy is made of real files (the code closures name config files and refuse a link that leaves the tree), so a case may edit any file of it.
"""

from __future__ import annotations

import atexit
import functools
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
COPIED = ("scripts", "config/ingestion")
LINKED = ("config/acquisition", "config/lakehouse")
LINKED_FOLDERS = ("baseline_run", "bootstrap_seed")


def copy_inputs(destination: Path) -> Path:
    """The inputs the registry check reads. The code closures name config files and refuse a link that leaves the tree, so every folder is copied as real
    files except the two large folders no closure names, which are linked file by file."""
    for folder in (*COPIED, *LINKED):
        shutil.copytree(ROOT / folder, destination / folder, ignore=shutil.ignore_patterns("__pycache__", *LINKED_FOLDERS))
    for folder in LINKED_FOLDERS:
        for source in (ROOT / "config/acquisition" / folder).rglob("*"):
            if source.is_file():
                target = destination / source.relative_to(ROOT)
                target.parent.mkdir(parents=True, exist_ok=True)
                os.symlink(source, target)
    return destination


def replace(root: Path, relative: str, body: bytes) -> None:
    """Write a file of the copy."""
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    path.write_bytes(body)


def edit_json(root: Path, relative: str, change: Any) -> None:
    """Read a JSON file of the copy, apply ``change`` to the parsed document and write it back in the repository's two-space form."""
    document = json.loads((root / relative).read_bytes())
    change(document)
    replace(root, relative, (json.dumps(document, indent=2, sort_keys=True) + "\n").encode())


def append_text(root: Path, relative: str, text: str) -> None:
    """Add text to the end of a file of the copy."""
    replace(root, relative, (root / relative).read_bytes() + text.encode())


@functools.lru_cache(maxsize=1)
def scope_report() -> dict[str, Any]:
    """The real scope report of the repository (one pinned dbt parse for the whole test run)."""
    from scripts.ingestion import scope

    report, problems, _ = scope.scope_of_project(ROOT / "dbt", ROOT / "config/lakehouse/bronze_tables.json", ROOT)
    check(not problems, f"The real scope report has problems: {problems}")
    return report


@functools.lru_cache(maxsize=1)
def pristine_root() -> Path:
    """One copy of the inputs with a freshly written lock, made once per test run and removed when the run ends."""
    from scripts.ingestion import registry

    folder = Path(tempfile.mkdtemp(prefix="hai_registry_pristine_"))
    atexit.register(shutil.rmtree, folder, True)
    copy_inputs(folder)
    registry.write_lock(folder, scope_report())
    return folder


def fresh_copy(destination: Path) -> Path:
    """A private copy of the pristine inputs for one case."""
    source = pristine_root()
    for path in source.rglob("*"):
        if path.is_dir() or "__pycache__" in path.parts:
            continue
        target = destination / path.relative_to(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        if path.is_symlink():
            os.symlink(os.readlink(path), target)
        else:
            shutil.copyfile(path, target)
    return destination
