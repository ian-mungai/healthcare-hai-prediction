"""The retired catalog migration, one-off registry builders and September review runners are gone, and nothing lists, imports, copies or runs them.

Nothing called these scripts: the Mac-to-AWS catalog migration finished, the registry rev3 and successor builders each ran once, and the September
review runners wrote evidence that is kept under ``data/``. The unused ``alias`` reader and the unused OpenMetadata stand-in credentials went with them.
Every file that Git lists, Markdown, JSON, shell, TOML and Python included, is searched for each file name, and Python files are also parsed for every
import form. Names are spelled in pieces so that this file does not name what it guards against. History is excluded: the changelog, ``data/`` and ``e2e/``.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from scripts.process import run_command
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
CATALOG = "scripts/lakehouse/"
RETIRED = (
    CATALOG + "migrate" + "_aws_catalog.py",
    CATALOG + "catalog_migration" + "_cases.py",
    CATALOG + "run_catalog" + "_migration_e2e.py",
    CATALOG + "run_catalog" + "_migration_defects.py",
    "scripts/acquisition/one_off/registry_rev3/build" + "_rev3.py",
    "scripts/acquisition/one_off/successor/build" + "_successor.py",
    "scripts/review/run_hai" + "_archive_e2e.py",
    "scripts/review/run_remaining" + "_items_e2e.py",
    "scripts/review/run_sampled" + "_sources_e2e.py",
)
# Every retired file name is one word that no other file spells, so the bare name is enough; the dotted module form ends in it.
NAMES = tuple(path.rsplit("/", 1)[1].removesuffix(".py") for path in RETIRED)
DOTTED = frozenset(path.removesuffix(".py").replace("/", ".") for path in RETIRED)
PATTERNS = tuple(re.compile(r"(?<![\w])" + re.escape(name) + r"(?![\w])") for name in NAMES)
# Definitions that nothing called or read: (file, top-level name).
DEAD_DEFINITIONS = (
    ("scripts/training/mlflow_alias_read.py", "read" + "_alias"),
    ("scripts/infrastructure/run_openmetadata_e2e.py", "CLI" + "ENT"),
)
SKIPPED_FOLDERS = frozenset({".git", ".venv", ".terraform", "__pycache__", "node_modules", "data", "e2e", ".worktrees", ".tools", ".mypy_cache", ".ruff_cache"})
SKIPPED_NAMES = frozenset({"CHANGELOG.md", "uv.lock"})
MAX_BYTES = 2_000_000


def listed_files(root: Path) -> list[str]:
    """Every file Git lists as tracked or not ignored under ``root``; for a folder that is not a repository, every file outside the skipped folders."""
    if (root / ".git").exists():
        result = run_command("git", ["ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root, timeout=60, check=True)
        names = [name for name in result.stdout.split("\0") if name]
    else:
        names = [str(path.relative_to(root)) for path in root.rglob("*") if path.is_file()]
    return sorted(name for name in names if (root / name).is_file() and not SKIPPED_FOLDERS & set(Path(name).parts) and Path(name).name not in SKIPPED_NAMES)


def removed_import(tree: ast.AST) -> bool:
    """Whether the module imports a retired module in any absolute form: ``import a.b``, ``from a import b`` or an alias of either."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and not node.level:
            names = [node.module or "", *(f"{node.module}.{alias.name}" for alias in node.names)]
        else:
            continue
        if any(name == dotted or name.startswith(dotted + ".") for name in names for dotted in DOTTED):
            return True
    return False


def scan(root: Path, skipped: frozenset[str] = frozenset()) -> list[str]:
    """The files under ``root`` that import, list, copy or run a retired file, except the skipped ones."""
    found = []
    for relative in listed_files(root):
        path = root / relative
        if relative in skipped or path.stat().st_size > MAX_BYTES:
            continue
        try:
            text = path.read_text()
        except (UnicodeDecodeError, OSError):
            continue
        if any(pattern.search(text) for pattern in PATTERNS):
            found.append(relative)
        elif relative.endswith(".py"):
            try:
                tree = ast.parse(text)
            except (SyntaxError, ValueError):
                continue
            if removed_import(tree):
                found.append(relative)
    return found


def defined(relative: str) -> set[str]:
    """Every top-level name the file defines or assigns."""
    names: set[str] = set()
    for node in ast.parse((ROOT / relative).read_text()).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names.update(target.id for target in targets if isinstance(target, ast.Name))
    return names


def test_the_retired_files_are_gone() -> None:
    present = [name for name in RETIRED if (ROOT / name).exists()]
    check(not present, f"these retired files are back: {present}")


def test_no_file_lists_imports_copies_or_runs_a_retired_file() -> None:
    named = scan(ROOT, frozenset({"tests/test_no_retired_script_leftovers.py"}))
    check(not named, f"these files still name a retired file: {named}")


def test_the_unused_definitions_stay_gone() -> None:
    back = [f"{relative}: {name}" for relative, name in DEAD_DEFINITIONS if name in defined(relative)]
    check(not back, f"these unused definitions are back: {back}")


PLANTED = {
    "launcher input list": ("config/x.json", f'{{"files": ["{RETIRED[0]}"]}}\n'),
    "module command in a group script": ("scripts/ci/groups/x.sh", f'timed "x" "$python_bin" -m {RETIRED[2].removesuffix(".py").replace("/", ".")}\n'),
    "typos exemption": ("_typos.toml", f'extend-exclude = [\n  "{NAMES[4]}.py",\n]\n'),
    "from import": ("scripts/user.py", f"from scripts.review import {NAMES[6]}\n"),
    "aliased module import": ("scripts/user.py", f"import {RETIRED[3].removesuffix('.py').replace('/', '.')} as defects\n"),
    "Markdown command": ("notes/x.md", f"```bash\n.venv/bin/python -m scripts.review.{NAMES[8]} --source-root x\n```\n"),
}
CLEAN = {
    "scripts/lakehouse/catalog.py": "from scripts.lakehouse import catalog_journal\n",
    "scripts/user.py": "from scripts.review import review_remaining\nNAMESPACE = 'catalog_migration'\n",
    "notes/x.md": "The catalog migration finished; its builders are in Git history.\n",
}


def plant(root: Path, files: dict[str, str]) -> None:
    for relative, text in files.items():
        (root / relative).parent.mkdir(parents=True, exist_ok=True)
        (root / relative).write_text(text)


def test_the_scan_passes_files_that_name_only_surviving_modules(tmp_path: Path) -> None:
    plant(tmp_path, CLEAN)
    check(scan(tmp_path) == [], f"the scan refused a clean tree: {scan(tmp_path)}")


@pytest.mark.parametrize("case", sorted(PLANTED))
def test_the_scan_catches_every_planted_way_to_name_a_retired_file(case: str, tmp_path: Path) -> None:
    plant(tmp_path, CLEAN)
    relative, text = PLANTED[case]
    plant(tmp_path, {relative: text})
    check(scan(tmp_path) == [relative], f"the scan missed the planted file for {case!r}: {scan(tmp_path)}")


def test_the_scan_leaves_history_and_ignored_folders_out(tmp_path: Path) -> None:
    text = f"from scripts.review import {NAMES[6]}\n"
    plant(tmp_path, {"CHANGELOG.md": text, "data/e2e/x.md": text, "e2e/x.py": text, ".venv/x.py": text})
    check(scan(tmp_path) == [], f"the scan read a history or ignored folder: {scan(tmp_path)}")
