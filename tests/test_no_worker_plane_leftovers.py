"""The SDK worker plane's leftover modules, their cases, checks and fixtures are gone, and nothing lists, imports, copies or runs them.

The worker pool, its stderr reader, its frame protocol and the host process-group stop only served the SDK worker plane and the catalog tunnel, and both
are removed. Python files are parsed, so every import form is caught (``import``, ``from package import module``, aliases and relative imports). Every
other file that Git lists, Dockerfiles, compose files, Terraform, workflows, shell and Markdown included, is searched for the module names, their paths and
their commands. Names are spelled in pieces so that this file does not name what it guards against. History files are excluded: the changelog, ``data/``
and ``e2e/``.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import pytest

from scripts.process import run_command
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
GROUP = "process" + "_group"
STDERR = "worker" + "_stderr"
PROTOCOL = "worker" + "_protocol"
CASES = "sdk" + "_worker_cases"
STANDINS = "sdk" + "_standins"
SOURCE_CHECKS = "sdk" + "_source_checks"
MUTATIONS = "sdk" + "_static_mutations"
STOP_CASES = GROUP + "_stop_cases"
SCHEMA = "sdk" + "_worker"
# Every removed module, as (package, name).
MODULES = (
    ("scripts", GROUP),
    ("orchestration", STDERR),
    ("orchestration", PROTOCOL),
    ("scripts.orchestration", STOP_CASES),
    ("scripts.orchestration", SOURCE_CHECKS),
    ("scripts.orchestration", MUTATIONS),
    ("scripts.orchestration", CASES),
    ("scripts.orchestration", STANDINS),
)
DOTTED = frozenset(f"{package}.{name}" for package, name in MODULES)
PATHS = tuple(f"{package.replace('.', '/')}/{name}.py" for package, name in MODULES)
GONE = (
    *PATHS,
    f"data_contracts/orchestration/{SCHEMA}.schema.json",
    f"data_contracts/orchestration/examples/{SCHEMA}.json",
    f"data_contracts/orchestration/examples/{CASES}/case_matrix.json",
    f"data_contracts/orchestration/examples/{CASES}/mutation_cases.json",
)
# Names that no other file spells, so the bare word is enough. The host stop's own name is also a parameter and a function name elsewhere, so it is only
# judged in its dotted, path and import forms.
UNIQUE = (STDERR, PROTOCOL, CASES, STANDINS, SOURCE_CHECKS, MUTATIONS, STOP_CASES, f"{SCHEMA}.schema.json", f"examples/{SCHEMA}.json")
TEXT_PATTERNS = (
    *(re.compile(r"(?<![\w.])" + re.escape(dotted) + r"\b") for dotted in sorted(DOTTED)),
    *(re.compile(r"(?<![\w/.])" + re.escape(path)) for path in PATHS),
    *(re.compile(r"(?<![\w])" + re.escape(name) + r"(?![\w])") for name in UNIQUE),
    *(re.compile(r"\bfrom\s+" + re.escape(package) + r"\s+import\s+(?:\(\s*)?[^\n]*?\b" + re.escape(name) + r"\b") for package, name in MODULES),
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


def removed_import(tree: ast.AST, relative: str) -> bool:
    """Whether the module imports a removed module in any form: ``import a.b``, ``from a import b``, an alias of either, or a relative form."""
    package = relative.rsplit("/", 1)[0].replace("/", ".") if "/" in relative else ""
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                parts = package.split(".")[: len(package.split(".")) - (node.level - 1)] if package else []
                base = ".".join([*parts, *([node.module] if node.module else [])])
            names = [base, *(f"{base}.{alias.name}" for alias in node.names)]
        else:
            continue
        if any(name == dotted or name.startswith(dotted + ".") for name in names for dotted in DOTTED):
            return True
    return False


def scan(root: Path, skipped: frozenset[str] = frozenset()) -> list[str]:
    """The files under ``root`` that import, list, copy or run a removed worker plane file, except the skipped ones."""
    found = []
    for relative in listed_files(root):
        path = root / relative
        if relative in skipped or path.stat().st_size > MAX_BYTES:
            continue
        try:
            text = path.read_text()
        except (UnicodeDecodeError, OSError):
            continue
        if any(pattern.search(text) for pattern in TEXT_PATTERNS):
            found.append(relative)
            continue
        if relative.endswith(".py"):
            try:
                tree = ast.parse(text)
            except (SyntaxError, ValueError):
                continue
            if removed_import(tree, relative):
                found.append(relative)
    return found


PLANTED = {
    "from scripts import the host stop": ("scripts/user.py", f"from scripts import {GROUP}\n"),
    "from scripts import the host stop in a list": ("scripts/user.py", f"from scripts import process, {GROUP}\n"),
    "from orchestration import the stderr reader": ("orchestration/user.py", f"from orchestration import {STDERR}\n"),
    "from orchestration import the protocol with an alias": ("orchestration/user.py", f"from orchestration import {PROTOCOL} as frames\n"),
    "import the host stop with an alias": ("scripts/user.py", f"import scripts.{GROUP} as stop\n"),
    "from the module path": ("scripts/user.py", f"from orchestration.{STDERR} import StderrReader\n"),
    "relative import of a case module": ("scripts/orchestration/user.py", f"from . import {CASES}\n"),
    "relative import from a case module": ("scripts/orchestration/user.py", f"from .{STANDINS} import WorkerStandin\n"),
    "multi-line from import": ("scripts/user.py", f"from scripts import (\n    process,\n    {GROUP},\n)\n"),
    "relative import of the host stop": ("scripts/user.py", f"from . import {GROUP}\n"),
    "relative import from the host stop": ("scripts/user.py", f"from .{GROUP} import OwnedProcess\n"),
    "relative import of the stderr reader": ("orchestration/user.py", f"from . import {STDERR}\n"),
    "import inside a function": ("scripts/user.py", f"def run():\n    from scripts import {GROUP}\n    return {GROUP}\n"),
    "dynamic import by name": ("scripts/user.py", f'import importlib\nimportlib.import_module("scripts.{GROUP}")\n'),
    "Dockerfile copy": ("services/x/Dockerfile", f"FROM scratch\nCOPY scripts/{GROUP}.py /opt/hai/{GROUP}.py\n"),
    "Dockerfile copy of the protocol": ("services/x/Dockerfile.job", f"COPY orchestration/{PROTOCOL}.py /opt/hai/orchestration/\n"),
    "compose mount": ("docker-compose.yaml", f"services:\n  a:\n    volumes:\n      - ./orchestration/{STDERR}.py:/opt/hai/{STDERR}.py:ro\n"),
    "Terraform mount": ("infra/services/x.tf", f'locals {{\n  mount = "orchestration/{PROTOCOL}.py"\n}}\n'),
    "Terraform test": ("infra/services/tests/x.tftest.hcl", f'run "a" {{\n  command = plan\n  variables {{ file = "scripts/{GROUP}.py" }}\n}}\n'),
    "workflow command": (".github/workflows/x.yml", f"jobs:\n  a:\n    steps:\n      - run: python -m scripts.orchestration.{CASES} --check-fixtures\n"),
    "shell group step": ("scripts/ci/groups/x.sh", f'timed "x" "$python_bin" -B -m scripts.orchestration.{MUTATIONS} --output "$out"\n'),
    "Markdown command": ("notes/x.md", f"Run it:\n\n```bash\npython -m scripts.orchestration.{SOURCE_CHECKS}\n```\n"),
    "Markdown import example": ("README.md", f"```python\nfrom scripts import {GROUP}\n```\n"),
    "JSON registration": ("config/quality/x.json", f'{{"files": ["scripts/orchestration/{STOP_CASES}.py"]}}\n'),
    "restored schema name": ("config/x.json", f'{{"schema": "data_contracts/orchestration/{SCHEMA}.schema.json"}}\n'),
}
CLEAN = {
    "scripts/process.py": "def stop_process_group(process_group: int) -> None:\n    return None\n",
    "scripts/other.py": "from scripts import process\nfrom orchestration import launcher\n",
    "scripts/orchestration/other.py": "from . import ledger\nfrom .task_guard import guard\n",
    "notes/x.md": "The process group of a job is stopped by scripts/process.py.\n",
}


def plant(root: Path, files: dict[str, str]) -> None:
    for relative, text in files.items():
        (root / relative).parent.mkdir(parents=True, exist_ok=True)
        (root / relative).write_text(text)


def test_the_worker_plane_modules_cases_and_fixtures_are_gone() -> None:
    present = [name for name in GONE if (ROOT / name).exists()]
    check(not present, f"these worker plane files are back: {present}")
    folder = ROOT / "data_contracts/orchestration/examples" / CASES
    check(not folder.exists(), f"the worker case fixture folder is back: {folder}")


def test_no_file_lists_imports_copies_or_runs_a_removed_worker_plane_file() -> None:
    named = scan(ROOT, frozenset({"tests/test_no_worker_plane_leftovers.py"}))
    check(not named, f"these files still name a removed worker plane file: {named}")


# Definitions that nothing called or read. The bronze scan module held only its scan entry point, its role storage and the helpers that served them.
DEAD_MODULE = "scripts/lakehouse/" + "bronze" + "_scan.py"
DEAD_DEFINITIONS = {
    "scripts/lakehouse/run_launcher_e2e.py": ("registry" + "_state",),
    "scripts/infrastructure/services_code_inputs.py": ("INSTALLER" + "_SETTINGS",),
}


def test_the_dead_scan_module_and_definitions_stay_gone() -> None:
    check(not (ROOT / DEAD_MODULE).exists(), f"the unused module is back: {DEAD_MODULE}")
    for relative, names in DEAD_DEFINITIONS.items():
        tree = ast.parse((ROOT / relative).read_text())
        defined = {target.id for node in ast.walk(tree) for target in ([node] if isinstance(node, ast.Name) else []) if isinstance(target.ctx, ast.Store)} | {
            node.name for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.ClassDef))
        }
        back = [name for name in names if name in defined]
        check(not back, f"{relative} defines unused names again: {back}")
    bounds = (ROOT / "scripts/lakehouse/run_duckdb_bounds_cases.py").read_text()
    inputs = json.loads((ROOT / "config/quality/launcher_inputs.json").read_text())
    check(DEAD_MODULE not in bounds and DEAD_MODULE not in json.dumps(inputs), "a list still names the removed scan module")


def test_the_lineage_runtime_declares_the_mounted_package_it_imports() -> None:
    runtimes = json.loads((ROOT / "config/quality/dependency_runtimes.json").read_text())["runtimes"]
    check("hai_lineage" in runtimes["orchestration"]["provided"], "the orchestration runtime does not list the mounted hai_lineage package as provided")


def test_the_scan_passes_files_that_name_only_surviving_modules(tmp_path: Path) -> None:
    plant(tmp_path, CLEAN)
    check(scan(tmp_path) == [], f"the scan refused a clean tree: {scan(tmp_path)}")


@pytest.mark.parametrize("case", sorted(PLANTED))
def test_the_scan_catches_every_planted_way_to_name_a_removed_file(case: str, tmp_path: Path) -> None:
    plant(tmp_path, CLEAN)
    relative, text = PLANTED[case]
    plant(tmp_path, {relative: text})
    check(scan(tmp_path) == [relative], f"the scan missed the planted file for {case!r}: {scan(tmp_path)}")


def test_the_scan_leaves_history_and_ignored_folders_out(tmp_path: Path) -> None:
    text = f"from scripts import {GROUP}\n"
    plant(tmp_path, {"CHANGELOG.md": text, "data/e2e/x.md": text, "e2e/x.py": text, ".venv/x.py": text})
    check(scan(tmp_path) == [], f"the scan read a history or ignored folder: {scan(tmp_path)}")
