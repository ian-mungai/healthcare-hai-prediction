"""Run strict MyPy on the changed Python files and on every file that imports one of them.

A removed or renamed Python function, class or module breaks the files that import it, not the file that changed. The importers come from the import graph
that grimp builds (scripts/quality/importer_graph.py, run by the pinned import-linter's Python), read from the tree before the change so that the importers
of a deleted module are found too. Files that MyPy does not cover (``pending`` in config/quality/mypy_runtimes.json and the files that import them) are
left out here; the removed-names check reads those as text.

    .venv/bin/python -m scripts.quality.typecheck_importers

The change is the staged diff, or the range ``PRE_COMMIT_FROM_REF..PRE_COMMIT_TO_REF`` when both are set, as in the removed-names check.
E2E: ``.venv/bin/python -m scripts.quality.run_checks_e2e --hook typecheck-importers``.
"""

from __future__ import annotations

import json
import os
import sys
import tarfile
import tempfile
from pathlib import Path

from scripts.process import TimeoutExpired, run_command
from scripts.quality import lint_tools, typecheck

GIT_SECONDS = 120
GRAPH_SECONDS = 120
MYPY_SECONDS = 900
MYPY_FLAGS = ["--check-untyped-defs", "--disallow-untyped-defs"]
ROOT = Path(__file__).resolve().parents[2]
GRAPH_PYTHON = str(ROOT / ".tools/import-linter/bin/python")
GRAPH_SCRIPT = str(ROOT / "scripts/quality/importer_graph.py")


def module_of(path: str) -> str:
    """The dotted module name of a Python path: ``app/util.py`` is ``app.util`` and ``app/__init__.py`` is ``app``."""
    return path.removesuffix(".py").removesuffix("/__init__").replace("/", ".")


def path_of(root: Path, module: str) -> str | None:
    """The Python file of a dotted module name in the tree at ``root``, or None when the tree holds none."""
    base = module.replace(".", "/")
    return next((candidate for candidate in (f"{base}.py", f"{base}/__init__.py") if (root / candidate).is_file()), None)


def changed_python(root: Path, refs: list[str]) -> tuple[list[str], list[str]]:
    """The Python paths a diff adds or changes and the ones it deletes, a rename counting as both."""
    names = run_command("git", ["-C", str(root), "diff", *refs, "--no-renames", "--name-status", "--no-color"], timeout=GIT_SECONDS, check=True).stdout
    present: list[str] = []
    gone: list[str] = []
    for line in names.splitlines():
        status, _, path = line.partition("\t")
        if path.endswith(".py"):
            (gone if status.startswith("D") else present).append(path)
    return present, gone


def base_exists(root: Path, base: str) -> bool:
    return run_command("git", ["-C", str(root), "rev-parse", "--verify", "--quiet", f"{base}^{{commit}}"], timeout=GIT_SECONDS).returncode == 0


def importing_files(root: Path, base: str, paths: list[str]) -> list[str]:
    """The Python files in the tree at ``root`` that directly import one of ``paths``, by the import graph of revision ``base``."""
    if not paths:
        return []
    if not base_exists(root, base):
        if base == "HEAD":
            return []  # The first commit has no earlier tree and so no importers.
        raise ValueError(f"typecheck-importers: revision {base} does not exist")
    lint_tools.program("import-linter")  # Fails unless the pinned version is installed; its environment holds grimp.
    with tempfile.TemporaryDirectory(prefix="hai_importers_") as scratch:
        tree, archive = Path(scratch) / "tree", Path(scratch) / "base.tar"
        tree.mkdir()
        run_command("git", ["-C", str(root), "archive", "--format=tar", "-o", str(archive), base, "--", "*.py"], timeout=GIT_SECONDS, check=True)
        with tarfile.open(archive) as bundle:
            bundle.extractall(tree, filter="data")
        environment = {**os.environ, "PYTHONPATH": str(tree)}
        result = run_command(
            GRAPH_PYTHON,
            ["-P", str(GRAPH_SCRIPT), "--root", str(tree), *[arg for path in paths for arg in ("--module", module_of(path))]],
            env=environment,
            timeout=GRAPH_SECONDS,
        )
    if result.returncode:
        raise ValueError(f"typecheck-importers: the import graph failed ({result.returncode}): {(result.stdout + result.stderr)[-1500:]}")
    found = [path_of(root, module) for module in json.loads(result.stdout)]
    return sorted({path for path in found if path})


def covered(files: list[str]) -> list[str]:
    """The files MyPy covers: all but the pending ones and their importers (config/quality/mypy_runtimes.json)."""
    if not files:
        return []
    config = typecheck.ROOT / "config/quality/mypy_runtimes.json"
    pending = json.loads(config.read_text(encoding="utf-8"))["pending"] if config.is_file() else {}
    waiting = typecheck.pending_by_import(typecheck.source_files(), pending)
    return [path for path in files if path not in waiting]


def selection(root: Path, refs: list[str], base: str) -> tuple[list[str], list[str]]:
    """The changed files and the files that import them, both limited to what MyPy covers and the tree still holds."""
    present, gone = changed_python(root, refs)
    if not present and not gone:
        return [], []
    changed = sorted(path for path in present if (root / path).is_file())
    importers = [path for path in importing_files(root, base, [*present, *gone]) if path not in present]
    typed = set(covered([*changed, *importers]))
    return [path for path in changed if path in typed], [path for path in importers if path in typed]


def main() -> int:
    root = Path.cwd()
    start, end = os.environ.get("PRE_COMMIT_FROM_REF"), os.environ.get("PRE_COMMIT_TO_REF")
    refs, base = ([start, end], start) if start and end else (["--cached"], "HEAD")
    try:
        changed, importers = selection(root, refs, base)
    except (ValueError, SyntaxError, OSError, TimeoutExpired) as error:
        sys.stderr.write(f"typecheck-importers: {error}\n")
        return 1
    targets = [*changed, *importers]
    if not targets:
        sys.stdout.write("typecheck-importers: no Python file to check\n")
        return 0
    sys.stdout.write(f"typecheck-importers: {len(changed)} changed files and {len(importers)} files that import them\n")
    try:
        result = run_command(sys.executable, ["-m", "mypy", *MYPY_FLAGS, "--", *targets], cwd=root, timeout=MYPY_SECONDS)
    except TimeoutExpired:
        sys.stderr.write(f"typecheck-importers: MyPy exceeded its deadline of {MYPY_SECONDS} seconds and was stopped\n")
        return 1
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
