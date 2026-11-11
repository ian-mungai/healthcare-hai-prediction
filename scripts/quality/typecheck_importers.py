"""Run strict MyPy on the changed Python files and on every file that imports one of them.

A removed or renamed Python function, class or module breaks the files that import it, not the file that changed. The importers come from the import graph
that grimp builds (scripts/quality/importer_graph.py, run by the pinned import-linter's Python), read from the tree before the change so that the importers
of a deleted module are found too. The graph takes the import roots the project declares (MyPy's ``mypy_path``, pytest's ``pythonpath`` and Ruff's ``src`` in
pyproject.toml). When it cannot read them, every covered file counts as an importer, so the run falls back to whole-scope MyPy and never passes by default.
Files that MyPy does not cover (``pending`` in config/quality/mypy_runtimes.json and the files that import them) are
left out here; the removed-names check reads those as text.

    .venv/bin/python -m scripts.quality.typecheck_importers

The change is the staged diff, or the range ``PRE_COMMIT_FROM_REF..PRE_COMMIT_TO_REF`` when both are set, as in the removed-names check.
E2E: ``.venv/bin/python -m scripts.quality.run_checks_e2e --hook typecheck-importers``.
"""

from __future__ import annotations

import json
import os
import posixpath
import sys
import tarfile
import tempfile
import tomllib
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
# The pyproject.toml settings that declare import roots, and the other files that can declare them (this check reads only pyproject.toml).
ROOT_SETTINGS = (("mypy", "mypy_path"), ("pytest", "ini_options", "pythonpath"), ("ruff", "src"))
OTHER_CONFIGS = ("mypy.ini", ".mypy.ini", "pytest.ini", "ruff.toml", ".ruff.toml")
SHARED_CONFIGS = ("setup.cfg", "tox.ini")
ROOT_WORDS = ("mypy_path", "pythonpath", "[ruff")


def module_of(path: str, roots: list[str]) -> str:
    """The dotted module name of a Python path, below its import root: ``app/util.py`` is ``app.util``, ``app/__init__.py`` is ``app``, and with root
    ``src`` the path ``src/acme/rig.py`` is ``acme.rig``."""
    below = next((path[len(folder) + 1 :] for folder in sorted(roots, key=len, reverse=True) if path.startswith(f"{folder}/")), path)
    return below.removesuffix(".py").removesuffix("/__init__").replace("/", ".")


def path_of(root: Path, module: str, roots: list[str]) -> str | None:
    """The Python file of a dotted module name in the tree at ``root``, below the project root or one of its import roots, or None when the tree holds none."""
    base = module.replace(".", "/")
    candidates = [f"{prefix}{base}{tail}" for prefix in ("", *[f"{folder}/" for folder in roots]) for tail in (".py", "/__init__.py")]
    return next((candidate for candidate in candidates if (root / candidate).is_file()), None)


def declared_roots(root: Path, base: str) -> list[str]:
    """The import root folders (other than the project root) that pyproject.toml declares, now and at revision ``base``.

    Raises ValueError when a declaration cannot be read: invalid TOML, a value that is not a list of relative paths, a ``MYPYPATH`` variable or another
    configuration file that can declare roots.
    """
    if os.environ.get("MYPYPATH"):
        raise ValueError("MYPYPATH is set")
    for name in OTHER_CONFIGS:
        if (root / name).is_file():
            raise ValueError(f"{name} can declare import roots")
    for name in SHARED_CONFIGS:
        if (root / name).is_file() and any(word in (root / name).read_text(encoding="utf-8", errors="replace") for word in ROOT_WORDS):
            raise ValueError(f"{name} declares import roots")
    texts = [(root / "pyproject.toml").read_text(encoding="utf-8")] if (root / "pyproject.toml").is_file() else []
    earlier = run_command("git", ["-C", str(root), "show", f"{base}:pyproject.toml"], timeout=GIT_SECONDS)
    if earlier.returncode == 0:
        texts.append(earlier.stdout)
    roots: set[str] = set()
    for text in texts:
        try:
            tool = tomllib.loads(text).get("tool", {})
        except tomllib.TOMLDecodeError as error:
            raise ValueError(f"pyproject.toml is not valid TOML: {error}") from error
        for keys in ROOT_SETTINGS:
            value: object = tool
            for key in keys:
                value = value.get(key) if isinstance(value, dict) else None
            if value is None:
                continue
            if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                raise ValueError(f"tool.{'.'.join(keys)} must be a list of paths")
            for item in value:
                folder = posixpath.normpath(item)
                if posixpath.isabs(folder) or folder.startswith("..") or "$" in folder:
                    raise ValueError(f"tool.{'.'.join(keys)} names {item!r}, which is not a folder below the project")
                roots.add(folder)
    return sorted(roots - {"."})


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
    try:
        roots = declared_roots(root, base)
    except ValueError as error:
        sys.stderr.write(f"typecheck-importers: the import roots are unreadable ({error}); every covered file is checked\n")
        return sorted(typecheck.source_files())
    lint_tools.program("import-linter")  # Fails unless the pinned version is installed; its environment holds grimp.
    with tempfile.TemporaryDirectory(prefix="hai_importers_") as scratch:
        tree, archive = Path(scratch) / "tree", Path(scratch) / "base.tar"
        tree.mkdir()
        run_command("git", ["-C", str(root), "archive", "--format=tar", "-o", str(archive), base, "--", "*.py"], timeout=GIT_SECONDS, check=True)
        with tarfile.open(archive) as bundle:
            bundle.extractall(tree, filter="data")
        arguments = ["-P", str(GRAPH_SCRIPT), "--root", str(tree)]
        arguments += [argument for folder in roots for argument in ("--source-root", folder)]
        arguments += [argument for path in paths for argument in ("--module", module_of(path, roots))]
        result = run_command(GRAPH_PYTHON, arguments, timeout=GRAPH_SECONDS)
    if result.returncode:
        raise ValueError(f"typecheck-importers: the import graph failed ({result.returncode}): {(result.stdout + result.stderr)[-1500:]}")
    found = [path_of(root, module, roots) for module in json.loads(result.stdout)]
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
