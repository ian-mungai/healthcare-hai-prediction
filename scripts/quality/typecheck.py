"""Run strict MyPy on the host scope or a declared runtime's dependency-bound files.

Run with ``--check-untyped-defs --disallow-untyped-defs``. The default host scope includes every nonignored Python
source except the dependency-bound leaves in ``config/quality/mypy_runtimes.json`` and their transitive importers.
The host run checks that the leaves equal the files with import diagnostics in an unrestricted run. Select a declared
runtime with ``--runtime`` in an environment that supplies its dependencies and types. No profile suppresses import errors.
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from importlib.util import resolve_name
from pathlib import Path

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config/quality/mypy_runtimes.json"
MYPY_FLAGS = ["--check-untyped-defs", "--disallow-untyped-defs"]


def source_files() -> set[str]:
    return set(run_command("git", ["ls-files", "--cached", "--others", "--exclude-standard", "*.py"], cwd=ROOT, check=True).stdout.splitlines())


def imported_modules(tree: ast.AST, package: str) -> set[str]:
    modules = {package} if package else set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = resolve_name("." * node.level + (node.module or ""), package) if node.level else node.module or ""
            modules.add(module)
            modules.update(f"{module}.{alias.name}" for alias in node.names)
    return modules


def pending_by_import(files: set[str], pending: dict[str, dict[str, str]]) -> dict[str, str]:
    modules = {path.removesuffix(".py").replace("/", ".").removesuffix(".__init__"): path for path in files}
    dependencies = {}
    for module, path in modules.items():
        package = module if path.endswith("/__init__.py") else module.rpartition(".")[0]
        imports = imported_modules(ast.parse((ROOT / path).read_text(), filename=path), package)
        prefixes = {".".join(parts[:length]) for name in imports for parts in [name.split(".")] for length in range(1, len(parts) + 1)}
        dependencies[path] = {modules[name] for name in prefixes if name in modules}
    deferred = {path: entry["reason"] for path, entry in pending.items()}
    while True:
        added = {
            path: "imports a pending module: " + ", ".join(sorted(imports & deferred.keys()))
            for path, imports in dependencies.items()
            if path not in deferred and imports & deferred.keys()
        }
        if not added:
            return deferred
        deferred.update(added)


def targets(runtime: str, pending: dict[str, dict[str, str]]) -> tuple[list[str], dict[str, str]]:
    """Return the selected sources and each deferred source's dependency or import cause."""
    files = source_files()
    if stale := set(pending) - files:
        raise ValueError(f"{CONFIG.relative_to(ROOT)}: remove stale runtime paths: {', '.join(sorted(stale))}")
    if runtime not in {"host", "acquisition", "orchestration"}:
        return sorted(path for path, entry in pending.items() if entry["runtime"] == runtime), {}
    deferred = pending_by_import(files, pending)
    if runtime == "acquisition":
        files = {path for path in files if path.startswith("scripts/acquisition/") or path == "tests/test_source_registry.py"}
    elif runtime == "orchestration":
        files = {path for path in files if path.startswith(("orchestration/", "dags/", "scripts/orchestration/"))}
    return sorted(files - deferred.keys()), {path: deferred[path] for path in sorted(files & deferred.keys())}


def check_pending(pending: dict[str, dict[str, str]]) -> bool:
    result = run_command(
        sys.executable, ["-m", "mypy", "--output=json", "--no-error-summary", "--no-incremental", *MYPY_FLAGS, *sorted(source_files())], cwd=ROOT, timeout=900
    )
    diagnostics = [json.loads(line) for line in result.stdout.splitlines()]
    errors = [item for item in diagnostics if item["severity"] == "error"]
    if result.returncode not in (0, 1) or result.stderr or (result.returncode == 1 and not errors):
        raise ValueError(f"unrestricted MyPy failed: {result.stdout}{result.stderr}")
    imports = {item["file"] for item in errors if item["code"] in {"import-not-found", "import-untyped"}}
    problems = [f"missing manifest leaf: {path}" for path in sorted(imports - pending.keys())]
    problems += [f"stale manifest leaf: {path}" for path in sorted(pending.keys() - imports)]
    problems += [
        f"non-import MyPy diagnostic: {item['file']}:{item['line']}: {item['message']} [{item['code']}]"
        for item in errors
        if item["code"] not in {"import-not-found", "import-untyped"}
    ]
    if problems:
        sys.stderr.write("\n".join(problems) + "\n")
        return False
    sys.stdout.write(f"pending manifest matches {len(imports)} import-diagnostic files ({len(errors)} diagnostics).\n")
    return True


def main() -> int:
    """Select a declared scope and return the strict checker's actual result."""
    pending: dict[str, dict[str, str]] = json.loads(CONFIG.read_text())["pending"]
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--runtime", choices=sorted({"host", "acquisition", "orchestration"} | {entry["runtime"] for entry in pending.values()}), default="host"
    )
    parser.add_argument("--check-untyped-defs", action="store_true", required=True)
    parser.add_argument("--disallow-untyped-defs", action="store_true", required=True)
    args = parser.parse_args()
    try:
        selected, deferred = targets(args.runtime, pending)
        if not selected:
            parser.error("the runtime selects no Python source files")
        sys.stdout.write(
            f"MyPy runtime {args.runtime}: {len(selected)} source files, {len(deferred)} pending dependency-bound files (see {CONFIG.relative_to(ROOT)}).\n"
        )
        for path, reason in deferred.items():
            sys.stdout.write(f"pending {path}: {reason}\n")
        result = run_command(sys.executable, ["-m", "mypy", *MYPY_FLAGS, *selected], cwd=ROOT, timeout=900)
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        if args.runtime == "host" and not check_pending(pending):
            return 1
    except (ValueError, SyntaxError) as error:
        parser.exit(1, f"typecheck: {error}\n")
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
