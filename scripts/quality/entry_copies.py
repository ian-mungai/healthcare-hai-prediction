"""Check that a module run with ``python -m`` does not catch an exception class that a second copy of itself defines.

Run ``.venv/bin/python -m scripts.quality.entry_copies`` (the tracked Python files of the checkout) or ``... --planted`` (planted source trees that the check
must flag or pass). ``python -m package.module`` loads the file as ``__main__``. A module of the closure that imports ``package.module`` by name then loads a
second copy, whose exception classes are not the ones that the ``__main__`` copy catches, so a ``raise`` in the second copy escapes the ``except`` clause of
``main``, and the other way round. The check reads the import graph of the source with ``ast`` only; it runs no application code and needs no network.

A module is flagged when all of these hold:

1. it has an ``if __name__ == "__main__":`` block,
2. it defines an exception class (a class whose base name ends in ``Error`` or ``Exception``, or that derives from one of its own),
3. a module in its import closure (imports inside functions count) imports the module by name and names one of those classes, and
4. its block does not run the package copy (``from package.module import main as packaged_main``).

The fix is the block of ``scripts/infrastructure/host_session.py``. The check has no allowlist: a flagged module is fixed.
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path
from typing import Any

from scripts.process import run_command

RULE = "scripts/quality/entry_copies.py: a module run with python -m must not catch classes that a second copy of itself defines"
FIX = "in the __main__ block run the package copy: from <package>.<module> import main as packaged_main, then raise SystemExit(packaged_main())"
EXCEPTION_SUFFIXES = ("Error", "Exception")


def module_name(path: str) -> str:
    parts = Path(path).with_suffix("").parts
    return ".".join(parts[:-1] if parts[-1] == "__init__" else parts)


def parse(sources: dict[str, str]) -> dict[str, ast.Module]:
    trees = {}
    for path, text in sources.items():
        try:
            trees[module_name(path)] = ast.parse(text)
        except SyntaxError:
            continue
    return trees


def main_blocks(tree: ast.Module) -> list[ast.If]:
    """The top-level ``if __name__ == "__main__":`` blocks."""
    return [
        node
        for node in tree.body
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare)
        and isinstance(node.test.left, ast.Name)
        and node.test.left.id == "__name__"
        and any(isinstance(item, ast.Constant) and item.value == "__main__" for item in node.test.comparators)
    ]


def exception_classes(tree: ast.Module) -> set[str]:
    """The top-level classes that derive, by base name, from an exception or from an exception class of the same module."""
    found: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            bases = {base.id if isinstance(base, ast.Name) else base.attr if isinstance(base, ast.Attribute) else "" for base in node.bases}
            if any(name.endswith(EXCEPTION_SUFFIXES) or name in found for name in bases):
                found.add(node.name)
    return found


def imports_of(name: str, tree: ast.Module, known: set[str]) -> set[str]:
    """The first-party modules that ``tree`` imports anywhere, with the modules that ``from package import module`` names."""
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                base = ".".join([*name.split(".")[: -node.level], *([base] if base else [])])
            found.add(base)
            found.update(f"{base}.{alias.name}" for alias in node.names)
        elif isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
    return found & known


def names_class(tree: ast.Module, target: str, classes: set[str]) -> bool:
    """True when ``tree`` imports one of ``classes`` from ``target`` or reads one as an attribute of the imported module."""
    last = target.rsplit(".", 1)[-1]
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module == target and any(alias.name in classes for alias in node.names):
            return True
        if (
            isinstance(node, ast.Attribute)
            and node.attr in classes
            and isinstance(node.value, ast.Name | ast.Attribute)
            and getattr(node.value, "id", getattr(node.value, "attr", None)) == last
        ):
            return True
    return False


def runs_package_copy(blocks: list[ast.If], target: str) -> bool:
    return any(isinstance(node, ast.ImportFrom) and node.level == 0 and node.module == target for block in blocks for node in ast.walk(block))


def flagged(sources: dict[str, str]) -> dict[str, str]:
    """Each flagged module (dotted name) with its problem, over the given ``{path: source}`` tree."""
    trees = parse(sources)
    graph = {name: imports_of(name, tree, set(trees)) for name, tree in trees.items()}
    result: dict[str, str] = {}
    for name in sorted(trees):
        blocks, classes = main_blocks(trees[name]), exception_classes(trees[name])
        if not blocks or not classes or runs_package_copy(blocks, name):
            continue
        seen: set[str] = set()
        todo = [name]
        while todo:
            for module in graph[todo.pop()] - seen - {name}:
                seen.add(module)
                todo.append(module)
        importers = sorted(module for module in seen if name in graph[module] and names_class(trees[module], name, classes))
        if importers:
            line = blocks[0].lineno
            result[name] = (
                f"{line}: __main__ block runs this copy while {', '.join(importers)} import(s) the package copy and name(s) {', '.join(sorted(classes))}"
            )
    return result


def findings(sources: dict[str, str]) -> list[str]:
    """The problems of the tree, one text for each flagged module."""
    paths = {module_name(path): path for path in sources}
    return [f"{paths[name]}:{problem}\n  rule: {RULE}\n  fix: {FIX}" for name, problem in flagged(sources).items()]


PACKAGE = "package/module.py"
IMPORTER = "package/importer.py"
THIRD = "package/third.py"
MODULE_SOURCE = "class ModuleError(ValueError):\n    pass\n\n\ndef main() -> int:\n    return 0\n\n\nif __name__ == '__main__':\n    {guard}\n"
RAISES = "from package.module import ModuleError\n\n\ndef run() -> None:\n    raise ModuleError('x')\n"


def planted() -> dict[str, bool]:
    """Source trees that the check must flag or pass, each keyed by what it proves."""
    plain, delegating = "raise SystemExit(main())", "from package.module import main as packaged_main\n    raise SystemExit(packaged_main())"
    lazy = "def run() -> None:\n    from package.module import ModuleError\n\n    raise ModuleError('x')\n"
    chained = {PACKAGE: MODULE_SOURCE.format(guard=plain) + "import package.importer\n", IMPORTER: "import package.third\n", THIRD: RAISES}
    cases: dict[str, tuple[dict[str, str], bool]] = {
        "a module whose closure raises its class by name is flagged": (
            {PACKAGE: MODULE_SOURCE.format(guard=plain) + "import package.importer\n", IMPORTER: RAISES},
            True,
        ),
        "an import inside a function counts": ({PACKAGE: MODULE_SOURCE.format(guard=plain) + "import package.importer\n", IMPORTER: lazy}, True),
        "an importer that is two imports away counts": (chained, True),
        "a guard that runs the package copy is not flagged": (
            {PACKAGE: MODULE_SOURCE.format(guard=delegating) + "import package.importer\n", IMPORTER: RAISES},
            False,
        ),
        "an importer outside the closure is not flagged": ({PACKAGE: MODULE_SOURCE.format(guard=plain), IMPORTER: RAISES}, False),
        "an importer that names no class of the module is not flagged": (
            {PACKAGE: MODULE_SOURCE.format(guard=plain) + "import package.importer\n", IMPORTER: "from package.module import main\n"},
            False,
        ),
        "a module without an exception class is not flagged": (
            {
                PACKAGE: "def main() -> int:\n    return 0\n\n\nif __name__ == '__main__':\n    raise SystemExit(main())\nimport package.importer\n",
                IMPORTER: RAISES,
            },
            False,
        ),
        "a module without a main block is not flagged": (
            {PACKAGE: "class ModuleError(ValueError):\n    pass\nimport package.importer\n", IMPORTER: RAISES},
            False,
        ),
    }
    return {name: bool(flagged(sources)) is expected for name, (sources, expected) in cases.items()}


def tracked(root: Path) -> dict[str, str]:
    listed = run_command("git", ["ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", "*.py"], cwd=root, timeout=30, check=True).stdout
    return {name: (root / name).read_text(encoding="utf-8") for name in sorted(set(listed.split("\0")) - {""})}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--planted", action="store_true", help="check the planted source trees instead of the checkout")
    parser.add_argument("--root", type=Path, default=Path("."))
    arguments = parser.parse_args(argv)
    if arguments.planted:
        results: dict[str, Any] = planted()
        sys.stdout.write(json.dumps({"passed": all(results.values()), "results": results}, indent=2, sort_keys=True) + "\n")
        return 0 if all(results.values()) else 1
    problems = findings(tracked(arguments.root))
    for problem in problems:
        sys.stderr.write(f"FAIL {problem}\n")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
