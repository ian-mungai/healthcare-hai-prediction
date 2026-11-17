"""Check that a module run with ``python -m`` does not catch an exception class that a second copy of itself defines.

Run ``.venv/bin/python -m scripts.quality.entry_copies`` (the tracked Python files of the checkout) or ``... --planted`` (planted source trees that the check
must flag or pass). ``python -m package.module`` loads the file as ``__main__``. A module of the closure that imports ``package.module`` by name then loads a
second copy, whose exception classes are not the ones that the ``__main__`` copy catches, so a ``raise`` in the second copy escapes the ``except`` clause of
``main``, and the other way round. The check reads the import graph of the source with ``ast`` only; it runs no application code and needs no network.

A module is flagged when all of these hold:

1. it has an ``if __name__ == "__main__":`` block,
2. it defines an exception class (a class whose base name ends in ``Error`` or ``Exception``, or that derives from one of its own),
3. a module in its import closure (imports inside functions count; relative imports resolve against the package of the importing module, which is the
   folder itself for an ``__init__.py``) imports the module by name and names one of those classes: it imports the class (also under another name, relative
   or by ``import *``, which binds the names of ``__all__`` or else the public names) or reads it as an attribute of the module (``import a.b``,
   ``import a.b as t``, ``from a import b as t``, ``from . import b``), and
4. its block does not run the package copy: it must import a function of the module from the package name (``from package.module import main as
   packaged_main``) and call it, and it must not call the local function of the same name.

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


class Tree:
    """The modules of a source tree: their syntax trees and the names of the package initializers (``__init__.py``), whose relative imports start in the
    package itself and not in its parent."""

    def __init__(self, sources: dict[str, str]) -> None:
        self.modules: dict[str, ast.Module] = {}
        self.packages: set[str] = set()
        for path, text in sources.items():
            try:
                self.modules[module_name(path)] = ast.parse(text)
            except SyntaxError:
                continue
            if Path(path).name == "__init__.py":
                self.packages.add(module_name(path))

    def base(self, name: str, node: ast.ImportFrom) -> str:
        """The absolute module that a ``from`` import of module ``name`` reads: the relative levels resolved against the module's own package."""
        if not node.level:
            return node.module or ""
        parts = name.split(".") if name in self.packages else name.split(".")[:-1]
        return ".".join([*parts[: max(len(parts) - (node.level - 1), 0)], *([node.module] if node.module else [])])


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


def prefixes(dotted: str) -> list[str]:
    parts = dotted.split(".")
    return [".".join(parts[:length]) for length in range(1, len(parts) + 1)]


def imports_of(name: str, tree: Tree) -> set[str]:
    """The first-party modules that module ``name`` imports anywhere (a package initializer runs with the module under it), with the modules that
    ``from package import module`` names."""
    found: set[str] = set()
    for node in ast.walk(tree.modules[name]):
        if isinstance(node, ast.ImportFrom):
            base = tree.base(name, node)
            found.update(prefixes(base) if base else [])
            found.update(f"{base}.{alias.name}" for alias in node.names)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                found.update(prefixes(alias.name))
    return found & set(tree.modules)


def exports(module: ast.Module) -> set[str]:
    """What ``from module import *`` binds: the strings of a literal ``__all__``, else the public names that the module defines or imports."""
    for node in module.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.List | ast.Tuple) and any(dotted(target) == ["__all__"] for target in node.targets):
            return {item.value for item in node.value.elts if isinstance(item, ast.Constant) and isinstance(item.value, str)}
    names: set[str] = set()
    for node in module.body:
        if isinstance(node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            names.add(node.name)
        elif isinstance(node, ast.Assign | ast.AnnAssign):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names.update(item.id for item in targets if isinstance(item, ast.Name))
        elif isinstance(node, ast.Import | ast.ImportFrom):
            names.update((alias.asname or alias.name).split(".")[0] for alias in node.names if alias.name != "*")
    return {name for name in names if not name.startswith("_")}


def dotted(node: ast.expr) -> list[str] | None:
    """``a.b.c`` as ``["a", "b", "c"]``, or None for any other expression."""
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, ast.Attribute) and (head := dotted(node.value)) is not None:
        return [*head, node.attr]
    return None


def module_names(name: str, tree: Tree) -> dict[str, str]:
    """What each name that module ``name`` binds with an import means as a first-party module: ``import a.b as t`` and ``from a import b as t`` bind ``t`` to
    ``a.b``, and ``import a.b`` binds ``a`` to ``a``."""
    known, bound = set(tree.modules), {}
    for node in ast.walk(tree.modules[name]):
        if isinstance(node, ast.ImportFrom):
            base = tree.base(name, node)
            bound.update({alias.asname or alias.name: f"{base}.{alias.name}" for alias in node.names if f"{base}.{alias.name}" in known})
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname and alias.name in known:
                    bound[alias.asname] = alias.name
                elif not alias.asname:
                    bound[alias.name.split(".")[0]] = alias.name.split(".")[0]
    return bound


def imports_class(name: str, tree: Tree, target: str, classes: set[str]) -> bool:
    """True when module ``name`` imports one of ``classes`` from ``target`` by name, under another name, relative or by ``import *``."""
    starred = classes & exports(tree.modules[target])
    return any(
        tree.base(name, node) == target and any(alias.name in classes or (alias.name == "*" and starred) for alias in node.names)
        for node in ast.walk(tree.modules[name])
        if isinstance(node, ast.ImportFrom)
    )


def reads_class(name: str, tree: Tree, target: str, classes: set[str]) -> bool:
    """True when module ``name`` reads one of ``classes`` as an attribute of ``target``, through ``import a.b``, ``import a.b as t``, ``from a import b as t``
    or ``from . import b``."""
    bound = module_names(name, tree)
    for node in ast.walk(tree.modules[name]):
        parts = dotted(node.value) if isinstance(node, ast.Attribute) and node.attr in classes else None
        if parts and parts[0] in bound and ".".join([bound[parts[0]], *parts[1:]]) == target:
            return True
    return False


def names_class(name: str, tree: Tree, target: str, classes: set[str]) -> bool:
    """True when module ``name`` imports one of ``classes`` from ``target`` or reads one as an attribute of it."""
    return imports_class(name, tree, target, classes) or reads_class(name, tree, target, classes)


def calls(block: ast.AST) -> set[str]:
    """The names that the block calls as plain functions."""
    return {node.func.id for node in ast.walk(block) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}


def runs_package_copy(blocks: list[ast.If], target: str, module: ast.Module) -> bool:
    """True when a block imports a function of the module from its package name and calls it, and calls no local function of the same name that the import
    does not replace. A block that imports the package main and still calls the local ``main`` runs the copy that this check is about."""
    local = {node.name for node in module.body if isinstance(node, ast.FunctionDef)}
    for block in blocks:
        called = calls(block)
        imported = {
            alias.name: alias.asname or alias.name
            for node in ast.walk(block)
            if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module == target
            for alias in node.names
            if alias.name in local
        }
        if imported and any(binding in called for binding in imported.values()) and not any(name in called and imported[name] != name for name in imported):
            return True
    return False


def flagged(sources: dict[str, str]) -> dict[str, str]:
    """Each flagged module (dotted name) with its problem, over the given ``{path: source}`` tree."""
    tree = Tree(sources)
    trees = tree.modules
    graph = {name: imports_of(name, tree) for name in trees}
    result: dict[str, str] = {}
    for name in sorted(trees):
        blocks, classes = main_blocks(trees[name]), exception_classes(trees[name])
        if not blocks or not classes or runs_package_copy(blocks, name, trees[name]):
            continue
        seen: set[str] = set()
        todo = [name]
        while todo:
            for module in graph[todo.pop()] - seen - {name}:
                seen.add(module)
                todo.append(module)
        importers = sorted(module for module in seen if name in graph[module] and names_class(module, tree, name, classes))
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
API = "package/api.py"
INIT = "package/__init__.py"
INIT_IMPORTER = "package/importer/__init__.py"
INNER = "package/importer/inner.py"
MODULE_SOURCE = "class ModuleError(ValueError):\n    pass\n\n\ndef main() -> int:\n    return 0\n\n\nif __name__ == '__main__':\n    {guard}\n"
RAISES = "from package.module import ModuleError\n\n\ndef run() -> None:\n    raise ModuleError('x')\n"


def planted_cases() -> dict[str, tuple[dict[str, str], bool]]:
    """Source trees with the result that the check must give (True: flagged), each keyed by what it proves."""
    plain, delegating = "raise SystemExit(main())", "from package.module import main as packaged_main\n    raise SystemExit(packaged_main())"
    lazy = "def run() -> None:\n    from package.module import ModuleError\n\n    raise ModuleError('x')\n"
    uses = "def run() -> None:\n    raise {expression}('x')\n"
    plain_module = MODULE_SOURCE.format(guard=plain) + "import package.importer\n"
    chained = {PACKAGE: plain_module, IMPORTER: "import package.third\n", THIRD: RAISES}

    def importer(source: str) -> dict[str, str]:
        return {PACKAGE: plain_module, IMPORTER: source}

    def guarded(guard: str) -> dict[str, str]:
        return {PACKAGE: MODULE_SOURCE.format(guard=guard) + "import package.importer\n", IMPORTER: RAISES}

    reexport = {
        PACKAGE: plain_module,
        IMPORTER: "from package.api import ModuleError\n" + uses.format(expression="ModuleError"),
        API: "from package.module import {names}\n",
    }
    inner = {PACKAGE: plain_module, INIT_IMPORTER: "from .inner import run\n", INNER: RAISES}
    return {
        "a module whose closure raises its class by name is flagged": (importer(RAISES), True),
        "an import inside a function counts": (importer(lazy), True),
        "an importer that is two imports away counts": (chained, True),
        "a guard that runs the package copy is not flagged": (guarded(delegating), False),
        "an importer outside the closure is not flagged": ({PACKAGE: MODULE_SOURCE.format(guard=plain), IMPORTER: RAISES}, False),
        "an importer that names no class of the module is not flagged": (importer("from package.module import main\n"), False),
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
        "a class imported under another name counts": (
            importer("from package.module import ModuleError as Failure\n" + uses.format(expression="Failure")),
            True,
        ),
        "a class read through a dotted module name counts": (importer("import package.module\n" + uses.format(expression="package.module.ModuleError")), True),
        "a class read through an aliased module import counts": (
            importer("import package.module as target\n" + uses.format(expression="target.ModuleError")),
            True,
        ),
        "a class read through an aliased from import of the module counts": (
            importer("from package import module as target\n" + uses.format(expression="target.ModuleError")),
            True,
        ),
        "a class read through a relative from import of the module counts": (
            importer("from . import module as target\n" + uses.format(expression="target.ModuleError")),
            True,
        ),
        "a class imported by a relative import counts": (importer("from .module import ModuleError\n" + uses.format(expression="ModuleError")), True),
        "a class re-exported by a star import counts": (reexport | {API: reexport[API].format(names="*")}, True),
        "a star import of a module whose __all__ lists no class is not flagged": (
            reexport
            | {
                PACKAGE: plain_module + "__all__ = ['main']\n",
                API: reexport[API].format(names="*"),
                IMPORTER: "from package.api import main\n",
            },
            False,
        ),
        "a class re-exported by a named import counts": (reexport | {API: reexport[API].format(names="ModuleError")}, True),
        "a relative import in a package initializer resolves in that package": (inner, True),
        "a relative class import in a package initializer counts": (
            {PACKAGE: plain_module, INIT: "from .module import ModuleError\n", IMPORTER: "import package\n"},
            True,
        ),
        "an imported package main that is never called is flagged": (
            guarded("from package.module import main as packaged_main\n    raise SystemExit(main())"),
            True,
        ),
        "an imported package main with no call is flagged": (guarded("from package.module import main as packaged_main\n    pass"), True),
        "a block that calls both mains is flagged": (
            guarded("from package.module import main as packaged_main\n    packaged_main()\n    raise SystemExit(main())"),
            True,
        ),
        "a block that imports main without an alias and calls it is not flagged": (
            guarded("from package.module import main\n    raise SystemExit(main())"),
            False,
        ),
        "a block that exits through sys with the package main is not flagged": (
            guarded("from package.module import main as packaged_main\n    sys.exit(packaged_main())"),
            False,
        ),
    }


def planted() -> dict[str, bool]:
    """Run the planted trees: each must give the result that its key proves."""
    return {name: bool(flagged(sources)) is expected for name, (sources, expected) in planted_cases().items()}


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
