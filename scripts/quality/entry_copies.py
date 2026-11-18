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
   or by ``import *``, which binds the names of a literal ``__all__``, with its annotated and ``+=`` forms, else the public names) or reads it as an
   attribute of the module (``import a.b``, ``import a.b as t``, ``from a import b as t``, ``from . import b``). Every scope (module, function, class) has
   its own bindings, and a name that a scope binds twice means both modules, and
4. its block does not run the package copy. The statements of the block, in order, must import a function of the module from the package name at the top
   level of the block (``from package.module import main as packaged_main``) and call it where the call always runs (a simple statement, not a branch, a
   nested function, a conditional expression or the later operand of ``and`` or ``or``), and no statement may call the local function of the entry's name
   (``main`` and every name that the block imports from the package name). Any other layout is flagged.

Imports with a literal module name count like import statements: ``importlib.import_module("a.b")`` and ``__import__("a.b")`` (which returns the top-level
package unless it has a ``fromlist``), also as the value of a name. A declared limit: a module name that the code computes, a relative name, an attribute read
through ``getattr``, a star import of a package that only re-exports and a base class that is not named like an exception are not followed.

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


DYNAMIC_IMPORTS = ("import_module", "__import__")


def literal_import(node: ast.AST) -> tuple[str, bool] | None:
    """The module name of ``importlib.import_module("a.b")`` or ``__import__("a.b")`` (a literal name), and whether the call returns that module itself: an
    ``import_module`` call does, and a ``__import__`` call does only with a non-empty ``fromlist``, else it returns the top-level package."""
    if not isinstance(node, ast.Call) or not node.args or not (isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str)):
        return None
    function = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else ""
    if function not in DYNAMIC_IMPORTS:
        return None
    fromlist = node.args[3] if len(node.args) > 3 else next((item.value for item in node.keywords if item.arg == "fromlist"), None)
    whole = function == "import_module" or (isinstance(fromlist, ast.List | ast.Tuple) and bool(fromlist.elts))
    return node.args[0].value, whole


def imported_module(node: ast.AST) -> str | None:
    """The module that a literal import call evaluates to."""
    literal = literal_import(node)
    return None if literal is None else literal[0] if literal[1] else literal[0].split(".")[0]


def imports_of(name: str, tree: Tree) -> set[str]:
    """The first-party modules that module ``name`` imports anywhere (a package initializer runs with the module under it), with the modules that
    ``from package import module`` names and the modules that a call with a literal name imports."""
    found: set[str] = set()
    for node in ast.walk(tree.modules[name]):
        if isinstance(node, ast.ImportFrom):
            base = tree.base(name, node)
            found.update(prefixes(base) if base else [])
            found.update(f"{base}.{alias.name}" for alias in node.names)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                found.update(prefixes(alias.name))
        elif (literal := literal_import(node)) is not None:
            found.update(prefixes(literal[0]))
    return found & set(tree.modules)


def literal_names(value: ast.expr | None) -> set[str] | None:
    """The strings of a literal list or tuple, or None for any other expression."""
    if isinstance(value, ast.List | ast.Tuple) and all(isinstance(item, ast.Constant) and isinstance(item.value, str) for item in value.elts):
        return {str(item.value) for item in value.elts if isinstance(item, ast.Constant)}
    return None


def assigned(node: ast.Assign | ast.AnnAssign) -> list[ast.expr]:
    """The targets of an assignment; an annotated declaration without a value assigns nothing."""
    if isinstance(node, ast.Assign):
        return node.targets
    return [node.target] if node.value is not None else []


def listed_all(module: ast.Module) -> set[str] | None:
    """The names of the module's ``__all__``: literal assignments (``=``, annotated ``:`` and ``+=``) in order, or None when it has none or builds one that
    is not a literal list or tuple."""
    names: set[str] | None = None
    for node in module.body:
        if isinstance(node, ast.Assign | ast.AnnAssign) and any(dotted(item) == ["__all__"] for item in assigned(node)):
            names = literal_names(node.value)
        elif isinstance(node, ast.AugAssign) and dotted(node.target) == ["__all__"]:
            added = literal_names(node.value) if isinstance(node.op, ast.Add) else None
            names = None if added is None or names is None else names | added
    return names


def exports(module: ast.Module) -> set[str]:
    """What ``from module import *`` binds: the names of a literal ``__all__``, else the public names that the module defines or imports."""
    if (listed := listed_all(module)) is not None:
        return listed
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


SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)


def bindings(name: str, tree: Tree, node: ast.AST, bound: dict[str, set[str]]) -> None:
    """Add what one import or assignment of a literal import call binds to ``bound`` (name to every module it may mean: a name that two imports of one scope
    bind keeps both): ``import a.b as t`` and ``from a import b as t`` bind ``t`` to ``a.b``, ``import a.b`` binds ``a`` to ``a`` and
    ``t = import_module("a.b")`` (also ``t: Module = import_module("a.b")``) binds ``t`` to ``a.b``."""
    known = set(tree.modules)
    if isinstance(node, ast.ImportFrom):
        base = tree.base(name, node)
        for alias in node.names:
            if f"{base}.{alias.name}" in known:
                bound.setdefault(alias.asname or alias.name, set()).add(f"{base}.{alias.name}")
    elif isinstance(node, ast.Import):
        for alias in node.names:
            if alias.asname and alias.name in known:
                bound.setdefault(alias.asname, set()).add(alias.name)
            elif not alias.asname:
                bound.setdefault(alias.name.split(".")[0], set()).add(alias.name.split(".")[0])
    elif isinstance(node, ast.Assign | ast.AnnAssign) and node.value is not None and (module := imported_module(node.value)) is not None:
        for target in assigned(node):
            if isinstance(target, ast.Name):
                bound.setdefault(target.id, set()).add(module)


def meaning(parts: list[str], here: list[dict[str, set[str]]]) -> set[str]:
    """The modules that the dotted name may mean: the first part is what the innermost scope that binds it binds it to."""
    for bound in reversed(here):
        if parts[0] in bound:
            return {".".join([module, *parts[1:]]) for module in bound[parts[0]]}
    return set()


def reads_in(name: str, tree: Tree, scope: ast.AST, chain: list[dict[str, set[str]]], target: str, classes: set[str]) -> bool:
    """True when ``scope`` or a scope below it reads one of ``classes`` as an attribute of ``target``. A name means what its own scope binds, else what the
    scope around it binds; a scope with two bindings of a name means both."""
    bound: dict[str, set[str]] = {}
    attributes: list[ast.Attribute] = []
    nested: list[ast.AST] = []
    pending = list(ast.iter_child_nodes(scope))
    while pending:
        node = pending.pop()
        if isinstance(node, SCOPES):
            nested.append(node)
            continue
        bindings(name, tree, node, bound)
        if isinstance(node, ast.Attribute) and node.attr in classes:
            attributes.append(node)
        pending.extend(ast.iter_child_nodes(node))
    here = [*chain, bound]
    for attribute in attributes:
        parts = dotted(attribute.value)
        if target in (imported_module(attribute.value), dotted_import(attribute.value), *(meaning(parts, here) if parts else ())):
            return True
    return any(reads_in(name, tree, item, chain if isinstance(scope, ast.ClassDef) else here, target, classes) for item in nested)


def dotted_import(node: ast.expr) -> str | None:
    """The module that ``<literal import call>.a.b`` names: the module that the call returns with the attributes ``a.b`` added."""
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.insert(0, node.attr)
        node = node.value
    module = imported_module(node)
    return None if module is None or not parts else ".".join([module, *parts])


def imports_class(name: str, tree: Tree, target: str, classes: set[str]) -> bool:
    """True when module ``name`` imports one of ``classes`` from ``target`` by name, under another name, relative or by ``import *``."""
    starred = classes & exports(tree.modules[target])
    return any(
        tree.base(name, node) == target and any(alias.name in classes or (alias.name == "*" and starred) for alias in node.names)
        for node in ast.walk(tree.modules[name])
        if isinstance(node, ast.ImportFrom)
    )


def reads_class(name: str, tree: Tree, target: str, classes: set[str]) -> bool:
    """True when module ``name`` reads one of ``classes`` as an attribute of ``target``, through ``import a.b``, ``import a.b as t``, ``from a import b as t``,
    ``from . import b`` or a literal import call. Each scope (module, function, class) has its own bindings."""
    return reads_in(name, tree, tree.modules[name], [], target, classes)


def names_class(name: str, tree: Tree, target: str, classes: set[str]) -> bool:
    """True when module ``name`` imports one of ``classes`` from ``target`` or reads one as an attribute of it."""
    return imports_class(name, tree, target, classes) or reads_class(name, tree, target, classes)


def all_calls(node: ast.AST) -> set[str]:
    """The names that ``node`` calls as plain functions anywhere, in nested functions and branches too."""
    return {item.func.id for item in ast.walk(node) if isinstance(item, ast.Call) and isinstance(item.func, ast.Name)}


def sure_calls(node: ast.AST) -> set[str]:
    """The names that evaluating ``node`` always calls: not those in a function body, in a branch of a conditional expression, after the first operand of
    ``and`` or ``or`` or in a comprehension other than its first iterable."""
    found: set[str] = set()

    def visit(item: ast.AST) -> None:
        if isinstance(item, ast.Lambda):
            return
        if isinstance(item, ast.IfExp):
            visit(item.test)
            return
        if isinstance(item, ast.BoolOp):
            visit(item.values[0])
            return
        if isinstance(item, ast.ListComp | ast.SetComp | ast.DictComp | ast.GeneratorExp):
            visit(item.generators[0].iter)
            return
        if isinstance(item, ast.Call) and isinstance(item.func, ast.Name):
            found.add(item.func.id)
        for child in ast.iter_child_nodes(item):
            visit(child)

    visit(node)
    return found


SIMPLE = (ast.Expr, ast.Return, ast.Raise, ast.Assign, ast.AnnAssign, ast.AugAssign)


def block_delegates(block: ast.If, target: str, local: set[str]) -> bool:
    """True when the statements of the block, in order, import a function of the module from its package name at the top level of the block, call it where
    the call always runs, and call no local function of the entry's name. The entry's names are ``main`` and every name that the block imports from the
    package name, in any scope. A statement after ``raise`` or ``return`` never runs."""
    imports = [item for item in ast.walk(block) if isinstance(item, ast.ImportFrom) and item.level == 0 and item.module == target]
    entry = {"main"} | {alias.name for item in imports for alias in item.names if alias.name in local}
    package: set[str] = set()
    delegated = False
    for statement in block.body:
        if isinstance(statement, ast.ImportFrom) and statement.level == 0 and statement.module == target:
            package.update(alias.asname or alias.name for alias in statement.names if alias.name in local)
            continue
        if any(name in entry and name not in package for name in all_calls(statement)):
            return False
        if isinstance(statement, SIMPLE) and sure_calls(statement) & package:
            delegated = True
        if isinstance(statement, ast.Raise | ast.Return):
            break
    return delegated


def runs_package_copy(blocks: list[ast.If], target: str, module: ast.Module) -> bool:
    """True when a block runs the package copy: it imports the entry from the package name and calls it (``block_delegates``)."""
    local = {node.name for node in module.body if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)}
    return any(block_delegates(block, target, local) for block in blocks)


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


OTHER = "package/other.py"
USES = "def run() -> None:\n    raise {expression}('x')\n"
PLAIN_MODULE = MODULE_SOURCE.format(guard="raise SystemExit(main())") + "import package.importer\n"
HELPER_MODULE = (
    "class ModuleError(ValueError):\n    pass\n\n\ndef main() -> int:\n    return 0\n\n\ndef helper() -> int:\n    return 1\n\n\n"
    "if __name__ == '__main__':\n    {guard}\nimport package.importer\n"
)


def scope_cases() -> dict[str, tuple[dict[str, str], bool]]:
    """Trees for names that one module binds to different modules in different scopes."""

    def importer(source: str) -> dict[str, str]:
        return {PACKAGE: PLAIN_MODULE, OTHER: "value = 1\n", IMPORTER: source}

    read = USES.format(expression="module.ModuleError")
    return {
        "a dormant function that binds the name to another module does not hide the module level binding": (
            importer("from package import module\n\n\ndef unused():\n    from package import other as module\n    return module\n\n\n" + read),
            True,
        ),
        "a conditional import of another module under the name keeps the module level binding": (
            importer("from package import module\n\nif False:\n    from package import other as module\n\n\n" + read),
            True,
        ),
        "a function that binds the name to another module reads that module": (
            importer("from package import module\n\n\ndef run() -> None:\n    from package import other as module\n    raise module.ModuleError('x')\n"),
            False,
        ),
        "a function that binds the name to the module reads the module": (
            importer("from package import other as module\n\n\ndef run() -> None:\n    from package import module\n    raise module.ModuleError('x')\n"),
            True,
        ),
    }


def guard_cases() -> dict[str, tuple[dict[str, str], bool]]:
    """Trees for the layouts of the ``__main__`` block."""

    def guarded(guard: str, module: str = MODULE_SOURCE) -> dict[str, str]:
        return {PACKAGE: module.format(guard=guard) + ("" if module is HELPER_MODULE else "import package.importer\n"), IMPORTER: RAISES}

    return {
        "a nested import of the package main that never runs does not replace the local main": (
            guarded("def unused():\n        from package.module import main\n        return main()\n\n    raise SystemExit(main())"),
            True,
        ),
        "a conditional import of the package main does not replace the local main": (
            guarded("if os.environ.get('PLANTED_GUARD'):\n        from package.module import main\n    raise SystemExit(main())"),
            True,
        ),
        "an import of the package main after the call of the local main is flagged": (
            guarded("raise SystemExit(main())\n    from package.module import main"),
            True,
        ),
        "a package helper does not replace the call of the local main": (
            guarded("from package.module import helper\n    helper()\n    raise SystemExit(main())", HELPER_MODULE),
            True,
        ),
        "a call of the package main in a branch only is flagged": (
            guarded("from package.module import main as packaged_main\n    if len(sys.argv) > 1:\n        raise SystemExit(packaged_main())"),
            True,
        ),
        "a call of the package main in a conditional expression only is flagged": (
            guarded("from package.module import main as packaged_main\n    raise SystemExit(packaged_main() if len(sys.argv) > 1 else 0)"),
            True,
        ),
        "a block that calls another local function and the package main is not flagged": (
            guarded("from package.module import main as packaged_main\n    configure()\n    raise SystemExit(packaged_main())"),
            False,
        ),
    }


def all_cases() -> dict[str, tuple[dict[str, str], bool]]:
    """Trees for ``__all__`` and the star import of it."""

    def starred(module: str, importer: str) -> dict[str, str]:
        return {PACKAGE: module, API: "from package.module import *\n", IMPORTER: importer}

    private = "class _ModuleError(ValueError):\n    pass\n\n\ndef main() -> int:\n    return 0\n\n\nif __name__ == '__main__':\n    raise SystemExit(main())\n"
    return {
        "an annotated __all__ without the class keeps it out of a star import": (
            starred(PLAIN_MODULE + "__all__: list[str] = ['main']\n", "from package.api import main\n"),
            False,
        ),
        "an annotated __all__ that lists a private class exports it": (
            starred(private + "__all__: list[str] = ['_ModuleError']\nimport package.importer\n", "from package.api import _ModuleError\n"),
            True,
        ),
        "an extension of __all__ with the class exports it": (
            starred(PLAIN_MODULE + "__all__ = ['main']\n__all__ += ['ModuleError']\n", "from package.api import ModuleError\n"),
            True,
        ),
    }


def dynamic_cases() -> dict[str, tuple[dict[str, str], bool]]:
    """Trees for imports with a literal module name."""

    def importer(source: str) -> dict[str, str]:
        return {PACKAGE: PLAIN_MODULE, IMPORTER: "import importlib\n\n\n" + source}

    return {
        "a class read from an import_module call with a literal name counts": (
            importer(USES.format(expression="importlib.import_module('package.module').ModuleError")),
            True,
        ),
        "a class read through a name bound to an import_module call counts": (
            importer("module = importlib.import_module('package.module')\n\n\n" + USES.format(expression="module.ModuleError")),
            True,
        ),
        "a class read through a typed name bound to an import_module call counts": (
            importer("module: ModuleType = importlib.import_module('package.module')\n\n\n" + USES.format(expression="module.ModuleError")),
            True,
        ),
        "a typed name without a value binds nothing": (
            importer("module: ModuleType\n\n\n" + USES.format(expression="module.ModuleError")),
            False,
        ),
        "a class read from a __import__ call with a literal name and a fromlist counts": (
            importer(USES.format(expression="__import__('package.module', fromlist=['x']).ModuleError")),
            True,
        ),
        "a class read below the package that a __import__ call returns counts": (
            importer(USES.format(expression="__import__('package.module').module.ModuleError")),
            True,
        ),
        "a class read from the package that a __import__ call returns is not the class of the module": (
            importer(USES.format(expression="__import__('package.module').ModuleError")),
            False,
        ),
        "an importer that is reached through an import_module call with a literal name counts": (
            {PACKAGE: PLAIN_MODULE, IMPORTER: "import importlib\n\nimportlib.import_module('package.third')\n", THIRD: RAISES},
            True,
        ),
        "an import_module call with a computed name is not followed": (
            importer("NAME = 'package.module'\n\n\n" + USES.format(expression="importlib.import_module(NAME).ModuleError")),
            False,
        ),
    }


def planted() -> dict[str, bool]:
    """Run the planted trees: each must give the result that its key proves."""
    cases = planted_cases() | scope_cases() | guard_cases() | all_cases() | dynamic_cases()
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
