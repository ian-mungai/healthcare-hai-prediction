"""The closure of the trusted model code, its record and its verifier (standard library only, because the model API image runs it).

The model API image carries the trusted model-family modules, so a model load needs only the model artifact. The closure is the set of repository source files
those modules need to load and predict: the modules of the types that ``data_contracts/training/model_contract.json`` lists as trusted, plus every repository
module they import, found by reading the syntax tree (``import``, ``from ... import`` and ``importlib.import_module`` with a literal name), plus the
``__init__.py`` of every package on the way. Files the image already holds for another reason
(``IMAGE_PROVIDED``) are in the closure but not in the record's ``files``.

``data_contracts/training/model_code.json`` records the generation, the size and SHA-256 of each file and one digest over them. The digest is the SHA-256 of the
text made from the sorted entries, one line each: the path, a NUL, the size, a NUL, the SHA-256 and a newline. Only the bytes count: a file mode, a modification
time and a ``__pycache__`` folder never change it. Regenerate the record on purpose, never as a repair of a failed check:

    .venv/bin/python -m scripts.model_code check [--root FOLDER]
    .venv/bin/python -m scripts.model_code write --generation N [--root FOLDER]

The module also holds the data rules of the model's bundle files (``bundle/feature_list.json``, ``bundle/provenance.json`` and ``bundle/self_check.json``), the
one home of the rules that every reader applies: the model API loader, the promote action and the promotion check. They are pure functions of the bytes or of
the parsed document. They raise ``DataRuleError``, which names the rule that is broken, and each reader maps that to its own refusal.

It also holds the one bounded single-file fetch of a run's artifact (``mlflow_fetch``, standard library at import: MLflow loads when the fetch runs) and the
data-only read of a model file (``load_data_only``), so that the serving loader and the promotion check read a model the same way and never through
``mlflow.sklearn.load_model``.
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import hashlib
import importlib
import json
import math
import os
import posixpath
import re
import socket
import stat
import sys
import threading
import time
import tomllib
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, NamedTuple

ROOT = Path(__file__).resolve().parents[1]
RECORD = "data_contracts/training/model_code.json"
SCHEMA = "data_contracts/training/model_code.schema.json"
SCHEMA_ID = "model_code/1"
CONTRACT = "data_contracts/training/model_contract.json"
# The files of the closure that the model API image holds already (services/model_api/Dockerfile copies them), so the record does not list them.
IMAGE_PROVIDED = ("orchestration/__init__.py", "orchestration/training_contract.py")
# Third-party import names that differ from the package name in uv.lock (the others are the package name with - made _).
IMPORT_NAMES = {
    "scikit_learn": "sklearn",
    "mlflow_skinny": "mlflow",
    "python_dotenv": "dotenv",
    "pyjwt": "jwt",
    "databricks_sdk": "databricks",
    "google_auth": "google",
    "protobuf": "google",
}
# Imports of a library that the model_api group does not install, allowed only inside one function: (file, dotted function name) -> libraries. Each entry names
# code that a model load and a prediction never run, and a verifier problem reports an entry that no import uses any more.
OUTSIDE_IMAGE = {("scripts/training/models/trees.py", "shap_values"): ("shap",)}
# Functions that import by a name given at run time, and the other routes to code that the scan cannot follow (see ImportScan).
DYNAMIC = frozenset({"__import__", "builtins.__import__", "importlib.__import__", "importlib.import_module", "pkgutil.resolve_name"})
MODULE_OBJECTS = frozenset({"importlib", "pkgutil", "builtins"})
REFUSED_NAMES = frozenset({"exec", "eval", "compile", "__builtins__", "builtins.exec", "builtins.eval", "builtins.compile"})
REFUSED_PREFIXES = (
    "importlib.util",
    "importlib.machinery",
    "importlib.abc",
    "runpy",
    "imp",
    "zipimport",
    "sys.path",
    "sys.meta_path",
    "sys.path_hooks",
    "sys.path_importer_cache",
)
PATH_PATTERN = r"^([A-Za-z0-9_][A-Za-z0-9_.-]*/)*[A-Za-z0-9_][A-Za-z0-9_.-]*$"
DIGEST_PATTERN = r"^[0-9a-f]{64}$"
PATH = re.compile(PATH_PATTERN)
DIGEST = re.compile(DIGEST_PATTERN)
TOP_KEYS = {"schema", "generation", "image_provided", "files", "closure_sha256"}
FILE_KEYS = {"path", "size", "sha256"}


class RecordError(ValueError):
    """The record is absent, is not one JSON object or breaks the closed schema."""


# The digest


def entry(root: Path, path: str) -> dict[str, Any]:
    """The record entry of one file: its path, size and SHA-256, from the bytes alone."""
    data = (root / path).read_bytes()
    return {"path": path, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def entries(root: Path, paths: Iterable[str]) -> list[dict[str, Any]]:
    return [entry(root, path) for path in sorted(paths, key=lambda item: item.encode())]


def digest(items: Iterable[Mapping[str, Any]]) -> str:
    """The closure digest of entries: sorted ``path NUL size NUL sha256`` lines, each ended by a newline."""
    ordered = sorted(items, key=lambda item: str(item["path"]).encode())
    text = "".join(f"{item['path']}\0{item['size']}\0{item['sha256']}\n" for item in ordered)
    return hashlib.sha256(text.encode()).hexdigest()


# The import closure


class Found(NamedTuple):
    """One import in a file: the absolute module name, the names taken from it, the function it sits in (dotted, empty at module level) and its line."""

    name: str
    taken: tuple[str, ...]
    scope: str
    line: int


class Scan(NamedTuple):
    modules: list[Found]
    refused: list[str]
    outside: list[str]


class Closure(NamedTuple):
    files: list[str]
    problems: list[str]


def module_file(root: Path, module: str) -> str | None:
    """The repository file of a dotted module name (a module file, or a package's ``__init__.py``), or None when the root holds neither."""
    base = module.replace(".", "/")
    for candidate in (f"{base}.py", f"{base}/__init__.py"):
        if (root / candidate).is_file():
            return candidate
    return None


def is_repository_name(root: Path, module: str) -> bool:
    """Whether the first part of a dotted name is a folder or a module of the repository root (a library name is not)."""
    top = module.split(".")[0]
    return (root / top).is_dir() or (root / f"{top}.py").is_file()


def packages_of(root: Path, path: str) -> list[str]:
    """The ``__init__.py`` files of the packages above a file that exist, from the outermost down."""
    parts = Path(path).parts[:-1]
    found = []
    for count in range(1, len(parts) + 1):
        candidate = "/".join([*parts[:count], "__init__.py"])
        if (root / candidate).is_file() and candidate != path:
            found.append(candidate)
    return found


def under(name: str, prefixes: Iterable[str]) -> bool:
    return any(name == prefix or name.startswith(prefix + ".") for prefix in prefixes)


class ImportScan(ast.NodeVisitor):
    """Reads one file for every way it can reach other code: static imports in any scope, literal dynamic imports and attribute access through a package.

    A name bound by an import, or by an assignment of an imported name, is followed by its dotted form, so ``import x as y``, ``from x import z as w`` and
    ``f = importlib.import_module`` resolve to what they name. The rules:
    * ``import``, ``from ... import`` (relative ones made absolute), ``importlib.import_module``, ``importlib.__import__``, ``__import__`` and
      ``pkgutil.resolve_name`` with a literal name are imports of that name. Any other argument (a variable, a concatenation, an f-string,
      ``*args``) is refused.
    * A dynamic-import function, or the ``importlib``, ``pkgutil`` or ``builtins`` module itself, used other than as the called function is refused:
      the function could be called with a name this scan cannot read (``getattr(importlib, ...)``, ``loader = importlib.import_module``).
    * ``exec``, ``eval``, ``compile``, ``__builtins__``, ``importlib.util``, ``importlib.machinery``, ``importlib.abc``, ``runpy``, ``imp``, ``zipimport``,
      the other ``pkgutil`` functions and a change of ``sys.path``, ``sys.meta_path`` or ``sys.path_hooks`` are refused: they load code by a route the scan
      cannot follow.
    * An attribute chain that starts at an imported name and reaches a repository module (``pkg.submodule.X`` after ``import pkg``) imports that module."""

    def __init__(self, root: Path, path: str, tree: ast.Module) -> None:
        self.root, self.path = root, path
        self.package = list(Path(path).parts[:-1])
        self.scope: list[str] = []
        self.found: list[Found] = []
        self.refused: list[str] = []
        self.outside: list[str] = []
        self.aliases: dict[str, str] = {}
        self.collect_aliases(tree)

    def absolute(self, level: int, module: str | None) -> str | None:
        """The absolute dotted name of a relative import, or None when it climbs above the repository root."""
        if not level:
            return module or ""
        if level - 1 > len(self.package):
            return None
        base = self.package[: len(self.package) - (level - 1)]
        return ".".join([*base, *([module] if module else [])])

    def collect_aliases(self, tree: ast.Module) -> None:
        """Bind each imported name to its dotted form. A name that means more than one thing in the file (imported twice differently, or imported and also
        bound by an assignment, a parameter, a loop, a ``def`` or a ``class``) and that names a module or a dynamic-import function is refused: this scan
        resolves each name once for the whole file, so it refuses a name it cannot resolve unambiguously."""
        imported: dict[str, set[str]] = {}
        first: dict[str, ast.AST] = {}
        other: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    name = alias.asname or alias.name.split(".")[0]
                    imported.setdefault(name, set()).add(alias.name if alias.asname else alias.name.split(".")[0])
                    first.setdefault(name, node)
            elif isinstance(node, ast.ImportFrom):
                base = self.absolute(node.level, node.module)
                if base is not None and node.module != "__future__":
                    for alias in node.names:
                        if alias.name != "*":
                            name = alias.asname or alias.name
                            imported.setdefault(name, set()).add(f"{base}.{alias.name}" if base else alias.name)
                            first.setdefault(name, node)
            elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store | ast.Del):
                other.add(node.id)
            elif isinstance(node, ast.arg):
                other.add(node.arg)
            elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef | ast.ExceptHandler | ast.MatchAs | ast.MatchStar) and node.name:
                other.add(node.name)
        for name, values in imported.items():
            self.aliases[name] = sorted(values)[0]
            if (len(values) > 1 or name in other) and any(self.relevant(value) for value in values):
                self.refuse(first[name], f"binds the name {name} more than once, and one binding is a module or a dynamic-import function")

    def relevant(self, value: str) -> bool:
        """Whether a dotted name must be resolved with certainty: a repository module or package, a dynamic-import function or module, or code execution."""
        if value in DYNAMIC or value in MODULE_OBJECTS or value in REFUSED_NAMES or under(value, REFUSED_PREFIXES) or value.startswith("pkgutil."):
            return True
        return is_repository_name(self.root, value) and (module_file(self.root, value) is not None or (self.root / value.replace(".", "/")).is_dir())

    @staticmethod
    def root_name(node: ast.AST) -> str:
        while isinstance(node, ast.Attribute):
            node = node.value
        return node.id if isinstance(node, ast.Name) else ""

    def dotted(self, node: ast.AST) -> str | None:
        if isinstance(node, ast.Name):
            return self.aliases.get(node.id, node.id)
        if isinstance(node, ast.Attribute):
            base = self.dotted(node.value)
            return None if base is None else f"{base}.{node.attr}"
        return None

    def refuse(self, node: ast.AST, why: str) -> None:
        self.refused.append(f"{self.path}:{getattr(node, 'lineno', 0)}: {why}")

    def add(self, node: ast.AST, name: str, taken: tuple[str, ...] = ()) -> None:
        if under(name, REFUSED_PREFIXES) or any(under(f"{name}.{item}", REFUSED_PREFIXES) for item in taken):
            self.refuse(node, f"imports {name}, which loads code by a route this check cannot follow")
        self.found.append(Found(name, taken, ".".join(self.scope), getattr(node, "lineno", 0)))

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.add(node, alias.name)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        name = self.absolute(node.level, node.module)
        if name is None:
            self.outside.append(f"{self.path}:{node.lineno}: relative import beyond the repository root")
            return
        if any(alias.name == "*" for alias in node.names):
            self.refuse(node, f"uses a wildcard import from {name or node.module}, so the names it binds cannot be read")
        self.add(node, name, tuple(alias.name for alias in node.names))

    def generic_visit(self, node: ast.AST) -> None:
        if isinstance(node, ast.Assign | ast.AnnAssign) and node.value is not None:
            self.assigned(node, node.value)
        super().generic_visit(node)

    def assigned(self, node: ast.AST, value: ast.expr) -> None:
        """An imported module or dynamic-import name stored under another name would make a later use of the new name unreadable."""
        resolved = self.dotted(value)
        if resolved is not None and self.root_name(value) in self.aliases and self.relevant(resolved):
            self.refuse(node, f"assigns {resolved} to another name, so a later use of that name cannot be read")

    def visit_Call(self, node: ast.Call) -> None:
        callee = self.dotted(node.func)
        if callee in DYNAMIC:
            self.dynamic(node, callee)
            for argument in [*node.args, *(keyword.value for keyword in node.keywords)]:
                self.visit(argument)
            return
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        full = self.dotted(node)
        if full is None:
            self.generic_visit(node)
            return
        self.reference(node, full)
        root: ast.expr = node
        while isinstance(root, ast.Attribute):
            root = root.value
        if isinstance(root, ast.Name) and root.id in self.aliases:
            parts = full.split(".")
            if is_repository_name(self.root, parts[0]):
                for count in range(2, len(parts) + 1):
                    prefix = ".".join(parts[:count])
                    if module_file(self.root, prefix):
                        self.found.append(Found(prefix, (), ".".join(self.scope), node.lineno))

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load):
            self.reference(node, self.aliases.get(node.id, node.id), bare=node.id)

    def reference(self, node: ast.AST, full: str, bare: str = "") -> None:
        """A name or attribute chain used as a value (a called dynamic-import function never gets here)."""
        if full in REFUSED_NAMES:
            self.refuse(node, f"uses {full}, which runs code this check cannot read")
        elif full in DYNAMIC or (bare and full in MODULE_OBJECTS):
            self.refuse(node, f"uses {full} other than as the function a call names, so the import it makes cannot be read")
        elif under(full, REFUSED_PREFIXES) or (full.startswith("pkgutil.") and full != "pkgutil.resolve_name"):
            self.refuse(node, f"uses {full}, which loads code by a route this check cannot follow")

    def literal(self, node: ast.AST | None) -> str | None:
        return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None

    def dynamic(self, node: ast.Call, callee: str) -> None:
        """A call of a dynamic-import function: an import of the literal name it gets, or a refusal."""
        why = self.dynamic_import(node, callee)
        if isinstance(why, str):
            self.refuse(node, why)
        elif isinstance(why, list):
            for name in why:
                self.add(node, name)
        elif why is not None:
            self.add(node, *why)

    def dynamic_import(self, node: ast.Call, callee: str) -> str | tuple[str, tuple[str, ...]] | list[str] | None:
        """The refusal text, or the (name, names taken) to import, or a list of module names (pkgutil.resolve_name)."""
        if any(isinstance(item, ast.Starred) for item in node.args) or any(keyword.arg is None for keyword in node.keywords):
            return f"{callee} gets starred arguments, so its name cannot be read"
        keywords = {str(keyword.arg): keyword.value for keyword in node.keywords}
        arguments = list(node.args)
        name = self.literal(arguments[0] if arguments else keywords.get("name"))
        if name is None:
            return f"{callee} gets a name that is not a string literal"
        if callee == "pkgutil.resolve_name":
            if name.startswith(".") or len(arguments) > 1 or set(keywords) - {"name"}:
                return "pkgutil.resolve_name gets a relative name or extra arguments"
            parts = name.split(":")[0].split(".")
            if not is_repository_name(self.root, parts[0]):
                return [parts[0]]
            found = []
            for count in range(1, len(parts) + 1):
                prefix = ".".join(parts[:count])
                if not (module_file(self.root, prefix) or (self.root / prefix.replace(".", "/")).is_dir()):
                    break
                found.append(prefix)
            return found or [parts[0]]
        if callee.endswith("import_module"):
            if len(arguments) > 2 or set(keywords) - {"name", "package"}:
                return "importlib.import_module gets extra arguments"
            if name.startswith("."):
                package = self.literal(arguments[1] if len(arguments) > 1 else keywords.get("package"))
                level = len(name) - len(name.lstrip("."))
                parts = (package or "").split(".")
                if package is None or level > len(parts):
                    return "a relative import_module name needs a literal package inside the repository"
                name = ".".join([*parts[: len(parts) - (level - 1)], name.lstrip(".")])
            return (name, ())
        depth = arguments[4] if len(arguments) > 4 else keywords.get("level")
        if name.startswith(".") or len(arguments) > 5 or set(keywords) - {"name", "globals", "locals", "fromlist", "level"}:
            return f"{callee} gets a relative name or extra arguments"
        if depth is not None and not (isinstance(depth, ast.Constant) and depth.value == 0):
            return f"{callee} gets a level that is not the literal 0"
        fromlist = arguments[3] if len(arguments) > 3 else keywords.get("fromlist")
        taken: tuple[str, ...] = ()
        if isinstance(fromlist, ast.List | ast.Tuple) and all(self.literal(item) is not None for item in fromlist.elts):
            taken = tuple(str(self.literal(item)) for item in fromlist.elts)
        elif fromlist is not None and not (isinstance(fromlist, ast.Constant) and not fromlist.value):
            return f"{callee} gets a fromlist that is not a literal list of strings"
        return (name, taken)


def scan(root: Path, path: str) -> Scan:
    """Every import of one file (see ``ImportScan``)."""
    tree = ast.parse((root / path).read_bytes(), filename=path)
    visitor = ImportScan(root, path, tree)
    visitor.visit(tree)
    return Scan(visitor.found, visitor.refused, visitor.outside)


def imported_modules(root: Path, path: str) -> list[tuple[str, tuple[str, ...]]]:
    """Every module a file imports as (dotted name, names taken from it), with relative imports made absolute."""
    return [(item.name, item.taken) for item in scan(root, path).modules]


def repository_imports(root: Path, path: str) -> set[str]:
    """The dotted names of the repository modules a file imports (a library or a standard library name is left out)."""
    return {name for name, _ in imported_modules(root, path) if name and is_repository_name(root, name)}


def names_imported_from(root: Path, path: str, module: str) -> set[str]:
    """The names a file takes with ``from <module> import ...``."""
    return {name for source, names in imported_modules(root, path) if source == module for name in names}


def allowed_libraries(root: Path) -> frozenset[str]:
    """The top-level import names of the direct packages of the model_api group in ``uv.lock``: the third-party libraries the image installs."""
    try:
        lock = tomllib.loads((root / "uv.lock").read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise RecordError(f"cannot read uv.lock: {error}") from error
    owners = [package for package in lock.get("package", []) if "model-api" in package.get("dev-dependencies", {})]
    if len(owners) != 1:
        raise RecordError(f"uv.lock must have exactly one package with a model-api group, not {len(owners)}")
    names = {str(item["name"]).lower().replace("-", "_") for item in owners[0]["dev-dependencies"]["model-api"]}
    return frozenset(IMPORT_NAMES.get(name, name) for name in names)


def trusted_modules(root: Path) -> list[str]:
    """The repository files of the modules that hold the contract's trusted types. A type of a library (xgboost) has no repository file."""
    contract = json.loads((root / CONTRACT).read_text())
    found = {module_file(root, name.rpartition(".")[0]) for name in contract["serialization"]["trusted_types"]}
    return sorted(path for path in found if path)


def closure(root: Path, libraries: Collection[str] | None = None) -> Closure:
    """The closure files, sorted, and the problems found while reading them: a refused import, an import outside the allowed set, a repository module that
    does not exist and a declared exception that nothing uses. ``libraries`` defaults to ``allowed_libraries(root)``."""
    allowed = allowed_libraries(root) if libraries is None else libraries
    pending = list(trusted_modules(root))
    seen: set[str] = set()
    problems: set[str] = set()
    used: set[tuple[str, str, str]] = set()
    while pending:
        path = pending.pop()
        if path in seen:
            continue
        seen.add(path)
        pending.extend(packages_of(root, path))
        try:
            result = scan(root, path)
        except (OSError, SyntaxError, ValueError) as error:
            problems.add(f"closure_changed: {path} cannot be read as Python: {error}")
            continue
        problems.update(f"dynamic_import_refused: {text}" for text in result.refused)
        problems.update(f"import_outside: {text}" for text in result.outside)
        for item in result.modules:
            top = item.name.split(".")[0]
            if not item.name:
                continue
            if is_repository_name(root, top):
                target = module_file(root, item.name)
                if target is None and (root / item.name.replace(".", "/")).is_dir():
                    continue
                if target is None:
                    problems.add(f"closure_changed: {path}:{item.line}: imports {item.name}, which does not exist")
                    continue
                pending.append(target)
                pending.extend(module_file(root, f"{item.name}.{name}") or target for name in item.taken)
            elif top in sys.stdlib_module_names or top in allowed:
                continue
            elif top in OUTSIDE_IMAGE.get((path, item.scope), ()):
                used.add((path, item.scope, top))
            else:
                problems.add(
                    f"import_outside: {path}:{item.line}: imports {item.name}, which is no standard library, model_api library of uv.lock or repository module"
                )
    for (path, scope), names in OUTSIDE_IMAGE.items():
        problems.update(
            f"exception_unused: {path} {scope} no longer imports {name}, so its exception is stale" for name in names if (path, scope, name) not in used
        )
    return Closure(sorted(seen), sorted(problems))


def import_closure(root: Path, libraries: Collection[str] | None = None) -> list[str]:
    """Every file the trusted modules need, the image-provided ones included."""
    return closure(root, libraries).files


# The record


def read_record(path: Path) -> dict[str, Any]:
    """Read and check the record against the closed schema (the same rules as ``model_code.schema.json``). Raises RecordError."""

    try:
        record = json.loads(path.read_text(), object_pairs_hook=distinct_pairs)
    except (OSError, UnicodeDecodeError, ValueError) as error:
        raise RecordError(f"cannot read {path}: {error}") from error
    check_record(record)
    checked: dict[str, Any] = record
    return checked


def check_record(record: Any) -> None:
    if not isinstance(record, dict) or set(record) != TOP_KEYS:
        raise RecordError(f"the record must be an object with exactly the keys {sorted(TOP_KEYS)}")
    if record["schema"] != SCHEMA_ID:
        raise RecordError(f"schema must be {SCHEMA_ID}")
    if type(record["generation"]) is not int or record["generation"] < 1:
        raise RecordError("generation must be a whole number of at least 1")
    if not isinstance(record["closure_sha256"], str) or not DIGEST.fullmatch(record["closure_sha256"]):
        raise RecordError("closure_sha256 must be 64 lowercase hex characters")
    provided = record["image_provided"]
    if not isinstance(provided, list) or not provided or not all(isinstance(item, str) and PATH.fullmatch(item) for item in provided):
        raise RecordError("image_provided must be a non-empty list of relative paths")
    if provided != sorted(set(provided)):
        raise RecordError("image_provided must be sorted and unique")
    files = record["files"]
    if not isinstance(files, list) or not files:
        raise RecordError("files must be a non-empty list")
    for item in files:
        if not isinstance(item, dict) or set(item) != FILE_KEYS:
            raise RecordError(f"each file must be an object with exactly the keys {sorted(FILE_KEYS)}")
        if not isinstance(item["path"], str) or not PATH.fullmatch(item["path"]):
            raise RecordError("a file path must be relative, with no empty, . or .. part and no backslash")
        if type(item["size"]) is not int or item["size"] < 0:
            raise RecordError("a file size must be a whole number of at least 0")
        if not isinstance(item["sha256"], str) or not DIGEST.fullmatch(item["sha256"]):
            raise RecordError("a file sha256 must be 64 lowercase hex characters")
    paths = [item["path"] for item in files]
    if paths != sorted(set(paths), key=lambda item: item.encode()):
        raise RecordError("file paths must be sorted and unique")


def build_record(root: Path, generation: int, libraries: Collection[str] | None = None) -> dict[str, Any]:
    """The record of the closure at ``root``. Refuses any import problem (see ``closure``) and an image-provided file that the closure does not need."""
    files, problems = closure(root, libraries)
    if problems:
        raise RecordError("the trusted modules cannot be recorded: " + "; ".join(problems))
    missing = [path for path in IMAGE_PROVIDED if path not in files]
    if missing:
        raise RecordError(f"the closure does not need the image-provided files {missing}")
    listed = entries(root, [path for path in files if path not in IMAGE_PROVIDED])
    record = {
        "schema": SCHEMA_ID,
        "generation": generation,
        "image_provided": sorted(IMAGE_PROVIDED),
        "files": listed,
        "closure_sha256": digest(listed),
    }
    check_record(record)
    return record


def write(root: Path, generation: int, libraries: Collection[str] | None = None) -> dict[str, Any]:
    """Regenerate the record file. A deliberate step after a reviewed closure change, never an automatic repair."""
    record = build_record(root, generation, libraries)
    (root / RECORD).write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    return record


def closure_digest(root: Path) -> str:
    """The digest of the files the record names, hashed from the actual files at ``root`` (for the serving image at start: no lock, no import analysis).
    Raises RecordError when the record is invalid or one of its files is missing."""
    record = read_record(root / RECORD)
    missing = [item["path"] for item in record["files"] if not (root / item["path"]).is_file()]
    if missing:
        raise RecordError(f"file_missing: {', '.join(missing)}")
    return digest(entries(root, (item["path"] for item in record["files"])))


def check_files(root: Path) -> list[str]:
    """The problems of the actual files at ``root`` against the record: ``record_invalid``, ``file_missing``, ``file_changed`` and ``digest_mismatch``.
    Empty when every recorded file matches its entry and the files hash to ``closure_sha256``. It reads no ``uv.lock``, so the serving image can call it."""
    try:
        record = read_record(root / RECORD)
    except RecordError as error:
        return [f"record_invalid: {error}"]
    problems = [f"file_missing: {item['path']}" for item in record["files"] if not (root / item["path"]).is_file()]
    if problems:
        return problems
    actual = entries(root, (item["path"] for item in record["files"]))
    problems = [f"file_changed: {item['path']}" for item, found in zip(record["files"], actual, strict=True) if found != item]
    if digest(actual) != record["closure_sha256"]:
        problems.append("digest_mismatch: the files do not hash to closure_sha256")
    return problems


def verify(root: Path, libraries: Collection[str] | None = None) -> list[str]:
    """The problems of the record at ``root``, each starting with its reason; an empty list means the record matches the files."""
    try:
        record = read_record(root / RECORD)
    except RecordError as error:
        return [f"record_invalid: {error}"]
    problems = []
    for item in record["files"]:
        path = root / item["path"]
        if not path.is_file():
            problems.append(f"file_missing: {item['path']}")
        elif entry(root, item["path"]) != item:
            problems.append(f"file_changed: {item['path']}")
    if digest(record["files"]) != record["closure_sha256"]:
        problems.append("digest_mismatch: closure_sha256 is not the digest of the listed files")
    try:
        found, import_problems = closure(root, libraries)
    except (OSError, RecordError, ValueError) as error:
        return [*problems, f"closure_changed: the import closure cannot be read: {error}"]
    listed = {item["path"] for item in record["files"]} | set(record["image_provided"])
    problems += [f"closure_changed: {path} is needed by the trusted modules and not in the record" for path in sorted(set(found) - listed)]
    problems += [f"closure_changed: {path} is in the record and not needed by the trusted modules" for path in sorted(listed - set(found))]
    return [*problems, *import_problems]


# The data rules of the bundle files

FEATURE_LIST = "bundle/feature_list.json"
PROVENANCE = "bundle/provenance.json"
SELF_CHECK = "bundle/self_check.json"
CLOSURE_KEY = "model_code_closure_sha256"
# Provisional bounds (the first live load logs the real sizes): each JSON file 1 MiB, the model 64 MiB, at most 2000 features, 256 characters per text.
JSON_CAP = 1024 * 1024
MODEL_CAP = 64 * 1024 * 1024
MAX_FEATURES = 2000
MAX_TEXT = 256
MAX_ROWS = 8
# The keys the training writer writes (``scripts/training/bundle.py``: ``feature_list_document`` and ``build``). Any other key is refused. The nested
# metadata of a permitted key is checked under three budgets: values nested at most ``MAX_DEPTH`` containers deep, at most ``MAX_ENTRIES`` entries in one
# container and at most ``MAX_NODES`` values in a document. The real ``inputs`` value nests three deep (inputs, auxiliary_files, one file's record).
FEATURE_LIST_KEYS = frozenset({"slot", "references", "columns", "fitted_slot", "kept_references", "kept_columns"})
PROVENANCE_KEYS = frozenset({"run_id", "timings", "try", "git_commit", "git_dirty", "code_digest", "resolved_versions", CLOSURE_KEY, "inputs"})
SELF_CHECK_KEYS = frozenset({"schema", "slot", "references", "rows", "predictions"})
MAX_DEPTH = 4
MAX_ENTRIES = 64
MAX_NODES = 1024


class DataRuleError(ValueError):
    """A bundle file or its fetch breaks a rule. ``rule`` names it: ``unreadable``, ``too_large`` (a file over its byte cap), ``not_a_file`` (a folder, a link
    or another file where the fetch must leave one regular file), ``not_served`` (the run's artifacts are not on the tracking server), ``deadline`` (a fetch
    or a lookup did not finish in its time), ``busy`` (the one worker slot stayed taken by an abandoned call for the whole deadline), ``not_json``,
    ``feature_list``, ``provenance``, ``self_check_not_object``, ``self_check_keys``, ``self_check_references``, ``self_check_row_count``,
    ``self_check_row_width``, ``self_check_cells`` or ``self_check_predictions``. The message names the place and the rule, never a value of the file."""

    def __init__(self, rule: str, detail: str) -> None:
        super().__init__(detail)
        self.rule = rule


def refuse_constant(token: str) -> Any:
    raise ValueError(f"{token} is not a JSON number")


def distinct_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """An ``object_pairs_hook`` that refuses an object with a repeated key."""
    keys = [key for key, _ in pairs]
    if len(keys) != len(set(keys)):
        raise ValueError("a key appears twice")
    return dict(pairs)


def parse_json(data: bytes, name: str) -> Any:
    """Parse the bytes of one bundle file as one JSON document: at most ``JSON_CAP`` bytes, UTF-8 only (a byte order mark, UTF-16 and UTF-32 are refused),
    no repeated key and no NaN or Infinity token. Raises ``DataRuleError`` with the rule ``too_large`` or ``not_json``."""
    if len(data) > JSON_CAP:
        raise DataRuleError("too_large", f"{name} is over {JSON_CAP} bytes")
    try:
        return json.loads(data.decode(), object_pairs_hook=distinct_pairs, parse_constant=refuse_constant)
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise DataRuleError("not_json", f"{name} is not one UTF-8 JSON document of finite numbers with no repeated key") from error


def read_json_file(path: str, name: str) -> Any:
    """Read at most ``JSON_CAP`` + 1 bytes of the file and parse them with ``parse_json``. A file that cannot be read is the rule ``unreadable``."""
    try:
        with open(path, "rb") as stream:
            data = stream.read(JSON_CAP + 1)
    except OSError as error:
        raise DataRuleError("unreadable", f"{name} cannot be read") from error
    return parse_json(data, name)


def finite_number(value: Any) -> bool:
    """Whether a JSON value is a number (not a boolean) that a float holds and that is finite: an integer of 400 digits is not."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return False
    try:
        return math.isfinite(float(value))
    except OverflowError:
        return False


def text_list(document: Mapping[str, Any], key: str, name: str) -> list[str]:
    values = document.get(key)
    if not isinstance(values, list) or len(values) > MAX_FEATURES or not all(isinstance(item, str) and len(item) <= MAX_TEXT for item in values):
        raise DataRuleError("feature_list", f"{name} must hold {key} as a list of at most {MAX_FEATURES} texts of at most {MAX_TEXT} characters")
    return values


def optional_text(value: Any, rule: str, name: str) -> None:
    if value is not None and not (isinstance(value, str) and len(value) <= MAX_TEXT):
        raise DataRuleError(rule, f"{name} must be null or text of at most {MAX_TEXT} characters")


def check_metadata(value: Any, name: str, nodes: list[int]) -> None:
    """Check a nested JSON value under the three budgets: null, a boolean, a finite number or short text, in objects and lists of at most ``MAX_DEPTH``
    containers deep with at most ``MAX_ENTRIES`` entries each, and at most ``MAX_NODES`` values in the whole document (``nodes`` counts across calls).
    It walks with a list of pending values, so a deep value never reaches the parser's recursion limit."""
    pending = [(value, 1)]
    while pending:
        item, depth = pending.pop()
        nodes[0] += 1
        if nodes[0] > MAX_NODES:
            raise DataRuleError("provenance", f"{name} holds more than {MAX_NODES} values")
        if isinstance(item, dict | list):
            if depth > MAX_DEPTH or len(item) > MAX_ENTRIES:
                raise DataRuleError("provenance", f"{name} nests more than {MAX_DEPTH} deep or holds more than {MAX_ENTRIES} entries in one object or list")
            if isinstance(item, dict):
                if not all(isinstance(key, str) and len(key) <= MAX_TEXT for key in item):
                    raise DataRuleError("provenance", f"{name} holds a key that is not text of at most {MAX_TEXT} characters")
                item = list(item.values())
            pending.extend((child, depth + 1) for child in item)
        elif not (item is None or isinstance(item, bool) or (isinstance(item, str) and len(item) <= MAX_TEXT) or finite_number(item)):
            raise DataRuleError("provenance", f"{name} holds a value that is not null, a boolean, a finite number or text of at most {MAX_TEXT} characters")


def check_feature_list(document: Any) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """The closed shape of ``bundle/feature_list.json``: the six keys the writer writes. ``references`` and ``columns`` are required; the slots are null or text
    and the two kept lists, when present, are texts of equal length. Returns the references and the columns. Raises ``DataRuleError`` (``feature_list``)."""
    if not isinstance(document, dict):
        raise DataRuleError("feature_list", f"{FEATURE_LIST} must be one JSON object")
    if not set(document) <= FEATURE_LIST_KEYS:
        raise DataRuleError("feature_list", f"{FEATURE_LIST} holds a key outside {sorted(FEATURE_LIST_KEYS)}")
    references, columns = text_list(document, "references", FEATURE_LIST), text_list(document, "columns", FEATURE_LIST)
    if len(references) != len(columns):
        raise DataRuleError("feature_list", f"{FEATURE_LIST} must list as many columns as references")
    for key in ("slot", "fitted_slot"):
        optional_text(document.get(key), "feature_list", f"{FEATURE_LIST} {key}")
    kept = [text_list(document, key, FEATURE_LIST) for key in ("kept_references", "kept_columns") if key in document]
    if len(kept) == 1 or (kept and len(kept[0]) != len(kept[1])):
        raise DataRuleError("feature_list", f"{FEATURE_LIST} must hold kept_references and kept_columns together, of equal length")
    return tuple(references), tuple(columns)


def check_provenance(provenance: Any) -> None:
    """The closed shape of ``bundle/provenance.json``: the nine keys the writer writes, each value of its kind and within the metadata budgets. The closure
    value is not judged here: each reader compares it with its own. Raises ``DataRuleError`` (``provenance``)."""
    if not isinstance(provenance, dict):
        raise DataRuleError("provenance", f"{PROVENANCE} must be one JSON object")
    if not set(provenance) <= PROVENANCE_KEYS:
        raise DataRuleError("provenance", f"{PROVENANCE} holds a key outside {sorted(PROVENANCE_KEYS)}")
    nodes = [0]
    for key, value in provenance.items():
        check_metadata(value, f"{PROVENANCE} {key}", nodes)
    for key in ("run_id", "git_commit", "code_digest"):
        optional_text(provenance.get(key), "provenance", f"{PROVENANCE} {key}")
    dirty, attempt = provenance.get("git_dirty"), provenance.get("try")
    if not (dirty is None or isinstance(dirty, bool)):
        raise DataRuleError("provenance", f"{PROVENANCE} git_dirty must be null or a boolean")
    if attempt is not None and not (isinstance(attempt, int | str) and not isinstance(attempt, bool)):
        raise DataRuleError("provenance", f"{PROVENANCE} try must be null, an integer or text")
    for key in ("timings", "inputs"):
        if key in provenance and not isinstance(provenance[key], dict):
            raise DataRuleError("provenance", f"{PROVENANCE} {key} must be an object")
    versions = provenance.get("resolved_versions", {})
    if not isinstance(versions, dict) or not all(version is None or isinstance(version, str) for version in versions.values()):
        raise DataRuleError("provenance", f"{PROVENANCE} resolved_versions must be an object of texts and nulls")


def check_cell(value: Any, row: int) -> None:
    if value is None or (isinstance(value, str) and len(value) <= MAX_TEXT) or finite_number(value):
        return
    raise DataRuleError("self_check_cells", f"{SELF_CHECK} row {row} holds a cell that is not null, a finite number or short text")


def check_self_check(document: Any, references: Sequence[str]) -> tuple[list[list[Any]], list[float]]:
    """The shape of ``bundle/self_check.json`` as data: a closed key set, schema 1, a non-empty slot of at most ``MAX_TEXT`` characters, the feature list's
    references (at least one and none with an empty name: a self-check exists only for a registry candidate, so the empty lists that the writer saves for a
    result that is not registrable are a feature list matter and never reach this rule), 1 to ``MAX_ROWS`` rows as wide as the references, cells that are
    null, finite numbers or short text, and as many finite predictions as rows. Returns the rows and the predictions. Raises ``DataRuleError`` with the rule
    ``self_check_<part>``. The file is data only: no model runs."""
    if not isinstance(document, dict):
        raise DataRuleError("self_check_not_object", f"{SELF_CHECK} must be one JSON object")
    if set(document) != SELF_CHECK_KEYS or type(document["schema"]) is not int or document["schema"] != 1:
        raise DataRuleError("self_check_keys", f"{SELF_CHECK} must be one JSON object with exactly the keys {sorted(SELF_CHECK_KEYS)} and schema 1")
    if not isinstance(document["slot"], str) or not document["slot"] or len(document["slot"]) > MAX_TEXT:
        raise DataRuleError("self_check_keys", f"{SELF_CHECK} must name its slot as non-empty text of at most {MAX_TEXT} characters")
    if document["references"] != list(references):
        raise DataRuleError("self_check_references", f"the references of {SELF_CHECK} differ from those of {FEATURE_LIST}")
    if not references or not all(isinstance(name, str) and name for name in references):
        raise DataRuleError("self_check_references", f"{SELF_CHECK} must name at least one reference and no empty reference name")
    rows, predictions = document["rows"], document["predictions"]
    if not isinstance(rows, list) or not 1 <= len(rows) <= MAX_ROWS:
        raise DataRuleError("self_check_row_count", f"{SELF_CHECK} must hold 1 to {MAX_ROWS} rows")
    for index, row in enumerate(rows):
        if not isinstance(row, list) or len(row) != len(references):
            raise DataRuleError("self_check_row_width", f"{SELF_CHECK} row {index} must be a list with one cell per reference")
        for value in row:
            check_cell(value, index)
    if not isinstance(predictions, list) or len(predictions) != len(rows):
        raise DataRuleError("self_check_predictions", f"{SELF_CHECK} must hold one prediction per row")
    for index, value in enumerate(predictions):
        if not finite_number(value):
            raise DataRuleError("self_check_predictions", f"{SELF_CHECK} prediction {index} must be a finite number")
    return rows, [float(value) for value in predictions]


# The bounded single-file fetch and the data-only model read

MODEL_FILE = "model/model.skops"
STREAM_CHUNK = 1024 * 1024
# One total deadline per fetch: the run lookup, the listing, the request with every retry and the whole body count against it. A socket timeout cannot do this
# (a server that sends a byte every few seconds never trips it), so the fetch runs in a worker that the caller cancels. The sizes are provisional like the caps
# (the first live load logs the real times). The JSON files (1 MiB at most) take well under a second from the host's local MLflow, so 30 seconds is a margin of
# two orders and still a floor of 35 KiB/s. The model (64 MiB at most) takes about 6 seconds at 10 MiB/s, so 300 seconds is a margin of fifty and a floor of
# 0.21 MiB/s. The four fetches of one load run one after the other, so one load waits 30 + 3 * 30 + 300 = 420 seconds at most for the network.
JSON_FETCH_SECONDS = 30.0
MODEL_FETCH_SECONDS = 300.0
LOOKUP_SECONDS = 30.0
# How often the caller looks at the clock, and how long it waits for a cancelled worker to end before it reports the deadline anyway: a body or a socket
# read ends at once when its connection is shut down, so only a call that cannot be interrupted (a lookup or a listing that does not answer) costs the wait,
# and the refusal then comes at the deadline plus this wait at most.
POLL_SECONDS = 0.25
GRACE_SECONDS = 2.0
# The bound on abandoned work. A call that cannot be interrupted (a lookup, a listing, a request that waits for its headers or reads a metadata body inside
# MLflow) keeps its worker thread, one socket and one response buffer after its deadline. At most ``MAX_WORKERS`` = 1 such worker can be alive at once in one
# process, for the model API and for the promotion job alike: every wait of this module goes through ``run_within``, which takes the one ``WORKER_SLOT`` before
# it starts a worker and gives it back only when that worker has really ended, so a later wait finds the slot taken and is refused by name (``busy``) at its own
# deadline without starting a thread or sending a request. A body that is being streamed leaves nothing: its socket is shut down and its worker ends at once.
MAX_WORKERS = 1
WORKER_SLOT = threading.Lock()
Fetch = Callable[[str, str, str, int], str]


def monotonic() -> float:
    """The clock of every deadline (a name of its own, so that a case can replace it with a test clock)."""
    return time.monotonic()


class Cancel:
    """The cancel token of one worker. ``stop`` is final: after it no new file is created, and each registered closer has run (or runs when it is
    registered), so a response that arrives late is closed at once."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.stopped = False
        self.created = False
        self.closers: list[Callable[[], None]] = []

    def check(self, what: str) -> None:
        if self.stopped:
            raise DataRuleError("deadline", f"{what} was cancelled at its deadline")

    def watch(self, closer: Callable[[], None]) -> None:
        with self.lock:
            stopped = self.stopped
            if not stopped:
                self.closers.append(closer)
        if stopped:
            closer()

    def stop(self) -> None:
        with self.lock:
            self.stopped = True
            closers, self.closers = self.closers, []
        for closer in closers:
            with contextlib.suppress(Exception):  # a closer that fails must not stop the others
                closer()

    def create(self, path: str) -> int:
        """Create the new file of the worker, or refuse when the deadline already passed (the check and the creation are one step, so that no file appears
        after ``stop``). A file that was not created here (it existed already) is never removed by the caller."""
        with self.lock:
            if self.stopped:
                raise DataRuleError("deadline", f"the fetch into {os.path.basename(path)} was cancelled at its deadline")
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            self.created = True
            return descriptor


def run_within[Outcome](seconds: float, work: Callable[[Cancel], Outcome], what: str) -> Outcome:
    """Run ``work(cancel)`` in a worker thread and return its result, or raise its error, or at ``seconds`` stop and raise ``DataRuleError`` (``deadline``).

    The caller looks at the clock while the worker runs, so a body that is read a byte at a time, a listing that does not answer and a request that is retried
    all end at the deadline. At expiry the caller cancels the worker (its open response is closed) and waits ``GRACE_SECONDS`` for it to end. A worker that
    is still inside a call that cannot be interrupted (a lookup that does not answer) is left to end when that call returns: it finds the token stopped,
    creates no file and starts no request. The one ``WORKER_SLOT`` is taken before the worker starts, inside the same deadline (the time spent waiting for it
    counts against ``seconds``, so it adds no budget), and given back by the worker itself when it ends, even after the caller gave up: a call that finds the
    slot taken for the whole deadline raises ``DataRuleError`` (``busy``) and starts nothing.
    """
    cancel = Cancel()
    outcome: list[tuple[bool, Any]] = []
    expires = monotonic() + seconds
    while True:
        left = expires - monotonic()
        if WORKER_SLOT.acquire(timeout=max(min(left, POLL_SECONDS), 0.0)):
            break
        if left <= 0:
            raise DataRuleError("busy", f"{what} was refused as busy: an earlier call that was cancelled at its deadline still holds the one worker")

    def target() -> None:
        try:
            try:
                outcome.append((True, work(cancel)))
            except BaseException as error:  # the caller re-raises it
                outcome.append((False, error))
        finally:
            WORKER_SLOT.release()

    try:
        worker = threading.Thread(target=target, name=f"model-fetch {what}", daemon=True)
        worker.start()
    except BaseException:
        WORKER_SLOT.release()
        raise
    while worker.is_alive():
        left = expires - monotonic()
        if left <= 0:
            cancel.stop()
            worker.join(GRACE_SECONDS)
            raise DataRuleError("deadline", f"{what} did not finish within its deadline of {seconds:g} seconds")
        worker.join(min(left, POLL_SECONDS))
    ok, value = outcome[0]
    if not ok:
        raise value
    result: Outcome = value
    return result


def hang_up(response: Any) -> None:
    """Close the connection of a streamed response from another thread: shut its socket down first (a read that waits in the socket then ends at once, which
    ``close`` alone does not do), then close. The socket is the one the response reads from (``raw._fp.fp.raw._sock``, which stays when http.client has already
    dropped the connection's own handle of a close-delimited response) or the connection's. Without either, ``close`` is all there is."""
    raw = getattr(response, "raw", None)
    for path in (("_fp", "fp", "raw", "_sock"), ("_connection", "sock")):
        sock: Any = raw
        for name in path:
            sock = getattr(sock, name, None)
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            break
    response.close()


def write_limited(chunks: Iterable[bytes], local_path: str, limit: int, name: str, cancel: Cancel | None = None) -> None:
    """Write the chunks to a new file at ``local_path`` (never through a link). The running total is checked before each write, so a chunk that would pass
    ``limit`` is not written and the transport is not read any further. A stopped ``cancel`` ends the write at the next chunk and creates no file. Raises
    ``DataRuleError`` (``too_large`` or ``deadline``)."""
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    written = 0
    with os.fdopen(cancel.create(local_path) if cancel else os.open(local_path, flags, 0o600), "wb") as stream:
        for chunk in chunks:
            if cancel is not None:
                cancel.check(name)
            if written + len(chunk) > limit:
                raise DataRuleError("too_large", f"{name} is over {limit} bytes")
            stream.write(chunk)
            written += len(chunk)


def mlflow_fetch(run_id: str, artifact_path: str, local_path: str, limit: int, seconds: float | None = None) -> str:
    """Stream one file of a run to ``local_path`` and return that path: never more than ``limit`` bytes and never longer than ``seconds`` (the default is
    ``MODEL_FETCH_SECONDS`` for the model file and ``JSON_FETCH_SECONDS`` for any other). The caller chooses ``local_path``, the cap and the time: the artifact
    never does. Any other reader of a run's single file can call it with its own path, cap and time.

    It does not call ``download_artifacts``, which lists the path and downloads every file below a folder with no limit (MLflow 3.17.2
    ``artifact_repo.py:346``, ``http_artifact_repo.py:145``). It looks up the run, lists the path once, refuses a folder, then sends the one streamed GET of
    ``_download_file`` and counts each chunk, all inside the one deadline. At the deadline it closes the response, removes the file it wrote and raises
    ``DataRuleError`` (``deadline``); on any other failure it removes the file too. Other errors: ``not_served``, ``not_a_file`` and ``too_large``; a missing
    file or a failed request raises the MLflow or requests error as it is.
    """
    cancel_ref: list[Cancel] = []

    def transfer(cancel: Cancel) -> None:
        cancel_ref.append(cancel)
        mlflow = importlib.import_module("mlflow")
        registry = importlib.import_module("mlflow.store.artifact.artifact_repository_registry")
        http = importlib.import_module("mlflow.store.artifact.http_artifact_repo")
        credentials = importlib.import_module("mlflow.utils.credentials")
        rest = importlib.import_module("mlflow.utils.rest_utils")
        repository = registry.get_artifact_repository(mlflow.MlflowClient().get_run(run_id).info.artifact_uri)
        cancel.check(artifact_path)
        if not isinstance(repository, http.HttpArtifactRepository):
            raise DataRuleError("not_served", "the run's artifacts are not served by the tracking server")
        if repository.list_artifacts(artifact_path):
            raise DataRuleError("not_a_file", f"{artifact_path} is a folder, not a file")
        cancel.check(artifact_path)
        response = rest.http_request(credentials.get_default_host_creds(repository.artifact_uri), posixpath.join("/", artifact_path), "GET", stream=True)
        cancel.watch(lambda: hang_up(response))
        try:
            cancel.check(artifact_path)
            rest.augmented_raise_for_status(response)
            write_limited(response.iter_content(chunk_size=STREAM_CHUNK), local_path, limit, artifact_path, cancel)
        finally:
            response.close()

    if seconds is None:
        seconds = MODEL_FETCH_SECONDS if artifact_path == MODEL_FILE else JSON_FETCH_SECONDS
    try:
        run_within(seconds, transfer, f"the fetch of {artifact_path}")
    except BaseException:
        if cancel_ref and cancel_ref[0].created:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(local_path)
        raise
    return local_path


def fetch_regular_file(fetch: Fetch, run_id: str, artifact_path: str, local_path: str, limit: int) -> str:
    """Call ``fetch(run_id, artifact_path, local_path, limit)`` and require the result to be ``local_path`` itself, one regular file (no link, folder or other
    file) of at most ``limit`` bytes. Raises ``DataRuleError`` (``not_a_file`` or ``too_large``); an error of the fetch passes through unchanged."""
    returned = fetch(run_id, artifact_path, local_path, limit)
    try:
        status = os.lstat(local_path)
    except OSError as error:
        raise DataRuleError("not_a_file", f"{artifact_path} was not written to its local file") from error
    if os.fspath(returned) != local_path or not stat.S_ISREG(status.st_mode):
        raise DataRuleError("not_a_file", f"{artifact_path} is not one regular file at the path chosen for it")
    if status.st_size > limit:
        raise DataRuleError("too_large", f"{artifact_path} is over {limit} bytes")
    return local_path


def load_data_only(path: str, trusted: Iterable[str]) -> Any:
    """Read one ``model.skops`` file as data with skops, under the types the code root's own ``model_contract.json`` trusts (``serialization.trusted_types``),
    never a list that the artifact brings. It reads no ``MLmodel``, imports no artifact code and loads no pickle."""
    return importlib.import_module("skops.io").load(path, trusted=list(trusted))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check or regenerate the model code closure record.")
    parser.add_argument("command", choices=("check", "write"))
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--generation", type=int, help="the generation to write (required for write)")
    arguments = parser.parse_args(argv)
    if arguments.command == "write":
        if arguments.generation is None:
            parser.error("write needs --generation")
        record = write(arguments.root, arguments.generation)
        sys.stdout.write(f"wrote {RECORD}: {len(record['files'])} files, closure_sha256 {record['closure_sha256']}\n")
        return 0
    problems = verify(arguments.root)
    for problem in problems:
        sys.stdout.write(f"{problem}\n")
    sys.stdout.write("model code record: " + ("current\n" if not problems else f"{len(problems)} problem(s); regenerate it on purpose with write\n"))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
