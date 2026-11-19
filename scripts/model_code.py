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
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import sys
import tomllib
from collections.abc import Collection, Iterable, Mapping, Sequence
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
REFUSED_NAMES = frozenset({"exec", "eval", "compile", "__builtins__"})
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
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    self.aliases[alias.asname or alias.name.split(".")[0]] = alias.name if alias.asname else alias.name.split(".")[0]
            elif isinstance(node, ast.ImportFrom):
                base = self.absolute(node.level, node.module)
                if base is not None:
                    for alias in node.names:
                        self.aliases[alias.asname or alias.name] = f"{base}.{alias.name}" if base else alias.name
        for _ in range(3):
            for node in ast.walk(tree):
                if isinstance(node, ast.Assign | ast.AnnAssign) and node.value is not None:
                    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                    value = self.dotted(node.value)
                    if value is not None and self.root_name(node.value) in self.aliases:
                        for target in targets:
                            if isinstance(target, ast.Name) and target.id not in self.aliases:
                                self.aliases[target.id] = value

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
        self.add(node, name, tuple(alias.name for alias in node.names))

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
        if bare in REFUSED_NAMES and bare not in self.aliases:
            self.refuse(node, f"uses {bare}, which runs code this check cannot read")
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

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        keys = [key for key, _ in items]
        if len(keys) != len(set(keys)):
            raise RecordError("a key appears twice")
        return dict(items)

    try:
        record = json.loads(path.read_text(), object_pairs_hook=pairs)
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
