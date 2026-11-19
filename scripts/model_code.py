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
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RECORD = "data_contracts/training/model_code.json"
SCHEMA = "data_contracts/training/model_code.schema.json"
SCHEMA_ID = "model_code/1"
CONTRACT = "data_contracts/training/model_contract.json"
# The files of the closure that the model API image holds already (services/model_api/Dockerfile copies them), so the record does not list them.
IMAGE_PROVIDED = ("orchestration/__init__.py", "orchestration/training_contract.py")
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


def imported_modules(root: Path, path: str) -> list[tuple[str, tuple[str, ...]]]:
    """Every module a file imports as (dotted name, names taken from it), with relative imports made absolute. Imports in any scope count."""
    tree = ast.parse((root / path).read_bytes(), filename=path)
    package = list(Path(path).parts[:-1])
    found: list[tuple[str, tuple[str, ...]]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found += [(alias.name, ()) for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            base = package[: len(package) - (node.level - 1)] if node.level else []
            name = ".".join([*base, *([node.module] if node.module else [])])
            found.append((name, tuple(alias.name for alias in node.names)))
        elif isinstance(node, ast.Call) and node.args:
            function = node.func
            called = function.attr if isinstance(function, ast.Attribute) else function.id if isinstance(function, ast.Name) else ""
            first = node.args[0]
            if called == "import_module" and isinstance(first, ast.Constant) and isinstance(first.value, str):
                found.append((first.value, ()))
    return found


def repository_imports(root: Path, path: str) -> set[str]:
    """The dotted names of the repository modules a file imports (a library or a standard library name is left out)."""
    return {name for name, _ in imported_modules(root, path) if name and is_repository_name(root, name)}


def names_imported_from(root: Path, path: str, module: str) -> set[str]:
    """The names a file takes with ``from <module> import ...``."""
    return {name for source, names in imported_modules(root, path) if source == module for name in names}


def trusted_modules(root: Path) -> list[str]:
    """The repository files of the modules that hold the contract's trusted types. A type of a library (xgboost) has no repository file."""
    contract = json.loads((root / CONTRACT).read_text())
    found = {module_file(root, name.rpartition(".")[0]) for name in contract["serialization"]["trusted_types"]}
    return sorted(path for path in found if path)


def closure(root: Path) -> tuple[list[str], list[str]]:
    """The closure files, sorted, and the repository imports that resolve to no file."""
    pending = list(trusted_modules(root))
    seen: set[str] = set()
    unresolved: set[str] = set()
    while pending:
        path = pending.pop()
        if path in seen:
            continue
        seen.add(path)
        pending.extend(packages_of(root, path))
        for name, taken in imported_modules(root, path):
            if not name or not is_repository_name(root, name):
                continue
            target = module_file(root, name)
            if target is None:
                unresolved.add(name)
                continue
            pending.append(target)
            for item in taken:
                submodule = module_file(root, f"{name}.{item}")
                if submodule:
                    pending.append(submodule)
    return sorted(seen), sorted(unresolved)


def import_closure(root: Path) -> list[str]:
    """Every file the trusted modules need, the image-provided ones included."""
    return closure(root)[0]


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


def build_record(root: Path, generation: int) -> dict[str, Any]:
    """The record of the closure at ``root``. Refuses an import that resolves to no file and an image-provided file that the closure does not need."""
    files, unresolved = closure(root)
    if unresolved:
        raise RecordError(f"the trusted modules import repository modules that do not exist: {unresolved}")
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


def write(root: Path, generation: int) -> dict[str, Any]:
    """Regenerate the record file. A deliberate step after a reviewed closure change, never an automatic repair."""
    record = build_record(root, generation)
    (root / RECORD).write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    return record


def verify(root: Path) -> list[str]:
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
        found, unresolved = closure(root)
    except (OSError, SyntaxError) as error:
        return [*problems, f"closure_changed: the import closure cannot be read: {error}"]
    listed = {item["path"] for item in record["files"]} | set(record["image_provided"])
    problems += [f"closure_changed: {path} is needed by the trusted modules and not in the record" for path in sorted(set(found) - listed)]
    problems += [f"closure_changed: {path} is in the record and not needed by the trusted modules" for path in sorted(listed - set(found))]
    problems += [f"closure_changed: the trusted modules import {name}, which does not exist" for name in unresolved]
    return problems


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
