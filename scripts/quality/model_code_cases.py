"""Cases of the model code closure record (slice C1 of the model API redesign).

    .venv/bin/python -m scripts.quality.model_code_cases [--output FILE]

The model API image will carry the trusted model-family modules, so a model load needs only the model artifact. The closure is the set of source files those
modules need to load and predict. ``data_contracts/training/model_code.json`` records the closure with the size and SHA-256 of each file and one digest over
them, and ``scripts/model_code.py`` (standard library only, because the image runs it) builds, reads and verifies the record. The cases run in this order: the
source closure, the import origins in a clean interpreter, the changed file, the closed schema, then the digest and the files that must not change it. The
cases that change files work on a scratch copy; none writes into the repository."""

from __future__ import annotations

import argparse
import ast
import copy
import functools
import hashlib
import importlib
import json
import re
import shutil
import sys
import tempfile
import time
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts import model_code
from scripts import model_data_matrix as data_rule_matrix
from scripts.process import run_command
from scripts.quality import ci_groups, model_code_behavior

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = "data_contracts/training/model_contract.json"
DOCKERFILE = "services/model_api/Dockerfile"
# The Python modules of the trusted types that must stay outside the closure: the m3, m5 and m6 families and the suite are not registrable.
OUTSIDE = (
    "scripts/training/models/mixed.py",
    "scripts/training/models/panel.py",
    "scripts/training/models/spatial.py",
    "scripts/training/models/suite.py",
)
# What the closure takes from the image-provided module. The stand-in below must define exactly these names.
STAND_IN_NAMES = frozenset({"MODEL_LISTS"})
STAND_IN = model_code_behavior.STAND_IN
# Literal code for a clean interpreter (-I: no PYTHONPATH, no working directory on the path, no user site). It imports each trusted type of the repository and
# prints where every repository module came from. The scratch folder and the type names are its arguments.
PROBE = """
import json, pkgutil, sys
scratch, names = sys.argv[1], json.loads(sys.argv[2])
sys.path.insert(0, scratch)
loaded = {name: pkgutil.resolve_name(name).__module__ for name in names}
origins = {}
for key, module in sorted(sys.modules.items()):
    if key.split(".")[0] in ("scripts", "orchestration"):
        path = getattr(module, "__file__", None)
        origins[key] = path if path else sorted(getattr(module, "__path__", []))
print(json.dumps({"types": loaded, "origins": origins}))
"""

Case = Callable[[], list[str]]


def contract_types(root: Path) -> list[str]:
    return list(json.loads((root / CONTRACT).read_text())["serialization"]["trusted_types"])


def repository_types(root: Path) -> list[str]:
    """The trusted types that live in repository modules (the others come from a library)."""
    return [name for name in contract_types(root) if name.startswith("scripts.")]


def scratch_tree(base: Path, root: Path = ROOT) -> Path:
    """A copy of everything the record, its verifier and the probe read: the closure, the image-provided files, the contract, the record and the Dockerfile."""
    record = json.loads((root / model_code.RECORD).read_text())
    names = [item["path"] for item in record["files"]] + list(record["image_provided"]) + [CONTRACT, model_code.RECORD, model_code.SCHEMA, DOCKERFILE]
    for name in sorted(set(names)):
        target = base / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / name, target)
    (base / "uv.lock").symlink_to(root / "uv.lock")
    return base


def read_record(root: Path = ROOT) -> dict[str, Any]:
    record: dict[str, Any] = json.loads((root / model_code.RECORD).read_text())
    return record


def write_record(root: Path, record: dict[str, Any]) -> None:
    (root / model_code.RECORD).write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")


def good_record() -> dict[str, Any]:
    sha = "0" * 64
    return {
        "schema": model_code.SCHEMA_ID,
        "generation": 1,
        "image_provided": ["orchestration/__init__.py", "orchestration/training_contract.py"],
        "files": [{"path": "scripts/training/__init__.py", "size": 0, "sha256": sha}],
        "closure_sha256": sha,
    }


def probe(scratch: Path, names: list[str]) -> tuple[int, dict[str, Any], str]:
    result = run_command(sys.executable, ["-I", "-B", "-c", PROBE, str(scratch), json.dumps(names)], cwd=scratch, timeout=120)
    try:
        report = json.loads(result.stdout.strip().splitlines()[-1]) if result.returncode == 0 else {}
    except (IndexError, ValueError):
        report = {}
    return result.returncode, report, result.stderr


# 1. The source closure


def case_closure_equals_import_closure() -> list[str]:
    """The record's files plus the image-provided files are exactly the AST import closure of the trusted modules."""
    record, found = read_record(), model_code.import_closure(ROOT)
    listed = sorted([item["path"] for item in record["files"]] + record["image_provided"])
    return [] if listed == sorted(found) else [f"record lists {listed}, the import closure is {sorted(found)}"]


def case_closure_roots_come_from_the_contract() -> list[str]:
    """The roots are the repository modules of the contract's trusted types, and no module of an untrusted family or the suite is in the closure."""
    problems = []
    roots = model_code.trusted_modules(ROOT)
    expected = sorted({name.rpartition(".")[0].replace(".", "/") + ".py" for name in repository_types(ROOT)})
    if sorted(roots) != expected:
        problems.append(f"roots {sorted(roots)} differ from the contract's trusted type modules {expected}")
    listed = {item["path"] for item in read_record()["files"]}
    problems += [f"{name} is in the closure but its type is not trusted" for name in OUTSIDE if name in listed]
    problems += [f"trusted module {name} is not in the record" for name in expected if name not in listed]
    return problems


def case_image_provided_files_are_in_the_dockerfile() -> list[str]:
    """Every file the record leaves to the image is copied by the model API Dockerfile today; otherwise the closure would be incomplete in the image."""
    text = (ROOT / DOCKERFILE).read_text()
    copied = {token for line in text.splitlines() if line.startswith("COPY") for token in line.split() if token.endswith(".py")}
    return [f"{name} is not copied by {DOCKERFILE}" for name in read_record()["image_provided"] if name not in copied]


def dockerfile_copies() -> set[str]:
    """The repository paths that the model API Dockerfile copies into /app, from its COPY lines (the destination folder plus each source's file name)."""
    copied: set[str] = set()
    for line in (ROOT / DOCKERFILE).read_text().splitlines():
        if not line.startswith("COPY"):
            continue
        tokens = [token for token in line.split()[1:] if not token.startswith("--")]
        *sources, destination = tokens
        if not destination.startswith("/app/"):
            continue
        folder = destination.removeprefix("/app/")
        copied |= {folder + Path(source).name if folder.endswith("/") else folder for source in sources}
    return copied


def case_dockerfile_copies_exactly_the_files_of_the_record() -> list[str]:
    """The image holds the model code: the files under scripts/training that the Dockerfile copies are exactly the record's files, and the verifier, the
    record and the model contract come with them."""
    copied = dockerfile_copies()
    wanted = {item["path"] for item in read_record()["files"]}
    held = {name for name in copied if name.startswith("scripts/training/")}
    problems = [f"{name} is in the record and {DOCKERFILE} does not copy it" for name in sorted(wanted - held)]
    problems += [f"{DOCKERFILE} copies {name}, which the record does not list" for name in sorted(held - wanted)]
    problems += [f"{name} is not copied by {DOCKERFILE}" for name in (model_code.RECORD, "scripts/model_code.py", CONTRACT) if name not in copied]
    problems += [f"{DOCKERFILE} copies {name}, which is not a file of the repository" for name in sorted(copied) if not (ROOT / name).is_file()]
    return problems


def case_image_provided_module_needs_only_the_standard_library() -> list[str]:
    """The image-provided module imports no repository module, so nothing hides behind it."""
    problems = []
    for name in read_record()["image_provided"]:
        outside = model_code.repository_imports(ROOT, name)
        if outside:
            problems.append(f"{name} imports repository modules {sorted(outside)}")
    return problems


def good_bundle_documents() -> dict[str, Any]:
    """The three bundle documents of a model with two features, as the writer writes them (the closure value is a placeholder digest)."""
    return {
        data_rule_matrix.FEATURE_LIST: {"slot": "m2_elastic_net", "references": ["a:1", "b:2"], "columns": ["a", "b"], "fitted_slot": None},
        data_rule_matrix.PROVENANCE: {
            "run_id": "train_1",
            "timings": {"fit_seconds": 1.5},
            "model_code_closure_sha256": "0" * 64,
            "resolved_versions": {"numpy": "2.5.3"},
        },
        data_rule_matrix.SELF_CHECK: {
            "schema": 1,
            "slot": "m2_elastic_net",
            "references": ["a:1", "b:2"],
            "rows": [[1.0, "x"], [None, "y"]],
            "predictions": [0.5, 1.5],
        },
    }


def apply_shared_rules(file: str, data: bytes, references: tuple[str, ...]) -> None:
    """Parse and check one file with the shared functions, as every reader does. Raises ``model_code.DataRuleError``."""
    document = model_code.parse_json(data, file)
    if file == data_rule_matrix.FEATURE_LIST:
        model_code.check_feature_list(document)
    elif file == data_rule_matrix.PROVENANCE:
        model_code.check_provenance(document)
    else:
        model_code.check_self_check(document, references)


def case_shared_data_rules_name_the_rule_each_matrix_document_breaks() -> list[str]:
    """Every document of the shared matrix (``scripts/quality/data_rule_matrix.py``) is refused with the rule the matrix names, or accepted when it names none.
    The loader, the promote action and the promotion check run the same list, so a rule that one of them lacks fails their case."""
    problems = []
    good = good_bundle_documents()
    references = tuple(good[data_rule_matrix.FEATURE_LIST]["references"])
    entries = data_rule_matrix.entries()
    if len({entry.name for entry in entries}) != len(entries):
        problems.append("two entries of the matrix share a name")
    for entry in entries:
        built = data_rule_matrix.build(entry, good[entry.file])
        data = built if isinstance(built, bytes) else (json.dumps(built, sort_keys=True) + "\n").encode()
        try:
            apply_shared_rules(entry.file, data, references)
            found = ""
        except model_code.DataRuleError as error:
            found = error.rule
        if found != entry.rule:
            problems.append(f"{entry.name}: the shared rules gave {found or 'no refusal'}, and the matrix names {entry.rule or 'no refusal'}")
    for file in (data_rule_matrix.FEATURE_LIST, data_rule_matrix.PROVENANCE, data_rule_matrix.SELF_CHECK):
        try:
            apply_shared_rules(file, (json.dumps(good[file]) + "\n").encode(), references)
        except model_code.DataRuleError as error:
            problems.append(f"the good {file} was refused as {error.rule}")
    for probe in ("duplicate_schema_key", "utf16_self_check", "valid_provenance_with_an_unused_nan_field"):
        if probe not in {entry.name for entry in entries}:
            problems.append(f"the matrix lacks the reviewer's probe {probe}")
    return problems


def case_shared_data_rules_import_only_the_standard_library() -> list[str]:
    """``scripts/model_code.py`` runs in the model API image and in the training code root as it is: its imports are all standard library, and it is not one of
    the files of the closure record, so changing it does not change the closure digest."""
    tree = ast.parse((ROOT / "scripts/model_code.py").read_text())
    imported = {alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    imported |= {(node.module or "").split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.level == 0}
    problems = [f"scripts/model_code.py imports {name}, which is not in the standard library" for name in sorted(imported - set(sys.stdlib_module_names))]
    if "scripts/model_code.py" in {item["path"] for item in read_record()["files"]}:
        problems.append("scripts/model_code.py is a file of the closure record, so a change to it would change the closure digest")
    return problems


def case_one_home_for_the_bounded_fetch_and_the_data_only_read() -> list[str]:
    """The bounded single-file fetch (``write_limited``, ``mlflow_fetch``, ``fetch_regular_file``) and the data-only model read (``load_data_only``) are defined
    in ``scripts/model_code.py`` and nowhere else, and the two readers of a model (the serving loader and the promotion check) call them there: a second copy
    would be a second set of caps."""
    names = ("write_limited", "mlflow_fetch", "fetch_regular_file", "load_data_only")
    homes: dict[str, list[str]] = {name: [] for name in names}
    for path in sorted(Path(model_code.__file__).resolve().parent.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name in homes:
                homes[node.name].append(str(path.relative_to(ROOT)))
    problems = [f"{name} is defined in {found}, not once in scripts/model_code.py" for name, found in homes.items() if found != ["scripts/model_code.py"]]
    for reader in ("scripts/serving/mlflow_loader.py", "scripts/training/promotion_check.py"):
        text = (ROOT / reader).read_text()
        problems += [f"{reader} does not call model_code.{name}" for name in ("fetch_regular_file", "load_data_only") if f"model_code.{name}" not in text]
        if 'import_module("mlflow.sklearn")' in text or "pickle" in text:
            problems.append(f"{reader} loads a model through mlflow.sklearn or pickle")
    return problems


def case_planted_import_changes_the_closure() -> list[str]:
    """A closure file that starts importing an untrusted module changes the closure, and the verifier names the file the record misses."""
    with tempfile.TemporaryDirectory() as folder:
        scratch = scratch_tree(Path(folder))
        for name in OUTSIDE[:1]:
            target = scratch / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, target)
        trees = scratch / "scripts/training/models/trees.py"
        trees.write_text(trees.read_text() + "\nfrom scripts.training.models import mixed as _mixed\n")
        problems = model_code.verify(scratch)
    needed = [text for text in problems if text.startswith("closure_changed") and "scripts/training/models/mixed.py" in text]
    return [] if needed else [f"no closure_changed problem for mixed.py in {problems}"]


# 1b. Every way a closure module can reach code outside the closure

TARGET = "scripts/training/models/probe_target.py"
TARGET_MODULE = "scripts.training.models.probe_target"
TREES = "scripts/training/models/trees.py"

# Text appended to trees.py (@T is the module the planted code reaches). Each entry reaches probe_target.py by a different route; every one must make the
# closure grow by that file, and the record then verifies only after an explicit write.
JOINS = {
    "plain import": "import @T\n",
    "import as": "import @T as probe\n",
    "from-import of a module": "from scripts.training.models import probe_target\n",
    "from-import of a module as": "from scripts.training.models import probe_target as probe\n",
    "from-import of a name as": "from @T import VALUE as probe_value\n",
    "relative from-import of a module": "from . import probe_target\n",
    "relative from-import of a name": "from .probe_target import VALUE\n",
    "import in a function": "def _planted():\n    import @T\n",
    "import in a class body": "class _Planted:\n    import @T\n",
    "import in try and except ImportError": "try:\n    import @T\nexcept ImportError:\n    pass\n",
    "import under TYPE_CHECKING": "from typing import TYPE_CHECKING\n\nif TYPE_CHECKING:\n    from @T import VALUE\n",
    "importlib literal in a function": 'def _planted():\n    importlib.import_module("@T")\n',
    "importlib module alias": 'import importlib as il\n\n\ndef _planted():\n    il.import_module("@T")\n',
    "import_module alias": 'from importlib import import_module as load_module\n\n\ndef _planted():\n    load_module("@T")\n',
    "import_module by its own name": 'from importlib import import_module\n\n\ndef _planted():\n    import_module("@T")\n',
    "import_module keyword argument": 'def _planted():\n    importlib.import_module(name="@T")\n',
    "import_module relative with a package": 'def _planted():\n    importlib.import_module(".probe_target", package="scripts.training.models")\n',
    "importlib.__import__": 'def _planted():\n    importlib.__import__("@T")\n',
    "builtins.__import__": 'import builtins\n\n\ndef _planted():\n    builtins.__import__("@T")\n',
    "__import__ literal": 'def _planted():\n    __import__("@T")\n',
    "__import__ with fromlist in try and except": 'try:\n    __import__("@T", fromlist=["VALUE"])\nexcept ModuleNotFoundError:\n    pass\n',
    "pkgutil.resolve_name dotted": 'import pkgutil\n\n\ndef _planted():\n    pkgutil.resolve_name("@T.VALUE")\n',
    "pkgutil.resolve_name colon": 'import pkgutil as pk\n\n\ndef _planted():\n    pk.resolve_name("@T:VALUE")\n',
    "attribute through the package alias": "import scripts.training.models as pkg_alias\n\n\ndef _planted():\n    return pkg_alias.probe_target.VALUE\n",
    "attribute through the full dotted path": "import scripts.training.models\n\n\ndef _planted():\n    return scripts.training.models.probe_target.VALUE\n",
    "attribute through a from-imported package": "from scripts.training import models as pkg\n\n\ndef _planted():\n    return pkg.probe_target.VALUE\n",
}

# Code the verifier cannot read: the write is refused and the verifier names the file and line. The reason is the start of the problem text.
FETCH = 'from importlib import import_module as fetch_code\n\n\ndef _planted():\n    fetch_code("@T")\n\n\n'
PACKAGE = "import scripts.training.models as pkg_alias\n\n\ndef _planted():\n    return pkg_alias.probe_target.VALUE\n\n\n"
REFUSED = {
    "literal held in a variable": ('def _planted():\n    module = "@T"\n    importlib.import_module(module)\n', "dynamic_import_refused"),
    "literal held in a variable, optional": (
        'try:\n    _module = "@T"\n    importlib.import_module(_module)\nexcept ModuleNotFoundError:\n    pass\n',
        "dynamic_import_refused",
    ),
    "concatenated name": ('def _planted():\n    importlib.import_module("scripts.training.models." + "probe_target")\n', "dynamic_import_refused"),
    "f-string name": ('def _planted(x):\n    importlib.import_module(f"scripts.training.models.{x}")\n', "dynamic_import_refused"),
    "parameter as name": ("def _planted(name):\n    importlib.import_module(name)\n", "dynamic_import_refused"),
    "__import__ with a variable": ("def _planted(name):\n    __import__(name)\n", "dynamic_import_refused"),
    "__import__ with a variable, optional": ('try:\n    _m = "@T"\n    __import__(_m)\nexcept ModuleNotFoundError:\n    pass\n', "dynamic_import_refused"),
    "__import__ with a level": ('def _planted():\n    __import__("probe_target", globals(), None, [], 1)\n', "dynamic_import_refused"),
    "pkgutil.resolve_name with a variable": ("import pkgutil\n\n\ndef _planted(name):\n    pkgutil.resolve_name(name)\n", "dynamic_import_refused"),
    "alias called with a variable": ("from importlib import import_module as load\n\n\ndef _planted(name):\n    load(name)\n", "dynamic_import_refused"),
    "starred arguments": ("def _planted(args):\n    importlib.import_module(*args)\n", "dynamic_import_refused"),
    "no arguments": ("def _planted():\n    importlib.import_module()\n", "dynamic_import_refused"),
    "relative name without a package": ('def _planted():\n    importlib.import_module(".probe_target")\n', "dynamic_import_refused"),
    "relative name with a variable package": ('def _planted(p):\n    importlib.import_module(".probe_target", package=p)\n', "dynamic_import_refused"),
    "fromlist held in a variable": ('def _planted(names):\n    __import__("@T", fromlist=names)\n', "dynamic_import_refused"),
    "importlib module passed to getattr": ('def _planted():\n    return getattr(importlib, "import_module")("@T")\n', "dynamic_import_refused"),
    "import function stored in a variable": ('def _planted():\n    loader = importlib.import_module\n    loader("@T")\n', "dynamic_import_refused"),
    "import function alias stored": ("from importlib import import_module as a\n\nb = a\n", "dynamic_import_refused"),
    "__import__ stored": ("loader = __import__\n", "dynamic_import_refused"),
    "builtins module passed to getattr": ('import builtins\n\n\ndef _planted():\n    return getattr(builtins, "__import__")("@T")\n', "dynamic_import_refused"),
    "__builtins__": ("def _planted():\n    return __builtins__\n", "dynamic_import_refused"),
    "importlib.util": ('import importlib.util\n\n\ndef _planted():\n    importlib.util.spec_from_file_location("x", "x.py")\n', "dynamic_import_refused"),
    "importlib.machinery": ("import importlib.machinery as machinery\n\n\ndef _planted():\n    return machinery.SourceFileLoader\n", "dynamic_import_refused"),
    "runpy": ('import runpy\n\n\ndef _planted():\n    runpy.run_module("@T")\n', "dynamic_import_refused"),
    "exec": ('def _planted():\n    exec("import @T")\n', "dynamic_import_refused"),
    "eval": ('def _planted():\n    eval("1")\n', "dynamic_import_refused"),
    "compile": ('def _planted():\n    compile("1", "x", "eval")\n', "dynamic_import_refused"),
    "builtins exec by an imported alias": ('from builtins import exec as run_text\n\n\ndef _planted():\n    run_text("import @T")\n', "dynamic_import_refused"),
    "builtins exec imported without an alias": ('from builtins import exec\n\n\ndef _planted():\n    exec("import @T")\n', "dynamic_import_refused"),
    "builtins exec by attribute": ('import builtins\n\n\ndef _planted():\n    builtins.exec("import @T")\n', "dynamic_import_refused"),
    "builtins eval by attribute through an alias": ('import builtins as b\n\n\ndef _planted():\n    return b.eval("40 + 2")\n', "dynamic_import_refused"),
    "builtins compile by an imported alias": (
        'from builtins import compile as build\n\n\ndef _planted():\n    return build("1", "x", "eval")\n',
        "dynamic_import_refused",
    ),
    "exec stored in a variable": ("runner = exec\n", "dynamic_import_refused"),
    "eval passed to a function": ('def _planted():\n    return list(map(eval, ["1"]))\n', "dynamic_import_refused"),
    "wildcard import from importlib": ("from importlib import *\n", "dynamic_import_refused"),
    "wildcard import from the standard library": ("from json import *\n", "dynamic_import_refused"),
    "wildcard import from a repository module": ("from scripts.training.models.common import *\n", "dynamic_import_refused"),
    "import alias rebound by an import in an unrelated function": (
        FETCH + 'def _unrelated():\n    from json import loads as fetch_code\n    return fetch_code("1")\n',
        "dynamic_import_refused",
    ),
    "import alias rebound by a parameter": (
        'from importlib import import_module as fetch_code\n\n\ndef _planted():\n    fetch_code("@T")\n\n\ndef _other(fetch_code):\n    return fetch_code\n',
        "dynamic_import_refused",
    ),
    "import alias rebound by an assignment": (
        FETCH + "def _other():\n    fetch_code = len\n    return fetch_code\n",
        "dynamic_import_refused",
    ),
    "import alias rebound by a loop variable": (
        FETCH + "def _other():\n    for fetch_code in []:\n        pass\n",
        "dynamic_import_refused",
    ),
    "importlib name rebound by a parameter": ("def _other(importlib):\n    return importlib\n", "dynamic_import_refused"),
    "package alias rebound by a parameter": (
        PACKAGE + "def _other(pkg_alias):\n    return pkg_alias\n",
        "dynamic_import_refused",
    ),
    "package alias rebound by an import": (
        PACKAGE + "def _other():\n    import json as pkg_alias\n    return pkg_alias\n",
        "dynamic_import_refused",
    ),
    "imported package assigned with an annotation": (
        "import scripts.training.models as pkg_alias\n\nother_alias: object = pkg_alias\n",
        "dynamic_import_refused",
    ),
    "imported package assigned to another name": ("import scripts.training.models as pkg_alias\n\nother_alias = pkg_alias\n", "dynamic_import_refused"),
    "imported repository module assigned to another name": ("from scripts.training.models import common\n\nother_alias = common\n", "dynamic_import_refused"),
    "search path change": ('import sys\n\nsys.path.insert(0, "/elsewhere")\n', "dynamic_import_refused"),
    "meta path change": ("import sys\n\nsys.meta_path.append(object())\n", "dynamic_import_refused"),
    "third-party package outside the lock group": ("import requests\n", "import_outside"),
    "third-party package in try and except ImportError": ("try:\n    import torch\nexcept ImportError:\n    torch = None\n", "import_outside"),
    "third-party package in a function": ("def _planted():\n    import yaml\n", "import_outside"),
    "from a third-party package": ("from requests import get\n", "import_outside"),
    "third-party sub-package": ("import torch.nn\n", "import_outside"),
    "third-party package by importlib": ('def _planted():\n    importlib.import_module("requests")\n', "import_outside"),
    "third-party package by __import__ in try": ('try:\n    __import__("requests")\nexcept ModuleNotFoundError:\n    pass\n', "import_outside"),
    "third-party package by an alias": ('from importlib import import_module as lm\n\n\ndef _planted():\n    lm("pandas_gbq")\n', "import_outside"),
    "third-party package by pkgutil": ('import pkgutil\n\n\ndef _planted():\n    pkgutil.resolve_name("requests.get")\n', "import_outside"),
    "shap outside its function": ('def _planted():\n    importlib.import_module("shap")\n', "import_outside"),
    "relative import beyond the repository root": ("from ...... import x\n", "import_outside"),
    "repository module that does not exist": ("import scripts.training.models.ghost\n", "closure_changed"),
    "repository module that does not exist, by importlib": (
        'def _planted():\n    importlib.import_module("scripts.training.models.ghost")\n',
        "closure_changed",
    ),
}

# Imports the rule allows: they leave the closure exactly as it is.
CLEAN = {
    "optional standard library import": "try:\n    import zoneinfo\nexcept ImportError:\n    zoneinfo = None\n",
    "standard library by importlib": 'def _planted():\n    importlib.import_module("statistics")\n',
    "standard library by __import__": '__import__("textwrap")\n',
    "standard library by an alias": 'from importlib import import_module as lm\n\n\ndef _planted():\n    lm("fractions")\n',
    "library of the lock group": "import sklearn.tree\nfrom scipy import sparse\n",
    "library of the lock group by importlib": 'def _planted():\n    importlib.import_module("scipy.sparse")\n',
    "library of the lock group by an alias": 'from importlib import import_module as lm\n\n\ndef _planted():\n    lm("statsmodels.api")\n',
    "attribute of a library": "def _planted():\n    return np.linalg.norm\n",
    "attribute through a package to a closure module": "import scripts.training.models as pkg\n\n\ndef _planted():\n    return pkg.common.Feature\n",
    "relative import of a closure module": "from .common import Feature as _Feature\n",
    "the same import repeated in two functions": "def _first():\n    import json\n\n\ndef _second():\n    import json\n",
    "a class imported from a repository module assigned to another name": "from scripts.training.models.common import Feature\n\nOtherFeature = Feature\n",
    "a name that is no import bound in two places": "def _first(sparse):\n    return sparse\n\n\ndef _second():\n    sparse = 1\n    return sparse\n",
    "importlib.metadata": "import importlib.metadata\n\n\ndef _planted():\n    return importlib.metadata.version\n",
}


@functools.cache
def lock_libraries() -> frozenset[str]:
    return model_code.allowed_libraries(ROOT)


def planted_tree(base: Path, text: str, target_module: str = TARGET_MODULE) -> Path:
    """A scratch copy whose trees.py ends with the planted text, beside a probe module that holds one constant."""
    scratch = scratch_tree(base)
    (scratch / TARGET).write_text("VALUE = 1\n")
    path = scratch / TREES
    path.write_text(path.read_text() + "\n\n" + text.replace("@T", target_module))
    return scratch


def attempt_write(scratch: Path) -> tuple[list[str] | None, str]:
    """The files of the record that write would make, or None with the refusal text."""
    try:
        return [item["path"] for item in model_code.build_record(scratch, 1, lock_libraries())["files"]], ""
    except model_code.RecordError as error:
        return None, str(error)


def case_imports_that_reach_a_file_join_the_closure() -> list[str]:
    """Every static and literal dynamic route to another repository file (aliases, relative, nested, optional, attribute) puts that file in the closure."""
    problems = []
    original = [item["path"] for item in read_record()["files"]]
    for name, text in JOINS.items():
        with tempfile.TemporaryDirectory() as folder:
            scratch = planted_tree(Path(folder), text)
            files, refusal = attempt_write(scratch)
            if files is None:
                problems.append(f"{name}: write was refused: {refusal}")
            elif TARGET not in files or sorted(set(files) - {TARGET}) != sorted(original):
                problems.append(f"{name}: the closure would be {files}")
            else:
                (scratch / model_code.RECORD).write_text(json.dumps(model_code.build_record(scratch, 1, lock_libraries()), indent=2, sort_keys=True) + "\n")
                left = model_code.verify(scratch, lock_libraries())
                if left:
                    problems.append(f"{name}: the regenerated record still fails: {left}")
    return problems


def case_dynamic_imports_that_cannot_be_read_are_refused() -> list[str]:
    """What the verifier cannot read or allow is refused outright: the write fails, the record file is not touched and the verifier names the reason."""
    problems = []
    for name, (text, reason) in REFUSED.items():
        with tempfile.TemporaryDirectory() as folder:
            scratch = planted_tree(Path(folder), text)
            before = (scratch / model_code.RECORD).read_bytes()
            files, refusal = attempt_write(scratch)
            if files is not None:
                problems.append(f"{name}: write was not refused and would list {files}")
                continue
            try:
                model_code.write(scratch, 1, lock_libraries())
                problems.append(f"{name}: write did not raise")
            except model_code.RecordError:
                pass
            if (scratch / model_code.RECORD).read_bytes() != before:
                problems.append(f"{name}: a refused write changed the record file")
            found = [text for text in model_code.verify(scratch, lock_libraries()) if text.startswith(reason)]
            if not found:
                problems.append(f"{name}: the verifier gave no {reason} problem")
            elif not any(TREES in text for text in found):
                problems.append(f"{name}: the problem does not name the file: {found}")
    return problems


def case_allowed_imports_leave_the_closure_unchanged() -> list[str]:
    """The standard library (also optional or by literal dynamic import), the lock group libraries and attribute access inside the closure change nothing."""
    problems = []
    original = sorted(item["path"] for item in read_record()["files"])
    for name, text in CLEAN.items():
        with tempfile.TemporaryDirectory() as folder:
            scratch = planted_tree(Path(folder), text)
            files, refusal = attempt_write(scratch)
            if files is None:
                problems.append(f"{name}: write was refused: {refusal}")
            elif sorted(files) != original:
                problems.append(f"{name}: the closure would be {files}")
    return problems


# The four mutations of the round 1 review: (text placed after "import importlib" at the top of trees.py, text placed inside TreeRegressor.predict, outcome).
REVIEWER = {
    "static import": ("from scripts.training.models import mixed as _review_mixed\n", "", "joins"),
    "lazy alias": ("from importlib import import_module as load_module\n", '        load_module("scripts.training.models.mixed")\n', "joins"),
    "lazy variable": ("", '        module = "scripts.training.models.mixed"\n        importlib.import_module(module)\n', "refused"),
    "optional built-in": (
        'try:\n    __import__("scripts.training.models.mixed", fromlist=["ModelError"])\nexcept ModuleNotFoundError:\n    pass\n',
        "",
        "joins",
    ),
    "optional variable": (
        'try:\n    _module = "scripts.training.models.mixed"\n    importlib.import_module(_module)\nexcept ModuleNotFoundError:\n    pass\n',
        "",
        "refused",
    ),
    "round 2: builtins exec by an imported alias": (
        "from builtins import exec as run_text\n",
        '        run_text("import scripts.training.models.mixed")\n',
        "refused",
    ),
    "round 2: builtins eval by attribute": ("import builtins\n", '        builtins.eval("40 + 2")\n', "refused"),
    "round 2: builtins compile by an imported alias": ("from builtins import compile as build_code\n", '        build_code("1", "x", "eval")\n', "refused"),
    "round 2: global alias hidden by an unrelated local import": (
        'from importlib import import_module as fetch_code\n\n\ndef _unrelated():\n    from json import loads as fetch_code\n    return fetch_code("1")\n\n\n',
        '        fetch_code("scripts.training.models.mixed")\n',
        "refused",
    ),
    "round 2: wildcard import of importlib": ("from importlib import *\n", '        import_module("scripts.training.models.mixed")\n', "refused"),
}


def case_reviewer_planted_defects_are_caught() -> list[str]:
    """The mutations of the round 1 review, placed as the reviewer placed them, with the real mixed.py: each puts mixed.py in the closure or is refused."""
    tail = "        return self.native_.predict(design.matrix)"
    problems = []
    for name, (header, inside, outcome) in REVIEWER.items():
        with tempfile.TemporaryDirectory() as folder:
            scratch = scratch_tree(Path(folder))
            shutil.copyfile(ROOT / "scripts/training/models/mixed.py", scratch / "scripts/training/models/mixed.py")
            path = scratch / TREES
            text = path.read_text()
            if tail not in text or "import importlib\n" not in text:
                problems.append(f"{name}: trees.py no longer has the place the mutation needs")
                continue
            path.write_text(text.replace(tail, inside + tail).replace("import importlib\n", "import importlib\n" + header, 1))
            files, refusal = attempt_write(scratch)
            if outcome == "joins" and (files is None or "scripts/training/models/mixed.py" not in files):
                problems.append(f"{name}: expected mixed.py in the closure, got {files} {refusal}")
            if outcome == "refused" and files is not None:
                problems.append(f"{name}: expected a refusal, write would list {files}")
    return problems


def case_third_party_names_come_from_the_lock() -> list[str]:
    """The allowed third-party names are the direct packages of the model_api group of uv.lock (read again here from the lock), with their import names."""
    lock = tomllib.loads((ROOT / "uv.lock").read_text())
    owners = [package for package in lock["package"] if "model-api" in package.get("dev-dependencies", {})]
    if len(owners) != 1:
        return [f"{len(owners)} packages of the lock own a model-api group"]
    named = {entry["name"].lower().replace("-", "_") for entry in owners[0]["dev-dependencies"]["model-api"]}
    libraries = lock_libraries()
    problems = [
        f"{name} is not allowed" for name in ("numpy", "sklearn", "scipy", "statsmodels", "xgboost", "skops", "mlflow", "pandas") if name not in libraries
    ]
    problems += [f"{name} is allowed but is not in the model_api group" for name in ("shap", "yaml", "requests", "torch", "pytest") if name in libraries]
    problems += [
        f"{name} of the lock group has no import name in the set" for name in sorted(named) if name not in libraries and name not in model_code.IMPORT_NAMES
    ]
    return problems


def case_shap_exception_is_limited_to_its_function() -> list[str]:
    """trees.py imports shap, which the model_api group lacks, only inside shap_values. The exception covers that function only, and a stale one is reported."""
    problems = []
    if (TREES, "shap_values") not in model_code.OUTSIDE_IMAGE:
        problems.append("the exception for shap_values is not declared")
    with tempfile.TemporaryDirectory() as folder:
        scratch = scratch_tree(Path(folder))
        path = scratch / TREES
        text = path.read_text()
        path.write_text(
            text.replace('    shap = importlib.import_module("shap")\n', "    shap = None\n").replace(
                '    explainer_error = importlib.import_module("shap.utils._exceptions").ExplainerError\n', "    explainer_error = Exception\n"
            )
        )
        found = model_code.verify(scratch, lock_libraries())
        if not any(item.startswith("exception_unused") for item in found):
            problems.append(f"an exception with no import behind it was not reported: {found}")
    with tempfile.TemporaryDirectory() as folder:
        scratch = scratch_tree(Path(folder))
        path = scratch / TREES
        path.write_text(path.read_text().replace("def shap_values(", "def explain_values(", 1))
        found = model_code.verify(scratch, lock_libraries())
        if not any(item.startswith("import_outside") and "shap" in item for item in found):
            problems.append(f"shap in a function without the exception was not refused: {found}")
    return problems


# 1c. The inputs of the check select the check


def check_inputs() -> list[str]:
    """Every file the record check and these cases read, found two ways: the files the modules name in their strings and the files the constants name."""
    from scripts.quality.source_reads import source_inputs

    tracked = {line for line in run_command("git", ["ls-files"], cwd=ROOT, timeout=60, check=True).stdout.splitlines() if line}
    found = set()
    for module in ("scripts/model_code.py", "scripts/quality/model_code_cases.py", "scripts/quality/model_code_behavior.py"):
        found |= set(source_inputs((ROOT / module).read_text(), tracked, rooted=True))
        found.add(module)
    record = read_record()
    found |= {item["path"] for item in record["files"]} | set(record["image_provided"])
    found |= {model_code.RECORD, model_code.SCHEMA, CONTRACT, DOCKERFILE, "uv.lock", "scripts/process.py"}
    return sorted(path for path in found if path in tracked or (ROOT / path).exists())


GROUP = "repository_code_checks"


def case_inputs_of_the_check_select_the_closure_group() -> list[str]:
    """A change to any file the closure check reads (record, schema, contract, Dockerfile, lock, closure) runs its CI group."""
    config = ci_groups.load()
    problems = []
    for path in check_inputs():
        selected = ci_groups.plan("paths", "core", paths=[path], config=config)["run"]
        if GROUP not in selected:
            problems.append(f"{path} does not run {GROUP}")
    return problems


def case_each_selection_pattern_is_needed() -> list[str]:
    """Without the patterns that match it, a path stops selecting the group: it is no accident of another pattern or of the fall-through."""
    config = ci_groups.load()
    problems = []
    global_patterns = config["global"]
    for path in check_inputs():
        if any(re.search(pattern, path) for pattern in global_patterns):
            continue
        mutant = copy.deepcopy(config)
        mutant["groups"][GROUP] = [pattern for pattern in config["groups"][GROUP] if not re.search(pattern, path)]
        if mutant["groups"][GROUP] == config["groups"][GROUP]:
            problems.append(f"{path} matches no pattern of {GROUP}")
            continue
        run = ci_groups.plan("paths", "core", paths=[path], config=mutant)["run"]
        if GROUP in run:
            problems.append(f"{path} still runs {GROUP} without its pattern (another path, a global pattern or an unclassified fall-through selects it)")
    return problems


def case_launcher_inventory_lists_every_file_of_the_folders_it_reads() -> list[str]:
    """The launcher input list names every tracked file under a folder it lists (data_contracts/training, which holds the record, is read whole)."""
    committed = json.loads((ROOT / "config/quality/launcher_inputs.json").read_text())
    listed = set(committed["files"])
    tracked = [line for line in run_command("git", ["ls-files"], cwd=ROOT, timeout=60, check=True).stdout.splitlines() if line]
    folders = tuple(f"{folder}/" for folder in committed["folders"])
    return [f"{path} is under a folder the launcher suite reads and is not in the list" for path in tracked if path.startswith(folders) and path not in listed]


# 1d. What the image checks at start: the record against the actual files, with no lock and no import analysis


def image_tree(base: Path) -> Path:
    """The files of an image root: the record and the recorded files only (no uv.lock, no contract)."""
    record = read_record()
    for name in [model_code.RECORD, *[item["path"] for item in record["files"]]]:
        target = base / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target)
    return base


def case_image_check_passes_on_the_matching_files_without_a_lock() -> list[str]:
    """The image's start check reads the record, hashes the actual files and compares: it passes on the real files and returns the recorded digest."""
    with tempfile.TemporaryDirectory() as folder:
        root = image_tree(Path(folder))
        problems = model_code.check_files(root)
        digest = model_code.closure_digest(root)
    expected = read_record()["closure_sha256"]
    return [*problems, *([] if digest == expected else [f"closure_digest is {digest}, the record says {expected}"])]


def case_image_check_fails_by_name() -> list[str]:
    """A changed byte, a missing file and an extra record path each fail with the file named; a wrong recorded digest fails as a digest mismatch."""
    problems = []
    with tempfile.TemporaryDirectory() as folder:
        root = image_tree(Path(folder))
        target = root / "scripts/training/models/linear.py"
        target.write_bytes(target.read_bytes() + b"\n")
        found = model_code.check_files(root)
        if not any(text.startswith("file_changed") and "scripts/training/models/linear.py" in text for text in found):
            problems.append(f"a changed byte raised {found}")
        if not any(text.startswith("digest_mismatch") for text in found):
            problems.append(f"a changed byte gave no digest_mismatch: {found}")
    with tempfile.TemporaryDirectory() as folder:
        root = image_tree(Path(folder))
        (root / "scripts/training/models/trees.py").unlink()
        found = model_code.check_files(root)
        if not any(text.startswith("file_missing") and "scripts/training/models/trees.py" in text for text in found):
            problems.append(f"a missing file raised {found}")
    with tempfile.TemporaryDirectory() as folder:
        root = image_tree(Path(folder))
        record = read_record()
        record["files"] = sorted([*record["files"], {"path": "scripts/training/models/extra.py", "size": 0, "sha256": "0" * 64}], key=lambda item: item["path"])
        record["closure_sha256"] = model_code.digest(record["files"])
        write_record(root, record)
        found = model_code.check_files(root)
        if not any(text.startswith("file_missing") and "scripts/training/models/extra.py" in text for text in found):
            problems.append(f"an extra record path raised {found}")
    with tempfile.TemporaryDirectory() as folder:
        root = image_tree(Path(folder))
        record = read_record()
        record["closure_sha256"] = "f" * 64
        write_record(root, record)
        if not any(text.startswith("digest_mismatch") for text in model_code.check_files(root)):
            problems.append("a wrong recorded digest was not reported as digest_mismatch")
    with tempfile.TemporaryDirectory() as folder:
        found = model_code.check_files(Path(folder))
        if not any(text.startswith("record_invalid") for text in found):
            problems.append(f"an absent record raised {found}")
    return problems


def case_closure_digest_of_the_repository_root_is_the_record_value() -> list[str]:
    """closure_digest of the repository root equals closure_sha256 of the record (the value a bundle will store and the image will compare)."""
    found, expected = model_code.closure_digest(ROOT), read_record()["closure_sha256"]
    return [] if found == expected else [f"closure_digest is {found}, the record says {expected}"]


def case_closure_digest_changes_with_one_byte_of_any_closure_file() -> list[str]:
    """One added byte, and one byte replaced at the same length, in any one recorded file of a scratch copy gives another digest than the record's."""
    problems = []
    expected = read_record()["closure_sha256"]
    for item in read_record()["files"]:
        original = (ROOT / item["path"]).read_bytes()
        changes = {"one added byte": original + b" "}
        if original:
            changes["one replaced byte at the same length"] = bytes([original[0] ^ 1]) + original[1:]
        for name, content in changes.items():
            with tempfile.TemporaryDirectory() as folder:
                root = image_tree(Path(folder))
                (root / item["path"]).write_bytes(content)
                if model_code.closure_digest(root) == expected:
                    problems.append(f"{name} in {item['path']} left closure_digest unchanged")
    return problems


def case_closure_digest_refuses_a_missing_file_by_name() -> list[str]:
    """A recorded file that is absent raises RecordError naming it, for each recorded file: never a digest of the files that remain."""
    problems = []
    for item in read_record()["files"]:
        with tempfile.TemporaryDirectory() as folder:
            root = image_tree(Path(folder))
            (root / item["path"]).unlink()
            try:
                found = model_code.closure_digest(root)
            except model_code.RecordError as error:
                if "file_missing" not in str(error) or item["path"] not in str(error):
                    problems.append(f"{item['path']} missing raised RecordError without the reason and the name: {error}")
            else:
                problems.append(f"{item['path']} missing gave the digest {found} instead of an error")
    return problems


def case_image_check_is_independent_of_the_import_rule() -> list[str]:
    """The start check reads neither uv.lock nor the contract: it passes in a root that has neither."""
    with tempfile.TemporaryDirectory() as folder:
        root = image_tree(Path(folder))
        if (root / "uv.lock").exists() or (root / CONTRACT).exists():
            return ["the image root was meant to have no lock and no contract"]
        return model_code.check_files(root)


# 1e. Behaviour: the closure alone fits, saves, loads and predicts every family


class Skipped(Exception):
    """A case that cannot run here: it is reported as skipped, never as passed silently, and fails under --require-libraries."""


SETTINGS = {"require_libraries": False}


def behaviour_outcome(edit: Callable[[Path], None] | None = None) -> model_code_behavior.Outcome:
    outcome = model_code_behavior.run(ROOT, edit)
    if outcome.skipped:
        if SETTINGS["require_libraries"]:
            raise AssertionError(f"needs the model_api libraries: {outcome.skipped}")
        raise Skipped(outcome.skipped)
    return outcome


def case_clean_interpreter_fits_saves_loads_and_predicts_every_family() -> list[str]:
    """With only the declared files and the stand-ins, every family constructs, fits a tiny frame, saves with skops, loads (trusted list) and predicts."""
    outcome = behaviour_outcome()
    expected = {"m1_ols": "fit, predict, save, load, predict_loaded", "m2_elastic_net": "fit, predict, save, load, predict_loaded"}
    expected |= {"m4_boosting": "fit, predict, save, load, predict_loaded", "m4_forest": "fit, predict"}
    steps = {name: ", ".join(done) for name, done in outcome.families.items()}
    return [*outcome.problems, *([] if steps == expected else [f"steps completed: {steps}"])]


def remove_preprocessing(scratch: Path) -> None:
    (scratch / "scripts/training/models/preprocessing.py").unlink()


def plant_lazy_import(scratch: Path) -> None:
    path = scratch / TREES
    path.write_text(
        path.read_text().replace(
            "        return self.native_.predict(design.matrix)",
            '        importlib.import_module("scripts.training.models.mixed")\n        return self.native_.predict(design.matrix)',
        )
    )


def case_behaviour_check_fails_when_a_file_is_missing_or_a_lazy_import_needs_one() -> list[str]:
    """The check is not vacuous: without a declared file, or with a prediction-time import of a file outside the closure, it fails and names the file."""
    problems = []
    for name, edit, module in (
        ("a declared file is missing", remove_preprocessing, "preprocessing"),
        ("a prediction-time import needs mixed.py", plant_lazy_import, "mixed"),
    ):
        found = behaviour_outcome(edit).problems
        if not any(module in text for text in found):
            problems.append(f"{name}: no problem names {module}: {found}")
    return problems


def plant_prediction_failure(scratch: Path) -> None:
    path = scratch / "scripts/training/models/linear.py"
    path.write_text(path.read_text().replace("        return with_intercept(design.matrix) @ self.coefficients_", '        raise ValueError("planted")'))


def case_behaviour_failure_names_the_family_and_the_step() -> list[str]:
    """One broken family fails by name and step (m1_ols, predict) while the others still run, and the report never says "0 families failed" for a failure."""
    outcome = behaviour_outcome(plant_prediction_failure)
    named = [text for text in outcome.problems if text.startswith("m1_ols") and "predict" in text and "planted" in text]
    problems = [] if named else [f"no problem names m1_ols and its predict step: {outcome.problems}"]
    others = {"m2_elastic_net", "m4_boosting", "m4_forest"}
    return [*problems, *([] if others <= set(outcome.families) else [f"the other families did not run: {sorted(outcome.families)}"])]


def plant_nan_coefficients_in_the_saved_file(scratch: Path) -> None:
    path = scratch / "scripts/training/models/linear.py"
    line = "        return with_intercept(design.matrix) @ self.coefficients_"
    state = "\n\n    def __getstate__(self):\n        state = dict(self.__dict__)\n"
    state += '        state["coefficients_"] = np.full_like(self.coefficients_, np.nan)\n        return state'
    path.write_text(path.read_text().replace(line, line + state))


def case_behaviour_check_fails_when_the_loaded_predictions_are_not_finite() -> list[str]:
    """A model saved with NaN coefficients loads without error and predicts NaN: the check fails by family and step instead of reporting every family passed."""
    outcome = behaviour_outcome(plant_nan_coefficients_in_the_saved_file)
    named = [text for text in outcome.problems if text.startswith("m1_ols: predict_loaded failed") and "not finite" in text]
    problems = [] if named else [f"no problem names m1_ols, predict_loaded and the non-finite predictions: {outcome.problems}"]
    others = {"m2_elastic_net", "m4_boosting", "m4_forest"}
    return [*problems, *([] if others <= set(outcome.families) else [f"the other families did not run: {sorted(outcome.families)}"])]


# 2. Import origins in a clean interpreter


def case_clean_interpreter_imports_every_trusted_type_from_declared_files() -> list[str]:
    """With only the declared files and a stand-in for the image-provided module, every trusted type imports from the scratch."""
    record = read_record()
    with tempfile.TemporaryDirectory() as folder:
        scratch = Path(folder).resolve()
        for item in record["files"]:
            target = scratch / item["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / item["path"], target)
        for name, text in STAND_IN.items():
            (scratch / name).parent.mkdir(parents=True, exist_ok=True)
            (scratch / name).write_text(text)
        names = repository_types(ROOT)
        code, report, error = probe(scratch, names)
        if code != 0:
            return [f"the clean interpreter failed: {error.strip().splitlines()[-1:]}"]
        problems = []
        declared = {item["path"] for item in record["files"]} | set(STAND_IN)
        for module, origin in report["origins"].items():
            places = [origin] if isinstance(origin, str) else origin
            for place in places:
                path = Path(place).resolve()
                if scratch not in path.parents and path != scratch:
                    problems.append(f"{module} came from {path}, outside the scratch")
                elif path.is_file() and str(path.relative_to(scratch)) not in declared:
                    problems.append(f"{module} came from the undeclared file {path.relative_to(scratch)}")
        for name in names:
            if report["types"].get(name) != name.rpartition(".")[0]:
                problems.append(f"{name} reports the module {report['types'].get(name)}")
        files = {str(Path(place).resolve().relative_to(scratch)) for origin in report["origins"].values() if isinstance(origin, str) for place in [origin]}
        problems += [f"declared file {name} was never loaded" for name in sorted(declared - files)]
        return problems


def case_stand_in_matches_what_the_closure_takes() -> list[str]:
    """The stand-in defines exactly the names the closure imports from the image-provided module, so a new name fails here instead of in the image."""
    taken: set[str] = set()
    for item in read_record()["files"]:
        taken |= model_code.names_imported_from(ROOT, item["path"], "orchestration.training_contract")
    return [] if taken == STAND_IN_NAMES else [f"closure takes {sorted(taken)}, the stand-in defines {sorted(STAND_IN_NAMES)}"]


def case_clean_interpreter_fails_when_a_declared_file_is_missing() -> list[str]:
    """The probe is not vacuous: without one declared file the import fails."""
    record = read_record()
    with tempfile.TemporaryDirectory() as folder:
        scratch = Path(folder).resolve()
        for item in record["files"]:
            if item["path"] == "scripts/training/models/preprocessing.py":
                continue
            target = scratch / item["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / item["path"], target)
        for name, text in STAND_IN.items():
            (scratch / name).parent.mkdir(parents=True, exist_ok=True)
            (scratch / name).write_text(text)
        code, _, _ = probe(scratch, repository_types(ROOT))
    return [] if code != 0 else ["the probe passed without preprocessing.py"]


# 3. The changed file


def case_changed_file_without_the_record_fails() -> list[str]:
    """Editing one closure file without regenerating the record fails the verifier, and regenerating the record makes it pass again."""
    problems = []
    with tempfile.TemporaryDirectory() as folder:
        scratch = scratch_tree(Path(folder))
        if model_code.verify(scratch):
            return [f"the copy of the repository fails before any edit: {model_code.verify(scratch)}"]
        target = scratch / "scripts/training/models/elastic.py"
        target.write_text(target.read_text() + "\n# edited\n")
        found = model_code.verify(scratch)
        if not any(text.startswith("file_changed") and "scripts/training/models/elastic.py" in text for text in found):
            problems.append(f"an edited file raised {found}")
        model_code.write(scratch, generation=read_record()["generation"])
        if model_code.verify(scratch):
            problems.append(f"the regenerated record still fails: {model_code.verify(scratch)}")
    return problems


def case_changed_digest_alone_fails() -> list[str]:
    """A record whose per-file entries are right but whose closure digest is not (or the reverse) fails with its own reason."""
    problems = []
    with tempfile.TemporaryDirectory() as folder:
        scratch = scratch_tree(Path(folder))
        record = read_record(scratch)
        record["closure_sha256"] = "f" * 64
        write_record(scratch, record)
        found = model_code.verify(scratch)
        if not any(text.startswith("digest_mismatch") for text in found):
            problems.append(f"a wrong closure digest raised {found}")
        record = read_record()
        record["files"][1]["sha256"] = "f" * 64
        write_record(scratch, record)
        found = model_code.verify(scratch)
        if not any(text.startswith("file_changed") for text in found):
            problems.append(f"a wrong file hash raised {found}")
    return problems


def case_missing_and_extra_files_fail() -> list[str]:
    """A deleted closure file and a record that lists a file the closure does not need both fail."""
    problems = []
    with tempfile.TemporaryDirectory() as folder:
        scratch = scratch_tree(Path(folder))
        (scratch / "scripts/training/models/linear.py").unlink()
        if not any(text.startswith("file_missing") for text in model_code.verify(scratch)):
            problems.append("a deleted closure file was not reported as file_missing")
    with tempfile.TemporaryDirectory() as folder:
        scratch = scratch_tree(Path(folder))
        extra = scratch / "scripts/training/models/mixed.py"
        extra.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / "scripts/training/models/mixed.py", extra)
        record = read_record()
        record["files"] = sorted([*record["files"], model_code.entry(scratch, "scripts/training/models/mixed.py")], key=lambda item: item["path"])
        record["closure_sha256"] = model_code.digest(record["files"])
        write_record(scratch, record)
        if not any(text.startswith("closure_changed") for text in model_code.verify(scratch)):
            problems.append("a record that lists a file outside the import closure was not reported as closure_changed")
    return problems


# 4. The closed schema


def schema_validator() -> Any:
    """The validator of the schema file. jsonschema is imported here, so the behaviour cases run in an image that lacks it."""
    return importlib.import_module("jsonschema").Draft202012Validator(json.loads((ROOT / model_code.SCHEMA).read_text()))


def mutated(change: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
    record = good_record()
    change(record)
    return record


BAD_RECORDS: dict[str, Callable[[dict[str, Any]], None]] = {
    "unknown top-level key": lambda r: r.update(extra=1),
    "unknown file key": lambda r: r["files"][0].update(mode="0644"),
    "missing key": lambda r: r.pop("generation"),
    "digest not hex": lambda r: r.update(closure_sha256="z" * 64),
    "digest too short": lambda r: r.update(closure_sha256="0" * 63),
    "digest in capitals": lambda r: r.update(closure_sha256="A" * 64),
    "file digest not hex": lambda r: r["files"][0].update(sha256="g" * 64),
    "negative size": lambda r: r["files"][0].update(size=-1),
    "size is a string": lambda r: r["files"][0].update(size="0"),
    "generation zero": lambda r: r.update(generation=0),
    "generation is a boolean": lambda r: r.update(generation=True),
    "generation is a string": lambda r: r.update(generation="1"),
    "wrong schema id": lambda r: r.update(schema="model_code/2"),
    "absolute path": lambda r: r["files"][0].update(path="/etc/passwd"),
    "parent path": lambda r: r["files"][0].update(path="../scripts/x.py"),
    "backslash path": lambda r: r["files"][0].update(path="scripts\\x.py"),
    "no files": lambda r: r.update(files=[]),
}


def case_schema_and_reader_refuse_the_same_records() -> list[str]:
    """The schema file and the stdlib reader accept the good record and refuse each planted bad one, so the two cannot drift apart."""
    problems = []
    validator = schema_validator()
    with tempfile.TemporaryDirectory() as folder:
        target = Path(folder) / "record.json"
        samples = {"good record": good_record(), **{name: mutated(change) for name, change in BAD_RECORDS.items()}}
        for name, record in samples.items():
            target.write_text(json.dumps(record))
            by_schema = not list(validator.iter_errors(record))
            try:
                model_code.read_record(target)
                by_reader = True
            except model_code.RecordError:
                by_reader = False
            if by_schema != (name == "good record") or by_reader != (name == "good record"):
                problems.append(f"{name}: schema accepts {by_schema}, reader accepts {by_reader}")
    return problems


def case_reader_refuses_duplicates_and_unsorted_paths() -> list[str]:
    """Two rules that a JSON schema cannot state: the paths are unique and sorted, because the digest depends on the order."""
    problems = []
    first = good_record()["files"][0]
    second = {**first, "path": "scripts/training/models/__init__.py"}
    with tempfile.TemporaryDirectory() as folder:
        target = Path(folder) / "record.json"
        for name, files in (("duplicate path", [first, first]), ("unsorted paths", [second, first])):
            target.write_text(json.dumps({**good_record(), "files": files}))
            try:
                model_code.read_record(target)
                problems.append(f"{name} was accepted")
            except model_code.RecordError:
                pass
    return problems


def case_reader_refuses_text_that_is_not_one_json_object() -> list[str]:
    """Not JSON, a list, and duplicate keys are refused with RecordError and never with another exception."""
    problems = []
    with tempfile.TemporaryDirectory() as folder:
        target = Path(folder) / "record.json"
        for name, text in (("not JSON", "{"), ("a list", "[]"), ("a duplicate key", '{"schema": "a", "schema": "b"}'), ("empty", "")):
            target.write_text(text)
            try:
                model_code.read_record(target)
                problems.append(f"{name} was accepted")
            except model_code.RecordError:
                pass
        try:
            model_code.read_record(Path(folder) / "absent.json")
            problems.append("an absent record was accepted")
        except model_code.RecordError:
            pass
    return problems


# 5. The digest, and what must not change it


def independent_digest(root: Path, files: list[str]) -> str:
    """The digest as the design defines it, written again here from the definition: sorted ``path NUL size NUL sha256`` lines, each ended by a newline."""
    lines = []
    for name in sorted(files, key=lambda item: item.encode()):
        data = (root / name).read_bytes()
        lines.append(f"{name}\0{len(data)}\0{hashlib.sha256(data).hexdigest()}\n")
    return hashlib.sha256("".join(lines).encode()).hexdigest()


def case_record_digest_equals_recomputed_digest() -> list[str]:
    """The record's digest and every entry equal a fresh computation from the files at the head, and the record verifies clean."""
    problems = list(model_code.verify(ROOT))
    record = read_record()
    names = [item["path"] for item in record["files"]]
    if record["closure_sha256"] != independent_digest(ROOT, names):
        problems.append("closure_sha256 differs from the digest recomputed from the definition")
    if len(names) != 7:
        problems.append(f"the closure holds {len(names)} files; the design expects 7 at this head")
    for item in record["files"]:
        data = (ROOT / item["path"]).read_bytes()
        if item["size"] != len(data):
            problems.append(f"{item['path']} is {len(data)} bytes, the record says {item['size']}")
    return problems


def case_mode_mtime_and_bytecode_do_not_change_the_digest() -> list[str]:
    """Another file mode, a new modification time, a __pycache__ folder with bytecode and a stray file leave the digest and the verdict unchanged."""
    problems = []
    with tempfile.TemporaryDirectory() as folder:
        scratch = scratch_tree(Path(folder))
        before = model_code.digest(model_code.entries(scratch, [item["path"] for item in read_record(scratch)["files"]]))
        for item in read_record(scratch)["files"]:
            target = scratch / item["path"]
            target.chmod(0o755)
            target.touch()
        cache = scratch / "scripts/training/models/__pycache__"
        cache.mkdir()
        (cache / "common.cpython-312.pyc").write_bytes(b"\x00bytecode")
        (scratch / "scripts/training/models/notes.txt").write_text("not part of the closure")
        after = model_code.digest(model_code.entries(scratch, [item["path"] for item in read_record(scratch)["files"]]))
        if before != after:
            problems.append("mode, time or bytecode changed the digest")
        if model_code.verify(scratch):
            problems.append(f"the verifier failed after a mode, time or bytecode change: {model_code.verify(scratch)}")
    return problems


def case_digest_depends_on_content_size_and_path() -> list[str]:
    """The digest is not blind: one changed byte, a renamed path and a moved byte between files each change it."""
    problems = []
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        (root / "a.py").write_bytes(b"ab")
        (root / "b.py").write_bytes(b"")
        base = independent_digest(root, ["a.py", "b.py"])
        if model_code.digest(model_code.entries(root, ["a.py", "b.py"])) != base:
            problems.append("the module digest differs from the definition")
        (root / "a.py").write_bytes(b"ac")
        if model_code.digest(model_code.entries(root, ["a.py", "b.py"])) == base:
            problems.append("a changed byte left the digest unchanged")
        (root / "a.py").write_bytes(b"a")
        (root / "b.py").write_bytes(b"b")
        if model_code.digest(model_code.entries(root, ["a.py", "b.py"])) == base:
            problems.append("a byte moved between files left the digest unchanged")
        (root / "a.py").write_bytes(b"ab")
        (root / "b.py").write_bytes(b"")
        (root / "c.py").write_bytes(b"ab")
        if model_code.digest(model_code.entries(root, ["c.py", "b.py"])) == base:
            problems.append("a renamed path left the digest unchanged")
        if model_code.digest(model_code.entries(root, ["b.py", "a.py"])) != base:
            problems.append("the digest depends on the order the files were listed in")
    return problems


def case_generation_is_one_for_this_release() -> list[str]:
    """This release has one generation, a whole number, and the record carries it."""
    generation = read_record()["generation"]
    return [] if generation == 1 else [f"generation is {generation}"]


def case_recorded_numbers_are_the_integrated_head_numbers() -> list[str]:
    """The record is not the pre-m6 record: preprocessing.py carries the m6 selection functions, and the record's hash of it is the hash of today's bytes."""
    text = (ROOT / "scripts/training/models/preprocessing.py").read_text()
    if "def missing_selection" not in text:
        return ["the integrated m6 selection functions are absent from preprocessing.py; this record was built on the wrong base"]
    return []


ORDER = [
    case_closure_equals_import_closure,
    case_closure_roots_come_from_the_contract,
    case_image_provided_files_are_in_the_dockerfile,
    case_dockerfile_copies_exactly_the_files_of_the_record,
    case_image_provided_module_needs_only_the_standard_library,
    case_shared_data_rules_name_the_rule_each_matrix_document_breaks,
    case_shared_data_rules_import_only_the_standard_library,
    case_one_home_for_the_bounded_fetch_and_the_data_only_read,
    case_planted_import_changes_the_closure,
    case_imports_that_reach_a_file_join_the_closure,
    case_dynamic_imports_that_cannot_be_read_are_refused,
    case_allowed_imports_leave_the_closure_unchanged,
    case_reviewer_planted_defects_are_caught,
    case_third_party_names_come_from_the_lock,
    case_shap_exception_is_limited_to_its_function,
    case_inputs_of_the_check_select_the_closure_group,
    case_each_selection_pattern_is_needed,
    case_launcher_inventory_lists_every_file_of_the_folders_it_reads,
    case_image_check_passes_on_the_matching_files_without_a_lock,
    case_image_check_fails_by_name,
    case_closure_digest_of_the_repository_root_is_the_record_value,
    case_closure_digest_changes_with_one_byte_of_any_closure_file,
    case_closure_digest_refuses_a_missing_file_by_name,
    case_image_check_is_independent_of_the_import_rule,
    case_clean_interpreter_fits_saves_loads_and_predicts_every_family,
    case_behaviour_check_fails_when_a_file_is_missing_or_a_lazy_import_needs_one,
    case_behaviour_failure_names_the_family_and_the_step,
    case_behaviour_check_fails_when_the_loaded_predictions_are_not_finite,
    case_clean_interpreter_imports_every_trusted_type_from_declared_files,
    case_stand_in_matches_what_the_closure_takes,
    case_clean_interpreter_fails_when_a_declared_file_is_missing,
    case_changed_file_without_the_record_fails,
    case_changed_digest_alone_fails,
    case_missing_and_extra_files_fail,
    case_schema_and_reader_refuse_the_same_records,
    case_reader_refuses_duplicates_and_unsorted_paths,
    case_reader_refuses_text_that_is_not_one_json_object,
    case_record_digest_equals_recomputed_digest,
    case_mode_mtime_and_bytecode_do_not_change_the_digest,
    case_digest_depends_on_content_size_and_path,
    case_generation_is_one_for_this_release,
    case_recorded_numbers_are_the_integrated_head_numbers,
]
CASES = {function.__name__.removeprefix("case_"): function for function in ORDER}


def run(selected: list[str] | None = None) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for name, function in CASES.items():
        if selected and name not in selected:
            continue
        started = time.monotonic()
        skipped = ""
        try:
            problems = function()
        except Skipped as skip:
            problems, skipped = [], str(skip)
        except Exception as error:  # a case that raises fails by name and does not stop the rest
            problems = [f"raised {type(error).__name__}: {error}"]
        results[name] = {"passed": not problems, "skipped": skipped, "problems": problems, "seconds": round(time.monotonic() - started, 2)}
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the model code closure cases.")
    parser.add_argument("--output", type=Path, help="write the results as JSON to this file")
    parser.add_argument("--case", action="append", help="run only this case (repeatable)")
    parser.add_argument("--require-libraries", action="store_true", help="fail, instead of skip, a case that needs the model_api libraries")
    arguments = parser.parse_args()
    SETTINGS["require_libraries"] = arguments.require_libraries
    results = run(arguments.case)
    for name, result in results.items():
        label = "SKIP" if result["skipped"] else "PASS" if result["passed"] else "FAIL"
        sys.stdout.write(f"{label} {name} ({result['seconds']} s)\n")
        if result["skipped"]:
            sys.stdout.write(f"    skipped: {result['skipped']}\n")
        for problem in result["problems"]:
            sys.stdout.write(f"    {problem}\n")
    failed = [name for name, result in results.items() if not result["passed"]]
    skipped = [name for name, result in results.items() if result["skipped"]]
    sys.stdout.write(
        f"model code cases: {len(results) - len(failed)} of {len(results)} passed"
        + (f", {len(skipped)} of them skipped: {', '.join(skipped)}\n" if skipped else "\n")
    )
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(json.dumps({"cases": results, "failed": failed}, indent=2) + "\n")
    return 1 if failed or not results else 0


if __name__ == "__main__":
    sys.exit(main())
