"""Cases of the spill directory and byte cap on every DuckDB connection of a layer phase, with a real spill and no Docker.

    .tools/sqlfluff/bin/python -m scripts.lakehouse.run_duckdb_bounds_cases

``HAI_DUCKDB_TEMP_DIRECTORY`` and ``HAI_DUCKDB_MAX_TEMP_BYTES`` bound the disk a layer task spends on DuckDB. DuckDB applies them differently:
the directory only when it is given at connect, the cap only when it is set after the connect. A connection that gets neither spills beside its database
file without a cap.
The cases cover four things. The helper ``duckdb_bounds.connect`` (a real spill lands in the configured directory, the cap stops it, a read-only connection and
a connection with external access locked are bounded). The dbt profiles that the feature build writes (a real spill through the pinned adapter, then a second
cursor). A source scan: no layer phase opens a connection except through the helper, and every other module that opens one is listed here with its reason, so
a new connection cannot slip in unbounded. And the message of a phase that reached the cap."""

from __future__ import annotations

import ast
import os
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.lakehouse import duckdb_bounds
from scripts.lakehouse.ecs_engine import TemporaryStorageError
from scripts.lakehouse.run_duckdb_spill_cases import SPILL_SQL, run_output, spill_bytes

ROOT = Path(__file__).resolve().parents[2]
MIB = 2**20
# The modules whose DuckDB connections run in a layer phase. Each opens its connections through duckdb_bounds.connect.
PHASES = (
    "scripts/lakehouse/pipeline_build.py",
    "scripts/lakehouse/silver_quality.py",
    "scripts/lakehouse/gold_reconcile.py",
    "scripts/lakehouse/gold_export.py",
    "scripts/lakehouse/feature_columns.py",
    "scripts/features/features_build.py",
    "scripts/features/feature_validation.py",
)
HELPER = "scripts/lakehouse/duckdb_bounds.py"
# The phase modules that name a DuckDB URL for a library that opens the connection itself.
URLS = {
    "scripts/lakehouse/silver_quality.py": "Great Expectations opens this connection itself: only the spill directory can be given, not the cap",
}
# The other modules that open a DuckDB connection, and why each is not bounded by the helper.
LEFT = {
    "scripts/lakehouse/provenance.py": "the container reader opens the sealed copy read only and runs a plain select of the marker table",
    "scripts/lakehouse/bronze_scan.py": "decodes one Parquet row group of at most 32 MiB in memory on one thread, and the bronze job sets no spill variables",
    "monitoring/current_window.py": "converts the JSON lines of rows it holds to Parquet with duckdb.read_json on the default connection",
    "monitoring/drift_check.py": "reads Parquet files with duckdb.read_parquet on the default in-memory connection",
    "scripts/features/feature_fixture.py": "builds the fixture databases of the feature checks in CI, never in a task",
}


def skipped(relative: str) -> bool:
    """Case runners, tests and hidden folders open connections of their own for their fixtures."""
    name = relative.rsplit("/", 1)[-1]
    return name.startswith(("run_", "test_")) or name.endswith(("_cases.py", "_e2e.py")) or "/tests/" in relative or "/." in relative


TASK = {"HAI_DUCKDB_TEMP_DIRECTORY": "", "HAI_DUCKDB_MAX_TEMP_BYTES": "", "DUCKDB_MEMORY_LIMIT": "150MB", "JOB_THREADS": "2"}


def task_environment(folder: Path, cap: int) -> dict[str, str]:
    return {**TASK, "HAI_DUCKDB_TEMP_DIRECTORY": str(folder / "spill"), "HAI_DUCKDB_MAX_TEMP_BYTES": str(cap)}


def spill_with_peak(connection: Any, spill: Path, sql: str) -> tuple[str, int]:
    """Run a query that spills and return its error text and the most disk its spill files held while it ran (they are gone when it ends)."""
    import duckdb

    peak = [0]
    stop = threading.Event()

    def watch() -> None:
        while not stop.is_set():
            peak[0] = max(peak[0], spill_bytes(spill))
            time.sleep(0.02)

    watcher = threading.Thread(target=watch)
    watcher.start()
    error = ""
    try:
        connection.execute(sql)
    except duckdb.Error as caught:
        error = str(caught)
    stop.set()
    watcher.join()
    return error, peak[0]


def helper_spill(folder: Path, cap: int, read_only: bool = False, config: dict[str, Any] | None = None) -> tuple[str, int, bool]:
    """Spill through ``duckdb_bounds.connect``. Returns the error text, the most spill bytes seen afterwards and whether the database folder is clean."""
    import duckdb

    database = folder / "work.duckdb"
    if read_only:
        duckdb.connect(str(database)).close()
    with mock.patch.dict(os.environ, task_environment(folder, cap)):
        connection = duckdb_bounds.connect(duckdb, database, read_only=read_only, config=config)
        sql = SPILL_SQL.replace("CREATE OR REPLACE TABLE", "CREATE TEMP TABLE") if read_only else SPILL_SQL
        error, peak = spill_with_peak(connection, folder / "spill", sql)
        connection.close()
    return error, peak, not any(path.name.endswith(".tmp") or path.suffix == ".tmp" for path in folder.iterdir())


def helper_cases() -> dict[str, bool]:
    import duckdb

    checks: dict[str, bool] = {}
    with tempfile.TemporaryDirectory(prefix="duckdb_bounds_") as scratch:
        folder = Path(scratch)
        (folder / "spill").mkdir()
        with mock.patch.dict(os.environ, task_environment(folder, 20 * 2**30)):
            connection = duckdb.connect(str(folder / "control.duckdb"), config={**duckdb_bounds.options(), "max_temp_directory_size": "20MiB"})
            error, peak = spill_with_peak(connection, folder / "spill", SPILL_SQL)
            connection.close()
        checks["control: a cap given only at connect is not enforced by DuckDB"] = error == "" and peak > 50 * MIB
    for label, read_only, config in (
        ("a writable connection", False, None),
        ("a read-only connection", True, None),
        ("a connection with external access locked", False, {"enable_external_access": False}),
    ):
        with tempfile.TemporaryDirectory(prefix="duckdb_bounds_") as scratch:
            error, _, _ = helper_spill(Path(scratch), 20 * MIB, read_only, config)
        checks[f"{label}: the byte cap stops a spill that passes it"] = duckdb_bounds.cap_reached(error)
        with tempfile.TemporaryDirectory(prefix="duckdb_bounds_") as scratch:
            error, peak, clean = helper_spill(Path(scratch), 20 * 2**30, read_only, config)
        checks[f"{label}: a spill below the cap lands in the configured directory and not beside the database"] = error == "" and peak > 50 * MIB and clean
    with mock.patch.dict(os.environ, {"AWS_EXECUTION_ENV": "AWS_ECS_FARGATE", "HAI_DUCKDB_TEMP_DIRECTORY": "", "HAI_DUCKDB_MAX_TEMP_BYTES": ""}):
        try:
            duckdb_bounds.options()
            refused = False
        except TemporaryStorageError:
            refused = True
    checks["inside a task a missing spill setting is an error"] = refused
    with mock.patch.dict(os.environ, {"HAI_DUCKDB_TEMP_DIRECTORY": "", "HAI_DUCKDB_MAX_TEMP_BYTES": ""}, clear=False):
        os.environ.pop("AWS_EXECUTION_ENV", None)
        connection = duckdb_bounds.connect(duckdb)
        checks["outside a task nothing is bounded and the connection works"] = (
            connection.execute("SELECT 42").fetchall() == [(42,)] and "temp_directory" not in duckdb_bounds.options()
        )
        connection.close()
    checks["the cap sentence is recognised only when the cap stopped the query"] = not duckdb_bounds.cap_reached(
        "failed to pin block of size 256.0 KiB (190 MiB/190 MiB used)"
    )
    return checks


def profile_cases() -> dict[str, bool]:
    """The dbt profiles that the feature build writes, through the pinned adapter with a real spill."""
    import yaml

    from scripts.features import features_build
    from scripts.lakehouse import feature_columns

    checks: dict[str, bool] = {}
    writers = {"the feature build": features_build.write_profile, "the feature shell build": feature_columns.write_profile}
    for label, writer in writers.items():
        for locked in (False, True):
            name = f"{label}{' with external access locked' if locked else ''}"
            with tempfile.TemporaryDirectory(prefix="duckdb_profile_") as scratch:
                folder = Path(scratch)
                with mock.patch.dict(os.environ, task_environment(folder, 20 * 2**30)):
                    profile = yaml.safe_load((writer(folder / "profile", folder / "staging.duckdb", locked) / "profiles.yml").read_text())
                output = dict(profile["hai"]["outputs"]["shell"])
                output.pop("threads", None)
                (folder / "spill").mkdir(exist_ok=True)
                checks[f"{name}: the profile names the spill directory at connect and the cap as a setting"] = (output.get("config_options") or {}).get(
                    "temp_directory"
                ) == str(folder / "spill") and "max_temp_directory_size" in (output.get("settings") or {})
                checks[f"{name}: a real spill goes to the configured directory and a new cursor works after it"] = (
                    lambda result: result["spill_error"] == "" and result["spill_bytes"] > 50 * MIB and result["second_cursor"] == [(42,)]
                )(run_output(output, folder))
            with tempfile.TemporaryDirectory(prefix="duckdb_profile_") as scratch:
                folder = Path(scratch)
                with mock.patch.dict(os.environ, task_environment(folder, 20 * MIB)):
                    profile = yaml.safe_load((writer(folder / "profile", folder / "staging.duckdb", locked) / "profiles.yml").read_text())
                output = dict(profile["hai"]["outputs"]["shell"])
                output.pop("threads", None)
                (folder / "spill").mkdir(exist_ok=True)
                result = run_output(output, folder)
                checks[f"{name}: the byte cap stops a spill that passes it"] = (
                    duckdb_bounds.cap_reached(result["spill_error"]) and result["spill_bytes"] < 40 * MIB
                )
    return checks


def module_names(tree: ast.AST) -> tuple[set[str], set[str], list[int]]:
    """The names a module binds to the DuckDB module, the names it binds to a function of it, and the lines of star imports.

    ``import duckdb``, ``import duckdb as db``, ``import duckdb.typing`` and a name bound to ``importlib.import_module("duckdb")`` or ``__import__("duckdb")``
    are the first. ``from duckdb import connect`` and ``from duckdb import connect as open_database`` are the second."""
    modules: set[str] = set()
    functions: set[str] = set()
    stars: list[int] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules |= {(item.asname or "duckdb") for item in node.names if item.name.split(".")[0] == "duckdb"}
        elif isinstance(node, ast.ImportFrom) and node.module is not None and node.module.split(".")[0] == "duckdb" and node.level == 0:
            for item in node.names:
                if item.name == "*":
                    stars.append(node.lineno)
                else:
                    functions.add(item.asname or item.name)
        elif isinstance(node, ast.Assign) and imports_duckdb(node.value):
            modules |= {target.id for target in node.targets if isinstance(target, ast.Name)}
        elif isinstance(node, ast.AnnAssign) and node.value is not None and imports_duckdb(node.value) and isinstance(node.target, ast.Name):
            modules.add(node.target.id)
    return modules, functions, stars


def imports_duckdb(node: ast.AST) -> bool:
    """Whether a node is ``importlib.import_module("duckdb")`` or ``__import__("duckdb")``."""
    if not isinstance(node, ast.Call) or not node.args or not isinstance(node.args[0], ast.Constant) or node.args[0].value != "duckdb":
        return False
    function = node.func
    return (isinstance(function, ast.Attribute) and function.attr == "import_module") or (
        isinstance(function, ast.Name) and function.id in {"__import__", "import_module"}
    )


def connects(source: str, urls: bool = True) -> list[int]:
    """The line numbers where a module reaches DuckDB: a call on the DuckDB module under any name it was imported by (``connect``, and also ``sql`` and
    ``read_parquet``, which open the default in-memory connection of the process without any bound), a call of a function imported from it, a star import,
    and with ``urls`` a SQLAlchemy URL of DuckDB. A name that is only an annotation or an exception class is not a call."""
    tree = ast.parse(source)
    modules, functions, stars = module_names(tree)
    lines = list(stars)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            function = node.func
            if (
                isinstance(function, ast.Attribute)
                and ((isinstance(function.value, ast.Name) and function.value.id in modules) or imports_duckdb(function.value))
                or isinstance(function, ast.Name)
                and function.id in functions
            ):
                lines.append(node.lineno)
        elif urls and isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.startswith("duckdb:"):
            lines.append(node.lineno)
    return sorted(set(lines))


def unlisted(root: Path) -> list[str]:
    """The modules under ``root`` that open a DuckDB connection and are neither a phase, nor listed as left, nor the helper."""
    found = []
    for folder in ("scripts", "monitoring", "orchestration", "services"):
        for path in sorted((root / folder).rglob("*.py")):
            relative = path.relative_to(root).as_posix()
            if skipped(relative) or relative in PHASES or relative in LEFT or relative == HELPER:
                continue
            if connects(path.read_text()):
                found.append(relative)
    return found


# Each form a module can reach DuckDB by, with the call that opens a connection. A scanner that sees only ``duckdb.connect`` misses all but the first.
FORMS = {
    "the module under its own name": "import duckdb\nconnection = duckdb.connect()\n",
    "the module under an alias": "import duckdb as db\nconnection = db.connect()\n",
    "a submodule import that binds the module": "import duckdb.typing\nconnection = duckdb.connect()\n",
    "a function imported by name": "from duckdb import connect\nconnection = connect()\n",
    "a function imported under another name": "from duckdb import connect as open_database\nconnection = open_database()\n",
    "a default-connection function imported by name": "from duckdb import read_parquet\nrelation = read_parquet('x')\n",
    "a star import": "from duckdb import *\n",
    "the module bound by import_module": "import importlib\nd = importlib.import_module('duckdb')\nconnection = d.connect()\n",
    "the module bound by import_module with an annotation": "import importlib\nd: object = importlib.import_module('duckdb')\nconnection = d.connect()\n",
    "the module used straight from import_module": "import importlib\nconnection = importlib.import_module('duckdb').connect()\n",
    "the module bound by __import__": "d = __import__('duckdb')\nconnection = d.connect()\n",
    "a SQLAlchemy URL of DuckDB": "from sqlalchemy import create_engine\nengine = create_engine('duckdb:///x.duckdb')\n",
}
# Sources that open no connection: names that are only annotations or exception classes, and another module under the name ``db``.
HARMLESS = {
    "an annotation": "import duckdb\ndef f(c: duckdb.DuckDBPyConnection) -> None: ...\n",
    "an exception class": "from duckdb import Error\ntry:\n    pass\nexcept Error:\n    pass\n",
    "another module under the same alias": "import sqlite3 as db\nconnection = db.connect('x')\n",
    "a function of the same name from another module": "from sqlite3 import connect\nconnection = connect('x')\n",
}


def scan_cases() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for name in PHASES:
        source = (ROOT / name).read_text()
        checks[f"{name}: no connection is opened without the helper"] = not connects(source, urls=False)
        checks[f"{name}: the helper is used"] = "duckdb_bounds" in source
        checks[f"{name}: a DuckDB URL for a library is listed with its reason"] = bool(set(connects(source)) - set(connects(source, urls=False))) == (
            name in URLS
        )
    found = unlisted(ROOT)
    checks[f"every other module that opens a DuckDB connection is listed with its reason ({found})"] = not found
    checks["a module that is listed as left still opens one"] = all(connects((ROOT / name).read_text()) for name in LEFT)
    for label, source in FORMS.items():
        checks[f"the scan finds a connection made through {label}"] = bool(connects(source))
    for label, source in HARMLESS.items():
        checks[f"the scan ignores {label}"] = not connects(source)
    for label in ("an alias", "a function imported by name"):
        source = FORMS["the module under an alias" if label == "an alias" else "a function imported by name"]
        with tempfile.TemporaryDirectory(prefix="duckdb_scan_") as scratch:
            module = Path(scratch) / "scripts/lakehouse/uncapped.py"
            module.parent.mkdir(parents=True)
            module.write_text(source)
            checks[f"a new module that connects through {label} is reported as unlisted"] = unlisted(Path(scratch)) == ["scripts/lakehouse/uncapped.py"]
    return checks


NATIVE_CAP_ERROR = (
    "Out of Memory Error: failed to offload data block of size 256.0 KiB (99.8 MiB/100.0 MiB used).\n"
    "This limit was set by the 'max_temp_directory_size' setting.\n"
)
SPILL_CHILD = (
    "import duckdb\nfrom scripts.lakehouse import duckdb_bounds\nfrom scripts.lakehouse.run_duckdb_spill_cases import SPILL_SQL\n"
    "duckdb_bounds.connect(duckdb, 'probe.duckdb').execute(SPILL_SQL)\n"
)
QUOTED_CHILD = "import duckdb\nduckdb.connect().execute(\"SELECT 'This limit was set by the max_temp_directory_size' FROM missing_table\")\n"


def signature_cases() -> dict[str, bool]:
    """The cap is recognised by DuckDB's own offload error and nothing else that mentions the setting."""
    return {
        "the native offload error is a cap failure": duckdb_bounds.cap_reached(NATIVE_CAP_ERROR),
        "the native error indented by a log is a cap failure": duckdb_bounds.cap_reached(
            "  Runtime Error in model x\n  " + NATIVE_CAP_ERROR.replace("\n", "\n  ")
        ),
        "the sentence alone is not a cap failure": not duckdb_bounds.cap_reached("This limit was set by the 'max_temp_directory_size' setting."),
        "SQL that quotes the sentence is not a cap failure": not duckdb_bounds.cap_reached(
            "Catalog Error: Table with name t does not exist!\nLINE 1: SELECT 'This limit was set by the max_temp_directory_size' FROM t"
        ),
        "SQL that quotes the whole message is not a cap failure": not duckdb_bounds.cap_reached(
            "Catalog Error: Table with name t does not exist!\nLINE 1: SELECT 'Out of Memory Error: failed to offload data block of size 256.0 KiB "
            "(99.8 MiB/100.0 MiB used). This limit was set by the ''max_temp_directory_size'' setting.' FROM t"
        ),
        "another out of memory error is not a cap failure": not duckdb_bounds.cap_reached(
            "Out of Memory Error: failed to pin block of size 256.0 KiB (190.0 MiB/190.0 MiB used)\nPossible solutions: increase the memory limit"
        ),
    }


def message_cases() -> dict[str, bool]:
    """A phase that reached the byte cap stops with a message that names the cap, the variable and the directory. The children are real processes."""
    from scripts.lakehouse import layer_run

    checks: dict[str, bool] = {}
    with tempfile.TemporaryDirectory(prefix="duckdb_message_") as scratch:
        code = Path(scratch) / "code"
        code.mkdir()
        (code / "cap_probe.py").write_text(f"import sys\nsys.stderr.write({NATIVE_CAP_ERROR!r})\nsys.exit(1)\n")
        (code / "spill_probe.py").write_text(SPILL_CHILD)
        (code / "quoted_probe.py").write_text(QUOTED_CHILD)
        (code / "plain_probe.py").write_text("import sys\nsys.stderr.write('another failure\\n')\nsys.exit(1)\n")
        spill = Path(scratch) / "spill"
        spill.mkdir()
        job = layer_run.Job("silver", "20261101T000000Z-abcdef12", "real", code)
        environment = {**task_environment(Path(scratch), 20 * MIB), "PYTHONPATH": str(ROOT)}
        cap, directory = environment["HAI_DUCKDB_MAX_TEMP_BYTES"], environment["HAI_DUCKDB_TEMP_DIRECTORY"]
        for module in ("cap_probe", "spill_probe", "quoted_probe", "plain_probe"):
            try:
                layer_run.run_module(job, module, [], environment)
                message = ""
            except layer_run.StepError as error:
                message = str(error)
            says = "temporary storage cap" in message and "HAI_DUCKDB_MAX_TEMP_BYTES" in message and cap in message and directory in message
            if module in ("cap_probe", "spill_probe"):
                label = "a real over-cap run" if module == "spill_probe" else "a phase that wrote the native cap error"
                checks[f"{label} names the cap, the variable, the value and the directory"] = says
            else:
                label = "a failure that only quotes the cap sentence in SQL" if module == "quoted_probe" else "a failure for another reason"
                checks[f"{label} gets the ordinary message"] = "exited 1" in message and not says
    return checks


def main() -> int:
    results = {**helper_cases(), **profile_cases(), **scan_cases(), **signature_cases(), **message_cases()}
    for name, ok in results.items():
        sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}\n")
    failed = [name for name, ok in results.items() if not ok]
    sys.stdout.write(f"duckdb bounds cases: {len(results) - len(failed)} of {len(results)} checks passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
