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
# The other modules that open a DuckDB connection, and why each is not bounded by the helper.
LEFT = {
    "scripts/lakehouse/provenance.py": "runs on the host (the launcher) and reads one marker row of the sealed copy, read only",
    "scripts/lakehouse/bronze_scan.py": "decodes one Parquet row group of at most 32 MiB in memory on one thread, and the bronze job sets no spill variables",
    "monitoring/current_window.py": "the monitoring job reads one Parquet window on the default in-memory connection, and its task sets no spill variables",
    "monitoring/drift_check.py": "the monitoring job reads Parquet with the default in-memory connection, and its task sets no spill variables",
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


def connects(source: str) -> list[int]:
    """The line numbers of the calls on the ``duckdb`` module in a module: ``connect`` and also ``sql``, ``read_parquet`` and the others, which open the default
    in-memory connection of the process without any bound."""
    return [
        node.lineno
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name) and node.func.value.id == "duckdb"
    ]


def scan_cases() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for name in PHASES:
        source = (ROOT / name).read_text()
        checks[f"{name}: no connection is opened without the helper"] = not connects(source)
        checks[f"{name}: the helper is used"] = "duckdb_bounds" in source
    unlisted = []
    for folder in ("scripts", "monitoring", "orchestration", "services"):
        for path in sorted((ROOT / folder).rglob("*.py")):
            relative = path.relative_to(ROOT).as_posix()
            if skipped(relative) or relative in PHASES or relative in LEFT or relative == HELPER:
                continue
            if connects(path.read_text()):
                unlisted.append(relative)
    checks[f"every other module that opens a DuckDB connection is listed with its reason ({unlisted})"] = not unlisted
    checks["a module that is listed as left still opens one"] = all(connects((ROOT / name).read_text()) for name in LEFT)
    return checks


def message_cases() -> dict[str, bool]:
    """A phase that reached the byte cap stops with a message that names the cap, the variable and the directory."""
    from scripts.lakehouse import layer_run

    checks: dict[str, bool] = {}
    with tempfile.TemporaryDirectory(prefix="duckdb_message_") as scratch:
        code = Path(scratch) / "code"
        code.mkdir()
        (code / "cap_probe.py").write_text(
            'import sys\nsys.stderr.write("Out of Memory Error: failed to offload data block of size 256.0 KiB (99.8 MiB/100.0 MiB used).\\n"\n'
            "    \"This limit was set by the 'max_temp_directory_size' setting.\\n\")\nsys.exit(1)\n"
        )
        (code / "plain_probe.py").write_text("import sys\nsys.stderr.write('another failure\\n')\nsys.exit(1)\n")
        job = layer_run.Job("silver", "20261101T000000Z-abcdef12", "real", code)
        environment = {"HAI_DUCKDB_TEMP_DIRECTORY": "/workspace/duckdb_spill", "HAI_DUCKDB_MAX_TEMP_BYTES": "64424509440"}
        for module, wanted in (("cap_probe", True), ("plain_probe", False)):
            try:
                layer_run.run_module(job, module, [], environment)
                message = ""
            except layer_run.StepError as error:
                message = str(error)
            says = (
                "temporary storage cap" in message
                and "HAI_DUCKDB_MAX_TEMP_BYTES" in message
                and "64424509440" in message
                and "/workspace/duckdb_spill" in message
            )
            if wanted:
                checks["a phase that reached the cap names the cap, the variable, the value and the directory"] = says
            else:
                checks["a phase that failed for another reason gets the ordinary message"] = "exited 1" in message and not says
    return checks


def main() -> int:
    results = {**helper_cases(), **profile_cases(), **scan_cases(), **message_cases()}
    for name, ok in results.items():
        sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}\n")
    failed = [name for name, ok in results.items() if not ok]
    sys.stdout.write(f"duckdb bounds cases: {len(results) - len(failed)} of {len(results)} checks passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
