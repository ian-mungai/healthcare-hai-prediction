"""Spill cases of the bounded dbt profiles, run with the pinned dbt-duckdb adapter and no Docker.

    .tools/sqlfluff/bin/python -m scripts.lakehouse.run_duckdb_spill_cases

DuckDB, dbt-core and dbt-duckdb are not in ``.venv``. The pinned set (the versions of services/analytics/requirements.txt) is in ``.tools/sqlfluff``.

The adapter runs every ``settings`` entry of a profile as ``SET key = value`` on each new cursor. DuckDB refuses to switch its temporary directory once a
query has spilled into it, even to the same path, so a ``temp_directory`` setting fails every model and test after the first spill. The fixture suites never
spill, which is why they never saw it. Each case renders the real ``dbt/profiles.yml`` with the adapter's own environment, forces a real spill on the first
cursor and then opens a second cursor the way dbt does for the next node. The byte cap is the other way round: ``max_temp_directory_size`` given at connect is
read back but not enforced, so it must stay a setting. A case proves that too, and a control with the old profile shape must fail."""

from __future__ import annotations

import argparse
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

BOUNDED = ("pinned_silver_bounded", "lakehouse_session_bounded", "lakehouse_task", "fixture_bounded")
PROFILES = Path(__file__).resolve().parents[2] / "dbt/profiles.yml"
SPILL_SQL = "CREATE OR REPLACE TABLE spilled AS SELECT i, repeat('x', 20) AS s FROM range(12000000) r(i) ORDER BY hash(i)"
FAILURE = "Cannot switch temporary directory after the current one has been used"
CAP_MESSAGE = "max_temp_directory_size"


def rendered(target: str, folder: Path, cap: int, old_shape: bool = False) -> dict[str, Any]:
    """The target of the real profile with the task's variables: the file is rendered by Jinja and parsed by YAML as dbt does, and the parts that need a catalog
    or a network are dropped. ``old_shape`` moves ``temp_directory`` back into ``settings``, the way the profile was before the fix."""
    import jinja2
    import yaml

    values = {
        "JOB_THREADS": "2",
        "DUCKDB_MEMORY_LIMIT": "150MB",
        "HAI_DUCKDB_TEMP_DIRECTORY": str(folder / "spill"),
        "HAI_DUCKDB_MAX_TEMP_BYTES": str(cap),
        "STAGING_E2E_CASE": str(folder),
        "AWS_REGION": "us-west-2",
        "AWS_PROFILE": "none",
        "POLARIS_URI": "http://polaris.invalid:8181/api/catalog",
        "DBT_ENV_SECRET_POLARIS_CLIENT_ID": "id",
        "DBT_ENV_SECRET_POLARIS_CLIENT_SECRET": "secret",
    }

    def env_var(name: str, default: str | None = None) -> str | None:
        return values.get(name, default)

    environment = jinja2.Environment(autoescape=True)  # the values hold no character that this escapes
    environment.globals["env_var"] = env_var
    output = dict(yaml.safe_load(environment.from_string(PROFILES.read_text()).render())["hai"]["outputs"][target])
    for name in ("extensions", "secrets", "attach", "threads"):
        output.pop(name, None)
    output["path"] = str(folder / "staging.duckdb")
    if old_shape:
        options = dict(output.get("config_options") or {})
        output["settings"] = {**output.get("settings", {}), "temp_directory": options.pop("temp_directory", str(folder / "spill"))}
        output["config_options"] = options
    (folder / "spill").mkdir(parents=True, exist_ok=True)
    return output


def spill_bytes(folder: Path) -> int:
    """The disk the spill files hold now: allocated blocks, because DuckDB creates its temporary files sparse."""
    return sum(path.stat().st_blocks * 512 for path in folder.rglob("*") if path.is_file()) if folder.exists() else 0


def run_case(target: str, cap: int, old_shape: bool = False) -> dict[str, Any]:
    """Spill on the first cursor, then open a second one. Returns what happened, never raises."""
    from dbt.adapters.duckdb.credentials import DuckDBCredentials
    from dbt.adapters.duckdb.environments.local import LocalEnvironment

    with tempfile.TemporaryDirectory(prefix="duckdb_spill_") as scratch:
        folder = Path(scratch)
        credentials = DuckDBCredentials.from_dict(rendered(target, folder, cap, old_shape))
        environment = LocalEnvironment(credentials)
        peak = [0]
        stop = threading.Event()

        def watch() -> None:
            while not stop.is_set():
                peak[0] = max(peak[0], spill_bytes(folder / "spill"))
                time.sleep(0.02)

        result: dict[str, Any] = {"spill_error": "", "second_cursor_error": "", "second_cursor": None}
        first = environment.handle()
        watcher = threading.Thread(target=watch)
        watcher.start()
        try:
            first.cursor().execute(SPILL_SQL)
        except Exception as error:
            result["spill_error"] = str(error)
        stop.set()
        watcher.join()
        result["spill_bytes"] = peak[0]
        try:
            result["second_cursor"] = environment.handle().cursor().execute("SELECT 42").fetchall()
        except Exception as error:
            result["second_cursor_error"] = str(error)
        return result


def cases() -> dict[str, bool]:
    import yaml

    checks: dict[str, bool] = {}
    outputs = yaml.safe_load(PROFILES.read_text())["hai"]["outputs"]
    for name, output in outputs.items():
        checks[f"{name}: temp_directory is not a setting"] = "temp_directory" not in (output.get("settings") or {})
        checks[f"{name}: the byte cap is not a connect option"] = "max_temp_directory_size" not in (output.get("config_options") or {})
    for target in BOUNDED:
        result = run_case(target, 20 * 2**30)
        checks[f"{target}: a real spill happens in the configured directory"] = result["spill_error"] == "" and result["spill_bytes"] > 50 * 2**20
        checks[f"{target}: a new cursor works after the spill"] = result["second_cursor"] == [(42,)] and result["second_cursor_error"] == ""
        capped = run_case(target, 20 * 2**20)
        checks[f"{target}: the byte cap stops a spill that passes it"] = CAP_MESSAGE in capped["spill_error"] and capped["spill_bytes"] < 40 * 2**20
    control = run_case("fixture_bounded", 20 * 2**30, old_shape=True)
    checks["control: the old shape spills and then fails the next cursor"] = control["spill_bytes"] > 50 * 2**20 and FAILURE in control["second_cursor_error"]
    return checks


def main() -> int:
    argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0]).parse_args()
    results = cases()
    for name, ok in results.items():
        sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}\n")
    failed = [name for name, ok in results.items() if not ok]
    sys.stdout.write(f"duckdb spill cases: {len(results) - len(failed)} of {len(results)} checks passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
