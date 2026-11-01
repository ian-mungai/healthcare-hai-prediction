"""Cases of the silver build marker after a real dbt build, run with the pinned adapter and no Docker.

    .tools/sqlfluff/bin/python -m scripts.lakehouse.run_build_marker_cases

The marker table ``int_silver_build_marker`` is what validation reads first: without it the build "did not finish". The macros that decide whether a build keeps
its marker (dbt/macros/build_marker_hooks.sql) read the selection of the dbt command, and ``pipeline_build`` always passes one. Each case builds a small project
that carries the real macros, the real marker model and the real hook registration, through the bounded profile of dbt/profiles.yml, in a child process that
ends the way the build step ends (dbt in process, then a normal exit). It then reads the file with a new process, as the validate container does, and also
looks for a leftover write-ahead log."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

from scripts.lakehouse.pipeline_build import layer_selection
from scripts.lakehouse.run_duckdb_spill_cases import rendered
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
MARKER = "int_silver_build_marker"
CHILD = """
import json, os, sys
from pathlib import Path
import yaml
folder, project, arguments, output = Path(sys.argv[1]), sys.argv[2], json.loads(sys.argv[3]), json.loads(sys.argv[4])
(folder / "profiles").mkdir(parents=True)
(folder / "profiles/profiles.yml").write_text(yaml.safe_dump({"hai": {"target": "x", "outputs": {"x": output}}}))
os.environ.update({"HAI_PROVENANCE_REQUIRED": "1", "HAI_BUILD_RUN_ID": "20261101T000000Z-abcdef12", "HAI_BUILD_DBT_TREE_SHA256": "a" * 64,
    "HAI_BUILD_PINS_SHA256": "b" * 64, "HAI_BUILD_PINS_JSON": json.dumps({"t": 1}), "HAI_BUILD_GIT_REVISION": "c" * 40,
    "DBT_TARGET_PATH": str(folder / "target"), "DBT_LOG_PATH": str(folder / "logs"), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"})
from dbt.cli.main import dbtRunner
result = dbtRunner().invoke(["build", "--target", "x", "--project-dir", project, "--profiles-dir", str(folder / "profiles"), *arguments])
sys.stdout.write(json.dumps({"success": result.success}))
"""
READ = """
import json, sys
import duckdb
c = duckdb.connect(sys.argv[1], read_only=True)
tables = sorted(r[0] for r in c.execute("select table_name from information_schema.tables").fetchall())
rows = c.execute("select run_id from int_silver_build_marker").fetchall() if "int_silver_build_marker" in tables else []
sys.stdout.write(json.dumps({"tables": tables, "marker_rows": len(rows)}))
"""


def project(folder: Path, failing: bool) -> Path:
    """A project with the real marker macros, the real marker model and the real hook registration, and stand-ins for the two models it depends on."""
    path = folder / "project"
    (path / "macros").mkdir(parents=True)
    (path / "models/silver/intermediate").mkdir(parents=True)
    for name in ("build_marker_hooks.sql", "build_provenance.sql"):
        (path / "macros" / name).write_text((ROOT / "dbt/macros" / name).read_text())
    (path / f"models/silver/intermediate/{MARKER}.sql").write_text((ROOT / f"dbt/models/silver/intermediate/{MARKER}.sql").read_text())
    for name in ("int_county_adjacency_edges", "int_change_of_ownership_holds"):
        (path / f"models/silver/intermediate/{name}.sql").write_text("{{ config(materialized='table') }}\nselect 1 as a\n")
    (path / "models/silver/big.sql").write_text("{{ config(materialized='table') }}\nselect i, repeat('x', 40) as s from range(200000) r(i)\n")
    (path / "models/other.sql").write_text("{{ config(materialized='table') }}\nselect 1 as a\n")
    test = "      - name: run_id\n        tests: [not_null" + (", accepted_values: {values: ['never']}" if failing else "") + "]\n"
    (path / "models/silver/schema.yml").write_text(f"version: 2\nmodels:\n  - name: {MARKER}\n    columns:\n{test}")
    (path / "dbt_project.yml").write_text(
        "name: marker_cases\nversion: '1.0.0'\nprofile: hai\nmodel-paths: [models]\nmacro-paths: [macros]\n"
        'flags: {send_anonymous_usage_stats: false, use_colors: false}\non-run-start:\n  - "{{ hai_drop_build_marker() }}"\n'
        'on-run-end:\n  - "{{ hai_finalize_build_marker(results) }}"\n'
    )
    return path


def build(arguments: list[str], failing: bool = False) -> dict[str, Any]:
    """Build in a child process, then read the file in another. Returns what the reader saw and whether a write-ahead log was left."""
    with tempfile.TemporaryDirectory(prefix="marker_case_") as scratch:
        folder = Path(scratch)
        output = rendered("pinned_silver_bounded", folder, 20 * 2**30)
        built = run_command(
            sys.executable, ["-c", CHILD, str(folder), str(project(folder, failing)), json.dumps(arguments), json.dumps(output)], cwd=ROOT, timeout=300
        )
        database = Path(output["path"])
        seen = run_command(sys.executable, ["-c", READ, str(database)], cwd=ROOT, timeout=120)
        result = json.loads(seen.stdout) if seen.returncode == 0 else {"tables": [], "marker_rows": 0, "read_error": seen.stderr[-300:]}
        result["wal_left"] = database.with_name(database.name + ".wal").exists()
        result["exit"] = built.returncode
        result["success"] = '"success": true' in built.stdout
        return result


def cases() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    layer = ["--select", *layer_selection("silver"), "--indirect-selection", "cautious"]
    built = build(layer)
    checks["the layer build passes"] = built["success"] and built["exit"] == 0
    checks["the layer build of silver keeps the marker with one row"] = MARKER in built["tables"] and built["marker_rows"] == 1
    checks["the layer build leaves no write-ahead log"] = not built["wal_left"]
    checks["a build with no selection keeps the marker"] = MARKER in build([])["tables"]
    partial = build(["--select", "other", "--indirect-selection", "cautious"])
    checks["a build of one model leaves no marker"] = partial["success"] and MARKER not in partial["tables"]
    checks["a build of the marker model alone leaves no marker"] = MARKER not in build(["--select", MARKER])["tables"]
    checks["the gold selection is not the silver layer: no marker"] = MARKER not in build(["--select", *layer_selection("gold")])["tables"]
    failed = build(layer, failing=True)
    checks["a layer build with a failing test leaves no marker"] = not failed["success"] and MARKER not in failed["tables"]
    return checks


def main() -> int:
    argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0]).parse_args()
    results = cases()
    for name, ok in results.items():
        sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}\n")
    failed = [name for name, ok in results.items() if not ok]
    sys.stdout.write(f"build marker cases: {len(results) - len(failed)} of {len(results)} checks passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
