"""E2E for the check that no model or test of the dbt project is disabled by a missing reference (``scripts.lakehouse.dbt_disabled_nodes``).

Run from the repository root: ``.venv/bin/python -m scripts.lakehouse.run_dbt_disabled_nodes_e2e``. No Docker, catalog or AWS service is used. The cases compile
scratch copies of the real dbt project with the lint target (the pinned dbt of .tools/sqlfluff):

- the project as committed has no disabled node of its own;
- a bronze table that ``copy_tables`` lists and that has no staging view disables both global staging tests, and the check names them;
- the same table with the deferral record (``vars.deferred_models``) naming its staging model disables nothing.

The report in ``data/e2e/dbt_disabled_nodes/`` holds outcomes only.
"""

from __future__ import annotations

import importlib
import json
import re
import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import dbt_disabled_nodes as nodes
from scripts.lakehouse import deferred_models

ROOT = Path(__file__).resolve().parents[2]
SOURCES = "dbt/models/silver/staging/_bronze__sources.yml"
TABLE = "scratch_without_view"
GLOBAL_TESTS = ("test.hai.assert_staging_rows_match_files", "test.hai.assert_staging_unique_grain")


def scratch(folder: Path, *, defer: bool) -> Path:
    """A scratch copy of the dbt project with one more bronze source table that copy_tables lists and no staging view exists for (the state of a table whose
    staging model is deferred), with or without a deferral record naming the model."""
    shutil.copytree(ROOT / "dbt", folder / "dbt", ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"))
    sources = folder / SOURCES
    text = sources.read_text()
    anchor = "      - name: file_metadata\n"
    if anchor not in text:
        raise AssertionError("the bronze sources file no longer has the anchor line this case edits")
    sources.write_text(text.replace(anchor, f"      - name: {TABLE}\n        description: A source table without a staging view.\n" + anchor, 1))
    project = folder / "dbt/dbt_project.yml"
    text = project.read_text().replace("  copy_tables:\n", f"  copy_tables:\n    {TABLE}: hai\n", 1)
    if defer:
        listed = "models: [stg_hcai_finance_csv, int_hcai_finance_values]"
        if listed not in text:
            raise AssertionError("the deferral record no longer has the model list this case extends")
        text = text.replace(listed, listed[:-1] + f", stg_{TABLE}]", 1)
    project.write_text(text)
    return folder / "dbt"


def disabled_of(project: Path, target: Path) -> list[str]:
    """The disabled nodes of a compile of the project."""
    return nodes.disabled_nodes(nodes.compile_manifest(project, target))


def cases() -> dict[str, bool]:
    """Run every case and return its outcome."""
    results: dict[str, bool] = {}
    with tempfile.TemporaryDirectory(prefix="dbt_disabled_") as folder:
        base = Path(folder)
        shutil.copytree(ROOT / "dbt", base / "committed" / "dbt", ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"))
        results["the_committed_project_disables_no_node"] = disabled_of(base / "committed" / "dbt", base / "committed" / "target") == []
        project = importlib.import_module("yaml").safe_load((ROOT / "dbt/dbt_project.yml").read_text())
        compiled = (base / "committed" / "target" / "compiled" / "hai" / "tests" / "assert_staging_rows_match_files.sql").read_text()
        macro_tables = sorted(re.findall(r"^staged_([a-z0-9_]+) as \(", compiled, flags=re.MULTILINE))
        results["the_macro_and_the_python_helper_agree_on_the_staged_tables"] = macro_tables == sorted(deferred_models.staged_tables(project)) and bool(
            macro_tables
        )
        results["the_deferred_finance_table_is_in_copy_tables_and_not_staged"] = (
            "hcai_finance_csv" in project["vars"]["copy_tables"] and "hcai_finance_csv" not in macro_tables
        )
        removed = disabled_of(scratch(base / "removed", defer=False), base / "removed" / "target")
        results["a_table_without_a_staging_view_disables_both_global_tests_and_the_check_names_them"] = all(name in removed for name in GLOBAL_TESTS)
        deferred = disabled_of(scratch(base / "deferred", defer=True), base / "deferred" / "target")
        results["a_deferred_staging_view_disables_nothing"] = deferred == []
    return results


def main() -> int:
    """Run the cases, write the report and return 1 when any case fails."""
    started = datetime.now(UTC)
    outcome = cases()
    folder = ROOT / "data" / "e2e" / "dbt_disabled_nodes"
    folder.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {"run_at_utc": started.strftime("%Y%m%dT%H%M%SZ"), "cases": outcome, "passed": all(outcome.values())}
    (folder / f"report_{report['run_at_utc']}.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    for name, passed in outcome.items():
        sys.stdout.write(f"{'ok' if passed else 'FAIL':<5} {name}\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
