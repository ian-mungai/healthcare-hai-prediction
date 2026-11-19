"""E2E for the declared consumer columns of the bronze tables that silver reads by name (config/lakehouse/bronze_consumer_columns.json).

Run from the repository root: ``.venv/bin/python -m scripts.lakehouse.run_bronze_consumer_columns_e2e``. No Docker, catalog or AWS service is used. The cases
compile the real dbt project with the lint target (the pinned dbt of .tools/sqlfluff), so the derived lists come from the real model text:

- the committed contract equals the lists derived from the compiled models;
- a model that starts to read one more bronze column makes the check fail and name the table and the column;
- a contract with a column no model reads, a missing table and an extra table each fail and name it;
- the load assertion refuses a loaded table by table and column name, passes an exact or wider table, passes a table with no rows whatever its columns, and
  reports a table without a declaration as undeclared, never as passed.

The report in ``data/e2e/bronze_consumer_columns/`` holds outcomes only.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import bronze_consumer_columns as columns

ROOT = Path(__file__).resolve().parents[2]
MACRO = "dbt/macros/state_columns.sql"
UTIL_TABLE = "hcai_util_pages_csv"
ILLINOIS_TABLE = "illinois_report_card_csv"
NEW_COLUMN = "reviewed_new_column"
ANCHOR = "    nullif(trim(bronze.name_friendly), '') as name_friendly,\n"


def scratch_derived(root: Path, edit: str) -> dict[str, list[str]]:
    """Derive the lists from a scratch copy of the dbt project whose Illinois projection also reads one new column."""
    shutil.copytree(ROOT / "dbt", root / "dbt", ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"))
    macro = root / MACRO
    text = macro.read_text()
    if ANCHOR not in text:
        raise AssertionError("the Illinois projection no longer has the anchor line this case edits")
    macro.write_text(text.replace(ANCHOR, ANCHOR + edit, 1))
    return columns.derive(columns.compile_project(root / "dbt", root / "target", packages=ROOT / "data/analytics/dbt/dbt_packages"))


def cases() -> dict[str, bool]:
    """Run every case and return its outcome."""
    contract = columns.load(ROOT / columns.CONTRACT)
    derived = columns.derive(columns.compile_project(ROOT / "dbt", Path(tempfile.mkdtemp(prefix="bronze_columns_")) / "target"))
    results: dict[str, bool] = {}
    results["contract_declares_the_three_state_tables"] = set(contract) == {UTIL_TABLE, "hcai_util_nonresponse_csv", ILLINOIS_TABLE}
    results["contract_equals_the_lists_derived_from_the_models"] = columns.differences(derived, contract) == []
    results["every_list_has_the_lineage_columns"] = all({"_object_key", "_member_sha256", "_row_number"} <= set(names) for names in contract.values())
    with tempfile.TemporaryDirectory(prefix="bronze_columns_case_") as folder:
        grown = scratch_derived(Path(folder), f"    nullif(trim(bronze.{NEW_COLUMN}), '') as {NEW_COLUMN},\n")
    found = columns.differences(grown, contract)
    results["a_model_reading_a_new_column_fails_the_check_by_table_and_column"] = any(ILLINOIS_TABLE in line and NEW_COLUMN in line for line in found)
    stale = {**contract, UTIL_TABLE: [*contract[UTIL_TABLE], "column_no_model_reads"]}
    found = columns.differences(derived, stale)
    results["a_column_no_model_reads_fails_the_check_by_table_and_column"] = any(UTIL_TABLE in line and "column_no_model_reads" in line for line in found)
    found = columns.differences(derived, {key: value for key, value in contract.items() if key != ILLINOIS_TABLE})
    results["a_missing_table_fails_the_check_by_table"] = any(ILLINOIS_TABLE in line for line in found)
    found = columns.differences(derived, {**contract, "table_no_model_reads": ["a"]})
    results["an_extra_table_fails_the_check_by_table"] = any("table_no_model_reads" in line for line in found)
    wanted = contract[ILLINOIS_TABLE]
    results["a_table_with_every_column_passes_the_load_assertion"] = columns.missing(ILLINOIS_TABLE, [*wanted, "another"], 5, contract) == []
    short = [name for name in wanted if name != "county_name"]
    results["a_table_one_column_short_is_refused_by_table_and_column"] = columns.missing(ILLINOIS_TABLE, short, 5, contract) == ["county_name"] and all(
        part in columns.refusal(ILLINOIS_TABLE, ["county_name"], short) for part in (ILLINOIS_TABLE, "county_name", "missing")
    )
    wide = ["_object_key", "_member_sha256", "_row_number", "u_id", "data_item", "fac_no"]
    refused = columns.missing(UTIL_TABLE, wide, 6995, contract)
    results["a_table_of_another_layout_is_refused_naming_the_columns"] = "fac_name" in refused and "tot_lic_beds" in refused and "fac_no" not in refused
    results["a_table_with_no_rows_passes_whatever_its_columns"] = columns.missing(UTIL_TABLE, wide, 0, contract) == []
    results["a_table_without_a_declaration_is_undeclared_not_refused"] = columns.missing("cms_hai_hospital", wide, 5, contract) == [] and (
        columns.declared("cms_hai_hospital", contract) is False and columns.declared(UTIL_TABLE, contract) is True
    )
    results["the_refusal_names_the_present_columns"] = "u_id" in columns.refusal(UTIL_TABLE, refused, wide)
    return results


def main() -> int:
    """Run the cases, write the report and return 1 when any case fails."""
    started = datetime.now(UTC)
    outcome = cases()
    folder = ROOT / "data" / "e2e" / "bronze_consumer_columns"
    folder.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {"run_at_utc": started.strftime("%Y%m%dT%H%M%SZ"), "cases": outcome, "passed": all(outcome.values())}
    (folder / f"report_{report['run_at_utc']}.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    for name, passed in outcome.items():
        sys.stdout.write(f"{'ok' if passed else 'FAIL':<5} {name}\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
