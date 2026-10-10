"""E2E for silver step 6: Great Expectations validation through the real entry point (scripts/lakehouse/silver.py).

The fixture build is the staging E2E's ``base`` case (every silver table, built by dbt from synthetic bronze). Each case
copies it, adds the build marker and applies one SQL change; the validation must fail exactly the named expectations
and pass every other one. Failure modes 639 to 651 and 713 (plans/silver_processed_zone_20261009/plan.md). Report:
data/e2e/silver_quality/fixture_<UTC>/report.json.

    .venv/bin/python -m scripts.lakehouse.run_silver_e2e
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import silver
from scripts.process import run_command

REPO_ROOT = silver.REPO_ROOT
BASE = REPO_ROOT / "data/analytics/dbt/e2e/base/staging.duckdb"
BASE_MANIFEST = BASE.parent / "target/manifest.json"
CASES_ROOT = REPO_ROOT / "data/analytics/dbt/e2e/silver_quality"
FIXTURE_BASELINES = silver.CONTRACTS / "fixture_baselines.json"
SENTINEL = "SENTINEL_VALUE_7f3a"
# The base build has its own marker; each case replaces it with a fixed one.
MARKER = (
    "CREATE OR REPLACE TABLE int_silver_build_marker AS "
    "SELECT 'fixture-invocation' AS dbt_invocation_id, 'fixture' AS git_revision, version() AS duckdb_version;"
)

# (case, SQL applied after the marker, expectation ids that must fail; every other one must pass).
CASES: list[tuple[str, str, set[str]]] = [
    ("good", "", set()),
    ("dropped_column", "ALTER TABLE int_spine_linkage DROP COLUMN value_note;", {"int_spine_linkage.columns", "int_spine_linkage.types"}),  # [640]
    ("empty_table", "DELETE FROM int_spine_linkage;", {"int_spine_linkage.has_rows", "linkage.every_spine_row"}),  # [640]
    ("wrong_type", "ALTER TABLE int_spine_linkage ALTER value_number TYPE VARCHAR;", {"int_spine_linkage.types"}),  # [641]
    (
        "county_without_source",
        "UPDATE int_hospital_spine SET county_source = NULL WHERE spine_key = (SELECT min(spine_key) FROM int_hospital_spine WHERE county_fips IS NOT NULL);",
        {"spine.county_has_source"},
    ),  # [648]
    (
        "token_filled",
        "UPDATE int_spine_county_context SET value_text = 'Suppressed', value_number = 5 "
        "WHERE alignment_key = (SELECT min(alignment_key) FROM int_spine_county_context);",
        {"county_context.token_not_filled"},
    ),  # [648]
    (
        "validation_control_in_predictor",
        "UPDATE int_spine_care_compare_measures SET measure_control = (SELECT min(measure_control) FROM validation_measures) "
        "WHERE alignment_key = (SELECT min(alignment_key) FROM int_spine_care_compare_measures);",
        {"care_compare.no_validation_or_linkage_control"},
    ),  # [649]
    (
        "missing_row",
        "DELETE FROM int_spine_county_measures WHERE alignment_key = (SELECT min(alignment_key) FROM int_spine_county_measures);",
        {"county_measures.rows_per_spine_row"},
    ),  # [650]
    (
        "sir_outside_interval",
        "UPDATE int_spine_hai_outcomes SET sir = ci_upper + 1 WHERE outcome_key = (SELECT min(outcome_key) FROM int_spine_hai_outcomes "
        "WHERE sir IS NOT NULL AND ci_upper IS NOT NULL AND observed IS NOT NULL AND predicted > 0);",
        {"outcomes.sir_inside_interval", "outcomes.sir_recomputed"},
    ),  # [651]
    (
        "negative_age",
        "UPDATE int_spine_hospital_measures SET age_months = -1 "
        "WHERE alignment_key = (SELECT min(alignment_key) FROM int_spine_hospital_measures WHERE alignment_status = 'aligned');",
        {"hospital_measures.age_not_negative"},
    ),  # [651]
    (
        "sentinel_token",
        "UPDATE int_spine_linkage SET value_text = 'SENTINEL_VALUE_7f3a', value_number = 1 "
        "WHERE alignment_key = (SELECT min(alignment_key) FROM int_spine_linkage);",
        {"linkage.token_not_filled"},
    ),  # [643] [648]: the sentinel must not appear in the record
    (
        "primary_outside_sensitivity",
        "UPDATE int_hospital_spine SET is_sensitivity_population = false "
        "WHERE spine_key = (SELECT min(spine_key) FROM int_hospital_spine WHERE is_primary_population);",
        {"spine.primary_in_sensitivity"},
    ),
    (
        "self_link_unflagged",
        "UPDATE int_county_adjacency_edges SET is_self_link = NOT is_self_link WHERE edge_key = (SELECT min(edge_key) FROM int_county_adjacency_edges);",
        {"adjacency.self_link_flagged"},
    ),
]
# Cases that stop before any expectation runs, with the text the stop must name [646] [647].
STOPS = [
    ("no_marker", "DROP TABLE int_silver_build_marker;", "no int_silver_build_marker"),
    ("other_duckdb_version", "UPDATE int_silver_build_marker SET duckdb_version = 'v0.0.0';", "written by DuckDB v0.0.0"),
]


def make_case(name: str, sql: str) -> Path:
    """Copy the fixture build, add the marker and apply the case's change with the DuckDB CLI (same 1.5.6 engine)."""
    path = CASES_ROOT / f"{name}.duckdb"
    shutil.copyfile(BASE, path)
    result = run_command("duckdb", [str(path), "-c", MARKER + sql], cwd=REPO_ROOT, timeout=300)
    if result.returncode:
        raise RuntimeError(f"case {name}: {result.stderr.strip().splitlines()[-1:]}")
    return path


def baselines(name: str, approved: bool, spine_rows_max: int | None = None) -> Path:
    """A copy of the fixture baselines with one band moved, to prove bands come from the file and D6 warns [644] [645]."""
    document = json.loads(FIXTURE_BASELINES.read_text())
    document["approved"] = approved
    if spine_rows_max is not None:
        document["values"]["spine.rows.max"] = spine_rows_max
    path = CASES_ROOT / f"{name}_baselines.json"
    path.write_text(json.dumps(document, indent=2) + "\n")
    return path


def levels(record: dict[str, Any]) -> dict[str, str]:
    return {item["id"]: item["level"] for item in record.get("expectations", [])}


def run(name: str, database: Path, baseline: Path) -> tuple[int, dict[str, Any]]:
    """Validate through the real entry point; a stop returns its message as the record."""
    try:
        code, path = silver.validate(database, silver.CONTRACTS / "suites", baseline, BASE_MANIFEST)
    except silver.SilverError as error:
        return 2, {"error": str(error)}
    return code, json.loads(path.read_text())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.parse_args()
    if not BASE.is_file():
        sys.stderr.write("run the staging E2E first: its base case is the fixture build\n")
        return 2
    started = time.monotonic()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    shutil.rmtree(CASES_ROOT, ignore_errors=True)
    CASES_ROOT.mkdir(parents=True)
    checks: dict[str, bool] = {}
    detail: dict[str, Any] = {}
    fixture = baselines("fixture", approved=True)
    for name, sql, failing in CASES:
        code, record = run(name, make_case(name, sql), fixture)
        seen = levels(record)
        wrong = sorted(key for key, level in seen.items() if (level == "fail") != (key in failing))
        checks[f"{name}_fails_exactly_its_expectations"] = bool(seen) and not wrong and (code != 0) == bool(failing) and not record["repeated_dbt_tests"]
        checks[f"{name}_record_has_no_sentinel"] = SENTINEL not in json.dumps(record)
        detail[name] = {"exit_code": code, "unexpected_levels": wrong, "counts": record.get("counts"), "error": record.get("error")}
    good = make_case("bands", "")
    code, record = run("band_approved", good, baselines("band_approved", approved=True, spine_rows_max=5))
    checks["shifted_band_fails_when_approved"] = code == 1 and {key for key, level in levels(record).items() if level != "pass"} == {"spine.rows"}
    checks["shifted_band_fails_as_fail"] = levels(record).get("spine.rows") == "fail"
    code, record = run("band_unapproved", good, baselines("band_unapproved", approved=False, spine_rows_max=5))
    checks["shifted_band_warns_until_approved"] = code == 0 and {key for key, level in levels(record).items() if level != "pass"} == {"spine.rows"}
    checks["shifted_band_warns_as_warn"] = levels(record).get("spine.rows") == "warn"
    for name, sql, text in STOPS:
        code, record = run(name, make_case(name, sql), fixture)
        checks[f"{name}_stops"] = code == 2 and text in json.dumps(record)
        detail[name] = {"exit_code": code, "error": record.get("error")}
    report = {
        "mode": "full",
        "started_at_utc": stamp,
        "seconds": round(time.monotonic() - started, 1),
        "checks": checks,
        "detail": detail,
        "passed": sum(checks.values()),
        "total": len(checks),
    }
    out = silver.RESULTS / f"fixture_{stamp}"
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    shutil.rmtree(CASES_ROOT, ignore_errors=True)
    sys.stdout.write(f"silver quality e2e: {report['passed']} of {report['total']} passed; report {out.relative_to(REPO_ROOT)}/report.json\n")
    return 0 if report["passed"] == report["total"] else 1


if __name__ == "__main__":
    sys.exit(main())
