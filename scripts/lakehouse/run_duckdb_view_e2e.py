"""E2E check of the DuckDB viewer against the real local catalog and the loaded bronze tables (failure modes 54 to 61, 95, 96).

Run from the repository root with Docker running:

    .venv/bin/python -m scripts.lakehouse.run_duckdb_view_e2e

It reads every bronze table through ``scripts/lakehouse/query.sh`` and through Spark and compares row counts and value
checksums, compares object counts with the bronze load reports, checks each table's data dictionary against its
published columns, reads a synthetic edge-value table written by Spark,
tries writes through the viewer and the reader principal, resets the reader credentials once, and scans every output
and the image for the stored secrets. The scratch table is created and purged in the ``duckdb_view_check`` namespace;
no bronze table is changed. The report in ``data/e2e/duckdb_view/`` holds outcomes and counts, never values or secrets.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from typing import Any

from scripts.lakehouse import bronze, catalog, memory_budget
from scripts.lakehouse.checksums import EDGE_ROWS
from scripts.process import run_command

REPO_ROOT = catalog.REPO_ROOT
QUERY = REPO_ROOT / "scripts/lakehouse/query.sh"
REPORTS = (
    REPO_ROOT / "data/e2e/bronze_load/hai_migration_report.json",
    REPO_ROOT / "data/e2e/bronze_load/report_20261002T151302Z.json",
    REPO_ROOT / "data/e2e/bronze_load/report_20261002T161250Z.json",
)
OUTPUT = REPO_ROOT / "data/e2e/duckdb_view"
IMAGE = "hai-analytics:duckdb1.5.6-dbt1.11.15"
TIMEOUT = 3600
# The same row checksum as checksums.checksum, for the table named in the checked_table variable.
CHECKSUM_SQL = (
    "SELECT count(*) AS rows, "
    "sum(('0x' || substr(d, 1, 8))::BIGINT) AS h1, sum(('0x' || substr(d, 9, 8))::BIGINT) AS h2 "
    "FROM (SELECT md5(array_to_string(list_transform(list_value(*COLUMNS(*)), lambda v: coalesce(v, '\\N')), '|')) AS d "
    "FROM (SELECT CASE WHEN typeof(COLUMNS(*)) LIKE '%[]' THEN to_json(COLUMNS(*))::VARCHAR ELSE COLUMNS(*)::VARCHAR END "
    "FROM query_table(getvariable('checked_table'))));"
)
# A dictionary's columns in order, then its bronze table's columns one row each: list() over DESCRIBE loses the
# order on wide tables, while plain rows keep it [95].
DICTIONARY_SQL = (
    "SELECT list(column_name ORDER BY position) AS described FROM query_table(getvariable('dictionary_table')); "
    "SELECT column_name FROM (DESCRIBE SELECT * FROM query_table(getvariable('checked_table')));"
)
OBJECTS_SQL = "SELECT count(DISTINCT _object_key) AS objects FROM query_table(getvariable('checked_table'));"
FIXTURE_SQL = "SELECT _object_key, value, note FROM lakehouse.duckdb_view_check.edge_values ORDER BY _object_key;"
outputs: list[str] = []


def run(program: str, args: list[str]) -> tuple[int, str]:
    """Run a command, keep its output for the secret scan and return the exit code and stdout."""
    result = run_command(program, args, cwd=REPO_ROOT, env=catalog.system_environment(), timeout=TIMEOUT)
    outputs.extend((result.stdout, result.stderr))
    return result.returncode, result.stdout


def duckdb(sql: str) -> tuple[int, list[dict[str, Any]]]:
    """Run SQL through the viewer with one JSON object per result row; return the exit code and the rows."""
    code, stdout = run("bash", [str(QUERY), "-cmd", ".mode jsonlines", "-c", sql])
    return code, [json.loads(line) for line in stdout.splitlines() if line.startswith("{")]


def spark_checksums(args: list[str]) -> dict[str, Any]:
    """Run the Spark checksum job and return its JSON line."""
    code, stdout = run(".venv/bin/python", ["-m", "scripts.lakehouse.catalog", "job", "checksums", "--", *args])
    if code:
        raise RuntimeError("the Spark checksum job failed")
    return dict(json.loads(stdout.strip().splitlines()[-1]))


def duckdb_checksum(table: str) -> dict[str, int]:
    """Return DuckDB's row count and checksum for one table."""
    code, rows = duckdb(f"SET VARIABLE checked_table = '{table}'; {CHECKSUM_SQL}")
    if code or not rows:
        raise RuntimeError(f"DuckDB could not checksum {table}")
    return {key: int(rows[0][key] or 0) for key in ("rows", "h1", "h2")}


def expected_objects() -> dict[str, tuple[int, int]]:
    """Return each loaded table's expected objects and rows from the bronze load reports."""
    expected = {}
    for path in REPORTS:
        for table, check in json.loads(path.read_text())["checks"].items():
            expected[table] = (int(check["objects"]), int(check["rows"]))
    return expected


def scenarios() -> dict[str, bool]:
    """Run every scenario and return its outcome."""
    results: dict[str, bool] = {}
    # [58] Setup is idempotent, and a lost reader secret is reset, never duplicated.
    run(".venv/bin/python", ["-m", "scripts.lakehouse.catalog", "up"])
    _, stdout = run(".venv/bin/python", ["-m", "scripts.lakehouse.catalog", "up"])
    results["setup_rerun_keeps_reader"] = "read-only principal already present" in stdout
    stored = catalog.read_env(catalog.SECRETS)
    catalog.write_private(catalog.SECRETS, {key: value for key, value in stored.items() if not key.startswith("POLARIS_READER_")})
    _, stdout = run(".venv/bin/python", ["-m", "scripts.lakehouse.catalog", "up"])
    rotated = catalog.read_env(catalog.SECRETS)
    results["lost_reader_secret_is_reset"] = "credentials reset" in stdout and rotated.get("POLARIS_READER_CLIENT_SECRET") not in (
        None,
        stored.get("POLARIS_READER_CLIENT_SECRET"),
    )
    # [56] Writes fail in the viewer and at the catalog; the table is unchanged.
    code, before = duckdb("SELECT count(*) AS n FROM cms_hai_national;")
    create_code, _ = duckdb("CREATE TABLE lakehouse.bronze.viewer_write_check (a INTEGER);")
    insert_code, _ = duckdb("INSERT INTO cms_hai_national SELECT * FROM cms_hai_national LIMIT 1;")
    _, after = duckdb("SELECT count(*) AS n FROM cms_hai_national;")
    results["viewer_refuses_writes"] = code == 0 and create_code != 0 and insert_code != 0 and before == after
    reader = catalog.token({"POLARIS_CLIENT_ID": rotated["POLARIS_READER_CLIENT_ID"], "POLARIS_CLIENT_SECRET": rotated["POLARIS_READER_CLIENT_SECRET"]})
    status, _ = catalog.request("POST", f"/api/catalog/v1/{catalog.CATALOG}/namespaces", reader, body={"namespace": ["reader_write_check"]})
    results["reader_principal_refused_by_catalog"] = status == 403
    status, _ = catalog.request("DELETE", f"/api/catalog/v1/{catalog.CATALOG}/namespaces/bronze/tables/cms_hai_national", reader)
    results["reader_cannot_drop_a_table"] = status == 403
    # [55] Edge values written by Spark read back unchanged.
    try:
        fixture = spark_checksums(["--fixture", "create"])["duckdb_view_check.edge_values"]
        code, rows = duckdb(FIXTURE_SQL)
        results["edge_values_unchanged"] = code == 0 and [(row["_object_key"], row["value"], row["note"]) for row in rows] == EDGE_ROWS
        results["edge_checksum_matches_spark"] = duckdb_checksum("lakehouse.duckdb_view_check.edge_values") == fixture
    finally:
        results["fixture_purged"] = bool(spark_checksums(["--fixture", "drop"]).get("dropped"))
    # [54] Every bronze table: DuckDB and Spark agree on rows and value checksums; objects and rows match the load reports.
    _, tables = duckdb("SHOW TABLES;")
    names = sorted(row["name"] for row in tables)
    spark = spark_checksums(["--tables", *names])
    expected = expected_objects()
    for name in names:
        viewed = duckdb_checksum(name)
        results[f"checksum_matches_spark_{name}"] = viewed == spark[f"bronze.{name}"]
        if name in expected:
            _, rows = duckdb(f"SET VARIABLE checked_table = '{name}'; {OBJECTS_SQL}")
            results[f"matches_load_report_{name}"] = (int(rows[0]["objects"]), viewed["rows"]) == expected[name]
    results["every_loaded_table_checked"] = set(expected) <= set(names)
    # [95] Every mapped table's dictionary reads through the read-only principal and lists exactly its published columns.
    mapped = [table["table"] for table in bronze.load_table_map(REPO_ROOT / bronze.TABLE_MAP)["tables"]]
    described = []
    for name in mapped:
        _, rows = duckdb(
            f"SET VARIABLE checked_table = 'lakehouse.bronze.{name}'; SET VARIABLE dictionary_table = 'lakehouse.bronze_dictionary.{name}'; {DICTIONARY_SQL}"
        )
        listed = next((row["described"] for row in rows if "described" in row), None)
        published = [row["column_name"] for row in rows if "column_name" in row and row["column_name"] not in bronze.LINEAGE]
        results[f"dictionary_matches_columns_{name}"] = bool(published) and listed == published
        described += [name] if listed else []
    results["every_mapped_table_has_a_dictionary"] = described == mapped
    # [96] Every SAS label in the column map is a description in the SAS table's dictionary.
    _, rows = duckdb(
        "SELECT (SELECT count(*) FROM lakehouse.bronze.column_map WHERE table_name = 'cms_ipps_sas' AND original_label IS NOT NULL) AS labels, "
        "(SELECT count(*) FROM lakehouse.bronze_dictionary.cms_ipps_sas WHERE description_source = 'publisher variable label') AS described;"
    )
    results["sas_labels_become_descriptions"] = bool(rows) and rows[0]["labels"] > 0 and rows[0]["labels"] == rows[0]["described"]
    # [57] [59] Secrets stay redacted and out of the image, the analytics environment and every output; extensions are local.
    _, rows = duckdb("SELECT count(*) FILTER (NOT secret_string LIKE '%redacted%') AS exposed FROM duckdb_secrets();")
    results["duckdb_secrets_redacted"] = rows == [{"exposed": 0}]
    _, history = run("docker", ["history", "--no-trunc", IMAGE])
    _, inspect = run("docker", ["image", "inspect", IMAGE])
    env = catalog.system_environment()
    config = catalog.compose("--profile", "query", "config", "--format", "json", env=env)
    analytics_env = json.loads(config)["services"]["analytics"]["environment"]
    results["root_credentials_not_in_analytics"] = "POLARIS_CLIENT_SECRET" not in analytics_env and "POSTGRES_PASSWORD" not in analytics_env
    inspection_plan = memory_budget.launch_plan()
    _, extensions = run("docker", ["run", "--rm", "--memory", str(inspection_plan.budget.free), "--entrypoint", "cat", IMAGE, "/home/analytics/extensions.csv"])
    results["extensions_installed_in_image"] = all(name in extensions for name in ("iceberg", "httpfs", "avro", "aws"))
    values = [value for key, value in catalog.read_env(catalog.SECRETS).items() if "SECRET" in key or "PASSWORD" in key]
    values += [stored.get("POLARIS_READER_CLIENT_SECRET", "")]
    scanned = "\n".join([*outputs, history, inspect])
    results["no_secret_in_outputs_or_image"] = bool(values) and not any(value and value in scanned for value in values)
    return results


def main() -> int:
    """Run the scenarios and write an outcomes-only report."""
    results = scenarios()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    OUTPUT.mkdir(parents=True, exist_ok=True)
    report = {"run_at_utc": stamp, "image": IMAGE, "results": results, "passed": all(results.values())}
    (OUTPUT / f"report_{stamp}.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    for name, outcome in sorted(results.items()):
        sys.stdout.write(f"{'PASS' if outcome else 'FAIL'} {name}\n")
    sys.stdout.write(f"{sum(results.values())} of {len(results)} scenarios passed\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
