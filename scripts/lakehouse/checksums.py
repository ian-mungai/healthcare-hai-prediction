"""Spark-side checksums of bronze tables, and a synthetic edge-value table, for the DuckDB viewer's E2E check.

Run from the repository root; the catalog script starts it inside the Spark container:

    .venv/bin/python -m scripts.lakehouse.catalog job checksums -- --tables cms_hai_state cms_hai_national
    .venv/bin/python -m scripts.lakehouse.catalog job checksums -- --fixture create
    .venv/bin/python -m scripts.lakehouse.catalog job checksums -- --fixture drop

Each table's checksum is computed the same way in DuckDB (``run_duckdb_view_e2e.CHECKSUM_SQL``): every column cast to
text (arrays as JSON) with nulls marked ``\\N``, joined with ``|`` in schema order, hashed with MD5, and two 32-bit slices summed. It
prints counts and sums only, never a value. The fixture lives in its own scratch namespace and is replaced or purged on
each call, so reruns are safe.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

FIXTURE_NAMESPACE = "duckdb_view_check"
FIXTURE = f"{FIXTURE_NAMESPACE}.edge_values"
# Values the viewer must return unchanged: empty strings, nulls, line breaks, quotes, commas, padding, leading zeros
# and characters decoded from Windows-1252 (failure mode 55).
EDGE_ROWS = [
    ("1", "", None),
    ("2", "line one\r\nline two", 'say "hi", then go'),
    ("3", "  padded  ", "007"),
    ("4", "Saint Mary’s – East", "café  no-break"),
]


def checksum(spark: Any, table: str) -> dict[str, int]:
    """Count rows and sum two 32-bit slices of each row's MD5 in Spark."""
    from pyspark.sql import functions as F

    frame = spark.table(table)
    # Array columns (Excel cells) render as JSON in both engines; every other column as plain text.
    text = [F.to_json(F.col(field.name)) if field.dataType.typeName() == "array" else F.col(field.name).cast("string") for field in frame.schema.fields]
    digest = F.md5(F.concat_ws("|", *[F.coalesce(value, F.lit("\\N")) for value in text]))
    row = frame.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.conv(F.substring(digest, 1, 8), 16, 10).cast("bigint")).alias("h1"),
        F.sum(F.conv(F.substring(digest, 9, 8), 16, 10).cast("bigint")).alias("h2"),
    ).collect()[0]
    return {key: int(row[key] or 0) for key in ("rows", "h1", "h2")}


def main() -> int:
    """Print checksums for the named tables, or create or drop the fixture."""
    # Imported here so the host-side E2E can import EDGE_ROWS without PySpark installed.
    from scripts.lakehouse.session import job_memory, spark_session

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tables", nargs="*", default=[])
    parser.add_argument("--fixture", choices=["create", "drop"])
    args = parser.parse_args()
    spark = spark_session("bronze-checksums", memory=job_memory())
    result: dict[str, Any] = {}
    if args.fixture == "create":
        spark.sql("DROP TABLE IF EXISTS duckdb_view_check.edge_values PURGE")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS duckdb_view_check")
        frame = spark.createDataFrame(EDGE_ROWS, "_object_key STRING, value STRING, note STRING")
        frame.writeTo(FIXTURE).using("iceberg").create()
        result[FIXTURE] = checksum(spark, FIXTURE)
    elif args.fixture == "drop":
        spark.sql("DROP TABLE IF EXISTS duckdb_view_check.edge_values PURGE")
        spark.sql("DROP NAMESPACE IF EXISTS duckdb_view_check")
        result["dropped"] = FIXTURE_NAMESPACE not in [row[0] for row in spark.sql("SHOW NAMESPACES").collect()]
    for name in args.tables:
        result[f"bronze.{name}"] = checksum(spark, f"bronze.{name}")
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
