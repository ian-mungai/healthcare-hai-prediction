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

from scripts.lakehouse.namespace_binding import BRONZE_NAMESPACE, FIXTURE_NAMESPACE, namespace_name

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


def main(argv: list[str] | None = None) -> int:
    """Print checksums for the named tables, or create or drop the fixture."""
    # Imported here so the host-side E2E can import EDGE_ROWS without PySpark installed.
    from scripts.lakehouse.session import job_memory, spark_session

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tables", nargs="*", default=[], type=namespace_name)
    parser.add_argument("--fixture", choices=["create", "drop"])
    parser.add_argument("--namespace", type=namespace_name, default=BRONZE_NAMESPACE)
    parser.add_argument("--fixture-namespace", type=namespace_name, default=FIXTURE_NAMESPACE)
    args = parser.parse_args(argv)
    fixture = f"{args.fixture_namespace}.edge_values"
    spark = spark_session("bronze-checksums", memory=job_memory())
    result: dict[str, Any] = {}
    try:
        if args.fixture == "create":
            spark.sql(f"DROP TABLE IF EXISTS {fixture} PURGE")
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.fixture_namespace}")
            frame = spark.createDataFrame(EDGE_ROWS, "_object_key STRING, value STRING, note STRING")
            frame.writeTo(fixture).using("iceberg").create()
            result[fixture] = checksum(spark, fixture)
        elif args.fixture == "drop":
            spark.sql(f"DROP TABLE IF EXISTS {fixture} PURGE")
            spark.sql(f"DROP NAMESPACE IF EXISTS {args.fixture_namespace}")
            result["dropped"] = args.fixture_namespace not in [row[0] for row in spark.sql("SHOW NAMESPACES").collect()]
        for name in args.tables:
            result[f"{args.namespace}.{name}"] = checksum(spark, f"{args.namespace}.{name}")
    finally:
        spark.stop()
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
