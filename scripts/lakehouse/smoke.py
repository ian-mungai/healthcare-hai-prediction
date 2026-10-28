"""Round-trip check of the local catalog: create, write, read and purge a scratch table under lakehouse/.

Run from the repository root; the catalog script starts it inside the Spark container:

    .venv/bin/python -m scripts.lakehouse.catalog job smoke

It prints counts and the heap it ran with only. The scratch table is purged at the end, and a scratch table left by an interrupted run is purged
first, so reruns are safe.
"""

from __future__ import annotations

import argparse
import json
import sys

from scripts.lakehouse.namespace_binding import SMOKE_NAMESPACE, namespace_name

NAMESPACE = SMOKE_NAMESPACE


def main(argv: list[str] | None = None) -> int:
    """Create, write, read and purge one scratch table, then report what happened."""
    from scripts.lakehouse.session import job_memory, spark_session

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--namespace", type=namespace_name, default=NAMESPACE)
    args = parser.parse_args(argv)
    namespace = args.namespace
    table = f"{namespace}.round_trip"
    spark = spark_session("lakehouse-smoke", memory=job_memory())
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        spark.sql(f"CREATE TABLE {table} (id STRING, facility_id STRING) USING iceberg")
        spark.createDataFrame([("1", "010001"), ("2", "450056")], "id STRING, facility_id STRING").writeTo(table).append()
        rows = spark.table(table).orderBy("id").collect()
        location = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").where("col_name = 'Location'").collect()[0]["data_type"]
        snapshots = spark.table(f"{table}.snapshots").count()
        spark.sql(f"DROP TABLE {table} PURGE")
        spark.sql(f"DROP NAMESPACE {namespace}")
        remaining = [row[0] for row in spark.sql("SHOW NAMESPACES").collect()]
        result = {
            "rows_read": len(rows),
            "leading_zero_kept": len(rows) == 2 and rows[0]["facility_id"] == "010001",
            "location_under_lakehouse": "/lakehouse/" in location,
            "snapshots": snapshots,
            "namespace_removed": namespace not in remaining,
            "driver_memory": spark.conf.get("spark.driver.memory"),
        }
    finally:
        spark.stop()
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0 if all(result[key] for key in ("leading_zero_kept", "location_under_lakehouse", "namespace_removed")) and result["rows_read"] == 2 else 1


if __name__ == "__main__":
    raise SystemExit(main())
