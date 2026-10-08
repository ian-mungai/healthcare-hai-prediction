"""Round-trip check of the local catalog: create, write, read and purge a scratch table under lakehouse/.

Run from the repository root; the catalog script starts it inside the Spark container:

    .venv/bin/python -m scripts.lakehouse.catalog job smoke

It prints counts and the heap it ran with only. The scratch table is purged at the end, and a scratch table left by an interrupted run is purged
first, so reruns are safe.
"""

from __future__ import annotations

import json
import sys

from scripts.lakehouse.session import job_memory, spark_session

NAMESPACE = "smoke_check"
TABLE = f"{NAMESPACE}.round_trip"


def main() -> int:
    """Create, write, read and purge one scratch table, then report what happened."""
    spark = spark_session("lakehouse-smoke", memory=job_memory())
    spark.sql("DROP TABLE IF EXISTS smoke_check.round_trip PURGE")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS smoke_check")
    spark.sql("CREATE TABLE smoke_check.round_trip (id STRING, facility_id STRING) USING iceberg")
    spark.createDataFrame([("1", "010001"), ("2", "450056")], "id STRING, facility_id STRING").writeTo(TABLE).append()
    rows = spark.table(TABLE).orderBy("id").collect()
    location = spark.sql("DESCRIBE TABLE EXTENDED smoke_check.round_trip").where("col_name = 'Location'").collect()[0]["data_type"]
    snapshots = spark.table(f"{TABLE}.snapshots").count()
    spark.sql("DROP TABLE smoke_check.round_trip PURGE")
    spark.sql("DROP NAMESPACE smoke_check")
    remaining = [row[0] for row in spark.sql("SHOW NAMESPACES").collect()]
    result = {
        "rows_read": len(rows),
        "leading_zero_kept": rows[0]["facility_id"] == "010001",
        "location_under_lakehouse": "/lakehouse/" in location,
        "snapshots": snapshots,
        "namespace_removed": NAMESPACE not in remaining,
        # The heap the job ran with, to compare with the one the launcher computed [486].
        "driver_memory": spark.conf.get("spark.driver.memory"),
    }
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    spark.stop()
    return 0 if all(result[key] for key in ("leading_zero_kept", "location_under_lakehouse", "namespace_removed")) and result["rows_read"] == 2 else 1


if __name__ == "__main__":
    raise SystemExit(main())
