"""Exercise real namespace arguments with synthetic Spark and boto3 service boundaries."""

from __future__ import annotations

import contextlib
import io
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import patch


class Frame:
    """The Spark frame operations used by the namespace interface checks."""

    def __init__(self, session: Spark, rows: list[Any]) -> None:
        self.session = session
        self.rows = rows
        self.target = ""
        self.columns: list[str] = ["original_label"]

    def collect(self) -> list[Any]:
        return self.rows

    def where(self, condition: str) -> Frame:
        return self

    def orderBy(self, name: str) -> Frame:
        return self

    def count(self) -> int:
        return len(self.rows)

    def coalesce(self, size: int) -> Frame:
        return self

    def writeTo(self, target: str) -> Frame:
        self.target = target
        self.session.writes.append(target)
        return self

    def using(self, name: str) -> Frame:
        return self

    def tableProperty(self, name: str, value: str) -> Frame:
        return self

    def append(self) -> None:
        self.session.tables[self.target] = self.rows

    def createOrReplace(self) -> None:
        self.session.tables[self.target] = self.rows

    def createOrReplaceTempView(self, name: str) -> None:
        return None


class Spark:
    """A minimal Spark service replacement that records target names and scratch cleanup."""

    def __init__(self) -> None:
        self.queries: list[str] = []
        self.writes: list[str] = []
        self.tables: dict[str, list[Any]] = {}
        self.namespaces: set[str] = set()
        self.catalog = self
        self.conf = self
        self.stopped = False

    def get(self, name: str) -> str:
        return "1g"

    def sql(self, query: str) -> Frame:
        self.queries.append(query)
        words = query.split()
        if query.startswith("CREATE NAMESPACE"):
            self.namespaces.add(words[-1])
        elif query.startswith("DROP NAMESPACE"):
            self.namespaces.discard(words[-1])
        elif query.startswith("DROP TABLE"):
            self.tables.pop(words[-2], None)
        elif query.startswith("SHOW TABLES"):
            return Frame(self, [{"tableName": "sample"}])
        elif query == "SHOW NAMESPACES":
            return Frame(self, [(name,) for name in sorted(self.namespaces)])
        elif query.startswith("DESCRIBE"):
            return Frame(self, [{"data_type": "s3://example-data-bucket/lakehouse/smoke_check_e2e/round_trip"}])
        return Frame(self, [])

    def table(self, name: str) -> Frame:
        if name.endswith(".snapshots"):
            return Frame(self, [1])
        return Frame(self, self.tables.get(name, []))

    def createDataFrame(self, rows: list[Any], schema: str) -> Frame:
        names = [item.strip().split()[0] for item in schema.split(",")]
        converted = [dict(zip(names, row, strict=True)) for row in rows] if schema == "id STRING, facility_id STRING" else rows
        return Frame(self, converted)

    def tableExists(self, name: str) -> bool:
        return name in self.tables

    def currentDatabase(self) -> str:
        return ""

    def setCurrentDatabase(self, name: str) -> None:
        return None

    def stop(self) -> None:
        self.stopped = True


def namespace_arguments(subject: ModuleType, journal: ModuleType, out: Path) -> dict[str, Any]:
    """Run the actual dictionary, removal, smoke and fixture entry points with bound namespace arguments."""
    from scripts.lakehouse import bronze, checksums, dictionary
    from scripts.lakehouse.bronze_writer_cases import require

    spark = Spark()
    with patch.object(dictionary, "describe", return_value=[("synthetic",)]):
        dictionary.build(spark, {"tables": [{"table": "sample"}]}, ["sample"], "bronze_e2e", dictionary_namespace="bronze_dictionary_e2e")
    require(spark.writes == ["bronze_dictionary_e2e.sample"], "dictionary build wrote outside the bound namespace")
    bronze.drop_removed(spark, ["sample"], "bronze_e2e", dictionary_namespace="bronze_dictionary_e2e")
    require("DROP TABLE IF EXISTS bronze_dictionary_e2e.sample PURGE" in spark.queries, "removal did not purge the bound dictionary")
    bronze.ensure_table(spark, "bronze_e2e", "sample", "text_lines")
    require(any(query.startswith("CREATE NAMESPACE IF NOT EXISTS bronze_e2e") for query in spark.queries), "bronze twin namespace was refused")
    session = SimpleNamespace(spark_session=lambda *args, **kwargs: spark, job_memory=lambda: "1g")
    with patch.dict("sys.modules", {"scripts.lakehouse.session": session}):
        from scripts.lakehouse import smoke

        with contextlib.redirect_stdout(io.StringIO()):
            status = smoke.main(["--namespace", "smoke_check_e2e"])
            checksums.main(["--fixture", "drop", "--fixture-namespace", "duckdb_view_check_e2e"])
            spark.tables["bronze_e2e.sample"] = [("synthetic",)]
            spark.tables["bronze_dictionary_e2e.sample"] = [("synthetic",)]
            with (
                patch.object(bronze, "load_table_map", return_value={"namespace": "bronze", "tables": [{"table": "sample"}]}),
                patch.object(bronze, "REPORT_ROOT", out / "qualification_removal"),
            ):
                removed = bronze.main(
                    ["--drop-removed", "--namespace", "bronze_e2e", "--dictionary-namespace", "bronze_dictionary_e2e", "--qualification-tables", "sample"]
                )
                before = len(spark.queries)
                with contextlib.redirect_stderr(io.StringIO()):
                    refused = bronze.main(["--drop-removed", "--qualification-tables", "sample"])
    require(
        removed == 0 and "bronze_e2e.sample" not in spark.tables and "bronze_dictionary_e2e.sample" not in spark.tables,
        "qualification removal did not reach both loaded twins",
    )
    require(refused == 1 and len(spark.queries) == before, "qualification table selection reached a canonical mutation")
    require(status == 0 and spark.stopped, "bound smoke did not finish and stop its session")
    require("smoke_check_e2e.round_trip" in spark.writes, "smoke wrote outside its bound twin")
    require(not any(query.startswith("DROP TABLE") and "duckdb_view_check.edge_values" in query for query in spark.queries), "fixture drop purged production")
    require("DROP TABLE IF EXISTS duckdb_view_check_e2e.edge_values PURGE" in spark.queries, "fixture drop did not use its twin")
    return {
        "real_entry_points": ["dictionary.build", "bronze.drop_removed", "bronze.ensure_table", "smoke.main", "checksums.main"],
        "substitutions": ["Spark service", "dictionary row generation"],
    }


def task_credentials(subject: ModuleType, journal: ModuleType, out: Path) -> dict[str, Any]:
    """Reach the real S3 adapter and require task credentials without reading a local AWS profile."""
    from scripts.lakehouse import bronze
    from scripts.lakehouse.bronze_writer_cases import require

    with patch.dict("os.environ", {"AWS_REGION": "us-west-2"}, clear=True), patch("boto3.Session") as session:
        bronze.S3Storage(task_role=True)
        require(session.call_args.kwargs == {"region_name": "us-west-2"}, "AWS source adapter selected a local profile")
    return {"credential_source": "task default chain", "source_adapter_reached": True}
