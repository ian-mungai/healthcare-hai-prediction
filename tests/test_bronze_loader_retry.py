"""The loader's identical retry writes nothing: a rerun on unchanged records reaches no table write and no data load (inert Spark, real bronze.main)."""

from __future__ import annotations

import copy
import io
import sys
import types
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any

import pytest

from scripts.lakehouse import bronze, bronze_metadata, bronze_pin, dictionary, session
from tests import test_bronze_completion_records as f
from tests.support import check


class Expression:
    def isin(self, values: Any) -> Expression:
        return self


def test_an_identical_retry_of_a_reversion_run_makes_no_write_and_no_data_load(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HAI_READ_RAW_ROOT", "true")
    lake = f.Lake()
    first, _, previous = f.first_then_second(lake, tmp_path)
    f.write_completion(lake, first, f.UNIT_A, f.NEW, run=f.REVERTED)
    config = f.config(tmp_path)
    inventory = bronze_pin.build_inventory(lake, f.BUCKET, config, f.BOTH)
    inventory_file = tmp_path / "inventory.json"
    inventory_file.write_bytes(bronze_pin.inventory_bytes(inventory))
    state = {"rows": copy.deepcopy(previous)}
    writes: list[str] = []
    loads: list[int] = []
    metadata_target = f"bronze.{bronze_metadata.TABLE}"

    class Writer:
        def __init__(self, rows: list, target: str) -> None:
            self.rows, self.target = rows, target

        def option(self, *args: Any) -> Writer:
            return self

        def overwrite(self, predicate: Any) -> None:
            writes.append(self.target)
            if self.target == metadata_target:
                state["rows"] = [dict(zip(bronze_metadata.FIELDS, row, strict=True)) for row in self.rows]

    class Frame:
        def __init__(self, rows: list) -> None:
            self.rows = rows

        def writeTo(self, target: str) -> Writer:
            return Writer(self.rows, target)

    class Spark:
        def sql(self, statement: str) -> None:
            return None

        def createDataFrame(self, rows: list, schema: Any) -> Frame:
            return Frame(rows)

        def stop(self) -> None:
            return None

    def load_objects(spark: Any, inputs: list[dict], *args: Any) -> dict:
        loads.append(len(inputs))
        return {}

    functions = types.ModuleType("pyspark.sql.functions")
    functions.__dict__.update(col=lambda name: Expression(), lit=lambda value: Expression())
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", types.ModuleType("pyspark.sql"))
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions)
    monkeypatch.setattr(bronze, "S3Storage", lambda **kwargs: lake)
    monkeypatch.setattr(bronze, "load_table_map", lambda: config)
    monkeypatch.setattr(bronze, "load_retired", bronze.RetiredObjects)
    monkeypatch.setattr(bronze, "REPORT_ROOT", tmp_path / "reports")
    monkeypatch.setattr(session, "job_memory", lambda: "1g")
    monkeypatch.setattr(session, "spark_session", lambda *args, **kwargs: Spark())
    monkeypatch.setattr(bronze_metadata, "read", lambda *args: copy.deepcopy(state["rows"]))
    monkeypatch.setattr(bronze_metadata, "snapshot", lambda *args: 99)
    monkeypatch.setattr(bronze_metadata, "write_bootstrap", lambda *args: f.digest(b"index"))
    monkeypatch.setattr(bronze, "load_objects", load_objects)
    monkeypatch.setattr(bronze, "prune", lambda *args: {})
    monkeypatch.setattr(bronze, "verify", lambda *args: {name: {"passed": True} for name in f.BOTH})
    monkeypatch.setattr(dictionary, "build", lambda *args, **kwargs: {})

    def run() -> tuple[int, str]:
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = bronze.main(["--tables", ",".join(f.BOTH), "--bucket", f.BUCKET, "--task-role", "--inventory", str(inventory_file)])
        return status, stderr.getvalue()

    status, error = run()
    check(status == 0, error)
    check(metadata_target in writes and "bronze.stored_copies" in writes and loads == [2], f"the reversion run writes and loads: {writes} {loads}")
    published = copy.deepcopy(state["rows"])
    writes.clear()
    loads.clear()
    status, error = run()
    check(status == 0, error)
    check(writes == [] and loads == [], f"an identical retry writes and loads nothing: {writes} {loads}")
    check(state["rows"] == published, "the metadata generation is the one the first run published")
