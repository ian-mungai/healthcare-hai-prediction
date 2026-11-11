from __future__ import annotations

import json
import re
import sys
import types
from pathlib import Path
from typing import Any

import pytest
import yaml

from scripts.lakehouse import pins
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
SOURCES_FILE = "dbt/models/silver/staging/_bronze__sources.yml"
TABLE_MAP_FILE = "config/lakehouse/bronze_tables.json"
SOURCES_TEXT = """version: 2

sources:
  - name: bronze
    description: Bronze tables.
    database: lakehouse
    schema: bronze
    tables:
      - name: file_metadata
        columns:
          - name: generation
            data_tests: [not_null]
      - name: alpha
        description: Declared and mapped.
        columns:
          - name: _object_key
      - name: beta
"""


class AnalysisException(Exception):
    """Stands in for pyspark.errors.AnalysisException, which exists only in the Spark image."""


class FakeSpark:
    """A catalog of bronze tables with their main snapshot IDs; a table that is not in it raises like Iceberg does for a table that does not exist."""

    def __init__(self, snapshots: dict[str, str]) -> None:
        self.snapshots = snapshots
        self.read: list[str] = []

    def sql(self, statement: str) -> Any:
        table = re.search(r"bronze\.([a-z0-9_]+)\.refs", statement).group(1)
        self.read.append(table)
        if table not in self.snapshots:
            raise AnalysisException(table)
        return types.SimpleNamespace(collect=lambda: [{"snapshot_id": self.snapshots[table]}])


@pytest.fixture(autouse=True)
def fake_pyspark(monkeypatch: pytest.MonkeyPatch) -> None:
    package = types.ModuleType("pyspark")
    errors = types.ModuleType("pyspark.errors")
    errors.AnalysisException = AnalysisException
    monkeypatch.setitem(sys.modules, "pyspark", package)
    monkeypatch.setitem(sys.modules, "pyspark.errors", errors)


def checkout(root: Path, tables: list[str], sources: str = SOURCES_TEXT) -> Path:
    """A small checkout with a table map and a sources file; returns the table map path."""
    (root / "config/lakehouse").mkdir(parents=True)
    (root / "dbt/models/silver/staging").mkdir(parents=True)
    table_map = root / TABLE_MAP_FILE
    table_map.write_text(json.dumps({"tables": [{"table": name} for name in tables]}))
    (root / SOURCES_FILE).write_text(sources)
    return table_map


def test_a_bronze_table_no_silver_source_declares_is_not_pinned(tmp_path: Path) -> None:
    table_map = checkout(tmp_path, ["alpha", "beta", "gamma_unused"])
    tables = pins.mapped_tables(table_map)
    check(tables == ["alpha", "beta", "file_metadata"], "the pin lists the declared tables and the file metadata, not the undeclared one")
    spark = FakeSpark({"alpha": "1", "beta": "2", "file_metadata": "3"})
    check(
        pins.current_snapshots(spark, "hai_lakehouse", "bronze", tables) == {"alpha": "1", "beta": "2", "file_metadata": "3"},
        "every listed table has its snapshot although the undeclared table is absent",
    )
    check("gamma_unused" not in spark.read, "the undeclared table is never read")


def test_a_declared_source_that_is_missing_still_stops_the_pin(tmp_path: Path) -> None:
    tables = pins.mapped_tables(checkout(tmp_path, ["alpha", "beta", "gamma_unused"]))
    spark = FakeSpark({"alpha": "1", "file_metadata": "3", "gamma_unused": "9"})
    with pytest.raises(pins.PinError, match="bronze table beta cannot be read"):
        pins.current_snapshots(spark, "hai_lakehouse", "bronze", tables)


def test_the_pin_has_no_table_the_sources_file_leaves_out_even_when_the_map_lists_it(tmp_path: Path) -> None:
    only_alpha = SOURCES_TEXT.replace("      - name: beta\n", "")
    check(
        pins.mapped_tables(checkout(tmp_path, ["alpha", "beta"], only_alpha)) == ["alpha", "file_metadata"],
        "a table the sources file leaves out is not pinned",
    )


def test_a_missing_or_empty_sources_file_refuses_the_pin(tmp_path: Path) -> None:
    table_map = checkout(tmp_path / "empty", ["alpha"], "version: 2\nsources: []\n")
    with pytest.raises(pins.PinError, match="declares no bronze tables"):
        pins.mapped_tables(table_map)
    absent = checkout(tmp_path / "absent", ["alpha"])
    (tmp_path / "absent" / SOURCES_FILE).unlink()
    with pytest.raises(pins.PinError, match="cannot be read"):
        pins.mapped_tables(absent)


def test_the_committed_sources_file_reads_the_same_with_and_without_a_yaml_parser() -> None:
    text = (ROOT / SOURCES_FILE).read_text()
    document = yaml.safe_load(text)
    expected = {table["name"] for source in document["sources"] for table in source["tables"]}
    check(pins.declared_sources(text) == expected, "the standard-library reader finds the same tables as a YAML parser")
    check(len(expected) > 50, "the committed sources file declares its tables")


def test_the_committed_pin_set_is_the_declared_sources_the_table_map_lists() -> None:
    declared = pins.declared_sources((ROOT / SOURCES_FILE).read_text())
    mapped = {entry["table"] for entry in json.loads((ROOT / TABLE_MAP_FILE).read_text())["tables"]}
    tables = pins.mapped_tables(ROOT / TABLE_MAP_FILE)
    check(set(tables) == (declared & mapped) | {"file_metadata"}, "the pin is the declared sources the map lists, plus the file metadata")
    check(tables == sorted(tables), "the pin list is sorted")
    check("hcai_util_workbook_sheet_rows" in mapped, "the table map lists the HCAI utilization sheet table")
    check("hcai_util_workbook_sheet_rows" not in tables, "no silver source declares it, so it is not pinned")
    check(len(tables) < len(mapped | {"file_metadata"}), "the pin is smaller than the table map")
