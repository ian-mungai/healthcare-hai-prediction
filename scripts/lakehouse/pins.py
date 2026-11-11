"""Pin the bronze snapshot every table the silver build reads is at before a build, and check after the build that none moved.

Run from the repository root; the catalog script asks the launcher to run it as a ``spark_read`` attempt with the read-only principal:

    .venv/bin/python -m scripts.lakehouse.catalog job pins -- pin --run <run_id>      # write runs/<run_id>/pins.json
    .venv/bin/python -m scripts.lakehouse.catalog job pins -- verify --run <run_id>   # exit 1 when a table moved

``pins.json`` is a flat JSON object of bronze table to the snapshot ID of its ``main`` branch, written once and
atomically in the run directory. Passing it to the build marker (failure mode 894) is deferred to wave 2 with the
launcher wiring (amendment A28). The job only reads catalog metadata. Failure modes 890 to 894 are in
plans/wave1_20261011/gate/pins_failure_modes.md.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

TABLE_MAP = Path("config/lakehouse/bronze_tables.json")
SOURCES = Path("dbt/models/silver/staging/_bronze__sources.yml")
RUNS = Path("data/analytics/dbt/runs")
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,127}")
SOURCE_TABLE = re.compile(r"\s*- name:\s*(\S+)\s*")
# Iceberg's refs metadata table; every name is checked against IDENTIFIER first.
REFS_SQL = "SELECT snapshot_id FROM {catalog}.{namespace}.{table}.refs WHERE name = 'main'"


class PinError(RuntimeError):
    """A table is missing or has no snapshot, a pin exists already or a pins file is malformed."""


def declared_sources(text: str) -> set[str]:
    """The table names under every ``tables:`` key of a dbt sources file, read with the standard library (the Spark image has no YAML parser)."""
    names: set[str] = set()
    block_indent: int | None = None
    item_indent: int | None = None
    for line in text.splitlines():
        content = line.strip()
        if not content or content.startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        if block_indent is not None and indent <= block_indent:
            block_indent = item_indent = None
        if block_indent is None:
            block_indent = indent if content == "tables:" else None
            continue
        item_indent = indent if item_indent is None else item_indent
        match = SOURCE_TABLE.fullmatch(line) if indent == item_indent else None
        if match:
            names.add(match.group(1).strip("'\""))
    return names


def pinned_tables(mapped: set[str], declared: set[str]) -> list[str]:
    """The pin's table list: the mapped tables that a silver source declares, and the file metadata table."""
    return sorted((mapped & declared) | {"file_metadata"})


def mapped_tables(table_map: Path = TABLE_MAP, sources: Path | None = None) -> list[str]:
    """The bronze tables to pin [891]: those in the committed table map that a silver dbt source declares, and the file metadata table.

    A table that no silver source declares is not read by silver, so it is neither pinned nor needed. A declared table that bronze lacks still stops the pin.
    """
    sources = sources or table_map.parents[2] / SOURCES
    document = json.loads(table_map.read_text())
    try:
        declared = declared_sources(sources.read_text())
    except OSError as exc:
        raise PinError(f"{sources} cannot be read: {type(exc).__name__}") from exc
    if not declared:
        raise PinError(f"{sources} declares no bronze tables")
    tables = pinned_tables({entry["table"] for entry in document["tables"]}, declared)
    bad = [table for table in tables if not IDENTIFIER.fullmatch(table)]
    if bad:
        raise PinError(f"table map names tables that are not plain identifiers: {bad}")
    return tables


def current_snapshots(spark: Any, catalog: str, namespace: str, tables: list[str]) -> dict[str, str]:
    """Each table's main-branch snapshot ID, read from Iceberg's refs metadata table; read-only [891] [893]."""
    # pyspark exists only in the Spark image; it is imported by name so the launcher can import this module's table map without it.
    AnalysisException = importlib.import_module("pyspark.errors").AnalysisException

    snapshots = {}
    for table in tables:
        try:
            if not all(IDENTIFIER.fullmatch(name) for name in (catalog, namespace, table)):
                raise PinError(f"{catalog}.{namespace}.{table} is not a plain identifier")
            rows = spark.sql(REFS_SQL.format(catalog=catalog, namespace=namespace, table=table)).collect()
        except AnalysisException as exc:
            raise PinError(f"bronze table {table} cannot be read: {type(exc).__name__}") from exc
        if len(rows) != 1 or rows[0]["snapshot_id"] is None:
            raise PinError(f"bronze table {table} has no current snapshot")
        snapshots[table] = str(rows[0]["snapshot_id"])
    return snapshots


def write_once(path: Path, snapshots: dict[str, str]) -> str:
    """Publish the pins once and return the SHA-256 of the bytes written; a hard link from the fsynced temporary file fails when pins exist [892] [904]."""
    if path.exists():
        raise PinError(f"{path} exists; a run is pinned once")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{os.urandom(4).hex()}.tmp")
    text = json.dumps(snapshots, indent=2, sort_keys=True) + "\n"
    try:
        with temporary.open("x") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            raise PinError(f"{path} exists; a run is pinned once") from exc
    finally:
        temporary.unlink(missing_ok=True)
    return hashlib.sha256(text.encode()).hexdigest()


def read_pins(path: Path) -> dict[str, str]:
    """A pins file's table-to-snapshot object; anything else is refused [892]."""
    try:
        document = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise PinError(f"{path} is not a readable JSON pins file: {exc}") from exc
    if not isinstance(document, dict) or not document or not all(isinstance(value, str) and value.isdigit() for value in document.values()):
        raise PinError(f"{path} must be a non-empty JSON object of bronze table to snapshot ID")
    return {str(table): value for table, value in document.items()}


def differences(pinned: dict[str, str], current: dict[str, str]) -> list[str]:
    """Each table that moved, appeared or disappeared since the pin [890]."""
    changed = [f"{table}: {pinned[table]} to {current[table]}" for table in sorted(pinned.keys() & current.keys()) if pinned[table] != current[table]]
    changed += [f"{table}: not pinned" for table in sorted(current.keys() - pinned.keys())]
    changed += [f"{table}: no longer present" for table in sorted(pinned.keys() - current.keys())]
    return changed


def run_path(run_id: str) -> Path:
    """The run's pins file in its run directory."""
    if not RUN_ID.fullmatch(run_id):
        raise PinError(f"run ID {run_id!r} is not of the form 20261011T090000Z-0a1b2c3d")
    return RUNS / run_id / "pins.json"


def main() -> int:
    """Pin or verify for one run; prints counts and table names only."""
    # Imported by name: session.py needs pyspark, which exists only in the Spark image.
    session = importlib.import_module("scripts.lakehouse.session")

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("pin", "verify"))
    parser.add_argument("--run", required=True)
    args = parser.parse_args()
    try:
        path = run_path(args.run)
        tables = mapped_tables()
        spark = session.spark_session("bronze-pins", memory=session.job_memory())
        try:
            current = current_snapshots(spark, session.CATALOG, "bronze", tables)
        finally:
            spark.stop()
        if args.command == "pin":
            digest = write_once(path, current)
            sys.stdout.write(json.dumps({"pinned": len(current), "path": str(path), "sha256": digest}) + "\n")
            return 0
        changed = differences(read_pins(path), current)
        sys.stdout.write(json.dumps({"tables": len(current), "changed": changed}) + "\n")
        return 1 if changed else 0
    except PinError as error:
        sys.stderr.write(f"pins: {error}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
