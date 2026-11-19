"""Test the declared bronze consumer columns against the column lists of the really loaded tables, before a live run. Counts and names only: no row is read.

Export the column lists of the bronze Glue mirror (read-only, names only) with the owner profile:

    aws glue get-tables --database-name bronze --query 'TableList[].{Name:Name,Columns:StorageDescriptor.Columns[].Name,Partitions:PartitionKeys[].Name}' \\
        --output json > bronze_glue_columns.json

The file is a JSON list of ``{"Name": table, "Columns": [column names], "Partitions": [column names] or null}``. The full output of ``aws glue get-tables``
(``{"TableList": [{"Name": ..., "StorageDescriptor": {"Columns": [{"Name": ...}]}, "PartitionKeys": [{"Name": ...}]}]}``) is read too. Run:

    .venv/bin/python -m scripts.lakehouse.bronze_consumer_precheck bronze_glue_columns.json [--row-counts counts.json]

``counts.json`` is optional, ``{"table": row count}``: a table with a count of 0 is passed as empty, the way the bronze load passes it; without a count every
table is checked. For each declared table the report gives ``ok``, ``missing`` (the declared columns that the loaded table lacks, by name, with the models that
read them), ``any`` (declared as read through any column: nothing to check), ``absent`` (declared but not in the export: not loaded in the mirror, nothing is
checked, and silver would stop on it by itself if it reads it) or ``empty``. Tables in the export that are not declared are counted. The exit code is 1 only
when a declared column is missing.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from scripts.lakehouse import bronze_consumer_columns as columns

ROOT = Path(__file__).resolve().parents[2]


def read_export(path: Path) -> dict[str, list[str]]:
    """Table to column names (partition columns included, names lowered) from either export shape."""
    document = json.loads(path.read_text())
    tables = document["TableList"] if isinstance(document, dict) else document
    found: dict[str, list[str]] = {}
    for table in tables:
        if "StorageDescriptor" in table:
            names = [column["Name"] for column in table["StorageDescriptor"].get("Columns") or []]
            names += [column["Name"] for column in table.get("PartitionKeys") or []]
        else:
            names = [*(table.get("Columns") or []), *(table.get("Partitions") or [])]
        found[str(table["Name"]).lower()] = [str(name).lower() for name in names]
    return found


def report(contract: Mapping[str, columns.Declaration], export: Mapping[str, list[str]], rows: Mapping[str, int] | None = None) -> dict[str, Any]:
    """Per declared table: its status and the missing columns; plus the counts."""
    tables: dict[str, Any] = {}
    for table in sorted(contract):
        if table not in export:
            tables[table] = {"status": "absent"}
            continue
        known = rows is not None and table in rows
        outcome = columns.check_loaded(table, True, export[table], rows[table] if rows is not None and known else 1, contract)
        entry: dict[str, Any] = {"status": outcome.status}
        if outcome.missing:
            entry["status"] = "missing"
            entry["missing"] = {name: list(models) for name, models in outcome.missing.items()}
        tables[table] = entry
    counts: dict[str, int] = {}
    for entry in tables.values():
        counts[entry["status"]] = counts.get(entry["status"], 0) + 1
    return {"declared": len(contract), "loaded_not_declared": sorted(set(export) - set(contract)), "counts": counts, "tables": tables}


def main(argv: list[str] | None = None) -> int:
    """Print the report and return 1 when a declared column is missing from a loaded table."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("export", type=Path, help="the Glue column export (see the module docstring)")
    parser.add_argument("--row-counts", type=Path, default=None, help='optional JSON {"table": row count}')
    parser.add_argument("--contract", type=Path, default=ROOT / columns.CONTRACT)
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    rows = {str(key).lower(): int(value) for key, value in json.loads(args.row_counts.read_text()).items()} if args.row_counts else None
    result = report(columns.load(args.contract), read_export(args.export), rows)
    counts = ", ".join(f"{status} {count}" for status, count in sorted(result["counts"].items()))
    sys.stdout.write(f"declared tables: {result['declared']}; {counts}; loaded but not declared: {len(result['loaded_not_declared'])}\n")
    for table, entry in result["tables"].items():
        if entry["status"] == "missing":
            for name, models in entry["missing"].items():
                sys.stdout.write(f"MISSING {table}: {name} (read by {', '.join(models[:3])}{' and more' if len(models) > 3 else ''})\n")
    absent = [table for table, entry in result["tables"].items() if entry["status"] == "absent"]
    if absent:
        sys.stdout.write(f"absent ({len(absent)}, declared but not in the export, nothing checked): {', '.join(absent)}\n")
    return 1 if result["counts"].get("missing") else 0


if __name__ == "__main__":
    sys.exit(main())
