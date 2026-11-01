"""Validate a build file's suites and reconcile every gold fact with its silver model, inside the quality image.

Runs the Great Expectations suites as ``scripts.lakehouse.silver_quality`` does, then fingerprints each gold fact and its silver model over the contract's
columns and compares row counts and content (``gold_checks.reconcile_statements`` and ``gold_checks.reconcile_problems``). The result file is the suites'
record plus a ``reconciliation`` entry whose problems name facts only: no data value leaves the build file.

    python -m scripts.lakehouse.gold_reconcile --database BUILD --suites DIR [DIR ...] --baselines FILE --output FILE
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import duckdb

from scripts.lakehouse import duckdb_bounds, gold_checks, silver_quality


def rows_of(connection: duckdb.DuckDBPyConnection, sql: str) -> list[list[str]]:
    return [[str(value) for value in row] for row in connection.execute(sql).fetchall()]


def reconcile(database: Path) -> dict[str, Any]:
    """The per-fact row and content reconciliation of a build file: how many facts could be compared and the problems found, by check name."""
    contract = gold_checks.load_contract()
    connection = duckdb_bounds.connect(duckdb, database, read_only=True)
    try:
        relations = gold_checks.parse_relations(rows_of(connection, gold_checks.COLUMNS_SQL))
        ready, unready = gold_checks.reconcilable(contract, relations)
        rows = [row for statement in gold_checks.reconcile_statements(ready) for row in rows_of(connection, statement)]
    finally:
        connection.close()
    problems = gold_checks.reconcile_problems(ready, rows)
    problems["reconcile_rows"] = [*unready, *problems["reconcile_rows"]]
    return {"facts": len(ready), "facts_in_contract": len(contract.facts), "problems": problems}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--suites", type=Path, nargs="+", required=True, help="one or more suite folders (silver atomic, gold)")
    parser.add_argument("--baselines", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    reconciliation: dict[str, Any] = {}
    try:
        with silver_quality.private_copy(args.database) as (copy, digest):
            reconciliation = reconcile(copy)
            record = silver_quality.validate(copy, args.suites, args.baselines)
        record["build_file_sha256"] = digest
    except silver_quality.QualityError as error:
        args.output.write_text(json.dumps({"error": str(error), "reconciliation": reconciliation or None}, indent=2) + "\n")
        sys.stderr.write(f"gold reconcile: stopped: {error}\n")
        return 2
    record["reconciliation"] = reconciliation
    args.output.write_text(json.dumps(record, indent=2) + "\n")
    found = sum(len(items) for items in reconciliation["problems"].values())
    counts = record["counts"]
    sys.stdout.write(
        f"gold reconcile: {counts['pass']} passed, {counts['warn']} warned, {counts['fail']} failed; "
        f"{reconciliation['facts']} of {reconciliation['facts_in_contract']} facts reconciled, {found} problems\n"
    )
    return 1 if counts["fail"] or found else 0


if __name__ == "__main__":
    sys.exit(main())
