"""Compare the data tables of two builds of one pin, called by the feature store build runner.

A rebuild with the same pin and data must give the same rows in every data table. ``differing_tables`` names each table whose row count or content hash
differs and shows a few rows that only one of the two builds holds, so a failing case points at the table and the column that changed.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from typing import Any

TABLE_HASH = 'select count(*), sum(hash(t)::hugeint) from "{table}" as t'
TABLE_ROWS = 'select * from "{table}"'
SAMPLED_TABLES = 3
SAMPLE_ROWS = 3
SAMPLE_CHARACTERS = 400


def fingerprint(connection: Any, table: str) -> tuple[Any, ...]:
    """The row count and the sum of the row hashes of one table."""
    return tuple(connection.execute(TABLE_HASH.format(table=table)).fetchone())


def row_sample(first: Any, second: Any, table: str) -> dict[str, Any]:
    """Rows that only the first or only the second build holds, as column-to-value text in a fixed order, and for rows with the same first column in both
    sets the columns whose values differ."""
    query = TABLE_ROWS.format(table=table)
    names = [column[0] for column in first.execute(query).description]
    in_first = Counter(tuple(row) for row in first.execute(query).fetchall())
    in_second = Counter(tuple(row) for row in second.execute(query).fetchall())

    def shown(rows: Counter[tuple[Any, ...]]) -> list[str]:
        texts = sorted(str(dict(zip(names, row, strict=True)))[:SAMPLE_CHARACTERS] for row in rows.elements())
        return texts[:SAMPLE_ROWS]

    only_first, only_second = in_first - in_second, in_second - in_first
    second_by_key = {str(row[0]): row for row in only_second.elements()}
    changed: dict[str, dict[str, list[str]]] = {}
    for row in sorted(only_first.elements(), key=str):
        other = second_by_key.get(str(row[0]))
        if other is not None and len(changed) < SAMPLE_ROWS:
            changed[str(row[0])] = {
                name: [str(a)[:SAMPLE_CHARACTERS], str(b)[:SAMPLE_CHARACTERS]] for name, a, b in zip(names, row, other, strict=True) if a != b
            }
    return {"only_in_first_build": shown(only_first), "only_in_second_build": shown(only_second), "changed_columns_by_first_column": changed}


def differing_tables(first: Any, second: Any, tables: Iterable[str]) -> dict[str, dict[str, Any]]:
    """The tables whose fingerprint differs between two builds, with both fingerprints; the first few also carry a sample of the differing rows."""
    found: dict[str, dict[str, Any]] = {}
    for table in sorted(tables):
        one, two = fingerprint(first, table), fingerprint(second, table)
        if one == two:
            continue
        entry: dict[str, Any] = {"rows_and_hash_first": [str(value) for value in one], "rows_and_hash_second": [str(value) for value in two]}
        if len(found) < SAMPLED_TABLES:
            entry["sample"] = row_sample(first, second, table)
        found[table] = entry
    return found
