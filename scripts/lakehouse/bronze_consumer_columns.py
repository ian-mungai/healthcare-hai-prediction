"""The bronze columns that silver reads by name, declared per table and checked after every bronze load.

``config/lakehouse/bronze_consumer_columns.json`` declares every bronze table that a compiled silver model reads. Each entry is one of:

- ``columns``: the columns the models read, each with the models that read it (the union over every consumer; the models are kept for the error message);
- ``any``: the table cannot be reduced to a column list (a model reads every column, or the layout is dynamic), with the reason. Nothing is checked for it,
  and the declaration says so by table name.

A table that a deferred staging view would read (``vars.deferred_models``) keeps a ``deferred`` note: it has no consumer now beyond the lineage readers.

The lists are not written by hand. ``scripts.lakehouse.bronze_consumer_declare`` parses the compiled models (``bronze_consumer_lineage``) and writes the file
with ``--write``; the default (no flag; ``--root`` names another project folder) checks and fails when the file and the models disagree, or when a staged table
has no declaration, and names each table and column.

At the bronze load, ``check_loaded`` compares a loaded table with its declaration and stops by table and column name before silver runs:

- a table with no input object in this run is skipped (nothing was loaded, so nothing is checked; silver stops by itself on a table that does not exist);
- a table with no rows passes, whatever its columns (a pruned table keeps the columns of its old files);
- an ``any`` or undeclared table passes with that status recorded;
- every other table must hold every declared column; extra columns are never a failure.

This module reads only the standard library, because the bronze job runs without dbt or DuckDB; the host-only modules are imported when a command needs them.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

CONTRACT = "config/lakehouse/bronze_consumer_columns.json"
VERSION = 2
SHOWN = 12


class ConsumerColumnsError(ValueError):
    """The contract file or the compile of the models cannot be read."""


@dataclass(frozen=True)
class Declaration:
    """What the models read of one bronze table: the columns with their consumers, or ``any`` with the reason."""

    columns: Mapping[str, tuple[str, ...]] | None
    reason: str = ""
    deferred: str = ""


@dataclass(frozen=True)
class LoadCheck:
    """The outcome of the declaration check of one table of one load: its status and the missing columns with their consumers."""

    status: str
    missing: Mapping[str, tuple[str, ...]] = field(default_factory=dict)


def load(path: Path) -> dict[str, Declaration]:
    """Read the committed contract: table to declaration."""
    try:
        document = json.loads(path.read_text())
        tables = document["tables"]
        if document["version"] != VERSION or not isinstance(tables, dict):
            raise ValueError("version or tables")
        found: dict[str, Declaration] = {}
        for table, entry in tables.items():
            deferred = str(entry.get("deferred", ""))
            if "any" in entry:
                found[str(table)] = Declaration(None, str(entry["any"]), deferred)
            else:
                found[str(table)] = Declaration({str(name): tuple(str(model) for model in models) for name, models in entry["columns"].items()}, "", deferred)
        return found
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        raise ConsumerColumnsError(f"{path}: the contract cannot be read ({error.__class__.__name__}); regenerate it with --write") from error


def status(table: str, contract: Mapping[str, Declaration]) -> str:
    """``undeclared``, ``any`` or ``columns`` for a table."""
    if table not in contract:
        return "undeclared"
    return "any" if contract[table].columns is None else "columns"


def check_loaded(table: str, has_inputs: bool, present: Collection[str], rows: int, contract: Mapping[str, Declaration]) -> LoadCheck:
    """The declaration check of one table of one load. Only a loaded table with rows and a column list can be refused, and only for a missing column."""
    if not has_inputs:
        return LoadCheck("skipped")
    kind = status(table, contract)
    if kind != "columns":
        return LoadCheck(kind)
    if rows == 0:
        return LoadCheck("empty")
    columns = contract[table].columns or {}
    held = {name.lower() for name in present}
    lacking = {name: models for name, models in sorted(columns.items()) if name not in held}
    return LoadCheck("refused" if lacking else "ok", lacking)


def refusal(table: str, lacking: Mapping[str, Sequence[str]], present: Collection[str]) -> str:
    """The message of a refused table: the table, each missing column with the models that read it and the first present columns."""
    shown = sorted(present)
    more = f" and {len(shown) - SHOWN} more" if len(shown) > SHOWN else ""
    names = sorted(lacking)
    extra = f" and {len(names) - SHOWN} more" if len(names) > SHOWN else ""
    detail = ", ".join(f"{name} (read by {', '.join(lacking[name][:3])}{' and more' if len(lacking[name]) > 3 else ''})" for name in names[:SHOWN])
    return f"{table}: the loaded table lacks columns its silver models read: missing {detail}{extra}; the table has {', '.join(shown[:SHOWN])}{more}"


def main(argv: list[str] | None = None) -> int:
    """Check or write the contract file against a compile of the real dbt project (the commands of ``bronze_consumer_declare``)."""
    from scripts.lakehouse import bronze_consumer_declare

    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--write", action="store_true", help="regenerate the contract file from the models")
    parser.add_argument("--root", type=Path, default=None, help="repository root")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    return bronze_consumer_declare.run(args.root, write=args.write)


if __name__ == "__main__":
    sys.exit(main())
