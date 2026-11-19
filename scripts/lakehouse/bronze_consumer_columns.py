"""The bronze columns that silver models read by name, declared per table and checked after every bronze load.

``config/lakehouse/bronze_consumer_columns.json`` lists, for each bronze CSV table that a staging projection reads, the columns that projection reads.
The lists are not written by hand: the macro ``canonical_projection`` computes them from its own projection text and writes them as a
``-- bronze_consumer_columns`` line into the compiled SQL, and ``derive`` reads those lines from a compile of the real dbt project (lint target, no
catalog). ``--check`` fails when the committed file and the models disagree and names each table and column; ``--write`` regenerates the file. The bronze
load calls ``missing`` after it loads a table and stops by table and column name before silver runs, so a table that holds another layout never reaches
the silver build.

A table without a declaration is undeclared: nothing is checked for it, and it is not a pass. The models on ``canonical_rows`` read named columns only in the
intermediate models downstream of their staging views; their lists are not derived yet.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Collection, Mapping
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = "config/lakehouse/bronze_consumer_columns.json"
DBT = ROOT / ".tools" / "sqlfluff" / "bin" / "dbt"
MARKER = re.compile(r"^-- bronze_consumer_columns ([a-z][a-z0-9_]*): ([a-z0-9_,]+)$", re.MULTILINE)
COMPILE_SECONDS = 300
BASIS = (
    "The bronze columns each staging projection reads, as the macro canonical_projection computes them from its projection text and writes them into the "
    "compiled SQL. Regenerate with `.venv/bin/python -m scripts.lakehouse.bronze_consumer_columns --write`; "
    "the bronze load refuses a table that lacks one of them."
)


class ConsumerColumnsError(ValueError):
    """The contract file or the compile of the models cannot be read."""


def compile_project(project: Path, target: Path, packages: Path | None = None) -> Path:
    """Compile the dbt project with the lint target (an in-memory DuckDB with nothing attached) and return the folder of the compiled models."""
    from scripts.process import run_command

    environment = {
        "DUCKDB_MEMORY_LIMIT": "2GB",
        "JOB_THREADS": "1",
        "DBT_PACKAGES_INSTALL_PATH": str((packages or ROOT / "data/analytics/dbt/dbt_packages").resolve()),
    }
    result = run_command(
        str(DBT),
        ["compile", "--target", "lint", "--project-dir", str(project), "--profiles-dir", str(project), "--target-path", str(target)],
        env=environment,
        timeout=COMPILE_SECONDS,
    )
    if result.returncode != 0:
        raise ConsumerColumnsError(
            f"dbt compile failed with code {result.returncode}: {result.stdout.strip().splitlines()[-1:] or result.stderr.strip()[-300:]}"
        )
    return target / "compiled"


def derive(compiled: Path) -> dict[str, list[str]]:
    """The declared columns of each table, read from the marker lines of the compiled models. Two models that read one table must agree."""
    found: dict[str, list[str]] = {}
    for path in sorted(compiled.rglob("*.sql")):
        for table, names in MARKER.findall(path.read_text()):
            listed = sorted(set(names.split(",")))
            if found.setdefault(table, listed) != listed:
                raise ConsumerColumnsError(f"{table}: two models read it through different column lists ({path.name})")
    return dict(sorted(found.items()))


def render(derived: Mapping[str, Collection[str]]) -> str:
    """The contract file text for the derived lists."""
    document = {"version": 1, "basis": BASIS, "tables": {table: sorted(names) for table, names in sorted(derived.items())}}
    return json.dumps(document, indent=2) + "\n"


def load(path: Path) -> dict[str, list[str]]:
    """Read the committed contract: table to sorted columns."""
    try:
        document = json.loads(path.read_text())
        tables = document["tables"]
        if document["version"] != 1 or not isinstance(tables, dict):
            raise ValueError("version or tables")
        return {str(table): [str(name) for name in names] for table, names in tables.items()}
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise ConsumerColumnsError(f"{path}: the contract cannot be read ({error.__class__.__name__}); regenerate it with --write") from error


def differences(derived: Mapping[str, Collection[str]], contract: Mapping[str, Collection[str]]) -> list[str]:
    """One line for each table or column on which the models and the contract disagree, naming the table and the column."""
    lines: list[str] = []
    for table in sorted(set(derived) | set(contract)):
        if table not in contract:
            lines.append(f"{table}: a model reads it by name but the contract does not declare it")
            continue
        if table not in derived:
            lines.append(f"{table}: the contract declares it but no model reads it by name")
            continue
        lines += [f"{table}: a model reads {name} but the contract does not declare it" for name in sorted(set(derived[table]) - set(contract[table]))]
        lines += [f"{table}: the contract declares {name} but no model reads it" for name in sorted(set(contract[table]) - set(derived[table]))]
    return lines


def declared(table: str, contract: Mapping[str, Collection[str]]) -> bool:
    """Whether the contract declares the table. An undeclared table is checked for nothing."""
    return table in contract


def missing(table: str, present: Collection[str], rows: int, contract: Mapping[str, Collection[str]]) -> list[str]:
    """The declared columns a loaded table lacks. A table without rows has nothing to read, so it lacks none; an undeclared table lacks none."""
    if rows == 0 or table not in contract:
        return []
    return sorted(set(contract[table]) - set(present))


def refusal(table: str, lacking: Collection[str], present: Collection[str]) -> str:
    """The message of a refused table: the table, the missing columns and the first present ones."""
    shown = sorted(present)
    more = f" and {len(shown) - 12} more" if len(shown) > 12 else ""
    names = sorted(lacking)
    extra = f" and {len(names) - 12} more" if len(names) > 12 else ""
    return (
        f"{table}: the loaded table lacks columns its silver models read: missing {', '.join(names[:12])}{extra}; the table has {', '.join(shown[:12])}{more}"
    )


def main(argv: list[str] | None = None) -> int:
    """Check or write the contract file against a compile of the real dbt project."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--write", action="store_true", help="regenerate the contract file from the models")
    parser.add_argument("--root", type=Path, default=ROOT, help="repository root")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    import tempfile

    with tempfile.TemporaryDirectory(prefix="bronze_columns_") as folder:
        derived = derive(compile_project(args.root / "dbt", Path(folder) / "target", packages=ROOT / "data/analytics/dbt/dbt_packages"))
    path = args.root / CONTRACT
    if args.write:
        path.write_text(render(derived))
        sys.stdout.write(f"wrote {len(derived)} tables to {CONTRACT}\n")
        return 0
    lines = differences(derived, load(path))
    for line in lines:
        sys.stderr.write(f"{CONTRACT}: {line}\n")
    if lines:
        sys.stderr.write(f"{len(lines)} differences. Regenerate with: .venv/bin/python -m scripts.lakehouse.bronze_consumer_columns --write\n")
        return 1
    sys.stdout.write(f"bronze consumer columns ok: {len(derived)} tables\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
