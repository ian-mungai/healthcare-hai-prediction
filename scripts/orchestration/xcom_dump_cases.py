"""Offline cases of ``scripts/orchestration/xcom_dump.py`` on a stand-in metadata database (failure mode 932b).

``xcom_dump.py`` imports ``psycopg2``, which only the Airflow image provides, so the cases load it with a stand-in of the ``sql`` module and run ``dump_xcom``
itself over a stand-in database. Nothing starts a container; the real database check is the dump job of the ``orchestration_dump`` template in the stack.
"""

from __future__ import annotations

import importlib.util
import io
import re
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
DUMP_SCRIPT = ROOT / "scripts" / "orchestration" / "xcom_dump.py"
LEGACY_MARKER, CURRENT_MARKER = "LEGACY_CONTROL_MARKER", "CURRENT_CONTROL_MARKER"


class StubComposable:
    """The part of ``psycopg2.sql`` the dump script uses: a SQL template, or a quoted identifier."""

    def __init__(self, text: str) -> None:
        self.text = text

    def format(self, *parts: StubComposable) -> StubComposable:
        """Fill each ``{}`` with the next part, as ``sql.SQL.format`` does."""
        text = self.text
        for part in parts:
            text = text.replace("{}", part.as_string(None), 1)
        return StubComposable(text)

    def as_string(self, _context: Any) -> str:
        return self.text


def stub_identifier(name: str) -> StubComposable:
    """``sql.Identifier``: the name in double quotes, a double quote doubled."""
    return StubComposable('"' + name.replace('"', '""') + '"')


class UndefinedTable(Exception):
    """PostgreSQL's refusal of a query on a table the database does not have."""


class FakeMetadataDatabase:
    """A metadata database by table name to rows. It lists its tables for the ``information_schema`` query and serves ``COPY (SELECT * FROM <table> ...)``
    as tab-separated lines, and refuses a table it does not have as PostgreSQL does."""

    def __init__(self, tables: dict[str, list[tuple[str, ...]]]) -> None:
        self.tables = tables
        self.statements: list[str] = []
        self.rowcount = -1
        self.found: list[tuple[str, ...]] = []

    def cursor(self) -> FakeMetadataDatabase:
        return self

    def __enter__(self) -> FakeMetadataDatabase:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, query: str) -> None:
        if "information_schema.tables" not in query:
            raise AssertionError(f"the dump script sent a query the stand-in does not serve: {query}")
        self.found = [(name,) for name in sorted(self.tables)]

    def fetchall(self) -> list[tuple[str, ...]]:
        return self.found

    def copy_expert(self, statement: str, handle: Any) -> None:
        self.statements.append(statement)
        named = re.search(r'FROM\s+"?([A-Za-z0-9_]+)"?', statement)
        if named is None or named.group(1) not in self.tables:
            raise UndefinedTable(f"relation {named.group(1) if named else '?'} does not exist")
        rows = sorted(self.tables[named.group(1)])
        for row in rows:
            handle.write("\t".join(row) + "\n")
        self.rowcount = len(rows)


def load_xcom_dump() -> ModuleType:
    """``xcom_dump.py`` imported with ``psycopg2`` replaced by the stand-in of its ``sql`` module."""
    sql_module = SimpleNamespace(SQL=StubComposable, Identifier=stub_identifier)
    package = SimpleNamespace(sql=sql_module)
    spec = importlib.util.spec_from_file_location("xcom_dump_under_test", DUMP_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("xcom_dump.py cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, {"psycopg2": package, "psycopg2.sql": sql_module}):
        spec.loader.exec_module(module)
    return module


def dump(module: ModuleType, tables: dict[str, list[tuple[str, ...]]]) -> tuple[Any, str, str, FakeMetadataDatabase]:
    """Run ``dump_xcom`` on a stand-in database: the counts it returned (None when it refused), the text it wrote, the refusal and the database."""
    database = FakeMetadataDatabase(tables)
    handle = io.StringIO()
    try:
        counts: Any = module.dump_xcom(database, handle)
        error = ""
    except (UndefinedTable, SystemExit) as exc:
        counts, error = None, f"{type(exc).__name__}: {exc}"
    return counts, handle.getvalue(), error, database


def xcom_dump_cases() -> dict[str, bool]:
    """The logical XCom dump reads both XCom tables of an Airflow 3.4.0 metadata database.

    That release's migration renames ``xcom`` to ``xcom_v1`` and creates ``xcom_v2``, so a database has no table named ``xcom``. Failure modes: a query on
    ``xcom`` ends in an undefined-table error and the dump file stays empty; only one of the two tables is dumped, so the rows of the other are never
    scanned; a missing XCom table is skipped and the job still reports success; a table name is written into the query text without identifier quoting.
    """
    module = load_xcom_dump()
    release_340: dict[str, list[tuple[str, ...]]] = {
        "task_instance": [("1",)],
        "xcom_v1": [("1", "task", "-1", "key", LEGACY_MARKER)],
        "xcom_v2": [("2", "task", "-1", "key", CURRENT_MARKER)],
    }
    counts, text, error, database = dump(module, release_340)
    no_v2 = dump(module, {name: rows for name, rows in release_340.items() if name != "xcom_v2"})
    no_v1 = dump(module, {name: rows for name, rows in release_340.items() if name != "xcom_v1"})
    release_332 = dump(module, {"task_instance": [("1",)], "xcom": [("1", "task", "-1", "key", LEGACY_MARKER)]})
    return {
        "the dump of the 3.4.0 layout (xcom_v1 and xcom_v2, no xcom) ends without an error": error == "",
        "it holds the row of xcom_v1 and the row of xcom_v2": LEGACY_MARKER in text and CURRENT_MARKER in text,
        "it reports two tables and two rows": counts == (2, 2),
        "each table follows a comment line with its name": "-- table xcom_v1\n" in text and "-- table xcom_v2\n" in text,
        "both tables are read through quoted identifiers": all(
            any(f'"{name}"' in statement for statement in database.statements) for name in ("xcom_v1", "xcom_v2")
        ),
        "a database without xcom_v2 is refused by name, not dumped in part": no_v2[0] is None and "xcom_v2" in no_v2[2],
        "a database without xcom_v1 is refused by name, not dumped in part": no_v1[0] is None and "xcom_v1" in no_v1[2],
        "the 3.3.2 layout (a table named xcom) is refused by name": release_332[0] is None and "xcom_v1" in release_332[2],
    }
