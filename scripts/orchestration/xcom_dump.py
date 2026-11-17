"""Read-only logical dump of the Airflow metadata database for the sentinel scan (Airflow plan section 10; failure mode 932b).

It runs inside the thin Airflow image as a finite job of the ``orchestration_dump`` template, on the stack's network, with the repository file mounted
read-only. The connection string comes from the job's environment, which the launcher fills from AWS Parameter Store, and is never printed.
``metadata`` writes every table of the ``public`` schema as COPY text (a column of bytes appears as hexadecimal text) to
``/out/metadata.sql``; ``xcom`` writes the rows of both XCom tables of Airflow 3.4.0 (``xcom_v1``, the renamed legacy table, and ``xcom_v2``) to
``/out/xcom.txt``, each after a comment line with its name, and refuses a database that lacks either table. The session is read-only.
The job prints only the number of tables and rows it wrote.

It imports the standard library and ``psycopg2``, which the Airflow image provides.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2 import sql

CONNECTION_VARIABLE = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
OUT = Path("/out")
URL_PREFIX = "postgresql+psycopg2://"
TARGETS = {"metadata": "metadata.sql", "xcom": "xcom.txt"}
XCOM_TABLES = ("xcom_v1", "xcom_v2")  # Airflow 3.4.0 migration 0142 renames xcom to xcom_v1 and adds xcom_v2; no table is named xcom.


def connect() -> Any:
    """A read-only connection to the metadata database; the connection string never leaves this function."""
    url = os.environ[CONNECTION_VARIABLE].strip()
    if not url.startswith(URL_PREFIX):
        raise SystemExit("the connection string is not a psycopg2 URL")
    connection = psycopg2.connect("postgresql://" + url[len(URL_PREFIX) :])
    connection.set_session(readonly=True, autocommit=True)
    return connection


def table_names(connection: Any) -> list[str]:
    """Every base table of the public schema, by name."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name")
        return [str(row[0]) for row in cursor.fetchall()]


def copy_query(connection: Any, query: sql.Composable, handle: Any) -> int:
    """Write one query's rows to the open text file as COPY text and return the number of rows."""
    statement = sql.SQL("COPY ({}) TO STDOUT").format(query)
    with connection.cursor() as cursor:
        cursor.copy_expert(statement.as_string(connection), handle)
        return int(cursor.rowcount)


def dump_metadata(connection: Any, handle: Any) -> tuple[int, int]:
    """Every table, each after a comment line with its name; returns the tables and rows written."""
    names = table_names(connection)
    rows = 0
    for name in names:
        handle.write(f"-- table {name}\n")
        rows += max(0, copy_query(connection, sql.SQL("SELECT * FROM {}").format(sql.Identifier(name)), handle))
    return len(names), rows


def dump_xcom(connection: Any, handle: Any) -> tuple[int, int]:
    """The rows of both XCom tables, each after a comment line with its name and ordered by its first columns; returns the tables and rows written.

    A database that lacks either table is refused, so the scan never passes on a part of the XCom data.
    """
    present = set(table_names(connection))
    for name in XCOM_TABLES:
        if name not in present:
            raise SystemExit(f"the XCom table {name} is missing from the public schema; the dump needs the Airflow 3.4.0 tables {' and '.join(XCOM_TABLES)}")
    rows = 0
    for name in XCOM_TABLES:
        handle.write(f"-- table {name}\n")
        rows += max(0, copy_query(connection, sql.SQL("SELECT * FROM {} ORDER BY 1, 2, 3, 4, 5").format(sql.Identifier(name)), handle))
    return len(XCOM_TABLES), rows


def main() -> int:
    """Write the chosen dump and print its size."""
    cli = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    cli.add_argument("what", choices=sorted(TARGETS))
    args = cli.parse_args()
    target = OUT / TARGETS[args.what]
    connection = connect()
    try:
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w") as handle:
            tables, rows = (dump_metadata if args.what == "metadata" else dump_xcom)(connection, handle)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        connection.close()
    sys.stdout.write(f"DUMP {args.what} tables={tables} rows={rows}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
