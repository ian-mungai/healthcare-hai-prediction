"""Write a bounded logical dump of the synthetic Marquez database from an internal, read-only launcher job."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2 import sql

MAX_BYTES = 64 * 1024**2


class BoundedDump:
    """Stream COPY bytes to a private file and refuse an oversized dump; psycopg2 writes bytes to any file object that is not a text file."""

    def __init__(self, descriptor: int) -> None:
        self.handle = os.fdopen(descriptor, "wb")
        self.bytes = 0

    def write(self, data: bytes) -> int:
        """Write no more than the configured byte bound."""
        self.bytes += len(data)
        if self.bytes > MAX_BYTES:
            raise ValueError("lineage database dump exceeds 64 MiB")
        return self.handle.write(data)


def copy_tables(connection: Any, tables: list[str], output: BoundedDump) -> None:
    """Write each table's COPY text to the bounded dump, after a comment line with its name."""
    for table in tables:
        output.write(f"-- table {table}\n".encode())
        with connection.cursor() as cursor:
            query = sql.SQL("copy (select * from public.{}) to stdout").format(sql.Identifier(table))
            cursor.copy_expert(query.as_string(connection), output)


def main() -> int:
    """Dump public base tables without exposing their contents or connection credentials in output."""
    connection = None
    output = None
    try:
        connection = psycopg2.connect(
            host="lineage-postgres",
            dbname="marquez",
            user="marquez",
            password=os.environ["MARQUEZ_DB_PASSWORD"],
            connect_timeout=5,
            options="-c statement_timeout=15000 -c default_transaction_read_only=on",
        )
        connection.set_session(readonly=True, autocommit=False, isolation_level="REPEATABLE READ")
        with connection.cursor() as cursor:
            cursor.execute("show transaction_read_only")
            if cursor.fetchone()[0] != "on":
                raise ValueError("lineage database dump session is not read-only")
            cursor.execute("select table_name from information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE' order by table_name")
            tables = [str(row[0]) for row in cursor.fetchall()]
        if not tables or len(tables) > 200:
            raise ValueError("lineage database table inventory is empty or exceeds its bound")
        target = Path("/out/marquez.sql")
        output = BoundedDump(os.open(target.with_suffix(".pending"), os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600))
        copy_tables(connection, tables, output)
        output.handle.flush()
        os.fsync(output.handle.fileno())
        target.with_suffix(".pending").replace(target)
        directory = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        sys.stdout.write(json.dumps({"tables": len(tables), "bytes": output.bytes, "readonly": True}) + "\n")
        return 0
    except (OSError, ValueError, psycopg2.Error) as error:
        sys.stderr.write(f"lineage dump failed ({type(error).__name__})\n")
        return 1
    finally:
        if output is not None:
            output.handle.close()
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
