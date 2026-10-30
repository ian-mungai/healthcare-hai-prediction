"""Append prediction rows to the Iceberg table ``serving.predictions`` through Polaris. It runs only in the serving image.

Each append gets a fresh token and a fresh catalog handle, because the server runs for days and a token does not. The append runs in one daemon worker
thread with a deadline for the caller. Python cannot stop a thread, so a worker that outlives its deadline keeps running and may still commit. Three
rules keep that from duplicating rows. Only one worker runs at a time, so a second append waits (and fails at its own deadline) until the first ends.
A worker that finishes after its caller gave up leaves its request ids in a memory, and the next append skips those rows. And the thread is a daemon,
so a stuck call cannot hold the process open at shutdown. ``close`` stops new appends and reports whether the worker ended within its deadline. The S3
file system has connect and request timeouts. PyIceberg has no verified timeout for its REST calls, so the one-worker rule is what bounds them. A
commit whose answer is lost to a failure may be sent again, and a reader removes such rows by ``request_id``. The table must exist: the owner creates
it with ``create_table`` under the catalog administrator, and the writer principal can append to the ``serving`` namespace only.
"""

from __future__ import annotations

import importlib
import json
import threading
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from scripts.features import publication_reads as reads

CONTRACT = Path(__file__).resolve().parents[2] / "data_contracts/serving/predictions.json"
Writer = Callable[[Sequence[Mapping[str, Any]], float], None]
TYPES = {"string": "StringType", "long": "LongType", "double": "DoubleType", "boolean": "BooleanType", "date": "DateType", "timestamp": "TimestamptzType"}


class Append:
    """One append running in its own daemon thread."""

    def __init__(self, write: Writer, rows: Sequence[Mapping[str, Any]], timeout: float) -> None:
        self.ids = {str(row["request_id"]) for row in rows}
        self.done = threading.Event()
        self.error: BaseException | None = None
        self.thread = threading.Thread(target=self._run, args=(write, list(rows), timeout), name="iceberg_append", daemon=True)

    def _run(self, write: Writer, rows: list[Mapping[str, Any]], timeout: float) -> None:
        try:
            write(rows, timeout)
        except BaseException as error:
            self.error = error
        finally:
            self.done.set()


class IcebergSink:
    def __init__(self, environ: Mapping[str, str], writer: Writer | None = None) -> None:
        self._endpoint = environ["POLARIS_URI"]
        self._id, self._secret = environ["POLARIS_CLIENT_ID"], environ["POLARIS_CLIENT_SECRET"]
        self._contract = json.loads(CONTRACT.read_text())
        self._write: Writer = writer if writer is not None else self._append_to_table
        self._lock = threading.Lock()
        self._running: Append | None = None
        self._late: set[str] = set()
        self._closed = False

    def _catalog(self, timeout: float) -> Any:
        token = reads.polaris_token(self._endpoint, self._id, self._secret, timeout=timeout / 2, attempts=1)
        return importlib.import_module("pyiceberg.catalog").load_catalog(
            reads.CATALOG_NAME,
            type="rest",
            uri=self._endpoint,
            warehouse=reads.WAREHOUSE,
            token=token,
            **{"s3.connect-timeout": reads.S3_CONNECT_TIMEOUT, "s3.request-timeout": reads.S3_REQUEST_TIMEOUT},
        )

    def _append_to_table(self, rows: Sequence[Mapping[str, Any]], timeout: float) -> None:
        table = self._catalog(timeout).load_table(f"{self._contract['namespace']}.{self._contract['table']}")
        arrow = importlib.import_module("pyarrow")
        table.append(arrow.Table.from_pylist([dict(row) for row in rows], schema=table.schema().as_arrow()))

    def _settle(self) -> None:
        """Take in the result of a worker that ended after its caller gave up: its rows are committed, so remember their ids. Hold the lock."""
        if self._running is not None and self._running.done.is_set():
            if self._running.error is None:
                self._late |= self._running.ids
            self._running = None

    def append(self, rows: Sequence[Mapping[str, Any]], timeout: float) -> None:
        """Append the rows or raise. The rows that a late worker already committed are skipped. A worker still running makes this call fail at once."""
        with self._lock:
            if self._closed:
                raise RuntimeError("the prediction sink is closed")
            self._settle()
            if self._running is not None:
                raise TimeoutError("the previous prediction append is still running")
            ids = {str(row["request_id"]) for row in rows}
            fresh = [row for row in rows if str(row["request_id"]) not in self._late]
            if not fresh:
                self._late -= ids
                return
            job = Append(self._write, fresh, timeout)
            self._running = job
            job.thread.start()
        if not job.done.wait(timeout):
            raise TimeoutError(f"the prediction append did not finish within {timeout:g} s")
        with self._lock:
            if self._running is job:
                self._running = None
            if job.error is not None:
                raise job.error
            self._late -= ids

    def wait_idle(self, timeout: float) -> bool:
        """Wait for the running worker to end. True when no worker runs."""
        with self._lock:
            job = self._running
        if job is not None and not job.done.wait(timeout):
            return False
        with self._lock:
            self._settle()
        return True

    def close(self, timeout: float) -> bool:
        """Refuse new appends and wait for the running worker. False when it did not end within the deadline."""
        with self._lock:
            self._closed = True
        return self.wait_idle(timeout)

    def worker_is_daemon(self) -> bool:
        with self._lock:
            return self._running is None or self._running.thread.daemon


def create_table(environ: Mapping[str, str]) -> None:
    """Create the namespace and the table from the contract when absent. Run by the owner with the catalog administrator principal, never by the server."""
    contract = json.loads(CONTRACT.read_text())
    types = importlib.import_module("pyiceberg.types")
    schema_module = importlib.import_module("pyiceberg.schema")
    fields = [
        types.NestedField(index, name, getattr(types, TYPES[kind])(), required=False) for index, (name, kind) in enumerate(contract["columns"].items(), start=1)
    ]
    token = reads.polaris_token(environ["POLARIS_URI"], environ["POLARIS_CLIENT_ID"], environ["POLARIS_CLIENT_SECRET"])
    catalog = importlib.import_module("pyiceberg.catalog").load_catalog(
        reads.CATALOG_NAME, type="rest", uri=environ["POLARIS_URI"], warehouse=reads.WAREHOUSE, token=token
    )
    catalog.create_namespace_if_not_exists(contract["namespace"])
    catalog.create_table_if_not_exists(f"{contract['namespace']}.{contract['table']}", schema=schema_module.Schema(*fields))
