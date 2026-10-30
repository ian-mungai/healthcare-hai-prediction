"""Append prediction rows to the Iceberg table ``serving.predictions`` through Polaris. It runs only in the serving image.

Each append gets a fresh token and a fresh catalog handle, because the server runs for days and a token does not. The append runs in a worker with a
deadline. A table that does not exist is an error, never created here: the owner creates it with ``create_table`` under the catalog administrator. The
writer principal can append to the ``serving`` namespace only.
"""

from __future__ import annotations

import concurrent.futures
import importlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from scripts.features import publication_reads as reads

CONTRACT = Path(__file__).resolve().parents[2] / "data_contracts/serving/predictions.json"
TYPES = {"string": "StringType", "long": "LongType", "double": "DoubleType", "boolean": "BooleanType", "date": "DateType", "timestamp": "TimestamptzType"}


class IcebergSink:
    def __init__(self, environ: Mapping[str, str]) -> None:
        self._endpoint = environ["POLARIS_URI"]
        self._id, self._secret = environ["POLARIS_CLIENT_ID"], environ["POLARIS_CLIENT_SECRET"]
        self._contract = json.loads(CONTRACT.read_text())
        self._pool = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="iceberg_sink")

    def _catalog(self, timeout: float) -> Any:
        token = reads.polaris_token(self._endpoint, self._id, self._secret, timeout=timeout / 2, attempts=1)
        return importlib.import_module("pyiceberg.catalog").load_catalog(
            reads.CATALOG_NAME, type="rest", uri=self._endpoint, warehouse=reads.WAREHOUSE, token=token
        )

    def _append(self, rows: Sequence[Mapping[str, Any]], timeout: float) -> None:
        table = self._catalog(timeout).load_table(f"{self._contract['namespace']}.{self._contract['table']}")
        arrow = importlib.import_module("pyarrow")
        table.append(arrow.Table.from_pylist([dict(row) for row in rows], schema=table.schema().as_arrow()))

    def append(self, rows: Sequence[Mapping[str, Any]], timeout: float) -> None:
        """Append once or raise. A worker still running after the deadline keeps the single worker busy, so later appends fail at their own deadline."""
        try:
            self._pool.submit(self._append, rows, timeout).result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"the prediction append did not finish within {timeout:g} s") from None


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
