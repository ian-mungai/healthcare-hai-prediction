"""Which declared bronze tables the bronze run records leave unsourced: the table has no source object yet, so no run loaded it and it has no Iceberg table.

The bronze stage leaves such a table out of the request and the check phase writes it into the run record ``lakehouse/bronze_runs/<run>/check.json`` as a
skipped table. A step that needs every mapped table (the Glue mirror ``register``, the AWS catalog registration) asks this module which tables it may leave
out. A table is unsourced when the newest run record that names it, as loaded or as skipped, skips it: a table that a later run loaded is required again.
A table that a silver model reads is never left out, because silver would fail without it.
"""

from __future__ import annotations

import json
from collections.abc import Collection, Iterable, Sequence
from typing import Any

RECORD_PREFIX = "lakehouse/bronze_runs/"
RECORD_NAME = "check.json"


class UnsourcedError(ValueError):
    """A bronze run record cannot be read, so no table may be left out on its word."""


def record_tables(raw: bytes, key: str = RECORD_NAME) -> tuple[frozenset[str], frozenset[str]]:
    """The tables a run record loaded and the tables it skipped. An older record, written before the check phase named skipped tables, skipped none."""
    try:
        record = json.loads(raw)
    except ValueError:
        raise UnsourcedError(f"{key}: is not JSON") from None
    if not isinstance(record, dict) or record.get("schema_version") != 1 or record.get("target") != "real":
        raise UnsourcedError(f"{key}: is not a schema version 1 record of a real bronze run")
    loaded, skipped = record.get("tables"), record.get("skipped_tables", [])
    if not isinstance(loaded, list) or not all(isinstance(name, str) for name in loaded):
        raise UnsourcedError(f"{key}: holds no list of loaded tables")
    names = (
        [item.get("table") if isinstance(item, dict) and set(item) == {"table", "reason"} else None for item in skipped]
        if isinstance(skipped, list)
        else [None]
    )
    if None in names or not all(isinstance(name, str) for name in names) or len(set(names)) != len(names) or set(names) & set(loaded):
        raise UnsourcedError(f"{key}: the skipped tables are not distinct tables outside the loaded ones, each with a reason")
    return frozenset(loaded), frozenset(str(name) for name in names)


def unsourced_after(records: Sequence[tuple[Collection[str], Collection[str]]]) -> frozenset[str]:
    """The tables left unsourced after ``records``, oldest first, each as (loaded, skipped): a later record decides for each table it names."""
    state: set[str] = set()
    for loaded, skipped in records:
        state -= set(loaded)
        state |= set(skipped)
    return frozenset(state)


def read_unsourced(s3: Any, bucket: str) -> frozenset[str]:
    """The tables left unsourced by the run records under ``lakehouse/bronze_runs/``, ordered by the time each record was written (it is written once)."""
    found: list[tuple[Any, str]] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=RECORD_PREFIX):
        for item in page.get("Contents", []):
            key = str(item["Key"])
            parts = key[len(RECORD_PREFIX) :].split("/")
            if len(parts) == 2 and parts[1] == RECORD_NAME:
                if item.get("LastModified") is None:
                    raise UnsourcedError(f"{key}: the listing gives no write time, so the records cannot be put in order")
                found.append((item["LastModified"], key))
    records = [record_tables(bytes(s3.get_object(Bucket=bucket, Key=key)["Body"].read()), key) for _, key in sorted(found)]
    return unsourced_after(records)


def left_out(declared: Iterable[str], unsourced: Collection[str], readers: Collection[str]) -> frozenset[str]:
    """The declared tables a step may leave out: unsourced ones that no silver model reads. A skipped name that the table map no longer declares is not
    required in any case, so it is ignored."""
    return frozenset((set(unsourced) & set(declared)) - set(readers))
