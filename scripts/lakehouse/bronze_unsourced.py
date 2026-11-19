"""Which declared bronze tables the bronze run records leave unsourced: the table has no source object yet, so no run loaded it and it has no Iceberg table.

The bronze stage leaves such a table out of the request and the check phase writes it into the run record ``lakehouse/bronze_runs/<run>/check.json`` as a
skipped table. A step that needs every mapped table (the Glue mirror ``register``, the AWS catalog registration) asks this module which tables it may leave
out. A record is evidence only when it is a complete run record whose run ID is the one in its key (the shape ``orchestration.raw_lineage.loaded_objects``
checks, with the stage's reason for every skip). A table is unsourced when some record skips it and no record loaded it, whatever the order of the records,
their write times or the order of the arguments. A table that a silver model reads is never left out, because silver would fail without it. A table that
already exists in the catalog is never left out either, whatever a record says: the callers check that (``glue_mirror`` in S3 and Glue,
``register_aws_catalog`` in the source catalog). A record alone never authorizes an omission: each route also lists the table's own source collection with
its own credentials (``source_evidence``) and leaves the table out only when that finds no source object and no observation history.
"""

from __future__ import annotations

import json
import re
from collections.abc import Collection, Iterable, Sequence
from typing import Any

from orchestration import raw_lineage
from orchestration.training_contract import run_id_pattern
from scripts.lakehouse import bronze
from scripts.lakehouse.bronze_dispatch import SKIPPED_REASON

RECORD_PREFIX = "lakehouse/bronze_runs/"
RECORD_NAME = "check.json"
RUN_ID = re.compile(run_id_pattern("bronze"))
RECORD_KEY = re.compile(re.escape(RECORD_PREFIX) + "(" + run_id_pattern("bronze") + ")/" + re.escape(RECORD_NAME))
SHA256 = re.compile(r"[a-f0-9]{64}")


class UnsourcedError(ValueError):
    """A bronze run record cannot be read as evidence, so no table may be left out on its word."""


def record_tables(raw: bytes, key: str, run_id: str) -> tuple[frozenset[str], frozenset[str]]:
    """The tables a run record loaded and the tables it skipped, after the checks that bind it to ``run_id`` (the run of its key).

    An older record, written before the check phase named skipped tables, is accepted as one that skipped none. Any other defect is refused.
    """
    try:
        record = json.loads(raw)
    except ValueError:
        raise UnsourcedError(f"{key}: is not JSON") from None
    if isinstance(record, dict) and set(record) == raw_lineage.BRONZE_RECORD_KEYS - {"skipped_tables"}:
        record = {**record, "skipped_tables": []}
    if not RUN_ID.fullmatch(run_id):
        raise UnsourcedError(f"{key}: {run_id!r} is not a bronze run ID")
    try:
        raw_lineage.loaded_objects(record, key, run_id)
    except raw_lineage.LineageRefused as error:
        raise UnsourcedError(str(error)) from None
    if not isinstance(record["report_sha256"], str) or not SHA256.fullmatch(record["report_sha256"]):
        raise UnsourcedError(f"{key}: has no SHA-256 of the loader report")
    if not isinstance(record["metadata_generation"], str) or not record["metadata_generation"]:
        raise UnsourcedError(f"{key}: names no metadata generation")
    if any(item["reason"] != SKIPPED_REASON for item in record["skipped_tables"]):
        raise UnsourcedError(f"{key}: a skipped table has a reason other than {SKIPPED_REASON!r}")
    return frozenset(record["tables"]), frozenset(item["table"] for item in record["skipped_tables"])


def unsourced_in(records: Iterable[tuple[Collection[str], Collection[str]]]) -> frozenset[str]:
    """The tables that some record skipped and no record loaded, each record as (loaded, skipped). A table that a record loaded has data, so its order
    against the skip does not matter: a later skip cannot unload it and an earlier one is over."""
    skipped: set[str] = set()
    loaded: set[str] = set()
    for tables, left_out in records:
        loaded |= set(tables)
        skipped |= set(left_out)
    return frozenset(skipped - loaded)


def read_unsourced(s3: Any, bucket: str) -> frozenset[str]:
    """The unsourced tables of the run records under ``lakehouse/bronze_runs/``. A key that is not a run's record key is not evidence and is ignored; a record
    under a run's key that is not a complete record of that run refuses the read."""
    records = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=RECORD_PREFIX):
        for item in page.get("Contents", []):
            key = str(item["Key"])
            found = RECORD_KEY.fullmatch(key)
            if found is not None:
                records.append(record_tables(bytes(s3.get_object(Bucket=bucket, Key=key)["Body"].read()), key, found.group(1)))
    return unsourced_in(records)


def left_out(declared: Iterable[str], unsourced: Collection[str], readers: Collection[str]) -> frozenset[str]:
    """The declared tables a step may leave out: unsourced ones that no silver model reads. A skipped name that the table map no longer declares is not
    required in any case, so it is ignored."""
    return frozenset((set(unsourced) & set(declared)) - set(readers))


def records_of(runs: Sequence[tuple[str, bytes]]) -> frozenset[str]:
    """The unsourced tables of records given as (run ID, body) pairs, in any order."""
    return unsourced_in(record_tables(raw, f"{RECORD_PREFIX}{run}/{RECORD_NAME}", run) for run, raw in runs)


READ_ROOTS = ("", "raw/")


def source_evidence(storage: bronze.Storage, bucket: str, config: dict[str, Any], tables: Collection[str]) -> frozenset[str]:
    """The tables, of ``tables``, that have a source object or observation history in the table map's own collection.

    This is the loader's own discovery (collection, manifests, member pattern or dataset IDs, retirement) over both read roots, so a loadable object, an
    object whose capture was rejected, a retired object and every matching superseded copy count. Superseded copies count whether or not an earlier generation
    published them (``count_superseded``), because a correction that moved an object out of the table's roles leaves the history behind.

    ``config`` must be a table map as ``bronze.load_table_map`` returns it (it adds the roles and encodings that discovery reads): a map without the named
    table or its defaults raises ``UnsourcedError``, so a malformed map cannot pass as a refusal. A listing or read that fails raises: nothing is guessed.
    """
    names = list(tables)
    if not names:
        return frozenset()
    by_name = {entry.get("table"): entry for entry in config.get("tables", []) if isinstance(entry, dict)}
    for name in names:
        entry = by_name.get(name)
        if entry is None or not isinstance(entry.get("roles"), list) or not isinstance(entry.get("encodings"), list):
            raise UnsourcedError(f"{name}: the table map is malformed for discovery (it lacks the table or the roles and encodings that the map loader adds)")
    found, excluded = bronze.discover(storage, bucket, config, names, bronze.RetiredObjects(), roots=READ_ROOTS, count_superseded=True)
    return frozenset({item["table"] for item in found} | {entry["copy"]["table"] for entry in excluded if "copy" in entry})
