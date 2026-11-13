"""The stale record of one ingestion unit: its schema, keys and checks, shared by the writer and every reader.

A unit that a publisher did not serve in a run keeps its last capture. The run writes one create-once record,
``<root>ingestion_stale/<unit_key>/<run_id>.json``, under a prefix apart from ``ingestion_units/``, so the skip rule never counts a stale unit as done.
``last_capture`` names the objects bronze can still load, with the exact version of each. ``last_capture_verified`` is true only when a HEAD of every
named version matched its recorded digest and length. Nothing in this module reads or writes storage.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

KIND = "ingestion_unit_stale"
SCHEMA_VERSION = 1
STALE_FOLDER = "ingestion_stale/"
REASONS = ("http_status", "not_expected_format", "contract_failed", "robots_disallowed", "robots_unreadable", "rate_limited", "host_refused", "network")
READ_ROOTS = ("", "raw/")
DIGEST = re.compile(r"[a-f0-9]{64}")
RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}")
COLLECTION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*")
TIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
ARTIFACT_FIELDS = frozenset({"key", "version_id", "sha256", "byte_count", "role", "stored_file_name", "retrieved_at_utc", "read_root"})
FIELDS = frozenset(
    {
        "kind",
        "schema_version",
        "unit_key",
        "run_id",
        "collection",
        "source_id",
        "snapshot_id",
        "attempted_at_utc",
        "reason",
        "http_status",
        "code_sha256",
        "bundle_sha256",
        "last_capture_verified",
        "last_capture",
    }
)


class StaleRecordError(ValueError):
    """A stale record, key or run ID that is malformed. It is refused, never skipped over."""


def canonical(value: Any) -> bytes:
    """The byte form of the completion record's canonical JSON."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()


def stale_prefix(root: str, unit_key: str) -> str:
    """The folder that holds every stale record of one unit."""
    if DIGEST.fullmatch(unit_key) is None:
        raise StaleRecordError("A unit key is 64 lowercase hex digits")
    return f"{root}{STALE_FOLDER}{unit_key}/"


def stale_key(root: str, unit_key: str, run_id: str) -> str:
    """The create-once key of the record a run writes for one unit."""
    if RUN_ID.fullmatch(run_id) is None:
        raise StaleRecordError("A run ID is plain text without a slash or a dot segment")
    return f"{stale_prefix(root, unit_key)}{run_id}.json"


def text(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def whole(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def artifact_ok(item: Any) -> bool:
    return (
        isinstance(item, dict)
        and set(item) == ARTIFACT_FIELDS
        and text(item["key"])
        and text(item["version_id"])
        and item["version_id"] != "null"
        and DIGEST.fullmatch(str(item["sha256"])) is not None
        and whole(item["byte_count"])
        and text(item["role"])
        and text(item["stored_file_name"])
        and TIME.fullmatch(str(item["retrieved_at_utc"])) is not None
        and item["read_root"] in READ_ROOTS
        and item["key"].startswith(item["read_root"])
    )


def check(record: Any) -> None:
    """Refuse a record that is not exactly this schema."""
    if not (isinstance(record, dict) and set(record) == FIELDS and record["kind"] == KIND and record["schema_version"] == SCHEMA_VERSION):
        raise StaleRecordError("Stale record has other fields than its schema")
    names = ("unit_key", "run_id", "collection", "source_id", "snapshot_id", "attempted_at_utc", "reason", "code_sha256", "bundle_sha256")
    if not all(text(record[name]) for name in names):
        raise StaleRecordError("Stale record has an empty or non-text field")
    if not all(DIGEST.fullmatch(record[name]) for name in ("unit_key", "code_sha256", "bundle_sha256")):
        raise StaleRecordError("Stale record has a malformed digest")
    if RUN_ID.fullmatch(record["run_id"]) is None or COLLECTION.fullmatch(record["collection"]) is None or TIME.fullmatch(record["attempted_at_utc"]) is None:
        raise StaleRecordError("Stale record has a malformed run ID, collection or time")
    if record["reason"] not in REASONS:
        raise StaleRecordError("Stale record has a reason outside the fixed list")
    if record["http_status"] is not None and not (whole(record["http_status"]) and 100 <= record["http_status"] <= 599):
        raise StaleRecordError("Stale record has a status that is not an HTTP status code or null")
    if not isinstance(record["last_capture_verified"], bool) or not isinstance(record["last_capture"], list):
        raise StaleRecordError("Stale record has a malformed last capture")
    if not all(artifact_ok(item) for item in record["last_capture"]):
        raise StaleRecordError("Stale record names a malformed last capture object")
    if record["last_capture_verified"] and not record["last_capture"]:
        raise StaleRecordError("Stale record claims a verified last capture it does not name")


def build(
    *,
    unit_key: str,
    run_id: str,
    collection: str,
    source_id: str,
    snapshot_id: str,
    attempted_at_utc: str,
    reason: str,
    http_status: int | None,
    code_sha256: str,
    bundle_sha256: str,
    last_capture_verified: bool,
    last_capture: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """The record body, checked before it is returned."""
    record = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "unit_key": unit_key,
        "run_id": run_id,
        "collection": collection,
        "source_id": source_id,
        "snapshot_id": snapshot_id,
        "attempted_at_utc": attempted_at_utc,
        "reason": reason,
        "http_status": http_status,
        "code_sha256": code_sha256,
        "bundle_sha256": bundle_sha256,
        "last_capture_verified": last_capture_verified,
        "last_capture": sorted((dict(item) for item in last_capture), key=lambda item: item["key"]),
    }
    check(record)
    return record


def encode(record: Mapping[str, Any]) -> bytes:
    """The bytes a writer stores: the canonical form plus a final newline."""
    check(record)
    return canonical(record) + b"\n"


def parse(body: bytes, *, key: str | None = None, root: str | None = None) -> dict[str, Any]:
    """Read a stored record. With a key and a root, the key must be exactly the one the record's own unit and run name."""
    try:
        record = json.loads(body)
    except ValueError:
        raise StaleRecordError("Stale record is not JSON") from None
    check(record)
    if key is not None and root is not None and key != stale_key(root, record["unit_key"], record["run_id"]):
        raise StaleRecordError("Stale record differs from the unit and run its key names")
    return record
