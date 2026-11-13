"""The completion record of one ingestion unit: its schema, keys and checks, shared by the writer and every reader.

A unit is finished when ``<root>ingestion_units/<unit_key>/<payload_sha256>.json`` exists. ``unit_key`` is the SHA-256 of the unit's canonical request
identity, so it is known before any request. ``payload_sha256`` is the SHA-256 of the sorted (role ``data``, stored file name, SHA-256) triples, so an API
page that differs while the data files stay the same names the same record. The record is created once, after every object it names passed its version
read-back. When a unit's payload returns to an older record's payload while its newest record has another one (A, then B, then A), the run writes a
reversion record under ``<payload_sha256>.<run_id>.json`` that names the older record's objects, so the newest record is that payload again and nothing is
copied. Nothing in this module reads or writes storage.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from typing import Any

KIND = "ingestion_unit_completion"
SCHEMA_VERSION = 1
UNITS_FOLDER = "ingestion_units/"
STALE_FOLDER = "ingestion_stale/"
RUNS_FOLDER = "ingestion_runs/"
DIGEST = re.compile(r"[a-f0-9]{64}")
OBJECT_FIELDS = frozenset({"key", "version_id", "sha256", "byte_count"})
ROLE_FIELDS = OBJECT_FIELDS | {"role"}
DATA_FIELDS = ROLE_FIELDS | {"stored_file_name"}
RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}")
NAME = re.compile(rf"(?P<payload>[a-f0-9]{{64}})(?:\.(?P<run>{RUN_ID.pattern}))?\.json")
COLLECTION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*")
FIELDS = frozenset(
    {
        "kind",
        "schema_version",
        "unit_key",
        "payload_sha256",
        "source_id",
        "collection",
        "mode",
        "run_id",
        "code_sha256",
        "bundle_sha256",
        "retrieved_at_utc",
        "receipt",
        "manifests",
        "data_objects",
        "other_objects",
    }
)


class RecordError(ValueError):
    """A completion record that is malformed or does not agree with its own key. It is refused, never skipped over."""


def canonical(value: Any) -> bytes:
    """The one byte form every digest here hashes."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()


def sha256_text(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def unit_key(identity: Mapping[str, Any]) -> str:
    """SHA-256 of the canonical request identity: source, handler mode, canonical request, reviewed selection. Never a snapshot ID, a time or a run."""
    return sha256_text(canonical(identity))


def payload_sha256(data_files: Iterable[tuple[str, str]]) -> str:
    """SHA-256 over the sorted ("data", stored file name, SHA-256) triples of the data-role files only."""
    triples = sorted(["data", name, digest] for name, digest in data_files)
    return sha256_text(canonical(triples))


def unit_prefix(root: str, key: str) -> str:
    return f"{root}{UNITS_FOLDER}{key}/"


def stale_prefix(root: str, key: str) -> str:
    """The folder of a unit's stale records, ``<root>ingestion_stale/<unit_key>/``. It is separate from the unit folder, so a stale unit is never done."""
    return f"{root}{STALE_FOLDER}{key}/"


def record_key(root: str, key: str, payload: str, run_id: str | None = None) -> str:
    """The key of a completion record. A reversion record adds its run ID; a run ID that cannot join a key is refused."""
    if run_id is None:
        return f"{unit_prefix(root, key)}{payload}.json"
    if RUN_ID.fullmatch(run_id) is None:
        raise RecordError("Run ID cannot join a completion record key")
    return f"{unit_prefix(root, key)}{payload}.{run_id}.json"


def key_payload(key: str, root: str, unit: str) -> tuple[str, str | None] | None:
    """The payload digest and the run ID (None for the plain form) a listed key under a unit's folder names, or None when it is no completion record key."""
    prefix = unit_prefix(root, unit)
    found = NAME.fullmatch(key[len(prefix) :]) if key.startswith(prefix) else None
    return (found.group("payload"), found.group("run")) if found else None


def run_record_key(root: str, run_id: str) -> str:
    return f"{root}{RUNS_FOLDER}{run_id}/run.json"


def run_state_prefix(root: str, run_id: str) -> str:
    return f"{root}{RUNS_FOLDER}{run_id}/state/"


def stored_object(item: Mapping[str, Any]) -> dict[str, Any]:
    """The fields of one stored object a record names: key, exact version, digest and length."""
    return {"key": item["key"], "version_id": str(item["version_id"]), "sha256": item["sha256"], "byte_count": int(item["byte_count"])}


def role_object(item: Mapping[str, Any]) -> dict[str, Any]:
    """A stored object with the role the capture gave it: data, dictionary, methodology, reference, api_page, evidence and so on."""
    return stored_object(item) | {"role": item["role"]}


def build(
    *,
    key: str,
    source_id: str,
    collection: str,
    mode: str,
    run_id: str,
    code_sha256: str,
    bundle_sha256: str,
    retrieved_at_utc: str,
    receipt: Mapping[str, Any],
    manifests: Iterable[Mapping[str, Any]],
    data_objects: Iterable[Mapping[str, Any]],
    other_objects: Iterable[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """The record body. The payload digest is computed here from the data objects, so a writer cannot name another one."""
    data = [role_object(item) | {"stored_file_name": item["stored_file_name"]} for item in data_objects]
    record = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "unit_key": key,
        "payload_sha256": payload_sha256((item["stored_file_name"], item["sha256"]) for item in data),
        "source_id": source_id,
        "collection": collection,
        "mode": mode,
        "run_id": run_id,
        "code_sha256": code_sha256,
        "bundle_sha256": bundle_sha256,
        "retrieved_at_utc": retrieved_at_utc,
        "receipt": stored_object(receipt),
        "manifests": sorted((stored_object(item) for item in manifests), key=lambda item: item["key"]),
        "data_objects": sorted(data, key=lambda item: (item["stored_file_name"], item["key"])),
        "other_objects": sorted((role_object(item) for item in other_objects), key=lambda item: item["key"]),
    }
    check(record)
    return record


def encode(record: Mapping[str, Any]) -> bytes:
    """The bytes a writer stores. They are the canonical form plus a final newline."""
    return canonical(record) + b"\n"


def _object_ok(item: Any, fields: frozenset[str]) -> bool:
    return (
        isinstance(item, dict)
        and set(item) == fields
        and isinstance(item["key"], str)
        and bool(item["key"])
        and isinstance(item["version_id"], str)
        and item["version_id"] not in {"", "null"}
        and isinstance(item["sha256"], str)
        and DIGEST.fullmatch(item["sha256"]) is not None
        and isinstance(item["byte_count"], int)
        and not isinstance(item["byte_count"], bool)
        and item["byte_count"] >= 0
    )


def check(record: Any) -> None:
    """Refuse a record that is not exactly this schema or whose payload digest differs from its data objects."""
    if not (isinstance(record, dict) and set(record) == FIELDS and record["kind"] == KIND and record["schema_version"] == SCHEMA_VERSION):
        raise RecordError("Completion record has other fields than its schema")
    texts = ("unit_key", "payload_sha256", "source_id", "collection", "mode", "run_id", "code_sha256", "bundle_sha256", "retrieved_at_utc")
    if not all(isinstance(record[name], str) and record[name] for name in texts):
        raise RecordError("Completion record has an empty or non-text field")
    if not all(DIGEST.fullmatch(record[name]) for name in ("unit_key", "payload_sha256", "code_sha256", "bundle_sha256")):
        raise RecordError("Completion record has a malformed digest")
    data = record["data_objects"]
    lists = (record["manifests"], data, record["other_objects"])
    if not (_object_ok(record["receipt"], OBJECT_FIELDS) and all(isinstance(group, list) for group in lists) and bool(record["manifests"])):
        raise RecordError("Completion record has no receipt or no manifest")
    others = record["other_objects"]
    if not (
        all(_object_ok(item, OBJECT_FIELDS) for item in record["manifests"])
        and all(_object_ok(item, DATA_FIELDS) and item["role"] == "data" for item in data)
        and all(_object_ok(item, ROLE_FIELDS) and isinstance(item["role"], str) and item["role"] not in {"", "data"} for item in others)
    ):
        raise RecordError("Completion record names a malformed object or a wrong role")
    if COLLECTION.fullmatch(record["collection"]) is None:
        raise RecordError("Completion record collection is not <publisher>/<collection>")
    if record["payload_sha256"] != payload_sha256((item["stored_file_name"], item["sha256"]) for item in data):
        raise RecordError("Completion record payload digest differs from its data objects")


def reverted(older: Mapping[str, Any], *, run_id: str, code_sha256: str, bundle_sha256: str, retrieved_at_utc: str) -> dict[str, Any]:
    """The reversion record of an older record's payload: the older record's unit, payload and objects, under this run's identity and retrieval time."""
    return build(
        key=older["unit_key"],
        source_id=older["source_id"],
        collection=older["collection"],
        mode=older["mode"],
        run_id=run_id,
        code_sha256=code_sha256,
        bundle_sha256=bundle_sha256,
        retrieved_at_utc=retrieved_at_utc,
        receipt=older["receipt"],
        manifests=older["manifests"],
        data_objects=older["data_objects"],
        other_objects=older["other_objects"],
    )


def parse(body: bytes, *, key: str, root: str) -> dict[str, Any]:
    """Read a stored record by its key: the key must be ``<root>ingestion_units/<unit_key>/<payload_sha256>.json`` of the record's own digests, or the same
    name with the record's own run ID added (a reversion record), so a copied, renamed or edited record is refused."""
    try:
        record = json.loads(body)
    except ValueError:
        raise RecordError("Completion record is not JSON") from None
    check(record)
    plain = record_key(root, record["unit_key"], record["payload_sha256"])
    if key != plain and not (RUN_ID.fullmatch(record["run_id"]) and key == record_key(root, record["unit_key"], record["payload_sha256"], record["run_id"])):
        raise RecordError("Completion record differs from the unit and payload its key names")
    return record


def objects_named(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Every stored object the record names, each with the exact version a reader must check."""
    return [record["receipt"], *record["manifests"], *record["data_objects"], *record["other_objects"]]
