"""Choose bronze's inputs from the ingestion records of each unit instead of listing every manifest under a root.

Used only while the raw read capability is on (``HAI_READ_RAW_ROOT=true``). For each unit, the newest completion record names the manifests and the
stored objects bronze loads. A unit whose newest stale record is not older than its newest completion record loads the last capture that stale record
points to, labelled stale. That pointer must name objects of the unit's newest completion record, or the run stops. Records are read from
``raw/ingestion_units/`` and ``raw/ingestion_stale/`` alone, never from ``ingestion_test/``. A record that cannot be read, disagrees with its own key,
or names an object its manifest does not list stops the run and is named. A malformed record stops only the tables of its own collection. The record
schemas are ``scripts/acquisition/completion_record.py`` and ``scripts/acquisition/stale_record.py``, the modules their writers use.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from scripts.acquisition import completion_record, stale_record
from scripts.acquisition.raw_root import RAW_ROOT
from scripts.lakehouse.bronze import SINGLE_DOCUMENT, BronzeError, Storage

TEST_FOLDER = "ingestion_test/"
MISSING_CODES = frozenset({"NoSuchKey", "NoSuchVersion", "404"})
MOMENT = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})")
Identity = tuple[str, str]
ManifestLister = Callable[[str], dict[Identity, bytes]]


@dataclass
class Selection:
    """The manifests to read and the objects to take from them, for each collection."""

    manifests: dict[str, dict[Identity, bytes]] = field(default_factory=lambda: defaultdict(dict))
    named: dict[str, dict[Identity, dict[str, Any]]] = field(default_factory=lambda: defaultdict(dict))
    superseded: dict[str, dict[Identity, dict[str, str]]] = field(default_factory=lambda: defaultdict(dict))

    def confirm(self, collection: str, identity: Identity, stored: dict[str, Any]) -> bool:
        """Whether a record names this manifest entry. A named object whose digest, length or role differs from its manifest entry stops the run."""
        expected = self.named[collection].get(identity)
        if expected is None:
            return False
        found = stored["object"]
        if (expected["sha256"], expected["byte_count"]) != (found.get("sha256"), found.get("byte_count")) or expected["role"] not in {None, stored.get("role")}:
            raise BronzeError(f"{expected['record']}: the record's SHA-256, length or role for {identity[0]} differs from its manifest")
        return True


def moment(value: Any, key: str) -> datetime:
    """A record time in UTC. The capture writer stamps ``datetime.isoformat()``, so a ``Z`` or an offset and fractional seconds are read."""
    parsed = None
    if isinstance(value, str) and MOMENT.fullmatch(value) is not None:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            parsed = None
    if parsed is None:
        raise BronzeError(f"{key}: a record time is not a valid ISO 8601 time with a Z or an offset")
    return parsed.astimezone(UTC)


def parse_completion(body: bytes, *, key: str, root: str) -> dict[str, Any]:
    """Read one completion record with the writer's own parser. The message names the record."""
    try:
        record = completion_record.parse(body, key=key, root=root)
    except completion_record.RecordError as error:
        raise BronzeError(f"{key}: {error}") from None
    moment(record["retrieved_at_utc"], key)
    return record


def parse_stale(body: bytes, *, key: str, root: str) -> dict[str, Any]:
    """Read one stale record with the writer's own parser, then apply the rules only bronze adds: the roots the pointers may name and valid times."""
    try:
        record = stale_record.parse(body, key=key, root=root)
    except stale_record.StaleRecordError as error:
        raise BronzeError(f"{key}: {error}") from None
    for pointer in record["last_capture"]:
        if not key_in_root(pointer["key"], pointer["read_root"]):
            raise BronzeError(f"{key}: the stale record names a last capture object outside its read root or in the test folder")
        moment(pointer["retrieved_at_utc"], key)
    moment(record["attempted_at_utc"], key)
    return record


def key_in_root(key: str, read_root: str) -> bool:
    """Whether the key sits in the read root it is listed under, and outside the test folder."""
    return key.startswith(RAW_ROOT) == (read_root == RAW_ROOT) and not key.removeprefix(RAW_ROOT).startswith(TEST_FOLDER)


def collection_of(body: bytes) -> str | None:
    """The collection a record claims, read without checking the rest, or None when the body names none."""
    try:
        record = json.loads(body)
    except ValueError:
        return None
    return record["collection"] if isinstance(record, dict) and isinstance(record.get("collection"), str) else None


def read_records(
    storage: Storage, bucket: str, tables: dict[str, list[str]]
) -> tuple[dict[str, list[tuple[str, str, dict]]], dict[str, list[tuple[str, str, dict]]]]:
    """Read the records of the selected tables' collections. A malformed one stops the run naming its tables and itself. A record of another collection is left
    alone, and one that names no collection cannot be told apart, so it stops the run."""
    completions: dict[str, list[tuple[str, str, dict]]] = defaultdict(list)
    stales: dict[str, list[tuple[str, str, dict]]] = defaultdict(list)
    folders = ((completion_record.UNITS_FOLDER, completions, parse_completion), (stale_record.STALE_FOLDER, stales, parse_stale))
    for folder, found, parse in folders:
        for key, version in storage.list_current(bucket, f"{RAW_ROOT}{folder}"):
            body = storage.get(bucket, key, version)
            collection = collection_of(body)
            if collection is not None and collection not in tables:
                continue
            try:
                record = parse(body, key=key, root=RAW_ROOT)
            except BronzeError as error:
                raise BronzeError(f"{', '.join(tables[collection]) if collection is not None else 'the selected tables'}: {error}") from None
            found[record["unit_key"]].append((key, version, record))
    return completions, stales


def newest(entries: list[tuple[str, str, dict]], name: str, label: str, unit: str, tables: str) -> tuple[str, str, dict]:
    """The record with the latest time. Two records with the latest time stop the run, so no pick is silent."""
    ordered = sorted(entries, key=lambda entry: moment(entry[2][name], entry[0]))
    if len(ordered) > 1 and moment(ordered[-1][2][name], ordered[-1][0]) == moment(ordered[-2][2][name], ordered[-2][0]):
        raise BronzeError(f"{tables}: unit {unit} has two {label} records with the same retrieval time ({ordered[-2][0]}, {ordered[-1][0]})")
    return ordered[-1]


def guard_key(key: str, record_key: str) -> None:
    """A record never makes bronze read the test folder."""
    if key.removeprefix(RAW_ROOT).startswith(TEST_FOLDER):
        raise BronzeError(f"{record_key}: the record names an object in the test folder, which bronze never reads")


def fetch_manifest(storage: Storage, bucket: str, collection: str, ref: dict[str, Any], record_key: str) -> bytes:
    """Read a named manifest by its exact version and check it against the record's digest and length."""
    if not str(ref["key"]).startswith(f"{RAW_ROOT}{collection}/manifests/"):
        raise BronzeError(f"{record_key}: a named manifest is not under the record's collection")
    try:
        raw = storage.get(bucket, ref["key"], ref["version_id"])
    except Exception as error:
        if isinstance(error, KeyError) or getattr(error, "response", {}).get("Error", {}).get("Code") in MISSING_CODES:
            raise BronzeError(f"{record_key}: the manifest {ref['key']} is not stored at the version the record names") from None
        raise
    if hashlib.sha256(raw).hexdigest() != ref["sha256"] or len(raw) != ref["byte_count"]:
        raise BronzeError(f"{record_key}: the manifest {ref['key']} differs from the digest or length the record names")
    return raw


def holds(manifest: dict[str, Any], pointer: dict[str, Any]) -> bool:
    """Whether a manifest lists the pointed object by key, exact version and SHA-256."""
    if "objects" not in manifest:
        return set(manifest) >= SINGLE_DOCUMENT and manifest["sha256"] == pointer["sha256"] and f"/{pointer['sha256']}/" in pointer["key"]
    return any(
        entry.get("object", {}).get("key") == pointer["key"]
        and entry["object"].get("version_id") == pointer["version_id"]
        and entry["object"].get("sha256") == pointer["sha256"]
        for entry in manifest["objects"]
    )


def label(freshness: str, reason: str | None, stale_record: dict[str, str] | None, retrieved: str) -> dict[str, Any]:
    return {"freshness": freshness, "stale_reason": reason, "stale_record": stale_record, "last_retrieved_at_utc": retrieved}


def named_objects(record: dict[str, Any]) -> list[Identity]:
    """The exact objects a completion or stale record names for loading."""
    if record["kind"] == completion_record.KIND:
        return [(item["key"], item["version_id"]) for item in (*record["data_objects"], *record["other_objects"])]
    return [(pointer["key"], pointer["version_id"]) for pointer in record["last_capture"]]


def select(storage: Storage, bucket: str, tables: dict[str, list[str]], manifests_of: ManifestLister) -> Selection:
    """Pick, for the chosen tables' collections, the manifests and objects the newest record of each unit names. ``tables`` maps collection to table names.

    ``manifests_of`` lists the manifests under one collection prefix. Every object an older record of a unit names and the unit's newest record does not is
    superseded: the loader keeps it as a retired observation and never loads it.
    """
    completions, stales = read_records(storage, bucket, tables)
    selection = Selection()
    older: dict[str, dict[Identity, dict[str, str]]] = defaultdict(dict)
    for unit in sorted(set(completions) | set(stales)):
        found = [*completions.get(unit, []), *stales.get(unit, [])]
        collection = found[0][2]["collection"]
        if any(entry[2]["collection"] != collection for entry in found):
            raise BronzeError(f"unit {unit}: its records name different collections ({found[0][0]})")
        names = ", ".join(tables[collection])
        done = newest(completions[unit], "retrieved_at_utc", "completion", unit, names) if unit in completions else None
        late = newest(stales[unit], "attempted_at_utc", "stale", unit, names) if unit in stales else None
        if done is not None and (late is None or moment(done[2]["retrieved_at_utc"], done[0]) > moment(late[2]["attempted_at_utc"], late[0])):
            chosen = done
            take_completion(storage, bucket, selection, collection, done)
        elif late is not None:
            if done is not None:
                refuse_older_pointer(done, late, unit, names)
            chosen = late
            take_stale(storage, bucket, selection, collection, late, names, manifests_of)
        for entry in found:
            if entry is not chosen:
                for identity in named_objects(entry[2]):
                    older[collection].setdefault(identity, {"record_key": entry[0], "record_version_id": entry[1]})
    for collection, identities in older.items():
        selection.superseded[collection] = {identity: info for identity, info in identities.items() if identity not in selection.named[collection]}
    return selection


def refuse_older_pointer(done: tuple[str, str, dict], late: tuple[str, str, dict], unit: str, names: str) -> None:
    """A stale record may only point at the objects of the unit's newest completion record. Any other pointer names a capture that record superseded."""
    live = set(named_objects(done[2]))
    behind = sorted(identity[0] for identity in named_objects(late[2]) if identity not in live)
    if behind:
        raise BronzeError(
            f"{names}: stale pointer older than the newest completion: unit {unit}, record {late[0]} names {', '.join(behind)}, "
            f"which the newest completion {done[0]} does not name"
        )


def take_completion(storage: Storage, bucket: str, selection: Selection, collection: str, done: tuple[str, str, dict]) -> None:
    key, _, record = done
    for ref in record["manifests"]:
        selection.manifests[collection][(ref["key"], ref["version_id"])] = fetch_manifest(storage, bucket, collection, ref, key)
    for item in (*record["data_objects"], *record["other_objects"]):
        guard_key(item["key"], key)
        selection.named[collection][(item["key"], item["version_id"])] = {
            "sha256": item["sha256"],
            "byte_count": item["byte_count"],
            "role": item["role"],
            "record": key,
            "label": label("current", None, None, record["retrieved_at_utc"]),
        }


def take_stale(
    storage: Storage,
    bucket: str,
    selection: Selection,
    collection: str,
    late: tuple[str, str, dict],
    names: str,
    manifests_of: ManifestLister,
) -> None:
    key, version, record = late
    missing = f"{names}: the last capture of unit {record['unit_key']} is missing or unverified ({key}), so nothing can be loaded"
    if not record["last_capture_verified"] or not record["last_capture"]:
        raise BronzeError(missing)
    for pointer in record["last_capture"]:
        guard_key(pointer["key"], key)
        pool = manifests_of(f"{pointer['read_root']}{collection}/")
        holders = {identity: raw for identity, raw in pool.items() if holds(json.loads(raw), pointer)}
        if not holders:
            raise BronzeError(missing)
        selection.manifests[collection].update(holders)
        selection.named[collection].setdefault(
            (pointer["key"], pointer["version_id"]),
            {
                "sha256": pointer["sha256"],
                "byte_count": pointer["byte_count"],
                "role": None,
                "record": key,
                "label": label("stale", record["reason"], {"key": key, "version_id": version}, pointer["retrieved_at_utc"]),
            },
        )
