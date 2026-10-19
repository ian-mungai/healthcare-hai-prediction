"""Publish complete file observations and immutable physical references in one Iceberg snapshot."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
import tempfile
from collections import defaultdict
from collections.abc import Iterable
from contextlib import suppress
from datetime import date
from pathlib import Path
from typing import Any

from scripts.acquisition.single_copy import reference_scope
from scripts.acquisition.transport import CaptureError

TABLE = "file_metadata"
REFS_SQL = "SELECT snapshot_id FROM {namespace}.file_metadata.refs WHERE name = 'main'"
FIELDS = (
    "generation_row_count",
    "generation_object_count",
    "kind",
    "generation",
    "table_name",
    "observation_id",
    "_object_key",
    "canonical_object_key",
    "sha256",
    "bucket",
    "s3_key",
    "s3_version_id",
    "source_id",
    "snapshot_id",
    "dataset_id",
    "release_id",
    "release_partition",
    "manifest_key",
    "manifest_version_id",
    "manifest_sha256",
    "member_path",
    "file_name",
    "byte_count",
    "loaded",
    "retired",
    "failed_attempt",
    "manifest_entry_json",
    "manifest_evidence_json",
    "retirement_record_json",
    "superseded_by_manifest_key",
    "superseded_by_manifest_version_id",
    "canonical_previous_observation_id",
    "canonical_superseding_manifest_key",
    "canonical_superseding_manifest_version_id",
    "publication_release",
    "publication_date",
    "download_run_id",
    "release_type",
    "release_source",
)
BIGINT_FIELDS = {"byte_count", "generation_row_count", "generation_object_count"}
SCHEMA = ", ".join(
    f"{name} {'BIGINT' if name in BIGINT_FIELDS else 'BOOLEAN' if name in {'loaded', 'retired', 'failed_attempt'} else 'STRING'}" for name in FIELDS
)
CAPTURE_ID = re.compile(r".+__[0-9]{8}T[0-9]{6}Z__[a-f0-9]{32}")


class MetadataError(ValueError):
    """A generation is incomplete, inconsistent or changes an immutable reference."""


def release_label(member_path: str, release_id: str) -> dict[str, str | None]:
    """Classify a stored release label without treating a capture ID as a publication date."""
    nested = re.findall(r"(\d{4}-\d{2}-\d{2})\.zip!", member_path)
    source = "nested_archive" if nested else "container" if "!" in member_path else "release_partition"
    label = nested[-1] if nested else release_id
    publication = None
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", label):
        with suppress(ValueError):
            publication = date.fromisoformat(label).isoformat()
    download = label if CAPTURE_ID.fullmatch(label) else None
    return {
        "publication_release": label,
        "publication_date": publication,
        "download_run_id": download,
        "release_type": "publication_date" if publication else "download_run_id" if download else "unresolved",
        "release_source": source,
    }


def observation(item: dict[str, Any], loaded: bool) -> dict[str, Any]:
    """Return file metadata for one exact manifest entry, retaining its original physical identity."""
    row = dict.fromkeys(FIELDS)
    path = "!".join(item["member_chain"])
    row.update(
        kind="observation",
        table_name=item["table"],
        observation_id=item["observation_id"],
        _object_key=item["object_key"],
        sha256=item["sha256"],
        bucket=item["bucket"],
        s3_key=item["key"],
        s3_version_id=item["version_id"],
        source_id=item["source_id"],
        snapshot_id=item["snapshot_id"],
        dataset_id=item["dataset_id"],
        release_id=item["release_id"],
        release_partition=item["release_partition"],
        manifest_key=item["manifest_key"],
        manifest_version_id=item["manifest_version_id"],
        manifest_sha256=item["manifest_sha256"],
        member_path=path,
        file_name=item["file_name"],
        byte_count=int(item["byte_count"]),
        loaded=loaded,
        retired=bool(item.get("retired")),
        failed_attempt=bool(item.get("failed_attempt")),
        **{
            field: item.get(field)
            for field in (
                "manifest_entry_json",
                "manifest_evidence_json",
                "retirement_record_json",
                "superseded_by_manifest_key",
                "superseded_by_manifest_version_id",
            )
        },
        **release_label(path, item["release_id"]),
    )
    return row


def generation_digest(rows: list[dict[str, Any]]) -> str:
    """Hash the complete sorted generation content independently of input listing order."""
    records = sorted(json.dumps({key: value for key, value in row.items() if key != "generation"}, sort_keys=True) for row in rows)
    return hashlib.sha256(json.dumps(records).encode()).hexdigest()


def live(row: dict[str, Any]) -> bool:
    """A retired or failed attempt never establishes a live physical version."""
    return not row["retired"] and not row["failed_attempt"]


def validate(rows: list[dict[str, Any]]) -> None:
    """Reconcile each checksum once, accepting observation-only history only for retired or failed attempts."""
    if not rows or not any(row["kind"] == "observation" for row in rows) or any(row["kind"] not in {"observation", "object"} for row in rows):
        raise MetadataError("metadata generation requires observation rows and only known metadata kinds")
    if {row["generation"] for row in rows} != {generation_digest(rows)}:
        raise MetadataError("metadata generation is partial or its complete-content digest differs")
    objects = [row for row in rows if row["kind"] == "object"]
    if any(row["generation_row_count"] != len(rows) or row["generation_object_count"] != len(objects) for row in rows):
        raise MetadataError("metadata generation row counts are incomplete")
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    identities = set()
    for row in rows:
        if row["kind"] == "observation":
            groups[row["sha256"]].append(row)
            identities.add(row["observation_id"])
    index = {row["sha256"]: row for row in objects}
    if len(index) != len(objects) or len(identities) != sum(map(len, groups.values())):
        raise MetadataError("metadata generation repeats an object or observation identity")
    if set(index) - set(groups):
        raise MetadataError("physical object has no manifest observation")
    for digest, matches in groups.items():
        if not re.fullmatch(r"[a-f0-9]{64}", digest):
            raise MetadataError("observation requires a SHA-256")
        active = [row for row in matches if live(row)]
        obj = index.get(digest)
        if not active:
            if obj is not None or any(row["loaded"] or row["canonical_object_key"] is not None for row in matches):
                raise MetadataError("retired or failed-only observations cannot carry a live object or canonical reference")
            continue
        if obj is None:
            raise MetadataError("live observations require both object and observation kinds and one live physical object")
        if any(not isinstance(obj[field], str) or not obj[field] for field in ("bucket", "s3_key", "s3_version_id")) or obj["s3_version_id"] == "null":
            raise MetadataError("physical object requires an exact immutable version")
        physical = {(row["bucket"], row["s3_key"], row["s3_version_id"], row["byte_count"]) for row in active}
        identity = (obj["bucket"], obj["s3_key"], obj["s3_version_id"], obj["byte_count"])
        canonical = [row for row in active if row["loaded"] and not row["superseded_by_manifest_key"]]
        if (
            physical != {identity}
            or len(canonical) != 1
            or (canonical[0]["_object_key"], canonical[0]["observation_id"]) != (obj["_object_key"], obj["observation_id"])
        ):
            raise MetadataError("every live checksum must resolve to exactly one live physical version and one canonical observation")
        if {row["table_name"] for row in matches} != {obj["table_name"]}:
            raise MetadataError("a checksum spans tables and requires a reviewed bootstrap mapping")
        if any(row["canonical_object_key"] != obj["_object_key"] for row in matches):
            raise MetadataError("observation canonical reference differs from the immutable object index")


def canonical_transfer(old: dict[str, Any], new: dict[str, Any], observations: dict[str, dict[str, Any]]) -> bool:
    """Permit only the reviewed supersession transfer, preserving logical key, date and exact physical version."""
    prior = observations.get(old["observation_id"])
    if prior is None or not prior["superseded_by_manifest_key"]:
        return False
    same = ("table_name", "_object_key", "sha256", "bucket", "s3_key", "s3_version_id", "byte_count", "publication_date")
    return (
        all(old[field] == new[field] for field in same)
        and (prior["superseded_by_manifest_key"], prior["superseded_by_manifest_version_id"]) == (new["manifest_key"], new["manifest_version_id"])
        and live(new)
        and not new["superseded_by_manifest_key"]
    )


def build(
    names: Iterable[str], inputs: list[dict[str, Any]], excluded: list[dict[str, Any]], previous: list[dict[str, Any]], all_tables: Iterable[str]
) -> list[dict[str, Any]]:
    """Retain every observation and untouched table, transferring canonical lineage only through a validated correction."""
    if previous:
        validate(previous)
    chosen, complete_tables = set(names), set(all_tables)
    if not previous and chosen != complete_tables:
        raise MetadataError("first metadata publication requires the full committed bronze table map")
    rows = [observation(item, True) for item in inputs] + [observation(entry["copy"], False) for entry in excluded if "copy" in entry]
    rows = [row for row in rows if row["table_name"] in chosen]
    current = {row["observation_id"]: row for row in rows}
    mutable = {
        "generation",
        "generation_row_count",
        "generation_object_count",
        "canonical_object_key",
        "loaded",
        "retired",
        "superseded_by_manifest_key",
        "superseded_by_manifest_version_id",
        "retirement_record_json",
    }
    for old in previous:
        if old["kind"] == "observation" and old["table_name"] in chosen:
            found = current.get(old["observation_id"])
            if found is None or any(found[field] != old[field] for field in FIELDS if field not in mutable):
                raise MetadataError("an immutable manifest observation changed or disappeared from discovery")
    old_objects = {row["sha256"]: row for row in previous if row["kind"] == "object"}
    selected = {row["sha256"]: row for row in rows if row["loaded"]}
    for digest, old in old_objects.items():
        if old["table_name"] not in chosen:
            continue
        new = selected.get(digest)
        if new is None:
            if any(live(row) for row in rows if row["sha256"] == digest):
                raise MetadataError(f"{old['table_name']}: canonical key {old['_object_key']} has no eligible replacement; reviewed mapping required")
            continue
        if new["observation_id"] != old["observation_id"]:
            if not canonical_transfer(old, new, current):
                raise MetadataError(f"{old['table_name']}: canonical key {old['_object_key']} lacks an exact superseding entry; reviewed mapping required")
            obj = {
                **old,
                "observation_id": new["observation_id"],
                "canonical_previous_observation_id": old["observation_id"],
                "canonical_superseding_manifest_key": new["manifest_key"],
                "canonical_superseding_manifest_version_id": new["manifest_version_id"],
            }
        else:
            frozen = ("table_name", "_object_key", "sha256", "bucket", "s3_key", "s3_version_id", "byte_count", "publication_date")
            if any(old[field] != new[field] for field in frozen):
                raise MetadataError("bootstrap object identity or canonical lineage changed")
            obj = dict(old)
        rows.append(obj)
    rows += [{**row, "kind": "object"} for digest, row in selected.items() if digest not in old_objects]
    rows += [dict(row) for row in previous if row["table_name"] not in chosen]
    if {row["table_name"] for row in rows if row["kind"] == "observation"} != complete_tables:
        raise MetadataError("metadata generation is missing a committed bronze table or contains an undeclared table")
    objects = {row["sha256"]: row for row in rows if row["kind"] == "object"}
    for row in rows:
        row["canonical_object_key"] = objects[row["sha256"]]["_object_key"] if row["sha256"] in objects else None
        row["generation_row_count"], row["generation_object_count"] = len(rows), len(objects)
    digest = generation_digest(rows)
    for row in rows:
        row["generation"] = digest
    validate(rows)
    return sorted(rows, key=lambda row: (row["kind"], row["sha256"], row["observation_id"] or ""))


def snapshot(spark: Any, namespace: str) -> int | None:
    """Return the current metadata snapshot, rejecting an existing table with no published generation."""
    if not re.fullmatch(r"[a-z][a-z0-9_]*", namespace):
        raise MetadataError("metadata namespace must be a plain identifier")
    if not spark.catalog.tableExists(f"{namespace}.{TABLE}"):
        return None
    refs = spark.sql(REFS_SQL.format(namespace=namespace)).collect()
    if len(refs) != 1 or refs[0]["snapshot_id"] is None:
        raise MetadataError("metadata backing table has no complete published snapshot")
    return int(refs[0]["snapshot_id"])


def read(spark: Any, namespace: str, snapshot_id: int | None = None) -> list[dict[str, Any]]:
    """Read one exact metadata generation, or return an empty initial bootstrap when its table is absent."""
    head = snapshot(spark, namespace) if snapshot_id is None else snapshot_id
    if head is None:
        return []
    rows = [row.asDict() for row in spark.read.option("versionAsOf", str(head)).table(f"{namespace}.{TABLE}").collect()]
    validate(rows)
    return rows


def validate_successor(previous: list[dict[str, Any]], rows: list[dict[str, Any]]) -> None:
    """Reject observation loss, resurrection and physical remapping; allow audited supersession and full retirement."""
    current = {(row["kind"], row["observation_id"]): row for row in rows}
    observations = {row["observation_id"]: row for row in rows if row["kind"] == "observation"}
    objects = {row["sha256"]: row for row in rows if row["kind"] == "object"}
    mutable = {"generation", "generation_row_count", "generation_object_count", "retired", "retirement_record_json", "canonical_object_key"}
    for old in previous:
        found = current.get((old["kind"], old["observation_id"]))
        if old["kind"] == "object" and found is None:
            successor = objects.get(old["sha256"])
            if successor is None and not any(live(row) for row in observations.values() if row["sha256"] == old["sha256"]):
                continue
            candidate = observations.get(successor["observation_id"]) if successor else None
            if candidate is not None and canonical_transfer(old, candidate, observations) and successor is not None:
                expected = {
                    **old,
                    "observation_id": candidate["observation_id"],
                    "canonical_previous_observation_id": old["observation_id"],
                    "canonical_superseding_manifest_key": candidate["manifest_key"],
                    "canonical_superseding_manifest_version_id": candidate["manifest_version_id"],
                }
                if all(expected[field] == successor[field] for field in FIELDS if field not in mutable):
                    continue
        observation_mutable = (
            mutable | {"loaded", "superseded_by_manifest_key", "superseded_by_manifest_version_id"} if old["kind"] == "observation" else mutable
        )
        if found is None or any(found[field] != old[field] for field in FIELDS if field not in observation_mutable):
            raise MetadataError("publication loses or changes an immutable metadata identity")
        if old["retired"] and not found["retired"]:
            raise MetadataError("publication resurrects a retired physical version")
        if old["kind"] == "observation":
            prior = (old["superseded_by_manifest_key"], old["superseded_by_manifest_version_id"])
            after = (found["superseded_by_manifest_key"], found["superseded_by_manifest_version_id"])
            if prior != after and (
                any(prior) or not all(after) or not any((row["manifest_key"], row["manifest_version_id"]) == after for row in observations.values())
            ):
                raise MetadataError("publication changes supersession without an exact replacing manifest")


def publish(spark: Any, namespace: str, rows: list[dict[str, Any]]) -> str:
    """Publish one complete generation with conflict detection, retaining the snapshot on an identical retry."""
    lit = importlib.import_module("pyspark.sql.functions").lit
    validate(rows)
    head = snapshot(spark, namespace)
    previous = read(spark, namespace, head) if head is not None else []
    validate_successor(previous, rows)
    if previous and previous[0]["generation"] == rows[0]["generation"]:
        if snapshot(spark, namespace) != head:
            raise MetadataError("metadata snapshot changed during idempotent publication")
        return str(rows[0]["generation"])
    frame = spark.createDataFrame([tuple(row[field] for field in FIELDS) for row in rows], SCHEMA)
    writer = frame.writeTo(f"{namespace}.{TABLE}")
    if head is None:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        writer.using("iceberg").tableProperty("format-version", "2").create()
    else:
        writer.option("isolation-level", "serializable").option("validate-from-snapshot-id", str(head)).overwrite(lit(True))
    return str(rows[0]["generation"])


def bootstrap_bytes(rows: list[dict[str, Any]]) -> bytes:
    """Export the verified generation's immutable checksum-to-version map for acquisition review."""
    validate(rows)
    try:
        scopes = {reference_scope(row["s3_key"], row["sha256"]) for row in rows if row["kind"] == "object"}
    except CaptureError as error:
        raise MetadataError("bootstrap object is outside the data-object scope") from error
    if scopes - {"objects"}:
        raise MetadataError("bootstrap object is outside the data-object scope")
    document = {
        "schema_version": 2,
        "generation": rows[0]["generation"],
        "evidence_objects": {},
        "objects": {
            row["sha256"]: {
                "bucket": row["bucket"],
                "key": row["s3_key"],
                "version_id": row["s3_version_id"],
                "sha256": row["sha256"],
                "byte_count": row["byte_count"],
            }
            for row in rows
            if row["kind"] == "object"
        },
    }
    return (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()


def write_bootstrap(rows: list[dict[str, Any]], output: Path) -> str:
    """Publish a complete generation's bootstrap index once and return its content SHA-256."""
    content = bootstrap_bytes(rows)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        if output.read_bytes() != content:
            raise MetadataError("bootstrap output already exists with different immutable content")
    else:
        with tempfile.TemporaryDirectory(prefix="bootstrap_", dir=output.parent) as folder:
            path = Path(folder) / "index.json"
            with path.open("xb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(path, output)
            except FileExistsError:
                if output.read_bytes() != content:
                    raise MetadataError("concurrent bootstrap publication differs") from None
    return hashlib.sha256(content).hexdigest()
