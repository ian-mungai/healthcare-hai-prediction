"""Perfect files supplied to a copy of the committed tree: internally consistent and correctly bound.

In step S4 none of them may close or reduce a readiness item. The ``add_`` builders write the files a case asks for and ``bind`` binds the copy with the lock
command, exactly as an owner would.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from scripts.acquisition import completion_record
from scripts.ingestion import registry
from tests.registry_records_support import iam_evidence, retention_input
from tests.registry_support import edit_json, replace, scope_report

FILES = "config/ingestion/evidence_files"
RETENTION = "config/lakehouse/retention_input.json"
IAM = "config/ingestion/iam_plan_evidence.json"
EVIDENCE = "config/ingestion/input_evidence.json"
MANUAL = "config/ingestion/manual_outcomes.json"
OBJECTS = {
    "cms_hai_hospital": ("cms/hospitals/datasets/x/Healthcare_Associated_Infections-Hospital.csv", "unit_records", "cms/hospitals", "main-hai-pdc"),
    "illinois_report_card_csv": ("illinois/hospital_report_card/datasets/il/report.csv", "inventory_record", "illinois/hospital_report_card", "IL"),
}


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def put(root: Path, relative: str, document: Any) -> str:
    """Write a JSON file of the copy and return its SHA-256."""
    body = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()
    replace(root, relative, body)
    return digest(body)


def record_for(table: str, listed_name: str | None = None) -> tuple[dict[str, Any], str, dict[str, Any]]:
    """A perfect record of the table's mode for its one object, the key its name must equal and the object's facts. ``listed_name`` relabels the file the
    record lists (a valid record of another table's file)."""
    key, mode, collection, source = OBJECTS[table]
    key = key.rsplit("/", 1)[0] + "/" + listed_name if listed_name else key
    facts = {"key": key, "version_id": "v1", "sha256": "2" * 64, "byte_count": 10}
    record: dict[str, Any]
    if mode == "inventory_record":
        manifest = {
            "key": f"{collection}/manifests/m.json",
            "version_id": "mv1",
            "sha256": "8" * 64,
            "snapshot_id": "s1",
            "source_id": source,
            "release_id": "r1",
        }
        row = {
            "manifest_key": manifest["key"],
            "manifest_version_id": "mv1",
            "object_key": key,
            "version_id": "v1",
            "sha256": "2" * 64,
            "byte_count": 10,
            "role": "data",
            "dataset_id": "il",
            "member_chain": [],
            "header_sha256": "9" * 64,
            "column_count": 2,
        }
        record = {
            "kind": "source_inventory",
            "version": 1,
            "collection": collection,
            "root": "",
            "created_at_utc": "2026-11-19T00:00:00+00:00",
            "created_by_run": "run1",
            "registry_lock_sha256": "7" * 64,
            "manifests": [manifest],
            "objects": [row],
            "supersedes": [],
        }
        return record, f"{collection}/inventory/{table}.json", facts
    unit = digest(table.encode())
    receipt = {"key": f"{collection}/audit/receipt.json", "version_id": "rv1", "sha256": "4" * 64, "byte_count": 5}
    record = completion_record.build(
        key=unit,
        source_id=source,
        collection=collection,
        mode="file",
        run_id="run1",
        code_sha256="5" * 64,
        bundle_sha256="6" * 64,
        retrieved_at_utc="2026-09-24T00:00:00+00:00",
        receipt=receipt,
        manifests=[{**receipt, "key": f"{collection}/manifests/m.json"}],
        data_objects=[{**facts, "role": "data", "stored_file_name": key.rsplit("/", 1)[-1]}],
    )
    return record, completion_record.record_key("", unit, record["payload_sha256"]), facts


def add_evidence(root: Path, table: str, record: Any = None, live: Any = None, listed_name: str | None = None) -> None:
    """Append perfect bound evidence for one real table: a record, a successful live check and the object, each replaceable."""
    built, key, facts = record_for(table, listed_name)
    claimed = record_for(table)[2]
    row = {**claimed, "manifest_key": "m.json", "manifest_version_id": "mv1", "role": "data", "dataset_id": "x", "name": claimed["key"].rsplit("/", 1)[-1]}
    row |= {"snapshot_id": "s1", "layout_sha256": "3" * 64}
    live_doc = {"kind": "live_object_check", "schema_version": 1, "passed": True, "objects": [{**claimed, "exists": True, "current": True}]}
    record_path, live_path = f"{FILES}/{table}_record.json", f"{FILES}/{table}_live.json"
    entry = {
        "table": table,
        "mode": OBJECTS[table][1],
        "map_sha256": registry.map_digest(root),
        "record": {"key": key, "version_id": "r1", "sha256": put(root, record_path, record or built), "path": record_path},
        "objects": [row],
        "live_check": {"path": live_path, "sha256": put(root, live_path, live or live_doc)},
    }
    edit_json(root, EVIDENCE, lambda document: document["tables"].append(entry))


def add_manual(root: Path, collection: str = "unrelated/collection") -> None:
    """Make every manual outcome completed, each with a bound record of its own unit and source (and the collection given)."""

    def change(document: Any) -> None:
        for number, row in enumerate(document["outcomes"]):
            record = {
                "kind": "manual_outcome_record",
                "schema_version": 1,
                "unit": row["unit"],
                "source_id": row["source_id"],
                "collection": collection,
                "status": "completed",
                "recorded_at_utc": "2026-11-19T00:00:00+00:00",
                "objects": [
                    {"key": "hrsa/x.csv", "version_id": "v1", "sha256": "2" * 64, "byte_count": 10, "role": "data", "name": "x.csv", "dataset_id": "x"}
                ],
            }
            path = f"{FILES}/manual_{number:02d}.json"
            row.update(status="completed", record={"key": f"manual/{number}.json", "version_id": "v1", "sha256": put(root, path, record), "path": path})

    edit_json(root, MANUAL, change)


def add_r4(root: Path, zeros: bool = False) -> None:
    """Perfect retention input and IAM plan evidence for the tree's own map (or all-zero placeholders of the same shape)."""
    names = [table["table"] for table in registry.scope.load_map(root / registry.TABLE_MAP)["tables"]]
    map_digest = registry.map_digest(root)
    retention = retention_input(names, candidate_map_sha256=map_digest, **({"generation_digest": "0" * 64} if zeros else {}))
    put(root, RETENTION, retention)
    put(root, IAM, iam_evidence(candidate_map_sha256=map_digest, **({"plan_sha256": "0" * 64, "commit": "0" * 40} if zeros else {})))


def bind(root: Path) -> None:
    """Bind the copy with the lock command, as an owner would after adding files."""
    registry.write_lock(root, scope_report())
