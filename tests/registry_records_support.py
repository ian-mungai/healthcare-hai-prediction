"""Valid fixtures for every file an owner may supply, and the synthetic tree they are assessed in (none of them closes a readiness item in step S4).

One valid fixture per record kind: the completion record (built by the real ``completion_record.build``), the inventory record, the manual outcome record, the
live check record, the retention input and the IAM plan evidence. A case edits a fixture and checks its shape.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from scripts.acquisition import completion_record
from scripts.ingestion import registry

COLLECTION = "hrsa/shortage_areas"
TABLES = {"hrsa_hpsa_detail": ("HPSA", "BCD_HPSA_FCT_DET_PC.csv"), "hrsa_mua_detail": ("MUA", "MUA_DET.csv")}
DECISIONS: dict[str, Any] = {
    name: {"value": "A", "recorded": "2026-11-18", "source": "synthetic", "meaning": "synthetic"} for name in ("D1", "D2", "D3", "census_retirement")
}
DECISIONS["Q4"] = {**DECISIONS["D1"], "value": "C", "interim": {"trigger": "t", "follow_up": "f", "accountable": "a person", "due": "a date"}}
MAP = "1" * 64
HPSA = "hrsa_hpsa_detail"


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def columns_sha256(columns: list[str]) -> str:
    return digest("\n".join(sorted(columns)).encode())


def write_json(root: Path, relative: str, document: Any) -> str:
    """Write a record file and return its SHA-256."""
    body = json.dumps(document).encode()
    (root / relative).parent.mkdir(parents=True, exist_ok=True)
    (root / relative).write_bytes(body)
    return digest(body)


def object_facts(table: str, version: str = "v1", sha: str = "2" * 64, size: int = 10, name: str | None = None) -> dict[str, Any]:
    """The facts of the one object of a table's fixture: key under the collection folder, version, hash and length."""
    source, file_name = TABLES[table]
    file_name = name or file_name
    return {"key": f"{COLLECTION}/datasets/{source.lower()}/{file_name}", "version_id": version, "sha256": sha, "byte_count": size}


def completion(
    table: str, facts: dict[str, Any] | list[dict[str, Any]] | None = None, source_id: str | None = None, **change: Any
) -> tuple[dict[str, Any], str]:
    """A real completion record for the table's object and the storage key its name must equal."""
    source, file_name = TABLES[table]
    listed = facts if isinstance(facts, list) else [facts or object_facts(table)]
    unit = digest(f"unit {table}".encode())
    receipt = {"key": f"{COLLECTION}/audit/receipt.json", "version_id": "rv1", "sha256": "4" * 64, "byte_count": 5}
    record = completion_record.build(
        key=unit,
        source_id=source_id or source,
        collection=COLLECTION,
        mode="file",
        run_id="run1",
        code_sha256="5" * 64,
        bundle_sha256="6" * 64,
        retrieved_at_utc="2026-09-24T00:00:00+00:00",
        receipt=receipt,
        manifests=[{**receipt, "key": f"{COLLECTION}/manifests/m.json"}],
        data_objects=[{**item, "role": "data", "stored_file_name": item["key"].rsplit("/", 1)[-1]} for item in listed],
    )
    record.update(change)
    return record, completion_record.record_key("", unit, record["payload_sha256"])


def inventory(table: str, facts: dict[str, Any] | None = None, **change: Any) -> dict[str, Any]:
    """An inventory record (design v4 section 7, version 1) that lists the table's object."""
    source, file_name = TABLES[table]
    facts = facts or object_facts(table)
    record: dict[str, Any] = {
        "kind": "source_inventory",
        "version": 1,
        "collection": COLLECTION,
        "root": "",
        "created_at_utc": "2026-11-19T00:00:00+00:00",
        "created_by_run": "run1",
        "registry_lock_sha256": "7" * 64,
        "manifests": [
            {
                "key": f"{COLLECTION}/manifests/m.json",
                "version_id": "mv1",
                "sha256": "8" * 64,
                "snapshot_id": "HRSA__0",
                "source_id": source,
                "release_id": "r1",
            }
        ],
        "objects": [
            {
                "manifest_key": f"{COLLECTION}/manifests/m.json",
                "manifest_version_id": "mv1",
                "object_key": facts["key"],
                "version_id": facts["version_id"],
                "sha256": facts["sha256"],
                "byte_count": facts["byte_count"],
                "role": "data",
                "dataset_id": source.lower(),
                "member_chain": [file_name],
                "header_sha256": "9" * 64,
                "column_count": 2,
            }
        ],
        "supersedes": [],
    }
    record.update(change)
    return record


def manual_record(table: str, unit: str = "u1", status: str = "completed", **change: Any) -> dict[str, Any]:
    """A manual outcome record: the unit's terminal result and the exact objects it names."""
    source, file_name = TABLES[table]
    facts = object_facts(table)
    record: dict[str, Any] = {
        "kind": "manual_outcome_record",
        "schema_version": 1,
        "unit": unit,
        "source_id": source,
        "collection": COLLECTION,
        "status": status,
        "recorded_at_utc": "2026-11-19T00:00:00+00:00",
        "objects": [{**facts, "role": "data", "name": file_name, "dataset_id": source.lower()}],
    }
    record.update(change)
    return record


def live_record(evidence: list[dict[str, Any]], **change: Any) -> dict[str, Any]:
    """A successful lead-run record of the live check: the run passed and each object exists, is current and has its version, hash and length."""
    rows = [{**{k: item[k] for k in ("key", "version_id", "sha256", "byte_count")}, "exists": True, "current": True} for item in evidence]
    return {"kind": "live_object_check", "schema_version": 1, "passed": True, "objects": rows, **change}


def retention_input(tables: list[str], **change: Any) -> dict[str, Any]:
    return {
        "kind": "retention_input",
        "schema_version": 1,
        "status": "exported",
        "exported_at_utc": "2026-11-19T00:00:00+00:00",
        "generation_digest": "a" * 64,
        "snapshot_ids": {"bronze": "1234"},
        "observation_counts": {name: 1 for name in tables},
        "ownership_groups": 44,
        "side_table_counts": {"stored_copies": 1, "column_map": 1, "file_preambles": 1},
        "candidate_map_sha256": MAP,
        **change,
    }


def iam_evidence(**change: Any) -> dict[str, Any]:
    return {
        "kind": "iam_plan_evidence",
        "schema_version": 1,
        "status": "planned",
        "created_at_utc": "2026-11-19T00:00:00+00:00",
        "plan_sha256": "b" * 64,
        "commit": "c" * 40,
        "candidate_map_sha256": MAP,
        **change,
    }


def evidence_entry(
    root: Path, table: str, mode: str = "unit_records", layout: list[str] | None = None, name: str | None = None, version: str = "v1"
) -> dict[str, Any]:
    """Valid bound evidence for one table: a record of the mode's kind, the object it lists, a successful live check and the digests that bind them."""
    source, file_name = TABLES[table]
    facts = object_facts(table, version=version, name=name)
    columns = layout or ["a", "b"]
    row = {
        **facts,
        "manifest_key": f"{COLLECTION}/manifests/m.json",
        "manifest_version_id": "mv1",
        "role": "data",
        "dataset_id": source.lower(),
        "name": name or file_name,
        "snapshot_id": "HRSA__0",
        "layout_sha256": columns_sha256(columns),
    }
    if mode == "unit_records":
        record, key = completion(table, facts)
    elif mode == "inventory_record":
        record, key = inventory(table, facts), f"{COLLECTION}/inventory/{table}.json"
    else:
        record, key = manual_record(table), f"manual/{table}.json"
    record_path, live_path = f"records/{table}_record.json", f"records/{table}_live.json"
    return {
        "table": table,
        "mode": mode,
        "map_sha256": MAP,
        "record": {"key": key, "version_id": "r1", "sha256": write_json(root, record_path, record), "path": record_path},
        "objects": [row],
        "live_check": {"path": live_path, "sha256": write_json(root, live_path, live_record([row]))},
    }


def synthetic(root: Path) -> tuple[registry.Tree, dict[str, Any]]:
    """The two HRSA tables after the role change: correct role and file name and one queued file unit each, with no evidence and no layout contract."""
    tables = [
        {
            "table": name,
            "group": "g",
            "collection": COLLECTION,
            "format": "csv",
            "member_pattern": f"^{file_name.replace('.', chr(92) + '.')}$",
            "roles": ["data"],
        }
        for name, (_, file_name) in TABLES.items()
    ]
    units: list[dict[str, Any]] = []
    specs: dict[str, dict[str, Any]] = {}
    for number, (_, (source, file_name)) in enumerate(TABLES.items()):
        snapshot = f"HRSA__{number}"
        units.append({"snapshot_id": snapshot, "source_id": source, "mode": "file", "route": "publisher_url"})
        specs[snapshot] = {"stored_data_files": [file_name], "receipt_template": {"role": "data"}}
    sources = [
        {"source_id": source, "acquisition": "scheduled", "admission": "unit_records", "scope": "whole_source", "decision_ref": None}
        for source in ("HPSA", "MUA")
    ]
    collections = {"decisions": DECISIONS, "collections": [{"collection": COLLECTION, "sources": sources}], "feeds": [], "gaps": {}, "plans": []}
    built = registry.Tree(
        root=root,
        units=units,
        specs=specs,
        routes={"HPSA": COLLECTION, "MUA": COLLECTION},
        tables=tables,
        collections=collections,
        golden={"kind": "unit_golden", "schema_version": 1, "counts": {"units": 0, "file": 0, "plan_mode": 0, "manual": 0, "receipt_bearing": 0}, "units": []},
        manual={"kind": "manual_outcomes", "schema_version": 1, "outcomes": []},
        contracts={"kind": "layout_contracts", "schema_version": 1, "tables": {}},
        lock={"map_sha256": MAP},
        source_registry={},
    )
    report = {"in_scope": [{"table": name} for name in TABLES], "dictionary": []}
    return built, report
