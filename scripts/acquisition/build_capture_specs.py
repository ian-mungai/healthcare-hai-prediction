"""One-time development step: derive the capture specification from the kept stored receipts, job files and reconciliation files.

A bootstrap capture compares nothing with an old download, but it still needs facts the stored files hold: the stored data file names a collector must
produce, the expected format, reviewed unsigned redirect and receipt template of a file unit, the reviewed archive member map, the planned item of a
collector unit (its batch, release, year or file ID, or an MMD unit's measure and year) and the newest stored capture of a manual HUD or WONDER unit (object
keys and versions, without the bucket). A Census table unit has no stored receipt: its spec comes from the locked plan. A previous specification can
supply the specs of units that stay, for a queue that changed only in swapped units. The result has one entry per queue unit and no bucket name or local
path. The stored files are local only, so hosted CI cannot run this script. A receipt template carries the free text of old job plans: legacy agent-home
pointers are rewritten as in the registry, phone examples of a public dataset profile are redacted, and the document header counts both.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from scripts.acquisition import census_acs_tables_contract as tables_contract
from scripts.acquisition.check_baseline_run import SEED_MODES
from scripts.acquisition.data_paths import current
from scripts.acquisition.history_routes import RELEASE_BINDINGS
from scripts.acquisition.rewrite_registry_provenance import PHONE_PLACEHOLDER, POINTER_FROM, POINTER_TO

PLAN_ITEM_KEYS = {
    "bls_api_v2": "batch_id",
    "census_acs_api": "batch_id",
    "census_acs_detailed": "batch_id",
    "hud_api": "batch_id",
    "cms_owners_org": "release_id",
    "hcai_util_workbook": "year_id",
    "onc_mu_hospital": "file_id",
}
MMD_API_FLAG = "browser_export_moved_to_api"
TABLES_MODE = "census_acs_tables"
# A job plan binds a held source's reference document to a dated release record by that file's SHA-256. The specification names the record instead:
# a stored hash would bind the record the old capture used, which a clean checkout does not hold, and the hash text trips the secret scan.
BINDING_NAMES = {key: name for name, (key, _path) in RELEASE_BINDINGS.items()}
TEMPLATE_OMITS = ("route_id", "file_name", "expected_format")
PHONE = re.compile(r"\(\d{3}\) \d{3}-\d{4}")
FORMATS = {".csv": "csv", ".txt": "txt", ".zip": "zip", ".xlsx": "xlsx", ".xls": "xls", ".pdf": "pdf", ".json": "json", ".docx": "docx", ".gz": "gzip"}


def read_json(path: Path) -> dict[str, Any]:
    """Read one JSON file."""
    return json.loads(path.read_text(encoding="utf-8"))


def member_chain(text: str) -> list[str]:
    """A stored record's archive member: a list of nested names in JSON, or one name."""
    return list(json.loads(text)) if text.startswith("[") else [text]


def archive_members(folder: Path) -> list[dict[str, Any]]:
    """The reviewed member map of a stored capture: each member's role (data wins over reference) and dataset ID, sorted by member chain."""
    record = folder / "s3_collections_reconciliation.json"
    if not record.is_file():
        return []
    members: dict[tuple[str, ...], dict[str, Any]] = {}
    for item in read_json(record)["objects"]:
        if "archive_member" in item:
            chain = tuple(member_chain(item["archive_member"]))
            if chain not in members or item["role"] == "data":
                members[chain] = {"member_chain": list(chain), "role": "data" if item["role"] == "data" else "reference", "dataset_id": item["dataset_id"]}
    return [members[chain] for chain in sorted(members)]


def job_file(receipt: Path, base: Path) -> Path | None:
    """The nearest job.json above the receipt, inside the stored folder."""
    for parent in receipt.parents:
        if parent == base.parent:
            break
        if (parent / "job.json").is_file():
            return parent / "job.json"
    return None


def cleaned(value: Any, counts: Counter) -> Any:
    """A template value with each legacy agent-home pointer rewritten (as in the registry) and each phone example of a public dataset profile redacted."""
    if isinstance(value, str):
        counts["pointers"] += value.count(POINTER_FROM)
        counts["phone_examples"] += len(PHONE.findall(value))
        return PHONE.sub(PHONE_PLACEHOLDER, value.replace(POINTER_FROM, POINTER_TO))
    if isinstance(value, dict):
        return {key: cleaned(item, counts) for key, item in value.items()}
    if isinstance(value, list):
        return [cleaned(item, counts) for item in value]
    return value


def lineage_of(receipt: dict[str, Any]) -> dict[str, Any]:
    """The lineage record of a stored receipt: its extraction or query text parsed as JSON, or an empty record."""
    text = receipt.get("lineage", {}).get("extraction_or_query")
    return json.loads(text) if isinstance(text, str) and text.startswith("{") else {}


def release_binding(unit: dict[str, Any], bindings: dict[str, Any]) -> str:
    """The name of the tracked release record that a job plan's lineage binding stands for."""
    names = [BINDING_NAMES.get(key) for key in bindings]
    if len(names) != 1 or names[0] is None:
        raise ValueError(f"Unit {unit['snapshot_id']} has lineage bindings ({', '.join(sorted(bindings))}) that name no tracked release record")
    return names[0]


def template_from_receipt(unit: dict[str, Any], receipt: dict[str, Any]) -> dict[str, Any]:
    """The receipt template of a file unit that has no job file: its stored receipt holds the plan fields a capture copies."""
    lineage = lineage_of(receipt)
    source = receipt.get("source", {})
    fields = {
        "source_id": ("source.source_record_id", source.get("source_record_id")),
        "publisher": ("source.publisher", source.get("publisher")),
        "scope": ("lineage scope", lineage.get("scope")),
        "release": ("release", receipt.get("release")),
        "measurement_periods": ("measurement_periods", receipt.get("measurement_periods")),
        "governance": ("governance", receipt.get("governance")),
    }
    missing = [label for label, value in fields.values() if value is None or value == ""]
    if missing:
        raise ValueError(f"Unit {unit['snapshot_id']} has no job file and its receipt lacks {', '.join(missing)}")
    roles = [item["role"] for item in receipt["artifacts"]]
    template = {name: value for name, (_label, value) in fields.items()}
    template |= {
        "mode": lineage.get("mode", "file"),
        "role": "data" if "data" in roles else roles[0],
        "export_selections": receipt.get("acquisition", {}).get("export_selections", {}),
    }
    return template


def mmd_selection(unit: dict[str, Any], receipt: dict[str, Any], conditions: list[dict[str, Any]]) -> dict[str, Any]:
    """The measure and year of an MMD unit: an API capture records them, a browser export is matched by its condition, domain and county geography."""
    if unit["mode"] == "mmd_api":
        detail = lineage_of(receipt)
        return {"measure_id": detail["measure_id"], "year": int(detail["year"])}
    chosen = receipt["acquisition"]["export_selections"]
    matches = [item["measure_id"] for item in conditions if (item["condition_label"], item["domain_label"]) == (chosen["condition"], chosen["domain"])]
    if len(matches) != 1 or chosen["geography"] != "County":
        raise ValueError(f"Unit {unit['snapshot_id']} does not match exactly one planned county condition")
    return {"measure_id": matches[0], "year": int(chosen["year"])}


def last_capture(receipt_path: Path, receipt: dict[str, Any]) -> dict[str, Any] | None:
    """The newest stored capture of a manual HUD or WONDER unit from its reconciliation file: role, key, version, digest and size of each object, or None."""
    record = receipt_path.parent / "s3_collections_reconciliation.json"
    if not record.is_file():
        return None
    retrieved = receipt.get("acquisition", {}).get("retrieved_at_utc")
    artifacts = [
        {
            "role": item["role"],
            "key": item["object"]["key"],
            "version_id": item["object"]["version_id"],
            "sha256": item["object"]["sha256"],
            "byte_count": item["object"]["byte_count"],
            "retrieved_at_utc": retrieved,
        }
        for item in read_json(record)["objects"]
        if "archive_member" not in item and item["storage_path"] != "receipt.json"
    ]
    if not artifacts:
        return None
    root = "raw/" if all(item["key"].startswith("raw/") for item in artifacts) else ""
    return {"read_root": root, "artifacts": sorted(artifacts, key=lambda item: (item["role"], item["key"]))}


def tables_spec(unit: dict[str, Any], plan: dict[str, Any] | None) -> dict[str, Any]:
    """The specification of a Census table unit: the plan batch its snapshot ID names, with the one stored data file the collector writes."""
    found = [batch for batch in (plan or {"batches": []})["batches"] if tables_contract.queue_snapshot_id(batch) == unit["snapshot_id"]]
    if len(found) != 1:
        raise ValueError(f"Unit {unit['snapshot_id']} names no batch of the Census table plan")
    return {"mode": unit["mode"], "plan_item": found[0]["id"], "stored_data_files": [found[0]["file_name"]]}


def derive_unit(
    unit: dict[str, Any], base: Path, conditions: list[dict[str, Any]] | None = None, counts: Counter | None = None, tables_plan: dict[str, Any] | None = None
) -> dict[str, Any]:
    """The specification of one unit from its stored files under BASE (a Census table unit: from TABLES_PLAN)."""
    if unit["mode"] == TABLES_MODE:
        return tables_spec(unit, tables_plan)
    receipt_path = base / current(unit["receipt"])
    if not receipt_path.is_file():
        raise ValueError(f"Unit {unit['snapshot_id']} has no stored receipt")
    receipt = read_json(receipt_path)
    artifacts = receipt["artifacts"]
    manual = unit.get("disposition") == "approved_manual"
    spec: dict[str, Any] = {"mode": unit["mode"], "stored_data_files": sorted(item["stored_file_name"] for item in artifacts if item["role"] == "data")}
    moved_to_api = any(flag.startswith(MMD_API_FLAG) for flag in unit.get("flags", []))
    if unit["mode"] == "file":
        job = job_file(receipt_path, base)
        plan = read_json(job) if job else {}
        expected = plan.get("plan", {}).get("expected_format") or FORMATS.get(Path(artifacts[0]["stored_file_name"]).suffix.lower())
        redirect = plan.get("candidate", {}).get("reviewed_unsigned_redirect")
        spec |= ({"expected_format": expected} if expected else {}) | ({"reviewed_unsigned_redirect": redirect} if redirect else {})
        if not manual and not moved_to_api:
            template = (
                {key: value for key, value in plan["plan"].items() if key not in TEMPLATE_OMITS} if plan.get("plan") else template_from_receipt(unit, receipt)
            )
            if bindings := template.pop("lineage_bindings", None):
                spec["release_binding"] = release_binding(unit, bindings)
            spec["receipt_template"] = cleaned(template, counts if counts is not None else Counter())
    if unit["mode"] == "mmd_api" or moved_to_api:
        spec["mmd"] = mmd_selection(unit, receipt, conditions or [])
    elif unit["mode"] in PLAN_ITEM_KEYS:
        item = lineage_of(receipt).get(PLAN_ITEM_KEYS[unit["mode"]])
        if not isinstance(item, str) or not item:
            raise ValueError(f"Unit {unit['snapshot_id']} has no planned item ID ({PLAN_ITEM_KEYS[unit['mode']]}) in its receipt lineage")
        spec["plan_item"] = item
    if manual and unit["mode"] in SEED_MODES and (capture := last_capture(receipt_path, receipt)):
        spec["last_capture"] = capture
    members = archive_members(receipt_path.parent)
    if members:
        spec["archive_members"] = members
    return spec


def derive(
    units: list[dict[str, Any]],
    base: Path,
    queue_sha256: str,
    conditions: list[dict[str, Any]] | None = None,
    tables_plan: dict[str, Any] | None = None,
    previous: dict[str, Any] | None = None,
) -> bytes:
    """The capture specification bytes for UNITS: one entry per snapshot ID, sorted, bound to the queue's digest.

    CONDITIONS: the MMD plans'. TABLES_PLAN: the locked Census table plan. PREVIOUS: an earlier specification whose specs stay for the units it names.
    """
    counts: Counter = Counter()
    kept = (previous or {}).get("specs", {})
    specs = {
        unit["snapshot_id"]: kept[unit["snapshot_id"]] if unit["snapshot_id"] in kept else derive_unit(unit, base, conditions, counts, tables_plan)
        for unit in sorted(units, key=lambda item: item["snapshot_id"])
    }
    rewrites = (previous or {}).get("template_rewrites") or {"pointers": counts["pointers"], "phone_examples": counts["phone_examples"]}
    document = {"kind": "capture_specs", "queue_sha256": queue_sha256, "units": len(specs), "template_rewrites": rewrites, "specs": specs}
    return (json.dumps(document, indent=1, sort_keys=True) + "\n").encode()


def main() -> None:
    """Derive the specification for the tracked queue from the stored files under MIRROR and write it to OUTPUT."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("queue", type=Path)
    parser.add_argument("mirror", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "--mmd-plan", type=Path, action="append", default=[], help="an MMD collection plan whose conditions match the browser exports (repeatable)"
    )
    parser.add_argument("--previous", type=Path, help="an earlier capture specification whose specs stay for the units it names")
    args = parser.parse_args()
    body = args.queue.read_bytes()
    conditions = [item for plan in args.mmd_plan for item in read_json(plan)["conditions"]]
    previous = read_json(args.previous) if args.previous else None
    result = derive(json.loads(body)["queue"], args.mirror, hashlib.sha256(body).hexdigest(), conditions, tables_contract.load_plan(), previous)
    args.output.write_bytes(result)
    sys.stdout.write(json.dumps({"units": json.loads(result)["units"], "bytes": len(result), "sha256": hashlib.sha256(result).hexdigest()}) + "\n")


if __name__ == "__main__":
    main()
