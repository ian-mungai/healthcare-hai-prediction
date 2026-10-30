"""Name the downloaded source files no table or model uses, and plan their removal for the owner to approve.

``plan`` is read-only and the default: it reads bronze's copies list and an optional list of files to keep, then writes a report and a removal
plan. ``apply`` runs only with an approval file that names the plan's SHA-256. It writes delete markers, never deletes a version, and records each
marker so a person can restore a file by removing its marker. A retry skips keys that already carry a marker.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import AwsCli, encoded_json, write_once
from scripts.acquisition.transport import CaptureError
from scripts.infrastructure.render_project_config import REPO_ROOT, load_configuration, verify_project_identity

COLUMNS = ("source_id", "snapshot_id", "s3_key", "sha256", "loaded", "retired", "tables")
BATCH_LIMIT = 1000


def read_copies(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = [name for name in COLUMNS if name not in (reader.fieldnames or [])]
        if missing:
            raise CaptureError(f"The copies list lacks the columns {', '.join(missing)}.")
        return list(reader)


def read_inventory(path: Path | None, copies: list[dict[str, str]]) -> list[dict[str, str]]:
    """Return the stored files that the copies list does not name. The inventory holds one row per stored object: its source, snapshot, key and SHA-256."""
    if path is None:
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = [name for name in COLUMNS[:4] if name not in (reader.fieldnames or [])]
        if missing:
            raise CaptureError(f"The inventory lacks the columns {', '.join(missing)}.")
        named = {item["s3_key"] for item in copies}
        return [
            {**{name: row[name] for name in COLUMNS[:4]}, "loaded": "false", "retired": "false", "tables": ""} for row in reader if row["s3_key"] not in named
        ]


def read_keep(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    keep = json.loads(path.read_text(encoding="utf-8"))["keep"]
    return {item["snapshot_id"]: item["reason"] for item in keep}


def classify(copies: list[dict[str, str]], keep: dict[str, str]) -> dict[str, Any]:
    """Split the copies into kept files, unused files and files already retired. Anything a kept table or model needs stays."""
    loaded_digests = {item["sha256"] for item in copies if item["loaded"] == "true"}
    kept: list[dict[str, str]] = []
    unused: list[dict[str, str]] = []
    retired: list[str] = []
    for item in sorted(copies, key=lambda copy: (copy["snapshot_id"], copy["s3_key"])):
        identity = {"snapshot_id": item["snapshot_id"], "source_id": item["source_id"], "s3_key": item["s3_key"], "sha256": item["sha256"]}
        if item["retired"] == "true":
            retired.append(item["snapshot_id"])
        elif item["loaded"] == "true":
            kept.append({**identity, "reason": "loaded_by_bronze"})
        elif item["sha256"] in loaded_digests:
            kept.append({**identity, "reason": "same_bytes_as_loaded_file"})
        elif item["snapshot_id"] in keep:
            kept.append({**identity, "reason": keep[item["snapshot_id"]]})
        elif item["tables"] == "":
            unused.append({**identity, "reason": "not_in_bronze_copies"})
        elif int(item["tables"]) == 0:
            unused.append({**identity, "reason": "no_table_reads_it"})
        else:
            unused.append({**identity, "reason": "not_loaded_by_any_table"})
    return {"kept": kept, "unused": unused, "already_retired": retired}


def build_plan(classified: dict[str, Any], copies_sha256: str) -> dict[str, Any]:
    items = [
        {"snapshot_id": item["snapshot_id"], "s3_key": item["s3_key"], "sha256": item["sha256"], "reason": item["reason"]} for item in classified["unused"]
    ]
    body = {
        "kind": "unused_file_removal_plan",
        "copies_sha256": copies_sha256,
        "items": items,
        "method": "s3_delete_marker",
        "permanent_version_deletion": False,
        "executed": False,
    }
    return {**body, "plan_sha256": hashlib.sha256(encoded_json(body)).hexdigest()}


def plan(copies_path: Path, keep_path: Path | None, output: Path, inventory_path: Path | None = None) -> dict[str, Any]:
    copies = read_copies(copies_path)
    classified = classify([*copies, *read_inventory(inventory_path, copies)], read_keep(keep_path))
    inputs = copies_path.read_bytes() + (inventory_path.read_bytes() if inventory_path else b"")
    removal = build_plan(classified, hashlib.sha256(inputs).hexdigest())
    write_once(output / "unused_report.json", encoded_json({**classified, "plan_sha256": removal["plan_sha256"]}))
    write_once(output / "removal_plan.json", encoded_json(removal))
    return {"kept": len(classified["kept"]), "unused": len(classified["unused"]), "already_retired": len(classified["already_retired"]), "deleted": 0}


def approved_plan(plan_path: Path, approval_path: Path) -> dict[str, Any]:
    removal = json.loads(plan_path.read_text(encoding="utf-8"))
    body = {key: value for key, value in removal.items() if key != "plan_sha256"}
    if hashlib.sha256(encoded_json(body)).hexdigest() != removal["plan_sha256"]:
        raise CaptureError("The removal plan was changed after it was written.")
    if not approval_path.is_file():
        raise CaptureError("Removal needs the owner's approval file for this plan.")
    approval = json.loads(approval_path.read_text(encoding="utf-8"))
    if approval.get("plan_sha256") != removal["plan_sha256"] or not approval.get("approved_by"):
        raise CaptureError("The approval does not name this plan's SHA-256.")
    return removal


def apply(client: Any, plan_path: Path, approval_path: Path, output: Path) -> dict[str, Any]:
    removal = approved_plan(plan_path, approval_path)
    if client.call("s3api", "get-bucket-versioning").get("Status") != "Enabled":
        raise CaptureError("Recoverable removal requires enabled versioning.")
    listing = client.call("s3api", "list-object-versions")
    if listing.get("IsTruncated"):
        raise CaptureError("A complete version listing is required before removal.")
    keys = sorted(item["s3_key"] for item in removal["items"])
    live = {item["Key"] for item in listing.get("Versions", []) if item.get("IsLatest")}
    marked = {item["Key"]: item["VersionId"] for item in listing.get("DeleteMarkers", []) if item.get("IsLatest")}
    unknown = [key for key in keys if key not in live and key not in marked]
    if unknown:
        raise CaptureError("Planned keys have no version in the bucket; review a new plan.")
    pending = [key for key in keys if key not in marked]
    for number, start in enumerate(range(0, len(pending), BATCH_LIMIT)):
        batch = pending[start : start + BATCH_LIMIT]
        request = output / f"delete_request_{number}.json"
        request.parent.mkdir(parents=True, exist_ok=True)
        request.write_bytes(encoded_json({"Objects": [{"Key": key} for key in batch], "Quiet": False}))
        result = client.call("s3api", "delete-objects", ["--delete", "file://" + str(request.resolve()), "--checksum-algorithm", "SHA256"])
        if result.get("Errors") or {item["Key"] for item in result.get("Deleted", [])} != set(batch):
            raise CaptureError("Removal is incomplete; list the versions again before any retry. Keys with a marker are skipped.")
        if any(item.get("DeleteMarker") is not True or not item.get("DeleteMarkerVersionId") for item in result["Deleted"]):
            raise CaptureError("Removal did not confirm a delete marker for every key.")
        marked.update({item["Key"]: item["DeleteMarkerVersionId"] for item in result["Deleted"]})
    record = {
        "plan_sha256": removal["plan_sha256"],
        "items": [{"s3_key": key, "delete_marker_version_id": marked[key], "restore": "delete this marker version"} for key in keys],
    }
    write_once(output / "removal_record.json", encoded_json(record))
    return {"markers": len(keys), "newly_deleted": len(pending)}


def main(argv: Sequence[str] | None = None, client: Any = None) -> None:
    parser = argparse.ArgumentParser(description="Report unused downloaded files and apply an owner-approved removal plan.")
    commands = parser.add_subparsers(dest="command", required=True)
    planning = commands.add_parser("plan")
    planning.add_argument("--copies", required=True, type=Path)
    planning.add_argument("--keep", type=Path)
    planning.add_argument("--inventory", type=Path, help="CSV of every stored object: source_id, snapshot_id, s3_key, sha256")
    planning.add_argument("--output", required=True, type=Path)
    applying = commands.add_parser("apply")
    applying.add_argument("--plan", required=True, type=Path)
    applying.add_argument("--approval", required=True, type=Path)
    applying.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            summary = plan(args.copies, args.keep, args.output, args.inventory)
        else:
            approved_plan(args.plan, args.approval)
            if client is None:
                settings, _ = load_configuration(REPO_ROOT / ".env")
                verify_project_identity(settings)
                client = AwsCli(settings)
            summary = apply(client, args.plan, args.approval, args.output)
    except (CaptureError, ValueError, OSError, KeyError, TypeError) as error:
        parser.error(str(error))
    sys.stdout.write(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
