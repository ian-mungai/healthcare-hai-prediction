"""Name the downloaded source files no table or model uses, and plan their removal for the owner to approve.

``plan`` is read-only and the default: it reads bronze's copies list, a complete inventory of the stored objects with their immutable versions and an
optional list of files to keep, then writes a report and a removal plan. Only acquisition payload keys can be planned. A key any kept file uses, and any
file a table lists, stays. ``apply`` runs only with an approval file that names the plan's SHA-256, for the plan's own bucket, and only while every
planned key is still at its planned version. It writes delete markers, never deletes a version, and records each marker so a person can restore a file by
removing its marker. A retry skips keys that already carry a marker.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import AwsCli, encoded_json, write_once
from scripts.acquisition.transport import CaptureError

COPY_COLUMNS = ("source_id", "snapshot_id", "s3_key", "sha256", "loaded", "retired", "tables")
INVENTORY_COLUMNS = ("source_id", "snapshot_id", "s3_key", "s3_version_id", "sha256")
PAYLOAD_KEY = re.compile(r"^(?:raw/)?[a-z0-9_]+/[a-z0-9_]+/datasets/")
BATCH_LIMIT = 1000


def read_table(path: Path, columns: tuple[str, ...], name: str) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = [column for column in columns if column not in (reader.fieldnames or [])]
        if missing:
            raise CaptureError(f"The {name} lacks the columns {', '.join(missing)}.")
        rows = list(reader)
    if not rows:
        raise CaptureError(f"The {name} is empty, so it cannot show which files exist.")
    return rows


def read_keep(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    keep = json.loads(path.read_text(encoding="utf-8"))["keep"]
    return {item["snapshot_id"]: item["reason"] for item in keep}


def stored_objects(inventory: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    """Return one record per stored key. Two rows for one key must name the same version and bytes."""
    objects: dict[str, dict[str, str]] = {}
    for row in inventory:
        found = objects.setdefault(row["s3_key"], row)
        if (found["s3_version_id"], found["sha256"]) != (row["s3_version_id"], row["sha256"]):
            raise CaptureError(f"The inventory names two different objects for {row['s3_key']}.")
    return objects


def classify(copies: list[dict[str, str]], inventory: list[dict[str, str]], keep: dict[str, str]) -> dict[str, Any]:
    """Split the stored files into kept, unused and already retired. When in doubt a file is kept, and a key any kept file uses stays."""
    objects = stored_objects(inventory)
    absent = sorted({row["s3_key"] for row in copies if row["retired"] != "true"} - objects.keys())
    if absent:
        raise CaptureError(f"The inventory does not cover {len(absent)} files the copies list names, such as {absent[0]}. It is incomplete.")
    loaded_digests = {row["sha256"] for row in copies if row["loaded"] == "true"}
    rows: list[dict[str, str]] = [dict(row) for row in copies]
    named = {row["s3_key"] for row in copies}
    rows.extend({**row, "loaded": "false", "retired": "false", "tables": ""} for row in inventory if row["s3_key"] not in named)
    kept: list[dict[str, str]] = []
    unused: list[dict[str, str]] = []
    retired: list[str] = []
    for row in sorted(rows, key=lambda item: (item["snapshot_id"], item["s3_key"])):
        identity = {"snapshot_id": row["snapshot_id"], "source_id": row["source_id"], "s3_key": row["s3_key"], "sha256": row["sha256"]}
        if row["retired"] == "true":
            retired.append(row["snapshot_id"])
        elif PAYLOAD_KEY.match(row["s3_key"]) is None:
            kept.append({**identity, "reason": "not_an_acquisition_payload"})
        elif row["loaded"] == "true":
            kept.append({**identity, "reason": "loaded_by_bronze"})
        elif row["sha256"] in loaded_digests:
            kept.append({**identity, "reason": "same_bytes_as_loaded_file"})
        elif row["snapshot_id"] in keep:
            kept.append({**identity, "reason": keep[row["snapshot_id"]]})
        elif row["tables"] == "":
            unused.append({**identity, "reason": "not_in_bronze_copies"})
        elif int(row["tables"]) == 0:
            unused.append({**identity, "reason": "no_table_reads_it"})
        else:
            kept.append({**identity, "reason": "listed_as_a_table_copy"})
    protected = {item["s3_key"] for item in kept}
    still_unused = []
    for item in unused:
        if item["s3_key"] in protected:
            kept.append({**item, "reason": "shares_a_key_with_a_kept_file"})
        else:
            still_unused.append({**item, "s3_version_id": objects[item["s3_key"]]["s3_version_id"]})
    return {"kept": kept, "unused": still_unused, "already_retired": retired}


def build_plan(classified: dict[str, Any], bucket: str, inputs_sha256: str) -> dict[str, Any]:
    seen: set[str] = set()
    items = []
    for item in classified["unused"]:
        if item["s3_key"] in seen:
            continue
        seen.add(item["s3_key"])
        items.append(
            {
                "snapshot_id": item["snapshot_id"],
                "s3_key": item["s3_key"],
                "version_id": item["s3_version_id"],
                "sha256": item["sha256"],
                "reason": item["reason"],
            }
        )
    body = {
        "kind": "unused_file_removal_plan",
        "bucket": bucket,
        "inputs_sha256": inputs_sha256,
        "items": items,
        "method": "s3_delete_marker",
        "permanent_version_deletion": False,
        "executed": False,
    }
    return {**body, "plan_sha256": hashlib.sha256(encoded_json(body)).hexdigest()}


def plan(copies_path: Path, keep_path: Path | None, output: Path, inventory_path: Path, bucket: str) -> dict[str, Any]:
    copies = read_table(copies_path, COPY_COLUMNS, "copies list")
    inventory = read_table(inventory_path, INVENTORY_COLUMNS, "inventory")
    classified = classify(copies, inventory, read_keep(keep_path))
    removal = build_plan(classified, bucket, hashlib.sha256(copies_path.read_bytes() + inventory_path.read_bytes()).hexdigest())
    write_once(output / "unused_report.json", encoded_json({**classified, "plan_sha256": removal["plan_sha256"]}))
    write_once(output / "removal_plan.json", encoded_json(removal))
    return {"kept": len(classified["kept"]), "unused": len(removal["items"]), "already_retired": len(classified["already_retired"]), "deleted": 0}


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
    if client.configuration.get("data_bucket_name") != removal["bucket"]:
        raise CaptureError("The plan names another bucket than the client's.")
    if client.call("s3api", "get-bucket-versioning").get("Status") != "Enabled":
        raise CaptureError("Recoverable removal requires enabled versioning.")
    listing = client.call("s3api", "list-object-versions")
    if listing.get("IsTruncated"):
        raise CaptureError("A complete version listing is required before removal.")
    current = {item["Key"]: item["VersionId"] for item in listing.get("Versions", []) if item.get("IsLatest")}
    older: dict[str, set[str]] = {}
    for item in listing.get("Versions", []):
        older.setdefault(item["Key"], set()).add(item["VersionId"])
    marked = {item["Key"]: item["VersionId"] for item in listing.get("DeleteMarkers", []) if item.get("IsLatest")}
    pending: list[str] = []
    for item in removal["items"]:
        name, version = item["s3_key"], item["version_id"]
        if name in marked and version in older.get(name, set()):
            continue
        if current.get(name) != version:
            raise CaptureError(f"{name} is not at its planned version, so nothing was removed. Review a new plan.")
        pending.append(name)
    pending.sort()
    for number, start in enumerate(range(0, len(pending), BATCH_LIMIT)):
        batch = pending[start : start + BATCH_LIMIT]
        request = output / f"delete_request_{number}.json"
        request.parent.mkdir(parents=True, exist_ok=True)
        request.write_bytes(encoded_json({"Objects": [{"Key": name} for name in batch], "Quiet": False}))
        result = client.call("s3api", "delete-objects", ["--delete", "file://" + str(request.resolve()), "--checksum-algorithm", "SHA256"])
        if result.get("Errors") or {item["Key"] for item in result.get("Deleted", [])} != set(batch):
            raise CaptureError("Removal is incomplete. List the versions again before any retry. Keys with a marker are skipped.")
        if any(item.get("DeleteMarker") is not True or not item.get("DeleteMarkerVersionId") for item in result["Deleted"]):
            raise CaptureError("Removal did not confirm a delete marker for every key.")
        marked.update({item["Key"]: item["DeleteMarkerVersionId"] for item in result["Deleted"]})
    record = {
        "plan_sha256": removal["plan_sha256"],
        "bucket": removal["bucket"],
        "items": [
            {
                "s3_key": item["s3_key"],
                "version_id": item["version_id"],
                "delete_marker_version_id": marked[item["s3_key"]],
                "restore": "delete this marker version",
            }
            for item in sorted(removal["items"], key=lambda entry: entry["s3_key"])
        ],
    }
    write_once(output / "removal_record.json", encoded_json(record))
    return {"markers": len(removal["items"]), "newly_deleted": len(pending)}


def main(argv: Sequence[str] | None = None, client: Any = None) -> None:
    parser = argparse.ArgumentParser(description="Report unused downloaded files and apply an owner-approved removal plan.")
    commands = parser.add_subparsers(dest="command", required=True)
    planning = commands.add_parser("plan")
    planning.add_argument("--copies", required=True, type=Path)
    planning.add_argument("--keep", type=Path)
    planning.add_argument("--inventory", required=True, type=Path, help="CSV of every stored object: source_id, snapshot_id, s3_key, s3_version_id, sha256")
    planning.add_argument("--bucket", required=True)
    planning.add_argument("--output", required=True, type=Path)
    applying = commands.add_parser("apply")
    applying.add_argument("--plan", required=True, type=Path)
    applying.add_argument("--approval", required=True, type=Path)
    applying.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            summary = plan(args.copies, args.keep, args.output, args.inventory, args.bucket)
        else:
            approved_plan(args.plan, args.approval)
            if client is None:
                from scripts.infrastructure.render_project_config import REPO_ROOT, load_configuration, verify_project_identity

                settings, _ = load_configuration(REPO_ROOT / ".env")
                verify_project_identity(settings)
                client = AwsCli(settings)
            summary = apply(client, args.plan, args.approval, args.output)
    except (CaptureError, ValueError, OSError, KeyError, TypeError) as error:
        parser.error(str(error))
    sys.stdout.write(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
