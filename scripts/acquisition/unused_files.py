"""Name the downloaded source files no table or model uses, and list them for the owner to remove by hand.

``plan`` is read-only: it reads bronze's copies list, a complete inventory of the stored objects with their immutable versions and an optional list
of files to keep, then writes a report and a removal plan. Only acquisition payload keys can be planned. A key any kept file uses, and any file a
table lists, stays. Nothing here changes S3. The owner removes the planned keys by hand, and ``confirmation-template`` writes the record the owner signs
once that is done. The run 3 route gate refuses to start unless that record names the plan's hash and every planned key.
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

from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.transport import CaptureError

COPY_COLUMNS = ("source_id", "snapshot_id", "s3_key", "sha256", "loaded", "retired", "tables")
INVENTORY_COLUMNS = ("source_id", "snapshot_id", "s3_key", "s3_version_id", "sha256")
PAYLOAD_KEY = re.compile(r"^(?:raw/)?[a-z0-9_]+/[a-z0-9_]+/datasets/")


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
    conflicts = sorted(row["s3_key"] for row in copies if row["retired"] != "true" and objects[row["s3_key"]]["sha256"] != row["sha256"])
    if conflicts:
        raise CaptureError(
            f"The copies list and the inventory disagree about the bytes of {len(conflicts)} files, such as {conflicts[0]}. Fix the exports first."
        )
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
        "removal": "by hand by the owner",
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


def read_plan(plan_path: Path) -> dict[str, Any]:
    """Read a removal plan and refuse one that was changed after it was written."""
    removal = json.loads(plan_path.read_text(encoding="utf-8"))
    body = {key: value for key, value in removal.items() if key != "plan_sha256"}
    if hashlib.sha256(encoded_json(body)).hexdigest() != removal["plan_sha256"]:
        raise CaptureError("The removal plan was changed after it was written.")
    return removal


def confirmation_template(removal: dict[str, Any]) -> dict[str, Any]:
    """The record the owner signs after removing every planned key by hand. It names the plan, the bucket and each key."""
    return {
        "kind": "unused_file_removal_confirmation",
        "plan_sha256": removal["plan_sha256"],
        "bucket": removal["bucket"],
        "keys": sorted({item["s3_key"] for item in removal["items"]}),
        "confirmed_by": None,
    }


def write_confirmation_template(plan_path: Path, output: Path) -> dict[str, Any]:
    document = confirmation_template(read_plan(plan_path))
    write_once(output, encoded_json(document))
    return document


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Report unused downloaded files and write the confirmation the owner signs after removing them by hand.")
    commands = parser.add_subparsers(dest="command", required=True)
    planning = commands.add_parser("plan")
    planning.add_argument("--copies", required=True, type=Path)
    planning.add_argument("--keep", type=Path)
    planning.add_argument("--inventory", required=True, type=Path, help="CSV of every stored object: source_id, snapshot_id, s3_key, s3_version_id, sha256")
    planning.add_argument("--bucket", required=True)
    planning.add_argument("--output", required=True, type=Path)
    confirming = commands.add_parser("confirmation-template")
    confirming.add_argument("--plan", required=True, type=Path)
    confirming.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            summary = plan(args.copies, args.keep, args.output, args.inventory, args.bucket)
        else:
            summary = {"keys": len(write_confirmation_template(args.plan, args.output)["keys"]), "confirmed_by": None}
    except (CaptureError, ValueError, OSError, KeyError, TypeError) as error:
        parser.error(str(error))
    sys.stdout.write(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
