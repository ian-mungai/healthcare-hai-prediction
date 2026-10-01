"""Reconcile collection-layout versions against the verified post-cleanup baseline."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from scripts.acquisition.s3_store import AwsCli, encoded_json, write_once
from scripts.acquisition.source_registry import read_json
from scripts.acquisition.transport import CaptureError
from scripts.infrastructure.render_project_config import REPO_ROOT, load_configuration, verify_project_identity


def reconcile(reconciliation: dict, baseline: dict, listing: dict, versions: dict) -> dict:
    """Compare current storage listings with reconciled and baseline records; return verification counts."""
    if reconciliation.get("storage_contract_version") != "4.0.0" or not reconciliation.get("manifests"):
        raise CaptureError("Collection-layout reconciliation with completed dataset manifests is required.")
    if listing.get("IsTruncated") or versions.get("IsTruncated"):
        raise CaptureError("Incomplete S3 listing cannot prove storage completion.")
    records = [entry["object"] for entry in reconciliation["objects"] + reconciliation["manifests"]]
    expected: dict[str, dict] = {}
    for record in records:
        if record["key"] in expected and record != expected[record["key"]]:
            raise CaptureError("Conflicting manifest aliases for one stored object.")
        expected[record["key"]] = record
    old = {item["Key"]: item for item in baseline["objects"]}
    current = {item["Key"]: item for item in listing.get("Contents", [])}
    if set(old) & set(expected) or set(current) != set(old) | set(expected):
        raise CaptureError("Current object keys differ from the preserved baseline plus dataset manifests.")
    old_versions = {(item["Key"], item["VersionId"]): item for item in baseline["versions"]}
    after_versions = {(item["Key"], item["VersionId"]): item for item in versions.get("Versions", [])}
    new_versions = {(key, item["version_id"]) for key, item in expected.items()}
    if set(after_versions) != set(old_versions) | new_versions or versions.get("DeleteMarkers", []) != baseline["delete_markers"]:
        raise CaptureError("Object versions or deletion markers differ from the approved additive transition.")
    for key, item in old.items():
        if current[key]["Size"] != item["Size"]:
            raise CaptureError("Existing object size changed during the layout transition.")
    for identity, item in old_versions.items():
        if any(after_versions[identity].get(field) != item.get(field) for field in ("Size", "IsLatest")):
            raise CaptureError("Existing version changed during the layout transition.")
    for key, item in expected.items():
        version = after_versions[(key, item["version_id"])]
        if current[key]["Size"] != item["byte_count"] or version["Size"] != item["byte_count"] or version.get("IsLatest") is not True:
            raise CaptureError("Current version or byte count differs from verified upload evidence.")
    folders = {entry["dataset_id"] for entry in reconciliation["manifests"]}
    collection = f"{reconciliation['publisher']}/{reconciliation['collection']}"
    if {"/".join(key.split("/")[:2]) for key in expected} != {collection} or any(key.lower().endswith(".zip") for key in expected):
        raise CaptureError("New storage includes an unregistered folder or unexpanded ZIP.")
    data_folders = {key.split("/")[3] for key in expected if key.split("/")[2] == "datasets"}
    if not data_folders <= folders:
        raise CaptureError("A table folder is missing its dataset manifest.")
    return {
        "status": "collection_inventory_verified",
        "checked_at_utc": datetime.now(UTC).isoformat(),
        "collection_root": collection,
        "dataset_count": len(folders),
        "dataset_objects": len(expected),
        "total_current_objects": len(current),
        "total_versions": len(after_versions),
        "legacy_objects_preserved": len(old),
        "legacy_versions_preserved": len(old_versions),
        "delete_markers": len(versions.get("DeleteMarkers", [])),
        "new_zip_objects": 0,
        "category_counts": dict(Counter(key.split("/")[2] for key in expected)),
        "dictionary_copies": len({entry["object"]["key"] for entry in reconciliation["objects"] if entry.get("reference_kind") == "dictionary_candidate"}),
        "held_members": [
            {"dataset_id": entry["dataset_id"], "archive_member": entry["archive_member"], "reason": entry["hold_reason"]}
            for entry in reconciliation["objects"]
            if entry.get("hold_reason")
        ],
        "model_eligible": False,
        "dictionary_section_mapping": "pending",
        "complete_historical_acquisition": False,
    }


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description="Read-only project-profile S3 inventory reconciliation; no uploads or deletions.")
    parser.add_argument("--reconciliation", required=True, type=Path)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        settings, _ = load_configuration(REPO_ROOT / ".env")
        verify_project_identity(settings)
        client = AwsCli(settings)
        report = reconcile(
            read_json(args.reconciliation), read_json(args.baseline), client.call("s3api", "list-objects-v2"), client.call("s3api", "list-object-versions")
        )
        write_once(args.output, encoded_json(report))
    except (ValueError, OSError, KeyError, TypeError) as error:
        parser.error(str(error))
    sys.stdout.write(str(json.dumps(report, indent=2)) + "\n")


if __name__ == "__main__":
    main()
