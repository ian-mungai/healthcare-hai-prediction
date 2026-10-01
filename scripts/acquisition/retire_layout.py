"""Explicit, inventory-pinned removal of old current keys while preserving S3 versions."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import read_json
from scripts.acquisition.transport import CaptureError
from scripts.infrastructure.render_project_config import REPO_ROOT, load_configuration, verify_project_identity


def cleanup_keys(baseline: dict, listing: dict, versions: dict) -> list[str]:
    """Return only legacy object keys whose current versions match the verified baseline."""
    if listing.get("IsTruncated") or versions.get("IsTruncated"):
        raise CaptureError("Complete S3 inventories are required before cleanup.")
    keys = sorted(item["Key"] for item in baseline["objects"])
    if not keys or len(keys) != len(set(keys)) or len(keys) > 1000:
        raise CaptureError("Cleanup requires 1-1000 unique, explicitly inventoried old keys.")
    for key in keys:
        if not re.match(r"^(raw|reference|manifests|audit)/|^cms_[a-z0-9_]+/(data|references|manifests|audit)/", key):
            raise CaptureError("Cleanup cannot target new collection paths or unrelated objects.")
    if {(item["Key"], item["Size"]) for item in listing.get("Contents", [])} != {(item["Key"], item["Size"]) for item in baseline["objects"]}:
        raise CaptureError("Current objects changed after the cleanup inventory; review a new plan.")
    fields = ("Key", "VersionId", "Size", "IsLatest")
    if {tuple(item.get(field) for field in fields) for item in versions.get("Versions", [])} != {
        tuple(item.get(field) for field in fields) for item in baseline["versions"]
    } or versions.get("DeleteMarkers", []) != baseline["delete_markers"]:
        raise CaptureError("Object versions changed after the cleanup inventory.")
    return keys


def retire(client: AwsCli, baseline_path: Path, output: Path, execute: bool = False) -> dict:
    """Plan or explicitly execute legacy-layout retirement and return preserved verification evidence."""
    baseline = read_json(baseline_path)
    if client.call("s3api", "get-bucket-versioning").get("Status") != "Enabled":
        raise CaptureError("Recoverable cleanup requires enabled versioning.")
    keys = cleanup_keys(baseline, client.call("s3api", "list-objects-v2"), client.call("s3api", "list-object-versions"))
    plan = {"baseline_sha256": fingerprint(baseline_path)[0], "keys": keys, "permanent_version_deletion": False}
    write_once(output / "cleanup_plan.json", encoded_json(plan))
    if not execute:
        return {"status": "planned_only", "old_objects": len(keys), "s3_deletes": 0}
    request = output / "delete_request.json"
    write_once(request, encoded_json({"Objects": [{"Key": key} for key in keys], "Quiet": False}))
    result = client.call("s3api", "delete-objects", ["--delete", "file://" + str(request.resolve()), "--checksum-algorithm", "SHA256"])
    write_once(output / "delete_response.json", encoded_json(result))
    if result.get("Errors") or {item["Key"] for item in result.get("Deleted", [])} != set(keys):
        raise CaptureError("Cleanup is incomplete; inspect the preserved response before any retry.")
    if any(item.get("DeleteMarker") is not True or not item.get("DeleteMarkerVersionId") for item in result["Deleted"]):
        raise CaptureError("Cleanup did not confirm recoverable delete markers for every old key.")
    listing, versions = client.call("s3api", "list-objects-v2"), client.call("s3api", "list-object-versions")
    if listing.get("Contents") or listing.get("IsTruncated") or versions.get("IsTruncated"):
        raise CaptureError("Post-cleanup inventory is incomplete or old current objects remain.")
    identities = {(item["Key"], item["VersionId"], item["Size"]) for item in baseline["versions"]}
    retained = {(item["Key"], item["VersionId"], item["Size"]) for item in versions.get("Versions", [])}
    if identities != retained or any(item.get("IsLatest") for item in versions.get("Versions", [])):
        raise CaptureError("Historical object versions were not retained exactly as expected.")
    markers = versions.get("DeleteMarkers", [])
    if {item["Key"] for item in markers if item.get("IsLatest")} != set(keys):
        raise CaptureError("The expected current delete markers are missing.")
    after = {"objects": listing.get("Contents", []), "versions": versions.get("Versions", []), "delete_markers": markers}
    write_once(output / "after_cleanup.json", encoded_json(after))
    report = {
        "status": "old_layout_removed_from_current_objects",
        "objects_retired": len(keys),
        "historical_versions_preserved": len(retained),
        "current_objects": 0,
        "local_originals_changed": False,
        "checked_at_utc": datetime.now(UTC).isoformat(),
    }
    write_once(output / "cleanup_verification.json", encoded_json(report))
    return report


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description="Plan old-layout cleanup; --execute requires prior explicit user approval.")
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    try:
        settings, _ = load_configuration(REPO_ROOT / ".env")
        verify_project_identity(settings)
        report = retire(AwsCli(settings), args.baseline, args.output, args.execute)
    except (ValueError, OSError, KeyError, TypeError) as error:
        parser.error(str(error))
    sys.stdout.write(str(json.dumps(report, indent=2)) + "\n")


if __name__ == "__main__":
    main()
