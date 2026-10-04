"""Check every S3 object version against the local storage records, counting deliberately removed versions as retired.

Read-only. Upload records are JSON objects under ``data/`` carrying ``key``, ``version_id`` and the project bucket, as
written by ``store_file``. Retirement records list versions removed on purpose; replacement records list the redacted
copies that replaced them. A recorded version must be live with its recorded size unless it is retired, and a retired
version must be absent. Live versions that no upload record names are reported as observed in a saved S3 inventory, or
as unrecorded. The evidence root's e2e/, conformance/ and schema_review/ folders contain synthetic, policy-audit or
derived review evidence and are explicitly excluded with reasons and counts. Nested acquisition folders with those
names remain inspected. Unrecorded versions under the bucket's top-level lakehouse/ folder are Iceberg table files, not
acquisition objects: they are counted apart, while any upload record naming such a key is still checked. Run from the
repository root:

    .venv/bin/python -m scripts.acquisition.verify_storage_records --output data/storage_checks
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition.data_paths import current
from scripts.acquisition.s3_store import AwsCli, encoded_json, write_once
from scripts.acquisition.transport import CaptureError
from scripts.infrastructure.render_project_config import REPO_ROOT, load_configuration, verify_project_identity

# Retirements: the privacy deletion and the removal of byte-identical duplicates (Oct 4 2026).
RETIREMENTS = ("data/privacy_review/*/retired_objects*.json", "data/lakehouse_planning/dedup_*/retired_objects*.json")
REPLACEMENTS = "data/privacy_review/*/replacements*.json"
SKIPPED_FOLDERS = {"private_original", "storage_checks", "redacted_copies"}
NON_STORAGE_ROOTS = {
    "e2e": "Synthetic test inputs and deliberately malformed fixtures, not live acquisition evidence.",
    "conformance": "Repository policy audit evidence, not live acquisition evidence.",
    "schema_review": "Derived schema-review outputs, not upload records (owner decision Oct 1 2026).",
}
MAX_RECORD_FILE = 50 * 1024**2
# Failure modes 237 to 240: only the exact first path segment matches; recorded versions there are still checked.
TABLE_FILE_PREFIXES = {"lakehouse/": "Iceberg table files written by the lakehouse jobs, which Terraform scopes to this folder."}


def table_file(key: str) -> bool:
    return any(key.startswith(prefix) for prefix in TABLE_FILE_PREFIXES)


def size_of(node: dict[str, Any]) -> int | None:
    for field in ("byte_count", "bytes", "Size"):
        if isinstance(node.get(field), int):
            return int(node[field])
    return None


def collect(
    root: Path, bucket: str, empty_captures: set[str] | None = None
) -> tuple[dict[tuple[str, str], set[int | None]], set[tuple[str, str]], Counter[str]]:
    """Return upload records (identity to recorded sizes), identities seen in saved S3 listings, and walk counts."""
    uploads: dict[tuple[str, str], set[int | None]] = {}
    observed: set[tuple[str, str]] = set()
    counts: Counter[str] = Counter()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if isinstance(node.get("key"), str) and isinstance(node.get("version_id"), str) and node.get("bucket") == bucket:
                uploads.setdefault((node["key"], node["version_id"]), set()).add(size_of(node))
            elif isinstance(node.get("Key"), str) and isinstance(node.get("VersionId"), str):
                observed.add((node["Key"], node["VersionId"]))
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    for folder, directories, files in os.walk(root):
        if Path(folder).resolve() == root.resolve():
            for name in NON_STORAGE_ROOTS:
                if name in directories:
                    counts[f"excluded_{name}_json_files"] = sum(1 for path in (root / name).rglob("*.json") if path.is_file())
                    directories.remove(name)
        directories[:] = sorted(name for name in directories if name not in SKIPPED_FOLDERS)
        for name in sorted(files):
            path = Path(folder) / name
            if not name.endswith(".json"):
                continue
            if str(path.relative_to(root)) in (empty_captures or set()):
                if path.read_bytes() != b"":
                    raise CaptureError("A disposed empty publisher capture changed during the walk.")
                counts["reviewed_empty_publisher_captures"] += 1
                continue
            if path.stat().st_size > MAX_RECORD_FILE:
                raise CaptureError(f"Unreviewed evidence exceeds the JSON inspection limit: {path.relative_to(root)}")
            try:
                walk(json.loads(path.read_bytes()))
                counts["json_files_read"] += 1
            except (ValueError, UnicodeDecodeError):
                raise CaptureError(f"Unparseable JSON evidence requires a recorded disposition: {path.relative_to(root)}") from None
    return uploads, observed, counts


def load_empty_captures(path: Path | None, root: Path) -> set[str]:
    """Accept only exact, still-empty publisher groups.json captures with a recorded content-bound disposition."""
    if path is None:
        return set()
    record = json.loads(path.read_text(encoding="utf-8"))
    if record.get("kind") != "empty_publisher_capture_dispositions" or not isinstance(record.get("files"), list):
        raise CaptureError("Malformed empty-capture disposition.")
    accepted: set[str] = set()
    for item in record["files"]:
        relative = Path(item["path"])
        # A disposition written before the dataset move names the old folder [226].
        located = current(item["path"])
        target = root / located
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or relative.name != "groups.json"
            or relative.as_posix() != item["path"]
            or not target.resolve().is_relative_to(root.resolve())
            or located in accepted
            or not isinstance(item.get("reason"), str)
            or not item["reason"].strip()
            or item.get("sha256") != hashlib.sha256(b"").hexdigest()
            or target.read_bytes() != b""
        ):
            raise CaptureError("Empty-capture disposition is invalid, duplicated, missing or changed.")
        accepted.add(located)
    return accepted


def load_retired(paths: list[Path]) -> dict[tuple[str, str], int]:
    retired: dict[tuple[str, str], int] = {}
    for path in paths:
        for item in json.loads(path.read_text(encoding="utf-8"))["objects"]:
            if item.get("status") not in {"deleted_verified", "already_absent"}:
                raise CaptureError("A retirement record lists a version that was not verified as deleted.")
            identity = (item["key"], item["version_id"])
            if identity in retired and retired[identity] != int(item["bytes"]):
                raise CaptureError("Conflicting retirement records.")
            retired[identity] = int(item["bytes"])
    return retired


def load_replacements(paths: list[Path]) -> dict[tuple[str, str], int]:
    expected: dict[tuple[str, str], int] = {}
    for path in paths:
        for item in json.loads(path.read_text(encoding="utf-8"))["replacements"]:
            for part in ("redacted", "manifest"):
                identity, size = (item[part]["key"], item[part]["version_id"]), int(item[part]["byte_count"])
                if identity in expected and expected[identity] != size:
                    raise CaptureError("Conflicting replacement records.")
                expected[identity] = size
    return expected


def reconcile(
    uploads: dict[tuple[str, str], set[int | None]],
    observed: set[tuple[str, str]],
    retired: dict[tuple[str, str], int],
    replacements: dict[tuple[str, str], int],
    listing: dict[str, Any],
) -> dict[str, Any]:
    """Compare records with one complete S3 version listing and return counts and every failing identity."""
    if listing.get("IsTruncated") or listing.get("NextToken") or listing.get("NextKeyMarker") or listing.get("NextVersionIdMarker"):
        raise CaptureError("The S3 version listing is truncated; storage cannot be reconciled.")
    for field in ("Versions", "DeleteMarkers"):
        entries = listing.get(field, [])
        if not isinstance(entries, list) or any(
            not isinstance(item, dict)
            or not isinstance(item.get("Key"), str)
            or not isinstance(item.get("VersionId"), str)
            or not item["Key"]
            or not item["VersionId"]
            or (field == "Versions" and not isinstance(item.get("Size"), int))
            for item in entries
        ):
            raise CaptureError("Malformed S3 version listing.")
        if len({(item["Key"], item["VersionId"]) for item in entries}) != len(entries):
            raise CaptureError("Duplicate identities in S3 version listing.")
    live = {(item["Key"], item["VersionId"]): item.get("Size") for item in listing.get("Versions", [])}
    markers = {(item["Key"], item["VersionId"]) for item in listing.get("DeleteMarkers", [])}
    failures: dict[str, list[dict[str, Any]]] = {
        name: [] for name in ("missing", "size_mismatch", "conflicting_records", "retired_still_present", "replacement_missing", "inventory_missing")
    }
    for identity, sizes in sorted(uploads.items()):
        known = {size for size in sizes if size is not None}
        if len(known) > 1:
            failures["conflicting_records"].append({"key": identity[0], "version_id": identity[1]})
        if identity in retired:
            continue
        if identity not in live:
            failures["missing"].append({"key": identity[0], "version_id": identity[1]})
        elif known and live[identity] not in known:
            failures["size_mismatch"].append({"key": identity[0], "version_id": identity[1]})
    for identity in sorted(retired):
        if identity in live or identity in markers:
            failures["retired_still_present"].append({"key": identity[0], "version_id": identity[1]})
    for identity, size in sorted(replacements.items()):
        if live.get(identity) != size:
            failures["replacement_missing"].append({"key": identity[0], "version_id": identity[1]})
    for identity in sorted(observed - set(live) - markers - set(retired)):
        failures["inventory_missing"].append({"key": identity[0], "version_id": identity[1]})
    accounted = set(uploads) | set(replacements)
    unexplained = (set(live) | markers) - accounted - observed
    unrecorded = sorted(identity for identity in unexplained if not table_file(identity[0]))
    inventory_only = (set(live) | markers) - accounted - unexplained
    return {
        "status": "passed" if not any(failures.values()) and not unrecorded else "failed",
        "live_versions": len(live),
        "delete_markers": len(markers),
        "recorded_versions": len(uploads),
        "recorded_live_verified": len([identity for identity in uploads if identity not in retired and identity in live]),
        "retired_versions_confirmed_absent": len([identity for identity in retired if identity not in live and identity not in markers]),
        "retired_versions_without_upload_record": len(set(retired) - set(uploads)),
        "replacement_versions_verified": len([identity for identity, size in replacements.items() if live.get(identity) == size]),
        "live_versions_known_only_from_saved_inventories": len(inventory_only),
        "inventory_only_by_prefix": dict(sorted(Counter(key.split("/")[0] for key, _version in inventory_only).items())),
        "lakehouse_versions": len([identity for identity in unexplained - markers if table_file(identity[0])]),
        "lakehouse_delete_markers": len([identity for identity in unexplained & markers if table_file(identity[0])]),
        "unrecorded": [{"key": key, "version_id": version} for key, version in unrecorded],
        "failures": {name: items for name, items in failures.items() if items},
        "failure_counts": {name: len(items) for name, items in failures.items()},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", required=True, type=Path, help="folder for the run's report and saved listing")
    parser.add_argument("--evidence-root", type=Path, default=REPO_ROOT / "data")
    parser.add_argument("--retirements", nargs="*", type=Path, help=f"default: {RETIREMENTS}")
    parser.add_argument("--replacements", nargs="*", type=Path, help=f"default: {REPLACEMENTS}")
    parser.add_argument("--empty-capture-dispositions", type=Path, help="reviewed exact paths and empty-byte hashes for failed publisher groups.json captures")
    parser.add_argument("--listing", type=Path, help="offline replay: a saved list-object-versions result instead of a live call")
    parser.add_argument("--bucket", help="bucket name for offline replay; live runs read it from .env")
    args = parser.parse_args()
    started = datetime.now(UTC)
    try:
        retirements = args.retirements if args.retirements is not None else sorted(path for pattern in RETIREMENTS for path in REPO_ROOT.glob(pattern))
        replacements = args.replacements if args.replacements is not None else sorted(REPO_ROOT.glob(REPLACEMENTS))
        if args.listing:
            if not args.bucket:
                raise CaptureError("Offline replay needs --bucket.")
            bucket, listing, source = args.bucket, json.loads(args.listing.read_text(encoding="utf-8")), "saved_listing"
        else:
            settings, _ = load_configuration(REPO_ROOT / ".env")
            verify_project_identity(settings)
            bucket, listing, source = settings["data_bucket_name"], AwsCli(settings).call("s3api", "list-object-versions"), "live_listing"
        empty_captures = load_empty_captures(args.empty_capture_dispositions, args.evidence_root)
        uploads, observed, walked = collect(args.evidence_root, bucket, empty_captures)
        result = reconcile(uploads, observed, load_retired(retirements), load_replacements(replacements), listing)
    except (CaptureError, ValueError, OSError, KeyError, TypeError) as error:
        parser.error(str(error))
    folder = args.output / started.strftime("%Y%m%dT%H%M%SZ")
    if source == "live_listing":
        write_once(folder / "listing.json", encoded_json(listing))
    report = {
        "kind": "storage_records_check",
        "checked_at_utc": started.isoformat(),
        "listing_source": source,
        "listing_path": str(args.listing or folder / "listing.json"),
        "listing_sha256": hashlib.sha256(args.listing.read_bytes() if args.listing else encoded_json(listing)).hexdigest(),
        "implementation_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "evidence_root": str(args.evidence_root),
        "empty_capture_disposition_record": str(args.empty_capture_dispositions) if args.empty_capture_dispositions else None,
        "verification_boundary": "Complete version listing and recorded sizes; content hashes are not re-read by this check.",
        "retirement_records": [str(path) for path in retirements],
        "replacement_records": [str(path) for path in replacements],
        "evidence_walk": dict(sorted(walked.items())),
        "excluded_evidence_roots": NON_STORAGE_ROOTS,
        "table_file_prefixes": TABLE_FILE_PREFIXES,
        "reviewed_empty_capture_paths": sorted(empty_captures),
        "empty_capture_disposition_sha256": hashlib.sha256(args.empty_capture_dispositions.read_bytes()).hexdigest()
        if args.empty_capture_dispositions
        else None,
        **result,
        "reproduce": (
            "offline: --listing <listing_path> --bucket <name> --evidence-root <evidence_root> "
            "--empty-capture-dispositions <empty_capture_disposition_record> (omit when null); live: project profile from .env"
        ),
    }
    write_once(folder / "report.json", encoded_json(report))
    summary = {key: value for key, value in report.items() if key not in {"unrecorded", "failures", "inventory_only_by_prefix"}}
    sys.stdout.write(json.dumps({**summary, "unrecorded": len(result["unrecorded"]), "report": str(folder / "report.json")}) + "\n")
    raise SystemExit(0 if result["status"] == "passed" else 1)


if __name__ == "__main__":
    main()
