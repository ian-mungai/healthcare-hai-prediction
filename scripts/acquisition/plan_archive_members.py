"""Verify existing research ZIP evidence without downloading or extracting files."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import zipfile
from pathlib import Path, PurePosixPath

from scripts.acquisition.archive_review import reference_kind
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import canonical_hash, load_registry, read_json
from scripts.acquisition.transport import CaptureError


def evidence_records(value: object) -> list[dict]:
    """Recursively collect records that identify local, URL-bound archive evidence."""
    if isinstance(value, dict):
        found = [value] if value.get("local_path") and value.get("requested_url") and value.get("sha256") else []
        return found + [record for child in value.values() for record in evidence_records(child)]
    if isinstance(value, list):
        return [record for child in value for record in evidence_records(child)]
    return []


def inspect_existing(record: dict) -> dict:
    """Verify complete local ZIP evidence and return member checksums without extracting it."""
    path = Path(record["local_path"])
    if record.get("http_status", record.get("status")) != 200 or record.get("hash_scope", "complete_file") != "complete_file":
        raise CaptureError("Archive evidence does not establish a complete HTTP 200 file.")
    if path.is_symlink() or fingerprint(path) != (record["sha256"], record["bytes"]):
        raise CaptureError("Research archive differs from its recorded bytes or hash.")
    members, names, skipped = [], set(), []
    with zipfile.ZipFile(path) as archive:
        entries = archive.infolist()
        if len(entries) > 10000 or sum(entry.file_size for entry in entries) > 1024**3:
            raise CaptureError("Research archive exceeds the bounded member review limits.")
        for entry in entries:
            name = PurePosixPath(entry.filename)
            if (
                not name.parts
                or name.is_absolute()
                or ".." in name.parts
                or "\\" in entry.filename
                or ":" in name.parts[0]
                or entry.filename in names
                or entry.flag_bits & 1
                or (entry.external_attr >> 16) & 0o170000 == 0o120000
            ):
                raise CaptureError("Research archive has unsafe, ambiguous or encrypted members.")
            names.add(entry.filename)
            if entry.is_dir():
                continue
            if "__MACOSX" in name.parts or name.name == ".DS_Store":
                skipped.append(entry.filename)
                continue
            digest, size = hashlib.sha256(), 0
            with archive.open(entry) as handle:
                while block := handle.read(1024**2):
                    size += len(block)
                    if size > entry.file_size:
                        raise CaptureError("Research member exceeds its declared length.")
                    digest.update(block)
            if size != entry.file_size:
                raise CaptureError("Research member length mismatch.")
            members.append(
                {
                    "member": entry.filename,
                    "bytes": size,
                    "sha256": digest.hexdigest(),
                    "crc32": f"{entry.CRC:08x}",
                    "reference_kind": reference_kind(entry.filename),
                    "nested_zip": name.suffix.lower() == ".zip",
                }
            )
    return {
        "url": record["requested_url"],
        "archive_sha256": record["sha256"],
        "bytes": record["bytes"],
        "local_path": str(path),
        "members": members,
        "skipped_packaging": skipped,
        "status": "local_member_bytes_and_crc_verified",
        "source_acquisition_complete": False,
        "publisher_table_identity_mapping": "not_approved_by_member_inspection",
        "originals_changed": False,
    }


def review_evidence(registry: dict, paths: list[Path]) -> dict:
    """Return reviewed and rejected archive evidence matched to registered source routes."""
    routes: dict[str, list[str]] = {}
    for source in registry["sources"]:
        for route in source["file_routes"]:
            routes.setdefault(route["url"], []).append(route["route_id"])
    reviewed, rejected, seen = [], [], set()
    for path in paths:
        for record in evidence_records(read_json(path)):
            url, digest = record["requested_url"], record["sha256"]
            if url not in routes or not str(record["local_path"]).lower().endswith(".zip") or (url, digest) in seen:
                continue
            seen.add((url, digest))
            try:
                if not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest):
                    raise CaptureError("Research archive lacks a usable checksum.")
                result = inspect_existing(record)
                result.update(route_ids=routes[url], evidence_path=str(path), evidence_sha256=fingerprint(path)[0])
                reviewed.append(result)
            except (ValueError, OSError, KeyError, zipfile.BadZipFile) as error:
                rejected.append(
                    {
                        "url": url,
                        "route_ids": routes[url],
                        "evidence_path": str(path),
                        "status": "evidence_review_failed",
                        "reason": str(error) if isinstance(error, CaptureError) else type(error).__name__,
                    }
                )
    return {
        "review_version": 1,
        "registry_sha256": canonical_hash(registry),
        "archives": reviewed,
        "rejected_evidence": rejected,
        "counts": {
            "archives_verified": len(reviewed),
            "members_verified": sum(len(item["members"]) for item in reviewed),
            "route_bindings": sum(len(item["route_ids"]) for item in reviewed),
            "rejected_evidence": len(rejected),
        },
        "downloads": 0,
        "s3_calls": 0,
        "originals_changed": False,
    }


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", action="append", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        report = review_evidence(load_registry(), args.evidence)
        write_once(args.output, encoded_json(report))
    except (ValueError, OSError, KeyError, TypeError) as error:
        parser.error(str(error))
    sys.stdout.write(str(json.dumps(report["counts"], indent=2)) + "\n")


if __name__ == "__main__":
    main()
