"""Publish each retirement record into S3 beside the collection manifests that still name the removed versions.

The collection manifests are write-once, so they cannot be edited. Instead, one deterministic record per collection and
per retirement run is stored under ``<publisher>/<collection>/manifests/retirements/<sha256>/retired_versions.json``
through the collectors' write-once ``store_file``. The record holds keys, version IDs, sizes and hashes only. Reruns
create nothing new. Run from the repository root with the project profile from ``.env``:

    PYTHONPATH=. .venv/bin/python scripts/acquisition/one_off/privacy_review/publish_retirement_records.py \\
        --retirement <retired_objects.json> [--replacements <replacements.json>] --record <publication.json>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, store_file, write_once
from scripts.acquisition.transport import CaptureError
from scripts.infrastructure.render_project_config import load_configuration, verify_project_identity

HERE = Path("data/privacy_review/20260927")


def collection_records(retirement: dict[str, Any], replacements: dict[str, Any] | None, source: str) -> dict[str, dict[str, Any]]:
    """Group retired versions, and the redacted copies that replaced them, by publisher and collection."""
    copies = {(item["original"]["key"], item["original"]["version_id"]): item["redacted"] for item in (replacements or {}).get("replacements", [])}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in retirement["objects"]:
        if item["status"] not in {"deleted_verified", "already_absent"}:
            raise CaptureError("Only versions verified as deleted can be published as retired.")
        entry = {field: item[field] for field in ("key", "version_id", "sha256", "bytes", "status")} | {"deleted_utc": item.get("deleted_utc")}
        replacement = copies.get((item["key"], item["version_id"]))
        if replacements is not None and replacement is None:
            raise CaptureError("A retired version has no redacted replacement in the given record.")
        if replacement:
            entry["replaced_by"] = {field: replacement[field] for field in ("key", "version_id", "sha256", "byte_count")}
        grouped["/".join(item["key"].split("/")[:2])].append(entry)
    return {
        collection: {
            "kind": "retired_object_versions",
            "collection": collection,
            "reason": retirement["reason"],
            "local_retirement_record_sha256": source,
            "note": "Collection manifests written earlier still list these versions; readers must treat them as retired, not missing.",
            "versions": sorted(entries, key=lambda entry: (entry["key"], entry["version_id"])),
        }
        for collection, entries in sorted(grouped.items())
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--retirement", required=True, type=Path)
    parser.add_argument("--replacements", type=Path)
    parser.add_argument("--record", required=True, type=Path)
    args = parser.parse_args()
    try:
        retirement = json.loads(args.retirement.read_text(encoding="utf-8"))
        replacements = json.loads(args.replacements.read_text(encoding="utf-8")) if args.replacements else None
        records = collection_records(retirement, replacements, fingerprint(args.retirement)[0])
        settings, _ = load_configuration(Path(".env"))
        verify_project_identity(settings)
        client = AwsCli(settings)
        published, created = [], 0
        for collection, record in records.items():
            content = encoded_json(record)
            digest = hashlib.sha256(content).hexdigest()
            path = HERE / "retirement_publications" / f"{digest}.json"
            write_once(path, content)
            stored, made = store_file(client, path, f"{collection}/manifests/retirements/{digest}/retired_versions.json")
            created += made
            published.append({"collection": collection, "versions": len(record["versions"]), "object": stored})
        write_once(args.record, encoded_json({"kind": "retirement_publications", "retirement_record": str(args.retirement), "published": published}))
    except (CaptureError, ValueError, OSError, KeyError, TypeError) as error:
        sys.stdout.write(json.dumps({"status": "stopped", "reason": str(error) if isinstance(error, CaptureError) else type(error).__name__}) + "\n")
        return 1
    sys.stdout.write(json.dumps({"status": "passed", "collections": len(published), "created": created, "record": str(args.record)}) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
