"""Build the retired-objects list the bronze loader skips: manifest objects the privacy deletion removed from S3.

Run from the repository root with read-only S3 access:

    .venv/bin/python -m scripts.lakehouse.retired_objects --collection california_hcai/hospital_finance

An object is listed only when the collection's manifests name it, S3 holds no version or delete marker at its key, and
an executed deletion record verified its removal with the same key, version ID and SHA-256 [149]. A missing object
without such a record stops the build, so storage loss is never listed as retired. Each run rebuilds the list from
scratch, so an object restored to S3 drops off it. The list holds keys, version IDs and checksums only. Failure modes
146 to 149: plans/bronze_remaining_20261003/failure_modes.md.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from scripts.lakehouse.bronze import RETIRED, BronzeError
from scripts.lakehouse.catalog import deployment

# The deletion runs that removed objects from S3, kept locally with their verification results: the privacy deletion
# and the duplicate removal (failure mode 212).
DELETION_RECORDS = ("data/privacy_review/*/deletion_run_*_execute.json", "data/lakehouse_planning/dedup_*/deletion_run_*_execute.json")


def deleted_versions(patterns: Iterable[str] = DELETION_RECORDS) -> dict[tuple[str, str], str]:
    """Return (key, version ID) -> SHA-256 for every deletion an executed run verified."""
    verified: dict[tuple[str, str], str] = {}
    for path in sorted(path for pattern in patterns for path in glob.glob(pattern)):
        for result in json.loads(Path(path).read_text(encoding="utf-8"))["results"]:
            if result.get("status") == "deleted_verified":
                verified[(result["key"], result["version_id"])] = result["sha256"]
    return verified


def build(client: Any, bucket: str, collections: Iterable[str], verified: dict[tuple[str, str], str]) -> list[dict[str, str]]:
    """Return the sorted retired entries for the collections; refuse a missing object no deletion record explains."""
    entries: dict[tuple[str, str], str] = {}
    for collection in collections:
        live: set[tuple[str, str]] = set()
        marked: set[str] = set()
        for page in client.get_paginator("list_object_versions").paginate(Bucket=bucket, Prefix=f"{collection}/"):
            live.update((version["Key"], version["VersionId"]) for version in page.get("Versions", []))
            marked.update(marker["Key"] for marker in page.get("DeleteMarkers", []))
        manifests = [key for key, _ in live if key.startswith(f"{collection}/manifests/") and key.endswith("/manifest.json")]
        for manifest_key in sorted(manifests):
            manifest = json.loads(client.get_object(Bucket=bucket, Key=manifest_key)["Body"].read())
            for stored in manifest.get("objects", []):
                target = stored["object"]
                identity = (target["key"], target["version_id"])
                if identity in live:
                    continue
                if target["key"] in marked or any(key == target["key"] for key, _ in live):
                    raise BronzeError(f"{target['key']}: the listed version is gone but the key still has versions or a delete marker")
                if verified.get(identity) != target["sha256"]:
                    raise BronzeError(f"{target['key']}: missing from S3 with no verified deletion of this version and SHA-256")
                entries[identity] = target["sha256"]
    return [{"key": key, "version_id": version, "sha256": digest} for (key, version), digest in sorted(entries.items())]


def main() -> int:
    """Rebuild the committed list for the named collections and print counts only."""
    import boto3

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--collection", action="append", required=True, help="publisher/collection; repeat for more")
    args = parser.parse_args()
    settings = deployment()
    client = boto3.Session(profile_name=os.environ.get("AWS_PROFILE", settings["aws_profile"]), region_name=settings["aws_region"]).client("s3")
    try:
        entries = build(client, settings["data_bucket_name"], sorted(set(args.collection)), deleted_versions())
    except BronzeError as error:
        sys.stderr.write(f"retired objects: {error}\n")
        return 1
    document = {
        "version": 1,
        "basis": (
            "Objects removed from S3 that storage manifests still list: the privacy deletion (BRZ-012) and the "
            "removal of byte-identical duplicates (failure mode 212)."
        ),
        "collections": sorted(set(args.collection)),
        "objects": entries,
    }
    RETIRED.write_text(json.dumps(document, indent=2) + "\n")
    sys.stdout.write(f"{len(entries)} retired objects written to {RETIRED}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
