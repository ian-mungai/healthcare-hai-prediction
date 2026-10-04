"""Read-only: list every S3 manifest entry under the mapped collections as JSON Lines (no data values)."""

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.getcwd())
bronze = __import__("scripts.lakehouse.bronze", fromlist=["bronze"])
deployment = __import__("scripts.lakehouse.catalog", fromlist=["catalog"]).deployment

settings = deployment()
os.environ.setdefault("AWS_PROFILE", settings["aws_profile"])
os.environ.setdefault("AWS_REGION", settings["aws_region"])
storage = bronze.S3Storage()
bucket = settings["data_bucket_name"]
collections = sorted({t["collection"] for t in bronze.load_table_map()["tables"]})
out = Path("data/lakehouse_planning/dedup_20261003/manifest_entries.jsonl")
with out.open("w") as handle:
    for collection in collections:
        for key, version in storage.list_keys(bucket, f"{collection}/manifests/"):
            manifest = json.loads(storage.get(bucket, key, version))
            for stored in manifest.get("objects", []):
                obj = stored.get("object", {})
                handle.write(
                    json.dumps(
                        {
                            "collection": collection,
                            "manifest": key,
                            "snapshot": manifest.get("snapshot_id"),
                            "role": stored.get("role"),
                            "key": obj.get("key"),
                            "version": obj.get("version_id"),
                            "sha256": obj.get("sha256"),
                            "size": obj.get("byte_count"),
                            "chain": stored.get("member_chain"),
                            "fields": sorted(stored),
                            "object_fields": sorted(obj),
                        }
                    )
                    + "\n"
                )
        sys.stderr.write(collection + "\n")
