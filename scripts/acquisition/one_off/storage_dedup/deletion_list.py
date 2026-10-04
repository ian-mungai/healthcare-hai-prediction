"""Read-only: build the S3 duplicate deletion list (failure modes 212 to 214) from the manifests and bronze's kept copies.

Kept: every object a bronze table loads, and for each other checksum the object with the smallest object key (a hash of
snapshot and member chain, the bronze rule). Listed for deletion: every other current object with a kept twin.
"""

import collections
import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.getcwd())
bronze = __import__("scripts.lakehouse.bronze", fromlist=["bronze"])
deployment = __import__("scripts.lakehouse.catalog", fromlist=["catalog"]).deployment

HERE = Path("data/lakehouse_planning/dedup_20261003")
settings = deployment()
os.environ.setdefault("AWS_PROFILE", settings["aws_profile"])
os.environ.setdefault("AWS_REGION", settings["aws_region"])
config = bronze.load_table_map()
retired = bronze.load_retired()
inputs, unselected = bronze.discover(bronze.S3Storage(), settings["data_bucket_name"], config, [t["table"] for t in config["tables"]], retired)
loaded = {(item["key"], item["version_id"]) for item in inputs}

objects: dict[tuple[str, str], dict] = {}
for line in (HERE / "manifest_entries.jsonl").open():
    entry = json.loads(line)
    identity = (entry["key"], entry["version"])
    chain = entry["chain"] or [entry["key"].rsplit("/", 1)[-1]]
    object_key = hashlib.sha256(f"{entry['snapshot']}\x00{json.dumps(chain)}".encode()).hexdigest()[:32]
    current = objects.setdefault(identity, {**entry, "object_key": object_key, "roles": set()})
    current["object_key"] = min(current["object_key"], object_key)
    current["roles"].add(entry["role"])
live = {identity: entry for identity, entry in objects.items() if identity not in retired}
by_sha = collections.defaultdict(list)
for identity, entry in live.items():
    by_sha[entry["sha256"]].append(identity)
kept: set[tuple[str, str]] = set()
for _digest, identities in by_sha.items():
    chosen = {identity for identity in identities if identity in loaded}
    kept |= chosen or {min(identities, key=lambda identity: (live[identity]["object_key"], identity))}
listed = []
for digest, identities in sorted(by_sha.items()):
    survivors = sorted(identity for identity in identities if identity in kept)
    for identity in sorted(identities):
        if identity in kept:
            continue
        entry = live[identity]
        listed.append(
            {
                "key": identity[0],
                "version_id": identity[1],
                "sha256": digest,
                "byte_count": entry["size"],
                "collection": entry["collection"],
                "roles": sorted(entry["roles"]),
                "reason": "duplicate_of",
                "kept": {"key": survivors[0][0], "version_id": survivors[0][1]},
            }
        )
summary = collections.Counter(entry["collection"] for entry in listed)
size: collections.Counter[str] = collections.Counter()
for entry in listed:
    size[entry["collection"]] += entry["byte_count"] or 0
report = {
    "objects_listed": len(listed),
    "bytes_listed": sum(size.values()),
    "checksums_with_two_kept_objects": sum(1 for ids in by_sha.values() if len([i for i in ids if i in kept]) > 1),
    "listed_that_bronze_loads": sum(1 for entry in listed if (entry["key"], entry["version_id"]) in loaded),
    "by_collection": {name: {"objects": summary[name], "bytes": size[name]} for name in sorted(summary)},
    "by_role": dict(collections.Counter(role for entry in listed for role in entry["roles"])),
}
(HERE / "deletion_list.json").write_text(json.dumps({"version": 1, "basis": "failure modes 212 to 217", "summary": report, "objects": listed}, indent=1) + "\n")
sys.stdout.write(json.dumps(report, indent=1) + "\n")
