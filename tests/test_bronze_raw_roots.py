from __future__ import annotations

import hashlib
import json
from pathlib import Path

from scripts.lakehouse import bronze
from tests.support import check

BUCKET = "bucket"


class Storage:
    """Versioned objects in memory, standing in for S3."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put(self, key: str, data: bytes) -> str:
        version = f"v-{hashlib.sha256(key.encode() + data).hexdigest()[:12]}"
        self.objects[(key, version)] = data
        return version

    def list_keys(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        return sorted((key, version) for key, version in self.objects if key.startswith(prefix))

    def list_current(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        return self.list_keys(bucket, prefix)

    def get(self, bucket: str, key: str, version_id: str) -> bytes:
        return self.objects[(key, version_id)]


def store(storage: Storage, root: str, snapshot: str, name: str, data: bytes) -> dict[str, str]:
    collection = f"{root}state/hospitals"
    digest = hashlib.sha256(data).hexdigest()
    key = f"{collection}/datasets/ca/release_date=2026-01-01/{digest}/{name}"
    version = storage.put(key, data)
    obj = {"role": "data", "dataset_id": "ca", "object": {"bucket": BUCKET, "key": key, "version_id": version, "sha256": digest, "byte_count": len(data)}}
    manifest = {"snapshot_id": snapshot, "source_id": "CA", "dataset_id": "ca", "release_id": "2026-01-01", "objects": [obj]}
    storage.put(f"{collection}/manifests/release_date=2026-01-01/{snapshot}/ca/manifest.json", json.dumps(manifest).encode())
    return {"key": key, "version_id": version}


def test_bronze_reads_a_table_held_in_both_roots_with_exact_keys_and_versions(tmp_path: Path) -> None:
    storage = Storage()
    old = store(storage, "", "CA__old", "CA_file_001.csv", b"a,b\n1,2\n")
    new = store(storage, "raw/", "CA__new", "CA_file_002.csv", b"a,b\n3,4\n")
    table = {"table": "hospitals", "group": "r", "collection": "state/hospitals", "member_pattern": "\\.csv$", "format": "csv"}
    path = tmp_path / "tables.json"
    path.write_text(json.dumps({"version": 1, "namespace": "bronze", "tables": [table]}))
    config = bronze.load_table_map(path)
    inputs, _ = bronze.discover(storage, BUCKET, config, ["hospitals"])
    found = {(item["key"], item["version_id"]) for item in inputs}
    check(found == {(old["key"], old["version_id"]), (new["key"], new["version_id"])}, "both roots load with their exact keys and versions")
    check({item["key"].startswith("raw/") for item in inputs} == {True, False}, "one input per root")
    only_old = bronze.discover(storage, BUCKET, config, ["hospitals"], roots=("",))[0]
    check([item["key"] for item in only_old] == [old["key"]], "a configured root list limits where bronze reads")
