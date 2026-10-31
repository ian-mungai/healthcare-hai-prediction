from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

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


class GrantedStorage(Storage):
    """Storage whose role may list only the given roots, like the submitted source-prefix policy. Any other list is AccessDenied."""

    def __init__(self, allow_raw: bool) -> None:
        super().__init__()
        self.allow_raw = allow_raw
        self.listed: list[str] = []

    def list_keys(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        self.listed.append(prefix)
        if prefix.startswith("raw/") and not self.allow_raw:
            raise PermissionError("AccessDenied: the role has no grant under raw/")
        return super().list_keys(bucket, prefix)


def store(storage: Storage, root: str, snapshot: str, name: str, data: bytes) -> dict[str, str]:
    collection = f"{root}state/hospitals"
    digest = hashlib.sha256(data).hexdigest()
    key = f"{collection}/datasets/ca/release_date=2026-01-01/{digest}/{name}"
    version = storage.put(key, data)
    obj = {"role": "data", "dataset_id": "ca", "object": {"bucket": BUCKET, "key": key, "version_id": version, "sha256": digest, "byte_count": len(data)}}
    manifest = {"snapshot_id": snapshot, "source_id": "CA", "dataset_id": "ca", "release_id": "2026-01-01", "objects": [obj]}
    storage.put(f"{collection}/manifests/release_date=2026-01-01/{snapshot}/ca/manifest.json", json.dumps(manifest).encode())
    return {"key": key, "version_id": version}


def table_config(tmp_path: Path) -> dict:
    table = {"table": "hospitals", "group": "r", "collection": "state/hospitals", "member_pattern": "\\.csv$", "format": "csv"}
    path = tmp_path / "tables.json"
    path.write_text(json.dumps({"version": 1, "namespace": "bronze", "tables": [table]}))
    return bronze.load_table_map(path)


def test_bronze_reads_only_the_root_while_the_raw_capability_is_off(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("HAI_READ_RAW_ROOT", raising=False)
    storage = GrantedStorage(allow_raw=False)
    old = store(storage, "", "CA__old", "CA_file_001.csv", b"a,b\n1,2\n")
    store(storage, "raw/", "CA__new", "CA_file_002.csv", b"a,b\n3,4\n")
    inputs, _ = bronze.discover(storage, BUCKET, table_config(tmp_path), ["hospitals"])
    check([(item["key"], item["version_id"]) for item in inputs] == [(old["key"], old["version_id"])], "the root object loads with its exact key and version")
    check(not any(prefix.startswith("raw/") for prefix in storage.listed), "raw was never listed")


def test_bronze_lists_both_roots_once_the_capability_is_on_and_a_denied_list_is_not_swallowed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_READ_RAW_ROOT", "true")
    granted = GrantedStorage(allow_raw=True)
    store(granted, "", "CA__old", "CA_file_001.csv", b"a,b\n1,2\n")
    store(granted, "raw/", "CA__new", "CA_file_002.csv", b"a,b\n3,4\n")
    inputs, _ = bronze.discover(granted, BUCKET, table_config(tmp_path), ["hospitals"])
    check(len(inputs) == 2 and any(prefix.startswith("raw/") for prefix in granted.listed), "both roots load when the grant exists")
    denied = GrantedStorage(allow_raw=False)
    store(denied, "", "CA__old", "CA_file_001.csv", b"a,b\n1,2\n")
    with pytest.raises(PermissionError, match="AccessDenied"):
        bronze.discover(denied, BUCKET, table_config(tmp_path), ["hospitals"])


def test_a_collection_with_no_manifest_in_any_read_root_fails_discovery(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("HAI_READ_RAW_ROOT", raising=False)
    storage = Storage()
    with pytest.raises(bronze.BronzeError, match="no manifest"):
        bronze.discover(storage, BUCKET, table_config(tmp_path), ["hospitals"])
    store(storage, "raw/", "CA__new", "CA_file_002.csv", b"a,b\n3,4\n")
    with pytest.raises(bronze.BronzeError, match="no manifest"):
        bronze.discover(storage, BUCKET, table_config(tmp_path), ["hospitals"])


def test_a_deleted_manifest_cannot_leave_a_collection_silently_empty(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("HAI_READ_RAW_ROOT", raising=False)
    storage = Storage()
    store(storage, "", "CA__old", "CA_file_001.csv", b"a,b\n1,2\n")
    config = table_config(tmp_path)
    check(len(bronze.discover(storage, BUCKET, config, ["hospitals"])[0]) == 1, "the stored manifest gives one input")
    for key_version in [item for item in storage.objects if item[0].endswith("/manifest.json")]:
        del storage.objects[key_version]
    with pytest.raises(bronze.BronzeError, match="no manifest"):
        bronze.discover(storage, BUCKET, config, ["hospitals"])


def test_bronze_reads_a_table_held_in_both_roots_with_exact_keys_and_versions(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_READ_RAW_ROOT", "true")
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
