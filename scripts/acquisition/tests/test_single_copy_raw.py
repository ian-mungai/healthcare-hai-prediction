from __future__ import annotations

import base64
import hashlib
from pathlib import Path

import pytest

from scripts.acquisition import s3_store
from scripts.acquisition.tests.test_s3_store import FakeS3
from tests.support import check


def policy_client(objects: dict | None = None) -> FakeS3:
    client = FakeS3()
    client.storage_policy = {"enabled": True, "objects": objects or {}, "evidence_objects": {}, "bootstrap_sha256": None}
    return client


def payload(folder: Path, content: bytes = b"a,b\n1,2\n") -> tuple[Path, str]:
    path = folder / "file.csv"
    path.write_bytes(content)
    return path, hashlib.sha256(content).hexdigest()


def requested(digest: str, area: str = "datasets/table/release_date=2026", root: str = "raw/") -> str:
    return f"{root}example/collection/{area}/{digest}/file.csv"


def written(client: FakeS3) -> list[str]:
    return [args[args.index("--key") + 1] for operation, args in client.calls if operation == "put-object"]


def test_a_new_payload_on_the_raw_route_is_written_under_raw(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    path, digest = payload(tmp_path)
    client = policy_client()
    record, created = s3_store.store_file(client, path, requested(digest))
    check(created and written(client) == [f"raw/bronze_objects/sha256/{digest}"], "the single-copy key keeps the raw root")
    check(record["key"] == f"raw/bronze_objects/sha256/{digest}", "the returned record names the raw key")


def test_new_evidence_on_the_raw_route_is_written_under_raw(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    path, digest = payload(tmp_path)
    client = policy_client()
    s3_store.store_file(client, path, requested(digest, "audit/run"))
    check(written(client) == [f"raw/bronze_evidence/sha256/{digest}"], "the evidence key keeps the raw root")


def test_every_write_on_the_raw_route_stays_inside_raw(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    for enabled in (True, False):
        path, digest = payload(tmp_path, b"x,y\n" + str(enabled).encode())
        client = policy_client()
        client.storage_policy["enabled"] = enabled
        s3_store.store_file(client, path, requested(digest))
        check(written(client) and all(name.startswith("raw/") for name in written(client)), f"policy enabled={enabled} writes inside raw")


def test_an_existing_root_reference_is_reused_without_a_write_or_a_move(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    path, digest = payload(tmp_path)
    root_key = f"bronze_objects/sha256/{digest}"
    candidate = {"key": root_key, "version_id": "version_1", "bucket": "example-bucket", "sha256": digest, "byte_count": path.stat().st_size}
    client = policy_client({digest: candidate})
    client.configuration["data_bucket_name"] = "example-bucket"
    client.objects[root_key] = {
        "VersionId": "version_1",
        "ContentLength": path.stat().st_size,
        "ChecksumSHA256": base64.b64encode(bytes.fromhex(digest)).decode(),
        "ServerSideEncryption": "AES256",
        "body": path.read_bytes(),
    }
    record, created = s3_store.store_file(client, path, requested(digest))
    check(not created and written(client) == [], "reuse writes nothing")
    check(record["key"] == root_key and record["version_id"] == "version_1", "the existing object keeps its key and version")


def test_the_root_layout_still_writes_at_the_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("HAI_CAPTURE_ROOT", raising=False)
    path, digest = payload(tmp_path)
    client = policy_client()
    s3_store.store_file(client, path, requested(digest, root=""))
    check(written(client) == [f"bronze_objects/sha256/{digest}"], "root behavior is unchanged")
