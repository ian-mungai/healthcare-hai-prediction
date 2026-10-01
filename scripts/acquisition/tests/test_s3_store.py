from __future__ import annotations

import base64
import copy
import hashlib
import io
import json
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from scripts.acquisition import s3_store as storage
from scripts.acquisition.capture import capture
from scripts.acquisition.source_registry import read_json
from scripts.acquisition.tests.test_capture import Opener, Response
from scripts.acquisition.tests.test_capture import setup_capture as setup_capture
from scripts.acquisition.transport import Limits
from scripts.process import TimeoutExpired
from tests.support import check

SETTINGS = {
    "expected_account_id": ("1" * 12),
    "aws_profile": "example_project_user",
    "data_bucket_name": "example-test-bucket",
    "aws_region": "eu-west-1",
    "project_name": "example_project",
}
OUTPUTS = {
    "bucket_name": {"value": SETTINGS["data_bucket_name"]},
    "collection_prefixes": {
        "value": {
            name: f"s3://{SETTINGS['data_bucket_name']}/{{publisher}}/{{collection}}/{name}/" for name in ("datasets", "references", "manifests", "audit")
        }
    },
}


class FakeS3(storage.AwsCli):
    def __init__(self) -> None:
        self.configuration = SETTINGS.copy()
        self.objects: dict[str, dict] = {}
        self.calls: list = []
        self.overrides: dict = {}
        self.race = False
        self.put_failure: str | None = None
        self.corrupt = False

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
        args = arguments or []
        self.calls.append((operation, args))
        if operation in self.overrides:
            value = self.overrides[operation]
            if isinstance(value, Exception):
                raise value
            return value
        configuration: dict[str, dict] = {
            "get-caller-identity": {
                "Account": SETTINGS["expected_account_id"],
                "Arn": f"arn:aws:iam::{SETTINGS['expected_account_id']}:user/{SETTINGS['aws_profile']}",
            },
            "get-bucket-location": {"LocationConstraint": SETTINGS["aws_region"]},
            "get-bucket-versioning": {"Status": "Enabled"},
            "get-public-access-block": {
                "PublicAccessBlockConfiguration": dict.fromkeys(("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets"), True)
            },
            "get-bucket-encryption": {"ServerSideEncryptionConfiguration": {"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}},
            "get-bucket-ownership-controls": {"OwnershipControls": {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]}},
        }
        if operation in configuration:
            return configuration[operation]
        key = args[args.index("--key") + 1]
        if operation == "put-object":
            check(args[args.index("--if-none-match") + 1] == "*", 'args[args.index("--if-none-match") + 1] == "*"')
            check(key not in self.objects, "key not in self.objects")
            if self.put_failure:
                raise storage.StorageError(operation, self.put_failure)
            body = Path(args[args.index("--body") + 1]).read_bytes()
            checksum = base64.b64encode(hashlib.sha256(body).digest()).decode()
            check(args[args.index("--checksum-sha256") + 1] == checksum, 'args[args.index("--checksum-sha256") + 1] == checksum')
            self.objects[key] = {
                "VersionId": f"version_{len(self.objects) + 1}",
                "ContentLength": len(body),
                "ChecksumSHA256": checksum,
                "ServerSideEncryption": "AES256",
                "body": body,
            }
            if self.race:
                raise storage.StorageError(operation, "PreconditionFailed")
        if key not in self.objects:
            raise storage.StorageError(operation, "404")
        item = self.objects[key]
        if operation == "get-object":
            check(args[args.index("--version-id") + 1] == item["VersionId"], 'args[args.index("--version-id") + 1] == item["VersionId"]')
            Path(args[-1]).write_bytes(b"corrupted" if self.corrupt else item["body"])
        return {name: value for name, value in item.items() if name != "body"}


@pytest.fixture
def bundle(setup_capture: tuple, tmp_path: Path) -> tuple:
    source, plan, validator = setup_capture
    source["file_routes"][0]["url"] = "https://example.org/archive.zip"
    plan.update(expected_format="zip", file_name="archive.zip")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("release/data.csv", b"id,value\r\n0012A,2\r\n")
        archive.writestr("release/dictionary.pdf", b"%PDF-1.7\nexample documentation\n%%EOF")
    registry = {"sources": [source]}
    path = capture(plan, tmp_path, registry, validator, opener=Opener(Response(buffer.getvalue())))
    references = [{"artifact_id": "artifact_0001", "member": "release/dictionary.pdf", "file_name": "dictionary.pdf", "role": "dictionary", "format": "pdf"}]
    return path, references, FakeS3(), registry, validator


def upload(bundle: tuple) -> dict:
    path, references, client, registry, validator = bundle
    return storage.upload_snapshot(path, references, client, OUTPUTS, registry, validator)


def test_complete_storage_and_replay_are_identical(bundle: tuple) -> None:
    path, _, client, _, _ = bundle
    original = path.read_bytes()
    first = upload(bundle)
    saved = (path.parent / "s3_collections_reconciliation.json").read_bytes()
    versions = copy.deepcopy(client.objects)
    second = upload(bundle)
    check(first["objects_created"] == first["objects_verified"] == 6, 'first["objects_created"] == first["objects_verified"] == 6')
    check(second["objects_created"] == 0 and second["objects_reused"] == 6, 'second["objects_created"] == 0 and second["objects_reused"] == 6')
    check(
        path.read_bytes() == original and (path.parent / "s3_collections_reconciliation.json").read_bytes() == saved,
        'path.read_bytes() == original and (path.parent / "s3_collections_reconciliation.json").read_bytes() == saved',
    )
    check(client.objects == versions and not first["model_eligible"], 'client.objects == versions and not first["model_eligible"]')
    manifest = read_json(next((path.parent / "s3_collections").glob("*/manifest.json")))
    check(manifest["snapshot_status"] == "acquired_unvalidated", 'manifest["snapshot_status"] == "acquired_unvalidated"')
    reference = next(item for item in manifest["objects"] if item["role"] == "dictionary")
    check(
        reference["object"]["key"].split("/")[2] == "references" and reference["parent_sha256"],
        'reference["object"]["key"].split("/")[2] == "references" and reference["parent_sha256"]',
    )
    check(
        manifest["archive_storage_policy"] == "extracted_members_only_zip_retained_locally",
        'manifest["archive_storage_policy"] == "extracted_members_only_zip_retained_locally"',
    )
    check(not any(key.endswith(".zip") for key in client.objects), 'not any(key.endswith(".zip") for key in client.objects)')
    check(
        next(item["body"] for key, item in client.objects.items() if key.endswith("data.csv")) == b"id,value\r\n0012A,2\r\n",
        'next(item["body"] for key, item in client.objects.items() if key.endswith("data.csv")) == b"id,value\\r\\n0012A,2\\r\\n"',
    )
    check(
        {"/".join(key.split("/")[:2]) for key in client.objects} == {"example_publisher/example_collection"},
        '{"/".join(key.split("/")[:2]) for key in client.objects} == {"example_publisher/example_collection"}',
    )
    check(
        {key.split("/")[2] for key in client.objects} == {"datasets", "references", "manifests", "audit"},
        '{key.split("/")[2] for key in client.objects} == {"datasets", "references", "manifests", "audit"}',
    )


def zip_bytes(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, body in members.items():
            archive.writestr(name, body)
    return buffer.getvalue()


@pytest.mark.parametrize("category", ["datasets", "references", "manifests", "audit"])
@pytest.mark.parametrize("name", ["archive.zip", "archive.ZIP", "archive.bin"])
def test_zip_containers_never_reach_s3(tmp_path: Path, category: str, name: str) -> None:
    path = tmp_path / name
    path.write_bytes(zip_bytes({"data.csv": b"id\n1\n"}))
    digest, _ = storage.fingerprint(path)
    key = f"example_publisher/example_collection/{category}/example_table/{digest}/{name}"
    client = FakeS3()
    with pytest.raises(storage.CaptureError, match="ZIP containers must remain local"):
        storage.store_file(client, path, key)
    check(not client.calls, "not client.calls")


def test_zip_destination_is_rejected_even_for_non_zip_source(tmp_path: Path) -> None:
    path = tmp_path / "data.csv"
    path.write_bytes(b"id\n1\n")
    digest, _ = storage.fingerprint(path)
    client = FakeS3()
    with pytest.raises(storage.CaptureError, match="ZIP containers must remain local"):
        storage.store_file(client, path, f"example_publisher/example_collection/datasets/example_table/{digest}/archive.zip")
    check(not client.calls, "not client.calls")


@pytest.mark.parametrize("extension,part", [("xlsx", "xl/workbook.xml"), ("docx", "word/document.xml")])
def test_native_office_documents_stay_intact(setup_capture: tuple, tmp_path: Path, extension: str, part: str) -> None:
    source, plan, validator = setup_capture
    plan.update(expected_format=extension, file_name=f"data.{extension}")
    content = zip_bytes({"[Content_Types].xml": b"<Types/>", part: b"<document/>"})
    registry = {"sources": [source]}
    path = capture(plan, tmp_path, registry, validator, opener=Opener(Response(content)))
    client = FakeS3()
    storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator)
    data = [item["body"] for key, item in client.objects.items() if key.split("/")[2] == "datasets"]
    check(data == [content] and not (path.parent / "expanded").exists(), 'data == [content] and not (path.parent / "expanded").exists()')


@pytest.mark.parametrize("content", [zip_bytes({"data.csv": b"id\n1\n"}), b"PK\x03\x04truncated"])
def test_zip_cannot_masquerade_as_office_document(tmp_path: Path, content: bytes) -> None:
    path = tmp_path / "data.xlsx"
    path.write_bytes(content)
    digest, _ = storage.fingerprint(path)
    client = FakeS3()
    with pytest.raises(storage.CaptureError, match="Office document"):
        storage.store_file(client, path, f"example_publisher/example_collection/datasets/example_table/{digest}/data.xlsx")
    check(not client.calls, "not client.calls")


def test_renamed_zip_is_expanded_before_storage(setup_capture: tuple, tmp_path: Path) -> None:
    source, plan, validator = setup_capture
    plan.update(expected_format="zip", file_name="archive.bin")
    content = zip_bytes({"data.csv": b"id\n1\n"})
    registry = {"sources": [source]}
    path = capture(plan, tmp_path, registry, validator, opener=Opener(Response(content)))
    client = FakeS3()
    storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator)
    check(all(item["body"] != content for item in client.objects.values()), 'all(item["body"] != content for item in client.objects.values())')
    check(any(key.endswith("data.csv") for key in client.objects), 'any(key.endswith("data.csv") for key in client.objects)')
    check((path.parent / "raw/archive.bin").read_bytes() == content, '(path.parent / "raw/archive.bin").read_bytes() == content')


@pytest.mark.parametrize("status,content", [(206, zip_bytes({"data.csv": b"id\n1\n"})), (403, b"PK\x03\x04truncated")])
def test_failed_zip_retains_payload_locally_and_uploads_metadata_only(setup_capture: tuple, tmp_path: Path, status: int, content: bytes) -> None:
    source, plan, validator = setup_capture
    plan.update(expected_format="zip", file_name="archive.zip")
    registry = {"sources": [source]}
    path = capture(plan, tmp_path, registry, validator, opener=Opener(Response(content, status)))
    client = FakeS3()
    storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator, audit_only=True)
    check(
        client.objects and all(key.split("/")[2] == "audit" and not key.lower().endswith(".zip") for key in client.objects),
        'client.objects and all(key.split("/")[2] == "audit" and not key.lower().endswith(".zip") for key in client.objects)',
    )
    check(all(item["body"] != content for item in client.objects.values()), 'all(item["body"] != content for item in client.objects.values())')
    manifest = read_json(next((path.parent / "s3_collections").glob("*/manifest.json")))
    check(
        manifest["archive_storage_policy"] == "extracted_members_only_zip_retained_locally",
        'manifest["archive_storage_policy"] == "extracted_members_only_zip_retained_locally"',
    )
    check(len(manifest["local_only_archive_payloads"]) == 1, 'len(manifest["local_only_archive_payloads"]) == 1')
    payload = manifest["local_only_archive_payloads"][0]
    check((path.parent / payload["storage_path"]).read_bytes() == content, '(path.parent / payload["storage_path"]).read_bytes() == content')
    check(
        payload["sha256"] == hashlib.sha256(content).hexdigest() and payload["byte_count"] == len(content),
        'payload["sha256"] == hashlib.sha256(content).hexdigest() and payload["byte_count"] == len(content)',
    )
    check(
        storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator, audit_only=True)["objects_created"] == 0,
        'storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator, audit_only=True)["objects_created"] == 0',
    )


def test_failed_zip_retry_is_not_uploaded_with_successful_capture(setup_capture: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts.acquisition import transport

    monkeypatch.setattr(transport.time, "sleep", lambda _: None)
    source, plan, validator = setup_capture
    plan.update(expected_format="zip", file_name="archive.zip")
    failed = b"PK\x03\x04truncated"
    content = zip_bytes({"data.csv": b"id\n1\n"})
    registry = {"sources": [source]}
    path = capture(plan, tmp_path, registry, validator, limits=Limits(attempts=2), opener=Opener(Response(failed, 503), Response(content)))
    client = FakeS3()
    storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator)
    check(not any(key.lower().endswith(".zip") for key in client.objects), 'not any(key.lower().endswith(".zip") for key in client.objects)')
    check(
        all(item["body"] not in {failed, content} for item in client.objects.values()),
        'all(item["body"] not in {failed, content} for item in client.objects.values())',
    )
    manifest = read_json(next((path.parent / "s3_collections").glob("*/manifest.json")))
    check(
        manifest["local_only_archive_payloads"][0]["storage_path"] == "audit/request_0001/attempt_01/archive.zip",
        'manifest["local_only_archive_payloads"][0]["storage_path"] == "audit/request_0001/attempt_01/archive.zip"',
    )
    check(
        (path.parent / "audit/request_0001/attempt_01/archive.zip").read_bytes() == failed,
        '(path.parent / "audit/request_0001/attempt_01/archive.zip").read_bytes() == failed',
    )
    check(
        storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator)["objects_created"] == 0,
        'storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator)["objects_created"] == 0',
    )


def test_disguised_nested_zip_blocks_all_uploads(setup_capture: tuple, tmp_path: Path) -> None:
    source, plan, validator = setup_capture
    plan.update(expected_format="zip", file_name="archive.zip")
    nested = zip_bytes({"data.csv": b"id\n1\n"})
    registry = {"sources": [source]}
    path = capture(plan, tmp_path, registry, validator, opener=Opener(Response(zip_bytes({"nested.bin": nested}))))
    client = FakeS3()
    with pytest.raises(storage.CaptureError, match="Nested ZIP"):
        storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator)
    check(not client.objects, "not client.objects")


def test_existing_different_bytes_are_never_overwritten(bundle: tuple) -> None:
    upload(bundle)
    client = bundle[2]
    key = next(iter(client.objects))
    client.objects[key]["body"] = b"different"
    before = len([call for call in client.calls if call[0] == "put-object"])
    with pytest.raises(storage.CaptureError, match="byte hash"):
        upload(bundle)
    check(
        len([call for call in client.calls if call[0] == "put-object"]) == before, 'len([call for call in client.calls if call[0] == "put-object"]) == before'
    )


def test_fresh_capture_reuses_data_and_reference_versions(bundle: tuple, setup_capture: tuple, tmp_path: Path) -> None:
    upload(bundle)
    first_path, references, client, registry, validator = bundle
    source, plan, _ = setup_capture
    plan.update(expected_format="zip", file_name="archive.zip")
    source["file_routes"][0]["url"] = "https://example.org/archive.zip"
    archive = first_path.parent / read_json(first_path)["artifacts"][0]["storage_path"]
    newer = capture(plan, tmp_path / "next_capture", registry, validator, opener=Opener(Response(archive.read_bytes())))
    original_versions = {key: item["VersionId"] for key, item in client.objects.items() if key.split("/")[2] in {"datasets", "references"}}
    result = storage.upload_snapshot(newer, references, client, OUTPUTS, registry, validator)
    check(result["objects_created"] == 4 and result["objects_reused"] == 2, 'result["objects_created"] == 4 and result["objects_reused"] == 2')
    check(
        original_versions == {key: item["VersionId"] for key, item in client.objects.items() if key.split("/")[2] in {"datasets", "references"}},
        'original_versions == {key: item["VersionId"] for key, item in client.objects.items() if key.split("/")[2] in {"datasets", "references"}}',
    )


def test_permission_denied_is_not_absence(bundle: tuple) -> None:
    bundle[2].overrides["head-object"] = storage.StorageError("head-object", "403")
    with pytest.raises(storage.StorageError, match="403"):
        upload(bundle)
    check(not bundle[2].objects, "not bundle[2].objects")


def test_concurrent_identical_writer_is_reused(bundle: tuple) -> None:
    bundle[2].race = True
    report = upload(bundle)
    check(report["objects_created"] == 0 and report["objects_reused"] == 6, 'report["objects_created"] == 0 and report["objects_reused"] == 6')


def test_failed_upload_is_resumable_without_duplicate_versions(bundle: tuple) -> None:
    bundle[2].put_failure = "AccessDenied"
    with pytest.raises(storage.StorageError, match="AccessDenied"):
        upload(bundle)
    check(not (bundle[0].parent / "s3_collections_reconciliation.json").exists(), 'not (bundle[0].parent / "s3_collections_reconciliation.json").exists()')
    bundle[2].put_failure = None
    check(upload(bundle)["objects_created"] == 6, 'upload(bundle)["objects_created"] == 6')


def test_verification_failure_keeps_uploaded_object_for_retry(bundle: tuple) -> None:
    bundle[2].corrupt = True
    with pytest.raises(storage.CaptureError, match="byte hash"):
        upload(bundle)
    check(len(bundle[2].objects) == 1, "len(bundle[2].objects) == 1")
    bundle[2].corrupt = False
    check(upload(bundle)["objects_created"] == 5, 'upload(bundle)["objects_created"] == 5')


@pytest.mark.parametrize(
    "operation,payload,match",
    [
        ("get-caller-identity", {"Account": "000000000000", "Arn": "invalid"}, "identity"),
        ("get-caller-identity", {"Account": ("1" * 12), "Arn": ("arn:aws:sts::" + "1" * 12 + ":assumed-role/example/session")}, "identity"),
        ("get-caller-identity", {"Account": ("1" * 12), "Arn": ("arn:aws:iam::" + "1" * 12 + ":user/other_user")}, "same-named"),
        ("get-bucket-location", {"LocationConstraint": "us-east-2"}, "region"),
        ("get-bucket-versioning", {"Status": "Suspended"}, "versioning"),
        ("get-public-access-block", {"PublicAccessBlockConfiguration": {}}, "public-access"),
        ("get-bucket-encryption", {"ServerSideEncryptionConfiguration": {"Rules": []}}, "encryption"),
        ("get-bucket-ownership-controls", {}, "ownership"),
    ],
)
def test_preflight_holds_precede_writes(bundle: tuple, operation: str, payload: dict, match: str) -> None:
    bundle[2].overrides[operation] = payload
    with pytest.raises(storage.CaptureError, match=match):
        upload(bundle)
    check(not bundle[2].objects, "not bundle[2].objects")


def test_terraform_mismatch_blocks_storage(bundle: tuple) -> None:
    with pytest.raises(storage.CaptureError, match="Terraform"):
        storage.upload_snapshot(bundle[0], bundle[1], bundle[2], {}, bundle[3], bundle[4])
    check(not bundle[2].objects, "not bundle[2].objects")


@pytest.mark.parametrize("version", [None, "null", ""])
def test_missing_version_is_rejected(tmp_path: Path, version: str | None) -> None:
    with pytest.raises(storage.CaptureError, match="version ID"):
        storage.verify_version(FakeS3(), "raw/example", version, "0" * 64, 1)


def test_server_checksum_is_required(bundle: tuple) -> None:
    upload(bundle)
    client = bundle[2]
    next(iter(client.objects.values()))["ChecksumSHA256"] = "invalid"
    with pytest.raises(storage.CaptureError, match="checksum"):
        upload(bundle)


@pytest.mark.parametrize("key", ["other/file", "raw/../file", "/raw/file", "raw/not_content_addressed"])
def test_unapproved_keys_cannot_upload(tmp_path: Path, key: str) -> None:
    path = tmp_path / "file"
    path.write_bytes(b"test")
    with pytest.raises(storage.CaptureError):
        storage.store_file(FakeS3(), path, key)


@pytest.mark.parametrize("value", ["..", "", "../escape", "has space", "a/b"])
def test_key_components_are_bounded(value: str) -> None:
    with pytest.raises(storage.CaptureError):
        storage.component(value)


def test_incomplete_capture_does_not_upload(bundle: tuple) -> None:
    path = bundle[0]
    receipt = read_json(path)
    receipt["snapshot_status"], receipt["verification"]["status"] = "evidence_only_partial", "evidence_only"
    path.write_bytes(storage.encoded_json(receipt))
    with pytest.raises(storage.CaptureError, match="Only complete"):
        upload(bundle)
    check(not bundle[2].calls, "not bundle[2].calls")


def test_changed_registry_blocks_upload(bundle: tuple) -> None:
    bundle[3]["extra"] = True
    with pytest.raises(storage.CaptureError, match="fingerprints"):
        upload(bundle)
    check(not bundle[2].calls, "not bundle[2].calls")


@pytest.mark.parametrize("mutation", ["route", "hold"])
def test_route_checks_block_changed_receipt(bundle: tuple, mutation: str) -> None:
    path, _, client, registry, _ = bundle
    receipt = read_json(path)
    if mutation == "route":
        receipt["acquisition"]["requested_url"] = "https://example.org/unreviewed.zip"
    else:
        registry["sources"][0]["preferred_route"] = "access_hold"
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        lineage["registry_sha256"] = storage.canonical_hash(registry)
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
    path.write_bytes(storage.encoded_json(receipt))
    with pytest.raises(storage.CaptureError):
        upload(bundle)
    check(not client.calls, "not client.calls")


@pytest.mark.parametrize(
    "change,match",
    [
        ({"extra": True}, "exact member"),
        ({"role": "data"}, "roles"),
        ({"member": "../outside"}, "Unsafe"),
        ({"artifact_id": "missing"}, "parent"),
        ({"member": "absent.pdf"}, "missing"),
        ({"format": "json"}, "validation failed"),
    ],
)
def test_invalid_reference_blocks_upload(bundle: tuple, change: dict, match: str) -> None:
    bundle[1][0].update(change)
    with pytest.raises(storage.CaptureError, match=match):
        upload(bundle)
    check(not bundle[2].objects, "not bundle[2].objects")


def test_duplicate_reference_names_block_upload(bundle: tuple) -> None:
    bundle[1].append(bundle[1][0].copy())
    with pytest.raises(storage.CaptureError, match="unique"):
        upload(bundle)
    check(not bundle[2].objects, "not bundle[2].objects")


@pytest.mark.parametrize("symlink", [False, True])
def test_unexpected_audit_paths_block_upload(bundle: tuple, symlink: bool, tmp_path: Path) -> None:
    path = bundle[0].parent / "audit" / "extra.txt"
    if symlink:
        target = tmp_path / "outside"
        target.write_text("private")
        path.symlink_to(target)
    else:
        path.write_text("unregistered")
    with pytest.raises(storage.CaptureError, match="Audit evidence|Unexpected audit"):
        upload(bundle)
    check(not bundle[2].objects, "not bundle[2].objects")


def test_write_once_cannot_replace_existing_content(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    storage.write_once(path, b"original")
    storage.write_once(path, b"original")
    with pytest.raises(storage.CaptureError, match="overwrite"):
        storage.write_once(path, b"changed")
    check(path.read_bytes() == b"original", 'path.read_bytes() == b"original"')


def test_cli_pins_identity_owner_and_clears_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "synthetic_key")
    monkeypatch.setenv("AWS_PROFILE", "other_profile")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://example.org/not_aws")
    calls = []

    def runner(command: list, **kwargs: Any) -> SimpleNamespace:
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout='{"Status":"Enabled"}', stderr="")

    client = storage.AwsCli(SETTINGS, runner)
    check(client.call("s3api", "get-bucket-versioning")["Status"] == "Enabled", 'client.call("s3api", "get-bucket-versioning")["Status"] == "Enabled"')
    command, kwargs = calls[0]
    check(command[command.index("--profile") + 1] == SETTINGS["aws_profile"], 'command[command.index("--profile") + 1] == SETTINGS["aws_profile"]')
    check(
        command[command.index("--expected-bucket-owner") + 1] == SETTINGS["expected_account_id"],
        'command[command.index("--expected-bucket-owner") + 1] == SETTINGS["expected_account_id"]',
    )
    check(
        not {"AWS_ACCESS_KEY_ID", "AWS_PROFILE", "AWS_ENDPOINT_URL"} & kwargs["env"].keys(),
        'not {"AWS_ACCESS_KEY_ID", "AWS_PROFILE", "AWS_ENDPOINT_URL"} & kwargs["env"].keys()',
    )
    check(kwargs["env"]["AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"] == "true", 'kwargs["env"]["AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"] == "true"')
    check(kwargs["timeout"] == 180, 'kwargs["timeout"] == 180')


@pytest.mark.parametrize(
    "response,code",
    [
        (SimpleNamespace(returncode=1, stdout="", stderr="An error occurred (403) when calling HeadObject: private details"), "403"),
        (SimpleNamespace(returncode=1, stdout="", stderr="sensitive unstructured error"), "request_failed"),
        (SimpleNamespace(returncode=0, stdout="not json", stderr=""), "invalid_response"),
        (SimpleNamespace(returncode=0, stdout="[]", stderr=""), "invalid_response"),
        (OSError("private details"), "request_unconfirmed"),
        (TimeoutExpired("aws", 180), "request_unconfirmed"),
    ],
)
def test_cli_failures_are_redacted(response: object, code: str) -> None:
    def runner(*args: Any, **kwargs: Any) -> object:
        if isinstance(response, Exception):
            raise response
        return response

    with pytest.raises(storage.StorageError, match=code) as error:
        storage.AwsCli(SETTINGS, runner).call("sts", "get-caller-identity")
    check(
        "private details" not in str(error.value) and "sensitive" not in str(error.value),
        '"private details" not in str(error.value) and "sensitive" not in str(error.value)',
    )


def test_main_uses_configuration_and_local_outputs(bundle: tuple, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    reference_path = tmp_path / "refs.json"
    reference_path.write_text(json.dumps({"references": bundle[1]}))
    monkeypatch.setattr(sys, "argv", ["s3_store", "--receipt", str(bundle[0]), "--references", str(reference_path)])
    monkeypatch.setattr(storage, "load_configuration", lambda _: (SETTINGS, {}))
    monkeypatch.setattr(storage, "run_argv", lambda *args, **kwargs: SimpleNamespace(stdout=json.dumps(OUTPUTS)))
    monkeypatch.setattr(storage, "AwsCli", lambda _: bundle[2])
    monkeypatch.setattr(storage, "load_registry", lambda: bundle[3])
    monkeypatch.setattr(storage, "receipt_validator", lambda: bundle[4])
    storage.main()
    check(json.loads(capsys.readouterr().out)["objects_created"] == 6, 'json.loads(capsys.readouterr().out)["objects_created"] == 6')
    reference_path.write_text('{"references":{}}')
    with pytest.raises(SystemExit):
        storage.main()


def test_main_reports_configuration_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["s3_store", "--receipt", "example.json", "--references", "refs.json"])

    def fail(*args: Any, **kwargs: Any) -> None:
        raise ValueError("synthetic configuration error")

    monkeypatch.setattr(storage, "load_configuration", fail)
    with pytest.raises(SystemExit):
        storage.main()
