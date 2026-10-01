from __future__ import annotations

import copy
import io
import json
import zipfile
from pathlib import Path

import pytest

from scripts.acquisition import dataset_layout as layout
from scripts.acquisition import s3_store as storage
from scripts.acquisition.capture import capture
from scripts.acquisition.source_registry import read_json
from scripts.acquisition.tests.test_capture import Opener, Response
from scripts.acquisition.tests.test_capture import setup_capture as setup_capture
from scripts.acquisition.tests.test_s3_store import OUTPUTS, FakeS3
from scripts.acquisition.tests.test_s3_store import bundle as bundle
from tests.support import check


def cms_bundle(setup: tuple, root: Path, mutation: str | None = None) -> tuple:
    source, plan, validator = setup
    source["file_routes"][0]["url"] = "https://data.cms.gov/example/archive.zip"
    plan.update(expected_format="zip", file_name="archive.zip")
    records: list[dict] = []
    files: dict[str, bytes] = {}
    for identifier in ("test-one", "test-two"):
        name, body = f"{identifier}_2020-01-01_dataset.csv", b"id,value\r\n0012A,2\r\n"
        files["release/" + name] = body
        records.append(
            {
                "dataset_id": identifier,
                "name": identifier,
                "private": False,
                "modified_date": "2020-01-01",
                "resources": [{"filename": name, "filesize": len(body)}],
            }
        )
    files["release/dictionary.pdf"] = b"%PDF-1.7\nshared documentation\n%%EOF"
    if mutation == "duplicate":
        records.append(copy.deepcopy(records[0]))
    elif mutation == "private":
        records[0]["private"] = True
    elif mutation == "size":
        records[0]["resources"][0]["filesize"] += 1
    elif mutation == "missing":
        files.pop("release/" + records[0]["resources"][0]["filename"])
    elif mutation == "traversal":
        records[0]["resources"][0]["filename"] = "../outside.csv"
    elif mutation == "empty_resources":
        records[0]["resources"] = []
    elif mutation == "unmapped":
        files["release/unmapped.csv"] = b"id\n1\n"
    elif mutation == "non_cms":
        source["file_routes"][0]["url"] = "https://example.org/archive.zip"
    if mutation != "no_manifest":
        files["release/manifest.json"] = json.dumps([] if mutation == "empty_manifest" else records).encode()
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    registry = {"sources": [source]}
    receipt = capture(plan, root, registry, validator, opener=Opener(Response(stream.getvalue())))
    return receipt, [], FakeS3(), registry, validator


def test_collection_keeps_separate_tables_and_one_shared_dictionary(setup_capture: tuple, tmp_path: Path) -> None:
    receipt, references, client, registry, validator = cms_bundle(setup_capture, tmp_path)
    progress: list[dict] = []
    result = storage.upload_snapshot(receipt, references, client, OUTPUTS, registry, validator, progress=progress.append)
    check([item["datasets_verified"] for item in progress] == [1, 2], '[item["datasets_verified"] for item in progress] == [1, 2]')
    check(result["dataset_count"] == 2 and result["objects_created"] == 9, 'result["dataset_count"] == 2 and result["objects_created"] == 9')
    check({key.split("/")[0] for key in client.objects} == {"example_publisher"}, '{key.split("/")[0] for key in client.objects} == {"example_publisher"}')
    check(sum(key.endswith("dictionary.pdf") for key in client.objects) == 1, 'sum(key.endswith("dictionary.pdf") for key in client.objects) == 1')
    for folder in ("cms_test_one", "cms_test_two"):
        manifest = read_json(receipt.parent / "s3_collections" / folder / "manifest.json")
        check(
            manifest["storage_contract_version"] == "4.0.0" and not manifest["model_eligible"],
            'manifest["storage_contract_version"] == "4.0.0" and not manifest["model_eligible"]',
        )
        check(
            {item["object"]["key"].split("/")[3] for item in manifest["objects"] if item["role"] == "data"} == {folder},
            '{item["object"]["key"].split("/")[3] for item in manifest["objects"] if item["role"] == "data"} == {folder}',
        )
        check(
            len([item for item in manifest["objects"] if item["role"] == "data"]) == 1,
            'len([item for item in manifest["objects"] if item["role"] == "data"]) == 1',
        )
        dictionary = next(item for item in manifest["objects"] if item.get("archive_member", "").endswith("dictionary.pdf"))
        check(
            dictionary["shared_bundle_reference"] and dictionary["applicability_status"] == "not_reviewed",
            'dictionary["shared_bundle_reference"] and dictionary["applicability_status"] == "not_reviewed"',
        )
        check(
            dictionary["dictionary_sections"] == manifest["dictionary_sections"] == [],
            'dictionary["dictionary_sections"] == manifest["dictionary_sections"] == []',
        )
        check(manifest["dictionary_status"] == "applicability_and_sections_pending", 'manifest["dictionary_status"] == "applicability_and_sections_pending"')
        check(
            client.objects[dictionary["object"]["key"]]["body"].startswith(b"%PDF-1.7"),
            'client.objects[dictionary["object"]["key"]]["body"].startswith(b"%PDF-1.7")',
        )
    before = copy.deepcopy(client.objects)
    replay = storage.upload_snapshot(receipt, references, client, OUTPUTS, registry, validator)
    check(replay["objects_created"] == 0 and client.objects == before, 'replay["objects_created"] == 0 and client.objects == before')
    check(not any(key.endswith(".zip") for key in client.objects), 'not any(key.endswith(".zip") for key in client.objects)')


@pytest.mark.parametrize(
    "mutation", ["duplicate", "private", "missing", "traversal", "empty_resources", "unmapped", "no_manifest", "empty_manifest", "non_cms"]
)
def test_unverified_archive_mapping_stops_before_object_writes(setup_capture: tuple, tmp_path: Path, mutation: str) -> None:
    receipt, references, client, registry, validator = cms_bundle(setup_capture, tmp_path, mutation)
    with pytest.raises(storage.CaptureError):
        storage.upload_snapshot(receipt, references, client, OUTPUTS, registry, validator)
    check(not client.objects, "not client.objects")


def test_publisher_size_conflict_is_preserved_only_as_audit(setup_capture: tuple, tmp_path: Path) -> None:
    receipt, references, client, registry, validator = cms_bundle(setup_capture, tmp_path, "size")
    result = storage.upload_snapshot(receipt, references, client, OUTPUTS, registry, validator)
    check(result["dataset_count"] == 2, 'result["dataset_count"] == 2')
    manifest = read_json(receipt.parent / "s3_collections/cms_test_one/manifest.json")
    held = next(item for item in manifest["objects"] if item.get("hold_reason"))
    check(held["object"]["key"].split("/")[2] == "audit", 'held["object"]["key"].split("/")[2] == "audit"')
    check(held["publisher_byte_count"] != held["actual_byte_count"], 'held["publisher_byte_count"] != held["actual_byte_count"]')
    check(
        manifest["acquisition_holds"][0]["reason"] == "publisher_member_size_mismatch",
        'manifest["acquisition_holds"][0]["reason"] == "publisher_member_size_mismatch"',
    )
    check(
        client.objects[held["object"]["key"]]["body"] == b"id,value\r\n0012A,2\r\n",
        'client.objects[held["object"]["key"]]["body"] == b"id,value\\r\\n0012A,2\\r\\n"',
    )


@pytest.mark.parametrize("identifier", ["", "../escape", "MixedCase", "has-hyphens", "references", "audit", "x" * 121])
def test_invalid_dataset_names_are_rejected(identifier: str) -> None:
    with pytest.raises(storage.CaptureError, match="Dataset folder"):
        layout.dataset_id(identifier)


def test_source_normalization_collision_is_rejected() -> None:
    receipt = {"source": {"source_record_id": "example-one"}}
    with pytest.raises(storage.CaptureError, match="collide"):
        layout.group_entries(Path("."), receipt, [], set(), {"sources": [{"source_id": "example-one"}, {"source_id": "example_one"}]})


def test_same_bytes_cannot_reuse_another_dataset_folder(setup_capture: tuple, tmp_path: Path) -> None:
    receipt, references, client, registry, validator = cms_bundle(setup_capture, tmp_path)
    shared: dict = {}
    storage.upload_snapshot(receipt, references, client, OUTPUTS, registry, validator, shared=shared)
    records = list(shared.values())
    check(
        len(records) == 2 and records[0]["sha256"] == records[1]["sha256"] and records[0]["key"] != records[1]["key"],
        'len(records) == 2 and records[0]["sha256"] == records[1]["sha256"] and records[0]["key"] != records[1]["key"]',
    )
    shared[next(iter(shared))] = records[1]
    with pytest.raises(storage.CaptureError, match="Shared storage reference"):
        storage.upload_snapshot(receipt, references, client, OUTPUTS, registry, validator, shared=shared)


def test_legacy_evidence_is_not_overwritten(setup_capture: tuple, tmp_path: Path) -> None:
    receipt, references, client, registry, validator = cms_bundle(setup_capture, tmp_path)
    evidence = [receipt.parent / name for name in ("s3_manifest.json", "s3_reconciliation.json", "s3_members_manifest.json")]
    for path in evidence:
        path.write_text('{"legacy":true}')
    storage.upload_snapshot(receipt, references, client, OUTPUTS, registry, validator)
    check(all(path.read_text() == '{"legacy":true}' for path in evidence), "all(path.read_text() == '{\"legacy\":true}' for path in evidence)")


def test_legacy_terraform_outputs_do_not_enable_new_writes(setup_capture: tuple, tmp_path: Path) -> None:
    receipt, references, client, registry, validator = cms_bundle(setup_capture, tmp_path)
    outputs = {"bucket_name": OUTPUTS["bucket_name"], "dataset_prefixes": OUTPUTS["collection_prefixes"]}
    with pytest.raises(storage.CaptureError, match="Terraform outputs"):
        storage.upload_snapshot(receipt, references, client, outputs, registry, validator)
    check(not client.objects, "not client.objects")


def test_cms_single_table_archive_does_not_require_hospital_manifest(bundle: tuple) -> None:
    receipt, _, _, registry, _ = bundle
    item = {"role": "data", "storage_path": "raw/data.csv", "archive_member": "data.csv"}
    captured = read_json(receipt)
    captured["acquisition"]["requested_url"] = "https://data.cms.gov/single_table.zip"
    groups = layout.group_entries(receipt.parent, captured, [item], {"a" * 64}, registry)
    check(len(groups) == 1, "len(groups) == 1")
