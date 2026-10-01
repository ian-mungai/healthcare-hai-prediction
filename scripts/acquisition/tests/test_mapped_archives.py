"""Synthetic capture-to-storage lifecycle; AWS is isolated by an in-memory client."""

import copy
import hashlib
import io
import json
import sys
import zipfile
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import s3_store
from scripts.acquisition.capture import capture
from scripts.acquisition.source_registry import canonical_hash, read_json
from scripts.acquisition.tests.test_capture import Opener, Response
from scripts.acquisition.tests.test_capture import setup_capture as setup_capture
from scripts.acquisition.tests.test_s3_store import OUTPUTS, FakeS3
from tests.support import check


# Failure modes: wrong source/release/hash, missing or extra leaves, unsafe nesting,
# incorrect destinations, uploaded containers and non-idempotent replay.
@pytest.fixture
def mapped_bundle(setup_capture: Any, tmp_path: Path) -> Any:
    source, plan, validator = setup_capture
    source["file_routes"][0]["url"] = "https://example.org/archive.zip"
    plan.update(expected_format="zip", file_name="archive.zip")
    nested = io.BytesIO()
    with zipfile.ZipFile(nested, "w") as archive:
        archive.writestr("second.csv", b"id,value\nexample_b,2\n")
    content = io.BytesIO()
    files = {
        ("first.csv",): b"id,value\nexample_a,1\n",
        ("inner.zip", "second.csv"): b"id,value\nexample_b,2\n",
        ("dictionary.txt",): b"Example generic data dictionary\n",
    }
    with zipfile.ZipFile(content, "w") as archive:
        archive.writestr("first.csv", files[("first.csv",)])
        archive.writestr("inner.zip", nested.getvalue())
        archive.writestr("dictionary.txt", files[("dictionary.txt",)])
    registry = {"sources": [source]}
    path = capture(plan, tmp_path, registry, validator, opener=Opener(Response(content.getvalue())))
    receipt = read_json(path)
    mapping = {
        "registry_sha256": canonical_hash(registry),
        "sha256": hashlib.sha256(content.getvalue()).hexdigest(),
        "bytes": len(content.getvalue()),
        "source_ids": [source["source_id"]],
        "route_ids": [plan["route_id"]],
        "url": source["file_routes"][0]["url"],
        "release_date": receipt["release"]["release_date"],
        "collection_root": "example_publisher/example_collection",
        "members": [],
    }
    for index, (chain, body) in enumerate(files.items()):
        mapping["members"].append(
            {
                "member_chain": list(chain),
                "sha256": hashlib.sha256(body).hexdigest(),
                "bytes": len(body),
                "role": "data" if index < 2 else "reference",
                "dataset_id": f"example_table_{index}" if index < 2 else None,
            }
        )
    return path, registry, validator, mapping, FakeS3()


def test_mapped_capture_storage_and_replay(mapped_bundle: Any) -> None:
    path, registry, validator, mapping, client = mapped_bundle
    first = s3_store.upload_snapshot(path, [], client, OUTPUTS, registry, validator, archive_mapping=mapping)
    before = copy.deepcopy(client.objects)
    second = s3_store.upload_snapshot(path, [], client, OUTPUTS, registry, validator, archive_mapping=mapping)
    check(first["dataset_count"] == 2 and second["objects_created"] == 0, 'first["dataset_count"] == 2 and second["objects_created"] == 0')
    check(
        client.objects == before and not any(key.endswith(".zip") for key in client.objects),
        'client.objects == before and not any(key.endswith(".zip") for key in client.objects)',
    )
    dictionaries = [key for key in client.objects if "/references/" in key]
    check(len(dictionaries) == 1, "len(dictionaries) == 1")
    saved = read_json(path.parent / "s3_collections_reconciliation.json")
    check(
        {x["dataset_id"] for x in saved["manifests"]} == {"example_table_0", "example_table_1"},
        '{x["dataset_id"] for x in saved["manifests"]} == {"example_table_0", "example_table_1"}',
    )
    for member in mapping["members"]:
        matches = [entry for entry in saved["objects"] if entry.get("member_chain") == member["member_chain"]]
        check(
            matches and all(entry["object"]["sha256"] == member["sha256"] for entry in matches),
            'matches and all(entry["object"]["sha256"] == member["sha256"] for entry in matches)',
        )
    sys.stdout.write(
        json.dumps(
            {
                "synthetic_lifecycle": "passed",
                "manifest_sha256": hashlib.sha256((path.parent / "s3_collections_reconciliation.json").read_bytes()).hexdigest(),
                "live_aws": False,
            }
        )
        + "\n"
    )


# A documentation archive must not promote a generic text or spreadsheet member to observed data.
def test_documentation_archive_members_remain_references(mapped_bundle: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts.acquisition import history_routes

    path, registry, _, _, _ = mapped_bundle
    receipt = read_json(path)
    receipt["artifacts"][0]["role"] = "methodology"
    monkeypatch.setattr(history_routes, "read_json", lambda _: receipt)
    monkeypatch.setattr(
        history_routes, "load_routes", lambda _: {receipt["source"]["source_record_id"]: {"publisher": "example_publisher", "collection": "example_collection"}}
    )
    mapping = history_routes.map_captured_archive(path, registry)
    check(
        all(member["role"] == "reference" and member["dataset_id"] is None for member in mapping["members"]),
        'all(member["role"] == "reference" and member["dataset_id"] is None for member in mapping["members"])',
    )


# Annual bundles repeat filenames across releases; each must use its own manifest,
# retain its source chain and store both byte versions without uploading containers.
def test_nested_publisher_releases_keep_distinct_manifest_bindings(setup_capture: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts.acquisition import history_routes

    source, plan, validator = setup_capture
    source["file_routes"][0]["url"] = "https://example.org/annual.zip"
    plan.update(expected_format="zip", file_name="annual.zip")
    outer = io.BytesIO()
    with zipfile.ZipFile(outer, "w") as annual:
        annual.writestr("manifest.json", json.dumps([{"type": "theme", "dataset_id": None}]))
        for index in range(2):
            nested = io.BytesIO()
            content = f"id,value\nexample,{index}\n".encode()
            with zipfile.ZipFile(nested, "w") as release:
                release.writestr("abcd-1234_table.csv", content)
                release.writestr(
                    "manifest.json",
                    json.dumps(
                        [
                            {
                                "dataset_id": "abcd-1234",
                                "private": False,
                                "name": "Example table",
                                "resources": [{"filename": "abcd-1234_table.csv", "filesize": len(content)}],
                            }
                        ]
                    ),
                )
            annual.writestr(f"release_{index}.zip", nested.getvalue())
    registry = {"sources": [source]}
    path = capture(plan, tmp_path, registry, validator, opener=Opener(Response(outer.getvalue())))
    monkeypatch.setattr(history_routes, "load_routes", lambda _: {source["source_id"]: {"publisher": "cms", "collection": "hospitals"}})
    mapping = history_routes.map_captured_archive(path, registry)
    data = [member for member in mapping["members"] if member["role"] == "data"]
    check(
        len(data) == 2 and {member["dataset_id"] for member in data} == {"cms_abcd_1234"},
        'len(data) == 2 and {member["dataset_id"] for member in data} == {"cms_abcd_1234"}',
    )
    check(len({member["sha256"] for member in data}) == 2, 'len({member["sha256"] for member in data}) == 2')
    check(
        {tuple(member["member_chain"]) for member in data} == {("release_0.zip", "abcd-1234_table.csv"), ("release_1.zip", "abcd-1234_table.csv")},
        '{tuple(member["member_chain"]) for member in data} == {("release_0.zip", "abcd-1234_table.csv"), ("release_1.zip", "abcd-1234_table.csv")}',
    )
    mapping["collection_root"] = "example_publisher/example_collection"
    client = FakeS3()
    s3_store.upload_snapshot(path, [], client, OUTPUTS, registry, validator, archive_mapping=mapping)
    saved = read_json(path.parent / "s3_collections_reconciliation.json")
    objects = [item for item in saved["objects"] if item["role"] == "data"]
    check(
        len(objects) == 2 and len({item["object"]["key"] for item in objects}) == 2,
        'len(objects) == 2 and len({item["object"]["key"] for item in objects}) == 2',
    )
    check(not any(key.endswith(".zip") for key in client.objects), 'not any(key.endswith(".zip") for key in client.objects)')
    sys.stdout.write(
        json.dumps(
            {
                "nested_release_lifecycle": "passed",
                "reconciliation_sha256": hashlib.sha256((path.parent / "s3_collections_reconciliation.json").read_bytes()).hexdigest(),
                "live_aws": False,
            }
        )
        + "\n"
    )


@pytest.mark.parametrize("change", ["hash", "source", "release", "collection", "route", "registry", "missing", "extra", "duplicate", "member_hash", "folder"])
def test_mapping_rejection_has_no_s3_writes(mapped_bundle: Any, change: str) -> None:
    path, registry, validator, mapping, client = mapped_bundle
    if change in {"hash", "source", "release", "collection", "route", "registry"}:
        key, value = {
            "hash": ("sha256", "0" * 64),
            "source": ("source_ids", ["example_other"]),
            "release": ("release_date", "1999-01-01"),
            "collection": ("collection_root", "example/other"),
            "route": ("route_ids", ["example_other"]),
            "registry": ("registry_sha256", "0" * 64),
        }[change]
        mapping[key] = value
    elif change == "missing":
        mapping["members"].pop()
    elif change == "extra":
        mapping["members"].append({**mapping["members"][0], "member_chain": ["absent.csv"]})
    elif change == "duplicate":
        mapping["members"].append(mapping["members"][0])
    elif change == "member_hash":
        mapping["members"][0]["sha256"] = "0" * 64
    else:
        mapping["members"][0]["dataset_id"] = "../outside"
    with pytest.raises(ValueError):
        s3_store.upload_snapshot(path, [], client, OUTPUTS, registry, validator, archive_mapping=mapping)
    check(not client.objects, "not client.objects")


@pytest.mark.parametrize("member", ["../outside.csv", "/absolute.csv", "C:/outside.csv"])
def test_recursive_archive_rejects_unsafe_members(tmp_path: Path, member: str) -> None:
    from scripts.acquisition.mapped_archives import expand_tree

    path = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(member, b"example")
    with pytest.raises(ValueError):
        expand_tree(path, tmp_path / "expanded", hashlib.sha256(path.read_bytes()).hexdigest())


@pytest.mark.parametrize("extension", ["xlsx", "xlsm"])
def test_recursive_archive_preserves_office_and_enforces_global_budget(tmp_path: Path, extension: str) -> None:
    from scripts.acquisition.mapped_archives import expand_tree

    office = io.BytesIO()
    with zipfile.ZipFile(office, "w") as archive:
        archive.writestr("[Content_Types].xml", b"<Types/>")
        archive.writestr("xl/workbook.xml", b"<workbook/>")
    path = tmp_path / "example.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(f"example.{extension}", office.getvalue())
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    leaves = expand_tree(path, tmp_path / "expanded", digest)
    check(list(leaves) == [(f"example.{extension}",)], 'list(leaves) == [(f"example.{extension}",)]')
    check(
        leaves[(f"example.{extension}",)]["sha256"] == hashlib.sha256(office.getvalue()).hexdigest(),
        'leaves[(f"example.{extension}",)]["sha256"] == hashlib.sha256(office.getvalue()).hexdigest()',
    )
    with pytest.raises(ValueError):
        expand_tree(path, tmp_path / "too_small", digest, max_bytes=10)
