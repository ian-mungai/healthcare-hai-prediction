from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

import pytest

from scripts.acquisition import verify_dataset_storage as verification
from scripts.acquisition.tests.test_s3_store import SETTINGS
from tests.support import check


def inventory_fixture() -> tuple:
    key = "example/collection/datasets/example_dataset/release_date=2020-01-01/hash/example.csv"
    object_record = {"key": key, "version_id": "data_version", "byte_count": 25}
    manifest_record = {"key": "example/collection/manifests/2020/hash/manifest.json", "version_id": "manifest_version", "byte_count": 100}
    reconciliation = {
        "storage_contract_version": "4.0.0",
        "publisher": "example",
        "collection": "collection",
        "objects": [{"dataset_id": "example_dataset", "object": object_record}],
        "manifests": [{"dataset_id": "example_dataset", "object": manifest_record}],
    }
    baseline = {
        "objects": [{"Key": "raw/old.zip", "Size": 40}],
        "versions": [{"Key": "raw/old.zip", "Size": 40, "VersionId": "legacy_version", "IsLatest": True}],
        "delete_markers": [],
    }
    records = [object_record, manifest_record]
    listing = {"Contents": baseline["objects"] + [{"Key": item["key"], "Size": item["byte_count"]} for item in records]}
    versions = {
        "Versions": baseline["versions"]
        + [{"Key": item["key"], "Size": item["byte_count"], "VersionId": item["version_id"], "IsLatest": True} for item in records]
    }
    return tuple(copy.deepcopy(value) for value in (reconciliation, baseline, listing, versions))


def test_complete_additive_inventory_preserves_original_archive() -> None:
    report = verification.reconcile(*inventory_fixture())
    check(
        report["dataset_count"] == 1 and report["dataset_objects"] == 2 and report["total_current_objects"] == 3,
        'report["dataset_count"] == 1 and report["dataset_objects"] == 2 and report["total_current_objects"] == 3',
    )
    check(
        report["legacy_objects_preserved"] == 1 and report["new_zip_objects"] == 0 and not report["model_eligible"],
        'report["legacy_objects_preserved"] == 1 and report["new_zip_objects"] == 0 and not report["model_eligible"]',
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "contract",
        "truncated",
        "missing",
        "changed_old_size",
        "old_not_latest",
        "changed_new_size",
        "missing_version",
        "delete_marker",
        "wrong_folder",
        "conflicting_alias",
    ],
)
def test_incomplete_or_changed_inventory_is_not_success(mutation: str) -> None:
    reconciliation, baseline, listing, versions = inventory_fixture()
    if mutation == "contract":
        reconciliation["storage_contract_version"] = "1.0.0"
    elif mutation == "truncated":
        listing["IsTruncated"] = True
    elif mutation == "missing":
        listing["Contents"].pop()
    elif mutation == "changed_old_size":
        listing["Contents"][0]["Size"] += 1
    elif mutation == "old_not_latest":
        versions["Versions"][0]["IsLatest"] = False
    elif mutation == "changed_new_size":
        listing["Contents"][1]["Size"] += 1
    elif mutation == "missing_version":
        versions["Versions"].pop()
    elif mutation == "delete_marker":
        versions["DeleteMarkers"] = [{"Key": "raw/old.zip", "VersionId": "deleted"}]
    elif mutation == "wrong_folder":
        reconciliation["manifests"][0]["dataset_id"] = "different_dataset"
    else:
        alias = copy.deepcopy(reconciliation["objects"][0])
        alias["object"]["byte_count"] += 1
        reconciliation["objects"].append(alias)
    with pytest.raises(verification.CaptureError):
        verification.reconcile(reconciliation, baseline, listing, versions)


def test_read_only_cli_saves_verified_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    reconciliation, baseline, listing, versions = inventory_fixture()
    (tmp_path / "reconciliation.json").write_text(json.dumps(reconciliation))
    (tmp_path / "baseline.json").write_text(json.dumps(baseline))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "verify_dataset_storage",
            "--reconciliation",
            str(tmp_path / "reconciliation.json"),
            "--baseline",
            str(tmp_path / "baseline.json"),
            "--output",
            str(tmp_path / "verified.json"),
        ],
    )
    monkeypatch.setattr(verification, "load_configuration", lambda _: (SETTINGS, {}))
    monkeypatch.setattr(verification, "verify_project_identity", lambda _: None)
    calls: list[str] = []

    class Client:
        def __init__(self, settings: dict) -> None:
            check(settings == SETTINGS, "settings == SETTINGS")

        def call(self, service: str, operation: str) -> dict:
            calls.append(operation)
            return listing if operation == "list-objects-v2" else versions

    monkeypatch.setattr(verification, "AwsCli", Client)
    verification.main()
    check(json.loads(capsys.readouterr().out)["dataset_count"] == 1, 'json.loads(capsys.readouterr().out)["dataset_count"] == 1')
    check(
        (tmp_path / "verified.json").exists() and calls == ["list-objects-v2", "list-object-versions"],
        '(tmp_path / "verified.json").exists() and calls == ["list-objects-v2", "list-object-versions"]',
    )
