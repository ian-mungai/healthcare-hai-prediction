from __future__ import annotations

import copy
from pathlib import Path

import pytest

from scripts.acquisition import retire_layout as cleanup
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import read_json
from scripts.acquisition.tests.test_s3_store import FakeS3
from tests.support import check


def inventory() -> dict:
    return {
        "objects": [{"Key": "raw/example/file.csv", "Size": 12}],
        "versions": [{"Key": "raw/example/file.csv", "Size": 12, "VersionId": "original", "IsLatest": True}],
        "delete_markers": [],
    }


class CleanupClient(FakeS3):
    def __init__(self) -> None:
        super().__init__()
        self.state = inventory()
        self.deleted = False

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
        self.calls.append((operation, arguments))
        if operation == "get-bucket-versioning":
            return {"Status": "Enabled"}
        if operation == "list-objects-v2":
            return {"Contents": self.state["objects"]}
        if operation == "list-object-versions":
            return {"Versions": self.state["versions"], "DeleteMarkers": self.state["delete_markers"]}
        check(operation == "delete-objects" and arguments is not None, 'operation == "delete-objects" and arguments is not None')
        if arguments is None:
            raise ValueError("Missing deletion arguments")
        request = read_json(Path(arguments[1].removeprefix("file://")))
        check(
            request == {"Objects": [{"Key": "raw/example/file.csv"}], "Quiet": False},
            'request == {"Objects": [{"Key": "raw/example/file.csv"}], "Quiet": False}',
        )
        self.deleted = True
        self.state["objects"] = []
        self.state["versions"][0]["IsLatest"] = False
        self.state["delete_markers"] = [{"Key": "raw/example/file.csv", "VersionId": "marker", "IsLatest": True}]
        return {"Deleted": [{"Key": "raw/example/file.csv", "DeleteMarker": True, "DeleteMarkerVersionId": "marker"}]}


@pytest.mark.parametrize("execute", [False, True])
def test_cleanup_is_explicit_and_retains_versions(tmp_path: Path, execute: bool) -> None:
    baseline = tmp_path / "before.json"
    baseline.write_bytes(encoded_json(inventory()))
    client = CleanupClient()
    report = cleanup.retire(client, baseline, tmp_path / "evidence", execute)
    check(client.deleted == execute, "client.deleted == execute")
    if execute:
        check(
            report["historical_versions_preserved"] == 1 and report["current_objects"] == 0,
            'report["historical_versions_preserved"] == 1 and report["current_objects"] == 0',
        )
        check(
            read_json(tmp_path / "evidence/after_cleanup.json")["versions"][0]["VersionId"] == "original",
            'read_json(tmp_path / "evidence/after_cleanup.json")["versions"][0]["VersionId"] == "original"',
        )
    else:
        check(report["s3_deletes"] == 0, 'report["s3_deletes"] == 0')


@pytest.mark.parametrize("mutation", ["empty", "duplicate", "new_layout", "truncated", "changed_size", "changed_version", "delete_marker"])
def test_unsafe_cleanup_is_rejected(mutation: str) -> None:
    baseline = inventory()
    listing = {"Contents": copy.deepcopy(baseline["objects"])}
    versions = {"Versions": copy.deepcopy(baseline["versions"]), "DeleteMarkers": []}
    if mutation == "empty":
        baseline["objects"] = []
    elif mutation == "duplicate":
        baseline["objects"] *= 2
    elif mutation == "new_layout":
        baseline["objects"][0]["Key"] = "cms/hospitals/datasets/hai/file.csv"
    elif mutation == "truncated":
        listing["IsTruncated"] = True
    elif mutation == "changed_size":
        listing["Contents"][0]["Size"] += 1
    elif mutation == "changed_version":
        versions["Versions"][0]["VersionId"] = "changed"
    else:
        versions["DeleteMarkers"] = [{"Key": "raw/example/file.csv"}]
    with pytest.raises(cleanup.CaptureError):
        cleanup.cleanup_keys(baseline, listing, versions)
