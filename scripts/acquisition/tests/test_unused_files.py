from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from scripts.acquisition import unused_files
from tests.support import check

HEADER = ["source_id", "snapshot_id", "s3_key", "sha256", "loaded", "retired", "tables"]
LOADED, TWIN, DICTIONARY, NO_READER, NOT_LOADED, RETIRED = "a" * 64, "b" * 64, "c" * 64, "d" * 64, "e" * 64, "f" * 64


def row(snapshot: str, name: str, digest: str, loaded: str = "false", retired: str = "false", tables: str = "0") -> list[str]:
    return ["S1", snapshot, f"s1/capture_id={snapshot}/{digest}/{name}", digest, loaded, retired, tables]


ROWS = [
    row("S1__1", "used.csv", LOADED, "true", tables="1"),
    row("S1__2", "twin.csv", LOADED, tables="1"),
    row("S1__3", "dictionary.pdf", DICTIONARY),
    row("S1__4", "never_read.csv", NO_READER),
    row("S1__5", "superseded.csv", NOT_LOADED, tables="2"),
    row("S1__6", "gone.csv", RETIRED, retired="true"),
]


def write_inputs(folder: Path) -> tuple[Path, Path]:
    copies = folder / "copies.csv"
    with copies.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER)
        writer.writerows(ROWS)
    keep = folder / "keep.json"
    keep.write_text(json.dumps({"keep": [{"snapshot_id": "S1__3", "reason": "stored_dictionary"}]}))
    return copies, keep


def run(folder: Path, *extra: str) -> None:
    copies, keep = write_inputs(folder)
    unused_files.main(["plan", "--copies", str(copies), "--keep", str(keep), "--output", str(folder / "out"), *extra])


class Client:
    """S3 stand-in: delete-objects without a version id adds a delete marker and removes nothing."""

    def __init__(self, keys: list[str]) -> None:
        self.markers: dict[str, str] = {}
        self.versions = {key: f"v-{index}" for index, key in enumerate(keys)}
        self.deletes: list[list[str]] = []

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
        if operation == "get-bucket-versioning":
            return {"Status": "Enabled"}
        if operation == "list-object-versions":
            return {
                "Versions": [{"Key": key, "VersionId": version, "IsLatest": key not in self.markers} for key, version in self.versions.items()],
                "DeleteMarkers": [{"Key": key, "VersionId": marker, "IsLatest": True} for key, marker in self.markers.items()],
            }
        check(operation == "delete-objects" and arguments is not None, 'operation == "delete-objects" and arguments is not None')
        if arguments is None:
            raise ValueError("Missing deletion arguments")
        request = json.loads(Path(arguments[1].removeprefix("file://")).read_text())
        check(all(set(item) == {"Key"} for item in request["Objects"]), 'all(set(item) == {"Key"} for item in request["Objects"])')
        self.deletes.append([item["Key"] for item in request["Objects"]])
        for item in request["Objects"]:
            self.markers[item["Key"]] = f"m-{len(self.markers)}"
        return {"Deleted": [{"Key": item["Key"], "DeleteMarker": True, "DeleteMarkerVersionId": self.markers[item["Key"]]} for item in request["Objects"]]}


def test_report_names_each_unused_file_with_a_reason(tmp_path: Path) -> None:
    run(tmp_path)
    report = json.loads((tmp_path / "out" / "unused_report.json").read_text())
    unused = {item["snapshot_id"]: item["reason"] for item in report["unused"]}
    check(
        unused == {"S1__4": "no_table_reads_it", "S1__5": "not_loaded_by_any_table"},
        'unused == {"S1__4": "no_table_reads_it", "S1__5": "not_loaded_by_any_table"}',
    )
    kept = {item["snapshot_id"]: item["reason"] for item in report["kept"]}
    check(
        kept == {"S1__1": "loaded_by_bronze", "S1__2": "same_bytes_as_loaded_file", "S1__3": "stored_dictionary"},
        'kept == {"S1__1": "loaded_by_bronze", "S1__2": "same_bytes_as_loaded_file", "S1__3": "stored_dictionary"}',
    )
    check(report["already_retired"] == ["S1__6"], 'report["already_retired"] == ["S1__6"]')


def test_plan_is_dry_and_removal_is_recoverable(tmp_path: Path) -> None:
    run(tmp_path)
    plan = json.loads((tmp_path / "out" / "removal_plan.json").read_text())
    check(plan["permanent_version_deletion"] is False, 'plan["permanent_version_deletion"] is False')
    check(plan["executed"] is False, 'plan["executed"] is False')
    check([item["snapshot_id"] for item in plan["items"]] == ["S1__4", "S1__5"], '[item["snapshot_id"] for item in plan["items"]] == ["S1__4", "S1__5"]')
    check(len(plan["plan_sha256"]) == 64, 'len(plan["plan_sha256"]) == 64')


def test_rerun_with_same_inputs_changes_nothing_and_changed_inputs_are_refused(tmp_path: Path) -> None:
    run(tmp_path)
    before = (tmp_path / "out" / "removal_plan.json").read_bytes()
    run(tmp_path)
    check((tmp_path / "out" / "removal_plan.json").read_bytes() == before, '(tmp_path / "out" / "removal_plan.json").read_bytes() == before')
    ROWS.append(row("S1__7", "late.csv", "9" * 64))
    try:
        with pytest.raises(SystemExit):
            run(tmp_path)
    finally:
        ROWS.pop()


def test_missing_column_is_refused(tmp_path: Path) -> None:
    copies = tmp_path / "copies.csv"
    copies.write_text("snapshot_id,s3_key\nS1__1,k\n")
    with pytest.raises(SystemExit):
        unused_files.main(["plan", "--copies", str(copies), "--output", str(tmp_path / "out")])


def test_apply_needs_the_owner_approval_of_this_exact_plan(tmp_path: Path) -> None:
    run(tmp_path)
    plan = json.loads((tmp_path / "out" / "removal_plan.json").read_text())
    client = Client([item["s3_key"] for item in plan["items"]])
    out = tmp_path / "out"
    with pytest.raises(SystemExit):
        unused_files.main(
            ["apply", "--plan", str(out / "removal_plan.json"), "--approval", str(tmp_path / "missing.json"), "--output", str(out)], client=client
        )
    wrong = tmp_path / "approval.json"
    wrong.write_text(json.dumps({"plan_sha256": "0" * 64, "approved_by": "owner"}))
    with pytest.raises(SystemExit):
        unused_files.main(["apply", "--plan", str(out / "removal_plan.json"), "--approval", str(wrong), "--output", str(out)], client=client)
    check(client.deletes == [], "client.deletes == []")


def test_apply_writes_delete_markers_once_and_a_retry_has_no_second_effect(tmp_path: Path) -> None:
    run(tmp_path)
    out = tmp_path / "out"
    plan = json.loads((out / "removal_plan.json").read_text())
    approval = tmp_path / "approval.json"
    approval.write_text(json.dumps({"plan_sha256": plan["plan_sha256"], "approved_by": "owner"}))
    client = Client([item["s3_key"] for item in plan["items"]] + ["s1/kept/used.csv"])
    arguments = ["apply", "--plan", str(out / "removal_plan.json"), "--approval", str(approval), "--output", str(out)]
    unused_files.main(arguments, client=client)
    check(client.deletes == [sorted(item["s3_key"] for item in plan["items"])], 'client.deletes == [sorted(item["s3_key"] for item in plan["items"])]')
    record = json.loads((out / "removal_record.json").read_text())
    check(
        [item["delete_marker_version_id"] for item in record["items"]] == ["m-0", "m-1"],
        '[item["delete_marker_version_id"] for item in record["items"]] == ["m-0", "m-1"]',
    )
    check("s1/kept/used.csv" not in client.markers, '"s1/kept/used.csv" not in client.markers')
    unused_files.main(arguments, client=client)
    check(len(client.deletes) == 1, "len(client.deletes) == 1")


def test_apply_refuses_a_key_with_a_changed_version_list(tmp_path: Path) -> None:
    run(tmp_path)
    out = tmp_path / "out"
    plan = json.loads((out / "removal_plan.json").read_text())
    approval = tmp_path / "approval.json"
    approval.write_text(json.dumps({"plan_sha256": plan["plan_sha256"], "approved_by": "owner"}))
    client = Client([item["s3_key"] for item in plan["items"][:1]])
    with pytest.raises(SystemExit):
        unused_files.main(["apply", "--plan", str(out / "removal_plan.json"), "--approval", str(approval), "--output", str(out)], client=client)
    check(client.deletes == [], "client.deletes == []")
