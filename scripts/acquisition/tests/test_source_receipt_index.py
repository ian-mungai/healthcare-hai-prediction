"""E2E check of the receipt index: a fake local receipt tree builds the committed layout and each planted defect fails the check."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import source_receipt_index as index_module
from tests.support import check

VERSION_A = "Aa1.Bb2_Cc3-Dd4Ee5Ff6Gg7Hh8Ii9J"
VERSION_B = "Zz9.Yy8_Xx7-Ww6Vv5Uu4Tt3Ss2Rr1Q"
NOT_RECORDED = "not recorded"


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def make_capture(
    root: Path,
    source_id: str,
    stamp: str,
    retrieved: str,
    *,
    version: str | None = VERSION_A,
    verified: str | None = None,
    manifests: int = 1,
    folder: str = "acquisition_batches",
) -> None:
    capture = root / folder / source_id / f"{source_id}__{stamp}__0123456789abcdef0123456789abcdef"
    receipt = {
        "snapshot_id": capture.name,
        "source": {"source_record_id": source_id, "title": "A title"},
        "acquisition": {"retrieved_at_utc": retrieved},
        "verification": {"status": "pending", "verified_at_utc": verified},
    }
    write_json(capture / "receipt.json", receipt)
    if version is not None:
        write_json(capture / "s3_collections_reconciliation.json", {"manifests": [{"object": {"version_id": version}} for _ in range(manifests)]})


@pytest.fixture()
def tree(tmp_path: Path) -> Path:
    make_capture(tmp_path, "ACS", "20260924T010000Z", "2026-09-24T01:00:00.000000Z")
    make_capture(tmp_path, "ACS", "20260925T010000Z", "2026-09-25T01:00:00+00:00", version=VERSION_B, folder="historical_acquisition")
    make_capture(tmp_path, "CMS_OWNERS", "20260926T010000Z", "2026-09-26T01:00:00+00:00", version=None)
    make_capture(tmp_path, "SVI", "20260927T010000Z", "2026-09-27T01:00:00+00:00", verified="2026-09-28T09:00:00+00:00", manifests=2)
    write_json(tmp_path / "acquisition_batches" / "ACS" / "failed_attempt" / "receipt.json", {"attempt": 1, "complete": False, "failure": "timeout"})
    return tmp_path


@pytest.fixture()
def log() -> dict[str, Any]:
    rows = [("ACS", "Public"), ("CMS_OWNERS", "Confidential"), ("SVI", "Public"), ("care-hrrp-cms", "Public")]
    return {"rows": [{"dataset_id": dataset_id, "data_class": data_class} for dataset_id, data_class in rows]}


@pytest.fixture()
def built(tree: Path, log: dict[str, Any]) -> dict[str, Any]:
    return index_module.build_index(log, "f" * 64, index_module.read_receipts(tree))


def rows_by_id(index: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {row["dataset_id"]: row for row in index["rows"]}


def test_build_reads_the_newest_receipt_and_its_object_version(built: dict[str, Any]) -> None:
    acs = rows_by_id(built)["ACS"]
    check(acs["receipt_count"] == 2, "ACS has two receipts, the failed attempt is not one")
    check(acs["newest_retrieved_date"] == "2026-09-25", "the newest ACS capture was retrieved on Sep 25")
    check(acs["newest_object_version"] == VERSION_B, "the newest ACS capture holds the second version")
    check(len(acs["newest_receipt_sha256"]) == 64 and len(acs["receipt_set_sha256"]) == 64, "both receipt hashes are SHA-256 values")
    check(acs["newest_receipt_sha256"] != acs["receipt_set_sha256"], "the set hash covers more than the newest receipt")


def test_a_fact_no_local_record_holds_reads_not_recorded(built: dict[str, Any]) -> None:
    rows = rows_by_id(built)
    check(rows["CMS_OWNERS"]["newest_object_version"] == NOT_RECORDED, "no reconciliation file means no object version")
    check(rows["ACS"]["verification_date"] == NOT_RECORDED, "a receipt that was never verified gives no verification date")
    check(rows["SVI"]["verification_date"] == "2026-09-28", "a receipt with a verification time gives its date")
    check(rows["SVI"]["newest_object_version"] == NOT_RECORDED and rows["SVI"]["newest_manifest_count"] == 2, "two manifests give no single version")
    none = rows["care-hrrp-cms"]
    check(none["receipt_count"] == 0 and none["receipt_set_sha256"] == NOT_RECORDED, "a dataset without receipts is listed with a zero count")


def test_build_covers_exactly_the_datasets_of_the_log_and_is_deterministic(built: dict[str, Any], tree: Path, log: dict[str, Any]) -> None:
    check([row["dataset_id"] for row in built["rows"]] == sorted(row["dataset_id"] for row in log["rows"]), "one row per logged dataset, sorted")
    again = index_module.build_index(log, "f" * 64, index_module.read_receipts(tree))
    check(index_module.render(built) == index_module.render(again), "two builds of the same tree give the same text")


def test_a_receipt_of_a_dataset_outside_the_log_stops_the_build(tree: Path, log: dict[str, Any]) -> None:
    make_capture(tree, "UNLISTED", "20260928T010000Z", "2026-09-28T01:00:00+00:00")
    with pytest.raises(index_module.ReceiptIndexError, match="UNLISTED"):
        index_module.build_index(log, "f" * 64, index_module.read_receipts(tree))


def test_the_built_index_passes_the_check(built: dict[str, Any], log: dict[str, Any]) -> None:
    check(index_module.check_index(built, log, "f" * 64) == [], "a fresh build has no problem")


def test_a_missing_dataset_row_fails_the_check(built: dict[str, Any], log: dict[str, Any]) -> None:
    broken = copy.deepcopy(built)
    broken["rows"] = [row for row in broken["rows"] if row["dataset_id"] != "SVI"]
    check(any("missing row" in problem and "SVI" in problem for problem in index_module.check_index(broken, log, "f" * 64)), "a missing row is named")


def test_a_log_that_changed_since_the_build_fails_the_check(built: dict[str, Any], log: dict[str, Any]) -> None:
    check(any("stale" in problem for problem in index_module.check_index(built, log, "e" * 64)), "a different log hash is stale evidence")


def test_a_malformed_hash_version_or_date_fails_the_check(built: dict[str, Any], log: dict[str, Any]) -> None:
    for field, value in (("newest_receipt_sha256", "xyz"), ("newest_object_version", "bad version"), ("newest_retrieved_date", "Sep 25 2026")):
        broken = copy.deepcopy(built)
        rows_by_id(broken)["ACS"][field] = value
        check(any("ACS" in problem and field in problem for problem in index_module.check_index(broken, log, "f" * 64)), f"a malformed {field} is named")


def test_confidential_rows_hold_only_fixed_format_facts(built: dict[str, Any], log: dict[str, Any]) -> None:
    for field, value in (
        ("contact_name", "Example Person"),
        ("receipt_path", "data/datasets/acquisition_batches/x/receipt.json"),
        ("note", "ask owner@example.org"),
    ):
        broken = copy.deepcopy(built)
        rows_by_id(broken)["CMS_OWNERS"][field] = value
        problems = index_module.check_index(broken, log, "f" * 64)
        check(
            any("Confidential" in problem and "CMS_OWNERS" in problem and field in problem for problem in problems),
            f"an extra {field} on a Confidential row is named",
        )
    for field, value in (("newest_object_version", "Example Person"), ("newest_retrieved_date", "-".join(("555", "010", "0100")))):
        broken = copy.deepcopy(built)
        rows_by_id(broken)["CMS_OWNERS"][field] = value
        problems = index_module.check_index(broken, log, "f" * 64)
        check(any("Confidential" in problem and "CMS_OWNERS" in problem and field in problem for problem in problems), f"free text in {field} is named")


def test_a_class_that_differs_from_the_log_fails_the_check(built: dict[str, Any], log: dict[str, Any]) -> None:
    broken = copy.deepcopy(built)
    rows_by_id(broken)["CMS_OWNERS"]["data_class"] = "Public"
    check(
        any("data_class" in problem and "CMS_OWNERS" in problem for problem in index_module.check_index(broken, log, "f" * 64)),
        "a Confidential source cannot be relabeled Public",
    )


def test_the_writer_refuses_an_index_that_fails_the_check(built: dict[str, Any], log: dict[str, Any], tmp_path: Path) -> None:
    broken = copy.deepcopy(built)
    rows_by_id(broken)["CMS_OWNERS"]["newest_object_version"] = "Example Person"
    target = tmp_path / "index.json"
    with pytest.raises(index_module.ReceiptIndexError, match="Confidential"):
        index_module.write_index(broken, log, "f" * 64, target)
    check(not target.exists(), "no file is written when the check fails")
    index_module.write_index(built, log, "f" * 64, target)
    check(target.read_text(encoding="utf-8") == index_module.render(built), "a passing index is written in the canonical text")


def run_check_on(text: str, log_text: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> tuple[int, str]:
    (tmp_path / "index.json").write_text(text, encoding="utf-8")
    (tmp_path / "log.json").write_text(log_text, encoding="utf-8")
    monkeypatch.setattr(index_module, "INDEX_PATH", tmp_path / "index.json")
    monkeypatch.setattr(index_module, "LOG_PATH", tmp_path / "log.json")
    monkeypatch.setattr(sys, "argv", ["source_receipt_index", "--check"])
    try:
        index_module.main()
    except SystemExit as stop:
        return int(stop.code or 0), capsys.readouterr().err
    return 0, capsys.readouterr().out


def test_check_command_accepts_the_canonical_file_and_rejects_other_text(
    tree: Path, log: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    log_text = json.dumps(log)
    built = index_module.build_index(log, index_module.text_sha256(log_text), index_module.read_receipts(tree))
    code, output = run_check_on(index_module.render(built), log_text, tmp_path, monkeypatch, capsys)
    check(code == 0 and "valid" in output, "the canonical file passes")
    code, output = run_check_on(json.dumps(built), log_text, tmp_path, monkeypatch, capsys)
    check(code == 1 and "canonical" in output, "compact text is not the canonical serialization")
    duplicate = index_module.render(built).replace('"data_class": "Public"', '"data_class": "Confidential",\n      "data_class": "Public"', 1)
    code, output = run_check_on(duplicate, log_text, tmp_path, monkeypatch, capsys)
    check(code == 1 and "Duplicate JSON key" in output, "a repeated key hides nothing")
