from __future__ import annotations

import copy
import sys
import zipfile
from pathlib import Path

import pytest

from scripts.acquisition import plan_archive_members as review
from scripts.acquisition import plan_remaining as planning
from scripts.acquisition.historical_inventory import build_inventory
from scripts.acquisition.s3_store import encoded_json, fingerprint
from scripts.acquisition.tests.test_capture import setup_capture as setup_capture
from scripts.acquisition.tests.test_plan_remaining import inputs
from tests.support import check


def evidence(tmp_path: Path, names: list[str] | None = None) -> tuple[dict, dict, Path]:
    path = tmp_path / "example.zip"
    with zipfile.ZipFile(path, "w") as archive:
        for name in names or ["table.csv", "dictionary.pdf", "__MACOSX/metadata", "folder/"]:
            archive.writestr(name, b"id,value\n0012A,1\n")
    digest, size = fingerprint(path)
    record = {
        "local_path": str(path),
        "requested_url": "https://example.org/example.zip",
        "sha256": digest,
        "bytes": size,
        "http_status": 200,
        "hash_scope": "complete_file",
    }
    source = {"file_routes": [{"route_id": "example_route", "url": record["requested_url"]}]}
    metadata = tmp_path / "evidence.json"
    metadata.write_bytes(encoded_json({"nested": [record, copy.deepcopy(record), {"unrelated": True}]}))
    return record, {"sources": [source]}, metadata


def test_existing_zip_review_is_bounded_read_only_and_deduplicated(tmp_path: Path) -> None:
    record, registry, metadata = evidence(tmp_path)
    before = fingerprint(Path(record["local_path"]))
    report = review.review_evidence(registry, [metadata])
    check(
        report["counts"] == {"archives_verified": 1, "members_verified": 2, "route_bindings": 1, "rejected_evidence": 0},
        'report["counts"] == {"archives_verified": 1, "members_verified": 2, "route_bindings": 1, "rejected_evidence": 0}',
    )
    check(
        report["archives"][0]["members"][1]["reference_kind"] == "dictionary_candidate",
        'report["archives"][0]["members"][1]["reference_kind"] == "dictionary_candidate"',
    )
    check(report["archives"][0]["skipped_packaging"] == ["__MACOSX/metadata"], 'report["archives"][0]["skipped_packaging"] == ["__MACOSX/metadata"]')
    check(
        not report["originals_changed"] and report["downloads"] == report["s3_calls"] == 0,
        'not report["originals_changed"] and report["downloads"] == report["s3_calls"] == 0',
    )
    check(fingerprint(Path(record["local_path"])) == before, 'fingerprint(Path(record["local_path"])) == before')
    check(
        set(path.name for path in tmp_path.iterdir()) == {"example.zip", "evidence.json"},
        'set(path.name for path in tmp_path.iterdir()) == {"example.zip", "evidence.json"}',
    )


@pytest.mark.parametrize("change", ["http_error", "hash", "prefix", "bad_checksum", "unregistered", "missing_file"])
def test_bad_evidence_cannot_create_a_verified_archive_plan(tmp_path: Path, change: str) -> None:
    record, registry, metadata = evidence(tmp_path)
    if change == "http_error":
        record["http_status"] = 403
    elif change == "hash":
        record["sha256"] = "0" * 64
    elif change == "prefix":
        record["hash_scope"] = "bounded_prefix"
    elif change == "bad_checksum":
        record["sha256"] = "invalid"
    elif change == "unregistered":
        record["requested_url"] = "https://example.org/unregistered.zip"
    else:
        record["local_path"] = str(tmp_path / "missing.zip")
    metadata.write_bytes(encoded_json(record))
    report = review.review_evidence(registry, [metadata])
    check(not report["archives"], 'not report["archives"]')
    check(
        len(report["rejected_evidence"]) == (0 if change == "unregistered" else 1), 'len(report["rejected_evidence"]) == (0 if change == "unregistered" else 1)'
    )


def test_unsafe_members_remain_unusable(tmp_path: Path) -> None:
    record, _, _ = evidence(tmp_path, ["../escape.csv"])
    with pytest.raises(ValueError, match="unsafe"):
        review.inspect_existing(record)


def test_archive_review_cli_is_offline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, registry, metadata = evidence(tmp_path)
    monkeypatch.setattr(review, "load_registry", lambda: registry)
    output = tmp_path / "review.json"
    monkeypatch.setattr(sys, "argv", ["review", "--evidence", str(metadata), "--output", str(output)])
    review.main()
    review.main()
    monkeypatch.setattr(sys, "argv", ["review", "--evidence", str(tmp_path / "absent.json"), "--output", str(output)])
    with pytest.raises(SystemExit):
        review.main()


def test_local_member_review_pins_bytes_without_clearing_mapping_hold(setup_capture: tuple, tmp_path: Path) -> None:
    registry, _, rules, routes, validator = inputs(setup_capture)
    record, _, metadata = evidence(tmp_path)
    source = registry["sources"][0]
    source["file_routes"][0].update(url=record["requested_url"], format="ZIP")
    inventory = build_inventory(registry)
    report = review.review_evidence(registry, [metadata])
    _, register, batch = planning.build_plans(registry, inventory, rules, routes, [], validator, report)
    route = register["routes"][0]
    check(route["capture_plan"]["expected_sha256"] == record["sha256"], 'route["capture_plan"]["expected_sha256"] == record["sha256"]')
    check(route["extraction"]["local_members_verified"] and not batch["jobs"], 'route["extraction"]["local_members_verified"] and not batch["jobs"]')
    report["registry_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="different source registry"):
        planning.build_plans(registry, inventory, rules, routes, [], validator, report)
