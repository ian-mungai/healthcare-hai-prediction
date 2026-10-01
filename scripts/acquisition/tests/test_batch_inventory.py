from __future__ import annotations

import copy
import hashlib
import io
import json
import sys
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import archive_review as archives
from scripts.acquisition import batch as batches
from scripts.acquisition import historical_inventory as inventories
from scripts.acquisition import reuse_capture as reuse
from scripts.acquisition import s3_store as storage
from scripts.acquisition.capture import capture
from scripts.acquisition.source_registry import canonical_hash, read_json
from scripts.acquisition.tests.test_capture import Opener, Response, bls_response, configure_api, il_response
from scripts.acquisition.tests.test_capture import setup_capture as setup_capture
from scripts.acquisition.tests.test_s3_store import OUTPUTS, FakeS3
from scripts.acquisition.tests.test_s3_store import bundle as bundle
from tests.support import check


def batch_setup(setup: tuple, jobs: int = 1) -> tuple[dict, dict, object]:
    source, plan, validator = setup
    registry = {"sources": [source]}
    job = {"job_id": "example_job", "plan": plan, "references": [], "limits": {"max_bytes": 1024, "attempts": 1, "max_pages": 3}}
    batch = {
        "batch_version": 1,
        "registry_sha256": canonical_hash(registry),
        "jobs": [{**copy.deepcopy(job), "job_id": f"example_job_{index}"} for index in range(jobs)],
    }
    return batch, registry, validator


def runner_with_response(content: bytes = b"id,value\n0012A,2\n", status: int = 200) -> Any:
    def run(plan: Any, root: Any, registry: Any, validator: Any, limits: Any) -> Any:
        return capture(plan, root, registry, validator, limits, Opener(Response(content, status)))

    return run


def test_dry_run_has_no_files_or_aws_calls(setup_capture: tuple, tmp_path: Path) -> None:
    batch, registry, validator = batch_setup(setup_capture)
    client = FakeS3()
    result = batches.run_batch(batch, tmp_path / "state", registry, validator, client)
    check(result["status"] == "validated_offline" and result["aws_calls"] == 0, 'result["status"] == "validated_offline" and result["aws_calls"] == 0')
    check(not client.calls and not (tmp_path / "state").exists(), 'not client.calls and not (tmp_path / "state").exists()')


def test_bounded_batch_resume_and_immutable_plans(setup_capture: tuple, tmp_path: Path) -> None:
    batch, registry, validator = batch_setup(setup_capture, jobs=3)
    client = FakeS3()
    options = {"execute": True, "max_jobs": 1, "capture_fn": runner_with_response(), "sleep_fn": lambda _: None}
    first = batches.run_batch(batch, tmp_path, registry, validator, client, OUTPUTS, **options)
    check(
        first["events"][0]["status"] == "stored_unvalidated" and first["unprocessed_jobs"] == 2,
        'first["events"][0]["status"] == "stored_unvalidated" and first["unprocessed_jobs"] == 2',
    )
    second = batches.run_batch(batch, tmp_path, registry, validator, client, OUTPUTS, **options)
    check(
        [item["status"] for item in second["events"]] == ["already_completed", "stored_unvalidated"],
        '[item["status"] for item in second["events"]] == ["already_completed", "stored_unvalidated"]',
    )
    third = batches.run_batch(batch, tmp_path, registry, validator, client, OUTPUTS, **options)
    check(
        third["unprocessed_jobs"] == 0 and third["events"][-1]["status"] == "stored_unvalidated",
        'third["unprocessed_jobs"] == 0 and third["events"][-1]["status"] == "stored_unvalidated"',
    )
    versions = copy.deepcopy(client.objects)
    replay = batches.run_batch(batch, tmp_path, registry, validator, client, OUTPUTS, **options)
    check(all(item["status"] == "already_completed" for item in replay["events"]), 'all(item["status"] == "already_completed" for item in replay["events"])')
    check(versions == client.objects, "versions == client.objects")


def test_batch_reuses_pinned_capture_without_network(setup_capture: tuple, tmp_path: Path) -> None:
    content = b"id,value\n0012A,2\n"
    setup_capture[1]["expected_sha256"] = hashlib.sha256(content).hexdigest()
    batch, registry, validator = batch_setup(setup_capture, jobs=2)
    calls = []

    def record(*args: Any) -> Any:
        calls.append(args)
        return runner_with_response(content)(*args)

    client = FakeS3()
    result = batches.run_batch(batch, tmp_path, registry, validator, client, OUTPUTS, execute=True, capture_fn=record, sleep_fn=lambda _: None)
    check(
        len(calls) == 1 and all(item["status"] == "stored_unvalidated" for item in result["events"]),
        'len(calls) == 1 and all(item["status"] == "stored_unvalidated" for item in result["events"])',
    )
    second = read_json(Path(result["events"][1]["receipt_path"]))
    first = read_json(Path(result["events"][0]["receipt_path"]))
    check(second["lineage"]["parent_snapshot_ids"] == [first["snapshot_id"]], 'second["lineage"]["parent_snapshot_ids"] == [first["snapshot_id"]]')
    check(
        second["acquisition"]["retrieved_at_utc"] == first["acquisition"]["retrieved_at_utc"],
        'second["acquisition"]["retrieved_at_utc"] == first["acquisition"]["retrieved_at_utc"]',
    )


def test_completed_batch_rejects_a_changed_collection_layout(setup_capture: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    batch, registry, validator = batch_setup(setup_capture)
    client = FakeS3()
    options = {"execute": True, "capture_fn": runner_with_response(), "sleep_fn": lambda _: None}
    batches.run_batch(batch, tmp_path, registry, validator, client, OUTPUTS, **options)
    monkeypatch.setattr(
        storage,
        "load_routes",
        lambda _: {setup_capture[0]["source_id"]: {"publisher": "example_publisher", "collection": "another_collection", "layout_sha256": "b" * 64}},
    )
    with pytest.raises(storage.CaptureError, match="immutable plan"):
        batches.run_batch(batch, tmp_path, registry, validator, client, OUTPUTS, **options)


def test_interrupted_upload_recovers_without_downloading_again(setup_capture: tuple, tmp_path: Path) -> None:
    batch, registry, validator = batch_setup(setup_capture)
    client, calls = FakeS3(), []
    client.put_failure = "AccessDenied"

    def record(*args: Any) -> Any:
        calls.append(args)
        return runner_with_response()(*args)

    options = {"execute": True, "capture_fn": record, "sleep_fn": lambda _: None}
    first = batches.run_batch(batch, tmp_path, registry, validator, client, OUTPUTS, **options)
    check(first["events"][0]["status"] == "pending_failure", 'first["events"][0]["status"] == "pending_failure"')
    client.put_failure = None
    second = batches.run_batch(batch, tmp_path, registry, validator, client, OUTPUTS, **options)
    check(
        second["events"][0]["status"] == "stored_unvalidated" and len(calls) == 1, 'second["events"][0]["status"] == "stored_unvalidated" and len(calls) == 1'
    )


def test_failed_capture_stays_only_in_audit(setup_capture: tuple, tmp_path: Path) -> None:
    batch, registry, validator = batch_setup(setup_capture)
    client = FakeS3()
    result = batches.run_batch(
        batch, tmp_path, registry, validator, client, OUTPUTS, execute=True, capture_fn=runner_with_response(b"denied", 403), sleep_fn=lambda _: None
    )
    check(result["events"][0]["status"] == "evidence_only", 'result["events"][0]["status"] == "evidence_only"')
    check(
        client.objects and all(key.split("/")[2] == "audit" for key in client.objects),
        'client.objects and all(key.split("/")[2] == "audit" for key in client.objects)',
    )


def test_download_budget_holds_job_before_network(setup_capture: tuple, tmp_path: Path) -> None:
    batch, registry, validator = batch_setup(setup_capture)

    def fail(*args: Any) -> None:
        pytest.fail("Network capture must not run")

    result = batches.run_batch(batch, tmp_path, registry, validator, FakeS3(), OUTPUTS, execute=True, capture_fn=fail, max_download_bytes=1)
    check(
        result["events"][0]["status"] == "pending_failure" and "budget" in result["events"][0]["reason"],
        'result["events"][0]["status"] == "pending_failure" and "budget" in result["events"][0]["reason"]',
    )


@pytest.mark.parametrize("mutation", ["version", "hash", "empty", "duplicate", "extra_job_field", "large_file", "hold"])
def test_invalid_batch_is_rejected_before_execution(setup_capture: tuple, mutation: str) -> None:
    batch, registry, validator = batch_setup(setup_capture)
    if mutation == "version":
        batch["batch_version"] = 2
    elif mutation == "hash":
        batch["registry_sha256"] = "0" * 64
    elif mutation == "empty":
        batch["jobs"] = []
    elif mutation == "duplicate":
        batch["jobs"].append(copy.deepcopy(batch["jobs"][0]))
    elif mutation == "extra_job_field":
        batch["jobs"][0]["ignore_holds"] = True
    elif mutation == "large_file":
        batch["jobs"][0]["limits"]["max_bytes"] = 1024**3
    else:
        registry["sources"][0]["preferred_route"] = "access_hold"
        batch["registry_sha256"] = canonical_hash(registry)
    with pytest.raises(ValueError):
        batches.validate_batch(batch, registry, validator)


def test_bls_quota_reserves_retries_and_recovers_after_24_hours(tmp_path: Path) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    for _ in range(8):
        batches.reserve_bls_requests(tmp_path, 3, now)
    with pytest.raises(storage.CaptureError, match="budget"):
        batches.reserve_bls_requests(tmp_path, 3, now)
    batches.reserve_bls_requests(tmp_path, 3, now + timedelta(days=1, seconds=1))


@pytest.mark.parametrize("mode", ["bls_api", "il_directory"])
def test_approved_api_pages_can_be_stored(setup_capture: tuple, tmp_path: Path, mode: str) -> None:
    configure_api(setup_capture, "BLS" if mode == "bls_api" else "IL", mode)
    source, plan, validator = setup_capture
    if mode == "bls_api":
        plan.update(series_ids=["LAUCN999990000000003"], start_year=2020, end_year=2020, expected_periods=["M13"])
        responses = [bls_response(plan["series_ids"][0])]
    else:
        responses = [il_response(1), il_response(2)]
    registry = {"sources": [source]}
    path = capture(plan, tmp_path, registry, validator, opener=Opener(*responses))
    client = FakeS3()
    report = storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator)
    check(
        report["objects_created"] > 0 and any(key.split("/")[2] == "datasets" for key in client.objects),
        'report["objects_created"] > 0 and any(key.split("/")[2] == "datasets" for key in client.objects)',
    )
    receipt = read_json(path)
    receipt["acquisition"]["pagination"]["termination_verified"] = False
    path.write_bytes(storage.encoded_json(receipt))
    with pytest.raises(storage.CaptureError, match="termination"):
        storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator)


def make_zip(path: Path, names: dict[str, bytes]) -> str:
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in names.items():
            archive.writestr(name, data)
    return storage.fingerprint(path)[0]


def test_expansion_keeps_data_and_dictionary_bytes(tmp_path: Path) -> None:
    path = tmp_path / "archive.zip"
    digest = make_zip(path, {"source/data.csv": b"id\r\n0012A\r\n", "source/dictionary.pdf": b"pdf", "__MACOSX/meta": b"x"})
    result = archives.expand_archive(path, tmp_path / "expanded", digest)
    check(len(result["members"]) == 2 and len(result["skipped_members"]) == 1, 'len(result["members"]) == 2 and len(result["skipped_members"]) == 1')
    check(
        result["dictionary_status"] == "bundled_candidates_require_mapping_review", 'result["dictionary_status"] == "bundled_candidates_require_mapping_review"'
    )
    check(archives.expand_archive(path, tmp_path / "expanded", digest) == result, 'archives.expand_archive(path, tmp_path / "expanded", digest) == result')
    check(
        (tmp_path / "expanded/members/source/data.csv").read_bytes() == b"id\r\n0012A\r\n",
        '(tmp_path / "expanded/members/source/data.csv").read_bytes() == b"id\\r\\n0012A\\r\\n"',
    )


@pytest.mark.parametrize("name", ["../escape.csv", "/absolute.csv", "C:/escape.csv", "folder\\escape.csv"])
def test_unsafe_archive_members_are_rejected(tmp_path: Path, name: str) -> None:
    path = tmp_path / "archive.zip"
    digest = make_zip(path, {name: b"data"})
    with pytest.raises(storage.CaptureError, match="unsafe"):
        archives.expand_archive(path, tmp_path / "expanded", digest)


def test_archive_limits_checksum_and_existing_data(tmp_path: Path) -> None:
    path = tmp_path / "archive.zip"
    digest = make_zip(path, {"data.csv": b"test"})
    with pytest.raises(storage.CaptureError, match="checksum"):
        archives.expand_archive(path, tmp_path / "a", "0" * 64)
    with pytest.raises(storage.CaptureError, match="limit"):
        archives.expand_archive(path, tmp_path / "a", digest, max_total_bytes=1)
    with pytest.raises(storage.CaptureError, match="positive"):
        archives.expand_archive(path, tmp_path / "a", digest, max_members=0)
    archives.expand_archive(path, tmp_path / "expanded", digest)
    (tmp_path / "expanded/members/data.csv").write_bytes(b"changed")
    with pytest.raises(storage.CaptureError, match="overwrite"):
        archives.expand_archive(path, tmp_path / "expanded", digest)


def test_nested_archives_are_not_uploaded_as_data(setup_capture: tuple, tmp_path: Path) -> None:
    source, plan, validator = setup_capture
    plan.update(expected_format="zip", file_name="archive.zip")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("nested.zip", b"nested payload")
    registry = {"sources": [source]}
    path = capture(plan, tmp_path, registry, validator, opener=Opener(Response(buffer.getvalue())))
    client = FakeS3()
    with pytest.raises(storage.CaptureError, match="Nested ZIP"):
        storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator)
    check(not client.objects, "not client.objects")


def source_for_inventory(source: dict) -> dict:
    return {
        **source,
        "planning": {"wave": 1},
        "measurement_history": {"advertised": "Unverified historical range", "unresolved": "Earlier history"},
        "api_fallback": {},
        "required_checks": ["Keep every check"],
        "linked_measure_ids": ["M001"],
    }


def archive_index() -> dict:
    return {
        "meta": {"total_pages": 1, "current_page": 1, "total_items": 1},
        "data": [
            {
                "id": "release_1",
                "name": "Published hospitals snapshot",
                "date": "2021-01-01",
                "type": "theme",
                "theme": "hospitals",
                "url": "/provider-data/sites/default/files/dataset-archives/theme/hospitals/example.zip",
                "access_level": "public",
                "size": "100",
            }
        ],
    }


def test_inventory_keeps_all_sources_and_gaps(setup_capture: tuple) -> None:
    source = source_for_inventory(setup_capture[0])
    held = {**copy.deepcopy(source), "source_id": "held", "preferred_route": "access_hold"}
    held["file_routes"][0]["route_id"] = "held_route"
    registry = {"sources": [source, held]}
    catalog = {
        "example": {
            "landingPage": source["official_landing_url"],
            "distribution": [{"downloadURL": "https://example.org/older.zip", "title": "Earlier release"}],
            "describedBy": "https://example.org/dictionary.pdf",
        }
    }
    result = inventories.build_inventory(registry, archive_index(), catalog)
    check(
        result["counts"]["sources"] == 2 and result["counts"]["access_holds"] == 1, 'result["counts"]["sources"] == 2 and result["counts"]["access_holds"] == 1'
    )
    check(all(item["history_complete"] is False for item in result["sources"]), 'all(item["history_complete"] is False for item in result["sources"])')
    check(
        len(result["known_routes"]) == 2 and len(result["shared_download_groups"]) == 1,
        'len(result["known_routes"]) == 2 and len(result["shared_download_groups"]) == 1',
    )
    check(
        all(item["acquisition_authorized_by_this_inventory"] is False for item in result["discovery_candidates"]),
        'all(item["acquisition_authorized_by_this_inventory"] is False for item in result["discovery_candidates"])',
    )
    check(result["known_routes"][1]["status"] == "access_hold", 'result["known_routes"][1]["status"] == "access_hold"')


@pytest.mark.parametrize("mutation", ["pagination", "scope", "host", "duplicate"])
def test_invalid_archive_index_is_rejected(mutation: str) -> None:
    payload = archive_index()
    if mutation == "pagination":
        payload["meta"]["total_pages"] = 2
    elif mutation == "scope":
        payload["data"][0]["theme"] = "unrelated"
    elif mutation == "host":
        payload["data"][0]["url"] = "https://example.org/foreign.zip"
    else:
        payload["data"].append(payload["data"][0].copy())
        payload["meta"]["total_items"] = 2
    with pytest.raises(storage.CaptureError):
        inventories.cms_archive_candidates(payload, [])


def test_embedded_catalog_resources_are_not_discarded() -> None:
    found = inventories.embedded_candidates({"resources": [{"url": "https://example.org/old.csv", "name": "Old release"}]}, "example")
    check(len(found) == 1 and found[0]["source_ids"] == ["example"], 'len(found) == 1 and found[0]["source_ids"] == ["example"]')


def test_inventory_records_stored_snapshot_without_claiming_live_recheck(bundle: tuple) -> None:
    path, references, client, registry, validator = bundle
    storage.upload_snapshot(path, references, client, OUTPUTS, registry, validator)
    registry["sources"][0] = source_for_inventory(registry["sources"][0])
    # The inventory can also retain the older archive-only reconciliation from a checkpoint.
    (path.parent / "s3_reconciliation.json").write_text("{}")
    result = inventories.build_inventory(registry, receipts=[path])
    check(result["stored_snapshots"][0]["live_s3_rechecked"] is False, 'result["stored_snapshots"][0]["live_s3_rechecked"] is False')


def test_cli_offline_batch_does_not_read_env(setup_capture: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    batch, registry, validator = batch_setup(setup_capture)
    path = tmp_path / "batch.json"
    path.write_bytes(storage.encoded_json(batch))
    monkeypatch.setattr(sys, "argv", ["batch", "--plan", str(path)])
    monkeypatch.setattr(batches, "load_registry", lambda: registry)
    monkeypatch.setattr(batches, "receipt_validator", lambda: validator)
    monkeypatch.setattr(batches, "load_configuration", lambda _: pytest.fail("Private .env must not be read"))
    batches.main()
    check(json.loads(capsys.readouterr().out)["status"] == "validated_offline", 'json.loads(capsys.readouterr().out)["status"] == "validated_offline"')


def test_reuse_requires_pinned_equal_scope(setup_capture: tuple, tmp_path: Path) -> None:
    source, plan, validator = setup_capture
    registry = {"sources": [source]}
    path = capture(plan, tmp_path / "first", registry, validator, opener=Opener(Response(b"id\n1\n")))
    with pytest.raises(storage.CaptureError, match="checksum-pinned"):
        reuse.reuse_capture(plan, path, tmp_path / "second", registry, validator)
    plan["expected_sha256"] = hashlib.sha256(b"id\n1\n").hexdigest()
    with pytest.raises(storage.CaptureError, match="scope"):
        reuse.reuse_capture(plan, path, tmp_path / "second", registry, validator)
