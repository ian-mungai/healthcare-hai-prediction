"""Decision 115: a task unit records success only after its capture is stored and read back under raw/."""

from __future__ import annotations

import io
import json
import logging
import zipfile
from functools import partial
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import (
    collect_bls_api,
    collect_census_acs_api,
    collect_census_acs_detailed,
    collect_hud_api,
    collection_layout,
    s3_store,
    task_role,
    transport,
)
from scripts.acquisition import redownload as harness
from scripts.acquisition.capture import capture, receipt_validator
from scripts.acquisition.run_redownload_e2e import Fixture
from scripts.acquisition.source_registry import REPO_ROOT, load_registry, read_json
from scripts.acquisition.tests.leak_check import channel_leaks, tree_leaks
from scripts.acquisition.tests.task_fixture import task_run
from scripts.acquisition.tests.test_task_owners import TaskS3, Web, opener, storage
from scripts.acquisition.transport import Limits
from tests.support import check

PLANTED = "plantedVALUE" + "0123456789" * 2
STORED = b"a,b\n1,2\n"


@pytest.fixture(autouse=True)
def isolated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(s3_store, "load_routes", collection_layout.load_routes)
    for name in (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE):
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")


def real_plan(name: str) -> dict:
    """A real reviewed job plan, so the stored receipt, its route and its source are the ones storage checks."""
    batch = read_json(REPO_ROOT / read_json(REPO_ROOT / "config/acquisition/e2e_scenario.json")["batch_plan"])
    plan = dict(next(job for job in batch["jobs"] if job["job_id"] == "CMS_HCRIS_PUF_file_001")["plan"])
    plan.pop("expected_sha256", None)
    if name.endswith(".zip"):
        plan |= {"expected_format": "zip", "file_name": name}
    return plan


def stored_unit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, body: bytes, name: str) -> tuple[Fixture, dict]:
    """A stored capture made by the real capture path, on the bucket root as before this route."""
    registry, validator = load_registry(), receipt_validator()
    plan = real_plan(name)
    url = next(
        route["url"]
        for source in registry["sources"]
        if source["source_id"] == plan["source_id"]
        for route in source["file_routes"]
        if route["route_id"] == plan["route_id"]
    )
    with monkeypatch.context() as earlier:
        earlier.delenv("HAI_CAPTURE_ROOT", raising=False)
        receipt = capture(plan, tmp_path / "stored", registry, validator, Limits(attempts=1, timeout_seconds=5, max_seconds=30), opener(Web({url: body})))
    fixture = Fixture(tmp_path)
    record = read_json(receipt)
    unit = {
        "snapshot_id": record["snapshot_id"],
        "source_id": plan["source_id"],
        "receipt": str(receipt.relative_to(tmp_path)),
        "receipt_sha256": harness.digest(receipt.read_bytes()),
        "url": url,
        "mode": "file",
        "bytes": len(body),
        "pages": 1,
        "disposition": "direct_download",
        "route": "publisher_url",
        "flags": [],
    }
    fixture.entries.append(unit)
    return fixture, unit


def file_run(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, bucket: TaskS3, fresh: bytes, name: str = "a.csv", stored: bytes = STORED
) -> tuple[harness.Run, dict]:
    fixture, unit = stored_unit(monkeypatch, tmp_path, fresh if name.endswith(".zip") else stored, name)
    return task_run(tmp_path, fixture, opener=opener(Web({unit["url"]: fresh})), task_storage=storage(bucket)), unit


def stored_bodies(bucket: TaskS3) -> dict[str, bytes]:
    return {key: data for key, (_, data, _) in bucket.objects.items()}


def test_an_exact_file_unit_is_stored_under_raw_before_it_counts(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    summary = harness.execute(run)
    check(summary["outcomes"] == {"exact_match": 1}, f"the unit matched: {summary}")
    check(bool(bucket.objects) and all(key.startswith("raw/") for key in bucket.objects), f"keys: {sorted(bucket.objects)}")
    check(any(data == STORED for data in stored_bodies(bucket).values()), "the fresh bytes are stored")
    check(any(key.endswith("/manifest.json") for key in bucket.objects), "a manifest lets bronze find the capture")
    record = harness.outcome(run, unit["snapshot_id"]) or {}
    check(record["stored"]["receipt"]["key"] in bucket.objects, "the outcome names the stored receipt")
    check(all(item["key"] in bucket.objects for item in record["stored"]["manifests"]), "the outcome names the stored manifest")


def test_a_changed_file_unit_is_stored_too(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    run, unit = file_run(monkeypatch, tmp_path, bucket, b"a,b\n3,4\n")
    summary = harness.execute(run)
    check(summary["outcomes"] == {"changed_needs_review": 1}, f"the unit changed: {summary}")
    check(any(data == b"a,b\n3,4\n" for data in stored_bodies(bucket).values()), "the changed bytes are stored")
    check("stored" in (harness.outcome(run, unit["snapshot_id"]) or {}), "the outcome names them")


def test_a_refused_store_blocks_the_unit_and_records_no_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3(refuse_puts=True)
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    summary = harness.execute(run)
    check(summary["outcomes"] == {"blocked": 1}, f"a store failure blocks the unit: {summary}")
    record = harness.outcome(run, unit["snapshot_id"]) or {}
    check(record["reason"] == "raw_store_failed" and "stored" not in record, str(record))


def test_a_read_back_that_differs_blocks_the_unit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bucket = TaskS3()
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    original = bucket.put_object

    def corrupt(**arguments: Any) -> dict:
        answer = original(**arguments)
        version, data, fields = bucket.objects[arguments["Key"]]
        bucket.objects[arguments["Key"]] = (version, data + b"x", fields)
        return answer

    monkeypatch.setattr(bucket, "put_object", corrupt)
    check(harness.execute(run)["outcomes"] == {"blocked": 1}, "a wrong read back is no success")


def test_a_failed_download_stores_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    fixture, unit = stored_unit(monkeypatch, tmp_path, STORED, "a.csv")
    web = Web({unit["url"]: STORED})
    web.override[unit["url"]] = OSError("reset")
    run = task_run(tmp_path, fixture, opener=opener(web), task_storage=storage(bucket))
    summary = harness.execute(run)
    check("exact_match" not in summary["outcomes"] and not bucket.objects, f"nothing is stored: {summary}")


def archive(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as output:
        for name, body in members.items():
            output.writestr(name, body)
    return buffer.getvalue()


def test_a_single_table_archive_stores_its_extracted_member_and_never_the_container(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    body = archive({"data.csv": STORED})
    run, _unit = file_run(monkeypatch, tmp_path, bucket, body, "a.zip")
    summary = harness.execute(run)
    check(summary["outcomes"] == {"exact_match": 1}, f"the archive matched: {summary}")
    check(not any(key.endswith(".zip") for key in bucket.objects) and body not in stored_bodies(bucket).values(), "the container is not stored")
    check(STORED in stored_bodies(bucket).values(), "its member is")


def test_an_archive_with_no_reviewed_mapping_blocks_by_name_and_counts_for_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    body = archive({"one.csv": STORED, "two.csv": b"c\n1\n"})
    run, unit = file_run(monkeypatch, tmp_path, bucket, body, "a.zip")
    summary = harness.execute(run)
    record = harness.outcome(run, unit["snapshot_id"]) or {}
    check(summary["outcomes"] == {"blocked": 1} and record["reason"] == "raw_store_failed", f"{summary} {record}")


def test_no_planted_value_reaches_a_stored_name_or_body_from_a_store_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capfd: pytest.CaptureFixture, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    bucket = TaskS3()
    run, _unit = file_run(monkeypatch, tmp_path, bucket, STORED)

    def explode(*_args: Any, **_kwargs: Any) -> dict:
        raise ValueError(f"stored with {PLANTED}")

    monkeypatch.setattr(harness, "upload_snapshot", explode)
    harness.execute(run)
    captured = capfd.readouterr()
    check(tree_leaks(PLANTED, run.state_root) == [], "nothing in the state holds it")
    check(
        channel_leaks(
            PLANTED, {"stdout": captured.out, "stderr": captured.err, "log": caplog.text} | stored_bodies(bucket) | {"keys": json.dumps(sorted(bucket.objects))}
        )
        == [],
        "no channel holds it",
    )


def reviewed_archive_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, bucket: TaskS3, fresh: bytes) -> tuple[harness.Run, dict]:
    """A stored archive whose earlier storage names each member's dataset and role, in the reconciliation beside its receipt."""
    earlier = archive({"one.csv": STORED, "two.csv": b"c\n1\n", "notes.txt": b"notes\n"})
    fixture, unit = stored_unit(monkeypatch, tmp_path, earlier, "a.zip")
    rows = [
        {"role": "data", "dataset_id": "first_table", "mapped_dataset_id": "first_table", "archive_member": json.dumps(["one.csv"])},
        {"role": "data", "dataset_id": "second_table", "mapped_dataset_id": "second_table", "archive_member": json.dumps(["two.csv"])},
        {"role": "reference", "dataset_id": "first_table", "mapped_dataset_id": None, "archive_member": json.dumps(["notes.txt"])},
    ]
    (tmp_path / unit["receipt"]).parent.joinpath("s3_collections_reconciliation.json").write_text(json.dumps({"objects": rows, "manifests": []}))
    return task_run(tmp_path, fixture, opener=opener(Web({unit["url"]: fresh})), task_storage=storage(bucket)), unit


def test_a_reviewed_archive_stores_each_member_in_its_reviewed_dataset(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    body = archive({"one.csv": STORED, "two.csv": b"c\n1\n", "notes.txt": b"notes\n"})
    run, unit = reviewed_archive_run(monkeypatch, tmp_path, bucket, body)
    summary = harness.execute(run)
    check(summary["outcomes"] == {"exact_match": 1}, f"the archive matched: {summary} {harness.outcome(run, unit['snapshot_id'])}")
    check(
        any("/datasets/first_table/" in key for key in bucket.objects) and any("/datasets/second_table/" in key for key in bucket.objects),
        f"keys: {sorted(bucket.objects)}",
    )
    check(not any(key.endswith(".zip") for key in bucket.objects), "the container is not stored")
    record = harness.outcome(run, unit["snapshot_id"]) or {}
    check(len(record["stored"]["manifests"]) == 2, "each dataset has its manifest in the evidence")


def test_a_changed_member_keeps_its_reviewed_dataset_and_is_marked_changed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    body = archive({"one.csv": b"a,b\n9,9\n", "two.csv": b"c\n1\n", "notes.txt": b"notes\n"})
    run, _unit = reviewed_archive_run(monkeypatch, tmp_path, bucket, body)
    check(harness.execute(run)["outcomes"] == {"changed_needs_review": 1}, "the archive changed")
    check(b"a,b\n9,9\n" in stored_bodies(bucket).values(), "the changed member is stored")


@pytest.mark.parametrize(
    "members",
    [{"one.csv": STORED, "two.csv": b"c\n1\n", "notes.txt": b"notes\n", "new.csv": b"d\n"}, {"one.csv": STORED, "notes.txt": b"notes\n"}],
    ids=["new member", "lost member"],
)
def test_an_archive_whose_members_differ_from_the_review_blocks_and_stores_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, members: dict) -> None:
    bucket = TaskS3()
    run, unit = reviewed_archive_run(monkeypatch, tmp_path, bucket, archive(members))
    check(harness.execute(run)["outcomes"] == {"blocked": 1}, "a member the review never saw is blocked")
    check((harness.outcome(run, unit["snapshot_id"]) or {})["reason"] == "raw_store_failed" and not bucket.objects, "nothing was stored")


KEYED = ["bls_api_v2", "census_acs_api", "census_acs_detailed", "hud_api"]


@pytest.mark.parametrize("mode", KEYED)
def test_a_keyed_adapter_asks_its_collector_to_store_with_the_task_storage_client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mode: str) -> None:
    calls: list[tuple] = []

    def execute(*arguments: Any, **_options: Any) -> dict:
        calls.append(arguments)
        return {"receipt_path": str(tmp_path / "receipt.json")}

    modules = {
        "bls_api_v2": collect_bls_api,
        "census_acs_api": collect_census_acs_api,
        "census_acs_detailed": collect_census_acs_detailed,
        "hud_api": collect_hud_api,
    }
    monkeypatch.setattr(modules[mode], "execute", execute)
    monkeypatch.setattr(harness, "planned", lambda unit, run: ({"id": "p"}, {"id": "b"}))
    monkeypatch.setattr(harness, "require_bls_budget", lambda *_args: None)
    monkeypatch.setattr(harness, "collection_root", lambda path: tmp_path)
    monkeypatch.setattr(harness, "receipt_file", lambda unit, run: tmp_path / "stored.json")
    chosen = storage(TaskS3())
    run = harness.Run(
        tmp_path / "q.json",
        tmp_path / "state",
        base=tmp_path,
        stored_root=tmp_path,
        task_keys=partial(task_role.TaskKeys, environment={"AWS_REGION": "us-west-2"}),
        task_storage=chosen,
    )
    harness.ADAPTERS[mode]({"mode": mode}, run)
    upload, client = calls[0][4], calls[0][5]
    check(upload is True and client.storage is chosen, "the capture is stored through the one task storage client")
    check(client.configuration == chosen.configuration and client.storage_policy == chosen.storage_policy, "the key reader stores through that client")


def test_a_key_reader_without_a_storage_client_cannot_store() -> None:
    client = task_role.TaskKeys("hud_api", {"AWS_REGION": "us-west-2"})
    with pytest.raises(s3_store.StorageError):
        client.call("s3api", "head-object", ["--key", "raw/a"])
