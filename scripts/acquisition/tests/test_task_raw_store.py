"""Decision 115: a task unit records success only after its capture is stored and read back under raw/."""

from __future__ import annotations

import base64
import copy
import dataclasses
import hashlib
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
from scripts.acquisition.s3_store import encoded_json
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
    rows = []
    for name, role, dataset, body in (
        ("one.csv", "data", "first_table", STORED),
        ("two.csv", "data", "second_table", b"c\n1\n"),
        ("notes.txt", "reference", "first_table", b"notes\n"),
    ):
        key = f"cms/stored/datasets/{name}"
        checksum = base64.b64encode(hashlib.sha256(body).digest()).decode()
        bucket.objects[key] = ("v0", body, {"ChecksumSHA256": checksum, "ServerSideEncryption": "AES256"})
        record = {"key": key, "version_id": "v0", "sha256": harness.digest(body), "byte_count": len(body)}
        rows.append(
            {
                "role": role,
                "dataset_id": dataset,
                "mapped_dataset_id": dataset if role == "data" else None,
                "archive_member": json.dumps([name]),
                "object": record,
            }
        )
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
    stored_now = [key for key in bucket.objects if key.startswith("raw/")]
    check((harness.outcome(run, unit["snapshot_id"]) or {})["reason"] == "raw_store_failed" and not stored_now, "nothing was stored")


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


def restart(run: harness.Run) -> harness.Run:
    """A new task over the same state: nothing is cached."""
    return dataclasses.replace(run, cache={})


def forge(bucket: TaskS3, item: dict, body: bytes) -> dict:
    """Replace an object as a writer could, with a checksum that fits, and return the matching evidence."""
    version, _old, fields = bucket.objects[item["key"]]
    bucket.objects[item["key"]] = (version, body, fields | {"ChecksumSHA256": base64.b64encode(hashlib.sha256(body).digest()).decode()})
    return item | {"sha256": harness.digest(body), "byte_count": len(body)}


def rewrite_outcome(run: harness.Run, unit: dict, **fields: Any) -> None:
    path = run.state_root / "outcomes" / f"{unit['snapshot_id']}.json"
    path.write_bytes(encoded_json(read_json(path) | fields))


def test_a_real_outcome_survives_the_recomputation_and_its_unit_is_not_attempted_again(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    run, _unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    check(harness.execute(run)["outcomes"] == {"exact_match": 1}, "first task")
    check(harness.execute(restart(run))["attempted"] == 0, "a real outcome is not attempted again")


def test_a_real_changed_outcome_survives_too(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    run, _unit = file_run(monkeypatch, tmp_path, bucket, b"a,b\n3,4\n")
    check(harness.execute(run)["outcomes"] == {"changed_needs_review": 1}, "first task")
    check(harness.execute(restart(run))["attempted"] == 0, "a changed capture stays recorded")


def test_a_changed_capture_labelled_exact_stops_the_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    run, unit = file_run(monkeypatch, tmp_path, bucket, b"a,b\n3,4\n")
    harness.execute(run)
    rewrite_outcome(run, unit, outcome="exact_match")
    with pytest.raises(ValueError, match="outcome"):
        harness.execute(restart(run))


def test_evidence_of_another_unit_stops_the_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    harness.execute(run)
    stored = (harness.outcome(run, unit["snapshot_id"]) or {})["stored"]
    document = json.loads(bucket.objects[stored["receipt"]["key"]][1])
    lineage = json.loads(document["lineage"]["extraction_or_query"]) | {"fresh_of": "another_unit"}
    forged = encoded_json(document | {"lineage": document["lineage"] | {"extraction_or_query": json.dumps(lineage)}})
    rewrite_outcome(run, unit, stored={"receipt": forge(bucket, stored["receipt"], forged), "manifests": stored["manifests"]})
    with pytest.raises(ValueError, match="outcome"):
        harness.execute(restart(run))


def test_a_manifest_without_the_data_object_stops_the_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    harness.execute(run)
    stored = (harness.outcome(run, unit["snapshot_id"]) or {})["stored"]
    item = stored["manifests"][0]
    document = json.loads(bucket.objects[item["key"]][1])
    forged = encoded_json(document | {"objects": [entry for entry in document["objects"] if entry["role"] != "data"]})
    rewrite_outcome(run, unit, stored={"receipt": stored["receipt"], "manifests": [forge(bucket, item, forged)]})
    with pytest.raises(ValueError, match="outcome"):
        harness.execute(restart(run))


def test_data_bytes_that_differ_from_the_manifest_stop_the_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    harness.execute(run)
    name = next(key for key, (_, data, _) in bucket.objects.items() if data == STORED)
    forge(bucket, {"key": name}, b"other")
    with pytest.raises(ValueError, match="outcome"):
        harness.execute(restart(run))


def test_a_changed_archive_member_labelled_exact_stops_the_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    fresh = archive({"one.csv": b"a,b\n9,9\n", "two.csv": b"c\n1\n", "notes.txt": b"notes\n"})
    run, unit = reviewed_archive_run(monkeypatch, tmp_path, bucket, fresh)
    harness.execute(run)
    rewrite_outcome(run, unit, outcome="exact_match")
    with pytest.raises(ValueError, match="outcome"):
        harness.execute(restart(run))


def test_a_real_reviewed_archive_outcome_survives_the_recomputation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    run, _unit = reviewed_archive_run(monkeypatch, tmp_path, bucket, archive({"one.csv": STORED, "two.csv": b"c\n1\n", "notes.txt": b"notes\n"}))
    check(harness.execute(run)["outcomes"] == {"exact_match": 1}, "first task")
    check(harness.execute(restart(run))["attempted"] == 0, "the archive outcome verifies")


def denying_registry(unit: dict, **route: Any) -> dict:
    registry = copy.deepcopy(load_registry())
    source = next(item for item in registry["sources"] if item["source_id"] == unit["source_id"])
    source["file_routes"] = [{"route_id": "named_route", "url": unit["url"], **route}]
    return registry


@pytest.mark.parametrize(
    "route",
    [{"status": "access_failed"}, {"status": "HTTP_403_file_route_unverified"}, {"route_type": "file_index"}],
    ids=["access failed", "http 403", "file index"],
)
def test_a_route_the_registry_denies_stays_denied_and_stores_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, route: dict) -> None:
    bucket = TaskS3()
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    monkeypatch.setattr(harness, "load_registry", lambda **_options: denying_registry(unit, **route))
    summary = harness.execute(run)
    record = harness.outcome(run, unit["snapshot_id"]) or {}
    check(summary["outcomes"] == {"blocked": 1} and record["reason"] == "raw_store_failed", f"{summary} {record}")
    check(not bucket.objects, "nothing was stored")


def test_a_denied_route_under_another_spelling_of_the_address_stays_denied(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    respelled = unit["url"].replace("https://", "HTTPS://").replace(".gov", ".GOV") + "#fragment"
    monkeypatch.setattr(harness, "load_registry", lambda **_options: denying_registry(unit | {"url": respelled}, status="access_failed"))
    check(harness.execute(run)["outcomes"] == {"blocked": 1} and not bucket.objects, "the denial follows the address")


def test_a_route_denied_under_another_source_stays_denied(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)

    def registry(**_options: Any) -> dict:
        document = copy.deepcopy(load_registry())
        other = next(item for item in document["sources"] if item["source_id"] != unit["source_id"])
        other["file_routes"] = [*other.get("file_routes", []), {"route_id": "elsewhere", "url": unit["url"], "status": "access_failed"}]
        return document

    monkeypatch.setattr(harness, "load_registry", registry)
    check(harness.execute(run)["outcomes"] == {"blocked": 1} and not bucket.objects, "the denial follows the address across sources")


def test_a_route_the_registry_names_and_allows_is_used_with_its_own_identity(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    monkeypatch.setattr(harness, "load_registry", lambda **_options: denying_registry(unit))
    check(harness.execute(run)["outcomes"] == {"exact_match": 1}, "an allowed named route stores")
    receipt = json.loads(bucket.objects[(harness.outcome(run, unit["snapshot_id"]) or {})["stored"]["receipt"]["key"]][1])
    check(json.loads(receipt["lineage"]["extraction_or_query"])["route_id"] == "named_route", "no alias replaces a named route")


def test_only_a_route_the_registry_does_not_name_takes_the_frozen_queue_approval(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    registry = copy.deepcopy(load_registry())
    for source in registry["sources"]:
        source["file_routes"] = [route for route in source.get("file_routes", []) if route["url"] != unit["url"]]
    monkeypatch.setattr(harness, "load_registry", lambda **_options: copy.deepcopy(registry))
    check(harness.execute(run)["outcomes"] == {"exact_match": 1}, "an unnamed route stores")
    receipt = json.loads(bucket.objects[(harness.outcome(run, unit["snapshot_id"]) or {})["stored"]["receipt"]["key"]][1])
    check(json.loads(receipt["lineage"]["extraction_or_query"])["route_id"].startswith("frozen_queue_"), "the unnamed route carries the queue approval")
