from __future__ import annotations

import base64
import hashlib
import json
from contextlib import suppress
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import redownload as harness
from scripts.acquisition import redownload_route as route
from scripts.acquisition.s3_store import StorageError
from scripts.acquisition.tests.test_run_lanes import LANES, UNITS, dispatch, lane_files, refused
from tests.support import check

LANE = "mac_person_units"
PERSON_CLASSES = {"needs_person"}
PERSON_IDS = sorted(unit_id for unit_id, lane in LANES.items() if lane in PERSON_CLASSES)
NAMES = ("Jane Q. Example", "Ozzie Sample-Rivera", "Dr. Pat Fictional")


def test_the_person_template_names_the_lane_the_class_and_the_unit_count(tmp_path: Path) -> None:
    folder = tmp_path / "route"
    folder.mkdir()
    queue = folder / "queue.json"
    queue.write_text(json.dumps({"run": 3, "queue": UNITS}))
    document = route.write_lane_template(queue, folder / "template.json")
    check(document["lane"] == LANE and document["approved_by"] is None, "the template names the lane and no approver")
    check(document["classes"] == sorted(PERSON_CLASSES) and document["units"] == len(PERSON_IDS), "the template names the classes and the unit count")
    check("originals_stay_local" not in document and "records" not in document, "the template carries no private-data fields")
    check(any("approval" in item for item in route.lane_problems(queue, folder / "template.json")), "an unsigned template is not accepted")


@pytest.mark.parametrize(
    "change",
    [
        {"approved_by": None},
        {"queue_sha256": "0" * 64},
        {"classes": ["needs_person", "task"]},
        {"units": 1},
        {"lane": "another_lane"},
    ],
)
def test_the_person_lane_refuses_to_start_unless_the_owner_approval_names_it(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, change: dict) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    run, _, _ = lane_files(tmp_path, **change)
    refused(monkeypatch, run, "lane approval")


def test_the_person_lane_refuses_without_an_approval_file_and_inside_a_task(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    run, approval, _ = lane_files(tmp_path / "a")
    approval.unlink()
    refused(monkeypatch, run, "lane approval")
    task, _, _ = lane_files(tmp_path / "b")
    monkeypatch.setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/example")
    refused(monkeypatch, task, "where the browser saved them")
    task.stored_root = task.base
    monkeypatch.delenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
    refused(monkeypatch, task, "where the browser saved them")


def test_the_person_lane_needs_the_same_gate_and_the_raw_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("HAI_CAPTURE_ROOT", raising=False)
    run, _, files = lane_files(tmp_path)
    refused(monkeypatch, run, "raw/")
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    files.confirmation.unlink()
    refused(monkeypatch, run, "removal confirmation")


def test_an_approved_person_lane_passes_both_gates_and_stops_at_the_next_gate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    for name in ("HAI_APPROVED_HOSTS", "HAI_APPROVED_HOSTS_APPROVAL"):
        monkeypatch.setenv(name, "placeholder")
    for name in harness.TASK_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    run, _, _ = lane_files(tmp_path)
    with pytest.raises(ValueError, match="independent review"):
        harness.execute(run, allow_network=True)


def test_the_person_lane_and_the_task_partition_the_queue_with_no_unit_in_two(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    full_task = {"stored_root": tmp_path / "mirror", "task_keys": object()}
    runs: dict[str, dict[str, Any]] = {"person": {"person_lane": True, "task_storage": object()}, "task": full_task, "mac": {}}
    attempted: dict[str, list[str]] = {}
    for name, fields in runs.items():
        (tmp_path / name).mkdir()
        attempted[name], _ = dispatch(monkeypatch, tmp_path / name, **fields)
    check(sorted(attempted["person"]) == PERSON_IDS, f"the person lane attempted {sorted(attempted['person'])}")
    check(not set(attempted["person"]) & set(attempted["task"]), "no unit runs in the person lane and the task")
    check(not set(attempted["mac"]) & set(attempted["person"]), "a plain run on the raw route attempts no lane unit")


def test_a_plain_run_on_the_raw_route_lists_the_browser_units_and_attempts_none(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    seen, summary = dispatch(monkeypatch, tmp_path)
    check(not set(seen) & set(PERSON_IDS), "no browser unit was attempted")
    check(sorted(summary["not_attempted"]["needs_person"]) == PERSON_IDS, "they are listed by reason")


class RecordBucket:
    """The S3 calls the person lane makes through the project client: create-once puts and checksum heads, nothing else."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.puts: list[list[str]] = []
        self.lose_reply = False

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
        options = dict(zip(arguments[::2], arguments[1::2], strict=False)) if arguments else {}
        check(service == "s3api" and operation in {"put-object", "head-object"}, f"unexpected call {service} {operation}")
        key = options["--key"]
        if operation == "head-object":
            if key not in self.objects:
                raise harness.StorageError("head-object", "404")
            return {"ChecksumSHA256": base64.b64encode(hashlib.sha256(self.objects[key]).digest()).decode(), "VersionId": "v1"}
        self.puts.append(arguments or [])
        if options.get("--if-none-match") != "*":
            raise AssertionError("a record was written without the create-once condition")
        if key in self.objects:
            raise harness.StorageError("put-object", "PreconditionFailed")
        self.objects[key] = Path(options["--body"]).read_bytes()
        if self.lose_reply:
            raise harness.StorageError("put-object", "request_unconfirmed")
        return {"VersionId": "v1"}


def stored_stub(unit_id: str) -> dict:
    return {"receipt": {"key": f"raw/p/c/{unit_id}/receipt.json", "version_id": "v1", "sha256": "d" * 64}, "manifests": [{"key": "raw/p/c/manifest.json"}]}


def person_state(tmp_path: Path) -> tuple[harness.Run, RecordBucket, str]:
    run, _, _ = lane_files(tmp_path)
    bucket = RecordBucket()
    run.task_storage = bucket
    queue_sha = hashlib.sha256(run.queue_path.read_bytes()).hexdigest()
    (run.state_root / "outcomes").mkdir(parents=True)
    (run.state_root / "run.json").write_text(
        json.dumps({"queue_sha256": queue_sha, "controls_sha256": "c" * 64, "started_at_utc": "2026-11-05T00:00:00+00:00"})
    )
    for unit_id in PERSON_IDS:
        (run.state_root / "outcomes" / f"{unit_id}.json").write_text(
            json.dumps({"snapshot_id": unit_id, "outcome": "exact_match", "stored": stored_stub(unit_id)})
        )
    task_unit = next(unit_id for unit_id, lane in LANES.items() if lane == "task")
    (run.state_root / "outcomes" / f"{task_unit}.json").write_text(json.dumps({"snapshot_id": task_unit, "outcome": "exact_match"}))
    (run.state_root / "files" / PERSON_IDS[0]).mkdir(parents=True)
    (run.state_root / "files" / PERSON_IDS[0] / "saved.xlsx").write_bytes(b"fresh file bytes")
    return run, bucket, queue_sha


def test_the_person_lane_saves_its_run_binding_and_each_browser_outcome_once_under_raw(tmp_path: Path) -> None:
    run, bucket, queue_sha = person_state(tmp_path)
    harness.save_person_records(run)
    prefix = f"raw/run3_person/{queue_sha}/"
    expected = {f"{prefix}run.json"} | {f"{prefix}outcomes/{unit_id}.json" for unit_id in PERSON_IDS} | {f"raw/run3_runs/{queue_sha}/person.json"}
    check(set(bucket.objects) == expected, f"the saved records are {sorted(bucket.objects)}")
    check(bucket.objects[f"{prefix}run.json"] == (run.state_root / "run.json").read_bytes(), "the run binding is saved byte for byte")
    for unit_id in PERSON_IDS:
        local = (run.state_root / "outcomes" / f"{unit_id}.json").read_bytes()
        check(bucket.objects[f"{prefix}outcomes/{unit_id}.json"] == local, f"{unit_id}: the outcome is saved byte for byte")
    check(
        all({"--checksum-sha256", "--server-side-encryption", "--metadata"} <= set(put) for put in bucket.puts),
        "every put carries its checksum, encryption and digest",
    )


def test_the_person_records_name_only_the_lanes_own_units(tmp_path: Path) -> None:
    """The records are the lane's own outcomes; a task unit's outcome in the folder is not the lane's to save, and the files go with their captures."""
    run, bucket, _ = person_state(tmp_path)
    harness.save_person_records(run)
    task_units = {unit_id for unit_id, lane in LANES.items() if lane == "task"}
    check(not any(any(unit_id in key for unit_id in task_units) for key in bucket.objects), "a task unit's outcome was saved as a person record")
    check(not any("/files/" in key or key.endswith(".xlsx") for key in bucket.objects), "a record put sent a file")


def test_saving_again_after_a_lost_reply_creates_nothing_and_passes(tmp_path: Path) -> None:
    run, bucket, _ = person_state(tmp_path)
    bucket.lose_reply = True
    with pytest.raises(harness.StorageError):
        harness.save_person_records(run)
    bucket.lose_reply = False
    saved = dict(bucket.objects)
    harness.save_person_records(run)
    harness.save_person_records(run)
    check(len(bucket.objects) == len(PERSON_IDS) + 2 and all(bucket.objects[key] == body for key, body in saved.items()), "a repeat replaced or added a record")


def test_a_different_record_under_the_same_key_stops_the_lane(tmp_path: Path) -> None:
    run, bucket, queue_sha = person_state(tmp_path)
    bucket.objects[f"raw/run3_person/{queue_sha}/outcomes/{PERSON_IDS[0]}.json"] = b'{"outcome": "other"}'
    with pytest.raises(ValueError, match="differs"):
        harness.save_person_records(run)
    check(bucket.objects[f"raw/run3_person/{queue_sha}/outcomes/{PERSON_IDS[0]}.json"] == b'{"outcome": "other"}', "the earlier record was replaced")


def test_an_executed_person_lane_saves_its_records(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    for name in ("HAI_APPROVED_HOSTS", "HAI_APPROVED_HOSTS_APPROVAL"):
        monkeypatch.setenv(name, "placeholder")
    for name in harness.TASK_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    run, _, _ = lane_files(tmp_path)
    bucket = RecordBucket()
    run.task_storage = bucket
    run.cache["controls"] = {}
    monkeypatch.setattr(harness, "review_gate", lambda run: "c" * 64)

    def one_unit(run: harness.Run, *_: Any) -> dict:
        harness.write_once(
            run.state_root / "outcomes" / f"{PERSON_IDS[0]}.json", harness.encoded_json({"snapshot_id": PERSON_IDS[0], "outcome": "exact_match"})
        )
        return {"attempted": 1}

    monkeypatch.setattr(harness, "execute_locked", one_unit)
    harness.execute(run, allow_network=True)
    queue_sha = hashlib.sha256(run.queue_path.read_bytes()).hexdigest()
    check(
        set(bucket.objects) == {f"raw/run3_person/{queue_sha}/run.json", f"raw/run3_person/{queue_sha}/outcomes/{PERSON_IDS[0]}.json"},
        f"the run saved {sorted(bucket.objects)}",
    )


def test_a_complete_person_lane_names_every_saved_record_at_a_key_fixed_by_the_queue(tmp_path: Path) -> None:
    run, bucket, queue_sha = person_state(tmp_path)
    harness.save_person_records(run)
    record = json.loads(bucket.objects[f"raw/run3_runs/{queue_sha}/person.json"])
    prefix = f"raw/run3_person/{queue_sha}/"
    check(record["lane"] == LANE and record["queue_sha256"] == queue_sha, str(record))
    check(
        record["run"] == {"key": f"{prefix}run.json", "version_id": "v1", "sha256": hashlib.sha256(bucket.objects[f"{prefix}run.json"]).hexdigest()},
        str(record),
    )
    check(sorted(record["outcomes"]) == PERSON_IDS, f"the record names {sorted(record['outcomes'])}")
    for unit_id in PERSON_IDS:
        key = f"{prefix}outcomes/{unit_id}.json"
        named = {
            "record": {"key": key, "version_id": "v1", "sha256": hashlib.sha256(bucket.objects[key]).hexdigest()},
            "receipt": stored_stub(unit_id)["receipt"],
        }
        check(record["outcomes"][unit_id] == named, unit_id)
    before = dict(bucket.objects)
    harness.save_person_records(run)
    check(bucket.objects == before, "a repeat changes nothing")


@pytest.mark.parametrize("gap", ["missing", "unstored"])
def test_an_incomplete_person_lane_writes_no_run_record(tmp_path: Path, gap: str) -> None:
    run, bucket, queue_sha = person_state(tmp_path)
    path = run.state_root / "outcomes" / f"{PERSON_IDS[0]}.json"
    if gap == "missing":
        path.unlink()
    else:
        path.write_text(json.dumps({"snapshot_id": PERSON_IDS[0], "outcome": "blocked", "reason": "raw_store_failed"}))
    harness.save_person_records(run)
    check(f"raw/run3_runs/{queue_sha}/person.json" not in bucket.objects, "a lane with a unit still unsaved claimed to be complete")


@pytest.fixture
def raw_route(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts.acquisition import collection_layout, s3_store, transport

    monkeypatch.setattr(s3_store, "load_routes", collection_layout.load_routes)
    # The route gate sets the approved hosts variables in the process. Set and then delete each one, so the fixture removes what the gate set after the test.
    for name in (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE, *harness.TASK_ENVIRONMENT):
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")


def browser_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, bucket: Any, saved: bytes) -> tuple[harness.Run, dict]:
    """A run 3 person lane over one browser unit whose stored capture the real capture path made, with one file saved in the lane's folder."""
    from datetime import UTC, datetime

    from scripts.acquisition.tests.task_fixture import task_run
    from scripts.acquisition.tests.test_task_owners import storage
    from scripts.acquisition.tests.test_task_raw_store import STORED, stored_unit

    fixture, unit = stored_unit(monkeypatch, tmp_path, STORED, "a.csv")
    unit["disposition"] = "approved_manual"
    run = task_run(tmp_path, fixture, task_storage=storage(bucket))
    run.stored_root, run.materialize, run.person_lane = None, None, True
    run.lane_approval = run.queue_path.with_name("lane_approval.json")
    route.write_lane_template(run.queue_path, run.lane_approval)
    run.lane_approval.write_text(json.dumps(json.loads(run.lane_approval.read_text()) | {"approved_by": "synthetic_owner"}))
    run.manual_folder = tmp_path / "manual"
    run.manual_folder.mkdir()
    (run.manual_folder / "saved.csv").write_bytes(saved)
    monkeypatch.setattr(harness.download_metadata, "read_origin", lambda path: [unit["url"]])
    monkeypatch.setattr(harness.download_metadata, "download_created_at", lambda path: datetime.now(UTC).isoformat())
    return run, unit


def bucket_bodies(bucket: Any) -> dict[str, bytes]:
    return {key: data for key, (_, data, _) in bucket.objects.items()}


def test_a_browser_file_that_passes_is_stored_under_raw_with_a_task_shaped_receipt_and_its_record(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, raw_route: None
) -> None:
    from scripts.acquisition.tests.test_task_owners import TaskS3
    from scripts.acquisition.tests.test_task_raw_store import STORED

    bucket = TaskS3()
    run, unit = browser_run(monkeypatch, tmp_path, bucket, STORED)
    summary = harness.execute(run)
    check(summary["outcomes"] == {"exact_match": 1}, f"the saved file matched: {summary}")
    bodies = bucket_bodies(bucket)
    check(all(key.startswith("raw/") for key in bodies), f"every key is under raw/: {sorted(bodies)}")
    check(any(body == STORED for body in bodies.values()), "the saved file is stored")
    record = harness.outcome(run, unit["snapshot_id"]) or {}
    receipt = record["stored"]["receipt"]
    check(receipt["key"] in bodies and set(receipt) >= {"key", "version_id", "sha256"}, f"the outcome names the stored receipt: {receipt}")
    stored = json.loads(bodies[receipt["key"]])
    lineage = json.loads(stored["lineage"]["extraction_or_query"])
    check(
        lineage["fresh_of"] == unit["snapshot_id"] and stored["acquisition"]["requested_url"] == unit["url"], "the receipt has the task's fresh-capture shape"
    )
    check(all(item["key"] in bodies for item in record["stored"]["manifests"]), "the manifests are stored")
    queue_sha = hashlib.sha256(run.queue_path.read_bytes()).hexdigest()
    check(
        bodies[f"raw/run3_person/{queue_sha}/outcomes/{unit['snapshot_id']}.json"]
        == (run.state_root / "outcomes" / f"{unit['snapshot_id']}.json").read_bytes(),
        "the record is stored",
    )
    person = json.loads(bodies[f"raw/run3_runs/{queue_sha}/person.json"])
    check(person["outcomes"][unit["snapshot_id"]]["receipt"] == receipt, "the run record names the stored file's receipt")
    check(harness.report(run)["s3_writes"] == 1, "the report counts the stored capture")


def test_a_browser_file_whose_store_fails_counts_for_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, raw_route: None) -> None:
    from scripts.acquisition.tests.test_task_owners import TaskS3
    from scripts.acquisition.tests.test_task_raw_store import STORED

    bucket = TaskS3(refuse_puts=True)
    run, unit = browser_run(monkeypatch, tmp_path, bucket, STORED)
    # The lane's record save is refused too, and that stops the lane visibly; a repeat saves what is still unsaved.
    with pytest.raises(StorageError, match="AccessDenied"):
        harness.execute(run)
    record = harness.outcome(run, unit["snapshot_id"]) or {}
    check(record.get("outcome") == "blocked" and "stored" not in record, f"an unstored file is blocked: {record}")
    queue_sha = hashlib.sha256(run.queue_path.read_bytes()).hexdigest()
    check(f"raw/run3_runs/{queue_sha}/person.json" not in bucket.objects, "an unstored unit leaves the lane incomplete")
    check(harness.report(run)["s3_writes"] == 0, "the report counts no stored capture")


def test_the_hud_workbook_adapter_stores_through_the_lane_storage(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from scripts.acquisition import store_hud_xlsx
    from scripts.acquisition.tests.test_task_owners import TaskS3, storage

    calls: list[tuple] = []

    def execute(*arguments: Any) -> dict:
        calls.append(arguments)
        return {"receipt_path": str(tmp_path / "receipt.json")}

    monkeypatch.setattr(store_hud_xlsx, "execute", execute)
    monkeypatch.setattr(harness, "planned", lambda unit, run: ({"id": "p"}, {"id": "b", "file_name": "ZIP-COUNTY_032010.xlsx"}))
    monkeypatch.setattr(harness, "manual_file", lambda folder, name, started: tmp_path / name)
    chosen = storage(TaskS3())
    run = harness.Run(tmp_path / "q.json", tmp_path / "state", base=tmp_path, person_lane=True, task_storage=chosen)
    run.cache["started_at"] = None
    harness.adapt_hud_xlsx({"mode": "hud_xlsx", "snapshot_id": "HUD_XLSX__x"}, run)
    check(calls[0][4] is True and calls[0][5] is chosen, "the workbook is stored through the lane's storage client")


def reshaped_receipt(run: harness.Run, unit: dict, extra: list[tuple[str, str]], first_role: str) -> dict:
    """The stored receipt of the fixture unit with another first role and extra artifacts, as a reference or WONDER capture holds them."""
    stored = json.loads(harness.receipt_file(unit, run).read_text())
    first = stored["artifacts"][0] | {"role": first_role}
    added = [
        first | {"storage_path": path, "stored_file_name": Path(path).name, "role": role, "artifact_id": f"{first['artifact_id']}_{index}"}
        for index, (path, role) in enumerate(extra, 2)
    ]
    return stored | {"artifacts": [first, *added]}


def stored_receipt_body(bucket: Any, record: dict) -> dict:
    return json.loads(bucket_bodies(bucket)[record["stored"]["receipt"]["key"]])


def test_a_reference_page_is_stored_with_its_reference_role_and_a_fresh_download_proof(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, raw_route: None
) -> None:
    from scripts.acquisition.tests.test_task_owners import TaskS3
    from scripts.acquisition.tests.test_task_raw_store import STORED

    bucket = TaskS3()
    run, unit = browser_run(monkeypatch, tmp_path, bucket, STORED)
    reference = reshaped_receipt(run, unit, [("evidence/download_proof.json", "export_receipt")], "dictionary")
    monkeypatch.setattr(harness, "stored_receipt", lambda unit, run: reference)
    harness.execute(run)
    record = harness.outcome(run, unit["snapshot_id"]) or {}
    check("stored" in record, f"the reference page is stored: {record}")
    stored = stored_receipt_body(bucket, record)
    roles = {item["storage_path"]: item["role"] for item in stored["artifacts"]}
    check(set(roles.values()) == {"dictionary", "export_receipt"} and roles["evidence/download_proof.json"] == "export_receipt", f"roles: {roles}")
    proof_key = next(key for key in bucket_bodies(bucket) if key.endswith("/download_proof.json"))
    proof = json.loads(bucket_bodies(bucket)[proof_key])
    check(proof["origin_url"] == unit["url"] and proof["sha256"] == hashlib.sha256(STORED).hexdigest(), f"the proof is this save's: {proof}")
    check(proof["created_at_utc"] == stored["acquisition"]["retrieved_at_utc"], "the proof and the receipt name the same save time")


def test_a_fresh_wonder_export_is_stored_as_a_fresh_capture_with_its_derived_csv_and_the_plan_comparison_recorded(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, raw_route: None
) -> None:
    from scripts.acquisition import wonder_export_contract
    from scripts.acquisition.tests.test_task_owners import TaskS3
    from scripts.acquisition.tests.test_task_raw_store import STORED

    bucket = TaskS3()
    run, unit = browser_run(monkeypatch, tmp_path, bucket, STORED)
    batch = {"id": "b" * 64, "database": "D76", "file_name": "saved.csv", "sha256": "0" * 64, "bytes": 1, "years": [2020, 2020]}
    plan = {"batches": [batch]}
    wonder = reshaped_receipt(
        run, unit, [("derived/county_year.csv", "data"), ("evidence/download_proof.json", "export_receipt"), ("references/scope.json", "layout")], "data"
    )
    monkeypatch.setattr(harness, "stored_receipt", lambda unit, run: wonder)
    monkeypatch.setattr(harness, "planned", lambda unit, run: (plan, batch))
    monkeypatch.setattr(harness, "manual_file", lambda folder, name, started: run.manual_folder / name)
    monkeypatch.setattr(wonder_export_contract, "sanitize_origins", lambda origins, database: None)
    monkeypatch.setattr(wonder_export_contract, "validate_export", lambda raw, batch, plan: (b"county,year\n01001,2020\n", {"query": "q", "rows": 1}))
    monkeypatch.setattr(wonder_export_contract, "stored_name", lambda batch: "a.csv")
    run.cache["started_at"] = None
    receipt_path = harness.adapt_wonder(unit | {"mode": "wonder_export"}, run)
    receipt = json.loads(receipt_path.read_text())
    roles = {item["storage_path"]: item["role"] for item in receipt["artifacts"]}
    check(
        roles == {"raw/a.csv": "data", "derived/county_year.csv": "data", "evidence/download_proof.json": "export_receipt", "references/scope.json": "layout"},
        f"the fresh capture holds the export, the derived CSV, its proof and the scope: {roles}",
    )
    check(json.loads(receipt["lineage"]["extraction_or_query"])["fresh_of"] == unit["snapshot_id"], "it is a fresh capture of the stored WONDER receipt")
    check((receipt_path.parent / "s3_collections_reconciliation.json").is_file(), "it is stored")
    check(any(body == STORED for body in bucket_bodies(bucket).values()), "the export bytes are stored")
    proof = json.loads((receipt_path.parent / "evidence/download_proof.json").read_text())
    check(proof["planned_sha256"] == "0" * 64 and proof["sha256"] == hashlib.sha256(STORED).hexdigest(), "the plan comparison is recorded, not required")


def create_once_bucket() -> Any:
    """A bucket stand-in that refuses a second create of a key, as the real one does with its conditional write."""
    from scripts.acquisition.tests.test_task_owners import TaskS3

    class CreateOnceS3(TaskS3):
        def put_object(self, **arguments: Any) -> dict:
            if arguments["Key"] in self.objects:
                raise self.missing("PutObject", "PreconditionFailed")
            return super().put_object(**arguments)

    return CreateOnceS3()


def interrupted_before_the_outcome(monkeypatch: pytest.MonkeyPatch, action: Any) -> None:
    """Run the action and stop it by a user interruption at the write of the first outcome record, after the capture was stored."""
    original = harness.write_once

    def crash(path: Path, body: bytes) -> Any:
        if path.parent.name == "outcomes":
            raise KeyboardInterrupt("synthetic interruption before the outcome was saved")
        return original(path, body)

    monkeypatch.setattr(harness, "write_once", crash)
    with suppress(KeyboardInterrupt):
        action()
    monkeypatch.setattr(harness, "write_once", original)


def capture_keys(bucket: Any) -> set[str]:
    """The stored objects of the capture itself, not the lane's own records."""
    return {key for key in bucket.objects if not key.startswith(("raw/run3_person/", "raw/run3_runs/"))}


@pytest.mark.parametrize("kind", ["browser file", "reference save"])
def test_a_resumed_lane_reuses_the_capture_an_interrupted_attempt_stored_and_records_the_outcome(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, raw_route: None, kind: str
) -> None:
    from scripts.acquisition.tests.test_task_raw_store import STORED

    bucket = create_once_bucket()
    run, unit = browser_run(monkeypatch, tmp_path, bucket, STORED)
    # A browser file keeps the creation time it had, so the resumed attempt names the same capture folder.
    monkeypatch.setattr(harness.download_metadata, "download_created_at", lambda _path: "2099-01-01T00:00:00+00:00")
    if kind == "reference save":
        reference = reshaped_receipt(run, unit, [("evidence/download_proof.json", "export_receipt")], "dictionary")
        monkeypatch.setattr(harness, "stored_receipt", lambda unit, run: reference)
    interrupted_before_the_outcome(monkeypatch, lambda: harness.execute(run))
    stored = capture_keys(bucket)
    check(bool(stored) and harness.outcome(run, unit["snapshot_id"]) is None, "the interruption came after the capture was stored and before its outcome")
    summary = harness.execute(run)
    record = harness.outcome(run, unit["snapshot_id"]) or {}
    check(summary["outcomes"] == {"exact_match": 1} and "stored" in record, f"the resumed lane did not record the stored capture: {record}")
    check(capture_keys(bucket) == stored, "the resumed attempt stored the capture again or stored another one")
    check(record["stored"]["receipt"]["key"] in bucket.objects, "the outcome names a receipt that is not stored")
    check(harness.report(run)["s3_writes"] == 1, "the report does not count the stored capture")


def test_a_resumed_lane_refuses_a_capture_folder_whose_file_is_not_the_saved_one(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, raw_route: None) -> None:
    from scripts.acquisition.tests.test_task_raw_store import STORED

    bucket = create_once_bucket()
    run, unit = browser_run(monkeypatch, tmp_path, bucket, STORED)
    monkeypatch.setattr(harness.download_metadata, "download_created_at", lambda _path: "2099-01-01T00:00:00+00:00")
    interrupted_before_the_outcome(monkeypatch, lambda: harness.execute(run))
    stored = capture_keys(bucket)
    leftover = next((run.state_root / "captures").glob("*/raw/*"))
    leftover.unlink()
    leftover.write_bytes(b"another file under the same name")
    harness.execute(run)
    record = harness.outcome(run, unit["snapshot_id"]) or {}
    check(record.get("outcome") == "blocked" and "stored" not in record, f"a changed file in the capture folder was reused: {record}")
    check(capture_keys(bucket) == stored, "a changed file was stored")


def test_a_fresh_wonder_export_is_reused_when_the_adapter_runs_again_after_an_interruption(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, raw_route: None
) -> None:
    from scripts.acquisition import wonder_export_contract
    from scripts.acquisition.tests.test_task_raw_store import STORED

    bucket = create_once_bucket()
    run, unit = browser_run(monkeypatch, tmp_path, bucket, STORED)
    monkeypatch.setattr(harness.download_metadata, "download_created_at", lambda _path: "2099-01-01T00:00:00+00:00")
    batch = {"id": "b" * 64, "database": "D76", "file_name": "saved.csv", "sha256": "0" * 64, "bytes": 1, "years": [2020, 2020]}
    wonder = reshaped_receipt(
        run, unit, [("derived/county_year.csv", "data"), ("evidence/download_proof.json", "export_receipt"), ("references/scope.json", "layout")], "data"
    )
    monkeypatch.setattr(harness, "stored_receipt", lambda unit, run: wonder)
    monkeypatch.setattr(harness, "planned", lambda unit, run: ({"batches": [batch]}, batch))
    monkeypatch.setattr(harness, "manual_file", lambda folder, name, started: run.manual_folder / name)
    monkeypatch.setattr(wonder_export_contract, "sanitize_origins", lambda origins, database: None)
    monkeypatch.setattr(wonder_export_contract, "validate_export", lambda raw, batch, plan: (b"county,year\n01001,2020\n", {"query": "q", "rows": 1}))
    monkeypatch.setattr(wonder_export_contract, "stored_name", lambda batch: "a.csv")
    run.cache["started_at"] = None
    first = harness.adapt_wonder(unit | {"mode": "wonder_export"}, run)
    stored = capture_keys(bucket)
    again = harness.adapt_wonder(unit | {"mode": "wonder_export"}, run)
    check(again == first, "the resumed adapter chose another capture")
    check(capture_keys(bucket) == stored, "the resumed adapter stored the export again")
