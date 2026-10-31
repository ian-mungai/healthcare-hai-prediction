from __future__ import annotations

import csv
import hashlib
import io
import json
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import redownload as harness
from scripts.acquisition import stored_mirror
from scripts.acquisition.run_redownload_e2e import Web
from scripts.acquisition.transport import CaptureError
from tests.support import check

ACCOUNT = "synthetic_account"
ROLE = "project_job_publisher_task_dev"
BUCKET = "example-bucket"
URL = "https://publisher.example.org/file.csv"
STORED = b"id,value\n1,2\n"
FRESH = b"id,value\n1,3\n"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class Stored:
    """A synthetic local stored capture, its queue and the inventory row of its payload in S3."""

    def __init__(self, base: Path, payload: bytes = STORED) -> None:
        self.base, self.payload = base, payload
        self.snapshot = "S1__20260101T000000Z__abc"
        self.folder = base / "stored" / self.snapshot
        (self.folder / "raw").mkdir(parents=True)
        (self.folder / "raw" / "file.csv").write_bytes(payload)
        receipt = {
            "snapshot_id": self.snapshot,
            "artifacts": [{"role": "data", "stored_file_name": "file.csv", "storage_path": "raw/file.csv", "sha256": sha(payload), "byte_count": len(payload)}],
            "lineage": {"extraction_or_query": json.dumps({"mode": "file"})},
        }
        (self.folder / "receipt.json").write_bytes(json.dumps(receipt, sort_keys=True).encode())
        self.relative = f"stored/{self.snapshot}/receipt.json"
        self.unit = {
            "snapshot_id": self.snapshot,
            "source_id": "S1",
            "disposition": "direct_download",
            "mode": "file",
            "route": "publisher_url",
            "url": URL,
            "bytes": len(payload),
            "flags": [],
            "receipt": self.relative,
            "receipt_sha256": sha((self.folder / "receipt.json").read_bytes()),
        }
        self.queue = base / "queue.json"
        self.queue.write_text(json.dumps({"run": 2, "queue": [self.unit]}))
        self.inventory = base / "inventory.csv"
        self.key = f"raw/pub/coll/datasets/table/release_date=2026/{sha(payload)}/file.csv"
        self.version = "version_1"
        self.write_inventory([["S1", self.snapshot, self.key, self.version, sha(payload)]])

    def write_inventory(self, rows: list[list[str]]) -> None:
        with self.inventory.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["source_id", "snapshot_id", "s3_key", "s3_version_id", "sha256"])
            writer.writerows(rows)

    def build(self, out: Path) -> dict[str, Path]:
        stored_mirror.build(self.queue, self.base, self.inventory, out, BUCKET)
        return {"mirror": out / "mirror", "index": out / "stored_index.json", "manifest": out / "mirror_manifest.json"}


class Body(io.BytesIO):
    def iter_chunks(self, size: int = 1024 * 1024) -> Any:
        while chunk := self.read(size):
            yield chunk


class S3:
    """boto3's S3 client surface the mirror uses: get_object by exact key and version."""

    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = objects
        self.calls: list[dict[str, str]] = []

    def get_object(self, **request: str) -> dict[str, Any]:
        self.calls.append(request)
        data = self.objects[(request["Key"], request["VersionId"])]
        return {"Body": Body(data), "VersionId": request["VersionId"], "ContentLength": len(data)}


def test_the_builder_copies_receipts_and_indexes_each_stored_payload_by_exact_key_and_version(tmp_path: Path) -> None:
    stored = Stored(tmp_path)
    built = stored.build(tmp_path / "out")
    check((built["mirror"] / stored.relative).read_bytes() == (stored.folder / "receipt.json").read_bytes(), "the receipt is copied byte for byte")
    index = json.loads(built["index"].read_text())
    entry = index["objects"][f"stored/{stored.snapshot}/raw/file.csv"]
    check(entry == {"bucket": BUCKET, "key": stored.key, "version_id": stored.version, "sha256": sha(STORED), "byte_count": len(STORED)}, "payload entry")
    listed = {item["path"]: item for item in json.loads(built["manifest"].read_text())["files"]}
    check(listed[stored.relative]["sha256"] == sha((stored.folder / "receipt.json").read_bytes()), "the manifest lists the receipt with its digest")
    check(not (built["mirror"] / f"stored/{stored.snapshot}/raw/file.csv").exists(), "payload bytes are not copied, only indexed")


def test_building_twice_changes_nothing_and_a_changed_receipt_is_refused(tmp_path: Path) -> None:
    stored = Stored(tmp_path)
    first = stored.build(tmp_path / "out")
    before = first["index"].read_bytes()
    stored.build(tmp_path / "out")
    check(first["index"].read_bytes() == before, "a second build is identical")
    (stored.folder / "receipt.json").write_bytes(b"{}")
    with pytest.raises(CaptureError, match="receipt"):
        stored.build(tmp_path / "out2")


def test_a_payload_with_no_inventory_row_is_listed_as_unavailable_not_guessed(tmp_path: Path) -> None:
    stored = Stored(tmp_path)
    stored.write_inventory([["S1", stored.snapshot, "raw/pub/coll/datasets/t/r/" + "9" * 64 + "/other.csv", "version_9", "9" * 64]])
    built = stored.build(tmp_path / "out")
    index = json.loads(built["index"].read_text())
    check(index["objects"] == {} and index["unavailable"] == [f"stored/{stored.snapshot}/raw/file.csv"], "no row, no entry")


def test_materializing_fetches_the_exact_version_and_verifies_its_bytes(tmp_path: Path) -> None:
    stored = Stored(tmp_path / "src")
    built = stored.build(tmp_path / "out")
    client = S3({(stored.key, stored.version): STORED})
    mirror = stored_mirror.Mirror(built["mirror"], json.loads(built["index"].read_text()), client)
    target = built["mirror"] / f"stored/{stored.snapshot}/raw/file.csv"
    check(mirror.materialize(target) is True and target.read_bytes() == STORED, "the payload is written")
    check(client.calls == [{"Bucket": BUCKET, "Key": stored.key, "VersionId": stored.version}], "one read of the exact key and version")
    check(mirror.materialize(target) is True and len(client.calls) == 1, "a second call reads nothing")


def test_wrong_bytes_leave_no_file_and_an_unknown_path_is_not_fetched(tmp_path: Path) -> None:
    stored = Stored(tmp_path / "src")
    built = stored.build(tmp_path / "out")
    client = S3({(stored.key, stored.version): b"tampered"})
    mirror = stored_mirror.Mirror(built["mirror"], json.loads(built["index"].read_text()), client)
    target = built["mirror"] / f"stored/{stored.snapshot}/raw/file.csv"
    with pytest.raises(CaptureError, match="differ"):
        mirror.materialize(target)
    check(not target.exists() and list(target.parent.glob("*")) == [], "no partial file remains")
    check(mirror.materialize(built["mirror"] / "stored/unknown/raw/other.csv") is False, "a path with no entry is not fetched")
    with pytest.raises(CaptureError, match="outside"):
        mirror.materialize(tmp_path / "elsewhere.csv")


def test_the_task_identity_must_be_the_publisher_role_in_the_expected_account() -> None:
    class Sts:
        def __init__(self, arn: str, account: str = ACCOUNT) -> None:
            self.answer = {"Account": account, "Arn": arn}

        def get_caller_identity(self) -> dict[str, str]:
            return self.answer

    good = f"arn:aws:sts::{ACCOUNT}:assumed-role/{ROLE}/session"
    stored_mirror.require_task_identity(Sts(good), ACCOUNT, ROLE)
    for arn, account in (
        (f"arn:aws:sts::{ACCOUNT}:assumed-role/other_role/session", ACCOUNT),
        (f"arn:aws:iam::{ACCOUNT}:user/someone", ACCOUNT),
        (good, "another_account"),
    ):
        with pytest.raises(CaptureError, match="identity"):
            stored_mirror.require_task_identity(Sts(arn, account), ACCOUNT, ROLE)


def test_the_mirror_is_built_from_the_task_environment_only_after_the_identity_check(tmp_path: Path) -> None:
    stored = Stored(tmp_path / "src")
    built = stored.build(tmp_path / "out")
    environment = {
        "HAI_STORED_MIRROR": str(built["mirror"]),
        "HAI_STORED_INDEX": str(built["index"]),
        "EXPECTED_ACCOUNT_ID": ACCOUNT,
        "TASK_ROLE_NAME": ROLE,
        "AWS_REGION": "us-west-2",
    }
    made: list[str] = []

    class Sts:
        def get_caller_identity(self) -> dict[str, str]:
            return {"Account": ACCOUNT, "Arn": f"arn:aws:sts::{ACCOUNT}:assumed-role/{ROLE}/s"}

    def factory(service: str, region: str) -> Any:
        made.append(service)
        return Sts() if service == "sts" else S3({(stored.key, stored.version): STORED})

    mirror = stored_mirror.from_environment(environment, factory)
    check(made[0] == "sts" and "s3" in made, "the identity is checked before the S3 client is made")
    check(mirror.materialize(built["mirror"] / f"stored/{stored.snapshot}/raw/file.csv"), "the mirror reads through the task client")
    for name in environment:
        broken = {key: value for key, value in environment.items() if key != name}
        with pytest.raises(CaptureError, match="environment"):
            stored_mirror.from_environment(broken, factory)


def run_for(base: Path, stored: Stored, mirror: Any) -> harness.Run:
    web = Web()
    web.serve(URL, (200, FRESH))
    return harness.Run(queue_path=stored.queue, state_root=base / "run", base=base, opener=web, materialize=mirror.materialize if mirror else None)


def test_a_changed_file_is_compared_with_its_stored_payload_read_from_s3(tmp_path: Path) -> None:
    stored = Stored(tmp_path / "src")
    built = stored.build(tmp_path / "out")
    client = S3({(stored.key, stored.version): STORED})
    mirror = stored_mirror.Mirror(built["mirror"], json.loads(built["index"].read_text()), client)
    outcome = harness.file_unit(stored.unit, run_for(built["mirror"], stored, mirror))
    check(outcome["outcome"] == "changed_needs_review" and outcome["differing_data_files"], "the stored bytes were compared with the fresh ones")
    check(len(client.calls) == 1, "one stored payload was read")


def test_an_unchanged_file_needs_no_read_from_s3(tmp_path: Path) -> None:
    stored = Stored(tmp_path / "src", payload=FRESH)
    built = stored.build(tmp_path / "out")
    client = S3({})
    mirror = stored_mirror.Mirror(built["mirror"], json.loads(built["index"].read_text()), client)
    outcome = harness.file_unit(stored.unit, run_for(built["mirror"], stored, mirror))
    check(outcome["outcome"] == "exact_match" and client.calls == [], "an exact match is decided from the receipt digest alone")


def test_without_a_mirror_a_changed_file_is_still_reported_but_not_compared(tmp_path: Path) -> None:
    stored = Stored(tmp_path / "src")
    built = stored.build(tmp_path / "out")
    outcome = harness.file_unit(stored.unit, run_for(built["mirror"], stored, None))
    check(outcome["outcome"] == "changed_needs_review" and "differing_data_files" not in outcome, "no stored payload, no deep comparison")


def namespace(stored: Stored, **extra: Any) -> Any:
    import argparse

    values = {"queue": stored.queue, "state_root": None, "manual_folder": None, "removal_plan": None, "removal_confirmation": None}
    values |= {"hosts": None, "hosts_approval": None} | extra
    return argparse.Namespace(**values)


def environment_for(built: dict[str, Path]) -> dict[str, str]:
    return {
        "HAI_STORED_MIRROR": str(built["mirror"]),
        "HAI_STORED_INDEX": str(built["index"]),
        "EXPECTED_ACCOUNT_ID": ACCOUNT,
        "TASK_ROLE_NAME": ROLE,
        "AWS_REGION": "us-west-2",
    }


def test_the_run_gets_the_mirror_reader_only_when_the_task_environment_names_a_mirror(tmp_path: Path) -> None:
    stored = Stored(tmp_path / "src")
    built = stored.build(tmp_path / "out")

    class Sts:
        def get_caller_identity(self) -> dict[str, str]:
            return {"Account": ACCOUNT, "Arn": f"arn:aws:sts::{ACCOUNT}:assumed-role/{ROLE}/s"}

    def factory(service: str, region: str) -> Any:
        return Sts() if service == "sts" else S3({(stored.key, stored.version): STORED})

    plain = harness.run_from(namespace(stored), {}, factory)
    check(plain.materialize is None and plain.stored_root is None, "no mirror variable, no reader and no separate data root")
    mirrored = harness.run_from(namespace(stored), environment_for(built), factory)
    check(
        mirrored.materialize is not None and mirrored.stored_root == built["mirror"].resolve(), "the mirror variable gives the run its reader and its data root"
    )
    check(
        mirrored.data_root == mirrored.stored_root and mirrored.base == harness.REPO_ROOT, "receipts come from the mirror, code and config from the code root"
    )


def test_a_task_running_as_another_identity_gets_no_run(tmp_path: Path) -> None:
    stored = Stored(tmp_path / "src")
    built = stored.build(tmp_path / "out")

    class Sts:
        def get_caller_identity(self) -> dict[str, str]:
            return {"Account": ACCOUNT, "Arn": f"arn:aws:iam::{ACCOUNT}:user/someone"}

    with pytest.raises(CaptureError, match="identity"):
        harness.run_from(namespace(stored), environment_for(built), lambda service, region: Sts())


def recording_handle(attempts: list[str]) -> Any:
    def handle(unit: dict, run: Any) -> dict:
        attempts.append(unit["snapshot_id"])
        return {"outcome": "exact_match"}

    return handle


def key_unit(stored: Stored, mode: str, snapshot: str) -> dict:
    return stored.unit | {"snapshot_id": snapshot, "mode": mode, "source_id": mode.upper(), "route": "api"}


def test_units_that_need_an_api_key_are_reported_blocked_on_the_raw_route_and_never_attempted(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    stored = Stored(tmp_path / "src")
    keyed = [key_unit(stored, mode, f"K{n}") for n, mode in enumerate(sorted(harness.SECRET_MODES))]
    attempts: list[str] = []
    monkeypatch.setattr(harness, "handle", recording_handle(attempts))
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    queue = stored.base / "keyed.json"
    queue.write_text(json.dumps({"run": 2, "queue": keyed}))
    run = harness.Run(queue_path=queue, state_root=stored.base / "run", base=stored.base)
    run.cache["queue"] = keyed
    summary = harness.execute_locked(run, False, True, None, None, None)
    check(
        attempts == [] and sorted(summary["not_attempted"]["keyed_mac_lane"]) == sorted(unit["snapshot_id"] for unit in keyed),
        "key units are listed and not attempted",
    )


def test_the_root_layout_still_attempts_key_units(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    stored = Stored(tmp_path / "src")
    keyed = [key_unit(stored, "bls_api_v2", "K1")]
    attempts: list[str] = []
    monkeypatch.setattr(harness, "handle", recording_handle(attempts))
    monkeypatch.delenv("HAI_CAPTURE_ROOT", raising=False)
    run = harness.Run(queue_path=stored.queue, state_root=stored.base / "run", base=stored.base)
    run.cache["queue"] = keyed
    run.state_root.mkdir(parents=True)
    monkeypatch.setattr(harness, "in_reviewed_scope", lambda run, source: True)
    monkeypatch.setattr(harness, "acknowledged", lambda run: set())
    summary = harness.execute_locked(run, False, True, None, None, None)
    check("not_attempted" not in summary, "the layout without the raw root applies no placement rule")


def test_the_controls_and_receipt_paths_resolve_through_the_mirror_root(tmp_path: Path) -> None:
    stored = Stored(tmp_path / "src")
    built = stored.build(tmp_path / "out")
    mirror = built["mirror"]
    run = harness.Run(queue_path=stored.queue, state_root=tmp_path / "run", base=tmp_path / "code", stored_root=mirror)
    check(harness.receipt_file(stored.unit, run) == mirror / stored.relative, "the receipt is read from the mirror")
    check(harness.operational_key(mirror / stored.relative, run) == stored.relative, "an input in the mirror is keyed by its repository relative path")
    check(harness.operational_key(tmp_path / "code" / "config/x.json", run) == "config/x.json", "an input in the code root keeps its key")


def test_the_mirror_carries_the_queue_its_controls_and_every_frozen_input(tmp_path: Path) -> None:
    stored = Stored(tmp_path / "src")
    frozen = stored.base / "stored" / "frozen_input.json"
    frozen.write_bytes(b'{"plan": 1}')
    document = json.loads(stored.queue.read_text())
    document["inputs_sha256"] = {"stored/frozen_input.json": sha(b'{"plan": 1}'), "notes.md": "0" * 64}
    stored.queue.write_text(json.dumps(document))
    for name in ("queue.lock.json", "controls.json", "independent_review.json", "scope.json"):
        (stored.base / name).write_text(json.dumps({"name": name}))
    built = stored.build(tmp_path / "out")
    mirror = built["mirror"]
    present = {path.relative_to(mirror).as_posix() for path in mirror.rglob("*") if path.is_file()}
    check(
        {"queue.json", "queue.lock.json", "controls.json", "independent_review.json", "scope.json", "stored/frozen_input.json"} <= present,
        f"mirror files {present}",
    )
    check("notes.md" not in present, "mutable status documents are not carried")


def test_a_frozen_input_that_changed_after_the_queue_was_frozen_is_refused(tmp_path: Path) -> None:
    stored = Stored(tmp_path / "src")
    (stored.base / "stored" / "frozen_input.json").write_bytes(b"changed")
    document = json.loads(stored.queue.read_text())
    document["inputs_sha256"] = {"stored/frozen_input.json": sha(b"original")}
    stored.queue.write_text(json.dumps(document))
    with pytest.raises(CaptureError, match="frozen input"):
        stored.build(tmp_path / "out")
