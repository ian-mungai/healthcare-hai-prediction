from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, cast

import pytest

from scripts.acquisition import collection_layout as layout
from scripts.acquisition import publisher_run, s3_store, task_role, transport
from scripts.acquisition import redownload as harness
from scripts.acquisition.s3_store import AwsCli
from scripts.acquisition.single_copy import reference_scope
from scripts.acquisition.source_registry import RegistryError
from scripts.acquisition.tests.test_task_owners import TaskS3, storage
from scripts.acquisition.tests.test_task_raw_store import STORED, file_run
from scripts.acquisition.transport import CaptureError
from tests.support import check

ROUTE = {"publisher": "example", "collection": "collection"}
BUCKET = "bucket"
TEST_ROOT = "ingestion_test/ingestion_abcd1234/"
BAD_ROOTS = (
    "ingestion_test/",
    "ingestion_test/x/",
    "ingestion_test/ingestion_abcd1234",
    "ingestion_test/ingestion_abcd1234/raw/",
    "ingestion_test/20261112T220000Z-ABCD1234/",
    "ingestion_test/ingestion_abcd1234/\n",
    "Ingestion_test/ingestion_abcd1234/",
    "ingestion_tests/ingestion_abcd1234/",
    "raw/ingestion_test/ingestion_abcd1234/",
    "../raw/",
    "raw",
    "raw//",
)
ROLES = (("data", False), ("capture_receipt", False), ("dictionary", False), ("transport_audit", True))


@pytest.fixture(autouse=True)
def no_hosts_file_between_cases(monkeypatch: pytest.MonkeyPatch) -> None:
    """The route gate sets the hosts variables in the process; setting and removing them here makes the test end restore their absence."""
    for name in (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE):
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)


def test_without_a_capture_root_the_layout_is_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(layout.CAPTURE_ROOT_VARIABLE, raising=False)
    check(layout.object_prefix(ROUTE, "table", "data", "2026", "run", False) == "example/collection/datasets/table/release_date=2026", "root layout unchanged")


@pytest.mark.parametrize(("role", "evidence"), ROLES)
def test_the_raw_route_puts_every_new_prefix_under_raw(monkeypatch: pytest.MonkeyPatch, role: str, evidence: bool) -> None:
    monkeypatch.setenv(layout.CAPTURE_ROOT_VARIABLE, "raw/")
    prefix = layout.object_prefix(ROUTE, "table", role, "2026", "run", evidence)
    check(prefix.startswith("raw/example/collection/"), f"{role} prefix starts with raw/")


def test_a_capture_root_other_than_empty_or_raw_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(layout.CAPTURE_ROOT_VARIABLE, "elsewhere/")
    with pytest.raises(CaptureError):
        layout.object_prefix(ROUTE, "table", "data", "2026", "run", False)


class Stop(Exception):
    pass


class Client:
    def __init__(self) -> None:
        self.storage_policy: dict[str, Any] = {"enabled": False}
        self.configuration = {"data_bucket_name": BUCKET}

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
        raise Stop(operation)


def test_a_capture_never_writes_outside_raw(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(layout.CAPTURE_ROOT_VARIABLE, "raw/")
    path = tmp_path / "file.csv"
    path.write_bytes(b"a,b\n1,2\n")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    root_key = f"example/collection/datasets/table/release_date=2026/{digest}/file.csv"
    with pytest.raises(CaptureError, match="outside raw/"):
        s3_store.store_file(cast(AwsCli, Client()), path, root_key)
    with pytest.raises(Stop):
        s3_store.store_file(cast(AwsCli, Client()), path, f"raw/{root_key}")


def test_a_single_copy_reference_under_raw_has_the_same_scope_as_one_at_the_root() -> None:
    digest = "a" * 64
    tail = f"example/collection/datasets/table/release_date=2026/{digest}/file.csv"
    check(reference_scope(f"raw/{tail}", digest) == reference_scope(tail, digest) == "objects", "raw and root payload keys share a scope")
    audit = f"example/collection/audit/run/{digest}/file.json"
    check(reference_scope(f"raw/{audit}", digest) == reference_scope(audit, digest) == "evidence_objects", "raw and root evidence keys share a scope")


def test_a_test_folder_root_puts_every_new_prefix_under_it(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(layout.CAPTURE_ROOT_VARIABLE, TEST_ROOT)
    for role, evidence in ROLES:
        prefix = layout.object_prefix(ROUTE, "table", role, "2026", "run", evidence)
        check(prefix.startswith(f"{TEST_ROOT}example/collection/"), f"{role} prefix starts with the test folder")
    check(layout.capture_root() == TEST_ROOT, "the selected root is the test folder")


@pytest.mark.parametrize("root", BAD_ROOTS)
def test_only_raw_and_one_run_id_test_folder_are_valid_roots(monkeypatch: pytest.MonkeyPatch, root: str) -> None:
    monkeypatch.setenv(layout.CAPTURE_ROOT_VARIABLE, root)
    with pytest.raises(CaptureError):
        layout.capture_root()


def test_a_test_folder_capture_never_writes_outside_its_folder(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(layout.CAPTURE_ROOT_VARIABLE, TEST_ROOT)
    path = tmp_path / "file.csv"
    path.write_bytes(b"a,b\n1,2\n")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    tail = f"example/collection/datasets/table/release_date=2026/{digest}/file.csv"
    for outside in (tail, f"raw/{tail}", f"ingestion_test/ingestion_ffffffff/{tail}"):
        with pytest.raises(CaptureError, match="outside"):
            s3_store.store_file(cast(AwsCli, Client()), path, outside)
    with pytest.raises(Stop):
        s3_store.store_file(cast(AwsCli, Client()), path, f"{TEST_ROOT}{tail}")


def test_a_test_folder_route_still_requires_the_approved_hosts_file(monkeypatch: pytest.MonkeyPatch) -> None:
    for root, on_route in (("", False), ("raw/", True), (TEST_ROOT, True)):
        monkeypatch.setenv(layout.CAPTURE_ROOT_VARIABLE, root)
        check(transport.on_raw_route() is on_route, f"the AWS route flag for {root!r} is {on_route}")


def test_task_storage_reaches_only_the_selected_test_folder(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(layout.CAPTURE_ROOT_VARIABLE, TEST_ROOT)
    client = storage(TaskS3())
    path = tmp_path / "plain.csv"
    path.write_text("a")
    for key in ("raw/a", "other/a", "ingestion_test/ingestion_ffffffff/a", f"{task_role.BLS_LEDGER_PREFIX}0000000001.json"):
        with pytest.raises(task_role.StorageError, match="outside_raw_root"):
            client.call("s3api", "head-object", ["--key", key])
    with pytest.raises(task_role.StorageError, match="create_once_required"):
        client.call("s3api", "put-object", ["--key", f"{TEST_ROOT}a", "--body", str(path), "--if-none-match", "etag"])


def test_task_storage_on_raw_is_unchanged_by_the_test_folder_rule(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(layout.CAPTURE_ROOT_VARIABLE, "raw/")
    client = storage(TaskS3())
    with pytest.raises(task_role.StorageError, match="outside_raw_root"):
        client.call("s3api", "head-object", ["--key", f"{TEST_ROOT}a"])


@pytest.mark.parametrize("root", ["raw/", TEST_ROOT])
def test_the_run_state_follows_the_selected_root_and_the_bls_ledger_never_moves(root: str) -> None:
    environment = {"PUBLISHER_QUEUE_SHA256": "a" * 64, "RUN_ID": "ingestion_abcd1234", "HAI_CAPTURE_ROOT": root}
    check(publisher_run.state_prefix(environment) == f"{root}run3_state/{'a' * 64}/", "the state prefix follows the root")
    check(publisher_run.run_record_key(environment) == f"{root}run3_runs/{'a' * 64}/publisher/ingestion_abcd1234.json", "the run record follows the root")
    check(task_role.BLS_LEDGER_PREFIX == "raw/run3_ledger/bls/", "the BLS ledger stays at its one place for every root")


def test_the_run_state_defaults_to_raw_and_refuses_another_root() -> None:
    environment = {"PUBLISHER_QUEUE_SHA256": "a" * 64, "RUN_ID": "r"}
    check(publisher_run.state_prefix(environment).startswith("raw/run3_state/"), "no root selects raw/")
    for root in ("", "elsewhere/", "ingestion_test/"):
        with pytest.raises(publisher_run.PublisherError):
            publisher_run.state_prefix(environment | {"HAI_CAPTURE_ROOT": root})


def test_a_unit_stored_under_a_test_root_is_resumed_by_a_repeat_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    run, _unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    monkeypatch.setenv(layout.CAPTURE_ROOT_VARIABLE, TEST_ROOT)
    check(harness.execute(run)["outcomes"] == {"exact_match": 1}, "the first run matches")
    check(bool(bucket.objects) and all(key.startswith(TEST_ROOT) for key in bucket.objects), f"keys outside the test folder: {sorted(bucket.objects)}")
    stored = sorted(bucket.objects)
    harness.validate_restored(run)
    again = harness.execute(run)
    check(again["attempted"] == 0 and sorted(bucket.objects) == stored, f"the repeat run resumes the recorded unit and stores nothing new: {again}")


def test_a_restored_outcome_under_another_root_is_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    run, _unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    monkeypatch.setenv(layout.CAPTURE_ROOT_VARIABLE, TEST_ROOT)
    harness.execute(run)
    for root in ("raw/", "ingestion_test/ingestion_ffffffff/"):
        monkeypatch.setenv(layout.CAPTURE_ROOT_VARIABLE, root)
        with pytest.raises(RegistryError, match="outside"):
            harness.validate_restored(run)
