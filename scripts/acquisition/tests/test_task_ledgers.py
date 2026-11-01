"""Limits survive a task's end: the attempt and byte ledgers travel in the digest-bound state, and a restored ledger can only tighten them."""

from __future__ import annotations

import hashlib
import io
import json
import shutil
import tarfile
import time
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from scripts.acquisition import collection_layout, publisher_run, redownload_controls, restored_outcomes, s3_store, transport
from scripts.acquisition import redownload as harness
from scripts.acquisition.run_cms_owners_e2e import Response
from scripts.acquisition.tests.test_task_owners import TaskS3
from scripts.acquisition.tests.test_task_raw_store import STORED, file_run
from tests.support import check

QUEUE = "a" * 64


@pytest.fixture(autouse=True)
def raw_route(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(s3_store, "load_routes", collection_layout.load_routes)
    for name in (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE):
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")


class StateBucket:
    """The state prefix, with the boto3 call shape."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, **arguments: Any) -> dict:
        self.objects[arguments["Key"]] = arguments["Body"]
        return {"VersionId": "v1"}

    def get_object(self, **arguments: Any) -> dict:
        if arguments["Key"] not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "x"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[arguments["Key"]])}

    def list_objects_v2(self, **arguments: Any) -> dict:
        return {"Contents": [{"Key": key} for key in sorted(self.objects) if key.startswith(arguments["Prefix"])], "IsTruncated": False}


ENV = {"DATA_BUCKET_NAME": "b", "EXPECTED_ACCOUNT_ID": "1" * 12, "PUBLISHER_QUEUE_SHA256": QUEUE}


def replace_task(bucket: StateBucket, root: Path, parking: Path) -> None:
    """One real save and restore: the task's workspace ends and a new one starts from the saved state."""
    saved = publisher_run.save_state(bucket, root, ENV)
    time.sleep(0.002)
    shutil.move(root, parking / f"ended_{len(list(parking.iterdir()))}")
    publisher_run.restore_state(bucket, root, ENV | {"PUBLISHER_PRIOR_STATE": json.dumps(saved)})


def test_three_429_pauses_across_task_replacements_send_only_two_requests(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket, state, parking = TaskS3(), StateBucket(), tmp_path / "parking"
    parking.mkdir()
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    web = run.opener.handlers[1].web
    web.override[unit["url"]] = Response(b"", unit["url"], 429)
    counted: list[int] = []
    first = harness.execute(run)
    counted.append(web.requests)
    check(first["paused"] == [unit["source_id"]], f"the first request paused: {first}")
    for _cycle in range(2):
        replace_task(state, run.state_root, parking)
        again = harness.Run(
            run.queue_path,
            run.state_root,
            base=run.base,
            opener=run.opener,
            stored_root=run.stored_root,
            task_storage=run.task_storage,
            route=run.route,
            sleep=run.sleep,
        )
        harness.resume_source(again, unit["source_id"], "owner resume")
        harness.execute(again)
        counted.append(web.requests)
    check(counted == [1, 2, 2], f"requests after each task: {counted}")
    record = harness.outcome(again, unit["snapshot_id"]) or {}
    check(record.get("reason") == "attempt_or_disk_budget_exhausted", f"the third is refused: {record}")


class Body:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def read(self, size: int) -> bytes:
        return self.data[:size]


def boundary(root: Path, cap: int = 10) -> redownload_controls.Boundary:
    return redownload_controls.Boundary(root, root.parent, "unit", cap)


def test_a_ten_byte_limit_holds_across_a_task_replacement(tmp_path: Path) -> None:
    root, state, parking = tmp_path / "state", StateBucket(), tmp_path / "parking"
    parking.mkdir()
    check(boundary(root).read(Body(b"1234567"), 100) == b"1234567", "seven bytes in the first task")
    replace_task(state, root, parking)
    check(redownload_controls.charged(root) == 7, f"the charge survives: {redownload_controls.charged(root)}")
    check(boundary(root).read(Body(b"abcdefg"), 100) == b"abc", "the second task may read only what is left")
    replace_task(state, root, parking)
    with pytest.raises(redownload_controls.BudgetExceeded):
        boundary(root).read(Body(b"x"), 100)


def test_an_unsettled_charge_stays_whole_across_a_task_replacement(tmp_path: Path) -> None:
    root, state, parking = tmp_path / "state", StateBucket(), tmp_path / "parking"
    parking.mkdir()
    boundary(root).allocate(6, "received_bytes")
    replace_task(state, root, parking)
    check(redownload_controls.charged(root) == 6, "an interrupted read stays charged in full")


def test_the_same_key_keeps_counting_attempts_across_a_task_replacement(tmp_path: Path) -> None:
    root, state, parking = tmp_path / "state", StateBucket(), tmp_path / "parking"
    parking.mkdir()
    boundary(root).attempt("https://a.gov/x")
    replace_task(state, root, parking)
    boundary(root).attempt("https://a.gov/x")
    replace_task(state, root, parking)
    with pytest.raises(redownload_controls.BudgetExceeded):
        boundary(root).attempt("https://a.gov/x")


UNITS = {"unit": {"snapshot_id": "unit"}}
KEY = hashlib.sha256(b"k").hexdigest()


def budget(bytes_: Any, unit: str = "unit", kind: str = "received_bytes") -> bytes:
    return json.dumps({"bytes": bytes_, "kind": kind, "unit": unit}).encode()


GOOD = {
    ".budgets/000000001.json": budget(7),
    ".budgets/000000001.settled.json": b'{"bytes": 5}',
    ".budgets/000000002.json": budget(3),
    f".attempts/unit/{KEY}/001.json": b'{"reserved_at_utc": "2026-11-01T00:00:00+00:00"}',
}


def test_complete_ledgers_pass() -> None:
    restored_outcomes.check_ledgers(GOOD, {}, UNITS)


@pytest.mark.parametrize(
    "change",
    [
        {".budgets/000000002.json": budget(-3)},
        {".budgets/000000002.json": budget(True)},
        {".budgets/000000002.json": budget(3, unit="other")},
        {".budgets/000000002.json": budget(3, kind="other")},
        {".budgets/000000002.json": b"not json"},
        {".budgets/000000001.settled.json": b'{"bytes": 8}'},
        {".budgets/000000001.settled.json": b'{"bytes": -1}'},
        {".budgets/000000009.settled.json": b'{"bytes": 0}'},
        {".budgets/000000004.json": budget(1)},
        {f".attempts/other/{KEY}/001.json": b'{"reserved_at_utc": "2026-11-01T00:00:00+00:00"}'},
        {f".attempts/unit/{KEY}/003.json": b'{"reserved_at_utc": "2026-11-01T00:00:00+00:00"}'},
        {f".attempts/unit/{KEY}/001.json": b'{"reserved_at_utc": "yesterday"}'},
    ],
)
def test_a_ledger_that_could_loosen_a_limit_is_refused(change: dict) -> None:
    with pytest.raises(ValueError, match="ledger"):
        restored_outcomes.check_ledgers(GOOD | change, {}, UNITS)


def test_a_ledger_missing_its_first_entry_is_refused() -> None:
    files = {name: body for name, body in GOOD.items() if not name.startswith(".budgets/000000001")}
    with pytest.raises(ValueError, match="ledger"):
        restored_outcomes.check_ledgers(files, {}, UNITS)


def test_a_ledger_that_charged_less_than_the_outcomes_received_is_refused() -> None:
    outcomes = {"unit": {"outcome": "changed_needs_review", "fresh_bytes": 100}}
    with pytest.raises(ValueError, match="ledger"):
        restored_outcomes.check_ledgers(GOOD, outcomes, UNITS)


def test_a_claim_with_no_charge_for_its_unit_is_refused() -> None:
    with pytest.raises(ValueError, match="ledger"):
        restored_outcomes.check_ledgers({}, {"unit": {"outcome": "exact_match"}}, UNITS)


def test_removing_the_ledgers_from_a_real_state_stops_the_next_task(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = TaskS3()
    run, _unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    harness.execute(run)
    shutil.rmtree(run.state_root / ".budgets")
    again = harness.Run(
        run.queue_path, run.state_root, base=run.base, opener=run.opener, stored_root=run.stored_root, task_storage=run.task_storage, route=run.route
    )
    with pytest.raises(ValueError, match="ledger"):
        harness.execute(again)


def named(key: str, body: bytes) -> str:
    return json.dumps({"key": key, "version_id": "v1", "sha256": hashlib.sha256(body).hexdigest()})


def empty_state() -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz"):
        pass
    return buffer.getvalue()


def test_an_older_approved_state_cannot_roll_the_ledgers_back(tmp_path: Path) -> None:
    bucket = StateBucket()
    prefix = publisher_run.state_prefix(ENV)
    body = empty_state()
    bucket.objects[f"{prefix}20261101T000000000000Z_old.tar.gz"] = body
    bucket.objects[f"{prefix}20261101T010000000000Z_new.tar.gz"] = body
    with pytest.raises(publisher_run.PublisherError, match="latest"):
        publisher_run.restore_state(bucket, tmp_path / "state", ENV | {"PUBLISHER_PRIOR_STATE": named(f"{prefix}20261101T000000000000Z_old.tar.gz", body)})
    check(
        publisher_run.restore_state(bucket, tmp_path / "state", ENV | {"PUBLISHER_PRIOR_STATE": named(f"{prefix}20261101T010000000000Z_new.tar.gz", body)})
        is not None,
        "the latest restores",
    )


def test_naming_no_state_when_one_exists_cannot_reset_the_ledgers(tmp_path: Path) -> None:
    bucket = StateBucket()
    bucket.objects[f"{publisher_run.state_prefix(ENV)}20261101T000000000000Z_old.tar.gz"] = empty_state()
    with pytest.raises(publisher_run.PublisherError, match="latest"):
        publisher_run.restore_state(bucket, tmp_path / "state", ENV | {"PUBLISHER_PRIOR_STATE": ""})
    check(not (tmp_path / "state").exists(), "nothing was unpacked")


class Socket:
    def __init__(self, timeouts: list[float]) -> None:
        self.timeouts = timeouts

    def setsockopt(self, *_args: Any) -> None:
        return None

    def settimeout(self, value: float) -> None:
        self.timeouts.append(value)


class Context:
    def wrap_socket(self, sock: Any, server_hostname: str) -> Any:
        time.sleep(0.01)
        return sock


def phased(monkeypatch: pytest.MonkeyPatch, lookup_seconds: float, dial_seconds: float) -> list[float]:
    timeouts: list[float] = []

    def lookup(_host: str, port: int, **_options: Any) -> list:
        time.sleep(lookup_seconds)
        return [(2, 1, 6, "", ("8.8.8.8", port))]

    def dial(_address: Any, timeout: float, _source: Any = None) -> Socket:
        timeouts.append(timeout)
        time.sleep(dial_seconds)
        return Socket(timeouts)

    monkeypatch.setattr(transport.socket, "getaddrinfo", lookup)
    monkeypatch.setattr(transport.socket, "create_connection", dial)
    monkeypatch.setattr(transport.ssl, "create_default_context", Context)
    return timeouts


def test_one_allowance_covers_all_the_connection_phases_together(monkeypatch: pytest.MonkeyPatch) -> None:
    timeouts = phased(monkeypatch, 0.1, 0.1)
    transport.connect_within(transport.PublicHTTPSConnection("a.gov", timeout=60), 0.5)
    check(len(timeouts) == 2, f"dial and TLS each got a limit: {timeouts}")
    check(timeouts[0] <= 0.41 and timeouts[1] <= 0.31, f"each phase gets what is left: {timeouts}")


def test_an_allowance_spent_by_the_lookup_makes_no_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    timeouts = phased(monkeypatch, 0.05, 0.0)
    with pytest.raises(TimeoutError):
        transport.connect_within(transport.PublicHTTPSConnection("a.gov", timeout=60), 0.04)
    check(timeouts == [], "no connection was attempted")


def test_an_allowance_spent_by_the_dial_starts_no_tls(monkeypatch: pytest.MonkeyPatch) -> None:
    timeouts = phased(monkeypatch, 0.0, 0.1)
    with pytest.raises(TimeoutError):
        transport.connect_within(transport.PublicHTTPSConnection("a.gov", timeout=60), 0.05)
    check(len(timeouts) == 1, f"only the dial ran: {timeouts}")
