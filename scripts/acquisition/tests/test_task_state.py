"""State a task cannot trust: the prior state it restores, the BLS budget it spends and the outcomes it skips."""

from __future__ import annotations

import base64
import hashlib
import http.client
import io
import json
import tarfile
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError

from scripts.acquisition import bls_api_transport as bls
from scripts.acquisition import publisher_run, task_role, transport
from scripts.acquisition import redownload as harness
from scripts.acquisition.run_redownload_e2e import Fixture
from scripts.acquisition.s3_store import AwsCli, encoded_json
from scripts.acquisition.tests.task_fixture import task_run
from scripts.acquisition.tests.test_task_keys import ACCOUNT, ENVIRONMENT, PLANTED, TASK_IDENTITY, Reply, connection_class
from tests.support import check

BUCKET = "example-project-data-dev"
NOW = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
PREFIX = task_role.BLS_LEDGER_PREFIX
QUEUE = "a" * 64


@pytest.fixture(autouse=True)
def no_hosts_file_between_cases(monkeypatch: pytest.MonkeyPatch) -> None:
    """The route gate names the approved hosts file in the environment. Setting them here first makes the test's teardown restore them."""
    for name in (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE):
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)


def failure(operation: str, code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "synthetic"}}, operation)


class Ledger:
    """STS, Secrets Manager and a versioned bucket with the boto3 call shape. Create-once writes are conflicts, as in S3."""

    def __init__(self, earlier: int = 0, stale: int = 0) -> None:
        self.objects: dict[str, dict[str, Any]] = {}
        for index in range(stale + earlier):
            when = NOW - timedelta(hours=30) if index < stale else NOW - timedelta(hours=1)
            self.objects[f"{PREFIX}{index + 1:010d}.json"] = {"Body": b"{}", "LastModified": when}
        self.reads: list[str] = []
        self.fail_list = self.fail_put = False
        self.race: list[str] = []

    def factory(self, service: str, region: str) -> Any:
        return self

    def get_caller_identity(self) -> dict:
        return TASK_IDENTITY

    def get_secret_value(self, SecretId: str) -> dict:
        self.reads.append(SecretId)
        return {"SecretString": PLANTED[SecretId]}

    def list_objects_v2(self, **arguments: Any) -> dict:
        if self.fail_list:
            raise failure("ListObjectsV2", "AccessDenied")
        found = [{"Key": key, "LastModified": item["LastModified"]} for key, item in sorted(self.objects.items()) if key.startswith(arguments["Prefix"])]
        return {"Contents": found, "IsTruncated": False}

    def put_object(self, **arguments: Any) -> dict:
        if self.fail_put:
            raise failure("PutObject", "AccessDenied")
        check(arguments["IfNoneMatch"] == "*", "a ledger entry is created once")
        while self.race:
            self.objects[self.race.pop()] = {"Body": b"{}", "LastModified": NOW}
        if arguments["Key"] in self.objects:
            raise failure("PutObject", "PreconditionFailed")
        body = arguments["Body"] if isinstance(arguments["Body"], bytes) else arguments["Body"].read()
        self.objects[arguments["Key"]] = {"Body": body, "LastModified": NOW}
        return {"VersionId": "v1"}

    def get_object(self, **arguments: Any) -> dict:
        if arguments["Key"] not in self.objects:
            raise failure("GetObject", "NoSuchKey")
        return {"Body": io.BytesIO(self.objects[arguments["Key"]]["Body"]), "VersionId": arguments.get("VersionId", "v1")}


LIMIT = 450
BATCH = {"id": "batch", "series_ids": ["S1"], "start_year": 2020, "end_year": 2020}
PLAN = {"endpoint": "https://api.bls.gov/publicAPI/v2/timeseries/data/", "credential_reference": "bls_api_key", "request_limit_24h": LIMIT}
ENV = ENVIRONMENT | {"DATA_BUCKET_NAME": BUCKET}


def keys(cloud: Ledger) -> task_role.TaskKeys:
    return task_role.TaskKeys("bls_api_v2", ENV, cloud.factory, clock=lambda: NOW)


def entries(cloud: Ledger) -> list[str]:
    return sorted(key for key in cloud.objects if key.startswith(PREFIX))


def test_a_reservation_is_durable_before_the_request_and_a_kill_never_hands_it_back() -> None:
    cloud = Ledger(earlier=449)
    keys(cloud).reserve_request("first", LIMIT)
    check(len(entries(cloud)) == 450, "the reservation is stored before anything else")
    # A task killed here saves no state. A new task reads the same durable ledger.
    with pytest.raises(bls.QuotaPause):
        keys(cloud).reserve_request("second", LIMIT)
    check(len(entries(cloud)) == 450, "the second task reserved nothing and made no request")


def later_entries(cloud: Ledger, count: int, when: datetime) -> None:
    for index in range(count):
        cloud.objects[f"{PREFIX}{index + 2:010d}.json"] = {"Body": b"{}", "LastModified": when}


def test_a_reservation_counts_until_a_day_after_its_latest_possible_send() -> None:
    """The review's model: an entry reserved at T, sent 30 seconds later, with 449 others made after it. At T plus a day and a second it still counts."""
    cloud = Ledger()
    keys(cloud).reserve_request("first", LIMIT)
    later_entries(cloud, 449, NOW + timedelta(hours=1))
    client = task_role.TaskKeys("bls_api_v2", ENV, cloud.factory, clock=lambda: NOW + timedelta(hours=24, seconds=1))
    with pytest.raises(bls.QuotaPause):
        client.reserve_request("second", LIMIT)
    check(len(entries(cloud)) == 450, "the window still held 450 counted requests")


def test_a_reservation_leaves_the_window_only_after_the_send_limit_has_passed() -> None:
    cloud = Ledger()
    keys(cloud).reserve_request("first", LIMIT)
    later_entries(cloud, 449, NOW + timedelta(hours=1))
    client = task_role.TaskKeys("bls_api_v2", ENV, cloud.factory, clock=lambda: NOW + task_role.BLS_WINDOW + timedelta(seconds=1))
    client.reserve_request("second", LIMIT)
    check(len(entries(cloud)) == 451, "the first entry has left the window")
    check(timedelta(hours=24) + task_role.BLS_SEND_LIMIT + task_role.BLS_SEND_BOUND <= task_role.BLS_WINDOW, "the window covers the latest possible send")


def test_a_request_too_late_for_its_reservation_is_not_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[dict] = []
    monkeypatch.setattr(http.client, "HTTPSConnection", connection_class(Reply(200, b'{"status": "REQUEST_SUCCEEDED"}'), sent))
    monkeypatch.delenv("HAI_CAPTURE_ROOT", raising=False)
    now = [NOW]
    cloud = Ledger()
    reads = cloud.get_secret_value

    def slow(SecretId: str) -> dict:
        now[0] = NOW + task_role.BLS_SEND_LIMIT + timedelta(seconds=1)
        return reads(SecretId)

    monkeypatch.setattr(cloud, "get_secret_value", slow)
    client = task_role.TaskKeys("bls_api_v2", ENV, cloud.factory, clock=lambda: now[0])
    with pytest.raises(bls.QuotaPause):
        bls.request_live(PLAN, BATCH, cast(AwsCli, client))
    check(sent == [], "a late request stays unsent and its entry stays spent")
    check(len(entries(cloud)) == 1, "the entry remains")


class FakeSocket:
    def __init__(self, sent: list[bytes]) -> None:
        self.sent = sent
        self.timeouts: list[float] = []

    def setsockopt(self, *_args: Any) -> None:
        return None

    def settimeout(self, value: float) -> None:
        self.timeouts.append(value)

    def sendall(self, body: bytes) -> None:
        self.sent.append(body)

    def makefile(self, *_args: Any, **_kwargs: Any) -> io.BytesIO:
        return io.BytesIO(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 31\r\n\r\n{"status": "REQUEST_SUCCEEDED"}')

    def close(self) -> None:
        return None


class FakeContext:
    def __init__(self, on_wrap: Any = None) -> None:
        self.on_wrap = on_wrap

    def wrap_socket(self, sock: Any, **_: Any) -> Any:
        if self.on_wrap is not None:
            self.on_wrap()
        return sock


def real_path(monkeypatch: pytest.MonkeyPatch, on_lookup: Any = None, on_wrap: Any = None) -> tuple[list[bytes], list[Any]]:
    """The real connection class with the network underneath replaced, as the review's delayed lookup did."""
    sent: list[bytes] = []
    dialled: list[Any] = []

    def lookup(host: str, port: int, **_options: Any) -> list:
        if on_lookup is not None:
            on_lookup()
        return [(2, 1, 6, "", ("8.8.8.8", port))]

    def dial(address: Any, timeout: float, source: Any = None) -> FakeSocket:
        dialled.append(timeout)
        return FakeSocket(sent)

    monkeypatch.setattr(transport.socket, "getaddrinfo", lookup)
    monkeypatch.setattr(transport.socket, "create_connection", dial)
    monkeypatch.setattr(transport.ssl, "create_default_context", lambda: FakeContext(on_wrap))
    monkeypatch.delenv("HAI_CAPTURE_ROOT", raising=False)
    return sent, dialled


def test_a_slow_name_lookup_past_the_window_sends_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """The review's model: the lookup returns at T plus 11 minutes."""
    now = [NOW]
    sent, dialled = real_path(monkeypatch, on_lookup=lambda: now.__setitem__(0, NOW + timedelta(minutes=11)))
    client = task_role.TaskKeys("bls_api_v2", ENV, Ledger().factory, clock=lambda: now[0])
    with pytest.raises(bls.QuotaPause):
        bls.request_live(PLAN, BATCH, cast(AwsCli, client))
    check(sent == [], "nothing was sent after the window")


def test_a_slow_tls_handshake_past_the_window_sends_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    now = [NOW]
    sent, _dialled = real_path(monkeypatch, on_wrap=lambda: now.__setitem__(0, NOW + timedelta(minutes=6)))
    client = task_role.TaskKeys("bls_api_v2", ENV, Ledger().factory, clock=lambda: now[0])
    with pytest.raises(bls.QuotaPause):
        bls.request_live(PLAN, BATCH, cast(AwsCli, client))
    check(sent == [], "nothing was sent after the window")


def test_a_lookup_that_never_returns_gives_up_at_the_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    release = threading.Event()
    almost = NOW + task_role.BLS_SEND_LIMIT - timedelta(seconds=0.2)
    times = [NOW, almost]
    sent, dialled = real_path(monkeypatch, on_lookup=lambda: release.wait(30))
    client = task_role.TaskKeys("bls_api_v2", ENV, Ledger().factory, clock=lambda: times[-1] if client.reserved_at else NOW)
    started = time.monotonic()
    try:
        with pytest.raises(bls.RetryableRequest):
            bls.request_live(PLAN, BATCH, cast(AwsCli, client))
    finally:
        release.set()
    check(time.monotonic() - started < 10, "the wait ended at the deadline")
    check(sent == [] and dialled == [], "no connection was made")


def test_a_connection_in_time_uses_its_remaining_time_and_sends_with_a_bounded_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    sent, dialled = real_path(monkeypatch)
    client = task_role.TaskKeys("bls_api_v2", ENV, Ledger().factory, clock=lambda: NOW)
    bls.request_live(PLAN, BATCH, cast(AwsCli, client))
    check(len(sent) >= 1 and dialled[0] <= task_role.BLS_SEND_LIMIT.total_seconds(), f"sent {len(sent)}, connect timeout {dialled}")


def test_the_window_covers_the_latest_send() -> None:
    latest = task_role.BLS_SEND_LIMIT + timedelta(seconds=bls.SEND_SECONDS)
    check(timedelta(hours=24) + latest <= task_role.BLS_WINDOW, "an entry outlasts a day after its latest send")


def test_two_tasks_racing_for_the_last_slot_cannot_both_win() -> None:
    cloud = Ledger(earlier=449)
    cloud.race = [f"{PREFIX}{450:010d}.json"]
    with pytest.raises(bls.QuotaPause):
        keys(cloud).reserve_request("loser", LIMIT)
    check(len(entries(cloud)) == 450, "the loser saw the conflict, read again and stopped")


def test_a_race_below_the_limit_takes_the_next_slot() -> None:
    cloud = Ledger(earlier=10)
    cloud.race = [f"{PREFIX}{11:010d}.json"]
    keys(cloud).reserve_request("next", LIMIT)
    check(f"{PREFIX}{12:010d}.json" in cloud.objects, "the task takes the next slot after the conflict")


def test_entries_older_than_a_day_leave_the_window_by_the_time_the_store_gave_them() -> None:
    cloud = Ledger(earlier=449, stale=100)
    keys(cloud).reserve_request("ok", LIMIT)
    check(f"{PREFIX}{550:010d}.json" in cloud.objects, "the sequence runs on across the window")


@pytest.mark.parametrize("broken", ["fail_list", "fail_put"])
def test_a_ledger_that_cannot_be_read_or_written_stops_the_request(monkeypatch: pytest.MonkeyPatch, broken: str) -> None:
    sent: list[dict] = []
    monkeypatch.setattr(http.client, "HTTPSConnection", connection_class(Reply(200, b'{"status": "REQUEST_SUCCEEDED"}'), sent))
    monkeypatch.delenv("HAI_CAPTURE_ROOT", raising=False)
    cloud = Ledger()
    setattr(cloud, broken, True)
    with pytest.raises(bls.QuotaPause):
        bls.request_live(PLAN, BATCH, cast(AwsCli, keys(cloud)))
    check(sent == [] and cloud.reads == [], "no key was read and no request was sent")


def test_the_real_transport_reserves_before_it_reads_the_key(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[dict] = []
    monkeypatch.setattr(http.client, "HTTPSConnection", connection_class(Reply(200, b'{"status": "REQUEST_SUCCEEDED"}'), sent))
    monkeypatch.delenv("HAI_CAPTURE_ROOT", raising=False)
    cloud = Ledger()
    bls.request_live(PLAN, BATCH, cast(AwsCli, keys(cloud)))
    check(len(entries(cloud)) == 1 and len(sent) == 1, "one entry for one request")
    stored = cloud.objects[entries(cloud)[0]]["Body"]
    check(PLANTED["bls_api_key"].encode() not in stored, "the ledger holds no key")


def test_only_bls_has_a_shared_budget() -> None:
    cloud = Ledger()
    with pytest.raises(ValueError, match="budget"):
        task_role.TaskKeys("hud_api", ENV, cloud.factory).reserve_request("x", LIMIT)
    check(entries(cloud) == [], "nothing was reserved")


def state_archive(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, body in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(body)
            archive.addfile(info, io.BytesIO(body))
    return buffer.getvalue()


def prior(cloud: Ledger, body: bytes, name: str = "state.tar.gz") -> str:
    key = f"raw/run3_state/{QUEUE}/{name}"
    cloud.objects[key] = {"Body": body, "LastModified": NOW}
    return json.dumps({"key": key, "version_id": "v1", "sha256": hashlib.sha256(body).hexdigest()})


def prior_cloud(body: bytes) -> tuple[Ledger, str]:
    cloud = Ledger()
    return cloud, prior(cloud, body)


def environment(tmp_path: Path, prior_state: str = "") -> dict[str, str]:
    return ENV | {"PUBLISHER_QUEUE_SHA256": QUEUE, "PUBLISHER_PRIOR_STATE": prior_state, "PUBLISHER_WORKSPACE": str(tmp_path), "RUN_ID": "r"}


def test_a_first_task_restores_nothing_and_an_unnamed_state_is_never_unpacked(tmp_path: Path) -> None:
    check(publisher_run.restore_state(Ledger(), tmp_path / "state", environment(tmp_path)) is None, "no state at all means an empty start")
    cloud = Ledger()
    other = state_archive({"run.json": b"{}"})
    cloud.objects[f"raw/run3_state/{QUEUE}/zzzz_UNCHECKED.tar.gz"] = {"Body": other, "LastModified": NOW}
    with pytest.raises(publisher_run.PublisherError, match="latest"):
        publisher_run.restore_state(cloud, tmp_path / "state", environment(tmp_path))
    check(not (tmp_path / "state").exists(), "nothing was unpacked")


def test_only_the_approved_version_and_digest_are_restored(tmp_path: Path) -> None:
    cloud = Ledger()
    body = state_archive({"run.json": b"{}", "outcomes/unit.json": b"{}"})
    named = prior(cloud, body)
    check(publisher_run.restore_state(cloud, tmp_path / "state", environment(tmp_path, named)) == json.loads(named)["key"], "the approved state is restored")
    check((tmp_path / "state/outcomes/unit.json").is_file(), "its records come back")


def test_a_state_that_differs_from_its_approved_digest_is_refused(tmp_path: Path) -> None:
    cloud = Ledger()
    named = json.loads(prior(cloud, state_archive({"run.json": b"{}"})))
    named["sha256"] = "0" * 64
    with pytest.raises(publisher_run.PublisherError, match="digest"):
        publisher_run.restore_state(cloud, tmp_path / "state", environment(tmp_path, json.dumps(named)))
    check(not (tmp_path / "state").exists(), "nothing was unpacked")


@pytest.mark.parametrize(
    "change", [{"key": "raw/run3_state/other/state.tar.gz"}, {"key": "lakehouse/state.tar.gz"}, {"version_id": ""}, {"sha256": "short"}, {"extra": "x"}]
)
def test_an_unusable_prior_state_reference_is_refused(tmp_path: Path, change: dict) -> None:
    cloud = Ledger()
    named = json.loads(prior(cloud, state_archive({"run.json": b"{}"}))) | change
    with pytest.raises(publisher_run.PublisherError):
        publisher_run.restore_state(cloud, tmp_path / "state", environment(tmp_path, json.dumps(named)))


@pytest.mark.parametrize("name", ["files/fresh.csv", "private/diagnostics/u.json", "collectors/cms_owners_org/private_original/o.csv"])
def test_a_state_holding_anything_but_records_is_refused_even_with_the_right_digest(tmp_path: Path, name: str) -> None:
    cloud = Ledger()
    named = prior(cloud, state_archive({"run.json": b"{}", name: b"x"}))
    with pytest.raises(publisher_run.PublisherError, match="record"):
        publisher_run.restore_state(cloud, tmp_path / "state", environment(tmp_path, named))


def test_a_state_that_repeats_a_record_is_refused_even_with_the_right_digest(tmp_path: Path) -> None:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for body in (b"{}", b'{"outcome": "exact_match"}'):
            info = tarfile.TarInfo("outcomes/unit.json")
            info.size = len(body)
            archive.addfile(info, io.BytesIO(body))
    cloud, named = prior_cloud(buffer.getvalue())
    with pytest.raises(publisher_run.PublisherError, match="record"):
        publisher_run.restore_state(cloud, tmp_path / "state", environment(tmp_path, named))


def test_a_queue_that_differs_from_the_bound_digest_stops_before_any_state_is_read(tmp_path: Path) -> None:
    queue = tmp_path / "queue.json"
    queue.write_bytes(b"{}")
    with pytest.raises(publisher_run.PublisherError, match="queue"):
        publisher_run.require_bound_queue(queue, environment(tmp_path))
    publisher_run.require_bound_queue(queue, environment(tmp_path) | {"PUBLISHER_QUEUE_SHA256": hashlib.sha256(b"{}").hexdigest()})


def test_an_existing_plan_input_with_other_bytes_is_refused(tmp_path: Path) -> None:
    from scripts.acquisition import stored_mirror

    mirror = tmp_path / "mirror"
    (mirror / "data").mkdir(parents=True)
    (mirror / "data/a.csv").write_bytes(b"right")
    (mirror / stored_mirror.PLAN_INPUTS_NAME).write_text(json.dumps({"data/a.csv": hashlib.sha256(b"right").hexdigest()}))
    code = tmp_path / "code"
    (code / "data").mkdir(parents=True)
    (code / "data/a.csv").write_bytes(b"planted")
    with pytest.raises(publisher_run.PublisherError, match="plan input"):
        publisher_run.install_plan_inputs(mirror, code)


class VersionedS3(Ledger):
    """Serves a stored receipt by version through the task storage call shape."""

    def head_object(self, **arguments: Any) -> dict:
        raise failure("HeadObject", "404")

    def get_object(self, **arguments: Any) -> dict:
        found = self.objects.get(arguments["Key"])
        if found is None:
            raise failure("GetObject", "NoSuchKey")
        body = found["Body"]
        checksum = base64_sha(body)
        return {
            "Body": io.BytesIO(body),
            "VersionId": arguments["VersionId"],
            "ContentLength": len(body),
            "ChecksumSHA256": checksum,
            "ServerSideEncryption": "AES256",
        }


def base64_sha(body: bytes) -> str:
    return base64.b64encode(hashlib.sha256(body).digest()).decode()


def evidence(cloud: VersionedS3, body: bytes, key: str = "raw/x/y/manifests/m/manifest.json") -> dict:
    cloud.objects[key] = {"Body": body, "LastModified": NOW}
    return {"bucket": BUCKET, "key": key, "version_id": "v1", "sha256": hashlib.sha256(body).hexdigest(), "byte_count": len(body)}


def state_for(tmp_path: Path, cloud: VersionedS3, fields: dict) -> harness.Run:
    fixture = Fixture(tmp_path)
    unit = fixture.stored("ALPHA", "a.csv", b"a\n1\n")
    storage = task_role.TaskStorage(cloud, {"data_bucket_name": BUCKET, "expected_account_id": ACCOUNT, "aws_region": "us-west-2"})
    run = task_run(tmp_path, fixture, task_storage=storage)
    harness.execute(run, max_units=0)
    (run.state_root / "outcomes").mkdir(exist_ok=True)
    record = {"snapshot_id": unit["snapshot_id"], "source_id": "ALPHA", "route": unit["route"], "mode": "file", "recorded_at_utc": NOW.isoformat()} | fields
    (run.state_root / "outcomes" / f"{unit['snapshot_id']}.json").write_bytes(encoded_json(record))
    run.cache["unit"] = unit
    return run


@pytest.mark.parametrize(
    "fields",
    [
        {"outcome": "exact_match"},
        {
            "outcome": "exact_match",
            "stored": {"receipt": {"bucket": BUCKET, "key": "raw/x/none.json", "version_id": "v1", "sha256": "0" * 64, "byte_count": 2}, "manifests": []},
        },
        {"outcome": "changed_needs_review"},
    ],
    ids=["no evidence", "evidence of nothing", "changed with no evidence"],
)
def test_a_forged_outcome_stops_the_run_and_counts_for_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fields: dict) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    run = state_for(tmp_path, VersionedS3(), fields)
    with pytest.raises(ValueError, match="outcome"):
        harness.execute(run)


def test_an_outcome_whose_stored_receipt_differs_stops_the_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    cloud = VersionedS3()
    record = evidence(cloud, b"receipt") | {"sha256": "1" * 64}
    run = state_for(tmp_path, cloud, {"outcome": "exact_match", "stored": {"receipt": record, "manifests": [record]}})
    with pytest.raises(ValueError, match="outcome"):
        harness.execute(run)


def test_an_outcome_for_another_source_or_a_unit_outside_the_queue_stops_the_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    cloud = VersionedS3()
    record = evidence(cloud, b"receipt")
    run = state_for(tmp_path, cloud, {"outcome": "exact_match", "stored": {"receipt": record, "manifests": [record]}})
    (run.state_root / "outcomes" / "not_in_the_queue.json").write_bytes(
        encoded_json({"snapshot_id": "not_in_the_queue", "source_id": "ALPHA", "outcome": "blocked"})
    )
    with pytest.raises(ValueError, match="outcome"):
        harness.execute(run)


def test_a_blocked_outcome_needs_no_evidence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    run = state_for(tmp_path, VersionedS3(), {"outcome": "blocked", "reason": "collector_validation_failed"})
    check(harness.execute(run)["attempted"] == 0, "a blocked unit stays for the owner's review")


def test_the_task_needs_its_storage_client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    fixture = Fixture(tmp_path)
    fixture.stored("ALPHA", "a.csv", b"a\n1\n")
    run = task_run(tmp_path, fixture)
    run.task_storage = None
    with pytest.raises(ValueError, match="storage"):
        harness.execute(run)
