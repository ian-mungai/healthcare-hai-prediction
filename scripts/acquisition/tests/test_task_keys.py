"""Owner decision 137: the keyed units run on the task. Each key reaches only its matching collector and leaves no trace."""

from __future__ import annotations

import http.client
import json
import logging
import traceback
from functools import partial
from pathlib import Path
from typing import Any, cast

import pytest

from scripts.acquisition import (
    api_keys,
    census_acs_api_contract,
    census_acs_detailed_contract,
    collect_census_acs_detailed,
    hud_api_contract,
    task_role,
    transport,
)
from scripts.acquisition import bls_api_transport as bls
from scripts.acquisition import census_acs_api_transport as census
from scripts.acquisition import hud_api_transport as hud
from scripts.acquisition import redownload as harness
from scripts.acquisition.run_redownload_e2e import Fixture
from scripts.acquisition.s3_store import AwsCli
from scripts.acquisition.tests.leak_check import carries, channel_leaks, tree_leaks
from scripts.acquisition.tests.task_fixture import task_run
from tests.support import check

ACCOUNT = "1" * 12
ROLE = "example_project_job_publisher_task_dev"
ENVIRONMENT = {"AWS_REGION": "us-west-2", "EXPECTED_ACCOUNT_ID": ACCOUNT, "TASK_ROLE_NAME": ROLE}
PLANTED = {name: f"planted{name.split('_')[0].upper()}value{'0123456789' * 2}" for name in ("bls_api_key", "census_api_key", "hud_api_key")}
KEY_OF_MODE = {"bls_api_v2": "bls_api_key", "census_acs_api": "census_api_key", "census_acs_detailed": "census_api_key", "hud_api": "hud_api_key"}
TASK_IDENTITY = {"Account": ACCOUNT, "Arn": f"arn:aws:sts::{ACCOUNT}:assumed-role/{ROLE}/session"}


@pytest.fixture(autouse=True)
def no_hosts_file_between_cases(monkeypatch: pytest.MonkeyPatch) -> None:
    """The route gate names the approved hosts file in the environment. Setting them here first makes the test's teardown restore them."""
    for name in (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE):
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)


class Cloud:
    """Stand-ins for the two AWS clients a key read uses. Real AWS is never reached."""

    def __init__(self, identity: dict | None = None, values: dict | None = None, failure: Exception | None = None) -> None:
        self.identity, self.values, self.failure = identity or TASK_IDENTITY, PLANTED if values is None else values, failure
        self.reads: list[str] = []

    def factory(self, service: str, region: str) -> Any:
        check(service in {"sts", "secretsmanager"} and region == "us-west-2", f"unexpected client {service} {region}")
        return self

    def get_caller_identity(self) -> dict:
        return self.identity

    def get_secret_value(self, SecretId: str) -> dict:
        self.reads.append(SecretId)
        if self.failure is not None:
            raise self.failure
        return {"SecretString": self.values[SecretId]}


def keys_for(cloud: Cloud, mode: str) -> task_role.TaskKeys:
    return task_role.TaskKeys(mode, ENVIRONMENT, cloud.factory)


@pytest.mark.parametrize("mode", sorted(KEY_OF_MODE))
def test_each_collector_reads_its_own_key_and_no_other(mode: str) -> None:
    cloud = Cloud()
    check(keys_for(cloud, mode).read_api_key(KEY_OF_MODE[mode]) == PLANTED[KEY_OF_MODE[mode]], "the matching key is returned")
    check(cloud.reads == [KEY_OF_MODE[mode]], "one read of the matching secret")
    for other in sorted(set(PLANTED) - {KEY_OF_MODE[mode]}):
        with pytest.raises(api_keys.ApiKeyError, match="not the key of this collector"):
            keys_for(cloud, mode).read_api_key(other)
    check(cloud.reads == [KEY_OF_MODE[mode]], "another collector's secret is never requested")


def test_a_mode_without_a_key_gets_no_key_reader() -> None:
    with pytest.raises(api_keys.ApiKeyError, match="reads no key"):
        keys_for(Cloud(), "file")


@pytest.mark.parametrize(
    "identity",
    [
        {"Account": ACCOUNT, "Arn": f"arn:aws:iam::{ACCOUNT}:user/example_project_dev"},
        {"Account": ACCOUNT, "Arn": f"arn:aws:sts::{ACCOUNT}:assumed-role/other_role/session"},
        {"Account": "2" * 12, "Arn": f"arn:aws:sts::{'2' * 12}:assumed-role/{ROLE}/session"},
    ],
)
def test_no_key_is_read_under_any_identity_but_the_publisher_role(identity: dict) -> None:
    cloud = Cloud(identity=identity)
    with pytest.raises(ValueError, match="identity"):
        keys_for(cloud, "hud_api").read_api_key("hud_api_key")
    check(cloud.reads == [], "the secret was not requested")


@pytest.mark.parametrize("value", ["", "123456789e12345", None])
def test_an_unusable_secret_is_refused_without_its_value(value: Any) -> None:
    cloud = Cloud(values={"hud_api_key": value})
    with pytest.raises(api_keys.ApiKeyError) as caught:
        keys_for(cloud, "hud_api").read_api_key("hud_api_key")
    check(not value or value not in str(caught.value), "the message holds no value")


def test_a_provider_failure_carrying_the_key_becomes_a_fixed_message_with_no_chain() -> None:
    cloud = Cloud(failure=RuntimeError(f"provider said {PLANTED['hud_api_key']}"))
    with pytest.raises(api_keys.ApiKeyError) as caught:
        keys_for(cloud, "hud_api").read_api_key("hud_api_key")
    check(not carries(PLANTED["hud_api_key"], str(caught.value)), "the message holds no key")
    check(caught.value.__cause__ is None and caught.value.__suppress_context__, "the provider text is not chained into a traceback")


class Reply:
    def __init__(self, status: int, body: bytes, kind: str = "application/json") -> None:
        self.status, self.body, self.kind = status, body, kind

    def getheader(self, name: str, default: str = "") -> str:
        return self.kind if name == "Content-Type" else default

    def read(self, size: int = -1) -> bytes:
        body, self.body = self.body, b""
        return body


def connection_class(answer: Reply | Exception, sent: list[dict]) -> type:
    class Connection:
        def __init__(self, netloc: str, timeout: float) -> None:
            self.netloc = netloc

        def request(self, method: str, path: str, body: bytes | None = None, headers: dict | None = None) -> None:
            sent.append({"host": self.netloc, "path": path, "body": body or b"", "headers": headers or {}})

        def getresponse(self) -> Reply:
            if isinstance(answer, Exception):
                raise answer
            return answer

        def close(self) -> None:
            pass

    return Connection


DETAILED_BATCH = {"year": 2020, "table": sorted(census_acs_detailed_contract.UNIVERSE)[0]}
TRANSPORTS: dict[str, tuple[Any, dict, dict, bytes]] = {
    "bls_api_v2": (
        bls.request_live,
        {"endpoint": "https://api.bls.gov/publicAPI/v2/timeseries/data/", "credential_reference": "bls_api_key"},
        {"series_ids": ["S1"], "start_year": 2020, "end_year": 2020},
        b'{"status": "REQUEST_SUCCEEDED", "Results": {}}',
    ),
    "census_acs_api": (
        census.request_live,
        {"endpoint": census_acs_api_contract.ENDPOINT, "credential_reference": "census_api_key"},
        {"table": census_acs_api_contract.TABLES[0]},
        b'[["GEO_ID"], ["0500000US01001"]]',
    ),
    "census_acs_detailed": (
        collect_census_acs_detailed.live_request,
        {"credential_reference": "census_api_key"},
        DETAILED_BATCH,
        b'[["GEO_ID"], ["0500000US01001"]]',
    ),
    "hud_api": (
        hud.request_live,
        {"endpoint": hud_api_contract.ENDPOINT, "credential_reference": hud_api_contract.CREDENTIAL},
        {"type": sorted(hud_api_contract.CROSSWALKS)[0], "query": "All", "year": 2020, "quarter": 1},
        b'{"data": {"results": []}}',
    ),
}


def plant(monkeypatch: pytest.MonkeyPatch, answer: Reply | Exception) -> list[dict]:
    sent: list[dict] = []
    monkeypatch.setattr(http.client, "HTTPSConnection", connection_class(answer, sent))
    monkeypatch.delenv("HAI_CAPTURE_ROOT", raising=False)
    return sent


@pytest.mark.parametrize("mode", sorted(TRANSPORTS))
def test_a_real_transport_sends_its_key_only_to_its_publisher_and_returns_no_trace_of_it(monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    request, plan, batch, body = TRANSPORTS[mode]
    sent = plant(monkeypatch, Reply(200, body))
    cloud = Cloud()
    answer = request(plan, batch, cast(AwsCli, keys_for(cloud, mode)))
    key = PLANTED[KEY_OF_MODE[mode]]
    check(answer == body and not carries(key, answer), "the public reply comes back unchanged")
    check(cloud.reads == [KEY_OF_MODE[mode]], f"only the matching secret is read: {cloud.reads}")
    check(len(sent) == 1 and carries(key, str(sent[0])), "the key goes to the publisher in the one request")
    for other in sorted(set(PLANTED) - {KEY_OF_MODE[mode]}):
        check(not carries(PLANTED[other], str(sent[0])), f"{other} never reaches this publisher")


@pytest.mark.parametrize("mode", sorted(TRANSPORTS))
@pytest.mark.parametrize("kind", ["echoed reply", "error with the key", "network error with the key", "server failure", "html reply"])
def test_a_failing_request_raises_no_trace_of_the_key(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, mode: str, kind: str) -> None:
    request, plan, batch, _ = TRANSPORTS[mode]
    key = PLANTED[KEY_OF_MODE[mode]]
    answers: dict[str, Reply | Exception] = {
        "echoed reply": Reply(200, f'{{"status": "REQUEST_SUCCEEDED", "echo": "{key}", "data": ["{key}"]}}'.encode()),
        "error with the key": http.client.HTTPException(f"https://publisher.example.gov/?key={key}"),
        "network error with the key": OSError(f"connection to ?key={key} failed"),
        "server failure": Reply(503, key.encode()),
        "html reply": Reply(200, f"<html>{key}</html>".encode(), "text/html"),
    }
    plant(monkeypatch, answers[kind])
    caplog.set_level(logging.DEBUG)
    with pytest.raises((ValueError, OSError)) as caught:
        request(plan, batch, cast(AwsCli, keys_for(Cloud(), mode)))
    printed = "".join(traceback.format_exception(caught.value))
    check(channel_leaks(key, {"message": str(caught.value), "traceback": printed, "log": caplog.text}) == [], f"{mode} {kind} leaked the key")


def keyed_fixture(folder: Path, mode: str) -> Fixture:
    fixture = Fixture(folder)
    fixture.stored("ALPHA", "a.json", b'{"a": 1}', disposition="api", mode=mode)
    return fixture


def recorded_state(summary: dict, captured: Any, caplog: pytest.LogCaptureFixture) -> dict[str, bytes | str]:
    return {"summary": str(summary), "stdout": captured.out, "stderr": captured.err, "log": caplog.text}


@pytest.mark.parametrize("mode", sorted(KEY_OF_MODE))
def test_the_task_runs_a_keyed_unit_and_a_failure_with_the_key_in_its_text_leaves_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capfd: pytest.CaptureFixture, caplog: pytest.LogCaptureFixture, mode: str
) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    caplog.set_level(logging.DEBUG)
    cloud = Cloud()
    used: list[str] = []

    def adapter(unit: dict, run: harness.Run) -> Path:
        key = run.client(mode).read_api_key(KEY_OF_MODE[mode])
        used.append(mode)
        raise ValueError(f"the publisher refused {key}")

    run = task_run(
        tmp_path, keyed_fixture(tmp_path, mode), adapters={mode: adapter}, task_keys=partial(task_role.TaskKeys, environment=ENVIRONMENT, factory=cloud.factory)
    )
    summary = harness.execute(run)
    check(used == [mode] and summary["attempted"] == 1 and summary["outcomes"] == {"blocked": 1}, f"the task ran the unit: {summary}")
    check(cloud.reads == [KEY_OF_MODE[mode]], "only the matching secret is read")
    key = PLANTED[KEY_OF_MODE[mode]]
    check(tree_leaks(key, run.state_root) == [], f"the key reached {tree_leaks(key, run.state_root)}")
    captured = capfd.readouterr()
    check(channel_leaks(key, recorded_state(summary, captured, caplog)) == [], "the key reached a stream, the log or the summary")


def test_the_task_without_the_keys_flag_lists_a_keyed_unit_and_reads_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    fixture = keyed_fixture(tmp_path, "hud_api")
    run = task_run(tmp_path, fixture, adapters={"hud_api": lambda unit, run: pytest.fail("a keyed unit ran with the flag off")})
    summary = harness.execute(run)
    check(summary["attempted"] == 0 and summary["not_attempted"]["task_keyed"] == [fixture.entries[0]["snapshot_id"]], f"the unit is named: {summary}")


def stored_mirror_environment(tmp_path: Path, **extra: str) -> dict[str, str]:
    mirror = tmp_path / "mirror"
    mirror.mkdir(exist_ok=True)
    index = tmp_path / "index.json"
    index.write_text(json.dumps({"objects": {}, "bucket": "example"}))
    return ENVIRONMENT | {"HAI_STORED_MIRROR": str(mirror), "HAI_STORED_INDEX": str(index)} | extra


class Arguments:
    queue = Path("queue.json")
    state_root = Path("state")
    manual_folder = None
    removal_plan = removal_confirmation = hosts = hosts_approval = None


def mirror_factory(service: str, region: str) -> Any:
    return Cloud()


def test_the_command_line_gives_the_task_a_key_reader_only_when_the_flag_is_on(tmp_path: Path) -> None:
    off = harness.run_from(cast(Any, Arguments), stored_mirror_environment(tmp_path), mirror_factory)
    check(off.task_keys is None, "the keys flag is off by default")
    on = harness.run_from(cast(Any, Arguments), stored_mirror_environment(tmp_path, HAI_TASK_KEYS="1"), mirror_factory)
    check(on.task_keys is not None and isinstance(on.client("hud_api"), task_role.TaskKeys), "the flag gives the task a key reader")
    mac = harness.run_from(cast(Any, Arguments), {"HAI_TASK_KEYS": "1"}, mirror_factory)
    check(mac.task_keys is None, "without a mirror the flag means nothing")


@pytest.mark.parametrize("channel", ["summary", "stdout", "stderr", "log", "error"])
@pytest.mark.parametrize("form", [str, lambda key: key.upper().lower(), lambda key: f"key%3D{key}"])
def test_the_channel_check_finds_a_planted_key_in_every_channel(channel: str, form: Any) -> None:
    key = PLANTED["hud_api_key"]
    clean: dict[str, bytes | str] = dict.fromkeys(["summary", "stdout", "stderr", "log", "error"], "clean")
    check(channel_leaks(key, clean) == [], "clean channels pass")
    check(channel_leaks(key, clean | {channel: form(key)}) == [channel], f"the planted key in {channel} must fail the check")


@pytest.mark.parametrize(
    "name", ["outcomes/unit.json", "private/diagnostics/unit__01.json", "collectors/hud_api/batches/b/capture/receipt.json", "files/stored.csv"]
)
def test_the_tree_check_finds_a_planted_key_in_every_record(tmp_path: Path, name: str) -> None:
    key = PLANTED["hud_api_key"]
    path = tmp_path / name
    path.parent.mkdir(parents=True)
    path.write_text("clean")
    check(tree_leaks(key, tmp_path) == [], "a clean tree passes")
    path.write_text(f"prefix {key} suffix")
    check(tree_leaks(key, tmp_path) == [name], f"the planted key in {name} must fail the check")
    path.write_text("clean")
    named = path.with_name(key)
    named.write_text("clean")
    check(tree_leaks(key, tmp_path) == [str(named.relative_to(tmp_path))], "a planted key in a file name must fail the check")
