from __future__ import annotations

import json
import socket
from pathlib import Path
from typing import Any, cast

import pytest

from scripts.acquisition import census_acs_api_contract, compare_mmd_api, hud_api_contract, transport
from scripts.acquisition import census_acs_api_transport as census
from scripts.acquisition import collect_mmd_api as mmd
from scripts.acquisition import hud_api_transport as hud
from scripts.acquisition.s3_store import AwsCli
from tests.support import check

MMD_URL = "https://data.cms.gov/data-api/v1/mmd-tool/?a=1"


class Client:
    """A project client stand-in that records every call, so a refusal can be shown to come before any identity or secret read."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.configuration = {"expected_account_id": "synthetic_account", "aws_profile": "example", "aws_region": "us-west-2"}

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
        self.calls.append(f"{service}:{operation}")
        return {}


def hosts(monkeypatch: pytest.MonkeyPatch, folder: Path, names: list[str]) -> None:
    path = folder / "publisher_hosts.json"
    path.write_text(json.dumps({"hosts": sorted(names), "hosts_sha256": transport.hosts_digest(sorted(names))}))
    approval = folder / "hosts_approval.json"
    approval.write_text(json.dumps({"hosts_sha256": transport.hosts_digest(sorted(names)), "approved_by": "owner"}))
    monkeypatch.setenv(transport.APPROVED_HOSTS_VARIABLE, str(path))
    monkeypatch.setenv(transport.APPROVED_HOSTS_APPROVAL_VARIABLE, str(approval))


def no_network(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    attempts: list[str] = []

    def refuse(*args: Any, **kwargs: Any) -> Any:
        attempts.append(str(args[:1]))
        raise AssertionError("a connection was attempted")

    monkeypatch.setattr(socket, "getaddrinfo", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    return attempts


def requests(tmp_path: Path) -> dict[str, Any]:
    client = Client()
    return {
        "mmd": lambda: mmd.fetch(tmp_path / "mmd", "page", MMD_URL, True),
        "census": lambda: census.request_live(
            {"endpoint": census_acs_api_contract.ENDPOINT, "credential_reference": "census_api_key"}, {}, cast(AwsCli, client)
        ),
        "hud": lambda: hud.request_live({"endpoint": hud_api_contract.ENDPOINT, "credential_reference": hud_api_contract.CREDENTIAL}, {}, cast(AwsCli, client)),
        "compare": lambda: compare_mmd_api.api_bytes(tmp_path / "compare", 2020, MMD_URL, True),
        "client": client,
    }


@pytest.mark.parametrize("collector", ["mmd", "census", "hud", "compare"])
def test_a_collector_whose_host_is_off_the_hosts_file_is_refused_before_any_credential_or_connection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, collector: str
) -> None:
    hosts(monkeypatch, tmp_path, ["other.example.gov"])
    attempts = no_network(monkeypatch)
    (tmp_path / "mmd").mkdir()
    (tmp_path / "compare" / "api_raw").mkdir(parents=True)
    calls = requests(tmp_path)
    with pytest.raises(transport.CaptureError, match="approved hosts"):
        calls[collector]()
    check(calls["client"].calls == [] and attempts == [], "no identity call, secret read or connection")


@pytest.mark.parametrize("collector", ["mmd", "census", "hud", "compare"])
def test_a_collector_on_the_raw_route_without_a_hosts_file_is_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, collector: str) -> None:
    monkeypatch.delenv(transport.APPROVED_HOSTS_VARIABLE, raising=False)
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    attempts = no_network(monkeypatch)
    (tmp_path / "mmd").mkdir()
    (tmp_path / "compare" / "api_raw").mkdir(parents=True)
    calls = requests(tmp_path)
    with pytest.raises(transport.CaptureError, match="requires an approved hosts file"):
        calls[collector]()
    check(calls["client"].calls == [] and attempts == [], "no identity call, secret read or connection")


def test_no_collector_opens_a_publisher_connection_except_through_the_transport() -> None:
    offenders = []
    for path in sorted(Path(__file__).resolve().parents[1].glob("*.py")):
        if path.name in {"transport.py"} or path.name.startswith("run_"):
            continue
        if "http.client.HTTPSConnection(" in path.read_text(encoding="utf-8") or "urllib.request.urlopen(" in path.read_text(encoding="utf-8"):
            offenders.append(path.name)
    check(offenders == [], f"live requests must use transport.https_connection or transport.download: {offenders}")


def test_a_stand_in_connection_class_is_used_below_the_host_check(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    class Standin:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.args = args

    monkeypatch.setattr(transport.http.client, "HTTPSConnection", Standin)
    monkeypatch.delenv(transport.APPROVED_HOSTS_VARIABLE, raising=False)
    monkeypatch.delenv("HAI_CAPTURE_ROOT", raising=False)
    check(isinstance(transport.https_connection("data.example.gov", timeout=5), Standin), "the stand-in class receives the connection")
    hosts(monkeypatch, tmp_path, ["other.example.gov"])
    with pytest.raises(transport.CaptureError, match="approved hosts"):
        transport.https_connection("data.example.gov", timeout=5)
