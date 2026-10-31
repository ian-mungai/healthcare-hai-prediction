from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from scripts.acquisition import transport
from scripts.acquisition.s3_store import AwsCli
from tests.support import check

SIGNED_SOURCE = next(iter(transport.SIGNED_REDIRECT_ROUTES))


def hosts_file(folder: Path, hosts: list[str]) -> Path:
    path = folder / "publisher_hosts.json"
    path.write_text(json.dumps({"hosts": sorted(hosts), "hosts_sha256": transport.hosts_digest(sorted(hosts))}))
    (folder / "hosts_approval.json").write_text(json.dumps({"hosts_sha256": transport.hosts_digest(sorted(hosts)), "approved_by": "owner"}))
    return path


def use_hosts(monkeypatch: pytest.MonkeyPatch, folder: Path, hosts: list[str]) -> None:
    monkeypatch.setenv(transport.APPROVED_HOSTS_VARIABLE, str(hosts_file(folder, hosts)))
    monkeypatch.setenv(transport.APPROVED_HOSTS_APPROVAL_VARIABLE, str(folder / "hosts_approval.json"))


class Request:
    full_url = "https://data.example.gov/file.csv"

    def get_method(self) -> str:
        return "GET"


def test_without_a_hosts_file_requests_are_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(transport.APPROVED_HOSTS_VARIABLE, raising=False)
    transport.HttpsRequest("https://data.example.gov/file.csv")


def test_a_request_to_a_host_off_the_file_is_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    use_hosts(monkeypatch, tmp_path, ["data.example.gov"])
    transport.HttpsRequest("https://data.example.gov/file.csv")
    with pytest.raises(transport.CaptureError, match="approved hosts"):
        transport.HttpsRequest("https://elsewhere.example.org/file.csv")


def test_a_redirect_to_a_host_off_the_file_is_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    use_hosts(monkeypatch, tmp_path, ["data.example.gov"])
    handler = transport.ReviewedRedirects({"data.example.gov"}, source_url="https://data.example.gov/file.csv")
    with pytest.raises(transport.CaptureError, match="approved hosts"):
        handler.redirect_request(Request(), None, 302, "Found", {}, "https://elsewhere.example.org/file.csv")


def test_a_redirect_to_a_listed_host_other_than_the_request_host_is_still_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    use_hosts(monkeypatch, tmp_path, ["data.example.gov", "other.example.gov"])
    handler = transport.ReviewedRedirects({"data.example.gov"}, source_url="https://data.example.gov/file.csv")
    with pytest.raises(transport.CaptureError, match="not been reviewed"):
        handler.redirect_request(Request(), None, 302, "Found", {}, "https://other.example.gov/file.csv")


def test_the_signed_redirect_host_must_be_on_the_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    use_hosts(monkeypatch, tmp_path, ["data.chhs.ca.gov"])
    handler = transport.ReviewedRedirects({"data.chhs.ca.gov"}, source_url=SIGNED_SOURCE)
    target = transport.SIGNED_REDIRECT_ROUTES[SIGNED_SOURCE]
    with pytest.raises(transport.CaptureError, match="approved hosts"):
        handler.redirect_request(Request(), None, 302, "Found", {}, target + "?X-Amz-Algorithm=AWS4-HMAC-SHA256")


@pytest.mark.parametrize("damage", ["missing", "not json", "wrong digest", "empty list"])
def test_an_unusable_hosts_file_refuses_every_request(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, damage: str) -> None:
    path = tmp_path / "publisher_hosts.json"
    if damage == "not json":
        path.write_text("{")
    elif damage == "wrong digest":
        path.write_text(json.dumps({"hosts": ["data.example.gov"], "hosts_sha256": "0" * 64}))
    elif damage == "empty list":
        path.write_text(json.dumps({"hosts": [], "hosts_sha256": transport.hosts_digest([])}))
    monkeypatch.setenv(transport.APPROVED_HOSTS_VARIABLE, str(path))
    monkeypatch.setenv(transport.APPROVED_HOSTS_APPROVAL_VARIABLE, str(tmp_path / "hosts_approval.json"))
    with pytest.raises(transport.CaptureError, match="hosts file"):
        transport.HttpsRequest("https://data.example.gov/file.csv")
    check(True, "refused")


def test_the_bls_request_is_refused_before_any_secret_read_when_its_host_is_off_the_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from scripts.acquisition import bls_api_transport

    class Client:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
            self.calls.append(operation)
            return {}

    use_hosts(monkeypatch, tmp_path, ["data.example.gov"])
    client = Client()
    with pytest.raises(transport.CaptureError, match="approved hosts"):
        bls_api_transport.request_live({"endpoint": "https://api.bls.gov/publicAPI/v2/timeseries/data/"}, {}, cast(AwsCli, client))
    check(client.calls == [], "no identity or secret call was made")


def test_the_raw_route_without_a_hosts_file_refuses_every_request(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(transport.APPROVED_HOSTS_VARIABLE, raising=False)
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    with pytest.raises(transport.CaptureError, match="requires an approved hosts file"):
        transport.HttpsRequest("https://data.example.gov/file.csv")


def test_the_root_layout_without_a_hosts_file_is_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(transport.APPROVED_HOSTS_VARIABLE, raising=False)
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "")
    transport.HttpsRequest("https://data.example.gov/file.csv")


@pytest.mark.parametrize("damage", ["no approval variable", "no approval file", "approval of another list", "approval without an owner"])
def test_a_hosts_file_without_its_owner_approval_refuses_every_request(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, damage: str) -> None:
    use_hosts(monkeypatch, tmp_path, ["data.example.gov"])
    approval = tmp_path / "hosts_approval.json"
    if damage == "no approval variable":
        monkeypatch.delenv(transport.APPROVED_HOSTS_APPROVAL_VARIABLE)
    elif damage == "no approval file":
        approval.unlink()
    elif damage == "approval of another list":
        approval.write_text(json.dumps({"hosts_sha256": "0" * 64, "approved_by": "owner"}))
    else:
        approval.write_text(json.dumps({"hosts_sha256": transport.hosts_digest(["data.example.gov"])}))
    with pytest.raises(transport.CaptureError, match="hosts file"):
        transport.HttpsRequest("https://data.example.gov/file.csv")
