from __future__ import annotations

import socket
import urllib.request
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import collect_mmd_api as mmd
from scripts.acquisition import transport
from scripts.acquisition.tests.test_collector_hosts import MMD_URL, hosts
from tests.support import check


def answers(*addresses: str) -> list[tuple[Any, ...]]:
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 443)) for address in addresses]


class Recorder:
    def __init__(self, monkeypatch: pytest.MonkeyPatch, *addresses: str) -> None:
        self.connected: list[tuple[str, int]] = []
        self.server_names: list[str] = []
        monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: answers(*addresses))
        monkeypatch.setattr(socket, "create_connection", self.create)

    def create(self, address: tuple[str, int], *args: Any, **kwargs: Any) -> Any:
        self.connected.append(address)
        raise OSError("stand-in connection stops here")


@pytest.mark.parametrize("private", ["169.254.169.254", "10.0.0.7", "127.0.0.1", "192.168.1.1", "::1", "fd00::1"])
def test_a_name_that_resolves_to_a_private_address_is_refused_before_any_connection(monkeypatch: pytest.MonkeyPatch, private: str) -> None:
    recorder = Recorder(monkeypatch, private)
    connection = transport.PublicHTTPSConnection("data.example.gov", timeout=5)
    with pytest.raises(transport.CaptureError, match="non-public"):
        connection.connect()
    check(recorder.connected == [], "no connection was attempted")


def test_one_private_answer_among_public_ones_refuses_the_name(monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = Recorder(monkeypatch, "93.184.216.34", "169.254.169.254")
    with pytest.raises(transport.CaptureError, match="non-public"):
        transport.PublicHTTPSConnection("data.example.gov", timeout=5).connect()
    check(recorder.connected == [], "no connection was attempted")


def test_a_public_answer_is_the_address_the_connection_uses(monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = Recorder(monkeypatch, "93.184.216.34")
    with pytest.raises(OSError, match="stand-in"):
        transport.PublicHTTPSConnection("data.example.gov", timeout=5).connect()
    check(recorder.connected == [("93.184.216.34", 443)], "the validated address, not the name, is dialed")


def test_the_default_downloader_opener_refuses_a_private_answer_after_a_redirect_too(monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = Recorder(monkeypatch, "169.254.169.254")
    opener = urllib.request.build_opener(transport.PublicHTTPSHandler())
    with pytest.raises(transport.CaptureError, match="non-public"):
        opener.open(transport.HttpsRequest("https://data.example.gov/file.csv"), timeout=5)
    check(recorder.connected == [], "no connection was attempted")


def test_a_collector_connection_refuses_a_private_answer(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    hosts(monkeypatch, tmp_path, ["data.cms.gov"])
    recorder = Recorder(monkeypatch, "169.254.169.254")
    (tmp_path / "mmd").mkdir()
    with pytest.raises(transport.CaptureError, match="non-public"):
        mmd.fetch(tmp_path / "mmd", "page", MMD_URL, True)
    check(recorder.connected == [], "no connection was attempted")
