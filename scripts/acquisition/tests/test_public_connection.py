from __future__ import annotations

import socket
import ssl
import threading
import time
import urllib.request
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest

from scripts.acquisition import census_acs_api_transport as census
from scripts.acquisition import census_acs_tables_transport as tables
from scripts.acquisition import collect_mmd_api as mmd
from scripts.acquisition import transport
from scripts.acquisition.bls_api_transport import RetryableRequest
from scripts.acquisition.s3_store import AwsCli
from scripts.acquisition.tests.test_collector_hosts import MMD_URL, hosts
from tests.support import check


def answers(*addresses: str) -> list[tuple[Any, ...]]:
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 443)) for address in addresses]


class Recorder:
    def __init__(self, monkeypatch: pytest.MonkeyPatch, *addresses: str) -> None:
        self.connected: list[tuple[str, int]] = []
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


HEADERS = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000000\r\n\r\n"
DEADLINE = 1.0
ALLOWED_SECONDS = 3.5
SENDER_SECONDS = 9.0


class Publisher:
    """A publisher on a real local connection that sends one byte every ``pause`` seconds: its headers, or its body after quick headers. It stops after
    SENDER_SECONDS, so a transport with no total bound ends the case instead of hanging it."""

    def __init__(self, pause: float, slow_part: str) -> None:
        self.pause, self.slow_part = pause, slow_part
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self.serve, name="slow_publisher", daemon=True)
        self.thread.start()

    def drip(self, connection: socket.socket, data: bytes) -> bool:
        end = time.monotonic() + SENDER_SECONDS
        for index in range(len(data)):
            if time.monotonic() >= end:
                return False
            try:
                connection.sendall(data[index : index + 1])
            except OSError:
                return False
            time.sleep(self.pause)
        return True

    def serve(self) -> None:
        try:
            connection, _ = self.listener.accept()
        except OSError:
            return
        with connection:
            try:
                connection.recv(65536)
            except OSError:
                return
            if self.slow_part == "headers":
                self.drip(connection, HEADERS)
                return
            connection.sendall(HEADERS)
            self.drip(connection, b"[" + b"0," * 100_000)

    def close(self) -> None:
        self.listener.close()
        self.thread.join(timeout=SENDER_SECONDS + 5)


@pytest.fixture
def publisher_for(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """Point every connection of the transport at a local slow publisher, with the name and TLS steps of the transport left as they run."""
    started: list[Publisher] = []

    def start(pause: float, slow_part: str) -> Publisher:
        publisher = Publisher(pause, slow_part)
        started.append(publisher)
        monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: answers("93.184.216.34"))

        def to_the_publisher(address: tuple[str, int], timeout: float | None = None, source_address: Any = None) -> socket.socket:
            """Dial the local publisher, never the public address that the name lookup stand-in answered."""
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect(("127.0.0.1", publisher.port))
            return sock

        monkeypatch.setattr(socket, "create_connection", to_the_publisher)

        class PassThrough:
            def wrap_socket(self, sock: socket.socket, **_options: Any) -> socket.socket:
                return sock

        monkeypatch.setattr(ssl, "create_default_context", PassThrough)
        monkeypatch.delenv(transport.APPROVED_HOSTS_VARIABLE, raising=False)
        monkeypatch.delenv(transport.CAPTURE_ROOT_VARIABLE, raising=False)
        return publisher

    yield start
    for publisher in started:
        publisher.close()


class KeyClient:
    """A task's key reader, so the keyed requests need no identity call."""

    def read_api_key(self, name: str) -> str:
        return "abcdefghijklmnop1234567890ABCDEF"


@pytest.mark.parametrize("slow_part", ["headers", "body"])
def test_a_download_from_a_slow_publisher_ends_at_its_deadline(publisher_for: Any, tmp_path: Path, slow_part: str) -> None:
    publisher_for(0.1, slow_part)
    limits = transport.Limits(attempts=1, timeout_seconds=30, max_seconds=DEADLINE)
    started = time.monotonic()
    result = transport.download("https://data.example.gov/file.json", tmp_path, "file.json", "json", "data", limits)
    elapsed = time.monotonic() - started
    check(elapsed < ALLOWED_SECONDS, f"the download ran {elapsed:.1f} s past a {DEADLINE} s deadline")
    check(not result.complete and result.failure == "TimeoutError", f"the failure is {result.failure}")


@pytest.mark.parametrize("slow_part", ["headers", "body"])
def test_the_census_tables_request_ends_at_its_deadline(publisher_for: Any, monkeypatch: pytest.MonkeyPatch, slow_part: str) -> None:
    publisher_for(0.1, slow_part)
    monkeypatch.setattr(tables, "REQUEST_SECONDS", DEADLINE)
    started = time.monotonic()
    with pytest.raises(RetryableRequest, match="network failure"):
        tables.bounded_get("https://api.census.gov/data/2023/acs/acs5/groups/B01001.json", "/data/2023/acs/acs5/groups/B01001.json", 8 * 1024**2, "dictionary")
    elapsed = time.monotonic() - started
    check(elapsed < ALLOWED_SECONDS, f"the request ran {elapsed:.1f} s past a {DEADLINE} s deadline")


@pytest.mark.parametrize("slow_part", ["headers", "body"])
def test_the_census_api_request_ends_at_its_deadline(publisher_for: Any, monkeypatch: pytest.MonkeyPatch, slow_part: str) -> None:
    publisher_for(0.1, slow_part)
    monkeypatch.setattr(census, "REQUEST_SECONDS", DEADLINE)
    started = time.monotonic()
    with pytest.raises(RetryableRequest, match="network failure"):
        census.request_json("https://api.census.gov/data/2009/acs/acs5/profile", {"get": "NAME"}, cast(AwsCli, KeyClient()))
    elapsed = time.monotonic() - started
    check(elapsed < ALLOWED_SECONDS, f"the request ran {elapsed:.1f} s past a {DEADLINE} s deadline")
