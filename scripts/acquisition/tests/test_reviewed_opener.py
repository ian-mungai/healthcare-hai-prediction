from __future__ import annotations

import io
import urllib.request
from email.message import Message
from pathlib import Path
from typing import Any
from urllib.response import addinfourl

import pytest

from scripts.acquisition import transport
from scripts.acquisition.tests.test_collector_hosts import hosts
from tests.support import check

URL = "https://data.example.gov/file.csv"
OFF_FILE = "https://elsewhere.example.org/file.csv"
LIMITS = transport.Limits(attempts=1)


class Answer(addinfourl):
    """An addinfourl with the message urllib's error processor reads."""

    msg = "Found"


class Publisher(urllib.request.BaseHandler):
    """An HTTPS stand-in that records every request urllib hands it and answers the first with a redirect."""

    handler_order = 100

    def __init__(self, location: str) -> None:
        self.location = location
        self.requests: list[str] = []

    def https_open(self, req: Any) -> Any:
        self.requests.append(req.full_url)
        headers = Message()
        headers["Location"] = self.location
        return Answer(io.BytesIO(b""), headers, req.full_url, 302)


def fetch(tmp_path: Path, opener: Any) -> Any:
    return transport.download(URL, tmp_path / "download", "file.csv", "csv", "data", LIMITS, opener=opener)


def test_a_plain_opener_is_refused_before_any_request_when_the_hosts_file_is_set(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    hosts(monkeypatch, tmp_path, ["data.example.gov"])
    publisher = Publisher(OFF_FILE)
    with pytest.raises(transport.CaptureError, match="reviewed"):
        fetch(tmp_path, urllib.request.build_opener(publisher))
    check(publisher.requests == [], "no request reached the publisher stand-in")


def test_a_plain_opener_is_refused_on_the_raw_route_even_without_a_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv(transport.APPROVED_HOSTS_VARIABLE, raising=False)
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    publisher = Publisher(OFF_FILE)
    with pytest.raises(transport.CaptureError):
        fetch(tmp_path, urllib.request.build_opener(publisher))
    check(publisher.requests == [], "no request reached the publisher stand-in")


def test_a_reviewed_transport_still_stops_a_redirect_to_a_host_off_the_file_before_it_is_requested(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    hosts(monkeypatch, tmp_path, ["data.example.gov"])
    publisher = Publisher(OFF_FILE)
    result = fetch(tmp_path, transport.ReviewedTransport(publisher))
    check(publisher.requests == [URL], "only the approved first URL was requested")
    check(result.complete is False and result.failure == "CaptureError", "the redirect is recorded as a refused capture")


def test_a_plain_opener_still_works_for_local_tests_without_a_hosts_file_or_raw_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv(transport.APPROVED_HOSTS_VARIABLE, raising=False)
    monkeypatch.delenv("HAI_CAPTURE_ROOT", raising=False)
    publisher = Publisher(URL)
    fetch(tmp_path, urllib.request.build_opener(publisher))
    check(publisher.requests != [], "the legacy local path is unchanged")
