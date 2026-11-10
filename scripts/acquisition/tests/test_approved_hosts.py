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


# The one reviewed unsigned redirect route (owner decision "CHHS S3 redirect route for Run 3"): data.chhs.ca.gov to the CHHS open-data bucket prefix.
# The cases build every URL from the routes table, so the bucket name is spelled only there.
CHHS_SOURCE = "https://data.chhs.ca.gov/dataset/example/resource/0001/download/table.csv"
CHHS_PREFIX = transport.UNSIGNED_REDIRECT_ROUTES["data.chhs.ca.gov"]
CHHS_TARGET = CHHS_PREFIX + "0001/table.csv"


class ChhsRequest:
    full_url = CHHS_SOURCE
    headers = {"Authorization": "Bearer example", "Cookie": "session=example"}

    def get_method(self) -> str:
        return "GET"


def chhs_handler(
    monkeypatch: pytest.MonkeyPatch, folder: Path, hosts: tuple[str, ...] = ("data.chhs.ca.gov", "s3.amazonaws.com")
) -> transport.ReviewedRedirects:
    use_hosts(monkeypatch, folder, list(hosts))
    return transport.ReviewedRedirects({"data.chhs.ca.gov"}, source_url=CHHS_SOURCE)


def test_the_reviewed_chhs_redirect_is_followed_without_credentials_or_cookies(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    handler = chhs_handler(monkeypatch, tmp_path)
    for target in (CHHS_TARGET, CHHS_TARGET.replace("https://s3.amazonaws.com/", "https://s3.amazonaws.com:443/")):
        followed = handler.redirect_request(ChhsRequest(), None, 302, "Found", {}, target)
        check(followed.full_url == target, "the reviewed route is followed to the exact target")
        sent = {name.lower() for name in followed.headers} | {name.lower() for name in followed.unredirected_hdrs}
        check(not sent & {"authorization", "cookie", "proxy-authorization"}, "no authorization or cookie header crosses to S3")
        check(handler.resolved_metadata(target) == (target, False), "the resolved S3 URL is recorded as it is")
        handler = chhs_handler(monkeypatch, tmp_path)
    with pytest.raises(transport.CaptureError, match="Additional redirects"):
        handler.redirect_request(ChhsRequest(), None, 302, "Found", {}, CHHS_TARGET)
        handler.redirect_request(ChhsRequest(), None, 302, "Found", {}, CHHS_TARGET + "2")


def test_the_chhs_route_needs_s3_on_the_approved_hosts_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    handler = chhs_handler(monkeypatch, tmp_path, ("data.chhs.ca.gov",))
    with pytest.raises(transport.CaptureError, match="approved hosts"):
        handler.redirect_request(ChhsRequest(), None, 302, "Found", {}, CHHS_TARGET)


def test_the_same_bucket_from_another_source_host_is_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    use_hosts(monkeypatch, tmp_path, ["data.example.gov", "s3.amazonaws.com"])
    handler = transport.ReviewedRedirects({"data.example.gov"}, source_url="https://data.example.gov/file.csv")
    with pytest.raises(transport.CaptureError, match="not been reviewed"):
        handler.redirect_request(Request(), None, 302, "Found", {}, CHHS_TARGET)


@pytest.mark.parametrize(
    "target",
    [
        "https://s3.amazonaws.com/another-open-data-bucket/resources/0001/table.csv",
        CHHS_PREFIX.removesuffix("/resources/") + "-copy/resources/0001/table.csv",
        CHHS_PREFIX.removesuffix("resources/") + "resources-extra/0001/table.csv",
        CHHS_PREFIX.removesuffix("resources/") + "other/0001/table.csv",
        CHHS_PREFIX + "../../another-open-data-bucket/resources/table.csv",
        CHHS_TARGET.replace("s3.amazonaws.com", "s3.amazonaws.com.example.org"),
        CHHS_TARGET + "?X-Amz-Signature=" + "a" * 64,
    ],
    ids=["another bucket", "lookalike bucket", "lookalike prefix", "other folder", "dot segments", "lookalike host", "query string"],
)
def test_a_chhs_redirect_off_the_reviewed_prefix_is_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, target: str) -> None:
    handler = chhs_handler(monkeypatch, tmp_path, ("data.chhs.ca.gov", "s3.amazonaws.com", "s3.amazonaws.com.example.org"))
    with pytest.raises(transport.CaptureError):
        handler.redirect_request(ChhsRequest(), None, 302, "Found", {}, target)


@pytest.mark.parametrize(
    "target",
    [CHHS_TARGET.replace("https://", "http://"), CHHS_TARGET.replace("https://", "https://user:secret@")],
    ids=["http", "credentials"],
)
def test_a_chhs_redirect_without_plain_https_is_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, target: str) -> None:
    handler = chhs_handler(monkeypatch, tmp_path)
    with pytest.raises(transport.CaptureError):
        handler.redirect_request(ChhsRequest(), None, 302, "Found", {}, target)


def test_no_refusal_message_names_the_bucket(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    handler = chhs_handler(monkeypatch, tmp_path)
    bucket = CHHS_PREFIX.split("/")[3]
    with pytest.raises(transport.CaptureError) as caught:
        handler.redirect_request(ChhsRequest(), None, 302, "Found", {}, CHHS_PREFIX.removesuffix("resources/") + "other/table.csv")
    check(
        bucket not in str(caught.value) and bucket.rsplit("-", 1)[-1] not in str(caught.value), "the refusal does not repeat the bucket or its account number"
    )


SIGNED_QUERY = (
    "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=example%2F20261110%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20261110T000000Z"
    "&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=" + "a" * 64
)


class SignedRequest:
    full_url = SIGNED_SOURCE
    headers = {"Authorization": "Bearer example"}

    def get_method(self) -> str:
        return "GET"


@pytest.mark.parametrize(
    "target",
    [
        CHHS_PREFIX.removesuffix("resources/") + "/resources/0001/table.csv",
        CHHS_PREFIX + "0001//table.csv",
        CHHS_PREFIX + "0001%2Ftable.csv",
        CHHS_PREFIX.removesuffix("resources/") + "resources%2F0001/table.csv",
        CHHS_PREFIX + "%2E%2E/%2E%2E/another-open-data-bucket/table.csv",
        CHHS_PREFIX + "0001/%2e/table.csv",
    ],
    ids=["double slash before the prefix", "double slash inside", "encoded slash", "encoded slash in the prefix", "encoded dot segments", "encoded dot"],
)
def test_a_chhs_redirect_is_checked_on_its_raw_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, target: str) -> None:
    """The request keeps the raw path, so the check reads the raw path too: no folded slash, no escape and no dot segment passes."""
    handler = chhs_handler(monkeypatch, tmp_path)
    with pytest.raises(transport.CaptureError, match="reviewed redirect route"):
        handler.redirect_request(ChhsRequest(), None, 302, "Found", {}, target)


def test_the_reviewed_signed_route_is_still_followed_beside_the_unsigned_route(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    use_hosts(monkeypatch, tmp_path, ["data.chhs.ca.gov", "s3.amazonaws.com"])
    handler = transport.ReviewedRedirects({"data.chhs.ca.gov"}, source_url=SIGNED_SOURCE)
    target = transport.SIGNED_REDIRECT_ROUTES[SIGNED_SOURCE] + SIGNED_QUERY
    followed = handler.redirect_request(SignedRequest(), None, 302, "Found", {}, target)
    check(followed.full_url == target, "the signed route is followed with its query")
    check("authorization" not in {name.lower() for name in followed.headers}, "no authorization header crosses to S3")


def test_the_signed_source_refuses_an_unsigned_shaped_target(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    use_hosts(monkeypatch, tmp_path, ["data.chhs.ca.gov", "s3.amazonaws.com"])
    handler = transport.ReviewedRedirects({"data.chhs.ca.gov"}, source_url=SIGNED_SOURCE)
    for target in (transport.SIGNED_REDIRECT_ROUTES[SIGNED_SOURCE], CHHS_TARGET):
        with pytest.raises(transport.CaptureError, match="Signed redirect differs"):
            handler.redirect_request(SignedRequest(), None, 302, "Found", {}, target)


def test_an_unsigned_chhs_source_refuses_a_signed_shaped_target(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    handler = chhs_handler(monkeypatch, tmp_path)
    with pytest.raises(transport.CaptureError, match="reviewed redirect route"):
        handler.redirect_request(ChhsRequest(), None, 302, "Found", {}, CHHS_TARGET + SIGNED_QUERY)
