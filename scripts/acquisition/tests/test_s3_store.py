from __future__ import annotations

import base64
import copy
import functools
import hashlib
import http.client
import io
import json
import math
import os
import sys
import tempfile
import time
import urllib.request
import zipfile
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from scripts.acquisition import api_keys
from scripts.acquisition import s3_store as storage
from scripts.acquisition.capture import capture
from scripts.acquisition.source_registry import read_json
from scripts.acquisition.tests.aws_stand_in import AwsStandIn, InterceptingProxy, refreshing_process, write_certificate, write_profile
from scripts.acquisition.tests.test_capture import Opener, Response
from scripts.acquisition.tests.test_capture import setup_capture as setup_capture
from scripts.acquisition.transport import Limits
from scripts.process import TimeoutExpired
from tests.support import check

SETTINGS = {
    "expected_account_id": ("1" * 12),
    "aws_profile": "example_project_user",
    "data_bucket_name": "example-test-bucket",
    "aws_region": "eu-west-1",
    "project_name": "example_project",
}
OUTPUTS = {
    "bucket_name": {"value": SETTINGS["data_bucket_name"]},
    "collection_prefixes": {
        "value": {
            name: f"s3://{SETTINGS['data_bucket_name']}/{{publisher}}/{{collection}}/{name}/" for name in ("datasets", "references", "manifests", "audit")
        }
    },
}


REAL_READ = api_keys.read
CHECKED_KEY = "AKIASTANDINCHECKED01"
ENVIRONMENT_KEY = "AKIASTANDINENVIRON01"
FILE_KEY = "AKIASTANDINOVERRIDE1"
OTHER_ACCOUNT_KEY = "AKIASTANDINACCOUNT01"
OTHER_USER_KEY = "AKIASTANDINUSER00001"
READ_SECONDS = 20.0
NUMBER_SHAPED_KEY = "10000000000000000000"
NUMBER_SPELLED_KEY = "-1.5E+3"
NUMBER_LOOKALIKE_KEY = "10000000000000000x"
STALL_SECONDS = 6.5
REFRESH_LEAD_SECONDS = 5
REFRESH_HOLD_SECONDS = 0.25


def verified_identity() -> tuple[str, str]:
    """The account and ARN that the fake client's identity check answers and the transports verify."""
    account = SETTINGS["expected_account_id"]
    return account, f"arn:aws:iam::{account}:user/{SETTINGS['aws_profile']}"


def key_reader(client: FakeS3) -> Callable[..., str]:
    """A stand-in for ``api_keys.read``: it refuses settings or an identity other than the ones the fake client verified, records each accepted
    read in the client's calls and returns the plain-text override."""

    def read(name: str, profile: str, region: str, account: str, arn: str, seconds: float = api_keys.READ_DEADLINE_SECONDS, endpoint: str | None = None) -> str:
        if (profile, region) != (SETTINGS["aws_profile"], SETTINGS["aws_region"]):
            raise api_keys.ApiKeyError(f"reading secret {name} failed: settings differ from the checked client")
        if (account, arn) != verified_identity():
            raise api_keys.ApiKeyError(f"reading secret {name} failed: the SDK credentials belong to another identity than the checked one")
        client.calls.append(("get-secret-value", ["--secret-id", name]))
        return client.overrides["get-secret-value"]["SecretString"]

    return read


@contextmanager
def no_publisher_request() -> Iterator[None]:
    """Fail at once when code opens a publisher connection."""

    class Refused:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("a publisher request was opened")

    with patch.object(http.client, "HTTPSConnection", Refused):
        yield


def settings_refused(client: FakeS3, request: Callable[[], object]) -> None:
    """Run a live request whose checked client carries unusable key-read settings and require refusal before any reader process, key request or
    publisher request: a region other than the verified one (the stand-in reader refuses it) and a blank region (the real reader refuses it)."""
    verified = client.configuration
    reads = sum(call[0] == "get-secret-value" for call in client.calls)
    started: list[object] = []

    def no_process(*args: Any, **_kwargs: Any) -> None:
        started.append(args)
        raise AssertionError("a key reader process was started")

    for region, real in (("other-region-1", False), ("", True)):
        client.configuration = verified | {"aws_region": region}
        try:
            with ExitStack() as stack:
                stack.enter_context(no_publisher_request())
                stack.enter_context(patch.object(api_keys, "run_argv", no_process))
                if real:
                    stack.enter_context(patch.object(api_keys, "read", REAL_READ))
                request()
        except api_keys.ApiKeyError:
            pass
        else:
            raise AssertionError(f"settings with region {region!r} were accepted")
        finally:
            client.configuration = verified
    check(not started, "not started")
    check(sum(call[0] == "get-secret-value" for call in client.calls) == reads, "a key request was recorded for refused settings")


def refused(request: Callable[[], object], canary: str) -> api_keys.ApiKeyError:
    """Run a live request that the key reader must refuse before any publisher request; return the error after checking that it holds no value."""
    try:
        with no_publisher_request():
            request()
    except api_keys.ApiKeyError as error:
        check(canary not in str(error) and canary not in repr(error) and error.__context__ is None, "a refusal kept the value")
        return error
    raise AssertionError("the key reader accepted the request")


def interception_bound(request: Callable[[], object], name: str, canary: str, home: Path, folder: Path, identities: dict[str, tuple[str, str]]) -> None:
    """Run the real key reader, without a test endpoint, through a loopback HTTPS proxy named in ``HTTPS_PROXY`` that presents its own certificate
    and relays the decrypted traffic to a second loopback stand-in. The project profile and the default profile trust different certificates.

    Required: when only the default profile trusts the proxy's certificate, the read fails with the fixed ``sdk`` cause, the second stand-in receives
    no request and the proxy decrypts no byte, so the key never reaches it. When the project profile trusts the proxy's certificate the same proxy
    carries the identity request and the key request, both signed with the checked key, so the trust in force is the project profile's.
    """
    profile, region = SETTINGS["aws_profile"], SETTINGS["aws_region"]
    account, arn = verified_identity()
    hosts = [f"sts.{region}.amazonaws.com", f"secretsmanager.{region}.amazonaws.com", "sts.amazonaws.com"]
    proxy_certificate, proxy_key = write_certificate(folder, "proxy", hosts)
    other_certificate, _ = write_certificate(folder, "other", hosts)
    with AwsStandIn(identities, {name: canary}) as upstream, InterceptingProxy(proxy_certificate, proxy_key, upstream.server.server_address[1]) as proxy:
        proxies = {"HTTPS_PROXY": proxy.url, "https_proxy": proxy.url, "NO_PROXY": "", "no_proxy": ""}
        with patch.dict(os.environ, proxies), patch.object(api_keys, "read", functools.partial(REAL_READ, seconds=READ_SECONDS)):
            check(urllib.request.getproxies().get("https") == proxy.url, "the HTTPS proxy setting is not in force")
            write_profile(home, profile, region, CHECKED_KEY, ca_bundle=other_certificate, default_ca_bundle=proxy_certificate)
            error = refused(request, canary)
            check(api_keys.FAILURES["sdk"][1] in str(error), "an untrusted proxy certificate was not refused with the SDK cause")
            check(proxy.targets and proxy.refused_handshakes == len(proxy.targets), "the proxy was not tried or a handshake succeeded")
            check(not upstream.requests() and not proxy.intercepted, "a request or a byte reached the service side of an untrusted proxy")
            write_profile(home, profile, region, CHECKED_KEY, ca_bundle=proxy_certificate, default_ca_bundle=other_certificate)
            check(api_keys.read(name, profile, region, account, arn) == canary, "the read through the trusted proxy returned another value")
            check(
                [(item.action, item.key) for item in upstream.requests()] == [("GetCallerIdentity", CHECKED_KEY), ("GetSecretValue", CHECKED_KEY)],
                "the read through the trusted proxy did not use only the checked credentials, identity first",
            )
            check(canary.encode() in proxy.intercepted, "the trusted proxy did not carry the answer")


def number_shaped_refused(request: Callable[[], object], name: str, identities: dict[str, tuple[str, str]]) -> None:
    """Run the real key reader against a loopback stand-in whose secret is made only of characters a JSON number can hold.

    Required: for each such secret the read is refused with a fixed message that tells the owner to rotate the key and holds no part of the value, and
    no publisher request follows, so no JSON number can ever equal a key. A secret with one other character still reads.
    """
    profile, region = SETTINGS["aws_profile"], SETTINGS["aws_region"]
    account, arn = verified_identity()
    for secret in (NUMBER_SHAPED_KEY, NUMBER_SPELLED_KEY):
        with (
            AwsStandIn(identities, {name: secret}) as stand_in,
            patch.object(api_keys, "read", functools.partial(REAL_READ, seconds=READ_SECONDS, endpoint=stand_in.endpoint)),
        ):
            error = refused(request, secret)
            check("rotate" in str(error) and name in str(error), "a number-shaped key was refused without the rotate message")
            check(len(stand_in.requests("GetSecretValue")) == 1, "the number-shaped key was not read exactly once")
    with AwsStandIn(identities, {name: NUMBER_LOOKALIKE_KEY}) as stand_in:
        check(
            REAL_READ(name, profile, region, account, arn, seconds=READ_SECONDS, endpoint=stand_in.endpoint) == NUMBER_LOOKALIKE_KEY,
            "a key with a character a number cannot hold was refused",
        )


def identity_bound(request: Callable[[], object], name: str, canary: str) -> None:
    """Run the real key reader, in its child process, against the loopback STS and Secrets Manager stand-in while this process carries
    ``AWS_SHARED_CREDENTIALS_FILE``, ``AWS_CONFIG_FILE``, ``AWS_PROFILE`` and key variables toward other synthetic credentials.

    Required: the read succeeds and every request is signed with the checked profile's credentials, the identity request first; credentials of
    another account or another user under the same profile name are refused with no key request; when a credential process refreshes to another
    user while the identity answer is held, STS and the key request are both still signed with the checked key; when the project profile and the
    default profile trust different certificates and an HTTPS proxy in the environment presents the one only the default profile trusts, the read
    fails with the fixed ``sdk`` cause before any service receives a request and the proxy decrypts nothing, while the same proxy is used when the
    project profile trusts its certificate; a stalled identity request ends at the deadline and no key request follows it. ``HOME`` points at
    synthetic AWS files, so no real one is read.
    """
    account, arn = verified_identity()
    profile, region = SETTINGS["aws_profile"], SETTINGS["aws_region"]
    identities = {
        CHECKED_KEY: (account, arn),
        ENVIRONMENT_KEY: ("2" * 12, f"arn:aws:iam::{'2' * 12}:user/{profile}"),
        FILE_KEY: ("3" * 12, f"arn:aws:iam::{'3' * 12}:user/{profile}"),
        OTHER_ACCOUNT_KEY: ("4" * 12, f"arn:aws:iam::{'4' * 12}:user/{profile}"),
        OTHER_USER_KEY: (account, f"arn:aws:iam::{account}:user/{profile}-other"),
    }
    with tempfile.TemporaryDirectory(prefix="key_read_") as temporary, AwsStandIn(identities, {name: canary}) as stand_in:
        home = Path(temporary) / "home"
        write_profile(home, profile, region, CHECKED_KEY)
        overrides = write_profile(Path(temporary) / "other", profile, region, FILE_KEY) | {
            "HOME": str(home),
            "AWS_PROFILE": profile,
            "AWS_DEFAULT_PROFILE": profile,
            "AWS_ACCESS_KEY_ID": ENVIRONMENT_KEY,
            "AWS_SECRET_ACCESS_KEY": "stand-in-secret",
            "AWS_SESSION_TOKEN": "stand-in-token",
            "AWS_REGION": "us-east-2",
        }
        reader = functools.partial(REAL_READ, seconds=READ_SECONDS, endpoint=stand_in.endpoint)
        with patch.dict(os.environ, overrides), patch.object(api_keys, "read", reader):
            request()
            check(
                [(item.action, item.key) for item in stand_in.requests()] == [("GetCallerIdentity", CHECKED_KEY), ("GetSecretValue", CHECKED_KEY)],
                "the key read did not use only the checked credentials, identity first",
            )
            for key in (OTHER_ACCOUNT_KEY, OTHER_USER_KEY):
                write_profile(home, profile, region, key)
                before = len(stand_in.requests())
                check("another identity" in str(refused(request, canary)), "an identity mismatch was not named")
                check([item.action for item in stand_in.requests()[before:]] == ["GetCallerIdentity"], "a key request followed an identity mismatch")
            moment = math.ceil(time.time()) + REFRESH_LEAD_SECONDS
            command = refreshing_process(Path(temporary) / "process", CHECKED_KEY, OTHER_USER_KEY, moment)
            write_profile(home, profile, region, None, credential_process=command)
            refresh_at = time.monotonic() + moment - time.time()
            stand_in.held_until["GetCallerIdentity"] = moment + REFRESH_HOLD_SECONDS
            before = len(stand_in.requests())
            request()
            stand_in.held_until.clear()
            refreshed = stand_in.requests()[before:]
            check(
                [item.action for item in refreshed] == ["GetCallerIdentity", "GetSecretValue"] and refreshed[0].at < refresh_at,
                "the identity request did not arrive before the credential refresh",
            )
            check({item.key for item in refreshed} == {CHECKED_KEY}, "a request after a credential refresh was not signed with the checked credentials")
            interception_bound(request, name, canary, home, Path(temporary) / "interception", identities)
            write_profile(home, profile, region, CHECKED_KEY)
            number_shaped_refused(request, name, identities)
            stand_in.stalled.add("GetCallerIdentity")
            with patch.object(api_keys, "read", functools.partial(REAL_READ, seconds=STALL_SECONDS, endpoint=stand_in.endpoint)):
                began = time.monotonic()
                check("did not finish" in str(refused(request, canary)), "a stalled read did not end at its deadline")
                check(time.monotonic() - began < STALL_SECONDS, "a stalled read outlived its deadline")
            stand_in.released.set()
            time.sleep(1.0)
        check(len(stand_in.requests("GetSecretValue")) == 2, "a key request arrived after a refusal or a stalled read")
        check(not {item.key for item in stand_in.requests()} & {ENVIRONMENT_KEY, FILE_KEY}, "a request used the inherited override credentials")


class FakeS3(storage.AwsCli):
    def __init__(self) -> None:
        self.configuration = SETTINGS.copy()
        self.storage_policy = {"enabled": False, "objects": {}, "evidence_objects": {}, "bootstrap_sha256": None}
        self.objects: dict[str, dict] = {}
        self.calls: list = []
        self.overrides: dict = {}
        self.race = False
        self.put_failure: str | None = None
        self.corrupt = False

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
        if service == "secretsmanager":
            raise AssertionError("a key read must use api_keys.read, not the AWS command-line tool")
        args = arguments or []
        self.calls.append((operation, args))
        if operation in self.overrides:
            value = self.overrides[operation]
            if isinstance(value, Exception):
                raise value
            return value
        configuration: dict[str, dict] = {
            "get-caller-identity": {
                "Account": SETTINGS["expected_account_id"],
                "Arn": f"arn:aws:iam::{SETTINGS['expected_account_id']}:user/{SETTINGS['aws_profile']}",
            },
            "get-bucket-location": {"LocationConstraint": SETTINGS["aws_region"]},
            "get-bucket-versioning": {"Status": "Enabled"},
            "get-public-access-block": {
                "PublicAccessBlockConfiguration": dict.fromkeys(("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets"), True)
            },
            "get-bucket-encryption": {"ServerSideEncryptionConfiguration": {"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}},
            "get-bucket-ownership-controls": {"OwnershipControls": {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]}},
        }
        if operation in configuration:
            return configuration[operation]
        key = args[args.index("--key") + 1]
        if operation == "put-object":
            check(args[args.index("--if-none-match") + 1] == "*", 'args[args.index("--if-none-match") + 1] == "*"')
            check(key not in self.objects, "key not in self.objects")
            if self.put_failure:
                raise storage.StorageError(operation, self.put_failure)
            body = Path(args[args.index("--body") + 1]).read_bytes()
            checksum = base64.b64encode(hashlib.sha256(body).digest()).decode()
            check(args[args.index("--checksum-sha256") + 1] == checksum, 'args[args.index("--checksum-sha256") + 1] == checksum')
            self.objects[key] = {
                "VersionId": f"version_{len(self.objects) + 1}",
                "ContentLength": len(body),
                "ChecksumSHA256": checksum,
                "ServerSideEncryption": "AES256",
                "body": body,
            }
            if self.race:
                raise storage.StorageError(operation, "PreconditionFailed")
        if key not in self.objects:
            raise storage.StorageError(operation, "404")
        item = self.objects[key]
        if operation == "get-object":
            check(args[args.index("--version-id") + 1] == item["VersionId"], 'args[args.index("--version-id") + 1] == item["VersionId"]')
            Path(args[-1]).write_bytes(b"corrupted" if self.corrupt else item["body"])
        return {name: value for name, value in item.items() if name != "body"}


@pytest.fixture
def bundle(setup_capture: tuple, tmp_path: Path) -> tuple:
    source, plan, validator = setup_capture
    source["file_routes"][0]["url"] = "https://example.org/archive.zip"
    plan.update(expected_format="zip", file_name="archive.zip")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("release/data.csv", b"id,value\r\n0012A,2\r\n")
        archive.writestr("release/dictionary.pdf", b"%PDF-1.7\nexample documentation\n%%EOF")
    registry = {"sources": [source]}
    path = capture(plan, tmp_path, registry, validator, opener=Opener(Response(buffer.getvalue())))
    references = [{"artifact_id": "artifact_0001", "member": "release/dictionary.pdf", "file_name": "dictionary.pdf", "role": "dictionary", "format": "pdf"}]
    return path, references, FakeS3(), registry, validator


def upload(bundle: tuple) -> dict:
    path, references, client, registry, validator = bundle
    return storage.upload_snapshot(path, references, client, OUTPUTS, registry, validator)


def test_complete_storage_and_replay_are_identical(bundle: tuple) -> None:
    path, _, client, _, _ = bundle
    original = path.read_bytes()
    first = upload(bundle)
    saved = (path.parent / "s3_collections_reconciliation.json").read_bytes()
    versions = copy.deepcopy(client.objects)
    second = upload(bundle)
    check(first["objects_created"] == first["objects_verified"] == 6, 'first["objects_created"] == first["objects_verified"] == 6')
    check(second["objects_created"] == 0 and second["objects_reused"] == 6, 'second["objects_created"] == 0 and second["objects_reused"] == 6')
    check(
        path.read_bytes() == original and (path.parent / "s3_collections_reconciliation.json").read_bytes() == saved,
        'path.read_bytes() == original and (path.parent / "s3_collections_reconciliation.json").read_bytes() == saved',
    )
    check(client.objects == versions and not first["model_eligible"], 'client.objects == versions and not first["model_eligible"]')
    manifest = read_json(next((path.parent / "s3_collections").glob("*/manifest.json")))
    check(manifest["snapshot_status"] == "acquired_unvalidated", 'manifest["snapshot_status"] == "acquired_unvalidated"')
    reference = next(item for item in manifest["objects"] if item["role"] == "dictionary")
    check(
        reference["object"]["key"].split("/")[2] == "references" and reference["parent_sha256"],
        'reference["object"]["key"].split("/")[2] == "references" and reference["parent_sha256"]',
    )
    check(
        manifest["archive_storage_policy"] == "extracted_members_only_zip_retained_locally",
        'manifest["archive_storage_policy"] == "extracted_members_only_zip_retained_locally"',
    )
    check(not any(key.endswith(".zip") for key in client.objects), 'not any(key.endswith(".zip") for key in client.objects)')
    check(
        next(item["body"] for key, item in client.objects.items() if key.endswith("data.csv")) == b"id,value\r\n0012A,2\r\n",
        'next(item["body"] for key, item in client.objects.items() if key.endswith("data.csv")) == b"id,value\\r\\n0012A,2\\r\\n"',
    )
    check(
        {"/".join(key.split("/")[:2]) for key in client.objects} == {"example_publisher/example_collection"},
        '{"/".join(key.split("/")[:2]) for key in client.objects} == {"example_publisher/example_collection"}',
    )
    check(
        {key.split("/")[2] for key in client.objects} == {"datasets", "references", "manifests", "audit"},
        '{key.split("/")[2] for key in client.objects} == {"datasets", "references", "manifests", "audit"}',
    )


def zip_bytes(members: dict[str, bytes]) -> bytes:
    """A ZIP with a fixed timestamp, so a test id built from its bytes is the same in every collection."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, body in members.items():
            member = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            member.external_attr = 0o600 << 16
            archive.writestr(member, body)
    return buffer.getvalue()


@pytest.mark.parametrize("category", ["datasets", "references", "manifests", "audit"])
@pytest.mark.parametrize("name", ["archive.zip", "archive.ZIP", "archive.bin"])
def test_zip_containers_never_reach_s3(tmp_path: Path, category: str, name: str) -> None:
    path = tmp_path / name
    path.write_bytes(zip_bytes({"data.csv": b"id\n1\n"}))
    digest, _ = storage.fingerprint(path)
    key = f"example_publisher/example_collection/{category}/example_table/{digest}/{name}"
    client = FakeS3()
    with pytest.raises(storage.CaptureError, match="ZIP containers must remain local"):
        storage.store_file(client, path, key)
    check(not client.calls, "not client.calls")


def test_zip_destination_is_rejected_even_for_non_zip_source(tmp_path: Path) -> None:
    path = tmp_path / "data.csv"
    path.write_bytes(b"id\n1\n")
    digest, _ = storage.fingerprint(path)
    client = FakeS3()
    with pytest.raises(storage.CaptureError, match="ZIP containers must remain local"):
        storage.store_file(client, path, f"example_publisher/example_collection/datasets/example_table/{digest}/archive.zip")
    check(not client.calls, "not client.calls")


@pytest.mark.parametrize("extension,part", [("xlsx", "xl/workbook.xml"), ("docx", "word/document.xml")])
def test_native_office_documents_stay_intact(setup_capture: tuple, tmp_path: Path, extension: str, part: str) -> None:
    source, plan, validator = setup_capture
    plan.update(expected_format=extension, file_name=f"data.{extension}")
    content = zip_bytes({"[Content_Types].xml": b"<Types/>", part: b"<document/>"})
    registry = {"sources": [source]}
    path = capture(plan, tmp_path, registry, validator, opener=Opener(Response(content)))
    client = FakeS3()
    storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator)
    data = [item["body"] for key, item in client.objects.items() if key.split("/")[2] == "datasets"]
    check(data == [content] and not (path.parent / "expanded").exists(), 'data == [content] and not (path.parent / "expanded").exists()')


@pytest.mark.parametrize("content", [zip_bytes({"data.csv": b"id\n1\n"}), b"PK\x03\x04truncated"])
def test_zip_cannot_masquerade_as_office_document(tmp_path: Path, content: bytes) -> None:
    path = tmp_path / "data.xlsx"
    path.write_bytes(content)
    digest, _ = storage.fingerprint(path)
    client = FakeS3()
    with pytest.raises(storage.CaptureError, match="Office document"):
        storage.store_file(client, path, f"example_publisher/example_collection/datasets/example_table/{digest}/data.xlsx")
    check(not client.calls, "not client.calls")


def test_renamed_zip_is_expanded_before_storage(setup_capture: tuple, tmp_path: Path) -> None:
    source, plan, validator = setup_capture
    plan.update(expected_format="zip", file_name="archive.bin")
    content = zip_bytes({"data.csv": b"id\n1\n"})
    registry = {"sources": [source]}
    path = capture(plan, tmp_path, registry, validator, opener=Opener(Response(content)))
    client = FakeS3()
    storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator)
    check(all(item["body"] != content for item in client.objects.values()), 'all(item["body"] != content for item in client.objects.values())')
    check(any(key.endswith("data.csv") for key in client.objects), 'any(key.endswith("data.csv") for key in client.objects)')
    check((path.parent / "raw/archive.bin").read_bytes() == content, '(path.parent / "raw/archive.bin").read_bytes() == content')


@pytest.mark.parametrize("status,content", [(206, zip_bytes({"data.csv": b"id\n1\n"})), (403, b"PK\x03\x04truncated")])
def test_failed_zip_retains_payload_locally_and_uploads_metadata_only(setup_capture: tuple, tmp_path: Path, status: int, content: bytes) -> None:
    source, plan, validator = setup_capture
    plan.update(expected_format="zip", file_name="archive.zip")
    registry = {"sources": [source]}
    path = capture(plan, tmp_path, registry, validator, opener=Opener(Response(content, status)))
    client = FakeS3()
    storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator, audit_only=True)
    check(
        client.objects and all(key.split("/")[2] == "audit" and not key.lower().endswith(".zip") for key in client.objects),
        'client.objects and all(key.split("/")[2] == "audit" and not key.lower().endswith(".zip") for key in client.objects)',
    )
    check(all(item["body"] != content for item in client.objects.values()), 'all(item["body"] != content for item in client.objects.values())')
    manifest = read_json(next((path.parent / "s3_collections").glob("*/manifest.json")))
    check(
        manifest["archive_storage_policy"] == "extracted_members_only_zip_retained_locally",
        'manifest["archive_storage_policy"] == "extracted_members_only_zip_retained_locally"',
    )
    check(len(manifest["local_only_archive_payloads"]) == 1, 'len(manifest["local_only_archive_payloads"]) == 1')
    payload = manifest["local_only_archive_payloads"][0]
    check((path.parent / payload["storage_path"]).read_bytes() == content, '(path.parent / payload["storage_path"]).read_bytes() == content')
    check(
        payload["sha256"] == hashlib.sha256(content).hexdigest() and payload["byte_count"] == len(content),
        'payload["sha256"] == hashlib.sha256(content).hexdigest() and payload["byte_count"] == len(content)',
    )
    check(
        storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator, audit_only=True)["objects_created"] == 0,
        'storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator, audit_only=True)["objects_created"] == 0',
    )


def test_failed_zip_retry_is_not_uploaded_with_successful_capture(setup_capture: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts.acquisition import transport

    monkeypatch.setattr(transport.time, "sleep", lambda _: None)
    source, plan, validator = setup_capture
    plan.update(expected_format="zip", file_name="archive.zip")
    failed = b"PK\x03\x04truncated"
    content = zip_bytes({"data.csv": b"id\n1\n"})
    registry = {"sources": [source]}
    path = capture(plan, tmp_path, registry, validator, limits=Limits(attempts=2), opener=Opener(Response(failed, 503), Response(content)))
    client = FakeS3()
    storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator)
    check(not any(key.lower().endswith(".zip") for key in client.objects), 'not any(key.lower().endswith(".zip") for key in client.objects)')
    check(
        all(item["body"] not in {failed, content} for item in client.objects.values()),
        'all(item["body"] not in {failed, content} for item in client.objects.values())',
    )
    manifest = read_json(next((path.parent / "s3_collections").glob("*/manifest.json")))
    check(
        manifest["local_only_archive_payloads"][0]["storage_path"] == "audit/request_0001/attempt_01/archive.zip",
        'manifest["local_only_archive_payloads"][0]["storage_path"] == "audit/request_0001/attempt_01/archive.zip"',
    )
    check(
        (path.parent / "audit/request_0001/attempt_01/archive.zip").read_bytes() == failed,
        '(path.parent / "audit/request_0001/attempt_01/archive.zip").read_bytes() == failed',
    )
    check(
        storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator)["objects_created"] == 0,
        'storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator)["objects_created"] == 0',
    )


def test_disguised_nested_zip_blocks_all_uploads(setup_capture: tuple, tmp_path: Path) -> None:
    source, plan, validator = setup_capture
    plan.update(expected_format="zip", file_name="archive.zip")
    nested = zip_bytes({"data.csv": b"id\n1\n"})
    registry = {"sources": [source]}
    path = capture(plan, tmp_path, registry, validator, opener=Opener(Response(zip_bytes({"nested.bin": nested}))))
    client = FakeS3()
    with pytest.raises(storage.CaptureError, match="Nested ZIP"):
        storage.upload_snapshot(path, [], client, OUTPUTS, registry, validator)
    check(not client.objects, "not client.objects")


def test_existing_different_bytes_are_never_overwritten(bundle: tuple) -> None:
    upload(bundle)
    client = bundle[2]
    key = next(iter(client.objects))
    client.objects[key]["body"] = b"different"
    before = len([call for call in client.calls if call[0] == "put-object"])
    with pytest.raises(storage.CaptureError, match="byte hash"):
        upload(bundle)
    check(
        len([call for call in client.calls if call[0] == "put-object"]) == before, 'len([call for call in client.calls if call[0] == "put-object"]) == before'
    )


def test_fresh_capture_reuses_data_and_reference_versions(bundle: tuple, setup_capture: tuple, tmp_path: Path) -> None:
    upload(bundle)
    first_path, references, client, registry, validator = bundle
    source, plan, _ = setup_capture
    plan.update(expected_format="zip", file_name="archive.zip")
    source["file_routes"][0]["url"] = "https://example.org/archive.zip"
    archive = first_path.parent / read_json(first_path)["artifacts"][0]["storage_path"]
    newer = capture(plan, tmp_path / "next_capture", registry, validator, opener=Opener(Response(archive.read_bytes())))
    original_versions = {key: item["VersionId"] for key, item in client.objects.items() if key.split("/")[2] in {"datasets", "references"}}
    result = storage.upload_snapshot(newer, references, client, OUTPUTS, registry, validator)
    check(result["objects_created"] == 4 and result["objects_reused"] == 2, 'result["objects_created"] == 4 and result["objects_reused"] == 2')
    check(
        original_versions == {key: item["VersionId"] for key, item in client.objects.items() if key.split("/")[2] in {"datasets", "references"}},
        'original_versions == {key: item["VersionId"] for key, item in client.objects.items() if key.split("/")[2] in {"datasets", "references"}}',
    )


def test_permission_denied_is_not_absence(bundle: tuple) -> None:
    bundle[2].overrides["head-object"] = storage.StorageError("head-object", "403")
    with pytest.raises(storage.StorageError, match="403"):
        upload(bundle)
    check(not bundle[2].objects, "not bundle[2].objects")


def test_concurrent_identical_writer_is_reused(bundle: tuple) -> None:
    bundle[2].race = True
    report = upload(bundle)
    check(report["objects_created"] == 0 and report["objects_reused"] == 6, 'report["objects_created"] == 0 and report["objects_reused"] == 6')


def test_failed_upload_is_resumable_without_duplicate_versions(bundle: tuple) -> None:
    bundle[2].put_failure = "AccessDenied"
    with pytest.raises(storage.StorageError, match="AccessDenied"):
        upload(bundle)
    check(not (bundle[0].parent / "s3_collections_reconciliation.json").exists(), 'not (bundle[0].parent / "s3_collections_reconciliation.json").exists()')
    bundle[2].put_failure = None
    check(upload(bundle)["objects_created"] == 6, 'upload(bundle)["objects_created"] == 6')


def test_verification_failure_keeps_uploaded_object_for_retry(bundle: tuple) -> None:
    bundle[2].corrupt = True
    with pytest.raises(storage.CaptureError, match="byte hash"):
        upload(bundle)
    check(len(bundle[2].objects) == 1, "len(bundle[2].objects) == 1")
    bundle[2].corrupt = False
    check(upload(bundle)["objects_created"] == 5, 'upload(bundle)["objects_created"] == 5')


@pytest.mark.parametrize(
    "operation,payload,match",
    [
        ("get-caller-identity", {"Account": "000000000000", "Arn": "invalid"}, "identity"),
        ("get-caller-identity", {"Account": ("1" * 12), "Arn": ("arn:aws:sts::" + "1" * 12 + ":assumed-role/example/session")}, "identity"),
        ("get-caller-identity", {"Account": ("1" * 12), "Arn": ("arn:aws:iam::" + "1" * 12 + ":user/other_user")}, "same-named"),
        ("get-bucket-location", {"LocationConstraint": "us-east-2"}, "region"),
        ("get-bucket-versioning", {"Status": "Suspended"}, "versioning"),
        ("get-public-access-block", {"PublicAccessBlockConfiguration": {}}, "public-access"),
        ("get-bucket-encryption", {"ServerSideEncryptionConfiguration": {"Rules": []}}, "encryption"),
        ("get-bucket-ownership-controls", {}, "ownership"),
    ],
)
def test_preflight_holds_precede_writes(bundle: tuple, operation: str, payload: dict, match: str) -> None:
    bundle[2].overrides[operation] = payload
    with pytest.raises(storage.CaptureError, match=match):
        upload(bundle)
    check(not bundle[2].objects, "not bundle[2].objects")


def test_terraform_mismatch_blocks_storage(bundle: tuple) -> None:
    with pytest.raises(storage.CaptureError, match="Terraform"):
        storage.upload_snapshot(bundle[0], bundle[1], bundle[2], {}, bundle[3], bundle[4])
    check(not bundle[2].objects, "not bundle[2].objects")


@pytest.mark.parametrize("version", [None, "null", ""])
def test_missing_version_is_rejected(tmp_path: Path, version: str | None) -> None:
    with pytest.raises(storage.CaptureError, match="version ID"):
        storage.verify_version(FakeS3(), "raw/example", version, "0" * 64, 1)


def test_server_checksum_is_required(bundle: tuple) -> None:
    upload(bundle)
    client = bundle[2]
    next(iter(client.objects.values()))["ChecksumSHA256"] = "invalid"
    with pytest.raises(storage.CaptureError, match="checksum"):
        upload(bundle)


@pytest.mark.parametrize("key", ["other/file", "raw/../file", "/raw/file", "raw/not_content_addressed"])
def test_unapproved_keys_cannot_upload(tmp_path: Path, key: str) -> None:
    path = tmp_path / "file"
    path.write_bytes(b"test")
    with pytest.raises(storage.CaptureError):
        storage.store_file(FakeS3(), path, key)


@pytest.mark.parametrize("value", ["..", "", "../escape", "has space", "a/b"])
def test_key_components_are_bounded(value: str) -> None:
    with pytest.raises(storage.CaptureError):
        storage.component(value)


def test_incomplete_capture_does_not_upload(bundle: tuple) -> None:
    path = bundle[0]
    receipt = read_json(path)
    receipt["snapshot_status"], receipt["verification"]["status"] = "evidence_only_partial", "evidence_only"
    path.write_bytes(storage.encoded_json(receipt))
    with pytest.raises(storage.CaptureError, match="Only complete"):
        upload(bundle)
    check(not bundle[2].calls, "not bundle[2].calls")


def test_changed_registry_blocks_upload(bundle: tuple) -> None:
    bundle[3]["extra"] = True
    with pytest.raises(storage.CaptureError, match="fingerprints"):
        upload(bundle)
    check(not bundle[2].calls, "not bundle[2].calls")


@pytest.mark.parametrize("mutation", ["route", "hold"])
def test_route_checks_block_changed_receipt(bundle: tuple, mutation: str) -> None:
    path, _, client, registry, _ = bundle
    receipt = read_json(path)
    if mutation == "route":
        receipt["acquisition"]["requested_url"] = "https://example.org/unreviewed.zip"
    else:
        registry["sources"][0]["preferred_route"] = "access_hold"
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        lineage["registry_sha256"] = storage.canonical_hash(registry)
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
    path.write_bytes(storage.encoded_json(receipt))
    with pytest.raises(storage.CaptureError):
        upload(bundle)
    check(not client.calls, "not client.calls")


@pytest.mark.parametrize(
    "change,match",
    [
        ({"extra": True}, "exact member"),
        ({"role": "data"}, "roles"),
        ({"member": "../outside"}, "Unsafe"),
        ({"artifact_id": "missing"}, "parent"),
        ({"member": "absent.pdf"}, "missing"),
        ({"format": "json"}, "validation failed"),
    ],
)
def test_invalid_reference_blocks_upload(bundle: tuple, change: dict, match: str) -> None:
    bundle[1][0].update(change)
    with pytest.raises(storage.CaptureError, match=match):
        upload(bundle)
    check(not bundle[2].objects, "not bundle[2].objects")


def test_duplicate_reference_names_block_upload(bundle: tuple) -> None:
    bundle[1].append(bundle[1][0].copy())
    with pytest.raises(storage.CaptureError, match="unique"):
        upload(bundle)
    check(not bundle[2].objects, "not bundle[2].objects")


@pytest.mark.parametrize("symlink", [False, True])
def test_unexpected_audit_paths_block_upload(bundle: tuple, symlink: bool, tmp_path: Path) -> None:
    path = bundle[0].parent / "audit" / "extra.txt"
    if symlink:
        target = tmp_path / "outside"
        target.write_text("private")
        path.symlink_to(target)
    else:
        path.write_text("unregistered")
    with pytest.raises(storage.CaptureError, match="Audit evidence|Unexpected audit"):
        upload(bundle)
    check(not bundle[2].objects, "not bundle[2].objects")


def test_write_once_cannot_replace_existing_content(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    storage.write_once(path, b"original")
    storage.write_once(path, b"original")
    with pytest.raises(storage.CaptureError, match="overwrite"):
        storage.write_once(path, b"changed")
    check(path.read_bytes() == b"original", 'path.read_bytes() == b"original"')


def test_cli_pins_identity_owner_and_clears_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "synthetic_key")
    monkeypatch.setenv("AWS_PROFILE", "other_profile")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://example.org/not_aws")
    calls = []

    def runner(command: list, **kwargs: Any) -> SimpleNamespace:
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout='{"Status":"Enabled"}', stderr="")

    client = storage.AwsCli(SETTINGS, runner)
    check(client.call("s3api", "get-bucket-versioning")["Status"] == "Enabled", 'client.call("s3api", "get-bucket-versioning")["Status"] == "Enabled"')
    command, kwargs = calls[0]
    check(command[command.index("--profile") + 1] == SETTINGS["aws_profile"], 'command[command.index("--profile") + 1] == SETTINGS["aws_profile"]')
    check(
        command[command.index("--expected-bucket-owner") + 1] == SETTINGS["expected_account_id"],
        'command[command.index("--expected-bucket-owner") + 1] == SETTINGS["expected_account_id"]',
    )
    check(
        not {"AWS_ACCESS_KEY_ID", "AWS_PROFILE", "AWS_ENDPOINT_URL"} & kwargs["env"].keys(),
        'not {"AWS_ACCESS_KEY_ID", "AWS_PROFILE", "AWS_ENDPOINT_URL"} & kwargs["env"].keys()',
    )
    check(kwargs["env"]["AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"] == "true", 'kwargs["env"]["AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"] == "true"')
    check(kwargs["timeout"] == 180, 'kwargs["timeout"] == 180')


@pytest.mark.parametrize(
    "response,code",
    [
        (SimpleNamespace(returncode=1, stdout="", stderr="An error occurred (403) when calling HeadObject: private details"), "403"),
        (SimpleNamespace(returncode=1, stdout="", stderr="sensitive unstructured error"), "request_failed"),
        (SimpleNamespace(returncode=0, stdout="not json", stderr=""), "invalid_response"),
        (SimpleNamespace(returncode=0, stdout="[]", stderr=""), "invalid_response"),
        (OSError("private details"), "request_unconfirmed"),
        (TimeoutExpired("aws", 180), "request_unconfirmed"),
    ],
)
def test_cli_failures_are_redacted(response: object, code: str) -> None:
    def runner(*args: Any, **kwargs: Any) -> object:
        if isinstance(response, Exception):
            raise response
        return response

    with pytest.raises(storage.StorageError, match=code) as error:
        storage.AwsCli(SETTINGS, runner).call("sts", "get-caller-identity")
    check(
        "private details" not in str(error.value) and "sensitive" not in str(error.value),
        '"private details" not in str(error.value) and "sensitive" not in str(error.value)',
    )


def test_main_uses_configuration_and_local_outputs(bundle: tuple, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    reference_path = tmp_path / "refs.json"
    reference_path.write_text(json.dumps({"references": bundle[1]}))
    monkeypatch.setattr(sys, "argv", ["s3_store", "--receipt", str(bundle[0]), "--references", str(reference_path)])
    monkeypatch.setattr(storage, "load_configuration", lambda _: (SETTINGS, {}))
    monkeypatch.setattr(storage, "run_argv", lambda *args, **kwargs: SimpleNamespace(stdout=json.dumps(OUTPUTS)))
    monkeypatch.setattr(storage, "AwsCli", lambda _: bundle[2])
    monkeypatch.setattr(storage, "load_registry", lambda: bundle[3])
    monkeypatch.setattr(storage, "receipt_validator", lambda: bundle[4])
    storage.main()
    check(json.loads(capsys.readouterr().out)["objects_created"] == 6, 'json.loads(capsys.readouterr().out)["objects_created"] == 6')
    reference_path.write_text('{"references":{}}')
    with pytest.raises(SystemExit):
        storage.main()


def test_main_reports_configuration_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["s3_store", "--receipt", "example.json", "--references", "refs.json"])

    def fail(*args: Any, **kwargs: Any) -> None:
        raise ValueError("synthetic configuration error")

    monkeypatch.setattr(storage, "load_configuration", fail)
    with pytest.raises(SystemExit):
        storage.main()
