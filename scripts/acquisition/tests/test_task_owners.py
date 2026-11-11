"""The CMS owners and ONC units run on the task like any unit: all the data is public, so each release is stored whole under raw/ as published."""

from __future__ import annotations

import dataclasses
import io
import json
import urllib.request
import urllib.response
from email.message import Message
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from scripts.acquisition import cms_owners_contract as contract
from scripts.acquisition import collection_layout, s3_store, task_role, transport
from scripts.acquisition import onc_mu_contract as onc_contract
from scripts.acquisition import redownload as harness
from scripts.acquisition import store_cms_owners as collector
from scripts.acquisition import store_onc_mu as onc_collector
from scripts.acquisition.run_cms_owners_e2e import LIMITS, PEOPLE, Web, synthetic_plan
from scripts.acquisition.run_onc_mu_e2e import body as onc_body
from scripts.acquisition.run_onc_mu_e2e import locked
from scripts.acquisition.run_redownload_e2e import Fixture
from scripts.acquisition.s3_store import StorageError, encoded_json, write_once
from scripts.acquisition.source_registry import read_json
from scripts.acquisition.tests.task_fixture import task_run
from tests.support import check

ACCOUNT = "1" * 12
BUCKET = "example-project-data-dev"
NAMES = [" ".join(part for part in person if part) for person in PEOPLE]


@pytest.fixture(autouse=True)
def no_hosts_file_between_cases(monkeypatch: pytest.MonkeyPatch) -> None:
    """The route gate names the approved hosts file in the environment. Setting them here first makes the test's teardown restore them.

    These cases read back through the collectors' real route check, so they also use the real routes in place of the shared stand-in.
    """
    monkeypatch.setattr(s3_store, "load_routes", collection_layout.load_routes)
    for name in (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE):
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)


class TaskS3:
    """A versioned bucket stand-in with the boto3 call shape of the S3 client. It records every stored key. Real AWS is never reached."""

    def __init__(self, refuse_puts: bool = False) -> None:
        self.objects: dict[str, tuple[str, bytes, dict]] = {}
        self.refuse_puts = refuse_puts

    @staticmethod
    def missing(operation: str, code: str) -> ClientError:
        return ClientError({"Error": {"Code": code, "Message": "synthetic"}}, operation)

    def head_object(self, **arguments: Any) -> dict:
        check(arguments["Bucket"] == BUCKET and arguments["ExpectedBucketOwner"] == ACCOUNT, "the bucket and its owner are named")
        if arguments["Key"] not in self.objects:
            raise self.missing("HeadObject", "404")
        version, data, fields = self.objects[arguments["Key"]]
        return {"VersionId": version, "ContentLength": len(data), **fields}

    def put_object(self, **arguments: Any) -> dict:
        if self.refuse_puts:
            raise self.missing("PutObject", "AccessDenied")
        check(arguments["IfNoneMatch"] == "*", "an object is created once")
        data = arguments["Body"].read()
        fields = {"ChecksumSHA256": arguments["ChecksumSHA256"], "ServerSideEncryption": arguments["ServerSideEncryption"]}
        self.objects[arguments["Key"]] = (f"version{len(self.objects)}", data, fields)
        return {"VersionId": self.objects[arguments["Key"]][0], **fields}

    def get_object(self, **arguments: Any) -> dict:
        version, data, fields = self.objects[arguments["Key"]]
        check(arguments["VersionId"] == version, "a read names the version")

        class Stream:
            def __init__(self) -> None:
                self.left = data

            def read(self, size: int = -1) -> bytes:
                part, self.left = self.left[:size], self.left[size:]
                return part

        return {"VersionId": version, "ContentLength": len(data), "Body": Stream(), **fields}


def storage(bucket: TaskS3) -> task_role.TaskStorage:
    return task_role.TaskStorage(bucket, {"data_bucket_name": BUCKET, "expected_account_id": ACCOUNT, "aws_region": "us-west-2"})


class Answer(urllib.response.addinfourl):
    """An addinfourl with the message urllib reads."""

    msg = "OK"


class Publisher(urllib.request.HTTPSHandler):
    """Serves the synthetic release bytes below the reviewed redirect handler and counts every request."""

    def __init__(self, web: Web) -> None:
        super().__init__()
        self.web = web

    def https_open(self, request: Any) -> Any:
        reply = self.web.open(request, 5)
        headers = Message()
        for name, value in reply.headers.items():
            headers[name] = value
        return Answer(io.BytesIO(reply.read()), headers, request.full_url, reply.status)


def opener(web: Web) -> transport.ReviewedTransport:
    return transport.ReviewedTransport(urllib.request.ProxyHandler({}), Publisher(web))


@pytest.fixture
def owners(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Any:
    plan, plan_path, bodies = synthetic_plan(tmp_path)
    versions = tmp_path / "versions.json"
    write_once(versions, encoded_json({"versions": [{"code_sha256": contract.collector_code_hashes()}]}))
    monkeypatch.setattr(contract, "PLAN_PATH", plan_path)
    monkeypatch.setattr(contract, "VERSIONS_PATH", versions)
    return plan, bodies


def stored_unit(monkeypatch: pytest.MonkeyPatch, folder: Path, plan: dict, bodies: dict[str, bytes], fixture: Fixture) -> tuple[dict, dict]:
    """A stored capture of one release, made by the real collector on the Mac path, with its original outside the mirror."""
    release = plan["releases"][0]
    stored = folder / "stored_owners"
    with monkeypatch.context() as earlier:
        earlier.delenv("HAI_CAPTURE_ROOT", raising=False)
        collector.execute(plan, release, stored, True, False, None, {}, opener(Web(bodies)), LIMITS)
    receipt = stored / "batches" / release["id"] / "capture/receipt.json"
    entry = {
        "snapshot_id": read_json(receipt)["snapshot_id"],
        "source_id": "CMS_OWNERS",
        "receipt": str(receipt.relative_to(folder)),
        "receipt_sha256": harness.digest(receipt.read_bytes()),
        "url": release["url"],
        "mode": "cms_owners_org",
        "bytes": len(bodies[release["url"]]),
        "pages": 1,
        "disposition": "direct_download",
        "route": "publisher_url",
        "flags": [],
    }
    fixture.entries.append(entry)
    return entry, release


def run_task(monkeypatch: pytest.MonkeyPatch, folder: Path, plan: dict, bodies: dict, bucket: TaskS3, web: Web | None = None) -> tuple[harness.Run, dict, dict]:
    fixture = Fixture(folder)
    entry, release = stored_unit(monkeypatch, folder, plan, bodies, fixture)
    run = task_run(folder, fixture, opener=opener(web or Web(bodies)), task_storage=storage(bucket))
    return run, entry, release


def test_the_task_stores_the_owners_release_whole_under_raw_with_no_switch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, owners: Any) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    plan, bodies = owners
    bucket = TaskS3()
    run, entry, release = run_task(monkeypatch, tmp_path, plan, bodies, bucket)
    summary = harness.execute(run)
    check(summary["outcomes"] == {"exact_match": 1}, f"the fresh release matches the stored one: {summary}")
    check(bool(bucket.objects) and all(key.startswith("raw/") for key in bucket.objects), f"keys outside raw/: {sorted(bucket.objects)}")
    original = bodies[release["url"]]
    check(any(data == original for _, data, _ in bucket.objects.values()), "the release is stored as published")
    receipts = [json.loads(data) for key, (_, data, _) in bucket.objects.items() if key.endswith("/receipt.json")]
    check(
        len(receipts) == 1
        and receipts[0]["governance"]["contains_pii"] is True
        and contract.PSEUDONYMIZE_AT_LOAD in receipts[0]["governance"]["use_restrictions"],
        "the receipt says the public release names people and bronze pseudonymizes them",
    )
    proofs = [json.loads(data) for key, (_, data, _) in bucket.objects.items() if key.endswith("download_proof.json")]
    check(len(proofs) == 1 and proofs[0]["sha256"] == harness.digest(original) and "original_retention" not in proofs[0], "the proof names the stored release")
    check(not any("private_original" in path.parts for path in run.state_root.rglob("*")), "no private folder is made")
    check(harness.execute(dataclasses.replace(run, cache={}))["attempted"] == 0, "the real outcome survives a restart's recomputation")


@pytest.mark.parametrize("failure", ["bad bytes", "storage refused"])
def test_a_failing_owners_unit_is_blocked_and_stores_no_release(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, owners: Any, failure: str) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    plan, bodies = owners
    bucket = TaskS3(refuse_puts=failure == "storage refused")
    web = Web(dict(bodies))
    run, entry, release = run_task(monkeypatch, tmp_path, plan, bodies, bucket, web)
    if failure == "bad bytes":
        web.bodies[release["url"]] = bodies[release["url"]].replace(b"ENROLLMENT ID", b"OTHER ID")
    summary = harness.execute(run)
    check(summary["outcomes"] == {"blocked": 1}, f"the unit is blocked: {summary}")
    check(not any(key.endswith(".csv") for key in bucket.objects), "no release reached S3")


def test_task_storage_refuses_anything_but_its_three_reads_and_writes(tmp_path: Path) -> None:
    client = storage(TaskS3())
    with pytest.raises(StorageError):
        client.call("sts", "get-caller-identity")
    with pytest.raises(StorageError):
        client.call("s3api", "delete-object", ["--key", "raw/a"])
    (tmp_path / "plain.csv").write_text("a")
    with pytest.raises(StorageError):
        client.call("s3api", "put-object", ["--key", "other/a", "--body", str(tmp_path / "plain.csv"), "--if-none-match", "*"])


@pytest.fixture
def onc(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Any:
    data = onc_body()
    plan, plan_path = locked(tmp_path, data)
    versions = tmp_path / "onc_versions.json"
    write_once(versions, encoded_json({"versions": [{"code_sha256": onc_contract.collector_code_hashes()}]}))
    monkeypatch.setattr(onc_contract, "PLAN_PATH", plan_path)
    monkeypatch.setattr(onc_contract, "VERSIONS_PATH", versions)
    return plan, data


def onc_task(monkeypatch: pytest.MonkeyPatch, folder: Path, plan: dict, data: bytes, bucket: TaskS3, web: Web | None = None) -> tuple[harness.Run, dict]:
    bodies = {plan["url"]: data}
    stored = folder / "stored_onc"
    with monkeypatch.context() as earlier:
        earlier.delenv("HAI_CAPTURE_ROOT", raising=False)
        onc_collector.execute(plan, stored, True, False, None, {}, opener(Web(bodies)), LIMITS)
    receipt = stored / "batches" / plan["id"] / "capture/receipt.json"
    fixture = Fixture(folder)
    entry = {
        "snapshot_id": read_json(receipt)["snapshot_id"],
        "source_id": onc_contract.SOURCE_ID,
        "receipt": str(receipt.relative_to(folder)),
        "receipt_sha256": harness.digest(receipt.read_bytes()),
        "url": plan["url"],
        "mode": "onc_mu_hospital",
        "bytes": len(data),
        "pages": 1,
        "disposition": "direct_download",
        "route": "publisher_url",
        "flags": [],
    }
    fixture.entries.append(entry)
    return task_run(folder, fixture, opener=opener(web or Web(bodies)), task_storage=storage(bucket)), entry


def test_the_task_stores_the_onc_file_whole_under_raw_with_no_switch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, onc: Any) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    plan, data = onc
    bucket = TaskS3()
    run, _entry = onc_task(monkeypatch, tmp_path, plan, data, bucket)
    summary = harness.execute(run)
    check(summary["outcomes"] == {"exact_match": 1}, f"the fresh file matches the stored one: {summary}")
    check(bool(bucket.objects) and all(key.startswith("raw/") for key in bucket.objects), f"keys outside raw/: {sorted(bucket.objects)}")
    check(any(stored == data for _, stored, _ in bucket.objects.values()), "the file is stored as published")
    receipts = [json.loads(stored) for key, (_, stored, _) in bucket.objects.items() if key.endswith("/receipt.json")]
    check(len(receipts) == 1 and receipts[0]["governance"]["contains_pii"] is True, "the receipt says the public file names people")
    check(not any("private_original" in path.parts for path in run.state_root.rglob("*")), "no private folder is made")
    check(harness.execute(dataclasses.replace(run, cache={}))["attempted"] == 0, "the real outcome survives a restart's recomputation")


@pytest.mark.parametrize("failure", ["changed bytes", "storage refused"])
def test_a_failing_onc_unit_is_blocked_and_stores_no_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, onc: Any, failure: str) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    plan, data = onc
    bucket = TaskS3(refuse_puts=failure == "storage refused")
    web = Web({plan["url"]: data})
    run, _entry = onc_task(monkeypatch, tmp_path, plan, data, bucket, web)
    if failure == "changed bytes":
        web.bodies[plan["url"]] = data + b'"changed"'
    summary = harness.execute(run)
    check(summary["outcomes"] == {"blocked": 1}, f"outcome: {summary}")
    check(not any(key.endswith(".csv") for key in bucket.objects), "no file reached S3")
