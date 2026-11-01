"""The publisher job's entry point: staged inputs by digest, the harness with the switches from its environment, and a state that holds only records."""

from __future__ import annotations

import hashlib
import io
import json
import tarfile
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from scripts.acquisition import publisher_run
from scripts.acquisition import redownload as harness
from scripts.acquisition.tests.test_task_keys import ACCOUNT, PLANTED, ROLE, TASK_IDENTITY
from tests.support import check

BUCKET = "example-project-data-dev"
RUN_ID = "20261101T000000Z-abcd0123"
QUEUE_SHA256 = "a" * 64


class Cloud:
    """STS and S3 stand-ins with the boto3 call shape. Real AWS is never reached."""

    def __init__(self, stage: dict[str, bytes]) -> None:
        self.objects = dict(stage)
        self.puts: list[str] = []

    def factory(self, service: str, region: str) -> Any:
        return self

    def get_caller_identity(self) -> dict:
        return TASK_IDENTITY

    def get_object(self, **arguments: Any) -> dict:
        if arguments["Key"] not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "synthetic"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[arguments["Key"]])}

    def list_objects_v2(self, **arguments: Any) -> dict:
        return {"Contents": [{"Key": key} for key in sorted(self.objects) if key.startswith(arguments["Prefix"])], "IsTruncated": False}

    def put_object(self, **arguments: Any) -> dict:
        check(arguments["IfNoneMatch"] == "*", "a state is created once")
        self.puts.append(arguments["Key"])
        self.objects[arguments["Key"]] = arguments["Body"]
        return {"VersionId": "v1"}


def archive(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as output:
        for name, body in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(body)
            output.addfile(info, io.BytesIO(body))
    return buffer.getvalue()


def staged(files: dict[str, bytes] | None = None) -> tuple[dict[str, bytes], str]:
    body = archive(files or {"mirror/queue.json": b"{}", "publisher_hosts.json": b"{}"})
    return {f"lakehouse/run_stage/{RUN_ID}/inputs/publisher_inputs.tar.gz": body}, hashlib.sha256(body).hexdigest()


def environment(workspace: Path, digest: str, **extra: str) -> dict[str, str]:
    return {
        "RUN_ID": RUN_ID,
        "AWS_REGION": "us-west-2",
        "EXPECTED_ACCOUNT_ID": ACCOUNT,
        "TASK_ROLE_NAME": ROLE,
        "DATA_BUCKET_NAME": BUCKET,
        "PUBLISHER_INPUTS_SHA256": digest,
        "PUBLISHER_QUEUE_SHA256": QUEUE_SHA256,
        "PUBLISHER_WORKSPACE": str(workspace),
    } | extra


def test_the_inputs_are_read_by_digest_and_a_changed_archive_is_refused(tmp_path: Path) -> None:
    stage, digest = staged()
    cloud = Cloud(stage)
    inputs = publisher_run.fetch_inputs(cloud, environment(tmp_path, digest))
    check((inputs / "mirror/queue.json").read_bytes() == b"{}", "the archive is unpacked under the workspace inputs")
    with pytest.raises(publisher_run.PublisherError, match="digest"):
        publisher_run.fetch_inputs(cloud, environment(tmp_path / "other", "0" * 64))


def test_an_archive_that_leaves_its_folder_is_refused(tmp_path: Path) -> None:
    stage, digest = staged({"../outside.json": b"{}"})
    with pytest.raises(publisher_run.PublisherError, match="unsafe"):
        publisher_run.fetch_inputs(Cloud(stage), environment(tmp_path, digest))
    check(not (tmp_path / "outside.json").exists(), "nothing was written outside")


def fill_state(root: Path, names: list[str]) -> None:
    (root / "outcomes").mkdir(parents=True)
    (root / "run.json").write_text('{"queue_sha256": "x"}')
    (root / "outcomes/unit.json").write_text('{"outcome": "exact_match"}')
    (root / "collectors/bls_api_v2/requests").mkdir(parents=True)
    (root / "collectors/bls_api_v2/requests/000001.json").write_text('{"batch_id": "b"}')
    (root / "collectors/cms_owners_org/private_original/r").mkdir(parents=True)
    (root / "collectors/cms_owners_org/private_original/r/owners.csv").write_text(names[0])
    (root / "files/unit").mkdir(parents=True)
    (root / "files/unit/fresh.csv").write_text(names[0])
    (root / "private/diagnostics").mkdir(parents=True)
    (root / "private/diagnostics/unit__01.json").write_text(names[0])


def test_the_saved_state_holds_only_the_records_and_a_new_task_restores_it(tmp_path: Path) -> None:
    secret = next(iter(PLANTED.values()))
    root = tmp_path / "state"
    fill_state(root, [secret])
    cloud = Cloud({})
    environ = environment(tmp_path, "0" * 64)
    key = publisher_run.save_state(cloud, root, environ)
    check(key.startswith(f"raw/run3_state/{QUEUE_SHA256}/") and cloud.puts == [key], f"the state goes under raw/: {key}")
    with tarfile.open(fileobj=io.BytesIO(cloud.objects[key]), mode="r:gz") as saved:
        names = sorted(saved.getnames())
        bodies = b"".join(reader.read() for member in saved.getmembers() if (reader := saved.extractfile(member)) is not None)
    check(names == ["collectors/bls_api_v2/requests/000001.json", "outcomes/unit.json", "run.json"], f"only the records are saved: {names}")
    check(secret.encode() not in bodies, "nothing else reached the saved state")
    fresh = tmp_path / "next"
    check(publisher_run.restore_state(cloud, fresh, environ) == key, "a new task finds the latest state")
    check(json.loads((fresh / "outcomes/unit.json").read_text())["outcome"] == "exact_match", "the outcomes come back")
    check(len(list((fresh / "collectors/bls_api_v2/requests").glob("*.json"))) == 1, "the BLS ledger comes back, so the request budget holds across tasks")
    check(publisher_run.restore_state(Cloud({}), tmp_path / "empty", environ) is None, "a first task starts empty")


def test_a_second_save_never_replaces_the_first(tmp_path: Path) -> None:
    root = tmp_path / "state"
    fill_state(root, ["x"])
    cloud = Cloud({})
    environ = environment(tmp_path, "0" * 64)
    first = publisher_run.save_state(cloud, root, environ)
    (root / "outcomes/other.json").write_text("{}")
    second = publisher_run.save_state(cloud, root, environ)
    check(first != second and first in cloud.objects and second in cloud.objects, "each save creates its own object")
    check(publisher_run.restore_state(cloud, tmp_path / "next", environ) == second, "the latest save wins")


def test_the_state_is_saved_even_when_the_harness_stops(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    stage, digest = staged()
    cloud = Cloud(stage)

    def stops(*_args: Any, **_kwargs: Any) -> dict:
        raise ValueError("stopped")

    def saved_run(args: Any, _environment: Any, _factory: Any) -> harness.Run:
        fill_state(args.state_root, ["x"])
        return harness.Run(args.queue, args.state_root)

    monkeypatch.setattr(harness, "run_from", saved_run)
    monkeypatch.setattr(harness, "execute", stops)
    with pytest.raises(ValueError, match="stopped"):
        publisher_run.main(environment(tmp_path, digest), cloud.factory)
    check(any(key.startswith("raw/run3_state/") for key in cloud.puts), "the records of a stopped run are kept for the next task")


@pytest.mark.parametrize("missing", ["RUN_ID", "PUBLISHER_INPUTS_SHA256", "PUBLISHER_QUEUE_SHA256"])
def test_a_missing_variable_stops_the_job_before_any_read(tmp_path: Path, missing: str) -> None:
    stage, digest = staged()
    cloud = Cloud(stage)
    environ = environment(tmp_path, digest)
    del environ[missing]
    with pytest.raises(publisher_run.PublisherError, match=missing):
        publisher_run.main(environ, cloud.factory)
    check(cloud.puts == [], "nothing was written")
