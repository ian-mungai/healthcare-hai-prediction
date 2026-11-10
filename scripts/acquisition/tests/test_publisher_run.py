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

from scripts.acquisition import publisher_run, stage_publisher, stored_mirror
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
    body = archive(
        files or {"mirror/data/q/queue.json": b"{}", "publisher_hosts.json": b"{}", "publisher_manifest.json": b'{"queue": "mirror/data/q/queue.json"}'}
    )
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
        "PUBLISHER_PRIOR_STATE": "",
        "PUBLISHER_WORKSPACE": str(workspace),
    } | extra


def test_the_inputs_are_read_by_digest_and_a_changed_archive_is_refused(tmp_path: Path) -> None:
    stage, digest = staged()
    cloud = Cloud(stage)
    inputs = publisher_run.fetch_inputs(cloud, environment(tmp_path, digest))
    check((inputs / "mirror/data/q/queue.json").read_bytes() == b"{}", "the archive is unpacked under the workspace inputs")
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


def saved_reference(cloud: Cloud, key: str) -> str:
    body = cloud.objects[key]
    return json.dumps({"key": key, "version_id": "v1", "sha256": hashlib.sha256(body).hexdigest()})


def test_the_saved_state_holds_only_the_records_and_a_new_task_restores_the_approved_one(tmp_path: Path) -> None:
    secret = next(iter(PLANTED.values()))
    root = tmp_path / "state"
    fill_state(root, [secret])
    cloud = Cloud({})
    environ = environment(tmp_path, "0" * 64)
    saved = publisher_run.save_state(cloud, root, environ)
    key = saved["key"]
    check(key.startswith(f"raw/run3_state/{QUEUE_SHA256}/") and cloud.puts == [key], f"the state goes under raw/: {key}")
    check(saved["sha256"] == hashlib.sha256(cloud.objects[key]).hexdigest() and saved["version_id"] == "v1", "the save names what the owner approves next")
    with tarfile.open(fileobj=io.BytesIO(cloud.objects[key]), mode="r:gz") as stored:
        names = sorted(stored.getnames())
        bodies = b"".join(reader.read() for member in stored.getmembers() if (reader := stored.extractfile(member)) is not None)
    check(names == ["collectors/bls_api_v2/requests/000001.json", "outcomes/unit.json", "run.json"], f"only the records are saved: {names}")
    check(secret.encode() not in bodies, "nothing else reached the saved state")
    fresh = tmp_path / "next"
    environ |= {"PUBLISHER_PRIOR_STATE": saved_reference(cloud, key)}
    check(publisher_run.restore_state(cloud, fresh, environ) == key, "a new task restores the approved state")
    check(json.loads((fresh / "outcomes/unit.json").read_text())["outcome"] == "exact_match", "the outcomes come back")


def test_a_second_save_never_replaces_the_first(tmp_path: Path) -> None:
    root = tmp_path / "state"
    fill_state(root, ["x"])
    cloud = Cloud({})
    environ = environment(tmp_path, "0" * 64)
    first = publisher_run.save_state(cloud, root, environ)["key"]
    (root / "outcomes/other.json").write_text("{}")
    second = publisher_run.save_state(cloud, root, environ)["key"]
    check(first != second and first in cloud.objects and second in cloud.objects, "each save creates its own object")


def test_the_state_is_saved_even_when_the_harness_stops(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    stage, digest = staged()
    cloud = Cloud(stage)
    bound = hashlib.sha256(b"{}").hexdigest()

    def stops(*_args: Any, **_kwargs: Any) -> dict:
        raise ValueError("stopped")

    def saved_run(args: Any, _environment: Any, _factory: Any) -> harness.Run:
        fill_state(args.state_root, ["x"])
        return harness.Run(args.queue, args.state_root)

    monkeypatch.setattr(harness, "run_from", saved_run)
    monkeypatch.setattr(harness, "execute", stops)
    with pytest.raises(ValueError, match="stopped"):
        publisher_run.main(environment(tmp_path, digest) | {"PUBLISHER_QUEUE_SHA256": bound}, cloud.factory)
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


def stage_files(tmp_path: Path) -> dict[str, Path]:
    built = tmp_path / "built"
    queue = built / "mirror/data/q/queue.json"
    queue.parent.mkdir(parents=True)
    queue.write_bytes(b'{"run": 3, "queue": []}')
    (built / "stored_index.json").write_text("{}")
    files = {"mirror_output": built}
    for name in ("removal_plan", "removal_confirmation", "hosts", "hosts_approval"):
        files[name] = tmp_path / f"{name}.json"
        files[name].write_text("{}")
    return files


def stage(tmp_path: Path, output: str = "stage", resume: tuple[str, ...] = ()) -> Path:
    files = (
        stage_files(tmp_path)
        if not (tmp_path / "built").exists()
        else {name: tmp_path / f"{name}.json" for name in ("removal_plan", "removal_confirmation", "hosts", "hosts_approval")}
        | {"mirror_output": tmp_path / "built"}
    )
    return stage_publisher.build(
        files["mirror_output"],
        "data/q/queue.json",
        files["removal_plan"],
        files["removal_confirmation"],
        files["hosts"],
        files["hosts_approval"],
        tmp_path / output,
        resume,
    )


def test_the_stage_is_deterministic_and_the_request_binds_the_archive_and_queue(tmp_path: Path) -> None:
    first = stage(tmp_path, "a", ("BLS",))
    second = stage(tmp_path, "b", ("BLS",))
    body = (first / "publisher_inputs.tar.gz").read_bytes()
    check(body == (second / "publisher_inputs.tar.gz").read_bytes(), "the same inputs give the same archive")
    request = json.loads((first / "publisher_request.json").read_text())
    check(
        request
        == {
            "inputs_sha256": hashlib.sha256(body).hexdigest(),
            "queue_sha256": hashlib.sha256(b'{"run": 3, "queue": []}').hexdigest(),
            "resume_sources": ["BLS"],
            "prior_state": None,
        },
        str(request),
    )
    with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as saved:
        names = set(saved.getnames())
    check(
        {
            "mirror/data/q/queue.json",
            "stored_index.json",
            "publisher_manifest.json",
            "publisher_hosts.json",
            "hosts_approval.json",
            "removal_plan.json",
            "removal_confirmation.json",
        }
        <= names,
        str(names),
    )


@pytest.mark.parametrize("planted", ["mirror/stored/private_original/r/owners.csv", ".env", "mirror/.env", "mirror/secrets.tfvars.json"])
def test_the_stage_refuses_an_original_or_a_settings_file(tmp_path: Path, planted: str) -> None:
    files = stage_files(tmp_path)
    (files["mirror_output"] / planted).parent.mkdir(parents=True, exist_ok=True)
    (files["mirror_output"] / planted).write_text("x")
    with pytest.raises(stage_publisher.StageError, match="refused"):
        stage(tmp_path)
    check(not (tmp_path / "stage").exists(), "nothing is staged")


def test_the_stage_refuses_a_queue_outside_the_mirror(tmp_path: Path) -> None:
    files = stage_files(tmp_path)
    with pytest.raises(stage_publisher.StageError, match="queue"):
        stage_publisher.build(
            files["mirror_output"],
            "../queue.json",
            files["removal_plan"],
            files["removal_confirmation"],
            files["hosts"],
            files["hosts_approval"],
            tmp_path / "stage",
            (),
        )


def carried(tmp_path: Path, body: bytes = b"county\n") -> Path:
    mirror = tmp_path / "mirror"
    (mirror / "data/refs").mkdir(parents=True)
    (mirror / "data/refs/county.csv").write_bytes(body)
    (mirror / stored_mirror.PLAN_INPUTS_NAME).write_text(json.dumps({"data/refs/county.csv": hashlib.sha256(b"county\n").hexdigest()}))
    return mirror


def test_the_plan_inputs_are_copied_into_the_code_root_as_real_files(tmp_path: Path) -> None:
    code = tmp_path / "code"
    code.mkdir()
    publisher_run.install_plan_inputs(carried(tmp_path), code)
    target = code / "data/refs/county.csv"
    check(target.read_bytes() == b"county\n" and not target.is_symlink(), "the file is a real copy, so the legacy registry's symlink check holds")
    publisher_run.install_plan_inputs(tmp_path / "mirror", code)
    check(target.read_bytes() == b"county\n", "a second install changes nothing")


def test_a_changed_plan_input_or_a_path_that_leaves_data_is_refused(tmp_path: Path) -> None:
    code = tmp_path / "code"
    code.mkdir()
    with pytest.raises(publisher_run.PublisherError, match="digest"):
        publisher_run.install_plan_inputs(carried(tmp_path, b"changed\n"), code)
    check(not (code / "data/refs/county.csv").exists(), "nothing is installed")
    other = tmp_path / "other"
    other.mkdir()
    mirror = carried(other)
    (mirror / stored_mirror.PLAN_INPUTS_NAME).write_text(json.dumps({"data/../../outside.csv": "0" * 64}))
    with pytest.raises(publisher_run.PublisherError, match="path"):
        publisher_run.install_plan_inputs(mirror, code)


def test_a_mirror_without_a_plan_list_installs_nothing(tmp_path: Path) -> None:
    code = tmp_path / "code"
    code.mkdir()
    (tmp_path / "mirror").mkdir()
    publisher_run.install_plan_inputs(tmp_path / "mirror", code)
    check(list(code.iterdir()) == [], "nothing is installed")


def test_the_stage_names_the_approved_prior_state(tmp_path: Path) -> None:
    files = stage_files(tmp_path)
    reference = {"key": "raw/run3_state/q/state.tar.gz", "version_id": "v1", "sha256": "a" * 64}
    folder = stage_publisher.build(
        files["mirror_output"],
        "data/q/queue.json",
        files["removal_plan"],
        files["removal_confirmation"],
        files["hosts"],
        files["hosts_approval"],
        tmp_path / "stage",
        (),
        reference,
    )
    check(json.loads((folder / "publisher_request.json").read_text())["prior_state"] == reference, "the request names it")


def test_the_job_saves_its_state_as_it_goes_so_a_kill_loses_little(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    stage, digest = staged()
    cloud = Cloud(stage)
    environ = environment(tmp_path, digest) | {"PUBLISHER_QUEUE_SHA256": hashlib.sha256(b"{}").hexdigest()}

    def saved_run(args: Any, _environment: Any, _factory: Any) -> harness.Run:
        fill_state(args.state_root, ["x"])
        return harness.Run(args.queue, args.state_root)

    def execute(_run: Any, allow_network: bool, log: Any) -> dict:
        for _ in range(publisher_run.SAVE_EVERY * 2):
            log({"snapshot_id": "u", "source_id": "s", "outcome": "exact_match"})
        raise KeyboardInterrupt

    monkeypatch.setattr(harness, "run_from", saved_run)
    monkeypatch.setattr(harness, "execute", execute)
    with pytest.raises(KeyboardInterrupt):
        publisher_run.main(environ, cloud.factory)
    check(len(cloud.puts) >= 3, f"two saves ran during the work and one at the end: {cloud.puts}")


def test_the_publisher_entry_point_runs_through_the_resource_peaks_recorder() -> None:
    """Like every job entry point, the publisher's ``__main__`` runs ``main`` through ``resource_peaks.recorded``, so its first host run logs its peaks."""
    import ast

    tree = ast.parse(Path(publisher_run.__file__).read_text(encoding="utf-8"))
    guards = [node for node in tree.body if isinstance(node, ast.If) and ast.unparse(node.test) == "__name__ == '__main__'"]
    check(len(guards) == 1, "one __main__ block")
    check([ast.unparse(statement) for statement in guards[0].body] == ["resource_peaks.recorded(main)"], "the __main__ block runs main through the recorder")


def test_a_failed_publisher_start_still_logs_one_resource_peaks_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from scripts import resource_peaks

    for name in publisher_run.REQUIRED:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(publisher_run.PublisherError):
        resource_peaks.recorded(publisher_run.main, scratch=(tmp_path,), interval=0.02)
    lines = [line for line in capsys.readouterr().err.splitlines() if line.startswith("event=resource_peaks ")]
    check(len(lines) == 1, f"one resource_peaks line: {lines}")
    check(json.loads(lines[0].removeprefix("event=resource_peaks "))["exit_code"] == 1, "the failed start is recorded with exit code 1")
