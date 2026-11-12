"""E2E of the automatic layer acceptance report that the DAG writes: the before listing of the job task, the evidence task and the DAG wiring of both.

    .venv/bin/python -m scripts.lakehouse.run_layer_report_e2e --out <NEW_DIRECTORY>

The runs have the shapes of ``scripts.lakehouse.run_layer_acceptance_e2e`` (the records the layer driver writes, with the frozen contract tables).
They are served by stand-in S3, ECS and Auto Scaling clients, so every call of ``orchestration.layer_report`` and of ``orchestration.airflow_steps`` runs for
real against those answers. The publish contract is read from a staged ``code.tar.gz`` that holds the files of this checkout's revision, after the archive
passed its size limit and its ``code.sha256``. Every run, digest and instance ID here is SYNTHETIC. Nothing calls AWS or Docker.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import shutil
import stat
import sys
import tarfile
import tempfile
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from botocore.exceptions import ClientError

from orchestration import airflow_steps, layer_report
from orchestration.interface import SubmissionRequest
from scripts.lakehouse import layer_acceptance as acceptance
from scripts.lakehouse import run_layer_acceptance_e2e as fixtures

BUCKET = "example-project-data-dev"
CLUSTER = "example_project_cluster_dev"
PREFIX = "example_project_dev_job_"
GROUP = "example_project_host_dev"
OTHER_INSTANCE = "i-0fedcba9876543210"
UNRESOLVED_INSTANCE = "i-0aaaabbbbccccdddd"
NAMES = fixtures.NAMES
ARCHIVE_PATHS = (
    *acceptance.CONTRACT.values(),
    acceptance.BRONZE_MAP,
    str(acceptance.pins.SOURCES),
)


def build_archive(revision: str = fixtures.REVISION, extra: Callable[[tarfile.TarFile], None] | None = None) -> bytes:
    """A ``code.tar.gz`` as ``scripts.lakehouse.code_archive`` writes it for the files this suite needs: the commit ID is the global pax comment."""
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz", format=tarfile.PAX_FORMAT, pax_headers={"comment": revision}) as tar:
        for path in ARCHIVE_PATHS:
            body = fixtures.frozen_text(path).encode()
            info = tarfile.TarInfo(path)
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
        if extra:
            extra(tar)
    return stream.getvalue()


ARCHIVE = build_archive()
ARCHIVE_SHA256 = hashlib.sha256(ARCHIVE).hexdigest()


def expect(condition: bool) -> None:
    """A stand-in refuses a call that the real service would refuse."""
    if not condition:
        raise ValueError("the stand-in was called with arguments the service refuses")


class FakeS3:
    """Objects by key and the version listing of the lakehouse. A missing key answers NoSuchKey, as S3 does for a role that may list."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.entries: list[dict[str, Any]] = []

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        if Bucket != BUCKET or Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "absent"}}, "GetObject")
        data = self.objects[Key]
        return {"Body": io.BytesIO(data), "ContentLength": len(data), "ETag": '"e"'}

    def list_object_versions(self, **arguments: Any) -> dict[str, Any]:
        return fixtures.FakeBucket(self.entries).list_object_versions(**arguments)


class FakeEcs:
    def __init__(self, box: Box) -> None:
        self.box = box

    def describe_tasks(self, cluster: str, tasks: list[str]) -> dict[str, Any]:
        document = json.loads((self.box.current.root / "describe.json").read_text())
        return document if tasks == [document["tasks"][0]["taskArn"]] else {"failures": [{"reason": "MISSING"}], "tasks": []}

    def list_tasks(self, **arguments: Any) -> dict[str, Any]:
        expect(arguments["cluster"] == CLUSTER and arguments["desiredStatus"] == "RUNNING")
        expect(arguments["family"] == f"{PREFIX}{self.box.current.spec.layer}_host")
        return {"taskArns": [*json.loads((self.box.current.root / "running.json").read_text())["taskArns"], *self.box.later_tasks]}

    def list_container_instances(self, **arguments: Any) -> dict[str, Any]:
        expect(arguments["cluster"] == CLUSTER and arguments["status"] == "ACTIVE")
        names = [*self.box.host()["container_instances"], *([UNRESOLVED_INSTANCE] if self.box.unresolved else [])]
        return {"containerInstanceArns": [f"arn:ci/{name}" for name in names]}

    def describe_container_instances(self, cluster: str, containerInstances: list[str]) -> dict[str, Any]:
        """The service answers what it can: an instance it cannot describe is a failures entry (``unresolved == "failure"``) or is silently left out."""
        found = [arn for arn in containerInstances if not (self.box.unresolved and arn == f"arn:ci/{UNRESOLVED_INSTANCE}")]
        failures = (
            [{"arn": f"arn:ci/{UNRESOLVED_INSTANCE}", "reason": "MISSING"}] if self.box.unresolved == "failure" and len(found) < len(containerInstances) else []
        )
        return {
            "containerInstances": [{"containerInstanceArn": arn, "ec2InstanceId": arn.removeprefix("arn:ci/")} for arn in found],
            "failures": failures,
        }


class FakeAutoscaling:
    def __init__(self, box: Box) -> None:
        self.box = box

    def describe_auto_scaling_groups(self, AutoScalingGroupNames: list[str]) -> dict[str, Any]:
        host = self.box.host()
        found = [
            {
                "AutoScalingGroupName": GROUP,
                "DesiredCapacity": host["desired_capacity"],
                "Instances": [{"InstanceId": name, "LifecycleState": "InService"} for name in host["group_instances"]],
            }
        ]
        return {"AutoScalingGroups": found if AutoScalingGroupNames == [GROUP] else []}


class Box:
    """One sandbox: the stand-in AWS clients, the report folder and the run whose fixtures the clients answer from."""

    def __init__(self, work: Path, name: str) -> None:
        self.s3 = FakeS3()
        self.current: Any = None
        self.unresolved = ""
        self.later_tasks: list[str] = []
        self.access = layer_report.ReportAccess(
            s3=self.s3,
            bucket=BUCKET,
            ecs=FakeEcs(self),
            autoscaling=FakeAutoscaling(self),
            cluster=CLUSTER,
            family_prefix=PREFIX,
            host_group=GROUP,
            directory=work / name / "reports",
        )

    def host(self) -> dict[str, Any]:
        return dict(json.loads((self.current.root / "host.json").read_text()))


def spec(layer: str, run: str, tag: str, code: str = ARCHIVE_SHA256) -> fixtures.Spec:
    return replace(fixtures.spec(layer, run, tag), code=code)


def entries_of(document: dict[str, Any]) -> list[dict[str, Any]]:
    versions = [{"Key": i["key"], "VersionId": i["version"], "ETag": i["etag"], "Size": i["bytes"]} for i in document["versions"]]
    markers = [{"Key": i["key"], "VersionId": i["version"], "Marker": True} for i in document.get("deleted") or []]
    return sorted(versions + markers, key=lambda entry: entry["Key"])


def stage(box: Box, env: fixtures.Env, archive: bytes = ARCHIVE) -> None:
    """Put the objects of one run where the evidence task reads them."""
    run, layer = env.spec.run, env.spec.layer
    for path in env.evidence.rglob("*.json"):
        box.s3.objects[f"lakehouse/layer_runs/{layer}/{run}/{path.relative_to(env.evidence)}"] = path.read_bytes()
    box.s3.objects[f"lakehouse/run_stage/{run}/inputs/code.sha256"] = (env.spec.code_sha256 + "\n").encode()
    box.s3.objects[f"lakehouse/run_stage/{run}/inputs/code.tar.gz"] = archive
    for path in (env.root / "launch").rglob("*.json"):
        box.s3.objects[f"lakehouse/launch_records/{run}/{layer}/{path.relative_to(env.root / 'launch')}"] = path.read_bytes()


def sandbox(work: Path, name: str) -> Box:
    return Box(work, name)


def make(
    work: Path, name: str, layer: str, producer: fixtures.Env | None = None, run: str | None = None, tag: str | None = None, code: str = ARCHIVE_SHA256
) -> fixtures.Env:
    runs = {"silver": (fixtures.SILVER_RUN, "a"), "gold": (fixtures.GOLD_RUN, "c")}
    env = fixtures.Env(work / name, spec(layer, run or runs[layer][0], tag or runs[layer][1], code), producer)
    fixtures.build_records(env)
    return env


def job_window(box: Box, env: fixtures.Env, waiting: tuple[dict[str, Any], ...] = (), running: tuple[dict[str, Any], ...] = ()) -> None:
    """The job task: other runs write while this run waits for the pool slot, the task saves the listing right before the launch, the job writes and the
    task saves the closing evidence right after the launch, while it still holds the slot."""
    box.current = env
    before = entries_of(json.loads((env.root / "before.json").read_text()))
    box.s3.entries = sorted([*before, *waiting], key=lambda entry: entry["Key"])
    layer_report.save_before_listing(box.access, env.spec.layer, env.spec.run)
    after = entries_of(json.loads((env.root / "after.json").read_text()))
    box.s3.entries = sorted([*after, *waiting, *running], key=lambda entry: entry["Key"])
    layer_report.save_closing_evidence(box.access, env.spec.layer, env.spec.run)


def attempt(function: Callable[[], Any]) -> tuple[str, Any]:
    try:
        return "ok", function()
    except layer_report.ReportFailed as failure:
        return "failed", failure
    except layer_report.ReportError as error:
        return "error", error


def accept(box: Box, env: fixtures.Env) -> tuple[str, Any]:
    box.current = env
    return attempt(lambda: layer_report.accept(box.access, env.spec.layer, env.spec.run))


def flow(box: Box, env: fixtures.Env, **window: Any) -> tuple[str, Any]:
    stage(box, env)
    job_window(box, env, **window)
    return accept(box, env)


def other_run_records(layer: str) -> tuple[dict[str, Any], ...]:
    """The keys that another run (the queued ingestion job) writes under its own prefixes: its launch records and its staged inputs."""
    other = "20261111T210000Z-0badc0de"
    names = (
        f"launch_records/{other}/ingestion/claim.json",
        f"launch_records/{other}/ingestion/01/intent.json",
        f"launch_records/{other}/ingestion/01/started.json",
        f"run_stage/{other}/inputs/code.sha256",
        f"run_stage/{other}/inputs/code.tar.gz",
    )
    return tuple({"Key": f"lakehouse/{name}", "VersionId": f"o{index}", "ETag": '"o"', "Size": 9} for index, name in enumerate(names))


def failed_names(result: tuple[str, Any]) -> list[str]:
    return sorted(result[1].failed) if result[0] == "failed" else []


def good_cases(work: Path) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    box = sandbox(work, "good")
    silver = make(work, "good_silver", "silver")
    kind, found = flow(box, silver)
    path = Path(found["report"]) if kind == "ok" else Path("missing")
    checks["silver: the evidence task writes a passed report"] = kind == "ok" and found["passed"] is True and path.is_file()
    document = json.loads(path.read_text()) if path.is_file() else {}
    checks["silver: the report passed every check and names the run"] = document.get("passed") is True and (document.get("layer"), document.get("run_id")) == (
        "silver",
        silver.spec.run,
    )
    checks["silver: the report folder is private (0700) and the report is private (0600)"] = (
        path.is_file() and stat.S_IMODE(path.stat().st_mode) == 0o600 and stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    )
    before = box.access.directory / "silver" / silver.spec.run / "before.json"
    checks["silver: the before listing is private (0600)"] = before.is_file() and stat.S_IMODE(before.stat().st_mode) == 0o600
    checks["silver: the report hashes the archive-checked contract inputs, the launch records and the staged digest"] = all(
        name in document.get("file_sha256", {})
        for name in ("code_digest", "launch/claim.json", "launch/01/started.json", "launch/01/end.json", "before", "after")
    )
    kind_again, again = accept(box, silver)
    checks["silver: a second evidence run returns the same passed report and writes nothing new"] = (
        kind_again == "ok" and again["report"] == found["report"] and path.read_bytes() == json.dumps(document, indent=2, sort_keys=True).encode() + b"\n"
    )
    gold = make(work, "good_gold", "gold", silver)
    kind, found = flow(box, gold)
    checks["gold: the evidence task reads the passed silver report and passes"] = kind == "ok" and found["passed"] is True
    return checks


def window_cases(work: Path) -> dict[str, bool]:
    """Other runs write launch records while this run waits for the pool slot: the listing is taken in the job task, after the wait."""
    checks: dict[str, bool] = {}
    for layer in ("silver", "gold"):
        box = sandbox(work, f"window_{layer}")
        producer = None
        if layer == "gold":
            producer = make(work, "window_gold_producer", "silver")
            flow(box, producer)
        env = make(work, f"window_{layer}_run", layer, producer)
        kind, found = flow(box, env, waiting=other_run_records(layer))
        checks[f"{layer}: another run's launch records written while this run waits do not count as this run's writes"] = (
            kind == "ok" and found["passed"] is True
        )
        other = make(work, f"window_{layer}_late", layer, producer, run="20261112T090000Z-0a0a0a0a" if layer == "silver" else "20261112T100000Z-0b0b0b0b")
        box2 = sandbox(work, f"window_late_{layer}")
        if producer is not None:
            flow(box2, producer)
        result = flow(box2, other, running=other_run_records(layer))
        checks[f"{layer}: another run's launch records written after the listing stay a failed write scope check"] = result[0] == "failed" and NAMES[
            "scope"
        ] in failed_names(result)
        saved = box2.access.directory / layer / other.spec.run / "report.json"
        document = json.loads(saved.read_text()) if saved.is_file() else {}
        wanted = sorted(entry["Key"] for entry in other_run_records(layer))
        checks[f"{layer}: the failed report lists every key outside the run's scope, with their count"] = document.get("out_of_scope") == {
            "count": len(wanted),
            "keys": wanted,
        }
        checks[f"{layer}: a passed report lists no key outside the run's scope"] = box.access.directory.joinpath(
            layer, env.spec.run, "report.json"
        ).is_file() and (
            json.loads(box.access.directory.joinpath(layer, env.spec.run, "report.json").read_text()).get("out_of_scope") == {"count": 0, "keys": []}
        )
    return checks


def later_job_cases(work: Path) -> dict[str, bool]:
    """The next queued job takes the released pool slot before the evidence task runs: its writes and its task are not this run's evidence."""
    checks: dict[str, bool] = {}
    for layer in ("silver", "gold"):
        box = sandbox(work, f"later_{layer}")
        producer = None
        if layer == "gold":
            producer = make(work, "later_gold_producer", "silver")
            flow(box, producer)
        env = make(work, f"later_{layer}_run", layer, producer)
        stage(box, env)
        job_window(box, env)
        box.s3.entries = sorted([*box.s3.entries, *other_run_records(layer)], key=lambda entry: entry["Key"])
        box.later_tasks.append(f"arn:aws:ecs:us-west-2:{fixtures.ACCOUNT}:task/x/9999")
        result = accept(box, env)
        checks[f"{layer}: a queued job that writes and runs after this job released the slot does not fail this run's evidence"] = (
            result[0] == "ok" and result[1]["passed"] is True
        )
    return checks


def listing_cases(work: Path) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    box = sandbox(work, "listing")
    env = make(work, "listing_run", "silver")
    other = make(work, "listing_other", "silver", run=fixtures.OTHER_SILVER_RUN, tag="b")
    stage(box, env)
    box.current = env
    box.s3.entries = entries_of(json.loads((env.root / "before.json").read_text()))
    first = layer_report.save_before_listing(box.access, "silver", env.spec.run)
    saved = first.read_bytes()
    document = json.loads(saved)
    checks["the listing file holds the run, the layer, every version and every delete marker"] = (
        (document.get("run_id"), document.get("layer")) == (env.spec.run, "silver")
        and len(document.get("versions", [])) == len([e for e in box.s3.entries if not e.get("Marker")])
        and len(document.get("deleted", [])) == len([e for e in box.s3.entries if e.get("Marker")])
    )
    box.s3.entries = box.s3.entries[:5]
    second = layer_report.save_before_listing(box.access, "silver", env.spec.run)
    checks["the listing is created once: a second save keeps the first file"] = second == first and first.read_bytes() == saved
    checks["the listing folder is private (0700)"] = stat.S_IMODE(first.parent.stat().st_mode) == 0o700
    # the evidence task refuses a missing file and names the cause
    box2 = sandbox(work, "listing_missing")
    stage(box2, env)
    box2.current = env
    result = accept(box2, env)
    checks["a missing before listing fails visibly and names the cause"] = (
        result[0] == "error" and "before listing" in str(result[1]) and "missing" in str(result[1])
    )
    # a file of another run under this run's name
    box3 = sandbox(work, "listing_wrong")
    stage(box3, env)
    box3.current = other
    box3.s3.entries = entries_of(json.loads((other.root / "before.json").read_text()))
    layer_report.save_before_listing(box3.access, "silver", other.spec.run)
    target = box3.access.directory / "silver" / env.spec.run
    target.mkdir(parents=True, mode=0o700)
    shutil.copyfile(box3.access.directory / "silver" / other.spec.run / "before.json", target / "before.json")
    os.chmod(target / "before.json", 0o600)
    result = accept(box3, env)
    checks["a before listing of another run fails visibly and names the cause"] = result[0] == "error" and "another run" in str(result[1])
    box4 = sandbox(work, "listing_corrupt")
    stage(box4, env)
    folder = box4.access.directory / "silver" / env.spec.run
    folder.mkdir(parents=True, mode=0o700)
    (folder / "before.json").write_text("{not json")
    os.chmod(folder / "before.json", 0o600)
    result = accept(box4, env)
    checks["a corrupt before listing fails visibly"] = result[0] == "error" and "before listing" in str(result[1])
    box5 = sandbox(work, "listing_open")
    stage(box5, env)
    folder = box5.access.directory / "silver" / env.spec.run
    folder.mkdir(parents=True, mode=0o700)
    (folder / "before.json").write_text(json.dumps({"run_id": env.spec.run, "layer": "silver", "versions": [], "deleted": []}))
    os.chmod(folder / "before.json", 0o644)
    result = accept(box5, env)
    checks["a before listing that others can read fails visibly"] = result[0] == "error" and "permissions" in str(result[1])
    box6 = sandbox(work, "listing_bound")
    box6.s3.entries = entries_of(json.loads((env.root / "before.json").read_text()))
    bounded = attempt(lambda: layer_report.save_before_listing(box6.access, "silver", env.spec.run, limit=1000))
    checks["a listing above the size bound fails visibly and leaves no file"] = (
        bounded[0] == "error" and "larger" in str(bounded[1]) and not (box6.access.directory / "silver" / env.spec.run / "before.json").exists()
    )
    box7 = sandbox(work, "listing_layer")
    refused = attempt(lambda: layer_report.save_before_listing(box7.access, "features", env.spec.run))
    checks["only silver and gold take a listing"] = refused[0] == "error"
    refused = attempt(lambda: layer_report.save_before_listing(box7.access, "silver", "not-a-run"))
    checks["a run ID of the wrong form is refused"] = refused[0] == "error"
    return checks


def cached_cases(work: Path) -> dict[str, bool]:
    """A retry of the evidence task with a passed report on disk checks the before listing like the first run: the cached pass is not a bypass."""
    checks: dict[str, bool] = {}

    def retry(number: int, change: Callable[[Path, fixtures.Env], None]) -> tuple[str, Any]:
        box = sandbox(work, f"cached_{number}")
        env = make(work, f"cached_{number}_run", "silver")
        first = flow(box, env)
        before = box.access.directory / "silver" / env.spec.run / "before.json"
        if first[0] != "ok":
            return "first run failed", first[1]
        change(before, env)
        return accept(box, env)

    def delete(path: Path, env: fixtures.Env) -> None:
        path.unlink()

    def damage(path: Path, env: fixtures.Env) -> None:
        path.unlink()
        path.write_text("{not json")
        os.chmod(path, 0o600)

    def foreign(path: Path, env: fixtures.Env) -> None:
        fixtures.edit(path, lambda d: d.update(run_id=fixtures.OTHER_SILVER_RUN))

    def readable(path: Path, env: fixtures.Env) -> None:
        os.chmod(path, 0o644)

    def other_content(path: Path, env: fixtures.Env) -> None:
        fixtures.edit(path, lambda d: d["versions"].append({"key": "lakehouse/silver/extra.parquet", "version": "v9", "etag": '"e"', "bytes": 1}))

    for number, (label, change, word) in enumerate(
        (
            ("missing", delete, "missing"),
            ("damaged", damage, "damaged"),
            ("of another run", foreign, "another run"),
            ("readable by others", readable, "permissions"),
            ("changed", other_content, "before listing"),
        )
    ):
        result = retry(number, change)
        checks[f"a retry with a passed report and a before listing that is {label} fails visibly"] = result[0] == "error" and word in str(result[1])
    result = retry(9, lambda path, env: None)
    checks["a retry with a passed report and the same before listing returns the passed report"] = result[0] == "ok" and result[1]["passed"] is True
    return checks


def archive_cases(work: Path) -> dict[str, bool]:
    checks: dict[str, bool] = {}

    def run_with(archive: bytes, label: str, digest: str | None = None, limit: int | None = None, bound: bool = False) -> tuple[str, Any]:
        box = sandbox(work, f"archive_{label}")
        env = make(work, f"archive_{label}_run", "silver", code=hashlib.sha256(archive).hexdigest() if bound else ARCHIVE_SHA256)
        stage(box, env, archive)
        if digest is not None:
            box.s3.objects[f"lakehouse/run_stage/{env.spec.run}/inputs/code.sha256"] = (digest + "\n").encode()
        job_window(box, env)
        if limit is not None:
            return attempt(lambda: layer_report.accept(box.access, "silver", env.spec.run, archive_limit=limit))
        return accept(box, env)

    checks["an archive that does not match code.sha256 is refused"] = (lambda r: r[0] == "error" and "code.sha256" in str(r[1]))(
        run_with(ARCHIVE, "digest", digest="f" * 64)
    )
    checks["an archive above the size limit is refused"] = (lambda r: r[0] == "error" and "larger" in str(r[1]))(run_with(ARCHIVE, "large", limit=100))
    checks["an archive of another commit than the run's revision is refused"] = (lambda r: r[0] == "error" and "revision" in str(r[1]))(
        run_with(other_commit := build_archive("1" * 40), "commit", digest=hashlib.sha256(other_commit).hexdigest())
    )

    def without(path: str) -> bytes:
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w:gz", format=tarfile.PAX_FORMAT, pax_headers={"comment": fixtures.REVISION}) as tar:
            for name in ARCHIVE_PATHS:
                if name != path:
                    body = fixtures.frozen_text(name).encode()
                    info = tarfile.TarInfo(name)
                    info.size = len(body)
                    tar.addfile(info, io.BytesIO(body))
        return stream.getvalue()

    missing = without(acceptance.CONTRACT["silver"])
    checks["an archive without the publish contract is refused"] = (lambda r: r[0] in ("error", "failed"))(
        run_with(missing, "missing", digest=hashlib.sha256(missing).hexdigest())
    )

    def linked() -> bytes:
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w:gz", format=tarfile.PAX_FORMAT, pax_headers={"comment": fixtures.REVISION}) as tar:
            for name in ARCHIVE_PATHS:
                if name == acceptance.CONTRACT["silver"]:
                    info = tarfile.TarInfo(name)
                    info.type = tarfile.SYMTYPE
                    info.linkname = "/etc/hostname"
                    tar.addfile(info)
                else:
                    body = fixtures.frozen_text(name).encode()
                    info = tarfile.TarInfo(name)
                    info.size = len(body)
                    tar.addfile(info, io.BytesIO(body))
        return stream.getvalue()

    link = linked()
    checks["a contract that is a link in the archive is refused"] = (lambda r: r[0] in ("error", "failed"))(
        run_with(link, "link", digest=hashlib.sha256(link).hexdigest())
    )

    def oversized() -> bytes:
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w:gz", format=tarfile.PAX_FORMAT, pax_headers={"comment": fixtures.REVISION}) as tar:
            for name in ARCHIVE_PATHS:
                body = fixtures.frozen_text(name).encode() + (b" " * (acceptance.MEMBER_LIMIT + 1) if name == acceptance.CONTRACT["silver"] else b"")
                info = tarfile.TarInfo(name)
                info.size = len(body)
                tar.addfile(info, io.BytesIO(body))
        return stream.getvalue()

    big = oversized()
    checks["a contract file above the member limit is refused"] = (lambda r: r[0] in ("error", "failed"))(
        run_with(big, "member", digest=hashlib.sha256(big).hexdigest())
    )

    def with_tail(size: int) -> bytes:
        def tail(tar: tarfile.TarFile) -> None:
            info = tarfile.TarInfo("unrelated/tail.bin")
            info.size = size
            tar.addfile(info, io.BytesIO(b"\0" * size))

        return build_archive(extra=tail)

    tail_archive = with_tail(600_000)
    with tarfile.open(fileobj=io.BytesIO(tail_archive), mode="r:gz") as tar:
        starts = [member.offset_data for member in tar if member.name == "unrelated/tail.bin"]
    bound = starts[0] + 1000  # the last member starts below the bound and ends far above it
    with mock.patch.object(acceptance, "UNPACKED_LIMIT", bound):
        crossing = run_with(tail_archive, "tail", bound=True)
    checks["an archive whose last member ends above the unpacked limit is refused"] = crossing[0] == "error" and "too large" in str(crossing[1])
    with mock.patch.object(acceptance, "UNPACKED_LIMIT", starts[0] + 600_000 + 2048):
        inside = run_with(tail_archive, "tail_inside", bound=True)
    checks["an archive whose last member ends inside the unpacked limit is read"] = inside[0] == "ok"
    box = sandbox(work, "archive_nothing_written")
    env = make(work, "archive_nothing_written_run", "silver")
    stage(box, env, ARCHIVE)
    box.s3.objects[f"lakehouse/run_stage/{env.spec.run}/inputs/code.sha256"] = b"f" * 64 + b"\n"
    job_window(box, env)
    accept(box, env)
    checks["a refused archive leaves no report and no extracted file"] = not (
        box.access.directory / "silver" / env.spec.run / "report.json"
    ).exists() and not any(path.suffix in (".gz", ".tar") for path in box.access.directory.rglob("*"))
    return checks


def host_cases(work: Path) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    plan: list[tuple[str, Callable[[dict[str, Any]], None], str]] = [
        ("a second container instance", lambda d: d["container_instances"].append(OTHER_INSTANCE), NAMES["host"]),
        ("a group desired capacity of two", lambda d: d.update(desired_capacity=2), NAMES["host"]),
        ("a container instance that is not the host", lambda d: d.update(container_instances=[OTHER_INSTANCE]), NAMES["host"]),
        ("no active container instance", lambda d: d.update(container_instances=[]), NAMES["host"]),
        ("a second group instance", lambda d: d["group_instances"].append(OTHER_INSTANCE), NAMES["host"]),
    ]
    for number, (label, change, name) in enumerate(plan):
        box = sandbox(work, f"host_{number}")
        env = make(work, f"host_{number}_run", "silver")
        fixtures.edit(env.root / "host.json", change)
        result = flow(box, env)
        checks[f"{label}: the report fails the host check and the evidence task fails"] = result[0] == "failed" and name in failed_names(result)

    def partial(mode: str) -> tuple[tuple[str, Any], tuple[str, Any]]:
        box = sandbox(work, f"host_partial_{mode}")
        env = make(work, f"host_partial_{mode}_run", "silver")
        box.unresolved = mode
        stage(box, env)
        return attempt(lambda: job_window(box, env)), accept(box, env)

    for label, mode in (("a failures entry", "failure"), ("a silently missing instance", "silent")):
        job, result = partial(mode)
        refusal = next((found for kind, found in (job, result) if kind == "error"), None)
        checks[f"a container instance that the description does not resolve ({label}) stops the host evidence and no report passes"] = (
            refusal is not None and "container instance" in str(refusal) and result[0] != "ok"
        )
    box = sandbox(work, "host_task")
    env = make(work, "host_task_run", "silver")
    fixtures.write(env.root / "running.json", {"taskArns": [f"arn:aws:ecs:us-west-2:{fixtures.ACCOUNT}:task/x/0123"]})
    result = flow(box, env)
    checks["a job task still running fails the running check"] = result[0] == "failed" and NAMES["running"] in failed_names(result)
    path = box.access.directory / "silver" / env.spec.run / "report.json"
    checks["a failed report is kept for the record and a second evidence run does not pass it"] = (
        path.is_file() and json.loads(path.read_text())["passed"] is False and accept(box, env)[0] == "failed"
    )
    checks["a failed report names its failed checks and echoes nothing from an input"] = "0123" not in str(result[1])
    return checks


def dependency_cases(work: Path) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    box = sandbox(work, "dependency_missing")
    silver = make(work, "dependency_missing_silver", "silver")
    gold = make(work, "dependency_missing_gold", "gold", silver)
    stage(box, silver)
    result = flow(box, gold)
    checks["a gold run without the silver report fails visibly and names the cause"] = result[0] == "error" and "silver report" in str(result[1])
    box = sandbox(work, "dependency_failed")
    silver = make(work, "dependency_failed_silver", "silver")
    fixtures.edit(silver.root / "host.json", lambda d: d.update(desired_capacity=3))
    failed = flow(box, silver)
    gold = make(work, "dependency_failed_gold", "gold", silver)
    fixtures.edit(silver.root / "host.json", lambda d: d.update(desired_capacity=1))
    result = flow(box, gold)
    checks["a gold run after a failed silver report fails the producer check"] = (
        failed[0] == "failed" and result[0] == "failed" and NAMES["passed"] in failed_names(result)
    )
    box = sandbox(work, "dependency_other_run")
    silver = make(work, "dependency_other_silver", "silver")
    flow(box, silver)
    other = make(work, "dependency_other_other", "silver", run=fixtures.OTHER_SILVER_RUN, tag="b")
    gold = make(work, "dependency_other_gold", "gold", other)
    fixtures.edit(gold.evidence / "silver_pins.json", lambda d: d.update(source_run_id=silver.spec.run))
    result = flow(box, gold)
    checks["a gold pin that names a silver run whose report is for another input fails"] = result[0] in ("failed", "error")
    return checks


def wiring_cases(work: Path) -> dict[str, bool]:
    """``job_step`` takes the listing after the gates and right before the launch, ``preflight_step`` takes none and ``evidence_step`` writes the report."""
    checks: dict[str, bool] = {}
    box = sandbox(work, "wiring")
    env = make(work, "wiring_run", "silver")
    stage(box, env)
    box.current = env
    store = airflow_steps.S3LaunchStore(box.s3, BUCKET, airflow_steps.launch_record_prefix())
    found = SimpleNamespace(preflight=SimpleNamespace(store=store), report=box.access)
    request = SubmissionRequest("pin_bronze", env.spec.run, "real", 1, "silver_transform_dag", "job", "manual__x")
    order: list[str] = []
    listing = box.access.directory / "silver" / env.spec.run / "before.json"

    def launched(services_: Any, spec_: Any, context: Any) -> None:
        order.append("launch")
        checks["job_step: the listing exists when the launch starts"] = listing.is_file()
        box.s3.entries = entries_of(json.loads((env.root / "after.json").read_text()))

    def gates(services_: Any, request_: Any, context: Any, selections: Any) -> Any:
        order.append("gates")
        checks["job_step: the gates run before the listing exists"] = not listing.is_file()
        return SimpleNamespace(job="silver")

    box.s3.entries = entries_of(json.loads((env.root / "before.json").read_text()))
    with (
        mock.patch.object(airflow_steps, "services", lambda: found),
        mock.patch.object(airflow_steps, "job_spec", gates),
        mock.patch.object(airflow_steps, "launch", launched),
    ):
        named = airflow_steps.preflight_step(request, {}, {})
        pre = named == "silver" and not listing.is_file()
        order.clear()
        outcome = airflow_steps.job_step(request, {}, {})
        done = airflow_steps.evidence_step(request, {}, {})
    checks["preflight_step takes no listing (it runs before the pool slot)"] = pre
    checks["job_step: the order is the gates, the listing and then the launch"] = order == ["gates", "launch"] and outcome == "succeeded"
    closing = box.access.directory / "silver" / env.spec.run / "closing.json"
    checks["job_step: the closing evidence is saved right after the launch, private, before the task ends"] = (
        closing.is_file() and stat.S_IMODE(closing.stat().st_mode) == 0o600
    )
    checks["evidence_step: the launch records and the report pass and the step returns the report"] = (
        done.get("job") == "silver" and done.get("passed") is True and Path(done["report"]).is_file()
    )
    box2 = sandbox(work, "wiring_failed")
    env2 = make(work, "wiring_failed_run", "silver")
    fixtures.edit(env2.root / "launch/01/end.json", lambda d: d.update(outcome="failed", exit_code=1))
    stage(box2, env2)
    job_window(box2, env2)
    found2 = SimpleNamespace(
        preflight=SimpleNamespace(store=airflow_steps.S3LaunchStore(box2.s3, BUCKET, airflow_steps.launch_record_prefix())), report=box2.access
    )
    with mock.patch.object(airflow_steps, "services", lambda: found2):
        try:
            airflow_steps.evidence_step(request, {}, {})
            refused = False
        except airflow_steps.LaunchRefused:
            refused = True
    checks["evidence_step: a launch that did not succeed is refused before any report"] = (
        refused and not (box2.access.directory / "silver" / env2.spec.run / "report.json").exists()
    )
    box3 = sandbox(work, "wiring_features")
    found3 = SimpleNamespace(preflight=SimpleNamespace(store=None), report=box3.access)
    with mock.patch.object(airflow_steps, "services", lambda: found3), mock.patch.object(airflow_steps, "job_of", lambda template: "train"):
        try:
            outcome3 = airflow_steps.evidence_step(request, {}, {})
        except Exception:
            outcome3 = {}
    checks["evidence_step: a job that is not silver or gold takes no report"] = outcome3 == {} or "report" not in outcome3
    return checks


def static_cases() -> dict[str, bool]:
    steps = (Path(airflow_steps.__file__)).read_text()
    return {
        "the pool slot job task is the one that takes the listing": "save_before_listing" in steps and "def preflight_step" in steps,
        "the report folder default is a writable container path, never the job scratch": str(layer_report.DEFAULT_DIRECTORY).startswith("/opt/airflow/")
        and "job_scratch" not in str(layer_report.DEFAULT_DIRECTORY),
    }


def group(name: str, function: Callable[..., dict[str, bool]], *arguments: Any) -> dict[str, bool]:
    try:
        return function(*arguments)
    except Exception as error:  # a crash is one failed check, not a lost report
        return {f"{name}: crashed with {type(error).__name__}: {str(error)[:200]}": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--out", type=Path, required=True, help="a new directory for the report")
    args = parser.parse_args()
    out = args.out.expanduser().absolute()
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    work = Path(tempfile.mkdtemp(prefix="SYNTHETIC_layer_report_", dir=out))
    results: dict[str, dict[str, bool]] = {"static": static_cases()}
    try:
        for name, function in (
            ("good", good_cases),
            ("window", window_cases),
            ("later", later_job_cases),
            ("listing", listing_cases),
            ("cached", cached_cases),
            ("archive", archive_cases),
            ("host", host_cases),
            ("dependency", dependency_cases),
            ("wiring", wiring_cases),
        ):
            results[name] = group(name, function, work)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    passed = all(all(checks.values()) for checks in results.values())
    (out / "report.json").write_text(json.dumps({"passed": passed, "results": results}, indent=2, sort_keys=True) + "\n")
    for name, checks in results.items():
        for check, ok in checks.items():
            sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}: {check}\n")
    total = sum(len(checks) for checks in results.values())
    sys.stdout.write(f"layer report E2E: {sum(ok for checks in results.values() for ok in checks.values())} of {total} checks passed\n")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
