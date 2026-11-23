"""The automatic layer acceptance report of the DAG: the version listing of the job task and the report of the evidence task.

The job task of a silver or gold DAG waits in the launch pool for every slot. When it holds them, it saves the listing of every object version under
``lakehouse/`` (``save_before_listing``) right before the launch, so the writes of another run during the wait are in the listing and not in the run's writes.
It saves the closing evidence (``save_closing_evidence``: the listing after the launch, the description of the stopped task, the tasks of the job still running
and the host facts) right after the launch and before it releases the slot, so a job that takes the slot next cannot change it, and ECS, which describes a
stopped task for about an hour only, is asked while it still does. The evidence task then reads the run's records from AWS and writes
the acceptance report (``accept``) with every check of ``scripts.lakehouse.layer_acceptance``.

Every file lives in a folder of the Airflow container that only its user reads (``<directory>/<layer>/<run>/``, mode 0700, files 0600), never on the job scratch
volume. A file is created once. A missing, damaged, readable-by-others or foreign listing or closing record fails the evidence task with its cause, also when a
report exists already: nothing falls back to a weaker check. A retry with a report on disk reads every input again, the saved records and the AWS records alike,
through the same functions, and returns the report only when each input has the digest that the report records. The publish contract is read from the run's
staged ``code.tar.gz`` after the archive matched ``code.sha256``. A pipeline run stages code under its ingestion run only. The evidence reads the files of
the code run that the job recorded (the ``code_run`` of its ``layer_run.json``), never a binding read later: the task description (its CODE_RUN_ID
overrides) must agree with that record, or the task fails by name. The report checks the code digest of the launch claim against the staged ``code.sha256``
(a failed check). The archive is bounded, is never unpacked to disk and a file is read only when it is a regular member of the expected name.
The report of silver is the input of the gold report (a passed report). Every wait has a deadline."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import stat
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from orchestration.ecs_launch import S3LaunchStore, launch_record_prefix
from orchestration.ecs_preflight import CODE_RUN
from scripts.lakehouse import layer_acceptance as acceptance

LAYERS = ("silver", "gold")
DEFAULT_DIRECTORY = Path("/opt/airflow/logs/layer_reports")
LISTING_PREFIX = "lakehouse/"
LISTING_LIMIT = acceptance.LIMIT
ARCHIVE_LIMIT = acceptance.ARCHIVE_LIMIT
DEADLINE_SECONDS = 720.0  # the evidence task stops at 15 minutes
FAILURE_EVIDENCE_SECONDS = 120.0  # the closing evidence of a failed launch is a best effort: it holds the pool slots, so it gets a short budget
CHUNK = acceptance.CHUNK
LOG = logging.getLogger(__name__)


class ReportError(RuntimeError):
    """A step cannot be done or an input is wrong. The text is fixed and echoes no input."""


class ReportFailed(ReportError):
    """The report was written and a check failed. ``failed`` names the checks."""

    def __init__(self, message: str, failed: list[str]) -> None:
        super().__init__(message)
        self.failed = failed


@dataclass(frozen=True)
class ReportAccess:
    """What the job and evidence tasks use: the clients of the scheduler's launch role, the cluster, the host group and the private report folder."""

    s3: Any
    bucket: str
    ecs: Any
    autoscaling: Any
    cluster: str
    family_prefix: str
    host_group: str
    directory: Path


def aws_error(action: str, error: Exception) -> ReportError:
    code = str(error.response.get("Error", {}).get("Code", "")) if isinstance(error, ClientError) else type(error).__name__
    return ReportError(f"{action} failed ({code or 'ClientError'})")


def folder_of(access: ReportAccess, layer: str, run: str) -> Path:
    """The private folder of a run, made when it is missing. Links are refused."""
    if layer not in LAYERS or not acceptance.RUN_ID.fullmatch(run):
        raise ReportError("the layer or the run ID has the wrong form")
    path = access.directory
    for part in ("", layer, run):
        path = path / part if part else path
        if path.is_symlink():
            raise ReportError("a report folder is a link")
        if not path.exists():
            path.mkdir(mode=0o700, parents=True)
            path.chmod(0o700)
        elif not path.is_dir():
            raise ReportError("a report folder is not a folder")
    return path


def write_private(path: Path, data: bytes) -> None:
    """Create a file once with mode 0600: a temporary file in the same folder is linked to its name, so a reader never sees half a file."""
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            raise ReportError("a record exists: a record is written once") from None
    finally:
        temporary.unlink(missing_ok=True)


def list_versions(s3: Any, bucket: str, limit: int, deadline: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Every version and delete marker under ``lakehouse/``, in the order S3 pages them. The size is bounded and the walk has a deadline."""
    versions: list[dict[str, Any]] = []
    deleted: list[dict[str, Any]] = []
    markers: dict[str, str] = {}
    size = 0
    while True:
        try:
            reply = s3.list_object_versions(Bucket=bucket, Prefix=LISTING_PREFIX, **markers)
        except (ClientError, BotoCoreError) as error:
            raise aws_error("the lakehouse version listing", error) from None
        for item in reply.get("Versions") or []:
            versions.append({"key": item["Key"], "version": item["VersionId"], "etag": item.get("ETag", ""), "bytes": item.get("Size", 0)})
            size += 96 + len(item["Key"]) + len(item["VersionId"]) + len(item.get("ETag", ""))
        for item in reply.get("DeleteMarkers") or []:
            deleted.append({"key": item["Key"], "version": item["VersionId"]})
            size += 48 + len(item["Key"]) + len(item["VersionId"])
        if size > limit:
            raise ReportError("the lakehouse version listing is larger than the report limit")
        if time.monotonic() > deadline:
            raise ReportError("the lakehouse version listing did not finish before its deadline")
        if not reply.get("IsTruncated"):
            return versions, deleted
        markers = {"KeyMarker": reply["NextKeyMarker"], "VersionIdMarker": reply["NextVersionIdMarker"]}


def listing_document(layer: str, run: str, listed: tuple[list[dict[str, Any]], list[dict[str, Any]]]) -> dict[str, Any]:
    return {"schema": 1, "run_id": run, "layer": layer, "prefix": LISTING_PREFIX, "versions": listed[0], "deleted": listed[1]}


def private_bytes(path: Path, what: str, limit: int) -> bytes:
    """A regular file of mode 0600 that is not a link, read within the limit. The cause is named when the file is missing or open to others."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        raise ReportError(f"the {what} is missing") from None
    except OSError:
        raise ReportError(f"the {what} cannot be opened (a link or an unreadable file)") from None
    with os.fdopen(descriptor, "rb") as handle:
        status = os.fstat(handle.fileno())
        if not stat.S_ISREG(status.st_mode):
            raise ReportError(f"the {what} is not a regular file")
        if stat.S_IMODE(status.st_mode) & 0o077:
            raise ReportError(f"the {what} has permissions that let others read it")
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise ReportError(f"the {what} is larger than its limit")
    return data


def checked_listing(path: Path, layer: str, run: str) -> bytes:
    """The bytes of the saved listing, which exists, is private and belongs to this layer and run."""
    data = private_bytes(path, "before listing", LISTING_LIMIT)
    try:
        document = json.loads(data)
    except ValueError:
        raise ReportError("the before listing is damaged (not JSON)") from None
    if not isinstance(document, dict) or (document.get("run_id"), document.get("layer")) != (run, layer):
        raise ReportError("the before listing belongs to another run or layer")
    if not isinstance(document.get("versions"), list) or not isinstance(document.get("deleted"), list):
        raise ReportError("the before listing is damaged (no version lists)")
    return data


def save_before_listing(access: ReportAccess, layer: str, run: str, limit: int = LISTING_LIMIT) -> Path:
    """Save the listing of ``lakehouse/`` once for the run. A listing that exists is kept (and checked): the first one is the one taken at the slot."""
    folder = folder_of(access, layer, run)
    path = folder / "before.json"
    if path.exists() or path.is_symlink():
        checked_listing(path, layer, run)
        return path
    data = json.dumps(listing_document(layer, run, list_versions(access.s3, access.bucket, limit, time.monotonic() + DEADLINE_SECONDS))).encode()
    if len(data) > limit:
        raise ReportError("the lakehouse version listing is larger than the report limit")
    write_private(path, data)
    return path


def read_object(access: ReportAccess, key: str, limit: int, what: str) -> bytes:
    try:
        reply = access.s3.get_object(Bucket=access.bucket, Key=key)
        data: bytes = reply["Body"].read(limit + 1)
    except ClientError as error:
        if str(error.response.get("Error", {}).get("Code", "")) == "NoSuchKey":
            raise ReportError(f"{what} is missing") from None
        raise aws_error(f"the read of {what}", error) from None
    except BotoCoreError as error:
        raise aws_error(f"the read of {what}", error) from None
    if len(data) > limit:
        raise ReportError(f"{what} is larger than its limit")
    return data


def code_run_of(identity: Mapping[str, Any], task: Mapping[str, Any]) -> str:
    """The run whose staged code the evidence reads: the code run that the job recorded in its ``layer_run.json``. The CODE_RUN_ID of every override of the
    described task must name it. A record or override that is missing, malformed or different fails by name and never means another run's code."""
    recorded = identity.get("code_run")
    if not isinstance(recorded, str) or not CODE_RUN.fullmatch(recorded):
        raise ReportError("the recorded code run of the job's layer_run.json is missing or malformed")
    containers = task.get("overrides", {}).get("containerOverrides") if isinstance(task.get("overrides"), dict) else None
    named = {
        entry.get("value")
        for item in containers or []
        if isinstance(item, dict)
        for entry in item.get("environment") or []
        if isinstance(entry, dict) and entry.get("name") == "CODE_RUN_ID"
    }
    if named != {recorded}:
        raise ReportError("the CODE_RUN_ID overrides of the stopped task do not all name the code run that the job recorded in its layer_run.json")
    return recorded


def fetch_archive(access: ReportAccess, run: str, digest: str, target: Path, limit: int, deadline: float) -> None:
    """Download the staged archive into ``target`` within the limit and refuse it unless its SHA-256 is the staged ``code.sha256``."""
    key = f"lakehouse/run_stage/{run}/inputs/code.tar.gz"
    try:
        reply = access.s3.get_object(Bucket=access.bucket, Key=key)
    except ClientError as error:
        if str(error.response.get("Error", {}).get("Code", "")) == "NoSuchKey":
            raise ReportError("the staged code archive is missing") from None
        raise aws_error("the read of the staged code archive", error) from None
    except BotoCoreError as error:
        raise aws_error("the read of the staged code archive", error) from None
    if int(reply.get("ContentLength", 0)) > limit:
        raise ReportError("the staged code archive is larger than its limit")
    hasher = hashlib.sha256()
    total = 0
    with target.open("xb") as handle:
        while chunk := reply["Body"].read(CHUNK):
            total += len(chunk)
            if total > limit:
                raise ReportError("the staged code archive is larger than its limit")
            if time.monotonic() > deadline:
                raise ReportError("the staged code archive did not download before its deadline")
            hasher.update(chunk)
            handle.write(chunk)
    if hasher.hexdigest() != digest:
        raise ReportError("the staged code archive does not match its code.sha256")


def contract_from_archive(archive: Path, deadline: float) -> acceptance.StagedContract:
    """The contract files of a verified archive (``scripts.lakehouse.layer_acceptance.contract_from_archive``), with this module's error."""
    try:
        return acceptance.contract_from_archive(archive, deadline)
    except acceptance.AcceptanceError as error:
        raise ReportError(str(error)) from None


def launch_events(access: ReportAccess, layer: str, run: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """The claim record of the run and job and the records of its one launch by event name. The started and end records must exist."""
    store = S3LaunchStore(access.s3, access.bucket, launch_record_prefix())
    try:
        claim = store.run_record(run, layer, "claim")
        if claim is None:
            raise ReportError("no launch claim is recorded for this run and job")
        events = {record["event"]: record for record in store.records(run, layer, [claim["attempt"]])}
    except ReportError:
        raise
    except Exception as error:
        raise ReportError(f"the launch records cannot be read ({type(error).__name__})") from None
    if "started" not in events or "end" not in events:
        raise ReportError("the started or end record of the launch is missing")
    return claim, events


def launch_files(access: ReportAccess, layer: str, run: str, target: Path) -> str:
    """The claim, started and end records of the run and job, saved as the report reads them. Returns the ARN of the started task."""
    claim, events = launch_events(access, layer, run)
    folder = target / "launch" / f"{claim['attempt']:02d}"
    folder.mkdir(parents=True, mode=0o700)
    (target / "launch" / "claim.json").write_text(json.dumps(claim, sort_keys=True))
    for event in ("started", "end"):
        (folder / f"{event}.json").write_text(json.dumps(events[event], sort_keys=True))
    return str(events["started"]["task_arn"])


def task_description(access: ReportAccess, task_arn: str) -> dict[str, Any]:
    """The description of the one task, in the form the report reads: the fields the checks use and the exit state of each container."""
    try:
        reply = access.ecs.describe_tasks(cluster=access.cluster, tasks=[task_arn])
    except (ClientError, BotoCoreError) as error:
        raise aws_error("the ECS task description", error) from None
    tasks = reply.get("tasks") or []
    if reply.get("failures") or len(tasks) != 1:
        raise ReportError("ECS describes no single task for the started task ARN")
    task = tasks[0]
    kept = {name: task.get(name) for name in ("taskArn", "taskDefinitionArn", "lastStatus", "overrides")}
    kept["containers"] = [{name: item.get(name) for name in ("name", "lastStatus", "exitCode")} for item in task.get("containers") or []]
    return {"failures": [], "tasks": [kept]}


def running_facts(access: ReportAccess, layer: str, deadline: float) -> dict[str, Any]:
    """The ECS tasks of the layer's host family that run now."""
    arns: list[str] = []
    token: dict[str, str] = {}
    while True:
        try:
            reply = access.ecs.list_tasks(cluster=access.cluster, family=f"{access.family_prefix}{layer}_host", desiredStatus="RUNNING", **token)
        except (ClientError, BotoCoreError) as error:
            raise aws_error("the ECS task list", error) from None
        arns += [str(arn) for arn in reply.get("taskArns") or []]
        if not reply.get("nextToken"):
            break
        if time.monotonic() > deadline:
            raise ReportError("the ECS task list did not finish before its deadline")
        token = {"nextToken": str(reply["nextToken"])}
    return {"taskArns": arns}


def container_instance_ids(access: ReportAccess, arns: list[str]) -> list[str]:
    """The EC2 instance IDs of the listed container instances. The description must resolve every listed ARN: a failure or a missing ARN is no evidence."""
    ids: list[str] = []
    for start in range(0, len(arns), 100):
        batch = arns[start : start + 100]
        described = access.ecs.describe_container_instances(cluster=access.cluster, containerInstances=batch)
        items = described.get("containerInstances") or []
        if described.get("failures") or sorted(str(item.get("containerInstanceArn")) for item in items) != sorted(batch):
            raise ReportError("the description of the active container instances does not resolve every listed container instance")
        ids += [str(item["ec2InstanceId"]) for item in items]
    return ids


def host_facts(access: ReportAccess, deadline: float) -> dict[str, Any]:
    """The facts the host check reads: the group's desired capacity and instances and the EC2 instances of the active container instances. No page is
    requested after ``deadline`` (a ``time.monotonic`` value)."""
    if not access.host_group:
        raise ReportError("HAI_LAUNCH_HOST_GROUP is not set in the Airflow service environment")
    try:
        groups = access.autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[access.host_group]).get("AutoScalingGroups") or []
        if len(groups) != 1:
            raise ReportError("the host auto scaling group is not found exactly once")
        containers: list[str] = []
        token: dict[str, str] = {}
        while True:
            if time.monotonic() > deadline:
                raise ReportError("the host description did not finish before its deadline")
            reply = access.ecs.list_container_instances(cluster=access.cluster, status="ACTIVE", **token)
            containers += container_instance_ids(access, [str(arn) for arn in reply.get("containerInstanceArns") or []])
            if not reply.get("nextToken"):
                break
            token = {"nextToken": str(reply["nextToken"])}
    except (ClientError, BotoCoreError) as error:
        raise aws_error("the host description", error) from None
    except KeyError:
        raise ReportError("the description of a container instance lacks its EC2 instance ID") from None
    group = groups[0]
    return {
        "desired_capacity": group.get("DesiredCapacity"),
        "group_instances": [str(item["InstanceId"]) for item in group.get("Instances") or []],
        "container_instances": containers,
    }


def save_closing_evidence(access: ReportAccess, layer: str, run: str, limit: int = LISTING_LIMIT, seconds: float = DEADLINE_SECONDS) -> Path:
    """Save the evidence that closes the job's window once for the run: the listing of ``lakehouse/`` after the launch, the description of the stopped task, the
    host family's running tasks and the host facts. The job task calls it right after the launch, while it still holds the pool slot, and after a launch that
    ended in a failure. A record that exists is kept (and checked). When an earlier attempt could not save it (the task was not yet stopped or the save failed),
    a retry that saves it late takes the listing at retry time: the writes of other runs between the stop and the retry are then in the listing. Every wait
    ends at ``seconds`` after the call, and a write that fails is a ``ReportError``."""
    folder = folder_of(access, layer, run)
    path = folder / "closing.json"
    if path.exists() or path.is_symlink():
        checked_closing(path, layer, run)
        return path
    deadline = time.monotonic() + seconds
    _, events = launch_events(access, layer, run)
    task = task_description(access, str(events["started"]["task_arn"]))
    if task["tasks"][0].get("lastStatus") != "STOPPED":
        raise ReportError("ECS does not describe the task as stopped, so no closing evidence is saved")
    document = {
        "schema": 1,
        "run_id": run,
        "layer": layer,
        "after": listing_document(layer, run, list_versions(access.s3, access.bucket, limit, deadline)),
        "task": task,
        "running": running_facts(access, layer, deadline),
        "host": host_facts(access, deadline),
    }
    data = json.dumps(document).encode()
    if len(data) > 2 * limit:
        raise ReportError("the closing evidence is larger than the report limit")
    try:
        write_private(path, data)
    except OSError as error:
        raise ReportError(f"the closing evidence could not be written ({type(error).__name__})") from None
    return path


def checked_closing(path: Path, layer: str, run: str) -> dict[str, Any]:
    """The closing evidence of the job task: it exists, is private, belongs to this layer and run and has its four parts."""
    try:
        document = json.loads(private_bytes(path, "closing evidence", 2 * LISTING_LIMIT))
    except ValueError:
        raise ReportError("the closing evidence is damaged (not JSON)") from None
    if not isinstance(document, dict) or (document.get("run_id"), document.get("layer")) != (run, layer):
        raise ReportError("the closing evidence belongs to another run or layer")
    after, running, host = (document.get(name) for name in ("after", "running", "host"))
    if not (isinstance(after, dict) and isinstance(running, dict) and isinstance(host, dict)):
        raise ReportError("the closing evidence is damaged (a part is missing)")
    task = document.get("task")
    if not (isinstance(task, dict) and isinstance(task.get("tasks"), list) and len(task["tasks"]) == 1 and isinstance(task["tasks"][0], dict)):
        raise ReportError("the closing evidence holds no task description of exactly one task")
    return document


def evidence_files(access: ReportAccess, layer: str, run: str, target: Path) -> dict[str, Any]:
    """The run's records below ``lakehouse/layer_runs/<layer>/<run>/``, read by name. Returns the pin the layer reads."""
    names = [*acceptance.EVIDENCE_FILES, acceptance.PIN_FILE[layer], *(["silver_snapshot.json"] if layer == "silver" else [])]
    for name in names:
        data = read_object(access, f"lakehouse/layer_runs/{layer}/{run}/{name}", acceptance.LIMIT, f"the run record {name}")
        path = target / "evidence" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    try:
        pin = json.loads((target / "evidence" / acceptance.PIN_FILE[layer]).read_bytes())
    except ValueError:
        raise ReportError("the pin record is not JSON") from None
    return pin if isinstance(pin, dict) else {}


def silver_input(access: ReportAccess, pin: Mapping[str, Any], target: Path) -> tuple[Path, Path]:
    """The silver records and the passed silver report that a gold report reads."""
    source = str(pin.get("source_run_id"))
    if not acceptance.STORED_RUN.fullmatch(source):
        raise ReportError("the gold pin names no silver run")
    report = access.directory / "silver" / source / "report.json"
    private_bytes(report, "silver report of the pinned run", acceptance.LIMIT)
    data = read_object(access, f"lakehouse/layer_runs/silver/{source}/silver_snapshot.json", acceptance.LIMIT, "the silver record silver_snapshot.json")
    (target / "input").mkdir()
    (target / "input" / "silver_snapshot.json").write_bytes(data)
    return target / "input", report


def stored_result(path: Path, layer: str, run: str, current: Mapping[str, Any]) -> dict[str, Any]:
    """The report that exists for the run: returned when it passed, raised as a failed report when it did not.

    ``current`` is the report that the inputs give now, built by the functions of the first run. The stored report must record the same digest for every input,
    the same checks and the same verdict. A stored report of other inputs is refused, never returned and never written again."""
    try:
        document = json.loads(private_bytes(path, "stored report", acceptance.LIMIT))
    except ValueError:
        raise ReportError("the stored report is damaged (not JSON)") from None
    if not isinstance(document, dict) or (document.get("layer"), document.get("run_id")) != (layer, run):
        raise ReportError("the stored report belongs to another run or layer")
    recorded, now = document.get("file_sha256"), current.get("file_sha256")
    recorded = recorded if isinstance(recorded, dict) else {}
    now = now if isinstance(now, dict) else {}
    changed = sorted(name for name in now if recorded.get(name) != now[name])
    others = sum(1 for name in recorded if name not in now)
    if changed or others or document.get("checks") != current.get("checks") or document.get("passed") != current.get("passed"):
        named = [*changed, *([f"{others} other field(s)"] if others else [])]
        raise ReportError(f"the stored report does not match its inputs (changed: {', '.join(named) or 'the checks'})")
    stored = document.get("checks")
    checks: dict[str, Any] = stored if isinstance(stored, dict) else {}
    failed = sorted(name for name, ok in checks.items() if ok is not True)
    if document.get("passed") is not True or failed or not checks:
        raise ReportFailed(f"the acceptance report of {layer} run {run} failed {len(failed)} check(s): {'; '.join(failed)}", failed)
    return {"passed": True, "report": str(path), "checks": len(checks)}


def collected_report(access: ReportAccess, layer: str, run: str, folder: Path, archive_limit: int) -> dict[str, Any]:
    """The report that the run's inputs give: the saved before listing and closing evidence of the private folder, and the records that AWS holds for the run.

    The first run and every retry call this, so each input is read and validated the same way every time."""
    try:
        checked_listing(folder / "before.json", layer, run)
    except ReportError as error:
        raise ReportError(
            f"{error}: the job task saves the listing right before the launch, so a scheduler restart or a cleared task loses it and the run needs a new run ID"
        ) from None
    try:
        closing = checked_closing(folder / "closing.json", layer, run)
    except ReportError as error:
        raise ReportError(f"{error}: the job task saves it right after the launch, before it releases the pool slot, so the run needs a new run ID") from None
    deadline = time.monotonic() + DEADLINE_SECONDS
    with tempfile.TemporaryDirectory(dir=folder, prefix="inputs_") as name:
        work = Path(name)
        task_arn = launch_files(access, layer, run, work)
        if closing["task"]["tasks"][0].get("taskArn") != task_arn:
            raise ReportError("the task description saved at the stop is of another task than the one the launch started")
        (work / "describe.json").write_text(json.dumps(closing["task"], sort_keys=True))
        pin = evidence_files(access, layer, run, work)
        try:
            identity = json.loads((work / "evidence" / "layer_run.json").read_bytes())
        except ValueError:
            raise ReportError("the run record layer_run.json is not JSON") from None
        code_run = code_run_of(identity if isinstance(identity, dict) else {}, closing["task"]["tasks"][0])
        digest = read_object(access, f"lakehouse/run_stage/{code_run}/inputs/code.sha256", 256, "the staged code.sha256").decode(errors="replace").strip()
        if not acceptance.SHA256.fullmatch(digest):
            raise ReportError("the staged code.sha256 is not a SHA-256")
        (work / "code.sha256").write_text(digest + "\n")
        fetch_archive(access, code_run, digest, work / "code.tar.gz", archive_limit, deadline)
        contract = contract_from_archive(work / "code.tar.gz", deadline)
        (work / "code.tar.gz").unlink()
        for name, part in (("after", "after"), ("running", "running"), ("host", "host")):
            (work / f"{name}.json").write_text(json.dumps(closing[part], sort_keys=True))
        input_evidence = input_report = None
        if layer == "gold":
            input_evidence, input_report = silver_input(access, pin, work)
        arguments = argparse.Namespace(
            layer=layer,
            run=run,
            code_digest=work / "code.sha256",
            describe=work / "describe.json",
            evidence=work / "evidence",
            launch_records=work / "launch",
            before=folder / "before.json",
            after=work / "after.json",
            running_tasks=work / "running.json",
            host_status=work / "host.json",
            input_evidence=input_evidence,
            input_report=input_report,
            frozen=contract.read,
        )
        try:
            return acceptance.report(arguments)
        except acceptance.AcceptanceError as error:
            raise ReportError(str(error)) from None
        except (AttributeError, KeyError, TypeError, ValueError):
            raise ReportError("an input of the report has an unexpected shape") from None


def accept(access: ReportAccess, layer: str, run: str, archive_limit: int = ARCHIVE_LIMIT) -> dict[str, Any]:
    """Write the acceptance report of a finished run and return its path, or raise with the cause. The report is created once.

    A second call collects every input again through ``collected_report`` and returns the stored report only when the inputs give the same digests, checks and
    verdict. A saved record that is missing, damaged, foreign, open to others or changed fails the call, whether a report exists or not."""
    folder = folder_of(access, layer, run)
    report_path = folder / "report.json"
    document = collected_report(access, layer, run, folder, archive_limit)
    if report_path.exists() or report_path.is_symlink():
        return stored_result(report_path, layer, run, document)
    write_private(report_path, (json.dumps(document, indent=2, sort_keys=True) + "\n").encode())
    LOG.info("layer acceptance report %s", json.dumps(document, sort_keys=True))
    return stored_result(report_path, layer, run, document)
