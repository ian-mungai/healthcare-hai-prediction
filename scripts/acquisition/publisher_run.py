"""The run 3 publisher job: fetch the staged inputs by digest, restore the last state, run the harness and save the new state under ``raw/``.

The inputs are one archive staged beside the code archive. The launch (orchestration.publisher_launch) reads its digest from the run's request and hands it to
the job, so a changed archive stops the job before anything runs. A task's workspace ends with the task, so the outcomes and the BLS request ledger travel
between tasks in a saved state. It holds those records only: no fresh file, no diagnostic and no original can enter it. BLS needs more than one day, so run 3
takes more than one task.
At the end each task writes one run record that names its final state, at a key fixed by the queue digest and the run ID, so a check finds it without
listing. The run records sit beside the states, never among them, because the next task takes the largest key under the state prefix as the latest state.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import sys
import tarfile
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from botocore.exceptions import ClientError

from scripts import resource_peaks
from scripts.acquisition import redownload as harness
from scripts.acquisition import stored_mirror
from scripts.acquisition.collection_layout import RAW_ROOT

REQUIRED = ("RUN_ID", "PUBLISHER_INPUTS_SHA256", "PUBLISHER_QUEUE_SHA256", "AWS_REGION", "EXPECTED_ACCOUNT_ID", "TASK_ROLE_NAME", "DATA_BUCKET_NAME")
# The attempt and byte ledgers travel too, settlements and unsettled charges alike, so a new task cannot spend a limit again.
SAVED = re.compile(
    r"(run\.json|(outcomes|pauses|resumes)/[^/]+\.json|collectors/bls_api_v2/requests/[^/]+\.json"
    r"|\.budgets/[0-9]{9}(\.settled)?\.json|\.attempts/[^/]+/[a-f0-9]{64}/[0-9]{3}\.json)"
)
INPUTS_NAME = "publisher_inputs.tar.gz"
SAVE_EVERY = 25
PRIOR_KEYS = {"key", "version_id", "sha256"}
WORKSPACE = "/workspace"
MANIFEST_FILE = "publisher_manifest.json"
ClientFactory = Callable[[str, str], Any]


class PublisherError(ValueError):
    """The job cannot start or finish. The message holds only fixed text and variable names."""


def workspace_of(environment: Mapping[str, str]) -> Path:
    return Path(environment.get("PUBLISHER_WORKSPACE", WORKSPACE))


def fetch_inputs(s3: Any, environment: Mapping[str, str]) -> Path:
    """Read the staged archive, compare its digest and unpack it under the workspace. An archive that leaves its folder is refused."""
    key = f"lakehouse/run_stage/{environment['RUN_ID']}/inputs/{INPUTS_NAME}"
    body = s3.get_object(Bucket=environment["DATA_BUCKET_NAME"], Key=key, ExpectedBucketOwner=environment["EXPECTED_ACCOUNT_ID"])["Body"].read()
    if hashlib.sha256(body).hexdigest() != environment["PUBLISHER_INPUTS_SHA256"]:
        raise PublisherError("The staged inputs differ from the digest the launch bound.")
    inputs = workspace_of(environment) / "inputs"
    inputs.mkdir(parents=True, exist_ok=True)
    try:
        with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as archive:
            archive.extractall(inputs, filter="data")
    except tarfile.FilterError:
        raise PublisherError("The staged inputs hold an unsafe path.") from None
    return inputs


def queue_of(inputs: Path) -> Path:
    """The queue the stage named inside the inputs. The archive's own unpacking already keeps it under the inputs folder."""
    queue = inputs / json.loads((inputs / MANIFEST_FILE).read_text(encoding="utf-8"))["queue"]
    if not queue.resolve().is_relative_to(inputs.resolve()) or not queue.is_file():
        raise PublisherError("The staged manifest names no queue under the inputs.")
    return queue


def install_plan_inputs(mirror: Path, code_root: Path) -> None:
    """Copy the files the locked plans read into the code root's data folder, as real files, because the registry check refuses a link."""
    listing = mirror / stored_mirror.PLAN_INPUTS_NAME
    if not listing.is_file():
        return
    listed = json.loads(listing.read_text(encoding="utf-8"))
    for name, expected in listed.items():
        relative = PurePosixPath(name)
        if relative.is_absolute() or ".." in relative.parts or relative.parts[0] != "data":
            raise PublisherError("A carried plan input names an unsafe path.")
        source = mirror / name
        if hashlib.sha256(source.read_bytes()).hexdigest() != expected:
            raise PublisherError("A carried plan input differs from its digest.")
    for name in listed:
        target = code_root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if target.is_symlink() or hashlib.sha256(target.read_bytes()).hexdigest() != listed[name]:
                raise PublisherError("A plan input already in the code root differs from its digest.")
            continue
        target.write_bytes((mirror / name).read_bytes())


def state_prefix(environment: Mapping[str, str]) -> str:
    return f"{RAW_ROOT}run3_state/{environment['PUBLISHER_QUEUE_SHA256']}/"


def save_state(s3: Any, state_root: Path, environment: Mapping[str, str]) -> dict[str, str]:
    """Create a new state object from the records of the state root and name it for the owner's approval. Only the names the pattern allows enter it."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for path in sorted(state_root.rglob("*")):
            relative = path.relative_to(state_root).as_posix()
            if path.is_file() and not path.is_symlink() and SAVED.fullmatch(relative):
                archive.add(path, arcname=relative, recursive=False)
    body = buffer.getvalue()
    digest = hashlib.sha256(body).hexdigest()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    key = f"{state_prefix(environment)}{stamp}_{digest}.tar.gz"
    reply = s3.put_object(
        Bucket=environment["DATA_BUCKET_NAME"],
        ExpectedBucketOwner=environment["EXPECTED_ACCOUNT_ID"],
        Key=key,
        Body=body,
        IfNoneMatch="*",
        ServerSideEncryption="AES256",
    )
    return {"key": key, "version_id": str(reply.get("VersionId", "")), "sha256": digest}


def run_record_key(environment: Mapping[str, str]) -> str:
    return f"{RAW_ROOT}run3_runs/{environment['PUBLISHER_QUEUE_SHA256']}/publisher/{environment['RUN_ID']}.json"


def save_run_record(s3: Any, state: Mapping[str, str], environment: Mapping[str, str]) -> str:
    """Create the run's one record of its final state. The same record again creates nothing; a different one stops the job and is never replaced."""
    body = json.dumps(
        {"run_id": environment["RUN_ID"], "queue_sha256": environment["PUBLISHER_QUEUE_SHA256"], "state": dict(state)}, sort_keys=True, separators=(",", ":")
    ).encode()
    key = run_record_key(environment)
    named = {"Bucket": environment["DATA_BUCKET_NAME"], "ExpectedBucketOwner": environment["EXPECTED_ACCOUNT_ID"], "Key": key}
    try:
        s3.put_object(**named, Body=body, IfNoneMatch="*", ServerSideEncryption="AES256")
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") not in {"PreconditionFailed", "412", "ConditionalRequestConflict", "409"}:
            raise
        if s3.get_object(**named)["Body"].read() != body:
            raise PublisherError("This run already has a run record that names another state.") from None
    return key


def approved_prior(environment: Mapping[str, str]) -> dict[str, str] | None:
    """The one prior state the launch bound from the request, or None for a first task. The task never lists the prefix, because any task can write there."""
    text = environment.get("PUBLISHER_PRIOR_STATE", "")
    if not text:
        return None
    try:
        reference = json.loads(text)
    except ValueError:
        raise PublisherError("The prior state reference is not usable.") from None
    if (
        not isinstance(reference, dict)
        or set(reference) != PRIOR_KEYS
        or not all(isinstance(value, str) and value for value in reference.values())
        or not str(reference["key"]).startswith(state_prefix(environment))
        or not re.fullmatch(r"[a-f0-9]{64}", reference["sha256"])
    ):
        raise PublisherError("The prior state reference is not usable.")
    return reference


def require_latest(s3: Any, reference: dict[str, str] | None, environment: Mapping[str, str]) -> None:
    """The ledgers only grow, so an older state or none would hand spent limits back. A writer's extra object can only stop the job, never choose a state."""
    named = {"Bucket": environment["DATA_BUCKET_NAME"], "ExpectedBucketOwner": environment["EXPECTED_ACCOUNT_ID"], "Prefix": state_prefix(environment)}
    keys: list[str] = []
    options: dict[str, str] = {}
    while True:
        page = s3.list_objects_v2(**named, **options)
        keys.extend(item["Key"] for item in page.get("Contents", []))
        if not page.get("IsTruncated"):
            break
        options = {"ContinuationToken": page["NextContinuationToken"]}
    if max(keys, default=None) != (None if reference is None else reference["key"]):
        raise PublisherError("The request does not name the latest saved state of this queue.")


def records_only(archive: tarfile.TarFile) -> bool:
    """A saved state holds plain record files, each once. The task and the owner's verifier both refuse anything else."""
    members = archive.getmembers()
    return all(member.isfile() and SAVED.fullmatch(member.name) for member in members) and len({member.name for member in members}) == len(members)


def restore_state(s3: Any, state_root: Path, environment: Mapping[str, str]) -> str | None:
    """Unpack exactly the approved prior state into an empty state root, or nothing. A digest that differs or a member that is not a record stops the job."""
    reference = approved_prior(environment)
    require_latest(s3, reference, environment)
    if reference is None:
        return None
    reply = s3.get_object(
        Bucket=environment["DATA_BUCKET_NAME"], ExpectedBucketOwner=environment["EXPECTED_ACCOUNT_ID"], Key=reference["key"], VersionId=reference["version_id"]
    )
    body = reply["Body"].read()
    if hashlib.sha256(body).hexdigest() != reference["sha256"]:
        raise PublisherError("The prior state differs from its approved digest.")
    with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as archive:
        if not records_only(archive):
            raise PublisherError("The saved state holds a file that is not a record.")
        state_root.mkdir(parents=True, mode=0o700, exist_ok=True)
        archive.extractall(state_root, filter="data")
    return reference["key"]


def require_bound_queue(queue: Path, environment: Mapping[str, str]) -> None:
    """The unpacked queue must be the one the launch bound, because that digest names the state the job restores and saves."""
    if hashlib.sha256(queue.read_bytes()).hexdigest() != environment["PUBLISHER_QUEUE_SHA256"]:
        raise PublisherError("The staged queue differs from the digest the launch bound.")


def main(environment: Mapping[str, str] | None = None, factory: ClientFactory = stored_mirror.default_client) -> None:
    """Run one publisher task. The harness writes one line per unit and no line holds a key or a name."""
    env = os.environ if environment is None else environment
    missing = [name for name in REQUIRED if not env.get(name)]
    if missing:
        raise PublisherError(f"The publisher job needs {', '.join(missing)}.")
    region = env["AWS_REGION"]
    stored_mirror.require_task_identity(factory("sts", region), env["EXPECTED_ACCOUNT_ID"], env["TASK_ROLE_NAME"])
    s3 = factory("s3", region)
    inputs = fetch_inputs(s3, env)
    queue = queue_of(inputs)
    require_bound_queue(queue, env)
    install_plan_inputs(inputs / "mirror", harness.REPO_ROOT)
    state_parent = workspace_of(env) / "state"
    state_root = state_parent / "run"
    restore_state(s3, state_root, env)
    arguments = argparse.Namespace(
        queue=queue,
        state_root=state_root,
        state_parent=state_parent,
        manual_folder=None,
        removal_plan=inputs / "removal_plan.json",
        removal_confirmation=inputs / "removal_confirmation.json",
        hosts=inputs / "publisher_hosts.json",
        hosts_approval=inputs / "hosts_approval.json",
    )
    run = harness.run_from(arguments, env, factory)

    def emit(item: dict) -> None:
        sys.stdout.write(json.dumps(item, sort_keys=True) + "\n")
        sys.stdout.flush()

    finished = [0]

    def progress(item: dict) -> None:
        emit(item)
        finished[0] += 1
        if finished[0] % SAVE_EVERY == 0:
            emit({"state": save_state(s3, state_root, env)})

    try:
        for source in filter(None, env.get("PUBLISHER_RESUME_SOURCES", "").split(",")):
            harness.load_queue(run)
            harness.resume_source(run, source, "resumed by the staged publisher request")
        emit(harness.execute(run, allow_network=True, log=progress))
    finally:
        final = save_state(s3, state_root, env)
        emit({"state": final})
        emit({"run_record": save_run_record(s3, final, env)})


if __name__ == "__main__":
    resource_peaks.recorded(main)
