"""The run 3 publisher job: fetch the staged inputs by digest, restore the last state, run the harness and save the new state under ``raw/``.

The inputs are one archive staged beside the code archive. The broker reads its digest from the run's request and hands it to the job, so a changed archive
stops the job before anything runs. A task's workspace ends with the task, so the outcomes and the BLS request ledger travel between tasks in a saved state.
It holds those records only: no fresh file, no diagnostic and no original can enter it. BLS needs more than one day, so run 3 takes more than one task.
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
from pathlib import Path
from typing import Any

from scripts.acquisition import redownload as harness
from scripts.acquisition import stored_mirror
from scripts.acquisition.collection_layout import RAW_ROOT

REQUIRED = ("RUN_ID", "PUBLISHER_INPUTS_SHA256", "PUBLISHER_QUEUE_SHA256", "AWS_REGION", "EXPECTED_ACCOUNT_ID", "TASK_ROLE_NAME", "DATA_BUCKET_NAME")
SAVED = re.compile(r"(run\.json|(outcomes|pauses|resumes)/[^/]+\.json|collectors/bls_api_v2/requests/[^/]+\.json)")
INPUTS_NAME = "publisher_inputs.tar.gz"
WORKSPACE = "/workspace"
QUEUE_FILE = "mirror/queue.json"
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
        raise PublisherError("The staged inputs differ from the digest the broker bound.")
    inputs = workspace_of(environment) / "inputs"
    inputs.mkdir(parents=True, exist_ok=True)
    try:
        with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as archive:
            archive.extractall(inputs, filter="data")
    except tarfile.FilterError:
        raise PublisherError("The staged inputs hold an unsafe path.") from None
    return inputs


def state_prefix(environment: Mapping[str, str]) -> str:
    return f"{RAW_ROOT}run3_state/{environment['PUBLISHER_QUEUE_SHA256']}/"


def save_state(s3: Any, state_root: Path, environment: Mapping[str, str]) -> str:
    """Create a new state object from the records of the state root. Only the names the pattern allows enter it."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for path in sorted(state_root.rglob("*")):
            relative = path.relative_to(state_root).as_posix()
            if path.is_file() and not path.is_symlink() and SAVED.fullmatch(relative):
                archive.add(path, arcname=relative, recursive=False)
    body = buffer.getvalue()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    key = f"{state_prefix(environment)}{stamp}_{hashlib.sha256(body).hexdigest()}.tar.gz"
    s3.put_object(
        Bucket=environment["DATA_BUCKET_NAME"],
        ExpectedBucketOwner=environment["EXPECTED_ACCOUNT_ID"],
        Key=key,
        Body=body,
        IfNoneMatch="*",
        ServerSideEncryption="AES256",
    )
    return key


def restore_state(s3: Any, state_root: Path, environment: Mapping[str, str]) -> str | None:
    """Unpack the latest saved state of this queue into an empty state root. A first task finds none and starts empty."""
    bucket, owner = environment["DATA_BUCKET_NAME"], environment["EXPECTED_ACCOUNT_ID"]
    keys: list[str] = []
    options: dict[str, str] = {}
    while True:
        page = s3.list_objects_v2(Bucket=bucket, ExpectedBucketOwner=owner, Prefix=state_prefix(environment), **options)
        keys.extend(item["Key"] for item in page.get("Contents", []))
        if not page.get("IsTruncated"):
            break
        options = {"ContinuationToken": page["NextContinuationToken"]}
    if not keys:
        return None
    latest = max(keys)
    body = s3.get_object(Bucket=bucket, ExpectedBucketOwner=owner, Key=latest)["Body"].read()
    state_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as archive:
        names = archive.getnames()
        if not all(SAVED.fullmatch(name) for name in names):
            raise PublisherError("The saved state holds a file that is not a record.")
        archive.extractall(state_root, filter="data")
    return latest


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
    state_parent = workspace_of(env) / "state"
    state_root = state_parent / "run"
    restore_state(s3, state_root, env)
    arguments = argparse.Namespace(
        queue=inputs / QUEUE_FILE,
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

    try:
        for source in filter(None, env.get("PUBLISHER_RESUME_SOURCES", "").split(",")):
            harness.load_queue(run)
            harness.resume_source(run, source, "resumed by the staged publisher request")
        emit(harness.execute(run, allow_network=True, log=emit))
    finally:
        emit({"state": save_state(s3, state_root, env)})


if __name__ == "__main__":
    main()
