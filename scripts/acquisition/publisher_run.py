"""The publisher job of the host: build the run's workspace from the tracked inputs in the code archive, check them and run capture mode.

Nothing is staged for the run but its code archive. The job takes its inputs from ``config/acquisition/baseline_run/`` (checked against ``inputs.lock.json``)
and from the bootstrap seed folder (checked against its manifest). ``host_inputs.prepare`` stops the run before any request when a file is missing, is an
unresolved Git LFS pointer or differs. The launch names the run kind and the capture root only, and a launch that names an inputs archive or a prior state is
refused. Capture mode decides each unit by its completion record, so a retry of a run skips what was captured and no state travels between tasks. At the end
the job runs capture mode's evidence step over the run and fails when it finds a problem, which includes a unit that failed, was refused or was not attempted.

The helpers below ``main`` that save, restore and verify a state, and the one that installs plan inputs from a mirror, belong to the earlier flow, which a later
lane removes with the mirror build.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import sys
import tarfile
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path, PurePosixPath
from typing import Any

from botocore.exceptions import ClientError

from scripts import resource_peaks
from scripts.acquisition import capture_mode, host_inputs, stored_mirror, task_role
from scripts.acquisition.collection_layout import run_state_root
from scripts.acquisition.completion_record import run_record_key as record_key
from scripts.acquisition.transport import CaptureError
from scripts.infrastructure import startup_check

REQUIRED = ("RUN_ID", "CODE_SHA256", "PUBLISHER_RUN_KIND", "HAI_CAPTURE_ROOT", "AWS_REGION", "EXPECTED_ACCOUNT_ID", "TASK_ROLE_NAME", "DATA_BUCKET_NAME")
# What the launch may not name any more: the staged inputs archive, the queue whose state a task restores, the resumed sources and the prior state.
RETIRED = ("PUBLISHER_INPUTS_SHA256", "PUBLISHER_QUEUE_SHA256", "PUBLISHER_RESUME_SOURCES", "PUBLISHER_PRIOR_STATE", "PUBLISHER_REQUEST_SHA256")
# The run kinds the launch lists (orchestration/ecs_preflight.py; a test keeps both lists equal). The capture root is checked by ``state_root``, which accepts
# raw/ and one ingestion_test/<run ID>/ folder as the launch does.
KINDS = frozenset({"bootstrap", "monthly", "retry"})
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


def state_root(environment: Mapping[str, str]) -> str:
    try:
        return run_state_root(environment)
    except CaptureError:
        raise PublisherError("The capture root is neither raw/ nor one test folder.") from None


def state_prefix(environment: Mapping[str, str]) -> str:
    return f"{state_root(environment)}run3_state/{environment['PUBLISHER_QUEUE_SHA256']}/"


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
    return f"{state_root(environment)}run3_runs/{environment['PUBLISHER_QUEUE_SHA256']}/publisher/{environment['RUN_ID']}.json"


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


RUN_ID_FORM = re.compile(r"([0-9]{8}T[0-9]{6}Z)-[a-f0-9]{8}")
RETRY_DAYS = 7


def retry_run_id(original_run: str, day: int) -> str:
    """The run ID of the retry of ``original_run`` on ``day``. The launch derives it by the same rule, and a test keeps both equal."""
    found = RUN_ID_FORM.fullmatch(original_run)
    if found is None or isinstance(day, bool) or not 1 <= day <= RETRY_DAYS:
        raise PublisherError("The retry names an original run that is not a run ID or a retry day outside one to seven.")
    moment = datetime.strptime(found[1], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC) + timedelta(days=day)
    return f"{moment.strftime('%Y%m%dT%H%M%SZ')}-{hashlib.sha256(f'{original_run}/{day}'.encode()).hexdigest()[:8]}"


RETRY_VARIABLES = ("PUBLISHER_ORIGINAL_RUN", "PUBLISHER_RETRY_DAY")


def check_retry(env: Mapping[str, str]) -> tuple[str, int] | None:
    """The original run and the day of a retry, or None. Only a retry names them, and a retry names both and runs under the run ID of that day."""
    named = {name: env.get(name, "") for name in RETRY_VARIABLES}
    if env["PUBLISHER_RUN_KIND"] != "retry":
        for name, value in named.items():
            if value:
                raise PublisherError(f"Only a retry takes {name}.")
        return None
    for name, value in named.items():
        if not value:
            raise PublisherError(f"A retry needs {name}.")
    original, day_text = named["PUBLISHER_ORIGINAL_RUN"], named["PUBLISHER_RETRY_DAY"]
    if not RUN_ID_FORM.fullmatch(original):
        raise PublisherError("The retry names an original run that is not a run ID.")
    if not re.fullmatch(r"[1-7]", day_text):
        raise PublisherError("The retry day is outside one to seven.")
    if env["RUN_ID"] != retry_run_id(original, int(day_text)):
        raise PublisherError("The retry does not run under the run ID of that day.")
    return original, int(day_text)


def read_run_record(storage: Any, root: str, run_id: str) -> dict[str, Any] | None:
    key = record_key(root, run_id)
    return json.loads(capture_mode.read_stored(storage, key)) if capture_mode.list_objects(storage, key) else None


def outstanding_units(storage: Any, root: str, original: str, day: int) -> frozenset[str]:
    """The snapshots a retry on ``day`` downloads again: those the original monthly run failed or did not attempt because its source paused, less those an
    earlier retry day of the chain recovered. Every other unit keeps the capture it has, so a retry spends no request on a unit that is complete."""
    first = read_run_record(storage, root, original)
    if first is None:
        raise PublisherError("The original run of the retry has no run record on this capture root.")
    outstanding = {item["snapshot_id"] for item in first["units"]["failed"]} | set(first["units"]["paused_not_attempted"])
    for earlier in range(1, day):
        found = read_run_record(storage, root, retry_run_id(original, earlier))
        if found is not None:
            outstanding -= {item["snapshot_id"] for name in ("completed", "changed", "unchanged") for item in found["units"][name]}
    return frozenset(outstanding)


def bootstrap_key(root: str, run_id: str) -> str:
    return f"{root}ingestion_runs/{run_id}/bootstrap.json"


def require_bootstrap(storage: Any, root: str) -> None:
    """A monthly or retry run starts only on a capture root where a bootstrap run passed: the root holds a ``bootstrap.json`` under one run folder."""
    pattern = re.compile(re.escape(f"{root}ingestion_runs/") + r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}/bootstrap\.json")
    if not any(item["kind"] == "version" and pattern.fullmatch(item["key"]) for item in capture_mode.list_versions(storage, [f"{root}ingestion_runs/"])):
        raise PublisherError("The capture root has no bootstrap run record, so no monthly or retry run starts on it.")


def main(environment: Mapping[str, str] | None = None, factory: ClientFactory = stored_mirror.default_client, opener: Any = None) -> None:
    """Run one publisher task. The job writes one line per unit and no line holds a key, a digest or a name."""
    env = os.environ if environment is None else environment
    missing = [name for name in REQUIRED if not env.get(name)]
    if missing:
        raise PublisherError(f"The publisher job needs {', '.join(missing)}.")
    if any(env.get(name) for name in RETIRED):
        raise PublisherError("The launch names an inputs archive or a prior state, which the host job never takes.")
    if env["PUBLISHER_RUN_KIND"] not in KINDS:
        raise PublisherError("The run kind is not one the launch lists.")
    retry = check_retry(env)
    root = state_root(env)
    try:
        bundle = host_inputs.prepare(Path.cwd())
    except host_inputs.InputsError as error:
        raise PublisherError(str(error)) from None
    startup_check.check_environment("publisher")
    switched = env.get(task_role.KEYS_VARIABLE) == "1"
    startup_check.check_queue_files("publisher", bundle / "queue.json", controls=None, keys=switched, roots={"code": Path.cwd()}, listing=False)
    storage = task_role.storage_from_environment(env, factory)
    if env["PUBLISHER_RUN_KIND"] != "bootstrap":
        require_bootstrap(storage, root)
    recheck_units = outstanding_units(storage, root, *retry) if retry is not None else frozenset()
    before = capture_mode.list_versions(storage, capture_mode.evidence_prefixes(root))

    def emit(item: dict) -> None:
        sys.stdout.write(json.dumps(item, sort_keys=True) + "\n")
        sys.stdout.flush()

    run = capture_mode.CaptureRun(
        queue_path=bundle / "queue.json",
        specs_path=bundle / "capture_specs.json",
        root=root,
        run_id=env["RUN_ID"],
        storage=storage,
        scratch=workspace_of(env) / "scratch",
        code_sha256=env["CODE_SHA256"],
        bundle_sha256=json.loads((bundle / "inputs.lock.json").read_text(encoding="utf-8"))["bundle_sha256"],
        hosts=bundle / "publisher_hosts_scope.json",
        hosts_approval=bundle / "hosts_approval.json",
        opener=opener,
        task_keys=partial(task_role.TaskKeys, environment=env) if switched else None,
        recheck=env["PUBLISHER_RUN_KIND"] == "monthly",
        recheck_units=recheck_units,
        log=emit,
    )
    record = capture_mode.execute(run)
    problems = capture_mode.check_run(storage, record, before)
    emit({"counters": record["counters"], "evidence_problems": len(problems)})
    if problems:
        raise PublisherError(f"The evidence check found {len(problems)} problem{'s' if len(problems) != 1 else ''} in the run.")
    if env["PUBLISHER_RUN_KIND"] == "bootstrap":
        body = json.dumps({"kind": "bootstrap_run", "run_id": env["RUN_ID"], "code_sha256": env["CODE_SHA256"]}, sort_keys=True).encode()
        capture_mode.put_once(run, bootstrap_key(root, env["RUN_ID"]), body)


if __name__ == "__main__":
    resource_peaks.recorded(main)
