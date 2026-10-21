"""Run control for the Airflow transform DAG: the actions behind the broker's control rows, and the build file checkpoint.

A broker template row with a ``control`` object names an action the broker performs itself on the launcher's run methods (``acquire``, ``finalize``,
``mark`` and ``release``) instead of forwarding a container. The caller sends only the template, run ID, mode and attempt, as for every submission.
Each action is idempotent, so a repeated request, a task retry and a broker restart all end in the same state:

- ``acquire``: take the run's lock through the launcher (a definite refusal ends the submission), make the run folder with the row's empty folders and copy
  the row's seed into it once.
- ``record``: write the durable run record (state ``finalizing``) and move the run to ``finalizing``.
- ``gate_accepted`` and ``gate_rejected``: write the gate's result into the run record. An accepted result needs a finalizing run.
- ``release_accepted``: after the recorded result is ``accepted``, stop the run's open attempts, delete the row's build files and release the lock.
- ``release_rejected``: stop the run's open attempts, keep the build files and move the run to ``failed_retained`` when it still holds the lock.

Only the owner's ``run_lock release`` frees a ``failed_retained`` run; no action here passes the owner flag. The build file checkpoint lives in the intent
line: a step that validates the build file records the file and the validator's result file there when the submission is accepted. The validator copies the
build file inside its container, runs every suite on that copy and reports the SHA-256 of the copy. When the job has exited the broker records that SHA-256 as
the validated identity and ends the step failed when the file on the host is not the one the validator read. A step that needs the validated file records the
validation it was accepted against and is refused before dispatch when the file is not that one.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import time
from pathlib import Path
from typing import Any

from orchestration import launcher
from orchestration.interface import SubmissionRequest
from scripts.orchestration.broker import (
    DOMAIN,
    REQUIRES_INPUT,
    VALIDATED_BY_INPUT,
    VALIDATED_SHA_INPUT,
    Broker,
    LauncherUnavailable,
    request_id_of,
    submission_request,
    substitute,
    write_private,
)
from scripts.orchestration.ledger import now

ACTIONS = (
    "acquire",
    "record",
    "gate_accepted",
    "gate_rejected",
    "release_accepted",
    "release_rejected",
    "catalog_up",
    "catalog_release",
    "snapshot_project",
    "reserve_silver",
    "seal_silver",
    "seal_gold",
    "fixture_pin",
    "fixture_verify",
)
REQUIRED_PARAMS = {
    "acquire": (),
    "record": ("record_file",),
    "gate_accepted": ("record_file", "expected_states"),
    "gate_rejected": ("record_file", "expected_states"),
    "release_accepted": ("record_file", "build_files"),
    "release_rejected": (),
    "catalog_up": (),
    "catalog_release": (),
    "snapshot_project": (),
    "reserve_silver": (),
    "seal_silver": (),
    "seal_gold": (),
    "fixture_pin": (),
    "fixture_verify": (),
}
OPTIONAL_PARAMS = {"acquire": ("seed", "folders")}
SEEDED_MARKER = ".seeded"
STOP_WAIT_SECONDS = 90.0
FINAL_STATES = ("finished", "failed_closed")
GATE_RESULTS = {"gate_accepted": "accepted", "gate_rejected": "rejected"}
SHA256_HEX = re.compile(r"[0-9a-f]{64}")
RESULT_BYTES = 8 << 20


class ControlRefused(Exception):
    """The launcher or the record refuses the action for a reason that will not change by repeating it; the submission ends with exit code 125."""


class ControlFailed(Exception):
    """The action could not be completed (a file system or launcher failure); the submission ends with exit code 1 and the same action can run again."""


def check_row(name: str, mode: str, control: dict[str, Any]) -> None:
    """Refuse a control object whose action is unknown or whose parameters are not exactly the action's."""
    action = control.get("action")
    if action not in ACTIONS:
        raise ValueError(f"{name}/{mode}: control action {action!r} is not one of {', '.join(ACTIONS)}")
    given = set(control.get("params", {}))
    allowed = set(REQUIRED_PARAMS[action]) | set(OPTIONAL_PARAMS.get(action, ()))
    missing, extra = sorted(set(REQUIRED_PARAMS[action]) - given), sorted(given - allowed)
    if missing or extra:
        raise ValueError(f"{name}/{mode}: control action {action} allows {sorted(allowed)}; missing {missing}, unexpected {extra}")


def inside(path: Path, roots: list[Path]) -> bool:
    existing = path
    rest: list[str] = []
    while not existing.exists() and existing != existing.parent:
        rest.append(existing.name)
        existing = existing.parent
    resolved = existing.resolve().joinpath(*reversed(rest))
    return any(resolved == root.resolve() or root.resolve() in resolved.parents for root in roots)


def perform(broker: Broker, request: SubmissionRequest, action: str, params: dict[str, Any]) -> dict[str, Any]:
    """Run one control action; the result is merged into the ``finished`` ledger line and the submission record."""
    from scripts.orchestration import pipeline_control

    handler = {
        "acquire": acquire,
        "record": record,
        "gate_accepted": gate,
        "gate_rejected": gate,
        "release_accepted": release_accepted,
        "release_rejected": release_rejected,
    }.get(action, pipeline_control.perform)
    try:
        return handler(broker, request, action, params)
    except OSError as error:
        raise ControlFailed(f"{action}: {type(error).__name__}: {error.strerror or error}") from None


def run_folder(broker: Broker, request: SubmissionRequest) -> Path:
    return broker.runs_root / request.run_id


def acquire(broker: Broker, request: SubmissionRequest, action: str, params: dict[str, Any]) -> dict[str, Any]:
    seed = checked_seed(broker, Path(str(params["seed"]))) if params.get("seed") else None
    held = True
    try:
        broker.principal(request.run_id)
    except LauncherUnavailable:
        if not broker.token_absent(request.run_id):
            raise
        held = False
    if not held:
        arguments = {"domain": DOMAIN, "run_id": request.run_id, "mode": request.mode, "holder": "broker", "checkout": str(broker.repo_root)}
        try:
            broker.call("acquire", arguments, request_id=request_id_of(f"{request.run_id}:{request.mode}", "acquire"))
        except launcher.ClientError as error:
            raise ControlRefused(f"the launcher refused the lock: {error} [{error.code}]") from None
        broker.principal(request.run_id)
    state = run_state(broker, request)
    if state != "held":
        raise ControlRefused(f"the run is {state}; only a held run is prepared")
    folder = run_folder(broker, request)
    folder.mkdir(parents=True, exist_ok=True)
    for name in params.get("folders", []):
        (folder / name).mkdir(exist_ok=True)
    seeded = False
    if seed is not None and not (folder / SEEDED_MARKER).exists():
        shutil.copytree(seed, folder, dirs_exist_ok=True)
        (folder / SEEDED_MARKER).write_text(f"{now()}\n")
        seeded = True
    return {"run_folder": str(folder), "seeded": seeded, "already_held": held}


def checked_seed(broker: Broker, source: Path) -> Path:
    if not source.is_dir() or not inside(source, [broker.repo_root / "data", broker.paths.state]):
        raise ControlRefused(f"the seed {source.name} is not a folder inside the data folder")
    if any(item.is_symlink() for item in source.rglob("*")):
        raise ControlRefused("the seed holds a link")
    return source


def run_state(broker: Broker, request: SubmissionRequest) -> str | None:
    try:
        principal = broker.principal(request.run_id)
    except LauncherUnavailable:
        if broker.token_absent(request.run_id):
            return None
        raise
    try:
        view = broker.call("status", {"what": "run", "id": request.run_id}, principal)
    except launcher.ClientError as error:
        raise ControlRefused(f"the launcher cannot show the run: {error} [{error.code}]") from None
    return str(view["state"])


def submission_states(broker: Broker, request: SubmissionRequest) -> list[dict[str, Any]]:
    states = []
    for submission_id, lines in sorted(broker.ledger.submissions(request.run_id).items()):
        last = lines[-1]
        if last["event"] == "finished":
            state = "no_op" if last["no_op"] else "succeeded" if last["exit_code"] == 0 else "cancelled" if last.get("cancelled") else "failed"
        else:
            state = "failed_closed" if last["event"] == "failed_closed" else "unfinished"
        states.append(
            {
                "submission_id": submission_id,
                "template": lines[0]["template"],
                "attempt": lines[0]["attempt"],
                "state": state,
                "exit_code": last.get("exit_code"),
            }
        )
    return states


def read_record(path: Path, run_id: str) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        body: dict[str, Any] = json.loads(path.read_text())
    except ValueError:
        raise ControlRefused(f"the run record {path.name} is not valid JSON") from None
    if body.get("run_id") != run_id:
        raise ControlRefused(f"the run record {path.name} names {body.get('run_id')}, not {run_id}")
    return body


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


UNREADABLE = "unreadable"


def file_sha256(path: Path) -> str:
    """The SHA-256 of the file at ``path`` as 64 hexadecimal digits, or ``UNREADABLE`` when the operating system cannot read it."""
    try:
        return sha256_of(path)
    except OSError:
        return UNREADABLE


def validated_sha256(broker: Broker, request: SubmissionRequest, validated_by: str, file: str) -> str:
    """The SHA-256 the latest successful ``validated_by`` step of the run recorded for ``file``, or an empty string when it recorded none. The caller holds
    the run's ledger lock."""
    recorded = [
        lines[-1]["build_file"]
        for lines in broker.ledger.submissions(request.run_id).values()
        if lines[0]["template"] == validated_by
        and lines[-1]["event"] == "finished"
        and lines[-1]["exit_code"] == 0
        and (lines[-1].get("build_file") or {}).get("file") == file
        and lines[-1]["build_file"]["run_id"] == request.run_id
    ]
    return str(recorded[-1]["sha256"]) if recorded else ""


def reported_sha256(result_file: Path) -> str:
    """The SHA-256 of the build file copy the validator read, from the result file it wrote. Raises ``ControlFailed`` when the file cannot be read, is not
    a JSON object or holds no SHA-256."""
    try:
        with result_file.open("rb") as handle:
            raw = handle.read(RESULT_BYTES + 1)
    except OSError as error:
        raise ControlFailed(f"the validator's result {result_file.name} cannot be read: {type(error).__name__}") from None
    if len(raw) > RESULT_BYTES:
        raise ControlFailed(f"the validator's result {result_file.name} is larger than {RESULT_BYTES} bytes")
    try:
        body = json.loads(raw)
    except ValueError:
        raise ControlFailed(f"the validator's result {result_file.name} is not valid JSON") from None
    reported = body.get("build_file_sha256") if isinstance(body, dict) else None
    if not isinstance(reported, str) or not SHA256_HEX.fullmatch(reported):
        raise ControlFailed(f"the validator's result {result_file.name} reports no SHA-256 of the build file it read")
    return reported


def record_build_file(broker: Broker, request: SubmissionRequest, file: str, result_file: str, attempt_id: str) -> dict[str, str]:
    """The identity of the build file a job has just validated: its path, the SHA-256 the validator reported for the copy it read, the run, the submission
    and the launcher attempt. A file that has another SHA-256 on the host now is not the file the validator read, and nothing is recorded."""
    path, result = Path(file), Path(result_file)
    if not inside(path, [broker.runs_root]) or not inside(result, [broker.runs_root]):
        raise ControlFailed(f"the build file {path.name} or the validator's result {result.name} is outside the run folder")
    reported = reported_sha256(result)
    digest = file_sha256(path)
    if digest == UNREADABLE:
        raise ControlFailed(f"the build file {path.name} could not be read when {request.template} ended")
    if digest != reported:
        raise ControlFailed(f"the build file changed while {request.template} ran (the validator read {reported[:12]}, now {digest[:12]})")
    return {"file": str(path), "sha256": reported, "run_id": request.run_id, "submission_id": request.submission_id, "attempt_id": attempt_id}


def checkpoint_problem(inputs: dict[str, str]) -> str | None:
    """Why the checkpoint in an intent line refuses the dispatch, or None: the validating step recorded no hash for the file when the submission was
    accepted, or the file is not the one it recorded."""
    if REQUIRES_INPUT not in inputs:
        return None
    validated_by, expected = inputs[VALIDATED_BY_INPUT], inputs[VALIDATED_SHA_INPUT]
    if not expected:
        return f"{validated_by} recorded no hash of the build file for this run"
    path = Path(inputs[REQUIRES_INPUT])
    digest = file_sha256(path)
    if digest == UNREADABLE:
        return f"the build file {path.name} cannot be read"
    if digest != expected:
        return f"the build file changed after {validated_by} validated it (recorded {expected[:12]}, now {digest[:12]})"
    return None


def recorded_result(broker: Broker, run_id: str, mode: str) -> str | None:
    """The gate result in the run's record (``accepted`` or ``rejected``), or None when there is no record, no result or the mode has no record step. A
    record that cannot be read raises ``OSError`` or ``ControlRefused``: the result is then unknown."""
    control = (broker.templates.get("run_record", {}).get("modes", {}).get(mode) or {}).get("control")
    if control is None:
        return None
    path = Path(str(substitute(control["params"]["record_file"], broker.placeholder_values(run_id, mode))))
    result = ((read_record(path, run_id) or {}).get("gate") or {}).get("result")
    return str(result) if result in GATE_RESULTS.values() else None


def write_record(broker: Broker, path: Path, body: dict[str, Any]) -> None:
    if not inside(path, [broker.repo_root / "data", broker.paths.state]):
        raise ControlRefused(f"the run record path {path.name} is outside the data folder")
    write_private(path, broker.redact(json.dumps(body, indent=2, sort_keys=True)) + "\n")


def record(broker: Broker, request: SubmissionRequest, action: str, params: dict[str, Any]) -> dict[str, Any]:
    state = run_state(broker, request)
    if state not in ("held", "finalizing"):
        raise ControlRefused("the lock is not held" if state is None else f"the run is {state}; only a held run is recorded")
    path = Path(str(params["record_file"]))
    previous = read_record(path, request.run_id) or {}
    body = {
        **previous,
        "schema_version": 1,
        "run_id": request.run_id,
        "mode": request.mode,
        "state": "finalizing",
        "written_at": now(),
        "submissions": submission_states(broker, request),
    }
    write_record(broker, path, body)
    broker.crash("after_record")
    try:
        broker.call("finalize", {}, broker.principal(request.run_id))
    except launcher.ClientError as error:
        raise ControlRefused(f"the launcher refused to finalize: {error} [{error.code}]") from None
    broker.crash("after_finalize")
    return {"record_file": str(path)}


def task_state_evidence(expected: dict[str, str], snapshot: dict[str, Any]) -> dict[str, Any]:
    if "unavailable" in snapshot:
        return {"status": "unavailable", "reason": str(snapshot["unavailable"])}
    observed = snapshot["tasks"]
    predecessors = [{"task": task, "expected": wanted, "observed": observed.get(task)} for task, wanted in expected.items()]
    return {
        "status": "read",
        "dag_run_id": snapshot["dag_run_id"],
        "dag_run_state": snapshot["dag_run_state"],
        "predecessors": predecessors,
        "failed_tasks": [item["task"] for item in predecessors if item["observed"] != item["expected"]],
    }


def gate(broker: Broker, request: SubmissionRequest, action: str, params: dict[str, Any]) -> dict[str, Any]:
    result = GATE_RESULTS[action]
    state = run_state(broker, request)
    if result == "accepted" and state != "finalizing":
        raise ControlRefused(f"an accepted result needs a finalizing run, not {state or 'no lock'}")
    path = Path(str(params["record_file"]))
    previous = read_record(path, request.run_id)
    if result == "accepted" and previous is None:
        raise ControlRefused("an accepted result needs the run record")
    submissions = submission_states(broker, request)
    failed = [
        item["submission_id"]
        for item in submissions
        if item["state"] in ("failed", "failed_closed", "cancelled") or (item["state"] == "unfinished" and item["template"] != request.template)
    ]
    snapshot = broker.dag_snapshot(request.run_id, request.mode) if broker.dag_snapshot is not None else {"unavailable": "the broker has no Airflow reader"}
    evidence = task_state_evidence(params["expected_states"], snapshot)
    contradicted = result == "accepted" and bool(evidence.get("failed_tasks"))
    stored = "rejected" if contradicted else result
    reason = (
        ""
        if stored == "accepted"
        else "lock not acquired"
        if state is None
        else "the DAG run's task states differ from the expected ones"
        if contradicted
        else "work failed"
    )
    body = {
        **(previous or {"schema_version": 1, "run_id": request.run_id, "mode": request.mode, "state": "failed"}),
        "submissions": submissions,
        "gate": {"result": stored, "reason": reason, "failed": failed, "task_states": evidence, "written_at": now()},
    }
    write_record(broker, path, body)
    if contradicted:
        raise ControlRefused(f"the gate cannot accept the run: {', '.join(evidence['failed_tasks'])} did not end as expected")
    return {"record_file": str(path), "gate": result}


def stop_open(broker: Broker, request: SubmissionRequest) -> list[str]:
    try:
        principal = broker.principal(request.run_id)
    except LauncherUnavailable:
        if broker.token_absent(request.run_id):
            return []
        raise
    attempts = []
    for submission_id, lines in sorted(broker.ledger.submissions(request.run_id).items()):
        if lines[-1]["event"] in FINAL_STATES or submission_id == request.submission_id or broker.recorded_as_control(lines[0]):
            continue
        asked = submission_request(submission_id, lines[0])
        forward = broker.read_forward(asked) or broker.accepted_forward(asked, principal)
        if forward is None:
            continue
        attempt_id = str(forward["attempt_id"])
        try:
            broker.call("cancel", {"attempt_id": attempt_id}, principal)
        except launcher.ClientError as error:
            raise ControlFailed(f"the launcher refused to cancel attempt {attempt_id}: {error} [{error.code}]") from None
        attempts.append(attempt_id)
    deadline = time.monotonic() + STOP_WAIT_SECONDS
    for attempt_id in attempts:
        while broker.call("status", {"what": "attempt", "id": attempt_id}, principal)["phase"] != "ended":
            if time.monotonic() > deadline:
                raise ControlFailed(f"attempt {attempt_id} did not end within {STOP_WAIT_SECONDS:.0f} s")
            time.sleep(0.5)
    return attempts


def release_accepted(broker: Broker, request: SubmissionRequest, action: str, params: dict[str, Any]) -> dict[str, Any]:
    path = Path(str(params["record_file"]))
    body = read_record(path, request.run_id)
    if body is None or body.get("gate", {}).get("result") != "accepted":
        raise ControlRefused("the recorded gate result is not accepted")
    state = run_state(broker, request)
    if state is None:
        raise ControlRefused("the lock is not held")
    stopped = stop_open(broker, request) if state != "released" else []
    if state != "released" and request.mode in ("publish_fixture", "real"):
        from scripts.orchestration import pipeline_control

        pipeline_control.perform(broker, request, "catalog_release", {})
    folder = run_folder(broker, request)
    deleted = []
    for name in params["build_files"]:
        target = Path(str(name))
        if target.parent.resolve() != folder.resolve():
            raise ControlRefused(f"the build file {target.name} is outside the run folder")
        if target.is_symlink() or target.is_file():
            target.unlink()
            deleted.append(target.name)
        elif target.exists():
            raise ControlRefused(f"the build file {target.name} is not a file")
    broker.crash("after_delete")
    if state != "released":
        try:
            broker.call("release", {"record": str(path)}, broker.principal(request.run_id), request_id_of(request.submission_id, "release"))
        except launcher.ClientError as error:
            raise ControlFailed(f"the launcher refused the release: {error} [{error.code}]") from None
    return {"deleted": deleted, "stopped_attempts": stopped, "released": True}


def release_rejected(broker: Broker, request: SubmissionRequest, action: str, params: dict[str, Any]) -> dict[str, Any]:
    stopped = stop_open(broker, request)
    state = run_state(broker, request)
    if state in ("held", "finalizing") and request.mode in ("publish_fixture", "real"):
        from scripts.orchestration import pipeline_control

        pipeline_control.perform(broker, request, "catalog_release", {})
    if state in ("held", "finalizing"):
        try:
            broker.call("mark", {"state": "failed_retained"}, broker.principal(request.run_id))
        except launcher.ClientError as error:
            raise ControlFailed(f"the launcher refused to retain the run: {error} [{error.code}]") from None
    return {"stopped_attempts": stopped, "run_state_before": state}
