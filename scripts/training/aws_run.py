"""Run one phase of a training run inside an AWS task.

    python -m scripts.training.aws_run prepare|extract|train|promote|research|canary|notify --run <run ID> [--mode real|fixture] [--work /workspace]

The task definition chains three containers on one shared ``/workspace``. A fetch container has already downloaded ``code.tar.gz`` of the run, checked
``CODE_SHA256`` and unpacked it to ``<work>/code``. Every phase runs from that folder, so the modules it imports are the bytes the record names.

- ``prepare`` (training image): checks the archive hash again, records the code identity in ``snapshot_training.json`` and the validated parameters in
  ``training_parameters.json``, then runs the ``guard`` action.
- ``extract`` (feature store image): runs the extraction against the catalog and the Feast registry of the data bucket.
- ``train`` (training image): logs in to MLflow with MLFLOW_TRACKING_USERNAME and MLFLOW_TRACKING_PASSWORD, because the server grants nothing without a login.
  It then runs the actions of the run's model (``training_contract.train_steps``): ``version_extract``, ``fit``, ``hac_validate`` and ``log_run``, the
  ``mediation`` for m2 and the ``promotion_check`` for m1, m2 and m4. The last of these actions judges the run by the saved fit results
  (``job_actions.accept_training``, which ``job.main`` runs after the action has saved its records): a run that fitted none of its requested types fails with
  ``nothing_fitted`` and names each type's status and reason, so the launcher records a failed training run and the bundles and MLflow runs stay. A run that
  fitted at least one type passes, and the phase prints one ``outcome`` line that names every unfitted type with its reason. The check holds for every model.
- ``research`` (training image, a separate task): checks the research parameters (``research_dag``), records the code identity and runs the ``research`` action,
  which reads the finished MLflow runs of m1, m2 and m4 on one ``base_run``, runs RQ1 and RQ4 and sets the one ``challenger`` alias of each type. It stores
  ``research/<run>/research_<type>.json`` once. It moves no production alias.
- ``canary`` (training image, a separate task of the promote family): judges the canary of one type's challenger from
  ``research/<research run>/replay_<type>.json`` with the owner's pass rule, tags the registry version with the result on a pass and stores
  ``canary_<type>.json`` once. A failed or missing replay is a stored hold, not a failed phase. ``TRAINING_PARAMETERS`` names the research run and the type
  (``training_contract.chain_parameters_for``).
- ``notify`` (training image, a separate task of the promote family): publishes the outcome of one type's challenger to the alerts topic
  ``ALERTS_TOPIC_ARN`` and stores ``notice_<type>.json`` once. It moves no alias and tags nothing.
- ``promote`` (training image, a separate task): records the code identity and the promotion parameters, then runs the ``promote`` action. It moves the
  ``production`` alias of one registered model to the version the parameters name. The rule and the canary decide first (``auto_promotion``). Before the
  alias moves, the action takes the lease of the model (``promote/models/<model>/lease-<generation>.json``, one run at a time, create-only) and stores the
  intent ``promote/<run>/intent.json`` in the data bucket, once, with the version production held. Before the phase reports success it copies the receipt
  ``promote.json`` to ``promote/<run>/promote.json``, once, because the workspace ends with the task, and only then closes the lease. A retry in a new task
  resumes its own lease from the intent, or returns the receipt, so it never moves the alias again for a run that completed. Another run for the model is
  refused while the lease is held.

Each action writes to ``<work>/in/<action>``, where the next action reads it, as the local renderer mounts them. A phase checks the completion records of
the steps an action needs before it starts that action. A rerun in the same workspace resumes: a completed step is verified and not run again. A rerun in a
new task starts empty and repeats safely, because the DVC remote is content addressed and ``log_run`` logs each run once by its key.

Each finished step prints one JSON line to standard output. A failure prints one JSON line to standard error and stops the phase with the action's exit code.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib
import io
import json
import os
import re
import sys
import tarfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from orchestration import code_digest, mlflow_service, training_contract
from scripts import phase_failure, resource_peaks
from scripts.infrastructure import startup_check
from scripts.lakehouse import task_capacity
from scripts.training import job, promote_lease, promote_receipt, training_metrics
from scripts.training.job_failure import EXIT_FAILED
from scripts.training.step_records import verify_done

PHASES = ("prepare", "extract", "train", "promote", "research", "canary", "notify")
ARCHIVE = "code.tar.gz"
COMMIT = re.compile(r"[a-f0-9]{40}")
TRACKING_URI = re.compile(rf"http://(?:10\.[0-9]{{1,3}}|172\.(?:1[6-9]|2[0-9]|3[01])|192\.168)\.[0-9]{{1,3}}\.[0-9]{{1,3}}:{mlflow_service.PORT}")
BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
TOPIC = re.compile(r"arn:aws:sns:[a-z0-9-]+:[0-9]{12}:[A-Za-z0-9_-]{1,256}")
ENVIRONMENT_DEFAULTS = {
    "MLFLOW_DISABLE_TELEMETRY": "true",
    "DO_NOT_TRACK": "true",
    "DVC_NO_ANALYTICS": "1",
    "MLFLOW_HTTP_REQUEST_TIMEOUT": "20",
    "MLFLOW_HTTP_REQUEST_MAX_RETRIES": "0",
}


class RunRefused(Exception):
    """The phase cannot start safely; the message says what to fix."""


def required(name: str) -> str:
    """Return a variable the task definition or the `run-task` override must set."""
    value = os.environ.get(name, "")
    if not value:
        raise RunRefused(f"{name} is not set")
    return value


def write_once(path: Path, text: str) -> None:
    """Write a file the first time, accept the same bytes again and refuse other bytes, so a rerun cannot change a record."""
    if path.exists():
        if path.read_text(encoding="utf-8") != text:
            raise RunRefused(f"{path.name} already holds another record")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.partial")
    with partial.open("w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(partial, path)


def read_parameters(raw: Any) -> dict[str, Any]:
    """Validate the parameters of an AWS task: the complete closed set, with the AWS MLflow lane."""
    if not isinstance(raw, dict) or raw.get("mlflow") != "aws":
        raise RunRefused("an AWS task needs the parameters of a training run with mlflow=aws")
    try:
        return training_contract.parameters_for("training_dag", raw, ("aws",))
    except ValueError as error:
        raise RunRefused(str(error)) from None


def recorded_parameters(work: Path) -> dict[str, Any]:
    """Read the parameter file that prepare wrote."""
    try:
        return read_parameters(json.loads((work / "training_parameters.json").read_bytes()))
    except (OSError, ValueError) as error:
        raise RunRefused(f"training_parameters.json cannot be read: {error}. Run the prepare phase first") from None


def recorded_digest(work: Path, run: str) -> str:
    """Return the code digest of the snapshot record that prepare wrote."""
    try:
        record = json.loads((work / "snapshot_training.json").read_bytes())
    except (OSError, ValueError) as error:
        raise RunRefused(f"snapshot_training.json cannot be read: {error}. Run the prepare phase first") from None
    if record.get("run_id") != run:
        raise RunRefused("the snapshot record belongs to another run")
    return str(record["code_digest"])


def check_prerequisites(work: Path, run: str, action: str) -> None:
    """Require the completion record of every step the action needs."""
    digest = recorded_digest(work, run)
    needed = training_contract.PREREQUISITES[action]
    if needed:
        needed = training_contract.prerequisites_for(action, str(recorded_parameters(work)["model"]))
    for step in needed:
        try:
            verify_done(work / "in" / step, step, digest)
        except (OSError, ValueError, KeyError) as error:
            raise RunRefused(f"{action} needs the completed step {step}: {error}") from None


def run_action(work: Path, run: str, mode: str, action: str) -> dict[str, Any]:
    """Run one job action in this process under the job's own deadline and return its step line."""
    check_prerequisites(work, run, action)
    folder = work / "in" / action
    folder.mkdir(parents=True, exist_ok=True)
    out, err = io.StringIO(), io.StringIO()
    arguments = [
        action,
        "--run",
        run,
        "--mode",
        mode,
        "--run-dir",
        str(folder),
        "--parameters",
        str(work / "training_parameters.json"),
        "--snapshot-record",
        str(work / "snapshot_training.json"),
    ]
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = job.main(arguments)
    if code:
        failure = json.loads(err.getvalue().strip().splitlines()[-1]) if err.getvalue().strip() else {}
        raise StepFailed(action, code, str(failure.get("failure", "failed")), str(failure.get("detail", "")))
    lines = [line for line in out.getvalue().splitlines() if line.startswith("{")]
    resumed = bool(json.loads(lines[-1]).get("resumed")) if lines else False
    return {"step": action, "code": 0, "resumed": resumed}


class StepFailed(Exception):
    """One action exited nonzero; the phase stops with its code."""

    def __init__(self, step: str, code: int, failure: str, detail: str) -> None:
        super().__init__(f"{step}: {failure}")
        self.step, self.code, self.failure, self.detail = step, code, failure, detail


def archive_commit(archive: Path) -> str:
    """Return the commit that git archive recorded in the global header of the archive."""
    try:
        with tarfile.open(archive) as tar:
            commit = str(tar.pax_headers.get("comment", ""))
    except (OSError, tarfile.TarError) as error:
        raise RunRefused(f"{archive.name} cannot be read: {error}") from None
    if not COMMIT.fullmatch(commit):
        raise RunRefused(f"{archive.name} carries no commit ID. It must come from git archive")
    return commit


def record_code(work: Path, run: str) -> None:
    """Check the archive against CODE_SHA256 and write the snapshot record of the unpacked code once."""
    archive = work / ARCHIVE
    expected = required("CODE_SHA256")
    if not archive.is_file() or hashlib.sha256(archive.read_bytes()).hexdigest() != expected:
        raise RunRefused(f"{ARCHIVE} does not match CODE_SHA256")
    commit = archive_commit(archive)
    code = work / "code"
    try:
        entries = code_digest.file_entries(code)
    except code_digest.SnapshotError as error:
        raise RunRefused(f"the unpacked code is not a training snapshot: {error}") from None
    record = {
        "schema_version": 1,
        "run_id": run,
        "code_digest": code_digest.entries_digest(entries),
        "files": entries,
        "git_commit": commit,
        "git_dirty": False,
    }
    write_once(work / "snapshot_training.json", json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")


def prepare(work: Path, run: str, mode: str) -> list[dict[str, Any]]:
    """Record the code identity and the parameters, then run the guard."""
    parameters = read_parameters(parse_json("TRAINING_PARAMETERS"))
    record_code(work, run)
    write_once(work / "training_parameters.json", json.dumps(parameters) + "\n")
    return [run_action(work, run, mode, "guard")]


def promote(work: Path, run: str, mode: str) -> list[dict[str, Any]]:
    """Record the code identity and the promotion parameters, then move the production alias in the MLflow service."""
    raw = parse_json("TRAINING_PARAMETERS")
    try:
        parameters = training_contract.parameters_for("model_promotion_dag", raw, ("aws",))
    except ValueError as error:
        raise RunRefused(f"an AWS promotion needs the parameters of a promotion run with mlflow=aws: {error}") from None
    uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if not TRACKING_URI.fullmatch(uri):
        raise RunRefused(f"MLFLOW_TRACKING_URI must be http://<private IPv4 address>:{mlflow_service.PORT}. It is {uri or 'not set'}")
    for name in ("MLFLOW_TRACKING_USERNAME", "MLFLOW_TRACKING_PASSWORD"):
        required(name)
    destination = bucket()
    task_environment(work)
    record_code(work, run)
    write_once(work / "training_parameters.json", json.dumps(parameters) + "\n")
    step = run_action(work, run, mode, "promote")
    publish_receipt(work, run, destination)
    training_metrics.promotion_executed(parameters)
    sys.stdout.write(json.dumps(step, sort_keys=True) + "\n")
    return [step]


def research(work: Path, run: str, mode: str) -> list[dict[str, Any]]:
    """Record the code identity and the research parameters, run the research action and store each type's record once in the data bucket."""
    raw = parse_json("TRAINING_PARAMETERS")
    try:
        parameters = training_contract.parameters_for("research_dag", raw, ("aws",))
    except ValueError as error:
        raise RunRefused(f"an AWS research needs the parameters of a research run with mlflow=aws: {error}") from None
    uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if not TRACKING_URI.fullmatch(uri):
        raise RunRefused(f"MLFLOW_TRACKING_URI must be http://<private IPv4 address>:{mlflow_service.PORT}. It is {uri or 'not set'}")
    for name in ("MLFLOW_TRACKING_USERNAME", "MLFLOW_TRACKING_PASSWORD"):
        required(name)
    destination = bucket()
    task_environment(work)
    record_code(work, run)
    write_once(work / "training_parameters.json", json.dumps(parameters) + "\n")
    step = run_action(work, run, mode, "research")
    publish_research(work, run, destination, parameters["hai_types"])
    sys.stdout.write(json.dumps(step, sort_keys=True) + "\n")
    return [step]


def publish_research(work: Path, run: str, destination: str, hai_types: Sequence[str]) -> None:
    """Store the research record of each type under ``research/<run>/`` once; a second store accepts the same bytes and refuses other bytes."""
    client = promote_receipt.s3_client()
    for hai_type in hai_types:
        name = f"research_{hai_type}.json"
        key, body = f"research/{run}/{name}", (work / "in" / "research" / name).read_bytes()
        try:
            created = promote_receipt.put_once(client, destination, key, body)
            stored = body if created else promote_receipt.read(client, destination, key)
        except promote_receipt.ReceiptError as error:
            raise StepFailed("research", EXIT_FAILED, "record_upload_failed", str(error)) from None
        if stored != body:
            raise StepFailed("research", EXIT_FAILED, "record_conflict", f"{key} already holds another record")


def sns_client() -> Any:
    """The notification client of the task role, with short time limits."""
    config = importlib.import_module("botocore.config").Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 2, "mode": "standard"})
    return importlib.import_module("boto3").client("sns", region_name=required("AWS_REGION"), config=config)


def chain_record(client: Any, destination: str, kind: str, parameters: dict[str, Any]) -> dict[str, Any] | None:
    """Read one stored record of the type after the research run, or None when it is not stored; a record that is not a JSON object is refused."""
    key = training_contract.stored_record_key(kind, parameters["research_run"], parameters["hai_type"])
    try:
        body = promote_receipt.read(client, destination, key)
        document = None if body is None else json.loads(body)
    except promote_receipt.ReceiptError as error:
        raise RunRefused(str(error)) from None
    except ValueError:
        raise RunRefused(f"{key} is not JSON") from None
    if document is not None and not isinstance(document, dict):
        raise RunRefused(f"{key} is not a JSON object")
    return document


def research_record(client: Any, destination: str, parameters: dict[str, Any]) -> dict[str, Any]:
    """The research record of the type, which must name its type and a challenger: the steps after research judge and announce that challenger only."""
    record = chain_record(client, destination, "research", parameters)
    if record is None:
        raise RunRefused(f"the research record of {parameters['hai_type']} is not stored for the research run {parameters['research_run']}")
    if record.get("hai_type") != parameters["hai_type"]:
        raise RunRefused(f"the research record is of {record.get('hai_type')}, not of {parameters['hai_type']}")
    challenger = (record.get("promotion") or {}).get("challenger")
    if not isinstance(challenger, dict) or not challenger.get("model") or not challenger.get("version"):
        raise RunRefused(f"the research record of {parameters['hai_type']} has no challenger")
    return record


def store_chain_record(client: Any, destination: str, step: str, kind: str, parameters: dict[str, Any], document: dict[str, Any]) -> None:
    """Store a record once; a second store accepts the same bytes and refuses other bytes, so a rerun cannot change a result."""
    key = training_contract.record_key(kind, parameters["research_run"], parameters["hai_type"])
    body = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
    try:
        created = promote_receipt.put_once(client, destination, key, body)
        stored = body if created else promote_receipt.read(client, destination, key)
    except promote_receipt.ReceiptError as error:
        raise StepFailed(step, EXIT_FAILED, "record_upload_failed", str(error)) from None
    if stored != body:
        raise StepFailed(step, EXIT_FAILED, "record_conflict", f"{key} already holds another record")


def chain_phase(work: Path, run: str, step: str) -> tuple[dict[str, Any], str]:
    """The checks and records the canary check and the owner notice share: the step's closed parameters, the code identity and the data bucket."""
    try:
        parameters = training_contract.chain_parameters_for(step, parse_json("TRAINING_PARAMETERS"), ("aws",))
    except ValueError as error:
        raise RunRefused(f"an AWS {step} needs its closed parameters with mlflow=aws: {error}") from None
    destination = bucket()
    task_environment(work)
    record_code(work, run)
    write_once(work / "training_parameters.json", json.dumps(parameters) + "\n")
    return parameters, destination


def canary(work: Path, run: str, mode: str) -> list[dict[str, Any]]:
    """Judge the canary of the type's challenger from the replay report stored for the research run and store the result once.

    A pass tags the registry version with the result bound to the version and its model run, which the ``promote`` action requires. A replay that failed a
    check, or that was never stored, is a hold: the result is stored with its reasons and the phase succeeds, so the decision, not the phase, carries the
    outcome. A replay, record or version that do not name one candidate are refused (``auto_promotion.CandidateRefused``) and nothing is tagged.
    """
    auto_promotion = importlib.import_module("scripts.training.auto_promotion")
    parameters, destination = chain_phase(work, run, "canary_check")
    uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if not TRACKING_URI.fullmatch(uri):
        raise RunRefused(f"MLFLOW_TRACKING_URI must be http://<private IPv4 address>:{mlflow_service.PORT}. It is {uri or 'not set'}")
    for name in ("MLFLOW_TRACKING_USERNAME", "MLFLOW_TRACKING_PASSWORD"):
        required(name)
    client = promote_receipt.s3_client()
    record = research_record(client, destination, parameters)
    report = chain_record(client, destination, "replay", parameters)
    if report is None:
        decision: dict[str, Any] = {"decision": "hold", "failed": ["replay_report_missing"]}
    else:
        try:
            decision = auto_promotion.record_canary(importlib.import_module("scripts.training.run_tracking").open_store(uri), record, report)
        except (auto_promotion.CandidateRefused, ValueError, KeyError) as error:
            raise RunRefused(f"the canary of {parameters['hai_type']} names no single candidate: {error}") from None
    store_chain_record(client, destination, "canary", "canary", parameters, auto_promotion.canary_result(parameters["research_run"], record, decision))
    training_metrics.canary_decision(parameters["hai_type"], decision)
    step = {"step": "canary", "code": 0, "resumed": False, "decision": decision["decision"]}
    sys.stdout.write(json.dumps(step, sort_keys=True) + "\n")
    return [step]


def notify(work: Path, run: str, mode: str) -> list[dict[str, Any]]:
    """Tell the owner through the alerts topic what became of the type's challenger, once.

    The receipt is stored after the publish, create-only, so a rerun finds it and publishes nothing. A task that dies between the publish and the receipt
    publishes the notice again on its retry: the notice is at-least-once, and the receipt holds the message ID of the one that was stored.
    """
    auto_promotion = importlib.import_module("scripts.training.auto_promotion")
    parameters, destination = chain_phase(work, run, "owner_notice")
    topic = required("ALERTS_TOPIC_ARN")
    if not TOPIC.fullmatch(topic):
        raise RunRefused("ALERTS_TOPIC_ARN is not an SNS topic ARN")
    client = promote_receipt.s3_client()
    record = research_record(client, destination, parameters)
    receipt = chain_record(client, destination, "notice", parameters)
    if receipt is not None:
        if receipt.get("outcome") != parameters["outcome"]:
            raise StepFailed("notify", EXIT_FAILED, "record_conflict", f"the owner was told {receipt.get('outcome')}, not {parameters['outcome']}")
        step = {"step": "notify", "code": 0, "resumed": True}
    else:
        detail = auto_promotion.notice_detail(record, chain_record(client, destination, "canary", parameters), parameters["outcome"])
        try:
            message_id = auto_promotion.notify(sns_client(), topic, parameters["hai_type"], parameters["outcome"], detail)
        except (RuntimeError, OSError, ValueError, ClientError, BotoCoreError) as error:
            raise StepFailed("notify", EXIT_FAILED, "publish_failed", f"{type(error).__name__}: {error}") from None
        store_chain_record(
            client,
            destination,
            "notify",
            "notice",
            parameters,
            {
                "schema": 1,
                "research_run": parameters["research_run"],
                "hai_type": parameters["hai_type"],
                "outcome": parameters["outcome"],
                "message_id": message_id,
            },
        )
        step = {"step": "notify", "code": 0, "resumed": False}
    training_metrics.notice_verdict(parameters["hai_type"], parameters["outcome"])
    sys.stdout.write(json.dumps(step, sort_keys=True) + "\n")
    return [step]


def publish_receipt(work: Path, run: str, destination: str) -> None:
    """Store the receipt of the promote step in the data bucket, so it outlives the task, then close the lease of the model.

    The lease stays held until the receipt is durable, so a failure of either call stops the phase before it reports success. A retry finds the receipt and
    closes the lease.
    """
    try:
        body = (work / "in" / "promote" / promote_receipt.RECORD).read_bytes()
        client = promote_receipt.s3_client()
        promote_receipt.upload(client, destination, run, body)
    except (OSError, promote_receipt.ReceiptError) as error:
        raise StepFailed("promote", EXIT_FAILED, "receipt_upload_failed", str(error)) from None
    try:
        promote_lease.release(client, destination, str(json.loads(body)["model"]), run)
    except (ValueError, KeyError, promote_receipt.ReceiptError) as error:
        raise StepFailed("promote", EXIT_FAILED, "lease_release_failed", f"the receipt is stored but the lease is open: {error}") from None


def parse_json(name: str) -> Any:
    """Read a JSON variable."""
    try:
        return json.loads(required(name))
    except ValueError:
        raise RunRefused(f"{name} is not JSON") from None


def task_environment(work: Path) -> None:
    """Set the variables every action expects, without replacing one the task definition already set."""
    for name, value in ENVIRONMENT_DEFAULTS.items():
        os.environ.setdefault(name, value)
    temporary = work / "tmp"
    temporary.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("TMPDIR", str(temporary))


def bucket() -> str:
    """Return the data bucket name."""
    name = required("DATA_BUCKET_NAME")
    if not BUCKET.fullmatch(name):
        raise RunRefused("DATA_BUCKET_NAME is not a bucket name")
    return name


def extract(work: Path, run: str, mode: str) -> list[dict[str, Any]]:
    """Run the extraction against the real features namespace and the Feast registry of the data bucket."""
    recorded_parameters(work)
    check_prerequisites(work, run, "extract")
    required("POLARIS_URI")
    task_environment(work)
    os.environ.update(
        importlib.import_module("scripts.features.training_extract").aws_reader_environment(bucket(), os.environ["TMPDIR"], mode, region=required("AWS_REGION"))
    )
    folder = work / "in" / "extract"
    folder.mkdir(parents=True, exist_ok=True)
    resumed = (folder / "extract.done.json").exists()
    module = importlib.import_module("scripts.features.training_extract")
    code = module.main(
        [
            "extract",
            "--run",
            run,
            "--mode",
            mode,
            "--run-dir",
            str(folder),
            "--parameters",
            str(work / "training_parameters.json"),
            "--snapshot-record",
            str(work / "snapshot_training.json"),
        ]
    )
    if code:
        raise StepFailed("extract", code, "extract_failed", "the extraction failed; its message is above")
    return [{"step": "extract", "code": 0, "resumed": resumed}]


def train(work: Path, run: str, mode: str) -> list[dict[str, Any]]:
    """Run the six training actions in order against the MLflow service and the DVC remote."""
    recorded_parameters(work)
    uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if not TRACKING_URI.fullmatch(uri):
        raise RunRefused(f"MLFLOW_TRACKING_URI must be http://<private IPv4 address>:{mlflow_service.PORT}. It is {uri or 'not set'}")
    for name in ("MLFLOW_TRACKING_USERNAME", "MLFLOW_TRACKING_PASSWORD"):
        required(name)
    os.environ.setdefault("DVC_REMOTE_URL", f"s3://{bucket()}/dvc/")
    task_environment(work)
    steps_of_model = training_contract.train_steps(str(recorded_parameters(work)["model"]))
    check_prerequisites(work, run, steps_of_model[0])
    steps = []
    for action in steps_of_model:
        step = run_action(work, run, mode, action)
        sys.stdout.write(json.dumps(step, sort_keys=True) + "\n")
        steps.append(step)
    report_unfitted(work)
    return steps


def report_unfitted(work: Path) -> None:
    """Print the types of a passed training phase that were not fitted, with their reasons.

    The last action of the model already failed the phase when no requested type was fitted (``job_actions.accept_training``), so here at least one type was
    fitted. The line names every other type, because the actions' own output stays inside this runner.
    """
    from scripts.training import job_actions

    parameters = recorded_parameters(work)
    model, hai_types = str(parameters["model"]), [str(hai_type) for hai_type in parameters["hai_types"]]
    fitted, unfitted = job_actions.training_outcome(work / "in" / "fit", hai_types, model)
    if unfitted:
        sys.stdout.write(json.dumps({"outcome": "partly_fitted", "model": model, "fitted": fitted, "unfitted": unfitted}, sort_keys=True) + "\n")


def main(argv: Sequence[str] | None = None) -> int:
    """Run one phase and report each step, or the failure, as one JSON line."""
    parser = argparse.ArgumentParser(prog="python -m scripts.training.aws_run", description=__doc__)
    parser.add_argument("phase", choices=PHASES)
    parser.add_argument("--run", required=True)
    parser.add_argument("--mode", choices=("fixture", "real"), default="real")
    parser.add_argument("--work", type=Path, default=Path("/workspace"))
    values = parser.parse_args(argv)
    if not re.fullmatch(training_contract.RUN_ID, values.run):
        sys.stderr.write(json.dumps({"phase": values.phase, "failure": "refused", "detail": "the run ID has another form"}) + "\n")
        return EXIT_FAILED
    handler = {"prepare": prepare, "extract": extract, "train": train, "promote": promote, "research": research, "canary": canary, "notify": notify}[
        values.phase
    ]
    try:
        startup_check.check_phase("promote" if values.phase == "promote" else "train", values.phase)
    except startup_check.StartupCheckError as refusal:
        sys.stderr.write(json.dumps({"phase": values.phase, "failure": "startup_check", "detail": str(refusal)}, sort_keys=True) + "\n")
        phase_failure.emit_error(values.phase, refusal)
        return EXIT_FAILED
    try:
        task_capacity.apply()
    except task_capacity.CapacityError as refusal:
        sys.stderr.write(json.dumps({"phase": values.phase, "failure": "refused", "detail": str(refusal)}, sort_keys=True) + "\n")
        phase_failure.emit_error(values.phase, refusal)
        return EXIT_FAILED
    try:
        steps = handler(values.work, values.run, values.mode)
    except RunRefused as refusal:
        sys.stderr.write(json.dumps({"phase": values.phase, "failure": "refused", "detail": str(refusal)}, sort_keys=True) + "\n")
        phase_failure.emit_error(values.phase, refusal)
        return EXIT_FAILED
    except StepFailed as failure:
        sys.stderr.write(json.dumps({"phase": values.phase, "step": failure.step, "failure": failure.failure, "detail": failure.detail}, sort_keys=True) + "\n")
        phase_failure.emit_error(values.phase, failure)
        return failure.code
    if values.phase not in ("train", "promote", "research", "canary", "notify"):
        for step in steps:
            sys.stdout.write(json.dumps(step, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    resource_peaks.recorded(main)
