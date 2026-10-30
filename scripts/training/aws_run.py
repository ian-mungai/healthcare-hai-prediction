"""Run one phase of a training run inside an AWS task (project decision 56).

    python -m scripts.training.aws_run prepare|extract|train --run <run ID> [--mode real|fixture] [--work /workspace]

The task definition chains three containers on one shared ``/workspace``. A fetch container has already downloaded ``code.tar.gz`` of the run, checked
``CODE_SHA256`` and unpacked it to ``<work>/code``. Every phase runs from that folder, so the modules it imports are the bytes the record names.

- ``prepare`` (training image): checks the archive hash again, records the code identity in ``snapshot_training.json`` and the validated parameters in
  ``training_parameters.json``, then runs the ``guard`` action.
- ``extract`` (feature store image): runs the extraction against the catalog and the Feast registry of the data bucket.
- ``train`` (training image): runs ``version_extract``, ``fit``, ``mediation``, ``hac_validate``, ``log_run`` and ``promotion_check``.

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

from orchestration import code_digest, mlflow_service, training_contract
from scripts.training import job
from scripts.training.job_failure import EXIT_FAILED
from scripts.training.step_records import verify_done

PHASES = ("prepare", "extract", "train")
TRAIN_STEPS = ("version_extract", "fit", "mediation", "hac_validate", "log_run", "promotion_check")
ARCHIVE = "code.tar.gz"
COMMIT = re.compile(r"[a-f0-9]{40}")
TRACKING_URI = re.compile(rf"http://(?:10\.[0-9]{{1,3}}|172\.(?:1[6-9]|2[0-9]|3[01])|192\.168)\.[0-9]{{1,3}}\.[0-9]{{1,3}}:{mlflow_service.PORT}")
BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
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
    for step in training_contract.PREREQUISITES[action]:
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


def prepare(work: Path, run: str, mode: str) -> list[dict[str, Any]]:
    """Record the code identity and the parameters, then run the guard."""
    parameters = read_parameters(parse_json("TRAINING_PARAMETERS"))
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
    write_once(work / "training_parameters.json", json.dumps(parameters) + "\n")
    write_once(work / "snapshot_training.json", json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
    return [run_action(work, run, mode, "guard")]


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
        {
            "FEAST_PROFILE": "aws",
            "FEAST_NAMESPACE": "features" if mode == "real" else "features_e2e",
            "FEAST_CATALOG_TYPE": "rest",
            "FEAST_REGISTRY_PATH": f"s3://{bucket()}/lakehouse/feast/registry.pb",
            "FEAST_RUN_FOLDER": os.environ["TMPDIR"],
        }
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
    os.environ.setdefault("DVC_REMOTE_URL", f"s3://{bucket()}/dvc/")
    task_environment(work)
    check_prerequisites(work, run, TRAIN_STEPS[0])
    steps = []
    for action in TRAIN_STEPS:
        step = run_action(work, run, mode, action)
        sys.stdout.write(json.dumps(step, sort_keys=True) + "\n")
        steps.append(step)
    return steps


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
    handler = {"prepare": prepare, "extract": extract, "train": train}[values.phase]
    try:
        steps = handler(values.work, values.run, values.mode)
    except RunRefused as refusal:
        sys.stderr.write(json.dumps({"phase": values.phase, "failure": "refused", "detail": str(refusal)}, sort_keys=True) + "\n")
        return EXIT_FAILED
    except StepFailed as failure:
        sys.stderr.write(json.dumps({"phase": values.phase, "step": failure.step, "failure": failure.failure, "detail": failure.detail}, sort_keys=True) + "\n")
        return failure.code
    if values.phase != "train":
        for step in steps:
            sys.stdout.write(json.dumps(step, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
