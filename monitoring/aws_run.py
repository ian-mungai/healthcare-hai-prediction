"""Run one phase of the drift task inside an AWS task.

    python -m monitoring.aws_run baseline|current|check|publish --run <run ID> [--work /workspace]

The task definition chains the containers on one shared ``/workspace``. A fetch container has already unpacked the code of the run to ``<work>/code``. Each
phase covers the six HAI types and writes under ``<work>/drift/<run>/``: one folder per type, ``baseline.done.json`` and ``current.done.json`` after those
phases, and ``records/drift_<type>.json`` after ``check``.

- ``baseline`` (training image): for each type, fetches the Production model's baseline from MLflow and DVC. A type with no Production model or no features is
  recorded as unmonitored with its reason. It is not an error, and a run where every type is unmonitored still ends well.
- ``current`` (feature store image): for each monitored type, reads the population's windows after the baseline's newest year from the latest publication.
- ``check`` (drift image): for each monitored type, runs the drift check and writes its record. It reads files only.
- ``publish`` (training image): uploads the records to ``s3://<bucket>/drift/<run>/`` and the marker ``_complete.json`` last. The marker names the monitored
  and unmonitored types and the SHA-256 of every record. The DAG reads only runs that have the marker.

A rerun in the same workspace resumes: ``baseline`` and ``current`` return their recorded result, and ``check`` and ``publish`` repeat safely and write the same
bytes. Exit codes are those of ``monitoring.drift_check``: 2 a refused input (the message names the failure), 3 a package or environment is missing.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib
import json
import os
import re
import sys
import tempfile
from argparse import Namespace
from pathlib import Path
from typing import Any

from monitoring import baseline, current_window, drift_check, drift_config
from monitoring.drift_check import HAI_TYPES, CheckError, deadline
from orchestration.training_contract import RUN_ID
from scripts.training import dvc_baseline

ROOT = Path(__file__).resolve().parents[1]
PHASES = ("baseline", "current", "check", "publish")
BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
PHASE_SECONDS = {"baseline": 3600, "current": 1800, "publish": 300}
MARKER = "_complete.json"
ENVIRONMENT_DEFAULTS = {
    "MLFLOW_DISABLE_TELEMETRY": "true",
    "DO_NOT_TRACK": "true",
    "DVC_NO_ANALYTICS": "1",
    "MLFLOW_HTTP_REQUEST_TIMEOUT": "20",
    "MLFLOW_HTTP_REQUEST_MAX_RETRIES": "0",
}


def run_folder(work: Path, run: str) -> Path:
    return work / "drift" / run


def write_json(path: Path, document: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, name = tempfile.mkstemp(dir=path.parent, prefix=".aws_run_", suffix=".tmp")
    try:
        with os.fdopen(handle, "w") as stream:
            stream.write(json.dumps(document, indent=2, sort_keys=True) + "\n")
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def read_done(folder: Path, phase: str, run: str) -> dict[str, Any] | None:
    """Return the recorded result of a finished phase of this run, or None."""
    path = folder / f"{phase}.done.json"
    if not path.is_file():
        return None
    try:
        done = json.loads(path.read_bytes())
    except ValueError:
        raise CheckError(f"{phase}_done_unreadable", f"{path} is not valid JSON") from None
    if done.get("run_id") != run or done.get("phase") != phase:
        raise CheckError(f"{phase}_done_foreign", f"{path} belongs to another run or phase")
    return dict(done)


def require_baseline(folder: Path, run: str) -> dict[str, Any]:
    done = read_done(folder, "baseline", run)
    if done is None:
        raise CheckError("baseline_missing", f"the baseline phase of run {run} has not finished in {folder}")
    return done


def baseline_phase(
    store: Any, work: Path, run: str, *, hai_types: tuple[str, ...] = HAI_TYPES, project: Path = ROOT, deadlines: dvc_baseline.Deadlines | None = None
) -> dict[str, Any]:
    """Fetch the baseline of every type that has a Production model and record the types that have none."""
    folder = run_folder(work, run)
    if (done := read_done(folder, "baseline", run)) is not None:
        return done
    monitored: list[str] = []
    unmonitored: dict[str, str] = {}
    for hai_type in hai_types:
        try:
            baseline.fetch(store, hai_type, folder / hai_type, project=project, deadlines=deadlines)
        except CheckError as error:
            if error.code != baseline.NOTHING_TO_MONITOR:
                raise
            unmonitored[hai_type] = error.name
        else:
            monitored.append(hai_type)
    done = {"phase": "baseline", "run_id": run, "monitored": monitored, "unmonitored": unmonitored}
    write_json(folder / "baseline.done.json", done)
    return done


def current_phase(root: Path, reader: Any, work: Path, run: str, *, features_run: str = "latest") -> dict[str, Any]:
    """Read the current windows of every monitored type, with the population and newest window year of its baseline."""
    folder = run_folder(work, run)
    if (done := read_done(folder, "current", run)) is not None:
        return done
    types = list(require_baseline(folder, run)["monitored"])
    for hai_type in types:
        manifest = json.loads((folder / hai_type / "reference_manifest.json").read_bytes())
        features = json.loads((folder / hai_type / "features.json").read_bytes())
        current_window.build(
            root,
            reader,
            features_run=features_run,
            population=manifest["population"],
            hai_type=hai_type,
            newest_window_year=manifest["newest_window_year"],
            features=features,
            out=folder / hai_type,
        )
    done = {"phase": "current", "run_id": run, "types": types}
    write_json(folder / "current.done.json", done)
    return done


def check_phase(work: Path, run: str, *, config: Path = drift_config.CONFIG) -> dict[str, Any]:
    """Run the drift check for every monitored type and write its record under ``records/``."""
    folder = run_folder(work, run)
    types = list(require_baseline(folder, run)["monitored"])
    for hai_type in types:
        base = folder / hai_type
        if not (base / "current_manifest.json").is_file():
            raise CheckError("current_missing", f"the current window of {hai_type} has not been read in {base}")
        drift_check.run(
            Namespace(
                reference_matrix=base / "reference.parquet",
                reference_manifest=base / "reference_manifest.json",
                current_matrix=base / "current.parquet",
                current_manifest=base / "current_manifest.json",
                features=base / "features.json",
                run_id=run,
                out=folder / "records",
                config=config,
            )
        )
    return {"phase": "check", "run_id": run, "types": types}


def publish_phase(client: Any, bucket: str, work: Path, run: str) -> dict[str, Any]:
    """Upload the records of the monitored types and then the marker that names them. Nothing is uploaded when a record is missing or foreign."""
    folder = run_folder(work, run)
    base = require_baseline(folder, run)
    bodies: dict[str, bytes] = {}
    for hai_type in base["monitored"]:
        name = f"drift_{hai_type}.json"
        path = folder / "records" / name
        if not path.is_file():
            raise CheckError("record_missing", f"{hai_type} is monitored and has no record at {path}")
        body = path.read_bytes()
        if json.loads(body).get("run_id") != run:
            raise CheckError("record_stale", f"{path} belongs to another run")
        bodies[name] = body
    marker = {
        "run_id": run,
        "monitored": base["monitored"],
        "unmonitored": base["unmonitored"],
        "files": {name: hashlib.sha256(body).hexdigest() for name, body in bodies.items()},
    }
    for name, body in bodies.items():
        client.put_object(Bucket=bucket, Key=f"drift/{run}/{name}", Body=body, ContentType="application/json")
    client.put_object(
        Bucket=bucket, Key=f"drift/{run}/{MARKER}", Body=(json.dumps(marker, indent=2, sort_keys=True) + "\n").encode(), ContentType="application/json"
    )
    return {"phase": "publish", "run_id": run, "files": sorted(bodies)}


def prepare_environment(bucket: str) -> None:
    """Switch telemetry off, fail fast on MLflow, and point DVC at the bucket's dvc/ prefix. A value the caller set stays."""
    for name, value in ENVIRONMENT_DEFAULTS.items():
        os.environ.setdefault(name, value)
    if BUCKET.fullmatch(bucket):
        os.environ.setdefault("DVC_REMOTE_URL", f"s3://{bucket}/dvc/")


def prepare_feature_reader(bucket: str, work: Path) -> None:
    """Set the variables of the AWS feature reader before the current phase opens it, the same ones the training extract sets. Without them the reader takes
    its fixture branch and refuses the real namespace."""
    from scripts.features import training_extract

    if not BUCKET.fullmatch(bucket):
        raise CheckError("dependency_missing", "the current window reader needs the feature image and DATA_BUCKET_NAME for the registry path", 3)
    folder = work / "tmp"
    folder.mkdir(parents=True, exist_ok=True)
    os.environ.update(training_extract.aws_reader_environment(bucket, str(folder)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("phase", choices=PHASES)
    parser.add_argument("--run", required=True)
    parser.add_argument("--work", type=Path, default=Path("/workspace"))
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--bucket", default=os.environ.get("DATA_BUCKET_NAME", ""))
    parser.add_argument("--features-run", default="latest")
    parser.add_argument("--config", type=Path, default=drift_config.CONFIG)
    arguments = parser.parse_args(argv)
    prepare_environment(arguments.bucket)
    try:
        config, _ = drift_config.load(arguments.config)
        if not re.fullmatch(RUN_ID, arguments.run):
            raise CheckError("run_invalid", f"{arguments.run!r} is not a run ID")
        seconds = int(config["deadline_seconds"]) if arguments.phase == "check" else PHASE_SECONDS[arguments.phase]
        with deadline(seconds), contextlib.ExitStack() as stack:
            if arguments.phase == "baseline":
                result = baseline_phase(open_store(), arguments.work, arguments.run)
            elif arguments.phase == "current":
                prepare_feature_reader(arguments.bucket, arguments.work)
                result = current_phase(arguments.root, current_window.open_reader(stack, arguments.root, arguments.features_run), arguments.work, arguments.run)
            elif arguments.phase == "check":
                result = check_phase(arguments.work, arguments.run, config=arguments.config)
            else:
                if not BUCKET.fullmatch(arguments.bucket):
                    raise CheckError("bucket_invalid", "set --bucket or DATA_BUCKET_NAME to the data bucket name")
                result = publish_phase(open_s3(), arguments.bucket, arguments.work, arguments.run)
    except (CheckError, drift_config.ConfigError) as error:
        sys.stderr.write(f"drift task {arguments.phase} failed: {error}\n")
        return error.code if isinstance(error, CheckError) else 2
    sys.stdout.write(json.dumps({"phase": arguments.phase, "run_id": arguments.run, "result": result}, sort_keys=True) + "\n")
    return 0


def open_store() -> Any:
    """The MLflow client for the address in ``MLFLOW_TRACKING_URI``; the login variables stay in the environment."""
    try:
        client = importlib.import_module("mlflow.tracking").MlflowClient
    except ImportError as error:
        raise CheckError("dependency_missing", f"the baseline phase needs the mlflow package: {error}", 3) from None
    uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if not uri:
        raise CheckError("tracking_uri_missing", "set MLFLOW_TRACKING_URI to the MLflow service address")
    return client(tracking_uri=uri, registry_uri=uri)


def open_s3() -> Any:
    try:
        return importlib.import_module("boto3").client("s3", region_name=os.environ.get("AWS_REGION"))
    except ImportError as error:
        raise CheckError("dependency_missing", f"the publish phase needs the boto3 package: {error}", 3) from None


if __name__ == "__main__":
    raise SystemExit(main())
