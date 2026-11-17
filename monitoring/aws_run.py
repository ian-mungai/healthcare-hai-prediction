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
bytes.

``--live-type HAI_n`` turns the task into the on-demand live check of ``monitoring.live_check`` for that one type. ``baseline`` fetches only that type,
``current`` reads nothing, ``check`` splits the baseline by window year and ``publish`` uploads the record and Evidently's HTML report to
``s3://<bucket>/drift_demo/<run>/``, never to ``drift/<run>/``.

Exit codes are those of ``monitoring.drift_check``: 2 a refused input (the message names the failure), 3 a package or environment is missing.
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

from monitoring import baseline, current_window, drift_check, drift_config, drift_gate, live_check
from monitoring.drift_check import HAI_TYPES, CheckError, deadline
from orchestration.training_contract import MODELS, RUN_ID
from scripts import metrics_push, resource_peaks
from scripts.infrastructure import startup_check
from scripts.lakehouse import task_capacity
from scripts.training import dvc_baseline, run_tracking

ROOT = Path(__file__).resolve().parents[1]
PHASES = ("baseline", "current", "check", "publish")
BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
PHASE_SECONDS = {"baseline": 3600, "current": 1800, "publish": 300}
MARKER = "_complete.json"
# The on-demand live check (``--live-type``) keeps its outputs under their own local folder and its own S3 prefix, never under drift/<run>/ of the gate runs.
LIVE_FOLDER = "drift_live"
LIVE_PREFIX = "drift_demo"
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
    store: Any,
    work: Path,
    run: str,
    *,
    hai_types: tuple[str, ...] = HAI_TYPES,
    project: Path = ROOT,
    deadlines: dvc_baseline.Deadlines | None = None,
    model: str | None = None,
    feature_list_sha: str | None = None,
    record_versions: bool = False,
) -> dict[str, Any]:
    """Fetch the baseline of every type that has a Production model of the model and record the types that have none, with the reason. A live run also
    records the Production run and version of each type it fetched, which bind its run ID to the model it checked."""
    folder = run_folder(work, run)
    if (done := read_done(folder, "baseline", run)) is not None:
        return done
    monitored: list[str] = []
    unmonitored: dict[str, str] = {}
    versions: dict[str, str] = {}
    for hai_type in hai_types:
        try:
            fetched = baseline.fetch(store, hai_type, folder / hai_type, project=project, deadlines=deadlines, model=model, feature_list_sha=feature_list_sha)
        except CheckError as error:
            if error.code != baseline.NOTHING_TO_MONITOR:
                raise
            unmonitored[hai_type] = error.name
        else:
            monitored.append(hai_type)
            versions[hai_type] = f"{fetched.run_id}:{fetched.version}"
    done = {"phase": "baseline", "run_id": run, "monitored": monitored, "unmonitored": unmonitored} | ({} if model is None else {"model": model})
    if record_versions:
        done["versions"] = versions
    write_json(folder / "baseline.done.json", done)
    return done


def current_phase(root: Path, reader: Any, work: Path, run: str, *, features_run: str) -> dict[str, Any]:
    """Read the current windows of every monitored type, with the population and newest window year of its baseline, from the publication ``features_run``
    names (``latest`` for the newest one). The caller states it: a default would let a pinned run fall back to the newest publication."""
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


def push_record(record: dict[str, Any]) -> None:
    """Hand the result of one type's check to the exit push of this job: its status, drift share, drifted count, trigger and current rows."""
    hai_type = record["hai_type"]
    metrics_push.add("hai_drift_status", 1, hai_type=hai_type, status=record["status"])
    if record["drift_share"] is not None:
        metrics_push.add("hai_drift_share", record["drift_share"], hai_type=hai_type)
    metrics_push.add("hai_drift_features_drifted", record["drifted_count"], hai_type=hai_type)
    metrics_push.add("hai_drift_triggered", 1 if record["triggered"] else 0, hai_type=hai_type)
    metrics_push.add("hai_drift_current_rows", record["current"]["rows"], hai_type=hai_type)


def check_phase(work: Path, run: str, *, config: Path = drift_config.CONFIG) -> dict[str, Any]:
    """Run the drift check for every monitored type and write its record under ``records/``."""
    folder = run_folder(work, run)
    types = list(require_baseline(folder, run)["monitored"])
    for hai_type in types:
        base = folder / hai_type
        if not (base / "current_manifest.json").is_file():
            raise CheckError("current_missing", f"the current window of {hai_type} has not been read in {base}")
        written = drift_check.run(
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
        push_record(json.loads(Path(written).read_bytes()))
    return {"phase": "check", "run_id": run, "types": types}


def publish_phase(client: Any, bucket: str, work: Path, run: str) -> dict[str, Any]:
    """Upload the records of the monitored types and then the marker that names them. Nothing is uploaded when a record is missing or foreign."""
    folder = run_folder(work, run)
    base = require_baseline(folder, run)
    bodies: dict[str, bytes] = {}
    records: dict[str, Any] = {}
    for hai_type in base["monitored"]:
        name = f"drift_{hai_type}.json"
        path = folder / "records" / name
        if not path.is_file():
            raise CheckError("record_missing", f"{hai_type} is monitored and has no record at {path}")
        body = path.read_bytes()
        records[hai_type] = json.loads(body)
        if records[hai_type].get("run_id") != run:
            raise CheckError("record_stale", f"{path} belongs to another run")
        bodies[name] = body
    marker = {
        "run_id": run,
        "monitored": base["monitored"],
        "unmonitored": base["unmonitored"],
        "files": {name: hashlib.sha256(body).hexdigest() for name, body in bodies.items()},
    }
    if "model" in base:
        marker["model"] = base["model"]
        marker["gate"] = drift_gate.decide(base["model"], base, records)
    for name, body in bodies.items():
        client.put_object(Bucket=bucket, Key=f"drift/{run}/{name}", Body=body, ContentType="application/json")
    client.put_object(
        Bucket=bucket, Key=f"drift/{run}/{MARKER}", Body=(json.dumps(marker, indent=2, sort_keys=True) + "\n").encode(), ContentType="application/json"
    )
    return {"phase": "publish", "run_id": run, "files": sorted(bodies)}


def live_folder(work: Path, run: str) -> Path:
    return work / LIVE_FOLDER / run


def live_names(hai_type: str) -> list[str]:
    """The files of a live run: the record, Evidently's HTML report, and the snapshot and project files that rebuild the UI workspace from S3."""
    return [f"drift_{hai_type}.json", f"report_{hai_type}.html", f"snapshot_{hai_type}.json", f"project_{hai_type}.json"]


def check_live_phase(work: Path, run: str, hai_type: str, workspace: Path, *, config: Path = drift_config.CONFIG) -> dict[str, Any]:
    """Run the live check on the baseline of ``hai_type``: the baseline matrix split by window year, its record and HTML report under ``drift_live/<run>/`` and
    its snapshot in the Evidently UI workspace."""
    folder = run_folder(work, run)
    base = require_baseline(folder, run)
    if hai_type not in base["monitored"]:
        raise CheckError(
            "live_type_not_monitored", f"{hai_type} has no baseline to check: {base['unmonitored'].get(hai_type, 'the baseline phase did not fetch it')}"
        )
    if "model" not in base:
        raise CheckError("model_missing", f"the baseline phase of run {run} did not gate a model, so the live check cannot name its project")
    live_check.run(
        Namespace(
            model=base["model"],
            matrix=folder / hai_type / "reference.parquet",
            manifest=folder / hai_type / "reference_manifest.json",
            features=folder / hai_type / "features.json",
            run_id=run,
            out=live_folder(work, run),
            workspace=workspace,
            config=config,
            production_version=base.get("versions", {}).get(hai_type),
        ),
        record_sinks=live_check.RECORD_SINKS,
    )
    return {"phase": "check", "run_id": run, "types": [hai_type]}


def error_code_of(error: Exception) -> str:
    """The S3 error code that boto3 carries on a client error, or an empty string."""
    response = getattr(error, "response", None)
    return str(response.get("Error", {}).get("Code", "")) if isinstance(response, dict) else ""


def read_object(client: Any, bucket: str, key: str) -> bytes | None:
    """The object's bytes, or None when the key does not exist (S3 answers NoSuchKey to a caller that may list the prefix)."""
    try:
        body: bytes = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as error:
        if error_code_of(error) in ("NoSuchKey", "404"):
            return None
        raise
    return body


def create_object(client: Any, bucket: str, key: str, body: bytes, content_type: str) -> None:
    """Create the object only when the key is free (If-None-Match). An object that is there with the same bytes is the result of an earlier try and stays; one
    with other bytes is never replaced."""
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type, IfNoneMatch="*")
    except Exception as error:
        if error_code_of(error) not in ("PreconditionFailed", "ConditionalRequestConflict"):
            raise
        if read_object(client, bucket, key) != body:
            raise CheckError("demo_object_conflict", f"s3://{bucket}/{key} exists with other content and is never replaced: use a new run ID") from None


def publish_live_phase(client: Any, bucket: str, work: Path, run: str, hai_type: str) -> dict[str, Any]:
    """Upload the live check's record, HTML report, UI snapshot and project file to ``drift_demo/<run>/`` and then the marker that names them, each created only
    when its key is free.

    A run whose marker exists is complete: the same record is a no-op (its stored HTML keeps matching the marker, whatever the local HTML is) and another
    record is refused as ``demo_run_conflict``. An earlier try that stopped before the marker left objects that the retry finds again: the same bytes complete
    the run, other bytes are refused as ``demo_object_conflict``. The prefix is never ``drift/<run>/``, which the drift DAG reads to start training."""
    base = require_baseline(run_folder(work, run), run)
    bodies: dict[str, bytes] = {}
    for name in live_names(hai_type):
        path = live_folder(work, run) / name
        if not path.is_file():
            raise CheckError("record_missing", f"the live check of {hai_type} has no {name} at {path}")
        bodies[name] = path.read_bytes()
    record_name = live_names(hai_type)[0]
    if json.loads(bodies[record_name]).get("run_id") != run:
        raise CheckError("record_stale", f"the live record of {hai_type} belongs to another run")
    digests = {name: hashlib.sha256(body).hexdigest() for name, body in bodies.items()}
    prefix = f"{LIVE_PREFIX}/{run}/"
    stored = read_object(client, bucket, prefix + MARKER)
    if stored is not None:
        try:
            published = json.loads(stored).get("files", {}).get(record_name)
        except ValueError:
            published = None
        if published != digests[record_name]:
            raise CheckError("demo_run_conflict", f"run {run} is already published with another record: use a new run ID")
        return {"phase": "publish", "run_id": run, "files": list(bodies), "reused": True}
    for name, body in bodies.items():
        create_object(client, bucket, prefix + name, body, "text/html" if name.endswith(".html") else "application/json")
    marker = {"kind": "drift_live", "run_id": run, "hai_type": hai_type, "model": base.get("model"), "files": digests}
    create_object(client, bucket, prefix + MARKER, (json.dumps(marker, indent=2, sort_keys=True) + "\n").encode(), "application/json")
    return {"phase": "publish", "run_id": run, "files": list(bodies), "reused": False}


def model_feature_list_sha(root: Path, model: str | None) -> str | None:
    """The hash of the model's predictor definitions as the training extract records it, or None for a run that gates no model."""
    if model is None:
        return None
    from scripts.features import training_extract

    try:
        schema = json.loads((root / "data_contracts/features/feature_schema.json").read_bytes())
        return training_extract.model_references(root, schema, model)[2]
    except (OSError, ValueError, training_extract.ExtractError) as error:
        raise CheckError("schema_invalid", f"the feature schema of {model} cannot be read: {error}") from None


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
    region = os.environ.get("AWS_REGION", "")
    if not region:
        raise CheckError("dependency_missing", "the current window reader needs AWS_REGION for the PyIceberg catalog", 3)
    folder = work / "tmp"
    folder.mkdir(parents=True, exist_ok=True)
    os.environ.update(training_extract.aws_reader_environment(bucket, str(folder), region=region))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("phase", choices=PHASES)
    parser.add_argument("--run", required=True)
    parser.add_argument("--work", type=Path, default=Path("/workspace"))
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--bucket", default=os.environ.get("DATA_BUCKET_NAME", ""))
    parser.add_argument("--features-run", default="latest")
    parser.add_argument(
        "--model", choices=MODELS, help="gate this model: its Production baseline and feature schema decide, and the marker carries the decision"
    )
    parser.add_argument("--config", type=Path, default=drift_config.CONFIG)
    parser.add_argument("--live-type", choices=("", *HAI_TYPES), default="", help="run the on-demand live check of this one HAI type; empty runs the gate task")
    parser.add_argument("--workspace", type=Path, default=Path("/evidently"), help="the Evidently UI workspace of the live check")
    arguments = parser.parse_args(argv)
    prepare_environment(arguments.bucket)
    try:
        config, _ = drift_config.load(arguments.config)
        if not re.fullmatch(RUN_ID, arguments.run):
            raise CheckError("run_invalid", f"{arguments.run!r} is not a run ID")
        seconds = int(config["deadline_seconds"]) if arguments.phase == "check" else PHASE_SECONDS[arguments.phase]
        startup_check.check_phase("drift", arguments.phase)
        task_capacity.apply()
        with deadline(seconds), contextlib.ExitStack() as stack:
            live = arguments.live_type
            if live and arguments.phase == "baseline" and not arguments.model:
                raise CheckError("model_missing", "a live check needs --model: the model whose Production baseline it checks")
            if arguments.phase == "baseline":
                result = baseline_phase(
                    open_store(),
                    arguments.work,
                    arguments.run,
                    hai_types=(live,) if live else HAI_TYPES,
                    model=arguments.model,
                    feature_list_sha=model_feature_list_sha(arguments.root, arguments.model),
                    record_versions=bool(live),
                )
            elif arguments.phase == "current" and live:
                result = {"phase": "current", "run_id": arguments.run, "types": []}
            elif arguments.phase == "current":
                prepare_feature_reader(arguments.bucket, arguments.work)
                reader = current_window.open_reader(stack, arguments.root, arguments.features_run)
                result = current_phase(arguments.root, reader, arguments.work, arguments.run, features_run=arguments.features_run)
            elif arguments.phase == "check" and live:
                result = check_live_phase(arguments.work, arguments.run, live, arguments.workspace, config=arguments.config)
            elif arguments.phase == "check":
                result = check_phase(arguments.work, arguments.run, config=arguments.config)
            else:
                if not BUCKET.fullmatch(arguments.bucket):
                    raise CheckError("bucket_invalid", "set --bucket or DATA_BUCKET_NAME to the data bucket name")
                if live:
                    result = publish_live_phase(open_s3(), arguments.bucket, arguments.work, arguments.run, live)
                else:
                    result = publish_phase(open_s3(), arguments.bucket, arguments.work, arguments.run)
    except (CheckError, drift_config.ConfigError, task_capacity.CapacityError, startup_check.StartupCheckError) as error:
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
    run_tracking.keep_downloads_on_tracking_server()
    return client(tracking_uri=uri, registry_uri=uri)


def open_s3() -> Any:
    try:
        return importlib.import_module("boto3").client("s3", region_name=os.environ.get("AWS_REGION"))
    except ImportError as error:
        raise CheckError("dependency_missing", f"the publish phase needs the boto3 package: {error}", 3) from None


if __name__ == "__main__":
    resource_peaks.recorded(main)
