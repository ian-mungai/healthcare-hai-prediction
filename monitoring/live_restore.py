"""Rebuild the Evidently UI workspace from S3: the completed live runs under ``drift_demo/`` become projects and snapshots in a folder.

``python -m monitoring.live_restore --bucket B --workspace DIR`` lists the run folders of ``s3://B/drift_demo/`` and restores each completed run. A run is
complete when its marker ``_complete.json`` exists, names a record, a snapshot and a project file, and each of those objects hashes to the value the marker
recorded. The record, the snapshot and the project file are fetched and checked; Evidently's HTML report is not needed to rebuild the workspace and is not
fetched. A run that is incomplete, has a marker that cannot be trusted, has a file whose hash differs, or whose snapshot belongs to another run is skipped and
reported with its reason, and nothing of it is written. A run whose snapshot is already in the workspace is left alone, so a second restore changes nothing.
The command prints one JSON line for each run and a summary line, and exits 0 when it listed the runs, whatever it skipped. It exits 1 when S3 cannot be read.
It only reads S3 (the task role holds read and list on ``drift_demo/`` and nothing else) and writes only below ``--workspace``.

Exit codes: 0 done, 1 S3 cannot be read or the deadline passed, 2 an argument is refused.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import re
import signal
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from monitoring import live_workspace

PREFIX = "drift_demo/"
MARKER = "_complete.json"
DEADLINE_SECONDS = 300
BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
RUN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}")
DIGEST = re.compile(r"[0-9a-f]{64}")
FILE = re.compile(r"(?:drift|snapshot|project)_(HAI_[1-6])\.json|report_(HAI_[1-6])\.html")


def error_code_of(error: Exception) -> str:
    response = getattr(error, "response", None)
    return str(response.get("Error", {}).get("Code", "")) if isinstance(response, dict) else ""


def read_object(client: Any, bucket: str, key: str) -> bytes | None:
    """The object's bytes, or None when the key does not exist."""
    try:
        body: bytes = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as error:
        if error_code_of(error) in ("NoSuchKey", "404"):
            return None
        raise
    return body


def run_names(client: Any, bucket: str) -> list[str]:
    """The names of the run folders directly under ``drift_demo/``, in order."""
    names: list[str] = []
    token: str | None = None
    while True:
        arguments = {"Bucket": bucket, "Prefix": PREFIX, "Delimiter": "/", **({"ContinuationToken": token} if token else {})}
        page = client.list_objects_v2(**arguments)
        names += [item["Prefix"][len(PREFIX) : -1] for item in page.get("CommonPrefixes", [])]
        if not page.get("IsTruncated"):
            return sorted(names)
        token = page["NextContinuationToken"]


def skipped(run: str, reason: str) -> dict[str, str]:
    return {"run": run, "status": "skipped", "reason": reason}


def restore_run(client: Any, bucket: str, workspace: Path, run: str) -> dict[str, str]:
    """Restore one run, or skip it with the reason. Nothing is written unless every file of the run is complete and matches its recorded hash."""
    if not RUN.fullmatch(run):
        return skipped(run, "marker_invalid: the run folder name is not a run name")
    stored = read_object(client, bucket, f"{PREFIX}{run}/{MARKER}")
    if stored is None:
        return skipped(run, f"incomplete: no {MARKER}")
    try:
        marker = json.loads(stored)
        files = marker["files"]
    except (ValueError, KeyError, TypeError):
        return skipped(run, "marker_unreadable: the completion marker is not a JSON document with a files table")
    kinds = {name: FILE.fullmatch(name) for name in files} if isinstance(files, dict) else {}
    types = {match[1] or match[2] for match in kinds.values() if match}
    if marker.get("kind") != "drift_live" or marker.get("run_id") != run or not files or not all(kinds.values()) or len(types) != 1:
        return skipped(run, "marker_invalid: the marker is not the marker of this live run")
    hai_type = next(iter(types))
    names = {kind: f"{kind}_{hai_type}.json" for kind in ("drift", "snapshot", "project")}
    if not all(name in files and isinstance(files[name], str) and DIGEST.fullmatch(files[name]) for name in names.values()):
        return skipped(run, "incomplete: the marker names no record, snapshot or project file with a hash")
    bodies: dict[str, bytes] = {}
    for kind, name in names.items():
        body = read_object(client, bucket, f"{PREFIX}{run}/{name}")
        if body is None:
            return skipped(run, f"incomplete: {name} is missing")
        if hashlib.sha256(body).hexdigest() != files[name]:
            return skipped(run, f"hash_mismatch: {name} does not match the hash its marker recorded")
        bodies[kind] = body
    try:
        if json.loads(bodies["snapshot"]).get("metadata", {}).get("run_id") != run:
            return skipped(run, "snapshot_run_mismatch: the snapshot belongs to another run")
        status = live_workspace.place_snapshot(workspace, bodies["project"], bodies["snapshot"])
    except ValueError as error:
        return skipped(run, f"corrupted: {error}")
    return {"run": run, "status": status}


def restore(client: Any, bucket: str, workspace: Path) -> list[dict[str, str]]:
    """Restore every completed run under ``drift_demo/`` into ``workspace`` and return one report for each run."""
    workspace.mkdir(parents=True, exist_ok=True)
    live_workspace.set_aside_incomplete_snapshots(workspace)
    return [restore_run(client, bucket, workspace, run) for run in run_names(client, bucket)]


@contextlib.contextmanager
def deadline(seconds: int) -> Iterator[None]:
    """End the enclosed work with a TimeoutError after ``seconds`` seconds, also when it waits in a blocking read."""

    def expired(_signal: int, _frame: Any) -> None:
        raise TimeoutError(f"the restore ran longer than {seconds} seconds")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bucket", required=True, help="the data bucket whose drift_demo/ prefix holds the completed live runs")
    parser.add_argument("--workspace", type=Path, required=True, help="the Evidently UI workspace folder to rebuild")
    arguments = parser.parse_args(argv)
    if not BUCKET.fullmatch(arguments.bucket):
        sys.stderr.write("live restore failed: bucket_invalid: set --bucket to the data bucket name\n")
        return 2
    try:
        import boto3
        from botocore.config import Config

        client = boto3.client("s3", config=Config(connect_timeout=5, read_timeout=30, retries={"max_attempts": 3}))
        with deadline(DEADLINE_SECONDS):
            reports = restore(client, arguments.bucket, arguments.workspace)
    except ImportError as error:
        sys.stderr.write(f"live restore failed: dependency_missing: the restore needs boto3: {error}\n")
        return 1
    except (OSError, TimeoutError) as error:
        sys.stderr.write(f"live restore failed: {type(error).__name__}: {error}\n")
        return 1
    except Exception as error:
        sys.stderr.write(f"live restore failed: s3_unreadable: {type(error).__name__}: {error_code_of(error) or error}\n")
        return 1
    for report in reports:
        sys.stdout.write(json.dumps(report, sort_keys=True) + "\n")
    counts = {status: sum(item["status"] == status for item in reports) for status in ("restored", "present", "skipped")}
    sys.stdout.write(json.dumps({"summary": counts}, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
