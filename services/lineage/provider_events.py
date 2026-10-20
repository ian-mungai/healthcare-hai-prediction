"""Project native Airflow events into a private, immutable spool without dataset or arbitrary metadata authority."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import secrets
import stat
import uuid
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

NAMESPACE = "hai-prediction-local"
PRODUCER = "https://github.com/ian-mungai/healthcare-hai-prediction"
SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
PARENT_SCHEMA = "https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json#/$defs/ParentRunFacet"
MAX_BYTES = 16384
STATES = ("START", "RUNNING", "COMPLETE", "FAIL", "ABORT")
LOG = logging.getLogger(__name__)


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def field(value: Any, name: str) -> Any:
    return value.get(name) if isinstance(value, Mapping) else getattr(value, name, None)


def identifier(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:+@=~-]{1,250}", value):
        raise ValueError("provider caller identity is outside the v2 contract")
    return value


def native_uuid(value: Any) -> str:
    if not isinstance(value, str) or str(uuid.UUID(value)) != value:
        raise ValueError("provider run identity is not a canonical UUID")
    return value


def instant(value: Any) -> str:
    if not isinstance(value, str) or len(value) > 40:
        raise ValueError("provider timestamp is not bounded UTC")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise ValueError("provider timestamp must use UTC")
    return parsed.isoformat()


def caller_key(caller: Mapping[str, Any]) -> str:
    """Return the exact task identity shared with a v2 broker request."""
    return canonical([identifier(caller[name]) for name in ("dag_id", "task_id", "dag_run_id")] + [caller["attempt"]])


def event_id(event: Mapping[str, Any]) -> str:
    """Identify one native callback by its canonical run, state and UTC time; identical resends keep that identity."""
    return hashlib.sha256(canonical(["provider", native_uuid(event["run"]["runId"]), event["eventType"], instant(event["eventTime"])]).encode()).hexdigest()


def file_name(envelope: Mapping[str, Any]) -> str:
    """Sort durable spool entries by UTC lifecycle time before applying a bounded batch."""
    event = envelope["event"]
    timestamp = datetime.fromisoformat(instant(event["eventTime"])).strftime("%Y%m%dT%H%M%S_%fZ")
    rank = {"START": 0, "RUNNING": 1}.get(event["eventType"], 2)
    return f"{timestamp}_{rank}_{envelope['id']}.json"


def project(event: Any) -> dict[str, Any]:
    """Keep native run UUIDs and caller fields only; never serialize a native facet or dataset wholesale."""
    run = field(event, "run")
    facets = field(run, "facets") or {}
    task_metadata = field(facets, "airflow")
    metadata = task_metadata or field(facets, "airflowDagRun")
    if metadata is None:
        raise ValueError("provider event has no Airflow caller metadata")
    caller: dict[str, Any] = {
        "dag_id": identifier(field(field(metadata, "dag"), "dag_id")),
        "dag_run_id": identifier(field(field(metadata, "dagRun"), "run_id")),
    }
    parent = None
    if task_metadata is not None:
        caller["task_id"] = identifier(field(field(metadata, "task"), "task_id"))
        instance = field(metadata, "taskInstance")
        attempt = field(instance, "try_number")
        if type(attempt) is not int or not 1 <= attempt <= 99 or field(instance, "map_index") not in (None, -1):
            raise ValueError("provider task cannot match an unmapped v2 attempt")
        caller["attempt"] = attempt
        source_parent = field(facets, "parent")
        source_job = field(source_parent, "job")
        if field(source_job, "namespace") != NAMESPACE or field(source_job, "name") != caller["dag_id"]:
            raise ValueError("provider task parent conflicts with its DAG")
        parent = {
            "_producer": PRODUCER,
            "_schemaURL": PARENT_SCHEMA,
            "run": {"runId": native_uuid(field(field(source_parent, "run"), "runId"))},
            "job": {"namespace": NAMESPACE, "name": caller["dag_id"]},
        }
    name = caller["dag_id"] + (f".{caller['task_id']}" if "task_id" in caller else "")
    job = field(event, "job")
    if field(job, "namespace") != NAMESPACE or field(job, "name") != name:
        raise ValueError("provider job conflicts with its caller metadata")
    state = field(event, "eventType")
    state = field(state, "value") if not isinstance(state, str) else state
    if state not in STATES:
        raise ValueError("provider event has an unsupported state")
    run_id = native_uuid(field(run, "runId"))
    clean: dict[str, Any] = {
        "eventType": state,
        "eventTime": instant(field(event, "eventTime")),
        "run": {"runId": run_id, "facets": {"parent": parent} if parent is not None else {}},
        "job": {"namespace": NAMESPACE, "name": name},
        "inputs": [],
        "outputs": [],
        "producer": PRODUCER,
        "schemaURL": SCHEMA,
    }
    return {"version": 1, "id": event_id(clean), "caller": caller, "event": clean}


def validate(envelope: Any) -> dict[str, Any]:
    """Reject extra fields and reconstruct the exact projected envelope before host ingestion."""
    if not isinstance(envelope, dict) or set(envelope) != {"version", "id", "caller", "event"} or envelope["version"] != 1:
        raise ValueError("provider envelope is not closed version 1")
    caller, event = envelope["caller"], envelope["event"]
    if not isinstance(caller, dict) or set(caller) not in ({"dag_id", "dag_run_id"}, {"dag_id", "dag_run_id", "task_id", "attempt"}):
        raise ValueError("provider caller has unknown or missing fields")
    metadata = {
        "dag": {"dag_id": caller["dag_id"]},
        "dagRun": {"run_id": caller["dag_run_id"]},
        "task": {"task_id": caller.get("task_id")},
        "taskInstance": {"try_number": caller.get("attempt"), "map_index": -1},
    }
    if not isinstance(event, dict) or not isinstance(event.get("run"), dict):
        raise ValueError("provider event is malformed")
    facets = event["run"].get("facets", {})
    native = {**event, "run": {"runId": event["run"].get("runId"), "facets": {"airflow" if "task_id" in caller else "airflowDagRun": metadata, **facets}}}
    expected = project(native)
    if canonical(envelope) != canonical(expected):
        raise ValueError("provider envelope contains unapproved or conflicting metadata")
    return expected


def read_at(directory: int, name: str) -> bytes:
    """Read one bounded regular provider file through its held directory."""
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > MAX_BYTES:
            raise ValueError("provider spool entry is not a bounded regular file")
        data = os.read(descriptor, MAX_BYTES + 1)
        if len(data) > MAX_BYTES:
            raise ValueError("provider spool entry exceeds its size bound")
        return data
    finally:
        os.close(descriptor)


def emit(folder: Path, event: Any) -> bool:
    """Persist only a projected event; report a safe error type without failing the Airflow callback."""
    try:
        envelope = project(event)
        data = (canonical(envelope) + "\n").encode()
        if len(data) > MAX_BYTES:
            raise ValueError("projected provider event exceeds its size bound")
        folder.mkdir(mode=0o700, parents=True, exist_ok=True)
        directory = os.open(folder, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        temporary = ".pending-" + secrets.token_hex(12)
        try:
            descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            name = file_name(envelope)
            try:
                os.link(temporary, name, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
            except FileExistsError:
                if read_at(directory, name) != data:
                    raise ValueError("provider replay conflicts with immutable event bytes") from None
            os.fsync(directory)
        finally:
            try:
                os.unlink(temporary, dir_fd=directory)
                os.fsync(directory)
            finally:
                os.close(directory)
    except (OSError, ValueError, TypeError, AttributeError, OverflowError) as error:
        LOG.warning("lineage provider spool failed (%s)", type(error).__name__)
        return False
    return True
