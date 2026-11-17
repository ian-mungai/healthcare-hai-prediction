"""Project declared completion records into OpenLineage events and a durable host outbox."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sqlite3
import stat
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Any

from jsonschema import Draft202012Validator

from orchestration import dbt_lineage
from orchestration.snapshots import SnapshotError, validate_input_record, validate_snapshot_record
from services.lineage import provider_events

ROOT = Path(__file__).resolve().parents[1]
JOB_NAMESPACE = "hai-prediction-local"
PRODUCER = "https://github.com/ian-mungai/healthcare-hai-prediction"
IDENTITY_SCHEMA = PRODUCER + "/blob/main/services/lineage/identity_facet.schema.json"
VERSION_SCHEMA = "https://openlineage.io/spec/facets/1-0-0/DatasetVersionDatasetFacet.json#/$defs/DatasetVersionDatasetFacet"
SNAPSHOT_STATUS_SCHEMA = PRODUCER + "/blob/main/services/lineage/snapshot_status_facet.schema.json"
EVENT_SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
MAX_RECORD_BYTES = 2 * 1024**2
MAX_RECORDS = 10000
REQUEST_KEYS = ("request_version", "template", "run_id", "mode", "attempt", "dag_id", "task_id", "dag_run_id")
SELECTOR_KEY = "input_run_id"
LOG = logging.getLogger(__name__)


class LineageError(ValueError):
    """A completion record or outbox cannot preserve declared lineage."""


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def record_validators() -> tuple[Draft202012Validator, Draft202012Validator]:
    request = json.loads((ROOT / "data_contracts/orchestration/submission_request.schema.json").read_text())
    definitions = json.loads((ROOT / "data_contracts/orchestration/ledger_line.schema.json").read_text())["$defs"]
    envelope = {"$ref": "#/$defs/lineage", "$defs": {key: definitions[key] for key in ("lineage", "dataset")}}
    return Draft202012Validator(request), Draft202012Validator(envelope)


def version_facets(identity: dict[str, str]) -> dict[str, Any]:
    return {"version": {"_producer": PRODUCER, "_schemaURL": VERSION_SCHEMA, "datasetVersion": identity["id"]}}


def timestamp(value: Any) -> str:
    if not isinstance(value, str) or len(value) > 40:
        raise LineageError("record needs a bounded UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise LineageError("record timestamp is invalid") from None
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise LineageError("record timestamp must use UTC")
    return parsed.isoformat()


@dataclass(frozen=True)
class Frame:
    """The validated identity and timing that every event of one completion record shares."""

    request: dict[str, Any]
    run_id: str
    attempt_id: str | None
    job: dict[str, str]
    started: str
    ended: str
    terminal: str


def identity_facet(frame: Frame, event_id: str) -> dict[str, Any]:
    identity = {
        "_producer": PRODUCER,
        "_schemaURL": IDENTITY_SCHEMA,
        "eventId": event_id,
        "runId": frame.request["run_id"],
        "attempt": frame.request["attempt"],
    }
    if frame.attempt_id is not None:
        identity["launcherAttemptId"] = frame.attempt_id
    return identity


def task_events(
    frame: Frame,
    declared: dict[str, Any],
    versions: dict[tuple[str, str], dict[str, str]],
    pins: dict[tuple[str, str], dict[str, str]],
    provider_run: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """The task run's START and terminal events, with pinned input and published output versions as dataset facets."""
    inputs = [
        {**dataset, "facets": version_facets(pins[(dataset["namespace"], dataset["name"])])}
        if (dataset["namespace"], dataset["name"]) in pins
        else dict(dataset)
        for dataset in declared["inputs"]
    ]
    outputs = []
    missing = []
    for dataset in declared["outputs"]:
        identity = versions.get((dataset["namespace"], dataset["name"]))
        if identity is None:
            missing.append(dict(dataset))
            outputs.append(dict(dataset))
        else:
            outputs.append({**dataset, "facets": version_facets(identity)})
    snapshot_state = "not_applicable" if not outputs else "missing" if not versions else "partial" if missing else "complete"
    events = []
    for event_type, instant in (("START", frame.started), (frame.terminal, frame.ended)):
        event_id = hashlib.sha256(canonical([frame.run_id, event_type]).encode()).hexdigest()
        facets: dict[str, Any] = {"haiIdentity": identity_facet(frame, event_id)}
        if provider_run is not None:
            facets["parent"] = provider_run["parent"]
        if event_type != "START":
            facets["haiSnapshotStatus"] = {"_producer": PRODUCER, "_schemaURL": SNAPSHOT_STATUS_SCHEMA, "state": snapshot_state, "missingOutputs": missing}
        events.append(
            {
                "eventType": event_type,
                "eventTime": instant,
                "run": {"runId": frame.run_id, "facets": facets},
                "job": frame.job,
                "inputs": inputs,
                "outputs": outputs if event_type == "COMPLETE" else [],
                "producer": PRODUCER,
                "schemaURL": EVENT_SCHEMA,
            }
        )
    return events


def model_events(frame: Frame, models: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One START and terminal event pair for each captured dbt model, parented to the task run."""
    events = []
    for model in models:
        model_run = str(uuid.uuid5(uuid.UUID(frame.run_id), model["model"]))
        model_terminal = (
            "ABORT" if frame.terminal == "ABORT" or model["status"] == "skipped" else "COMPLETE" if model["status"] in ("success", "pass", "warn") else "FAIL"
        )
        for event_type, instant in (("START", frame.started), (model_terminal, frame.ended)):
            event_id = hashlib.sha256(canonical([model_run, event_type]).encode()).hexdigest()
            parent = {
                "_producer": PRODUCER,
                "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json#/$defs/ParentRunFacet",
                "run": {"runId": frame.run_id},
                "job": frame.job,
            }
            events.append(
                {
                    "eventType": event_type,
                    "eventTime": instant,
                    "run": {"runId": model_run, "facets": {"haiIdentity": identity_facet(frame, event_id), "parent": parent}},
                    "job": {"namespace": JOB_NAMESPACE, "name": f"{frame.job['name']}.{model['model']}"},
                    "inputs": model["inputs"],
                    "outputs": model["outputs"] if event_type == "COMPLETE" and frame.terminal == "COMPLETE" else [],
                    "producer": PRODUCER,
                    "schemaURL": EVENT_SCHEMA,
                }
            )
    return events


def build_events(record: dict[str, Any], provider_run: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Return stable START and terminal events from validated, declared record fields only."""
    request_check, lineage_check = record_validators()
    request = {key: record[key] for key in (*REQUEST_KEYS, SELECTOR_KEY) if key in record}
    if list(request_check.iter_errors(request)) or list(lineage_check.iter_errors(record.get("lineage"))):
        raise LineageError("record has invalid caller identity or declared lineage")
    code = record.get("exit_code")
    if not isinstance(code, int) or isinstance(code, bool):
        raise LineageError("record needs a terminal integer exit code")
    launcher = record.get("launcher", {})
    if not isinstance(launcher, dict):
        raise LineageError("record launcher metadata must be an object")
    if "cancelled" in record and not isinstance(record["cancelled"], bool):
        raise LineageError("record cancellation state must be a boolean")
    attempt_id = launcher.get("id")
    if attempt_id is not None and (
        not isinstance(attempt_id, str) or len(attempt_id) > 100 or not attempt_id.isascii() or not all(c.isalnum() or c in "_-" for c in attempt_id)
    ):
        raise LineageError("record has an invalid launcher attempt identity")
    lines = record.get("ledger")
    if not isinstance(lines, list) or not lines or not isinstance(lines[0], dict) or lines[0].get("event") != "intent":
        raise LineageError("record needs its persisted intent")
    intent = lines[0]
    if any(intent.get(key) != value for key, value in request.items()) or intent.get(SELECTOR_KEY) != request.get(SELECTOR_KEY):
        raise LineageError("record identity conflicts with its intent")
    if intent.get("lineage") != record["lineage"]:
        raise LineageError("record lineage conflicts with its intent")
    started = timestamp(intent.get("at"))
    ended = timestamp(launcher.get("ended_at") or record.get("written_at"))
    if datetime.fromisoformat(ended) < datetime.fromisoformat(started):
        raise LineageError("record ended before its intent")
    run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, canonical([JOB_NAMESPACE, request, attempt_id])))
    if provider_run is not None:
        if provider_run.get("caller") != {key: request[key] for key in ("dag_id", "task_id", "dag_run_id", "attempt")}:
            raise LineageError("provider correlation conflicts with the v2 caller")
        run_id = provider_events.native_uuid(provider_run.get("run_id"))
    job = {"namespace": JOB_NAMESPACE, "name": f"{request['dag_id']}.{request['task_id']}"}
    terminal = "ABORT" if record.get("cancelled") is True else "COMPLETE" if code == 0 else "FAIL"
    versions: dict[tuple[str, str], dict[str, str]] = {}
    if "snapshot_record" in record:
        try:
            snapshot = validate_snapshot_record(record["snapshot_record"], request, record["lineage"]["outputs"], succeeded=terminal == "COMPLETE")
        except SnapshotError as error:
            raise LineageError(str(error)) from None
        versions = {(item["namespace"], item["name"]): item["snapshot"] for item in snapshot["outputs"]}
    pins: dict[tuple[str, str], dict[str, str]] = {}
    if "input_snapshot_record" in record:
        try:
            pinned = validate_input_record(
                record["input_snapshot_record"], request, record["lineage"]["inputs"], succeeded=terminal == "COMPLETE", pinned=intent.get("input_pins", [])
            )
        except SnapshotError as error:
            raise LineageError(str(error)) from None
        pins = {(item["namespace"], item["name"]): item["snapshot"] for item in pinned["inputs"]}
    frame = Frame(request, run_id, attempt_id, job, started, ended, terminal)
    events = task_events(frame, record["lineage"], versions, pins, provider_run)
    if "dbt_lineage" in record:
        if not isinstance(attempt_id, str):
            raise LineageError("dbt lineage requires a completed launcher attempt")
        try:
            dbt = dbt_lineage.validate_record(
                record["dbt_lineage"], request, attempt_id, record["lineage"], intent.get("dbt_nodes", {}), launcher.get("dbt_artifacts")
            )
        except dbt_lineage.DbtLineageError as error:
            raise LineageError(str(error)) from None
        events.extend(model_events(frame, dbt["models"]))
    return events


class Outbox:
    """Persist immutable event bytes under bounded SQLite transactions."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if path.is_symlink():
            raise LineageError("outbox path must not be a symlink")
        self.connection = sqlite3.connect(path, timeout=1)
        self.query_deadline: float | None = None
        os.chmod(path, 0o600)
        self.connection.execute("PRAGMA synchronous=FULL")
        try:
            self._upgrade_events()
        except BaseException:
            self.connection.close()
            raise
        self.connection.execute("CREATE TABLE IF NOT EXISTS sweep_cursors (root TEXT PRIMARY KEY, cursor TEXT)")
        self.connection.execute("CREATE TABLE IF NOT EXISTS provider_runs (caller TEXT PRIMARY KEY, link TEXT NOT NULL)")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS provider_events "
            "(id TEXT PRIMARY KEY, envelope TEXT NOT NULL, seen_cycle INTEGER NOT NULL, flushed INTEGER NOT NULL DEFAULT 0)"
        )

    def _upgrade_events(self) -> None:
        """Create and upgrade the events table under one write lock so concurrent openers never add a column twice.

        An outbox that already has every column needs no lock: a delivery can hold the write lock for seconds, and a
        reader must still open. Otherwise BEGIN IMMEDIATE takes the write lock before the column read; a second opener
        waits for it up to the connection timeout (1 s, far above the few milliseconds the upgrade holds it) and then
        reads the columns again.
        """
        if {"om_state", "om_result"} <= self._event_columns():
            return
        self.connection.execute("BEGIN IMMEDIATE")
        if not self.connection.in_transaction:
            raise LineageError("outbox schema upgrade did not open its transaction")
        try:
            self.connection.execute("CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, payload TEXT NOT NULL, sequence INTEGER NOT NULL)")
            columns = self._event_columns()
            if "om_state" not in columns:
                self.connection.execute("ALTER TABLE events ADD COLUMN om_state INTEGER NOT NULL DEFAULT 0")
            if "om_result" not in columns:
                self.connection.execute("ALTER TABLE events ADD COLUMN om_result TEXT")
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def _event_columns(self) -> set[str]:
        """Return the column names of the events table, empty when the table does not exist yet."""
        return {row[1] for row in self.connection.execute("PRAGMA table_info(events)")}

    def _set_query_deadline(self, deadline: float | None) -> None:
        """Install or clear the VM progress fence for this connection."""
        self.query_deadline = deadline
        if deadline is None:
            self.connection.set_progress_handler(None, 0)
        else:
            self.connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)

    @contextmanager
    def queries_until(self, deadline: float) -> Iterator[None]:
        """Bound SQLite VM work by an absolute deadline and restore any outer budget."""
        previous = self.query_deadline
        effective = min(previous, deadline) if previous is not None else deadline
        if not effective > time.monotonic():
            raise LineageError("outbox query deadline passed")
        self._set_query_deadline(effective)
        try:
            yield
        finally:
            self._set_query_deadline(previous)

    def record_cycle(self) -> int:
        """Return the completed bounded record traversal count used by deferred native callbacks."""
        value = self.cursor("!provider-record-cycle")
        return int(value) if value is not None else 0

    def cursor(self, root: str) -> str | None:
        """Recover a traversal position bound to the opened record directory."""
        row = self.connection.execute("SELECT cursor FROM sweep_cursors WHERE root=?", (root,)).fetchone()
        return row[0] if row is not None else None

    def checkpoint(self, root: str, cursor: str | None) -> None:
        """Advance only after this record's events have committed; crashes may repeat reads."""
        with self.connection:
            self.connection.execute(
                "INSERT INTO sweep_cursors(root,cursor) VALUES (?,?) ON CONFLICT(root) DO UPDATE SET cursor=excluded.cursor", (root, cursor)
            )

    def __enter__(self) -> Outbox:
        return self

    def __exit__(self, _exc_type: type[BaseException] | None, _exc: BaseException | None, _traceback: TracebackType | None) -> None:
        self.connection.close()

    def enqueue(self, events: list[dict[str, Any]]) -> int:
        """Atomically store new event bytes or reject a conflicting replay."""
        inserted = 0
        with self.connection:
            for sequence, event in enumerate(events):
                event_id = event["run"]["facets"]["haiIdentity"]["eventId"]
                inserted += self.enqueue_body(event_id, event, sequence)
        return inserted

    def enqueue_body(self, event_id: str, event: dict[str, Any], sequence: int) -> int:
        payload = canonical(event)
        previous = self.connection.execute("SELECT payload FROM events WHERE id=?", (event_id,)).fetchone()
        if previous is not None:
            if previous[0] != payload:
                raise LineageError("event identity already holds different bytes")
            return 0
        self.connection.execute("INSERT INTO events(id,payload,sequence) VALUES (?,?,?)", (event_id, payload, sequence))
        return 1

    def provider_run(self, request: dict[str, Any]) -> dict[str, Any] | None:
        """Resolve a native task run only from the persisted exact v2 caller fields."""
        row = self.connection.execute("SELECT link FROM provider_runs WHERE caller=?", (provider_events.caller_key(request),)).fetchone()
        return json.loads(row[0]) if row is not None else None


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise LineageError("record contains duplicate JSON keys")
        result[key] = value
    return result


def sweep(records: Path, outbox: Path, *, limit: int = 1000, deadline_seconds: float = 30, require_provider: bool = False) -> dict[str, int]:
    """Collect bounded completion records using directory-bound reads and queue stable events."""
    if not 1 <= limit <= MAX_RECORDS or not 0 < deadline_seconds <= 300:
        raise LineageError("sweep needs bounded record and time limits")
    queued = scanned = legacy_skipped = deferred = 0
    deadline = time.monotonic() + deadline_seconds
    root_fd = os.open(records, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(root_fd)
        root_key = f"{metadata.st_dev}:{metadata.st_ino}"
        with Outbox(outbox) as box, box.queries_until(deadline):
            cursor = box.cursor(root_key)
            resuming = cursor is not None
            for directory, directories, files, directory_fd in os.fwalk(".", dir_fd=root_fd, follow_symlinks=False):
                if time.monotonic() >= deadline:
                    raise LineageError(f"record sweep exceeded its {deadline_seconds:g} s deadline")
                if any(stat.S_ISLNK(os.stat(name, dir_fd=directory_fd, follow_symlinks=False).st_mode) for name in directories):
                    raise LineageError("record directory must not contain symlink directories")
                directories.sort()
                for name in sorted(files):
                    if not name.endswith(".json"):
                        continue
                    if time.monotonic() >= deadline:
                        raise LineageError(f"record sweep exceeded its {deadline_seconds:g} s deadline")
                    position = str(Path(directory) / name)
                    if resuming:
                        if position == cursor:
                            resuming = False
                        continue
                    if scanned >= limit:
                        return {"queued": queued, "scanned": scanned, "legacy_skipped": legacy_skipped, **({"deferred": deferred} if require_provider else {})}
                    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
                    try:
                        metadata = os.fstat(descriptor)
                        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > MAX_RECORD_BYTES:
                            raise LineageError("record must be a regular file of at most 2 MiB")
                        with os.fdopen(descriptor, "rb", closefd=False) as handle:
                            data = handle.read(MAX_RECORD_BYTES + 1)
                        if len(data) > MAX_RECORD_BYTES:
                            raise LineageError("record exceeded its size limit")
                    finally:
                        os.close(descriptor)
                    try:
                        record = json.loads(data, object_pairs_hook=unique_object)
                    except (UnicodeError, ValueError):
                        raise LineageError("completion record is not valid unique-key JSON") from None
                    if not isinstance(record, dict):
                        raise LineageError("completion record must be an object")
                    scanned += 1
                    if "request_version" not in record:
                        legacy_skipped += 1
                    else:
                        events = build_events(record)
                        if require_provider:
                            native = box.provider_run(record)
                            if native is None:
                                deferred += 1
                            else:
                                queued += box.enqueue(build_events(record, native))
                        else:
                            queued += box.enqueue(events)
                    box.checkpoint(root_key, position)
            box.checkpoint(root_key, None)
            if require_provider:
                box.checkpoint("!provider-record-cycle", str(box.record_cycle() + 1))
    finally:
        os.close(root_fd)
    return {"queued": queued, "scanned": scanned, "legacy_skipped": legacy_skipped, **({"deferred": deferred} if require_provider else {})}


def main() -> int:
    """Sweep explicit record and outbox paths."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=Path, required=True)
    parser.add_argument("--outbox", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--deadline-seconds", type=float, default=30)
    args = parser.parse_args()
    try:
        result = sweep(args.records, args.outbox, limit=args.limit, deadline_seconds=args.deadline_seconds)
    except LineageError as error:
        sys.stderr.write(f"lineage: {error}\n")
        return 1
    except (OSError, ValueError, sqlite3.Error) as error:
        sys.stderr.write(f"lineage: collection failed ({type(error).__name__}), inspect local storage and input contracts\n")
        return 1
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
