"""Lineage from acquisition to bronze: the raw files an acquisition run stored and the raw files a bronze run loaded, named the same way on both sides.

A raw file is the dataset ``s3://<bucket>`` / ``<object key>``, with the object version as its version facet (``raw_file``). The acquisition run's events
name each stored file as an output with a ``haiReceipt`` facet from its receipt: the address the publisher served it from (the resolved URL of the capture,
else the requested one), the landing page, the source, the snapshot, the artifact, the role, the SHA-256 and the size. The publisher address is a facet,
not an input dataset, because OpenMetadata has no entity for a web address and would report it as unresolved.

A bronze run's events give each table the run loaded its own run, a child of the step's run: that table's raw files (as the run record lists them) in,
that table out with the snapshot the record names, so OpenMetadata joins each bronze table to exactly the files it was loaded from. A table the run did
not load is claimed by no event. ``bronze_run_events`` reads the run record ``lakehouse/bronze_runs/<run>/check.json``, which the bronze check phase
writes once; its ``objects`` list names every loaded object with its table. The bookkeeping and dictionary tables are not claimed (recorded limit).

``acquisition_events`` reads one job of a saved acquisition batch: ``jobs/<job>/completed_collection_layout.json`` (``scripts/acquisition/batch.py``), the
receipt and the reconciliation record (``scripts/acquisition/s3_store.py``) it names. The acquisition DAG calls it after a job completes; ``replay_acquisition``
rebuilds every completed job's events from the same saved states, and gives the same bytes each time:

    python -m orchestration.raw_lineage replay-acquisition --state-root <batch folder>

Recorded limit: the saved state keeps the capture's retrieval time only, so START and COMPLETE carry that time.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
from collections.abc import Mapping, Sequence
from functools import cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from orchestration.ecs_lineage import (
    EVENT_SCHEMA,
    JOB_NAMESPACE,
    PRODUCER,
    JobLineage,
    LaunchFacts,
    LineageRefused,
    S3Evidence,
    canonical,
    dataset,
    events,
    launch_facet,
    run_uuid,
    timestamp_text,
    validators,
    version_facet,
)

ROOT = Path(__file__).resolve().parents[1]
RECEIPT_FACET_FILE = Path("data_contracts/orchestration/raw_file_receipt_facet.schema.json")
RECEIPT_FACET_SCHEMA = f"{PRODUCER}/blob/main/{RECEIPT_FACET_FILE.as_posix()}"
BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
KEY = re.compile(r"[^\x00-\x20\x7f]{1,1024}")
STATE_FILE = "completed_collection_layout.json"
VERIFIED = "version_get_sha256_and_length_match"
URL = re.compile(r"https?://[^\s]{1,2040}")


def raw_file(bucket: str, key: str) -> dict[str, str]:
    """The dataset of a raw file in S3: namespace ``s3://<bucket>``, name the object key."""
    if not isinstance(bucket, str) or not BUCKET.fullmatch(bucket):
        raise LineageRefused(f"{bucket!r} is not an S3 bucket name")
    if not isinstance(key, str) or not KEY.fullmatch(key) or key.startswith("/") or ".." in key.split("/"):
        raise LineageRefused(f"{key!r} is not a plain S3 object key")
    return {"namespace": f"s3://{bucket}", "name": key}


def versioned_file(stored: Mapping[str, Any], where: str) -> dict[str, Any]:
    version = stored.get("version_id")
    if not isinstance(version, str) or not version or version == "null":
        raise LineageRefused(f"{where}: {stored.get('key')!r} has no object version")
    return {**raw_file(stored.get("bucket", ""), stored.get("key", "")), "facets": version_facet({"id": version})}


def bronze_inputs(objects: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """The raw files a bronze run loaded, each once, in the given order."""
    found: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for stored in objects:
        item = versioned_file(stored, "bronze input")
        if (item["namespace"], item["name"]) in seen:
            raise LineageRefused(f"bronze input: {item['name']} is listed twice")
        seen.add((item["namespace"], item["name"]))
        found.append(item)
    return found


BRONZE_RECORD = "lakehouse/bronze_runs/{run}/check.json"
BRONZE_RECORD_KEYS = {"schema_version", "run_id", "target", "namespaces", "tables", "report_sha256", "metadata_generation", "snapshots", "objects"}
BRONZE_OBJECT_KEYS = {"table", "bucket", "key", "version_id", "sha256", "byte_count"}
BRONZE_NAMESPACES = ["bronze", "bronze_dictionary"]
SHA256 = re.compile(r"[a-f0-9]{64}")


def loaded_objects(record: Any, key: str, run_id: str) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """The loaded objects and the table snapshots of a bronze run record, after every shape rule; a broken record is refused with its key."""
    if not isinstance(record, dict) or set(record) != BRONZE_RECORD_KEYS or record["schema_version"] != 1:
        raise LineageRefused(f"{key}: does not have exactly the keys {sorted(BRONZE_RECORD_KEYS)} of schema version 1")
    if record["run_id"] != run_id:
        raise LineageRefused(f"{key}: belongs to another run ({record['run_id']})")
    if record["target"] != "real" or record["namespaces"] != BRONZE_NAMESPACES:
        raise LineageRefused(f"{key}: is not a real target record of the namespaces {BRONZE_NAMESPACES}")
    tables, snapshots = record["tables"], record["snapshots"]
    if not isinstance(tables, list) or not all(isinstance(name, str) for name in tables) or not isinstance(snapshots, dict) or set(snapshots) != set(tables):
        raise LineageRefused(f"{key}: the tables and their snapshots do not match")
    if not all(type(value) is int and value > 0 for value in snapshots.values()):
        raise LineageRefused(f"{key}: a table snapshot is not a snapshot ID")
    objects = record["objects"]
    if not isinstance(objects, list) or not objects:
        raise LineageRefused(f"{key}: names no loaded object")
    for item in objects:
        if not isinstance(item, dict) or set(item) != BRONZE_OBJECT_KEYS:
            raise LineageRefused(f"{key}: an object does not have exactly the fields {sorted(BRONZE_OBJECT_KEYS)}")
        if item["table"] not in tables:
            raise LineageRefused(f"{key}: an object belongs to {item['table']!r}, not a requested table")
        if not isinstance(item["sha256"], str) or not SHA256.fullmatch(item["sha256"]) or type(item["byte_count"]) is not int or item["byte_count"] < 0:
            raise LineageRefused(f"{key}: object {item['key']!r} has no valid SHA-256 or size")
    return objects, snapshots


PARENT_SCHEMA = "https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json#/$defs/ParentRunFacet"


def bronze_run_events(evidence: S3Evidence, declared: Mapping[str, Any], invocation: Mapping[str, Any], facts: LaunchFacts) -> list[dict[str, Any]]:
    """The events of a bronze run. Each table the run record evidences is its own run, a child of the step's run: only that table's loaded objects in,
    only that table out, with the snapshot the record names. A table the run did not load is claimed by no event. A failed run has one step run that
    claims no file and no table."""
    if facts.outcome != "succeeded":
        return events(JobLineage([], [], launch_facet(invocation, facts, "not_applicable", [])), invocation, facts)
    key = BRONZE_RECORD.format(run=facts.run_id)
    record = evidence.read(key)
    if record is None:
        raise LineageRefused(f"{key}: missing from the evidence of a succeeded job")
    objects, snapshots = loaded_objects(record, key, facts.run_id)
    parent = {
        "_producer": PRODUCER,
        "_schemaURL": PARENT_SCHEMA,
        "run": {"runId": run_uuid(invocation)},
        "job": {"namespace": JOB_NAMESPACE, "name": f"{invocation['dag_id']}.{invocation['task_id']}"},
    }
    found = []
    for table in sorted(snapshots):
        output = dataset("bronze", table)
        if output not in [dict(item) for item in declared["outputs"]]:
            raise LineageRefused(f"{key}: the table {table} is not declared by the bronze_load row")
        try:
            inputs = bronze_inputs([{name: item[name] for name in ("bucket", "key", "version_id")} for item in objects if item["table"] == table])
        except LineageRefused as error:
            raise LineageRefused(f"{key}: {error}") from None
        if not inputs:
            raise LineageRefused(f"{key}: the table {table} has a snapshot but no loaded object")
        lineage = JobLineage(inputs, [{**output, "facets": version_facet({"id": str(snapshots[table])})}], launch_facet(invocation, facts, "complete", []))
        table_invocation = {**invocation, "task_id": f"{invocation['task_id']}.{table}"}
        for event in events(lineage, table_invocation, facts):
            event["run"]["facets"]["parent"] = parent
            found.append(event)
    return found


def read_json(path: Path, where: str) -> Any:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        raise LineageRefused(f"{where}: {path.name} is missing") from None
    except (OSError, ValueError):
        raise LineageRefused(f"{where}: {path.name} is unreadable or not JSON") from None


def publisher_address(receipt: Mapping[str, Any], where: str) -> tuple[str, str]:
    acquisition = receipt.get("acquisition") or {}
    requested, resolved = acquisition.get("requested_url"), acquisition.get("resolved_url")
    address = resolved or requested
    if not isinstance(requested, str) or not URL.fullmatch(requested) or not isinstance(address, str) or not URL.fullmatch(address):
        raise LineageRefused(f"{where}: the receipt names no http or https publisher address")
    return requested, address


def acquisition_events(state_root: Path, job_id: str) -> list[dict[str, Any]]:
    """START and COMPLETE of one completed acquisition job, rebuilt from its saved state; each stored file is an output."""
    where = f"acquisition job {job_id}"
    state_path = state_root / "jobs" / job_id / STATE_FILE
    if not state_path.is_file():
        raise LineageRefused(f"{where}: not completed ({STATE_FILE} is missing)")
    state = read_json(state_path, where)
    receipt = read_json(Path(state["receipt_path"]), where)
    record = read_json(Path(state["reconciliation_path"]), where)
    if record.get("snapshot_id") != receipt.get("snapshot_id"):
        raise LineageRefused(f"{where}: the reconciliation belongs to another snapshot than the receipt")
    requested, address = publisher_address(receipt, where)
    source = receipt.get("source") or {}
    retrieved = timestamp_text(receipt.get("acquisition", {}).get("retrieved_at_utc"), where)
    artifacts = {item.get("artifact_id"): item for item in receipt.get("artifacts", [])}
    outputs = []
    for entry in record.get("objects", []):
        stored = entry.get("object") or {}
        if stored.get("verification") != VERIFIED:
            raise LineageRefused(f"{where}: {stored.get('key')!r} is not verified by version, SHA-256 and size")
        artifact = artifacts.get(entry.get("artifact_id"))
        if (
            artifact is not None
            and not entry.get("member_chain")
            and (artifact.get("sha256"), artifact.get("byte_count")) != (stored.get("sha256"), stored.get("byte_count"))
        ):
            raise LineageRefused(f"{where}: {stored.get('key')!r} differs from its receipt artifact")
        item = versioned_file(stored, where)
        item["facets"]["haiReceipt"] = {
            "_producer": PRODUCER,
            "_schemaURL": RECEIPT_FACET_SCHEMA,
            "sourceId": str(source.get("source_record_id", "")),
            "snapshotId": str(receipt.get("snapshot_id", "")),
            "publisher": source.get("publisher"),
            "landingUrl": source.get("official_landing_url"),
            "requestedUrl": requested,
            "publisherUrl": address,
            "retrievedAt": retrieved,
            "artifactId": entry.get("artifact_id"),
            "role": str(entry.get("role", "")),
            "sha256": stored.get("sha256"),
            "byteCount": stored.get("byte_count"),
        }
        outputs.append(item)
    run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, canonical([JOB_NAMESPACE, "acquisition", job_id, receipt.get("snapshot_id")])))
    job = {"namespace": JOB_NAMESPACE, "name": f"acquisition.{job_id}"}
    found = []
    for event_type in ("START", "COMPLETE"):
        found.append(
            {
                "eventType": event_type,
                "eventTime": retrieved,
                "run": {"runId": run_id, "facets": {}},
                "job": job,
                "inputs": [],
                "outputs": outputs if event_type == "COMPLETE" else [],
                "producer": PRODUCER,
                "schemaURL": EVENT_SCHEMA,
            }
        )
    for event in found:
        check_event(event)
    return found


@cache
def receipt_check() -> Draft202012Validator:
    return Draft202012Validator(json.loads((ROOT / RECEIPT_FACET_FILE).read_text()))


def check_event(event: Mapping[str, Any]) -> None:
    """Refuse an event that is not an OpenLineage 2-0-2 RunEvent or whose ``haiReceipt`` facets break their closed schema."""
    problems = [error.message for error in validators()[0].iter_errors(dict(event))]
    for item in event.get("outputs", []):
        facet = item.get("facets", {}).get("haiReceipt")
        if facet is not None:
            problems += [f"{item['name']} haiReceipt: {error.message}" for error in receipt_check().iter_errors(facet)]
    if problems:
        raise LineageRefused(f"event {event.get('eventType')} is not valid: {problems[:3]}")


def replay_acquisition(state_root: Path) -> list[str]:
    """The events of every completed job of a saved batch as canonical JSON lines, in job order. Nothing is written."""
    jobs = sorted(path.parent.name for path in (state_root / "jobs").glob(f"*/{STATE_FILE}"))
    return [canonical(event) for job in jobs for event in acquisition_events(state_root, job)]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m orchestration.raw_lineage", description="Rebuild acquisition lineage events from saved batch states.")
    sub = parser.add_subparsers(dest="command", required=True)
    replay = sub.add_parser("replay-acquisition", help="print the events of every completed job, one JSON line each")
    replay.add_argument("--state-root", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        lines = replay_acquisition(args.state_root)
    except LineageRefused as error:
        sys.stderr.write(f"replay refused: {error}\n")
        return 1
    sys.stdout.write("".join(line + "\n" for line in lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
