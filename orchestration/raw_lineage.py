"""Raw-file lineage: the acquisition builder, and the reader of the bronze run record.

A raw file is the dataset ``s3://<bucket>`` / ``<object key>``, with the object version as its version facet (``raw_file``). ``acquisition_events``
names each file a saved acquisition job stored as an output with a ``haiReceipt`` facet from its receipt: the address the publisher served it from
(the resolved URL of the capture, else the requested one), the landing page, the source, the snapshot, the artifact, the role, the SHA-256 and the
size. The publisher address is a facet, not an input dataset, because OpenMetadata has no entity for a web address and would report it as unresolved.

**Nothing in production calls the acquisition builder.** It is reached only by its own replay (``replay_acquisition``, below) and its cases, and no
DAG or job sends its events. It also could not deliver anything: the COMPLETE event has no input, and OpenMetadata 2.0.7
(``OpenLineageMapper.mapRunEvent``) returns before it resolves anything when the inputs or the outputs are empty. The owner decision "Raw-file lineage
route" (Nov 13 2026) sends no acquisition event; the acquisition run and its receipts stay in the immutable S3 records. Sending them as lineage needs
an S3 storage connector for container entities (a recorded follow-up). ``acquisition_events`` reads one job of a saved acquisition batch:
``jobs/<job>/completed_collection_layout.json`` (``scripts/acquisition/batch.py``), the receipt and the reconciliation record
(``scripts/acquisition/s3_store.py``) it names, and the replay rebuilds every completed job's events from the same saved states, with the same bytes
each time:

    python -m orchestration.raw_lineage replay-acquisition --state-root <batch folder>

``loaded_objects`` checks the run record ``lakehouse/bronze_runs/<run>/check.json`` that the bronze check phase writes once, and returns the objects
the run loaded (each with its table, bucket, key, version, SHA-256 and size) and the snapshot of each table. The metadata job writes those files onto
each bronze table.

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

from orchestration.ecs_lineage import EVENT_SCHEMA, JOB_NAMESPACE, PRODUCER, LineageRefused, canonical, timestamp_text, validators, version_facet

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


BRONZE_RECORD = "lakehouse/bronze_runs/{run}/check.json"
BRONZE_RECORD_KEYS = {
    "schema_version",
    "run_id",
    "target",
    "namespaces",
    "tables",
    "report_sha256",
    "metadata_generation",
    "snapshots",
    "objects",
    "skipped_tables",
}
BRONZE_OBJECT_KEYS = {"table", "bucket", "key", "version_id", "sha256", "byte_count"}
BRONZE_NAMESPACES = ["bronze", "bronze_dictionary"]
SHA256 = re.compile(r"[a-f0-9]{64}")


def loaded_objects(record: Any, key: str, run_id: str) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """The loaded objects and the table snapshots of a bronze run record, after every shape rule; a broken record is refused with its key."""
    if isinstance(record, dict) and set(record) == BRONZE_RECORD_KEYS - {"skipped_tables"}:
        raise LineageRefused(
            f"{key}: has the older shape without skipped_tables, which the bronze check phase wrote before it named the tables it skipped; "
            "a record of that shape is not read, so run bronze again to write a new one"
        )
    if not isinstance(record, dict) or set(record) != BRONZE_RECORD_KEYS or record["schema_version"] != 1:
        raise LineageRefused(f"{key}: does not have exactly the keys {sorted(BRONZE_RECORD_KEYS)} of schema version 1")
    if record["run_id"] != run_id:
        raise LineageRefused(f"{key}: belongs to another run ({record['run_id']})")
    if record["target"] != "real" or record["namespaces"] != BRONZE_NAMESPACES:
        raise LineageRefused(f"{key}: is not a real target record of the namespaces {BRONZE_NAMESPACES}")
    skipped = record["skipped_tables"]
    names = (
        [item.get("table") if isinstance(item, dict) and set(item) == {"table", "reason"} and isinstance(item["reason"], str) else None for item in skipped]
        if isinstance(skipped, list)
        else [None]
    )
    if None in names or len(set(names)) != len(names) or set(names) & set(record["tables"]):
        raise LineageRefused(f"{key}: the skipped tables are not a list of distinct tables outside the requested ones, each with a reason")
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
