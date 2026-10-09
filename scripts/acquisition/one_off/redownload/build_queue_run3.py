"""Freeze the run 3 redownload queue offline: the files bronze uses, plus the documents stored after the run 1 freeze.

Run 3 covers only the files bronze uses; the stored publisher dictionaries and reference documents join the queue
(amendment 2). No network or AWS request is made.

Inputs: the run 2 queue (checked against its lock), bronze's copies list exported from ``bronze.stored_copies``
(data/acquisition_planning/run3_scope_20261004/bronze_copies.csv) and capture_inventory_amendment2.json.
An active unit stays active when its capture holds a file bronze loads, or holds the only queued copy of a loaded file
whose loaded copy came from a capture that is not queued. Every other active unit becomes ``not_used_by_bronze``.

Usage (repository root): python3 scripts/acquisition/one_off/redownload/build_queue_run3.py
Writes queue.json and queue.lock.json to data/redownload_checks/20261004/; refuses to overwrite a different queue.
"""

import collections
import csv
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
OUT = ROOT / "data/redownload_checks/20261004"
RUN2 = ROOT / "data/redownload_checks/20261001"
PLAN = ROOT / "data/acquisition_planning/full_redownload_20260929"
COPIES = ROOT / "data/acquisition_planning/run3_scope_20261004/bronze_copies.csv"
AMENDMENT = PLAN / "capture_inventory_amendment2.json"
ACTIVE = ("api", "direct_download", "approved_manual")
# Browser-saved reference documents (store_reference_download) are compared from a new browser download.
MANUAL_MODES = {"reference_download"}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run2_queue() -> list[dict]:
    body = (RUN2 / "queue.json").read_bytes()
    lock = json.loads((RUN2 / "queue.lock.json").read_text())
    if hashlib.sha256(body).hexdigest() != lock["queue_sha256"]:
        sys.exit("The run 2 queue differs from its lock.")
    return json.loads(body)["queue"]


def amendment_entries() -> list[dict]:
    """Build queue entries for the documents stored after the freeze, from their receipts."""
    entries = []
    for candidate in json.loads(AMENDMENT.read_text())["added_candidates"]:
        path = ROOT / candidate["receipt"]
        if sha(path) != candidate["receipt_sha256"]:
            sys.exit(f"Receipt changed since amendment 2: {candidate['receipt']}")
        receipt = json.loads(path.read_text())
        acquisition = receipt["acquisition"]
        mode = json.loads(receipt["lineage"].get("extraction_or_query") or "{}").get("mode")
        if receipt["snapshot_status"] != "acquired_unvalidated" or mode not in {"file", *MANUAL_MODES}:
            sys.exit(f"Amendment capture has no reviewed route: {candidate['snapshot_id']}")
        manual = mode in MANUAL_MODES
        entries.append(
            {
                "snapshot_id": candidate["snapshot_id"],
                "source_id": candidate["source_id"],
                "receipt": candidate["receipt"],
                "receipt_sha256": candidate["receipt_sha256"],
                "url": acquisition.get("requested_url"),
                "mode": mode,
                "bytes": sum(item.get("byte_count") or 0 for item in receipt["artifacts"][:1]),
                "pages": 1,
                "disposition": "approved_manual" if manual else "direct_download",
                "route": "manual_browser_download" if manual else "publisher_url",
                "flags": ["amendment2_stored_after_queue_freeze_20261002"],
            }
        )
    return entries


def main() -> None:
    copies = list(csv.DictReader(COPIES.open()))
    loaded_snapshots = {row["snapshot_id"] for row in copies if row["loaded"] == "true"}
    snapshots_by_sha: dict[str, set[str]] = collections.defaultdict(set)
    for row in copies:
        snapshots_by_sha[row["sha256"]].add(row["snapshot_id"])
    queue = run2_queue()
    added = amendment_entries()
    known = {unit["snapshot_id"] for unit in queue}
    if any(entry["snapshot_id"] in known for entry in added):
        sys.exit("An amendment capture is already in the queue.")
    active = {unit["snapshot_id"] for unit in queue if unit["disposition"] in ACTIVE} | {entry["snapshot_id"] for entry in added}
    # A file loaded from a capture outside the queue is checked through a queued capture holding the same bytes.
    twins = {
        snapshot for row in copies if row["loaded"] == "true" and row["snapshot_id"] not in active for snapshot in snapshots_by_sha[row["sha256"]] & active
    }
    uncovered = sorted({row["snapshot_id"] for row in copies if row["loaded"] == "true" and not snapshots_by_sha[row["sha256"]] & active})
    result = []
    for unit in [*queue, *added]:
        unit = dict(unit, flags=list(unit["flags"]))
        if unit["disposition"] in ACTIVE and unit["snapshot_id"] not in loaded_snapshots | twins:
            reason = "duplicate_files_only" if unit["snapshot_id"] in {row["snapshot_id"] for row in copies} else "bronze_reads_no_file"
            unit.update(disposition="not_used_by_bronze", route=None)
            unit["flags"].append(f"run3_scope:{reason}")
        result.append(unit)
    in_scope = [unit for unit in result if unit["disposition"] in ACTIVE]
    by_route = collections.Counter(unit["route"] for unit in in_scope)
    bls = sum(1 for unit in in_scope if unit["source_id"] == "BLS" and unit["mode"] == "bls_api_v2")
    document = {
        "kind": "bounded_redownload_queue",
        "execution_status": "frozen_offline_no_requests_made",
        "run": 3,
        "model_eligible": False,
        "entries": len(result),
        "dispositions": dict(sorted(collections.Counter(unit["disposition"] for unit in result).items())),
        "loaded_files_no_unit_covers": uncovered,
        "budgets": {
            "acquisition_units": len(in_scope),
            "units_by_route": dict(sorted(by_route.items())),
            "max_primary_attempts": 2 * len(in_scope),
            "pages_recorded": sum(unit["pages"] for unit in in_scope),
            "recorded_bytes": sum(unit["bytes"] for unit in in_scope),
            "byte_cap": 128 * 2**30,
            "bls_calls": bls,
            "bls_rolling_24h_limit": 450,
            "bls_minimum_windows": -(-bls // 450),
        },
        "inputs_sha256": {
            "data/redownload_checks/20261001/queue.json": sha(RUN2 / "queue.json"),
            "data/acquisition_planning/run3_scope_20261004/bronze_copies.csv": sha(COPIES),
            "data/acquisition_planning/full_redownload_20260929/capture_inventory_amendment2.json": sha(AMENDMENT),
        },
        "queue": result,
    }
    body = (json.dumps(document, indent=1, sort_keys=True) + "\n").encode()
    target = OUT / "queue.json"
    if target.exists() and target.read_bytes() != body:
        sys.exit("A different frozen queue already exists; do not overwrite it.")
    target.write_bytes(body)
    (OUT / "queue.lock.json").write_text(json.dumps({"queue_sha256": hashlib.sha256(body).hexdigest(), "entries": len(result)}, indent=1) + "\n")
    sys.stdout.write(json.dumps({key: document[key] for key in ("entries", "dispositions", "budgets", "loaded_files_no_unit_covers")}, indent=1) + "\n")


if __name__ == "__main__":
    main()
