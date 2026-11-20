"""One-time development step: add the AHRQ Compendium files to the bootstrap queue as seed-only manual units (owner, Nov 19 2026).

Each file of the locked AHRQ plan becomes one ``approved_manual`` unit of the mode ``ahrq_compendium`` and one specification: its stored file, the tracked
release binding of the held source and where and when the owner saved the file (the browser's birth time, download date and origin). The unit's original
comes from the bootstrap seed only, so its URL is never requested and its host is not a host of the run. The step checks every file's SHA-256 and size
against the plan and its recorded origin against the plan's URL, refuses a bundle that already holds AHRQ units, appends the units after the existing ones,
updates the budgets and records the addition in ``derived_from``. It rewrites the queue lock and the hosts file's queue digest (the host list stays as approved:
the seed-only units add no host). Run ``build_seed_manifest`` and ``check_baseline_run --write-lock`` after it::

    python -m scripts.acquisition.add_ahrq_units --metadata <the metadata record of the owner's files>

The metadata record is a JSON document with a ``files`` list of file name, size, SHA-256, birth time and macOS attributes. It is read once and is not kept.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition.build_bootstrap_queue import budgets_with, queue_lock, rewrite_hosts
from scripts.acquisition.source_registry import canonical_hash

MODE = "ahrq_compendium"
SOURCE_ID = "AHRQ_SYSTEM"
ROUTE = "manual_browser_download"
RULE = "add one seed-only manual unit per file of the locked AHRQ Compendium plan, after the last unit (owner, Nov 19 2026)"
ROOT = Path("config/acquisition/baseline_run")
PLAN = Path("config/acquisition/ahrq_compendium_plan.json")
ORIGIN = "com.apple.metadata:kMDItemWhereFroms"
DOWNLOADED = "com.apple.metadata:kMDItemDownloadedDate"


def encode(document: dict[str, Any]) -> bytes:
    """The form of the queue and the specification: one-space indent, sorted keys and one final newline."""
    return (json.dumps(document, indent=1, sort_keys=True) + "\n").encode()


def instant(value: str) -> datetime:
    """A time as UTC; a time without a zone is taken as UTC, as the macOS download date is."""
    moment = datetime.fromisoformat(value)
    return (moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)).astimezone(UTC)


def snapshot_id(entry: dict[str, Any], birth_time_utc: str) -> str:
    """The unit's snapshot ID: the source, the time the owner saved the file and the first 32 characters of the plan's file ID."""
    return f"{SOURCE_ID}__{instant(birth_time_utc).strftime('%Y%m%dT%H%M%SZ')}__{entry['id'][:32]}"


def saved_record(entry: dict[str, Any], found: dict[str, Any]) -> dict[str, Any]:
    """Where and when the owner saved the file, from the macOS attributes. The record keeps the origin of the plan only, never a referring page."""
    attributes = found.get("attributes", {})
    if entry["origin_url"] not in attributes.get(ORIGIN, []):
        raise ValueError(f"The recorded origin of {entry['file_name']} does not include its planned origin URL")
    record: dict[str, Any] = {
        "origin_url": entry["origin_url"],
        "birth_time_utc": instant(found["birth_time_utc"]).isoformat(),
        "saved_by": "owner, browser",
        "role": entry["role"],
        "vintage": entry["vintage"],
    }
    if attributes.get(DOWNLOADED):
        record["downloaded_at_utc"] = instant(attributes[DOWNLOADED][0].replace(" ", "T")).isoformat()
    return record


def add(queue_bytes: bytes, specs_bytes: bytes, plan: dict[str, Any], metadata: dict[str, Any]) -> tuple[bytes, bytes]:
    """The queue and the capture specification with the AHRQ units added. Refuses a second run, a file that differs from the plan and a missing record."""
    queue, specs = json.loads(queue_bytes), json.loads(specs_bytes)
    if any(unit["mode"] == MODE for unit in queue["queue"]):
        raise ValueError("The queue already holds AHRQ units")
    found = {item["file_name"]: item for item in metadata["files"]}
    units = []
    for entry in plan["files"]:
        item = found.get(entry["file_name"])
        if item is None:
            raise ValueError(f"There is no metadata for {entry['file_name']}")
        if (item["sha256"], item["size"]) != (entry["sha256"], entry["bytes"]):
            raise ValueError(f"The file {entry['file_name']} differs from the plan in SHA-256 or size")
        spec = {
            "mode": MODE,
            "stored_data_files": [entry["file_name"]],
            "release_binding": "access_release",
            "source_id": SOURCE_ID,
            "saved": saved_record(entry, item),
        }
        unit = {
            "bytes": entry["bytes"],
            "disposition": "approved_manual",
            "flags": [],
            "mode": MODE,
            "pages": 1,
            "route": ROUTE,
            "snapshot_id": snapshot_id(entry, item["birth_time_utc"]),
            "source_id": SOURCE_ID,
            "url": entry["origin_url"],
        }
        units.append(unit)
        specs["specs"][unit["snapshot_id"]] = spec
    if len({unit["snapshot_id"] for unit in units}) != len(units) or any(unit["snapshot_id"] in {u["snapshot_id"] for u in queue["queue"]} for unit in units):
        raise ValueError("An AHRQ snapshot ID repeats")
    queue["queue"] = [*queue["queue"], *units]
    queue["budgets"] = budgets_with(queue["budgets"], units)
    queue["entries"] = len(queue["queue"])
    counts: dict[str, int] = {}
    for unit in queue["queue"]:
        counts[unit["disposition"]] = counts.get(unit["disposition"], 0) + 1
    queue["dispositions"] = dict(sorted(counts.items()))
    queue["derived_from"] = {
        **queue["derived_from"],
        "ahrq_units": {
            "rule": RULE,
            "plan_sha256": canonical_hash(plan),
            "added": len(units),
            "added_snapshot_ids_sha256": hashlib.sha256("\n".join(sorted(unit["snapshot_id"] for unit in units)).encode()).hexdigest(),
        },
    }
    new_queue = encode(queue)
    specs["specs"] = dict(sorted(specs["specs"].items()))
    specs["units"] = len(specs["specs"])
    specs["queue_sha256"] = hashlib.sha256(new_queue).hexdigest()
    return new_queue, encode(specs)


def main() -> None:
    """Add the AHRQ units to the tracked bundle in ROOT and rebind the queue lock and the hosts file. Follow it with the seed manifest and the inputs lock."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    plan = json.loads(PLAN.read_text(encoding="utf-8"))
    metadata = json.loads(args.metadata.read_text(encoding="utf-8"))
    queue, specs = add((args.root / "queue.json").read_bytes(), (args.root / "capture_specs.json").read_bytes(), plan, metadata)
    approved = json.loads((args.root / "publisher_hosts_scope.json").read_text(encoding="utf-8"))
    hosts = rewrite_hosts(approved, queue)
    (args.root / "queue.json").write_bytes(queue)
    (args.root / "capture_specs.json").write_bytes(specs)
    (args.root / "queue.lock.json").write_bytes(queue_lock(queue))
    (args.root / "publisher_hosts_scope.json").write_bytes(hosts)
    sys.stdout.write(json.dumps({"queue_sha256": hashlib.sha256(queue).hexdigest(), "units": json.loads(queue)["entries"]}) + "\n")


if __name__ == "__main__":
    main()
