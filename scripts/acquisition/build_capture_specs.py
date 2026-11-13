"""One-time development step: derive the capture specification from the kept stored receipts, job files and reconciliation files.

A bootstrap capture compares nothing with an old download, but it still needs four facts the stored files hold: the stored data file names a
collector must produce, the expected format and reviewed unsigned redirect of a file unit, and the reviewed archive member map. The result has one
entry per queue unit and no bucket, object key or local path. The stored files are local only, so hosted CI cannot run this script.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from scripts.acquisition.data_paths import current

FORMATS = {".csv": "csv", ".txt": "txt", ".zip": "zip", ".xlsx": "xlsx", ".xls": "xls", ".pdf": "pdf", ".json": "json", ".docx": "docx", ".gz": "gzip"}


def read_json(path: Path) -> dict[str, Any]:
    """Read one JSON file."""
    return json.loads(path.read_text(encoding="utf-8"))


def member_chain(text: str) -> list[str]:
    """A stored record's archive member: a list of nested names in JSON, or one name."""
    return list(json.loads(text)) if text.startswith("[") else [text]


def archive_members(folder: Path) -> list[dict[str, Any]]:
    """The reviewed member map of a stored capture: each member's role (data wins over reference) and dataset ID, sorted by member chain."""
    record = folder / "s3_collections_reconciliation.json"
    if not record.is_file():
        return []
    members: dict[tuple[str, ...], dict[str, Any]] = {}
    for item in read_json(record)["objects"]:
        if "archive_member" in item:
            chain = tuple(member_chain(item["archive_member"]))
            if chain not in members or item["role"] == "data":
                members[chain] = {"member_chain": list(chain), "role": "data" if item["role"] == "data" else "reference", "dataset_id": item["dataset_id"]}
    return [members[chain] for chain in sorted(members)]


def job_file(receipt: Path, base: Path) -> Path | None:
    """The nearest job.json above the receipt, inside the stored folder."""
    for parent in receipt.parents:
        if parent == base.parent:
            break
        if (parent / "job.json").is_file():
            return parent / "job.json"
    return None


def derive_unit(unit: dict[str, Any], base: Path) -> dict[str, Any]:
    """The specification of one unit from its stored files under BASE."""
    receipt = base / current(unit["receipt"])
    if not receipt.is_file():
        raise ValueError(f"Unit {unit['snapshot_id']} has no stored receipt")
    artifacts = read_json(receipt)["artifacts"]
    spec: dict[str, Any] = {"mode": unit["mode"], "stored_data_files": sorted(item["stored_file_name"] for item in artifacts if item["role"] == "data")}
    if unit["mode"] == "file":
        job = job_file(receipt, base)
        plan = read_json(job) if job else {}
        expected = plan.get("plan", {}).get("expected_format") or FORMATS.get(Path(artifacts[0]["stored_file_name"]).suffix.lower())
        redirect = plan.get("candidate", {}).get("reviewed_unsigned_redirect")
        spec |= ({"expected_format": expected} if expected else {}) | ({"reviewed_unsigned_redirect": redirect} if redirect else {})
    members = archive_members(receipt.parent)
    if members:
        spec["archive_members"] = members
    return spec


def derive(units: list[dict[str, Any]], base: Path, queue_sha256: str) -> bytes:
    """The capture specification bytes for UNITS: one entry per snapshot ID, sorted, bound to the tracked queue's digest."""
    specs = {unit["snapshot_id"]: derive_unit(unit, base) for unit in sorted(units, key=lambda item: item["snapshot_id"])}
    document = {"kind": "capture_specs", "queue_sha256": queue_sha256, "units": len(specs), "specs": specs}
    return (json.dumps(document, indent=1, sort_keys=True) + "\n").encode()


def main() -> None:
    """Derive the specification for the tracked queue from the stored files under MIRROR and write it to OUTPUT."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("queue", type=Path)
    parser.add_argument("mirror", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    body = args.queue.read_bytes()
    result = derive(json.loads(body)["queue"], args.mirror, hashlib.sha256(body).hexdigest())
    args.output.write_bytes(result)
    sys.stdout.write(json.dumps({"units": json.loads(result)["units"], "bytes": len(result), "sha256": hashlib.sha256(result).hexdigest()}) + "\n")


if __name__ == "__main__":
    main()
