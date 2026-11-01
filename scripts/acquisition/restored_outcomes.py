"""Recheck a recorded run 3 outcome from the bytes it claims: bound to its queue unit, read by version and compared again with the stored capture.

This stops a wrong label, a capture of another unit, a missing object and bytes that differ. It cannot tell who made a capture. A writer holding the task role
can create a capture that passes, because that role writes everything under ``raw/``. ``verify_publisher_run`` lets the owner look at the store's own times.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import fingerprint
from scripts.acquisition.source_registry import RegistryError, read_json, require

DATA_ROLES = frozenset({"data", "api_page"})
CLAIMS = frozenset({"exact_match", "changed_needs_review"})


def read_object(client: Any, item: dict, folder: Path) -> Path:
    """One object by exact key and version, kept only if its bytes match the record."""
    require(str(item["key"]).startswith("raw/") or item.get("stored_capture") is True, "Restored outcome names a capture outside the raw root")
    path = folder / f"object_{len(list(folder.iterdir())):06}"
    client.call("s3api", "get-object", ["--key", item["key"], "--version-id", item["version_id"], "--checksum-mode", "ENABLED", str(path)])
    require(fingerprint(path) == (item["sha256"], item["byte_count"]), "Restored outcome names an object whose bytes differ")
    return path


def bind(unit: dict, stored: dict, receipt: dict, manifests: list[dict], item: dict) -> None:
    """The capture must be this unit's: its source, its address or planned item, its own snapshot in every manifest and its receipt listed in each."""
    from scripts.acquisition.redownload import PLANNED

    fresh, earlier = json.loads(receipt["lineage"]["extraction_or_query"]), json.loads(stored["lineage"]["extraction_or_query"])
    require(receipt["source"]["source_record_id"] == unit["source_id"], "Restored outcome names a capture of another source")
    if unit["mode"] in PLANNED:
        key = PLANNED[unit["mode"]][3]
        require(fresh.get(key) == earlier.get(key) and fresh.get(key) is not None, "Restored outcome names a capture of another planned item")
    else:
        require(fresh.get("fresh_of") == unit["snapshot_id"], "Restored outcome names a capture of another unit")
        require(receipt["acquisition"]["requested_url"] == unit["url"], "Restored outcome names a capture of another address")
    for manifest in manifests:
        require(
            manifest["snapshot_id"] == receipt["snapshot_id"] and manifest["source_id"] == unit["source_id"],
            "Restored outcome names a manifest of another capture",
        )
        listed = {entry["object"]["key"]: entry["object"] for entry in manifest["objects"] if entry["storage_path"] == "receipt.json"}
        require(listed.get(item["key"], {}).get("sha256") == item["sha256"], "Restored outcome names a manifest without its receipt")


def same_data(unit: dict, run: Any, stored: dict, receipt: dict, entries: list[dict], local: dict[str, Path], client: Any, folder: Path) -> bool:
    """The comparison again, from the stored capture and the bytes just read. An archive compares member by member, as its container is never stored."""
    from scripts.acquisition.redownload import compare_files, ensure_stored, member_chain, receipt_file, same_content

    old = {item["stored_file_name"]: item for item in stored["artifacts"] if item["role"] in DATA_ROLES}
    new = {item["stored_file_name"]: item for item in receipt["artifacts"] if item["role"] in DATA_ROLES}
    if set(old) != set(new) or not new:
        return False
    members = {member_chain(entry["archive_member"]): entry for entry in entries if "archive_member" in entry and entry["role"] != "archive_inventory"}
    for name, artifact in new.items():
        direct = [entry for entry in entries if entry.get("artifact_id") == artifact["artifact_id"] and "archive_member" not in entry]
        if direct:
            require(direct[0]["object"]["sha256"] == artifact["sha256"], "Restored outcome names a receipt that differs from its stored object")
            if artifact["sha256"] != old[name]["sha256"]:
                path = receipt_file(unit, run).parent / old[name]["storage_path"]
                ensure_stored(path, run)
                if not path.is_file() or compare_files(path, local[direct[0]["object"]["key"]], name, old[name]["sha256"])[0]:
                    return False
            continue
        record = read_json(receipt_file(unit, run).parent / "s3_collections_reconciliation.json")
        earlier = {member_chain(entry["archive_member"]): entry for entry in record["objects"] if "archive_member" in entry}
        require(bool(earlier) and bool(members), "Restored outcome names an archive with no members to compare")
        if set(earlier) != set(members):
            return False
        for chain, entry in members.items():
            before = earlier[chain]["object"]
            if before["sha256"] == entry["object"]["sha256"]:
                continue
            body = read_object(client, before | {"stored_capture": True}, folder).read_bytes()
            if not same_content(body, local[entry["object"]["key"]].read_bytes(), chain[-1])[0]:
                return False
    return True


def recheck(unit: dict, run: Any, record: dict, client: Any) -> dict:
    """Refuse a claim that does not hold on its stored bytes and return its receipt. A failure holds fixed text only."""
    from scripts.acquisition.redownload import stored_receipt

    try:
        evidence = record["stored"]
        require(bool(evidence["manifests"]), "Restored outcome names no stored capture")
        with tempfile.TemporaryDirectory(prefix="outcome_") as scratch:
            folder = Path(scratch)
            receipt = read_json(read_object(client, evidence["receipt"], folder))
            manifests = [read_json(read_object(client, item, folder)) for item in evidence["manifests"]]
            stored = stored_receipt(unit, run)
            bind(unit, stored, receipt, manifests, evidence["receipt"])
            entries = [entry for manifest in manifests for entry in manifest["objects"]]
            local = {
                entry["object"]["key"]: read_object(client, entry["object"], folder) for entry in {item["object"]["key"]: item for item in entries}.values()
            }
            if record["outcome"] == "exact_match":
                require(same_data(unit, run, stored, receipt, entries, local, client, folder), "Restored outcome claims a match its stored bytes do not show")
            return receipt
    except (RegistryError, ValueError, OSError, KeyError, TypeError, IndexError) as error:
        message = (
            str(error)
            if isinstance(error, RegistryError) and str(error).startswith("Restored outcome")
            else "Restored outcome names a stored capture that does not verify"
        )
        raise RegistryError(message) from None
