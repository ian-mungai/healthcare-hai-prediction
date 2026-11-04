"""Recheck a recorded run 3 outcome from the bytes it claims: bound to its queue unit, read by version and compared again with the stored capture.

This stops a wrong label, a capture of another unit, a missing object and bytes that differ. It cannot tell who made a capture. A writer holding the task role
can create a capture that passes, because that role writes everything under ``raw/``. ``verify_publisher_run`` lets the owner look at the store's own times.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import fingerprint
from scripts.acquisition.source_registry import RegistryError, read_json, require
from scripts.acquisition.transport import canonical_url

DATA_ROLES = frozenset({"data", "api_page"})
CLAIMS = frozenset({"exact_match", "changed_needs_review"})
KNOWN_OUTCOMES = CLAIMS | {"blocked", "unavailable"}
CHARGE_KINDS = frozenset({"received_bytes", "injected_received_bytes", "persisted_copy_bytes"})


def read_object(client: Any, item: dict, folder: Path, *, earlier_capture: bool = False) -> Path:
    """One object by exact key and version, kept only if its bytes match the record. Only the stored capture's record may name a key outside the raw root."""
    require(earlier_capture or str(item["key"]).startswith("raw/"), "Restored outcome names a capture outside the raw root")
    path = folder / f"object_{len(list(folder.iterdir())):06}"
    client.call("s3api", "get-object", ["--key", item["key"], "--version-id", item["version_id"], "--checksum-mode", "ENABLED", str(path)])
    require(fingerprint(path) == (item["sha256"], item["byte_count"]), "Restored outcome names an object whose bytes differ")
    return path


def is_mmd(unit: dict) -> bool:
    """Both MMD variants, the API units and the browser exports moved to the API, are captured by the one MMD collector."""
    from scripts.acquisition.redownload import handler_mode

    return handler_mode(unit) == "mmd_api"


def bind(unit: dict, stored: dict, receipt: dict, manifests: list[dict], item: dict, run: Any) -> None:
    """The capture must be this unit's: its source, its address or planned item, its own snapshot in every manifest and its receipt listed in each."""
    from scripts.acquisition.redownload import PLANNED, mmd_selection

    fresh, earlier = json.loads(receipt["lineage"]["extraction_or_query"]), json.loads(stored["lineage"]["extraction_or_query"])
    require(receipt["source"]["source_record_id"] == unit["source_id"], "Restored outcome names a capture of another source")
    if is_mmd(unit):
        # The planned item of an MMD unit is its measure and year, which the stored capture names whichever variant it is.
        require(
            fresh.get("mode") == "mmd_api" and (fresh.get("measure_id"), fresh.get("year")) == mmd_selection(unit, run),
            "Restored outcome names a capture of another planned item",
        )
    elif unit["mode"] in PLANNED:
        key = PLANNED[unit["mode"]][3]
        require(fresh.get(key) == earlier.get(key) and fresh.get(key) is not None, "Restored outcome names a capture of another planned item")
    else:
        require(fresh.get("fresh_of") == unit["snapshot_id"], "Restored outcome names a capture of another unit")
        require(canonical_url(receipt["acquisition"]["requested_url"]) == canonical_url(unit["url"]), "Restored outcome names a capture of another address")
    for manifest in manifests:
        require(
            manifest["snapshot_id"] == receipt["snapshot_id"] and manifest["source_id"] == unit["source_id"],
            "Restored outcome names a manifest of another capture",
        )
        listed = {entry["object"]["key"]: entry["object"] for entry in manifest["objects"] if entry["storage_path"] == "receipt.json"}
        require(listed.get(item["key"], {}).get("sha256") == item["sha256"], "Restored outcome names a manifest without its receipt")


def data_artifacts(document: dict) -> dict[str, dict]:
    return {item["stored_file_name"]: item for item in document["artifacts"] if item["role"] in DATA_ROLES}


def compared(unit: dict, document: dict) -> dict[str, dict]:
    """The artifacts that decide a unit's outcome. An MMD capture is decided by its one derived file, whatever the stored export is called."""
    if is_mmd(unit):
        derived = [item for item in document["artifacts"] if item["role"] == "data"]
        return {"data": derived[0]} if len(derived) == 1 else {}
    return data_artifacts(document)


def members_of(entries: list[dict]) -> dict[tuple[str, ...], dict]:
    from scripts.acquisition.redownload import member_chain

    return {member_chain(entry["archive_member"]): entry for entry in entries if "archive_member" in entry and entry["role"] != "archive_inventory"}


def stored_members(unit: dict, run: Any) -> dict[tuple[str, ...], dict]:
    """The members the stored capture's own record names. A capture stored as one file has none."""
    from scripts.acquisition.redownload import receipt_file

    path = receipt_file(unit, run).parent / "s3_collections_reconciliation.json"
    return members_of(read_json(path)["objects"]) if path.is_file() else {}


def direct_entry(artifact: dict, entries: list[dict]) -> dict | None:
    return next((entry for entry in entries if entry.get("artifact_id") == artifact["artifact_id"] and "archive_member" not in entry), None)


def require_coverage(unit: dict, run: Any, stored: dict, receipt: dict, entries: list[dict]) -> None:
    """Whatever its label says, a capture must hold its data: every data artifact of the receipt has its object, or its archive has the reviewed members."""
    from scripts.acquisition.redownload import PLANNED

    fresh = data_artifacts(receipt)
    require(bool(fresh), "Restored outcome names a capture with no data")
    if unit["mode"] not in PLANNED and not is_mmd(unit):
        require(set(fresh) == set(data_artifacts(stored)), "Restored outcome names a capture with other data files than its unit")
    members = members_of(entries)
    for artifact in fresh.values():
        direct = direct_entry(artifact, entries)
        if direct is not None:
            require(direct["object"]["sha256"] == artifact["sha256"], "Restored outcome names a receipt that differs from its stored object")
        else:
            require(
                any(entry.get("parent_artifact_id") == artifact["artifact_id"] for entry in members.values()),
                "Restored outcome names a capture without its data",
            )
    reviewed = stored_members(unit, run)
    require(not reviewed or set(reviewed) == set(members), "Restored outcome names an archive whose members differ from the reviewed ones")


def same_data(unit: dict, run: Any, stored: dict, receipt: dict, entries: list[dict], local: dict[str, Path], client: Any, folder: Path) -> bool:
    """The comparison again, from the stored capture and the bytes just read. An archive compares member by member, as its container is never stored."""
    from scripts.acquisition.redownload import compare_files, ensure_stored, receipt_file, same_content

    old, new = compared(unit, stored), compared(unit, receipt)
    if set(old) != set(new):
        return False
    for name, artifact in new.items():
        direct = direct_entry(artifact, entries)
        if direct is not None:
            if artifact["sha256"] != old[name]["sha256"]:
                path = receipt_file(unit, run).parent / old[name]["storage_path"]
                ensure_stored(path, run)
                if not path.is_file() or compare_files(path, local[direct["object"]["key"]], artifact["stored_file_name"], old[name]["sha256"])[0]:
                    return False
            continue
        reviewed = stored_members(unit, run)
        if not reviewed and artifact["sha256"] != old[name]["sha256"]:
            return False
        for chain, entry in members_of(entries).items():
            before = reviewed[chain]["object"] if reviewed else entry["object"]
            if before["sha256"] == entry["object"]["sha256"]:
                continue
            body = read_object(client, before, folder, earlier_capture=True).read_bytes()
            if not same_content(body, local[entry["object"]["key"]].read_bytes(), chain[-1])[0]:
                return False
    return True


def container_changed(stored: dict, receipt: dict, entries: list[dict]) -> bool:
    """A rebuilt archive may differ only in what its stored members do not show, such as a comment, so a changed label on it cannot be disproved from them."""
    old = data_artifacts(stored)
    return any(direct_entry(item, entries) is None and item["sha256"] != old.get(name, {}).get("sha256") for name, item in data_artifacts(receipt).items())


def active_units(run: Any) -> dict[str, dict]:
    from scripts.acquisition.redownload import ACTIVE, accepted

    return {unit["snapshot_id"]: unit for unit in accepted(run).units() if unit["disposition"] in ACTIVE}


def ledger_body(body: bytes) -> dict:
    document = json.loads(body)
    if not isinstance(document, dict):
        raise ValueError("Not a record")
    return document


def is_count(value: Any) -> bool:
    return type(value) is int and value >= 0


def check_ledgers(files: Mapping[str, bytes], outcomes: Mapping[str, dict], units: Mapping[str, dict]) -> None:
    """A restored ledger can only tighten a limit. Numbered entries run without a gap, a settlement never exceeds its charge, every entry belongs to a queued
    unit, the charges cover what the outcomes received and every claimed unit was charged. A missing, shrunken or foreign entry could give spent budget back."""
    try:
        charges: dict[int, dict] = {}
        settled: dict[int, Any] = {}
        attempts: dict[tuple[str, str], list[int]] = {}
        for name, body in files.items():
            parts = name.split("/")
            record = ledger_body(body)
            if parts[0] == ".budgets" and parts[1].endswith(".settled.json"):
                settled[int(parts[1].removesuffix(".settled.json"))] = record["bytes"]
            elif parts[0] == ".budgets":
                charges[int(parts[1].removesuffix(".json"))] = record
            else:
                attempts.setdefault((parts[1], parts[2]), []).append(int(parts[3].removesuffix(".json")))
                datetime.fromisoformat(record["reserved_at_utc"])
                require(parts[1] in units, "Restored ledger names a unit outside the queue")
        require(sorted(charges) == list(range(1, len(charges) + 1)), "Restored ledger has a gap in its charges")
        require(all(count in charges for count in settled), "Restored ledger settles a charge it does not hold")
        for number, record in charges.items():
            usable = is_count(record["bytes"]) and record["kind"] in CHARGE_KINDS and record["unit"] in units
            require(usable, "Restored ledger holds a charge it cannot count")
            spent = settled.get(number, record["bytes"])
            require(is_count(spent) and spent <= record["bytes"], "Restored ledger settles more than it charged")
        require(
            all(sorted(numbers) == list(range(1, len(numbers) + 1)) and len(numbers) <= 2 for numbers in attempts.values()),
            "Restored ledger has wrong attempts",
        )
        total = sum(settled.get(number, record["bytes"]) for number, record in charges.items())
        received = sum(record.get("fresh_bytes", 0) for record in outcomes.values() if is_count(record.get("fresh_bytes", 0)))
        require(total >= received, "Restored ledger charged less than the outcomes received")
        charged_units = {record["unit"] for record in charges.values()}
        claims = [identity for identity, record in outcomes.items() if record.get("outcome") in CLAIMS]
        require(all(identity in charged_units for identity in claims), "Restored ledger has no charge for a claimed unit")
    except RegistryError:
        raise
    except (ValueError, KeyError, TypeError, IndexError):
        raise RegistryError("Restored ledger is not usable") from None


def check_record(identity: str, record: dict, units: dict[str, dict], run: Any, client: Any) -> dict | None:
    """The one check of a saved outcome, for the task's restore and the owner's verifier. It returns the receipt of a claim."""
    unit = units.get(identity)
    if not isinstance(record, dict) or unit is None:
        raise RegistryError("Restored outcome does not belong to this queue")
    named = (record.get("snapshot_id"), record.get("source_id"), record.get("mode"), record.get("route"))
    require(named == (identity, unit["source_id"], unit["mode"], unit["route"]), "Restored outcome does not belong to this queue")
    require(record.get("outcome") in KNOWN_OUTCOMES, "Restored outcome has an unknown label")
    return recheck(unit, run, record, client) if record["outcome"] in CLAIMS else None


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
            bind(unit, stored, receipt, manifests, evidence["receipt"], run)
            entries = [entry for manifest in manifests for entry in manifest["objects"]]
            local = {
                entry["object"]["key"]: read_object(client, entry["object"], folder) for entry in {item["object"]["key"]: item for item in entries}.values()
            }
            require_coverage(unit, run, stored, receipt, entries)
            same = same_data(unit, run, stored, receipt, entries, local, client, folder)
            if record["outcome"] == "exact_match":
                require(same, "Restored outcome claims a match its stored bytes do not show")
            else:
                require(not same or container_changed(stored, receipt, entries), "Restored outcome claims a change its stored bytes do not show")
            return receipt
    except (RegistryError, ValueError, OSError, KeyError, TypeError, IndexError) as error:
        message = (
            str(error)
            if isinstance(error, RegistryError) and str(error).startswith("Restored outcome")
            else "Restored outcome names a stored capture that does not verify"
        )
        raise RegistryError(message) from None
