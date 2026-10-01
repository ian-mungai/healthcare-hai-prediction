"""Dataset-first grouping without treating bundled documentation as reviewed coverage."""

from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urlsplit

from scripts.acquisition.transport import CaptureError

CATEGORIES = ("datasets", "references", "manifests", "audit")
CMS_ALIASES = {"77hc-ibv8": "hai_hospital", "dgck-syfz": "hcahps_hospital", "xubh-q36u": "hospital_general_information"}


def dataset_id(value: str) -> str:
    """Validate and return a bounded, nonreserved dataset folder identifier."""
    if not isinstance(value, str) or not re.fullmatch(r"[a-z0-9][a-z0-9_]{0,119}", value) or value in {*CATEGORIES, "raw", "reference", "data"}:
        raise CaptureError("Dataset folder must be a bounded lowercase identifier with underscores, not a reserved category.")
    return value


def source_folder(source_id: str) -> str:
    """Return the validated folder name derived from the native source ID."""
    return dataset_id(source_id.replace("-", "_").lower())


def cms_members(root: Path, entries: list[dict]) -> dict[str, dict]:
    """Map archive members to unique public CMS datasets using the publisher's manifest."""
    manifests = [entry for entry in entries if Path(entry.get("archive_member", "")).name == "manifest.json"]
    if len(manifests) != 1:
        raise CaptureError("CMS archive requires one publisher manifest before assigning dataset folders.")
    published = json.loads((root / manifests[0]["storage_path"]).read_bytes())
    if not isinstance(published, list) or not published:
        raise CaptureError("CMS publisher dataset list is missing.")
    available = {entry["archive_member"]: entry for entry in entries if "archive_member" in entry}
    parent = Path(manifests[0]["archive_member"]).parent
    mapping: dict[str, dict] = {}
    identities: set[str] = set()
    for record in published:
        native_id = record.get("dataset_id", "")
        if not re.fullmatch(r"[a-z0-9]+-[a-z0-9]+", native_id) or native_id in identities or record.get("private") is not False:
            raise CaptureError("CMS dataset identity is invalid, duplicated or not public.")
        identities.add(native_id)
        folder = dataset_id(CMS_ALIASES.get(native_id, "cms_" + native_id.replace("-", "_")))
        resources = record.get("resources")
        if not isinstance(resources, list) or not resources:
            raise CaptureError("CMS dataset has no published resources.")
        for resource in resources:
            filename = resource.get("filename", "")
            if not filename or Path(filename).name != filename or "\\" in filename or not filename.startswith(native_id + "_"):
                raise CaptureError("CMS resource filename does not match its dataset identity.")
            member = (parent / filename).as_posix()
            if member not in available or member in mapping:
                raise CaptureError("CMS resource is missing or belongs to multiple datasets.")
            entry = available[member]
            actual_size = (root / entry["storage_path"]).stat().st_size
            mapping[member] = {
                "dataset_id": folder,
                "publisher_dataset_id": native_id,
                "publisher_title": record.get("name"),
                "publisher_modified_date": record.get("modified_date"),
                "publisher_byte_count": resource.get("filesize"),
                "actual_byte_count": actual_size,
                "publisher_size_status": "matched" if actual_size == resource.get("filesize") else "conflict_requires_review",
            }
    if any(entry["archive_member"] not in mapping for entry in entries if entry["role"] == "data"):
        raise CaptureError("CMS archive includes unmapped data members; folder assignment is incomplete.")
    return mapping


def group_entries(root: Path, receipt: dict, entries: list[dict], containers: set[str], registry: dict, *, mapped: bool = False) -> dict[str, list[dict]]:
    """Group storage entries by dataset while preserving shared references and metadata holds."""
    source = receipt["source"]["source_record_id"]
    default = source_folder(source)
    if sum(source_folder(item["source_id"]) == default for item in registry["sources"]) != 1:
        raise CaptureError("Source IDs collide after folder normalization.")
    groups: dict[str, list[dict]] = {}
    if mapped:
        for entry in entries:
            if entry["role"] == "data":
                folder = dataset_id(entry["mapped_dataset_id"])
                groups.setdefault(folder, []).append({**entry, "dataset_id": folder})
        if not groups:
            groups[default] = []
        for folder, items in groups.items():
            items.extend({**entry, "dataset_id": folder} for entry in entries if entry["role"] != "data")
        return groups
    archive_entries = [entry for entry in entries if "archive_member" in entry]
    mapping: dict[str, dict] = {}
    has_manifest = any(Path(entry.get("archive_member", "")).name == "manifest.json" for entry in archive_entries)
    if containers and has_manifest and urlsplit(receipt["acquisition"]["requested_url"]).hostname == "data.cms.gov":
        mapping = cms_members(root, archive_entries)
    elif containers:
        data = [entry for entry in archive_entries if entry["role"] == "data"]
        if len(data) > 1:
            raise CaptureError("Multi-table archive requires a verified publisher dataset mapping before upload.")
    for entry in entries:
        member = entry.get("archive_member")
        if member in mapping:
            identity = mapping[member]
            item = {**entry, **identity}
            if identity["publisher_size_status"] != "matched":
                item.update(role="source_metadata_conflict", original_role=entry["role"], hold_reason="publisher_member_size_mismatch")
            groups.setdefault(identity["dataset_id"], []).append(item)
    if not groups:
        groups[default] = []
    shared_roles = {"dictionary", "methodology", "layout", "manifest", "reference", "capture_receipt", "transport_audit", "archive_inventory"}
    for folder, items in groups.items():
        owned = {entry["storage_path"] for entry in items}
        for entry in entries:
            if entry["storage_path"] in owned:
                continue
            if not mapping or entry["role"] in shared_roles:
                item = {**entry, "dataset_id": folder}
                if "archive_member" in entry and entry["role"] in shared_roles:
                    item.update(shared_bundle_reference=True, applicability_status="not_reviewed", dictionary_sections=[])
                items.append(item)
        for entry in items:
            entry.setdefault("dataset_id", folder)
    return groups
