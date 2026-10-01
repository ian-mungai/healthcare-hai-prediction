"""Safely open complete ZIP captures for human inspection without altering source bytes."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

from scripts.acquisition.capture import receipt_validator, validate_receipt
from scripts.acquisition.source_registry import read_json
from scripts.acquisition.transport import CaptureError


def is_zip_container(path: Path) -> bool:
    """Identify download ZIPs while validating and excluding supported Office packages."""
    if path.suffix.lower() == ".zip":
        return True
    with path.open("rb") as handle:
        signature = handle.read(4)
    if signature not in {b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"} and not zipfile.is_zipfile(path):
        return False
    # Office documents use ZIP internally but are data files, not download bundles.
    office_parts = {".xlsx": "xl/workbook.xml", ".xlsm": "xl/workbook.xml", ".docx": "word/document.xml"}
    if path.suffix.lower() in office_parts:
        try:
            with zipfile.ZipFile(path) as archive:
                required = {"[Content_Types].xml", office_parts[path.suffix.lower()]}
                if required <= set(archive.namelist()):
                    return False
        except zipfile.BadZipFile:
            pass
        raise CaptureError("Office document is missing its required ZIP package structure.")
    return True


def reference_kind(name: str) -> str | None:
    """Return a provisional reference category from a member name, without approving applicability."""
    lower = Path(name).name.lower()
    if re.search(r"dictionary|codebook|layout|readme|method|footnote|manifest", lower):
        return "dictionary_candidate" if re.search(r"dictionary|codebook", lower) else "reference_candidate"
    if lower.endswith((".pdf", ".doc", ".docx")):
        return "document_candidate"
    return None


def expand_archive(path: Path, output: Path, expected_sha256: str, max_total_bytes: int = 1024**3, max_members: int = 10000) -> dict:
    """Expand checksum-pinned ZIP members within size and path limits; return their inventory."""
    from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once

    if fingerprint(path)[0] != expected_sha256:
        raise CaptureError("Archive bytes differ from the captured checksum.")
    if max_total_bytes <= 0 or max_members <= 0:
        raise CaptureError("Archive expansion limits must be positive.")
    output.mkdir(parents=True, exist_ok=True)
    records, skipped = [], []
    with zipfile.ZipFile(path) as archive:
        infos = archive.infolist()
        if len(infos) > max_members or sum(info.file_size for info in infos) > max_total_bytes:
            raise CaptureError("Archive member count or declared expanded size exceeds the review limit.")
        names: set[str] = set()
        for info in infos:
            member = PurePosixPath(info.filename)
            safe = member.parts and not member.is_absolute() and ".." not in member.parts and "\\" not in info.filename and ":" not in member.parts[0]
            if not safe or info.filename in names or (info.external_attr >> 16) & 0o170000 == 0o120000 or info.flag_bits & 1:
                raise CaptureError("Archive contains unsafe, duplicate, symbolic-link or encrypted members.")
            names.add(info.filename)
        total = 0
        for info in infos:
            member = PurePosixPath(info.filename)
            if info.is_dir():
                continue
            if "__MACOSX" in member.parts or member.name == ".DS_Store":
                skipped.append({"member": info.filename, "reason": "OS packaging metadata retained inside original ZIP"})
                continue
            target = output / "members" / str(member)
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.is_symlink() or not target.resolve().is_relative_to(output.resolve()):
                raise CaptureError("Expanded path resolves outside its review directory.")
            with tempfile.TemporaryDirectory(dir=target.parent) as temporary:
                staged = Path(temporary) / "member"
                digest, written = hashlib.sha256(), 0
                with archive.open(info) as source, staged.open("xb") as destination:
                    while block := source.read(1024**2):
                        written += len(block)
                        total += len(block)
                        if total > max_total_bytes or written > info.file_size:
                            raise CaptureError("Actual expanded bytes exceed the declared bounded archive size.")
                        digest.update(block)
                        destination.write(block)
                if written != info.file_size:
                    raise CaptureError("Expanded member length differs from the ZIP directory.")
                if target.exists():
                    if fingerprint(target) != (digest.hexdigest(), written):
                        raise CaptureError("Existing extracted member differs; never overwrite review evidence.")
                else:
                    os.link(staged, target)
            records.append(
                {
                    "archive_member": info.filename,
                    "storage_path": target.relative_to(output).as_posix(),
                    "sha256": digest.hexdigest(),
                    "byte_count": written,
                    "crc32": f"{info.CRC:08x}",
                    "reference_kind": reference_kind(info.filename),
                    "original_member_bytes_unchanged": True,
                }
            )
    dictionaries = [item["storage_path"] for item in records if item["reference_kind"] == "dictionary_candidate"]
    manifest = {
        "archive_sha256": expected_sha256,
        "members": records,
        "skipped_members": skipped,
        "expanded_bytes": total,
        "dictionary_candidates": dictionaries,
        "dictionary_status": "bundled_candidates_require_mapping_review" if dictionaries else "not_bundled_or_unidentified",
        "dataset_documentation": [
            {
                "data_member": item["archive_member"],
                "dictionary_candidates": dictionaries,
                "status": "mapping_not_reviewed" if dictionaries else "separate_dictionary_required",
            }
            for item in records
            if item["reference_kind"] is None
        ],
        "review_status": "opened_for_inspection_not_schema_or_clinical_approval",
        "original_zip_unchanged": True,
    }
    write_once(output / "archive_inventory.json", encoded_json(manifest))
    return manifest


def expand_snapshot(receipt_path: Path, validator: Any = None) -> list[Path]:
    """Validate a complete receipt and return inventory paths for its locally expanded archives."""
    receipt = read_json(receipt_path)
    validate_receipt(receipt, validator or receipt_validator(), receipt_path.parent)
    if receipt["snapshot_status"] != "acquired_unvalidated":
        raise CaptureError("Only complete acquired captures may be opened as review datasets.")
    inventories = []
    for artifact in receipt["artifacts"]:
        path = receipt_path.parent / artifact["storage_path"]
        if not is_zip_container(path):
            continue
        output = receipt_path.parent / "expanded" / artifact["sha256"]
        expand_archive(path, output, artifact["sha256"])
        inventories.append(output / "archive_inventory.json")
    return inventories


def storage_entries(receipt_path: Path, receipt: dict, validator: Any, overrides: list[dict]) -> tuple[list[dict], list[dict], set[str]]:
    """Return extracted storage entries, documentation records and locally retained ZIP hashes."""
    inventories = expand_snapshot(receipt_path, validator)
    entries, documents, containers = [], [], set()
    selections = {(item["parent_artifact_id"], item["archive_member"]): item for item in overrides}
    for inventory_path in inventories:
        inventory = read_json(inventory_path)
        parent = next(item for item in receipt["artifacts"] if item["sha256"] == inventory["archive_sha256"])
        containers.add(parent["sha256"])
        documents.append(
            {
                "parent_artifact_id": parent["artifact_id"],
                "dictionary_status": inventory["dictionary_status"],
                "dataset_documentation": inventory["dataset_documentation"],
            }
        )
        for member in inventory["members"]:
            if is_zip_container(inventory_path.parent / member["storage_path"]):
                raise CaptureError("Nested ZIP needs a reviewed recursive extraction plan; do not upload the ZIP as table data.")
            selection = selections.get((parent["artifact_id"], member["archive_member"]))
            relative = (inventory_path.parent / member["storage_path"]).relative_to(receipt_path.parent).as_posix()
            role = "data" if member["reference_kind"] is None else "reference"
            if selection:
                relative, role = selection["storage_path"], selection["role"]
            name = re.sub(r"[^A-Za-z0-9._-]", "_", Path(relative).name)
            if not name or not name[0].isalnum():
                name = "member_" + name
            entries.append(
                {
                    "storage_path": relative,
                    "stored_file_name": name[:200],
                    "role": role,
                    "archive_member": member["archive_member"],
                    "parent_artifact_id": parent["artifact_id"],
                    "parent_sha256": parent["sha256"],
                    "member_sha256": member["sha256"],
                    "original_member_bytes_unchanged": True,
                    "reference_kind": member["reference_kind"],
                    "schema_review_status": "pending",
                }
            )
        entries.append({"storage_path": inventory_path.relative_to(receipt_path.parent).as_posix(), "role": "archive_inventory"})
    return entries, documents, containers


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description="Open a saved ZIP locally and index data/dictionary candidates. No AWS calls or schema approval.")
    parser.add_argument("--receipt", required=True, type=Path)
    args = parser.parse_args()
    try:
        paths = expand_snapshot(args.receipt)
    except (ValueError, OSError, KeyError, zipfile.BadZipFile) as error:
        parser.error(str(error))
    sys.stdout.write(str(json.dumps({"archive_inventories": [str(path) for path in paths], "s3_writes": 0}, indent=2)) + "\n")


if __name__ == "__main__":
    main()
