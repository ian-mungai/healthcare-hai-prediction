"""Apply explicit, checksum-bound archive maps while retaining native file bytes."""

from __future__ import annotations

import json
import re
import zipfile
from pathlib import Path

from scripts.acquisition.archive_review import expand_archive, is_zip_container
from scripts.acquisition.dataset_layout import dataset_id
from scripts.acquisition.source_registry import canonical_hash
from scripts.acquisition.transport import CaptureError


def expand_tree(path: Path, output: Path, digest: str, *, max_bytes: int = 1024**3, max_members: int = 10000, max_depth: int = 5) -> dict:
    """Return leaf members of a checksum-pinned archive tree within shared expansion bounds."""
    leaves: dict = {}
    budget = {"bytes": max_bytes, "members": max_members}

    def visit(container: Path, checksum: str, chain: tuple, depth: int) -> None:
        """Collect verified leaf members while consuming shared expansion limits."""
        if depth > max_depth:
            raise CaptureError("Recursive archive depth exceeds its bound.")
        with zipfile.ZipFile(container) as archive:
            count = len(archive.infolist())
        directory = output / canonical_hash(list(chain))
        inventory = expand_archive(container, directory, checksum, budget["bytes"], budget["members"])
        budget["bytes"] -= inventory["expanded_bytes"]
        budget["members"] -= count
        for member in inventory["members"]:
            local = directory / member["storage_path"]
            identity = (*chain, member["archive_member"])
            if is_zip_container(local):
                visit(local, member["sha256"], identity, depth + 1)
            else:
                leaves[identity] = {**member, "local_path": local, "member_chain": list(identity)}

    visit(path, digest, (), 0)
    return leaves


def mapped_storage_entries(receipt_path: Path, receipt: dict, registry: dict, mapping: dict, route: dict) -> tuple[list[dict], set[str]]:
    """Validate a complete reviewed archive map and return storage entries and container hashes."""
    from scripts.acquisition.s3_store import encoded_json, write_once

    root = receipt_path.parent
    source_id = receipt["source"]["source_record_id"]
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    if (
        mapping.get("registry_sha256") != canonical_hash(registry)
        or source_id not in mapping["source_ids"]
        or lineage.get("route_id") not in mapping["route_ids"]
        or receipt["acquisition"]["requested_url"] != mapping["url"]
        or mapping["release_date"] != receipt["release"]["release_date"]
        or mapping["collection_root"] != f"{route['publisher']}/{route['collection']}"
    ):
        raise CaptureError("Archive mapping differs from the source, registry, route, release or collection.")
    artifacts = receipt["artifacts"]
    if len(artifacts) != 1 or (artifacts[0]["sha256"], artifacts[0]["byte_count"]) != (mapping["sha256"], mapping["bytes"]):
        raise CaptureError("Archive mapping must match the complete captured file.")
    parent = artifacts[0]
    leaves = expand_tree(root / parent["storage_path"], root / "mapped_expanded" / parent["sha256"], parent["sha256"])
    selected = {tuple(item["member_chain"]): item for item in mapping["members"]}
    if len(selected) != len(mapping["members"]) or set(selected) != set(leaves):
        raise CaptureError("Archive map must identify every leaf exactly once.")
    entries = []
    for chain, item in selected.items():
        leaf = leaves[chain]
        if (leaf["sha256"], leaf["byte_count"]) != (item["sha256"], item["bytes"]) or item["role"] not in {"data", "reference"}:
            raise CaptureError("Mapped member hash, length or role differs from the selected leaf.")
        folder = dataset_id(item["dataset_id"]) if item["role"] == "data" else None
        kind = item.get("reference_kind", leaf["reference_kind"])
        if kind not in {None, "dictionary_candidate", "reference_candidate", "document_candidate"} or (
            "reference_kind" in item and item["role"] == "data" and kind is not None
        ):
            raise CaptureError("Explicit reference classification is invalid or attached to data.")
        name = re.sub(r"[^A-Za-z0-9._-]", "_", Path(chain[-1]).name)
        if not name or not name[0].isalnum():
            name = "member_" + name
        entries.append(
            {
                "storage_path": leaf["local_path"].relative_to(root).as_posix(),
                "stored_file_name": name[:200],
                "role": item["role"],
                "archive_member": json.dumps(list(chain)),
                "member_chain": list(chain),
                "mapped_dataset_id": folder,
                "parent_artifact_id": parent["artifact_id"],
                "parent_sha256": parent["sha256"],
                "member_sha256": leaf["sha256"],
                "member_crc32": leaf["crc32"],
                "original_member_bytes_unchanged": True,
                "reference_kind": kind,
                "schema_review_status": "pending",
            }
        )
    mapping_path = root / "archive_mapping.json"
    write_once(mapping_path, encoded_json(mapping))
    entries.append({"storage_path": mapping_path.name, "role": "archive_inventory"})
    return entries, {parent["sha256"]}
