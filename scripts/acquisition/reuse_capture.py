"""Reuse only checksum-pinned, scope-identical file captures with explicit lineage."""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
from typing import Any

from scripts.acquisition.capture import prepare_capture, validate_receipt
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import canonical_hash, read_json
from scripts.acquisition.storage_controls import reuse_identity
from scripts.acquisition.transport import CaptureError


def plan_identity(plan: dict, registry: dict, validator: Any) -> str:
    """Return the scope-sensitive identity used to decide whether a capture can be reused."""
    _, mode, _, _, url, _, body, expected_hash, receipt = prepare_capture(plan, registry, validator)
    receipt["acquisition"].update(requested_url=url, request_parameters=body or {})
    receipt["lineage"]["extraction_or_query"] = json.dumps({"mode": mode, "scope": plan["scope"], "expected_sha256": expected_hash})
    return reuse_identity(receipt)


def reuse_capture(plan: dict, parent_path: Path, output_root: Path, registry: dict, validator: Any) -> Path:
    """Reuse matching checksum-pinned bytes with explicit lineage and return the new receipt path."""
    parent = read_json(parent_path)
    validate_receipt(parent, validator, parent_path.parent)
    prepared = prepare_capture(plan, registry, validator)
    source, mode, role, _, url, _, _, expected_hash, receipt = prepared
    lineage = json.loads(parent["lineage"]["extraction_or_query"])
    if mode != "file" or parent["snapshot_status"] != "acquired_unvalidated" or not expected_hash:
        raise CaptureError("Local reuse requires a complete checksum-pinned file capture.")
    if lineage["registry_sha256"] != canonical_hash(registry) or reuse_identity(parent) != plan_identity(plan, registry, validator):
        raise CaptureError("Shared capture release, scope or registry differs from the requested capture.")
    if len(parent["artifacts"]) != 1 or parent["artifacts"][0]["sha256"] != expected_hash or parent["artifacts"][0]["role"] != role:
        raise CaptureError("Shared capture checksum or artifact role differs from the requested file.")
    root = output_root / source["source_id"] / receipt["snapshot_id"]
    root.mkdir(parents=True, exist_ok=False)
    paths = [parent_path.parent / item["storage_path"] for item in parent["artifacts"]]
    paths.extend(path for path in (parent_path.parent / "audit").rglob("*") if path.is_file())
    for path in paths:
        if path.is_symlink() or not path.resolve().is_relative_to(parent_path.parent.resolve()):
            raise CaptureError("Shared capture evidence escaped its parent snapshot.")
        target = root / path.relative_to(parent_path.parent)
        target.parent.mkdir(parents=True, exist_ok=True)
        os.link(path, target)
    receipt["artifacts"] = copy.deepcopy(parent["artifacts"])
    receipt["acquisition"] = copy.deepcopy(parent["acquisition"])
    receipt["acquisition"]["requested_url"] = url
    receipt["lineage"]["parent_snapshot_ids"] = [parent["snapshot_id"]]
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {**lineage, "route_id": plan["route_id"], "reuse_note": "No new HTTP request. Original capture retrieval time and transport bytes retained."}
    )
    receipt["quality_profile"]["checks_passed"] = ["checksum_pinned_shared_capture", "equal_release_and_requested_scope"]
    validate_receipt(receipt, validator, root)
    path = root / "receipt.json"
    write_once(path, encoded_json(receipt))
    return path
