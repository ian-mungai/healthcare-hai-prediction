"""Load the explicit single-copy setting and its checksum-bound immutable bootstrap index."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from scripts.acquisition.source_registry import REPO_ROOT
from scripts.acquisition.transport import CaptureError

POLICY = REPO_ROOT / "config/acquisition/storage_policy.json"


def reference_scope(key: str, checksum: str) -> str:
    """Classify only exact content keys or legacy payload categories, never a manifest name or extension."""
    parts = key.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise CaptureError("Single-copy bootstrap has an invalid scoped payload key.")
    for scope, prefix, legacy in (("objects", "bronze_objects", {"datasets", "references"}), ("evidence_objects", "bronze_evidence", {"audit"})):
        if parts == [prefix, "sha256", checksum] or (len(parts) >= 6 and parts[2] in legacy):
            return scope
    raise CaptureError("Single-copy bootstrap has an unknown payload scope.")


def load_policy(path: Path = POLICY) -> dict[str, Any]:
    """Return a disabled policy or a validated bootstrap index bound to the configured SHA-256."""
    document = json.loads(path.read_text())
    if set(document) != {"single_copy_enabled", "bootstrap_index", "bootstrap_index_sha256"} or type(document["single_copy_enabled"]) is not bool:
        raise CaptureError("Storage policy must declare the single-copy setting and exact bootstrap index binding.")
    if not document["single_copy_enabled"]:
        return {"enabled": False, "objects": {}, "evidence_objects": {}, "bootstrap_sha256": None}
    name, digest = document["bootstrap_index"], document["bootstrap_index_sha256"]
    if not isinstance(name, str) or not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest):
        raise CaptureError("Single-copy storage requires an immutable bootstrap path and SHA-256.")
    raw = Path(name).read_bytes()
    if hashlib.sha256(raw).hexdigest() != digest:
        raise CaptureError("Immutable single-copy bootstrap bytes differ from the approved SHA-256.")
    index = json.loads(raw)
    if set(index) != {"schema_version", "generation", "objects", "evidence_objects"} or index["schema_version"] != 2:
        raise CaptureError("Unsupported immutable single-copy bootstrap schema.")
    if not isinstance(index["generation"], str) or not re.fullmatch(r"[a-f0-9]{64}", index["generation"]):
        raise CaptureError("Single-copy bootstrap requires a complete generation and checksum-to-object map.")
    for scope in ("objects", "evidence_objects"):
        if not isinstance(index[scope], dict):
            raise CaptureError("Single-copy bootstrap requires separate data and evidence checksum maps.")
        for checksum, record in index[scope].items():
            if not re.fullmatch(r"[a-f0-9]{64}", checksum) or not isinstance(record, dict) or record.get("sha256") != checksum:
                raise CaptureError("Single-copy bootstrap contains an invalid checksum reference.")
            if any(not isinstance(record.get(key), str) or not record[key] for key in ("bucket", "key", "version_id")) or record["version_id"] == "null":
                raise CaptureError("Single-copy bootstrap requires exact bucket, key and immutable version identities.")
            if type(record.get("byte_count")) is not int or record["byte_count"] < 0:
                raise CaptureError("Single-copy bootstrap requires an exact nonnegative byte count.")
            if reference_scope(record["key"], checksum) != scope:
                raise CaptureError("Single-copy bootstrap reference crosses its data or evidence scope.")
    return {"enabled": True, "objects": index["objects"], "evidence_objects": index["evidence_objects"], "bootstrap_sha256": digest}
