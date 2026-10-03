"""Frozen scope and checks for publisher reference documents a person saved in a browser.

Some publishers refuse scripted requests for their documentation (BLS `la.txt`, CDC SVI documentation,
CDC WONDER help). A person saves each file in a browser; a locked plan binds each file to its source,
its exact origin URL, its SHA-256 and size. Only reference roles are accepted, never data. A held source
needs a release binding (terms or access release), as in the history route; storage rechecks it.
"""

import re
from pathlib import Path

from scripts.acquisition import hud_xlsx_contract as download_metadata
from scripts.acquisition.bls_api_contract import code_hashes, digest
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.history_routes import lineage_bindings
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require, require_collection_scope
from scripts.acquisition.transport import CaptureError

PLAN_PATH = REPO_ROOT / "config/acquisition/reference_downloads_plan.json"
VERSIONS_PATH = REPO_ROOT / "config/acquisition/reference_download_code_versions.json"
ROLES = {"dictionary", "methodology", "layout"}
REQUIRED = {"source_id", "role", "title", "publisher", "origin_url", "file_name", "media_type"}
OPTIONAL = {"release_binding"}
MAX_BYTES = 64 * 1024**2


def require_code(hashes: dict | None = None) -> dict:
    """Accept only an explicitly reviewed implementation version."""
    actual = code_hashes() if hashes is None else hashes
    require(bool(actual) and actual in [v["code_sha256"] for v in read_code_versions(VERSIONS_PATH)["versions"]], "Unreviewed reference download code version")
    return actual


def file_id(entry: dict) -> str:
    """Identify a planned file by everything bound to it, including its bytes."""
    return canonical_hash({k: v for k, v in entry.items() if k != "id"})


def source_for(entry: dict, registry: dict) -> dict:
    """Return the registry source of a planned file; refuse unknown or excluded sources."""
    matches = [s for s in registry["sources"] if s["source_id"] == entry["source_id"]]
    require(len(matches) == 1, "Reference source unknown or out of scope")
    try:
        require_collection_scope(entry["source_id"])
    except ValueError:
        raise ValueError("Reference source unknown or out of scope") from None
    return matches[0]


def bindings_for(entry: dict, source: dict) -> dict:
    """A held source needs a terms or access-release binding; a source that is not held takes none."""
    try:
        bindings = lineage_bindings(source, entry)
    except CaptureError:
        raise ValueError("Reference release binding differs") from None
    require(bool(bindings) == (source["preferred_route"] == "access_hold"), "Reference release binding differs")
    return bindings


def check_request(entry: dict) -> None:
    """Refuse data roles, unknown fields, non-HTTPS origins and file names that could leave their folder."""
    fields = set(entry) - {"id", "sha256", "bytes"}
    require(REQUIRED <= fields <= REQUIRED | OPTIONAL and entry["role"] in ROLES, "Reference plan entry invalid")
    require(all(isinstance(entry[k], str) and entry[k].strip() for k in REQUIRED), "Reference plan entry invalid")
    require(re.fullmatch(r"https://[A-Za-z0-9.-]+/\S*", entry["origin_url"]) is not None, "Reference plan entry invalid")
    require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}", entry["file_name"]) is not None, "Reference plan entry invalid")


def read_download(entry: dict, downloads: Path) -> bytes:
    """Read a saved file only when the browser-recorded origin includes the exact planned URL."""
    original = downloads / entry["file_name"]
    require(original.is_file() and not original.is_symlink(), "Reference download missing or not a regular file")
    try:
        origins = download_metadata.read_origin(original)
    except ValueError:
        raise ValueError("Reference download origin missing") from None
    require(entry["origin_url"] in origins, "Reference download origin differs from the plan")
    return original.read_bytes()


def build_plan(requests: list[dict], downloads: Path) -> dict:
    """Bind each requested document to the bytes and origin of its browser download, for review and locking."""
    registry = load_registry()
    files = []
    for request in requests:
        check_request(request)
        bindings_for(request, source_for(request, registry))
        body = read_download(request, downloads)
        require(0 < len(body) <= MAX_BYTES, "Reference download size invalid")
        entry = dict(request) | {"sha256": digest(body), "bytes": len(body)}
        files.append(entry | {"id": file_id(entry)})
    names = [f["file_name"] for f in files]
    require(len(set(names)) == len(names) and len({f["origin_url"] for f in files}) == len(files), "Reference plan entry invalid")
    return {"version": 1, "model_eligible": False, "registry_sha256": canonical_hash(registry), "files": files}


def load_plan(path: Path | None = None) -> dict:
    """Verify the locked scope, every file's binding and the base registry."""
    path = PLAN_PATH if path is None else path
    plan = read_json(path)
    require(read_json(path.with_suffix(".lock.json"))["plan_sha256"] == canonical_hash(plan), "Reference plan lock differs")
    require(plan["version"] == 1 and plan["model_eligible"] is False and bool(plan["files"]), "Reference plan hold differs")
    registry = load_registry(expected_sha256=plan["registry_sha256"])
    require(plan["registry_sha256"] == canonical_hash(registry), "Reference registry differs")
    for entry in plan["files"]:
        check_request(entry)
        bindings_for(entry, source_for(entry, registry))
        require(entry["id"] == file_id(entry), "Reference plan entry invalid")
        require(re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]) is not None and 0 < entry["bytes"] <= MAX_BYTES, "Reference plan entry invalid")
    names = [f["file_name"] for f in plan["files"]]
    require(len(set(names)) == len(names), "Reference plan entry invalid")
    return plan


def verify_capture(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Check the stored file and its proof against the locked plan, the release binding and the preserved holds."""
    from scripts.acquisition.capture import receipt_validator

    require(not evidence_only, "Reference downloads are stored complete or not at all")
    plan = load_plan()
    require(lineage["plan_sha256"] == canonical_hash(plan), "Reference capture plan differs")
    require(lineage["registry_sha256"] == plan["registry_sha256"], "Reference registry differs")
    require(lineage["schema_sha256"] == canonical_hash(receipt_validator().schema), "Reference schema differs")
    require(lineage["model_eligible"] is False and receipt["snapshot_status"] == "acquired_unvalidated", "Reference modeling hold differs")
    require_code(lineage["code_sha256"])
    matches = [f for f in plan["files"] if f["id"] == lineage["file_id"]]
    require(len(matches) == 1 and matches[0]["source_id"] == source["source_id"], "Reference capture file not in its plan")
    entry = matches[0]
    bindings = bindings_for(entry, source)
    require(all(lineage.get(k) == v for k, v in bindings.items()), "Reference release binding differs")
    require(not {"terms_sha256", "access_release_sha256"} & (set(lineage) - set(bindings)), "Reference release binding differs")
    acq = receipt["acquisition"]
    require(
        acq["requested_url"] == acq["resolved_url"] == entry["origin_url"]
        and acq["request_method"] == "manual_download"
        and acq["http_status"] is None
        and acq["transport_mode"] == "reference_document",
        "Reference route differs",
    )
    raw_path = f"raw/{entry['file_name']}"
    roles = {a["storage_path"]: a["role"] for a in receipt["artifacts"]}
    require(roles == {raw_path: entry["role"], "evidence/download_proof.json": "export_receipt"}, "Reference artifact set differs")
    raw = (root / raw_path).read_bytes()
    require(digest(raw) == lineage["expected_sha256"] == entry["sha256"] and len(raw) == entry["bytes"], "Reference stored bytes differ")
    proof = read_json(root / "evidence/download_proof.json")
    expected = {"file_name": entry["file_name"], "origin_url": entry["origin_url"], "sha256": entry["sha256"], "bytes": entry["bytes"]}
    require(all(proof.get(k) == v for k, v in expected.items()) and proof["created_at_utc"] == acq["retrieved_at_utc"], "Reference download proof differs")
