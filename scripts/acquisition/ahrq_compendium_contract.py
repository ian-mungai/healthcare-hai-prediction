"""Frozen scope and checks for the AHRQ Compendium of U.S. Health Systems hospital linkage files that the owner saved in a browser.

AHRQ refuses scripted downloads (the registry holds AHRQ_SYSTEM for that reason), so the owner saved each file by hand. A locked plan binds
every file to its exact origin URL, SHA-256 and size. The six linkage workbooks (2016, 2018, 2020, 2021, 2022 and 2023 vintages) are stored
as data and the technical documents as methodology references. The owner lifted the access hold for this capture on 8 Nov 2026; the lift is
one append-only release record that every receipt binds by SHA-256, and the base registry stays unchanged.
Classification: Public (hospital and system names, no persons). AHRQ publishes no terms text for these files; the evidence the owner's
decision rests on is one closed terms record that every receipt carries, and no reuse license is inferred.
"""

import hashlib
import re
from pathlib import Path
from urllib.parse import urlsplit

from scripts.acquisition import hud_xlsx_contract as download_metadata
from scripts.acquisition.code_closure import closure_hashes
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.legacy_versions import plan_matches
from scripts.acquisition.s3_store import SOURCE_RELEASE_COVERS
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require, require_collection_scope
from scripts.acquisition.transport import inspect_payload

PLAN_PATH = REPO_ROOT / "config/acquisition/ahrq_compendium_plan.json"
REQUESTS_PATH = REPO_ROOT / "config/acquisition/ahrq_compendium_requests.json"
VERSIONS_PATH = REPO_ROOT / "config/acquisition/ahrq_compendium_code_versions.json"
RELEASE_PATH = "config/acquisition/access_releases_20261108.json"
CODE_ENTRY = "scripts.acquisition.store_ahrq_compendium"
SOURCE_ID = "AHRQ_SYSTEM"
MODE = "ahrq_compendium"
PUBLISHER_PREFIX = "ahrq/health_system_compendium/"
ORIGIN_PREFIX = "https://www.ahrq.gov/sites/default/files/wysiwyg/"
ROLES = {"data", "methodology"}
# Suffix to (payload format, media type, role the suffix may carry).
FORMATS = {
    ".xlsx": ("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "data"),
    ".xls": ("xls", "application/vnd.ms-excel", "data"),
    ".pdf": ("pdf", "application/pdf", "methodology"),
}
REQUIRED = {"source_id", "role", "title", "publisher", "origin_url", "file_name", "media_type", "vintage", "release_binding"}
MAX_BYTES = 64 * 1024**2
TERMS_RECORD = {
    "terms_status": "no_published_terms_found",
    "internet_citation": "Compendium of U.S. Health Systems, 2023. Agency for Healthcare Research and Quality, Rockville, MD. "
    "https://www.ahrq.gov/chsp/data-resources/compendium-2023.html",
    "checked_on": "2026-11-09",
    "checked": [
        "Compendium 2023 page, https://www.ahrq.gov/chsp/data-resources/compendium-2023.html: Internet citation only",
        "https://www.ahrq.gov/policy/electronic/disclaimers/index.html: no reuse text",
        "the seven technical documents and six data files of the plan: no copyright, public domain or permission text",
    ],
    "reuse_license": "none inferred",
    "open_item": "settle terms before any publication of AHRQ-derived data",
}
USE_RESTRICTIONS = [
    "AHRQ publishes no terms text for these files (checked 2026-11-09, see the terms record in the receipt lineage); no reuse license is inferred.",
    "Hospital and health-system names only, no persons. A health system is joint ownership or management, not legal ownership.",
    "Each vintage reflects systems in its own year or the year before; read its reference date from the technical document.",
    "Preserve all source and measure holds; no model approval.",
]


def digest(body: bytes) -> str:
    """Return a complete byte SHA-256."""
    return hashlib.sha256(body).hexdigest()


def require_code(hashes: dict | None = None) -> dict:
    """Accept only an explicitly reviewed implementation version of the collector and every module it can run."""
    actual = closure_hashes(CODE_ENTRY) if hashes is None else hashes
    require(bool(actual) and actual in [v["code_sha256"] for v in read_code_versions(VERSIONS_PATH)["versions"]], "Unreviewed AHRQ capture code version")
    return actual


def file_id(entry: dict) -> str:
    """Identify a planned file by everything bound to it, including its bytes."""
    return canonical_hash({k: v for k, v in entry.items() if k != "id"})


def release_record() -> dict:
    """The release record that lifts the hold: path and SHA-256 of the exact bytes receipts bind."""
    return {"path": RELEASE_PATH, "sha256": digest((REPO_ROOT / RELEASE_PATH).read_bytes())}


def source_of(registry: dict) -> dict:
    """The held registry source, in scope of the collection layout."""
    matches = [s for s in registry["sources"] if s["source_id"] == SOURCE_ID]
    require(len(matches) == 1, "AHRQ source unknown or out of scope")
    require_collection_scope(SOURCE_ID)
    return matches[0]


def check_request(entry: dict) -> None:
    """Refuse other sources, roles, hosts and formats, and names that could leave their folder."""
    fields = set(entry) - {"id", "sha256", "bytes"}
    require(fields == REQUIRED and entry["source_id"] == SOURCE_ID and entry["role"] in ROLES, "AHRQ plan entry invalid")
    require(all(isinstance(entry[k], str) and entry[k].strip() for k in REQUIRED), "AHRQ plan entry invalid")
    name = entry["file_name"]
    require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}", name) is not None, "AHRQ plan entry invalid")
    form = FORMATS.get(Path(name).suffix.lower())
    require(form is not None and entry["role"] == form[2] and entry["media_type"] == form[1], "AHRQ plan entry invalid")
    origin = urlsplit(entry["origin_url"])
    require(
        entry["origin_url"].startswith(ORIGIN_PREFIX) and origin.hostname == "www.ahrq.gov" and not origin.query and origin.path.rsplit("/", 1)[-1] == name,
        "AHRQ plan entry invalid",
    )
    require(re.fullmatch(r"20[0-9]{2}", entry["vintage"]) is not None and entry["release_binding"] == "access_release", "AHRQ plan entry invalid")


def read_download(entry: dict, downloads: Path) -> bytes:
    """Read a saved file only when the browser-recorded origin includes the exact planned URL and the bytes are what the name claims."""
    original = downloads / entry["file_name"]
    require(original.is_file() and not original.is_symlink(), "AHRQ download missing or not a regular file")
    try:
        origins = download_metadata.read_origin(original)
    except ValueError:
        raise ValueError("AHRQ download origin missing") from None
    require(entry["origin_url"] in origins, "AHRQ download origin differs from the plan")
    form = FORMATS[original.suffix.lower()][0]
    require(inspect_payload(original, form, entry["role"]) is None, f"AHRQ download is not a valid {form} file")
    return original.read_bytes()


def build_plan(requests: list[dict], downloads: Path) -> dict:
    """Bind each requested file to the bytes and origin of its browser download, for review and locking."""
    registry = load_registry()
    source_of(registry)
    files = []
    for request in requests:
        check_request(request)
        body = read_download(request, downloads)
        require(0 < len(body) <= MAX_BYTES, "AHRQ download size invalid")
        entry = dict(request) | {"sha256": digest(body), "bytes": len(body)}
        files.append(entry | {"id": file_id(entry)})
    names = [f["file_name"] for f in files]
    require(len(set(names)) == len(names) and len({f["origin_url"] for f in files}) == len(files), "AHRQ plan entry invalid")
    return {
        "version": 1,
        "model_eligible": False,
        "registry_sha256": canonical_hash(registry),
        "access_release": release_record(),
        "files": sorted(files, key=lambda f: (f["role"] != "data", f["vintage"], f["file_name"])),
    }


def load_plan(path: Path | None = None) -> dict:
    """Verify the locked scope, every file's binding, the release record and the base registry."""
    path = PLAN_PATH if path is None else path
    plan = read_json(path)
    require(read_json(path.with_suffix(".lock.json"))["plan_sha256"] == canonical_hash(plan), "AHRQ plan lock differs")
    require(plan["version"] == 1 and plan["model_eligible"] is False and bool(plan["files"]), "AHRQ plan hold differs")
    registry = load_registry(expected_sha256=plan["registry_sha256"])
    require(plan["registry_sha256"] == canonical_hash(registry), "AHRQ registry differs")
    source_of(registry)
    require(plan["access_release"] == release_record(), "AHRQ release record differs")
    record = read_json(REPO_ROOT / RELEASE_PATH)
    require([r["source_id"] for r in record["releases"] if r["released"] is True] == [SOURCE_ID], "AHRQ release record differs")
    for entry in plan["files"]:
        check_request(entry)
        require(entry["id"] == file_id(entry), "AHRQ plan entry invalid")
        require(re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]) is not None and 0 < entry["bytes"] <= MAX_BYTES, "AHRQ plan entry invalid")
    names = [f["file_name"] for f in plan["files"]]
    require(len(set(names)) == len(names), "AHRQ plan entry invalid")
    return plan


def verify_capture(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Check the stored file and its proof against the locked plan, the bound release record and the preserved holds."""
    from scripts.acquisition.capture import receipt_validator

    require(not evidence_only and source["source_id"] == SOURCE_ID and lineage.get("mode") == MODE, "AHRQ storage source differs")
    plan = load_plan()
    require(plan_matches(plan, lineage["plan_sha256"]), "AHRQ capture plan differs")
    require(lineage["registry_sha256"] == plan["registry_sha256"], "AHRQ registry differs")
    require(lineage["schema_sha256"] == canonical_hash(receipt_validator().schema), "AHRQ schema differs")
    require(lineage["model_eligible"] is False and receipt["snapshot_status"] == "acquired_unvalidated", "AHRQ modeling hold differs")
    require_code(lineage["code_sha256"])
    matches = [f for f in plan["files"] if f["id"] == lineage["file_id"]]
    require(len(matches) == 1, "AHRQ capture file not in its plan")
    entry = matches[0]
    require(lineage.get("access_release_sha256") == plan["access_release"]["sha256"], "AHRQ release binding differs")
    needed = "whole_workbooks" if entry["role"] == "data" else "reference_documents"
    require(needed in SOURCE_RELEASE_COVERS.get(Path(RELEASE_PATH).name, frozenset()), "AHRQ release binding differs: the record does not cover this file")
    require(lineage.get("terms_record") == TERMS_RECORD, "AHRQ terms record differs")
    acq = receipt["acquisition"]
    mode = "download" if entry["role"] == "data" else "reference_document"
    require(
        acq["requested_url"] == acq["resolved_url"] == entry["origin_url"]
        and acq["request_method"] == "manual_download"
        and acq["http_status"] is None
        and acq["transport_mode"] == mode,
        "AHRQ route differs",
    )
    require(receipt["governance"]["access_class"] == "public" and receipt["governance"]["contains_pii"] is False, "AHRQ governance differs")
    require(receipt["governance"]["use_restrictions"] == USE_RESTRICTIONS, "AHRQ governance differs")
    raw_path = f"raw/{entry['file_name']}"
    roles = {a["storage_path"]: a["role"] for a in receipt["artifacts"]}
    require(roles == {raw_path: entry["role"], "evidence/download_proof.json": "export_receipt"}, "AHRQ artifact set differs")
    raw = (root / raw_path).read_bytes()
    require(digest(raw) == lineage["expected_sha256"] == entry["sha256"] and len(raw) == entry["bytes"], "AHRQ stored bytes differ from the plan")
    proof = read_json(root / "evidence/download_proof.json")
    expected = {"file_name": entry["file_name"], "origin_url": entry["origin_url"], "sha256": entry["sha256"], "bytes": entry["bytes"]}
    require(all(proof.get(k) == v for k, v in expected.items()) and proof["created_at_utc"] == acq["retrieved_at_utc"], "AHRQ download proof differs")
