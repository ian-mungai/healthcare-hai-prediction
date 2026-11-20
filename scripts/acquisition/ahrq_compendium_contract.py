"""Frozen scope of the AHRQ Compendium of U.S. Health Systems hospital linkage files that the owner saved in a browser.

AHRQ refuses scripted downloads (the registry holds AHRQ_SYSTEM for that reason), so the owner saved each file by hand. A locked plan binds
every file to its exact origin URL, SHA-256 and size: the six linkage workbooks (2016, 2018, 2020, 2021, 2022 and 2023 vintages) and the four technical
documents (three more that the owner saved are left out: each holds a user directory path). The files are seed-only manual units of the mode
ahrq_compendium (``manual_content``): the bootstrap seed holds the originals and the run binds each one to this plan.
The owner lifted the access hold for these files on 8 Nov 2026 by one append-only release record, and the base registry stays unchanged.
AHRQ publishes no terms text for them: the evidence the owner's decision rests on is the closed terms record below, and no reuse license is inferred.
"""

import hashlib
import re
from pathlib import Path
from urllib.parse import urlsplit

from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require, require_collection_scope

PLAN_PATH = REPO_ROOT / "config/acquisition/ahrq_compendium_plan.json"
RELEASE_PATH = "config/acquisition/access_releases_20261108.json"
SOURCE_ID = "AHRQ_SYSTEM"
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
        "the four technical documents and six data files of the plan: no copyright, public domain or permission text",
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
