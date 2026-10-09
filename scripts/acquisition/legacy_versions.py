"""Accept the exact predecessor of a tracked plan or record that stored captures still bind (failure modes S2 to S5).

Tracked plans and records state the current rule. Their exact earlier bytes sit in a private, Git-ignored archive;
the tracked catalog binds each one by SHA-256, so a capture that recorded an earlier plan or record keeps verifying.
A missing, changed or unlisted predecessor fails closed, as legacy registries do.
"""

from __future__ import annotations

import hashlib
from functools import lru_cache
from pathlib import Path

from scripts.acquisition.source_registry import REPO_ROOT, RegistryError, canonical_hash, parse_json, require

CATALOG_PATH = REPO_ROOT / "config/acquisition/legacy_versions.json"
CATALOG_LOCK_PATH = CATALOG_PATH.with_name("legacy_versions.lock.json")
ARCHIVE = REPO_ROOT / "data/acquisition_planning/acquisition_legacy_20261009"


@lru_cache(maxsize=4)
def _catalog(raw: bytes, lock_raw: bytes) -> dict:
    catalog = parse_json(raw, "legacy version catalog")
    require(catalog.get("catalog_version") == 1, "Unsupported legacy version catalog")
    lock = parse_json(lock_raw, "legacy version catalog lock")
    require(canonical_hash(catalog) == lock.get("catalog_sha256"), "Legacy version catalog differs from its lock")
    return catalog


def catalog() -> dict:
    """The locked catalog; an empty one when none is tracked."""
    if not CATALOG_PATH.is_file():
        return {"catalog_version": 1, "plans": {}, "records": {}}
    return _catalog(CATALOG_PATH.read_bytes(), CATALOG_LOCK_PATH.read_bytes())


def predecessor_bytes(entry: dict) -> bytes:
    """Read one archived predecessor and prove it is the catalogued file."""
    path = REPO_ROOT / entry["archive_path"]
    require(path.resolve().is_relative_to(ARCHIVE.resolve()), "Legacy file path escapes its archive")
    require(path.is_file() and not path.is_symlink(), "Legacy file is missing or symlinked")
    body = path.read_bytes()
    require(hashlib.sha256(body).hexdigest() == entry["sha256"], "Legacy file differs from its catalogued bytes")
    return body


def plan_matches(plan: dict, recorded: str) -> bool:
    """True for the current plan's hash or a catalogued predecessor of this plan whose archived bytes still match."""
    current = canonical_hash(plan)
    if recorded == current:
        return True
    matches = [entry for entry in catalog().get("plans", {}).get(current, []) if entry["canonical_sha256"] == recorded]
    if len(matches) != 1:
        return False
    try:
        return canonical_hash(parse_json(predecessor_bytes(matches[0]), "legacy plan")) == recorded
    except RegistryError:
        return False


def with_predecessors(plans: dict[str, dict]) -> dict[str, dict]:
    """Plans keyed by canonical hash, plus each verified predecessor hash pointing to its current plan."""
    keyed = dict(plans)
    for current, plan in plans.items():
        for entry in catalog().get("plans", {}).get(current, []):
            if plan_matches(plan, entry["canonical_sha256"]):
                keyed[entry["canonical_sha256"]] = plan
    return keyed


def _records(tracked: Path) -> list[dict]:
    """Catalogued predecessors of a tracked record; a file outside the repository (a test copy) has none."""
    resolved = tracked.resolve()
    if not resolved.is_relative_to(REPO_ROOT.resolve()):
        return []
    return list(catalog().get("records", {}).get(str(resolved.relative_to(REPO_ROOT.resolve())), []))


def bound_record(tracked: Path, recorded_sha256: str) -> bytes | None:
    """The bytes of the tracked record or its archived predecessor that a receipt binds by file SHA-256."""
    body = tracked.read_bytes()
    if hashlib.sha256(body).hexdigest() == recorded_sha256:
        return body
    matches = [entry for entry in _records(tracked) if entry["sha256"] == recorded_sha256]
    return predecessor_bytes(matches[0]) if len(matches) == 1 else None


def record_matches(tracked: Path, recorded_canonical: str) -> bool:
    """True when a receipt's canonical hash is the tracked record's or a catalogued predecessor's with matching bytes."""
    if recorded_canonical == canonical_hash(parse_json(tracked.read_bytes(), tracked.name)):
        return True
    matches = [entry for entry in _records(tracked) if entry.get("canonical_sha256") == recorded_canonical]
    if len(matches) != 1:
        return False
    try:
        return canonical_hash(parse_json(predecessor_bytes(matches[0]), "legacy record")) == recorded_canonical
    except RegistryError:
        return False


def plan_hashes(plan: dict) -> list[str]:
    """The current plan's hash first, then each catalogued predecessor whose archived bytes still match."""
    current = canonical_hash(plan)
    return [current] + [entry["canonical_sha256"] for entry in catalog().get("plans", {}).get(current, []) if plan_matches(plan, entry["canonical_sha256"])]
