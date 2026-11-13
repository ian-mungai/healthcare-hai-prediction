"""The tracked bundle of the rewritten legacy registry is bound together: its additions, its lock, the version catalog and the MMD additions plan."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from scripts.acquisition import mmd_api_contract as contract
from scripts.acquisition.registry_additions import validate_additions
from scripts.acquisition.source_registry import VERSION_CATALOG_LOCK_PATH, VERSION_CATALOG_PATH, canonical_hash
from tests.support import check

BUNDLE = Path("config/acquisition/baseline_run/plan_inputs")
FOLDER = BUNDLE / "data/acquisition_planning/registry_legacy_20260929"


def load(name: str) -> dict:
    return json.loads((FOLDER / name).read_text(encoding="utf-8"))


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_the_additions_and_their_lock_are_bound_to_the_rewritten_registry() -> None:
    registry, additions, lock = load("source_registry.json"), load("registry_additions.json"), load("registry_additions_lock.json")
    base = canonical_hash(registry)
    check(additions["base_registry_sha256"] == base == lock["base_registry_sha256"], "the additions and the lock name the rewritten registry")
    check(lock["additions_sha256"] == canonical_hash(additions), "the lock holds the digest of the additions")
    check(len(validate_additions(additions, lock, registry)) == len(lock["control_ids"]), "every added control validates against the rewritten registry")


def test_the_version_catalog_lists_the_bundle_files() -> None:
    catalog = json.loads(VERSION_CATALOG_PATH.read_text(encoding="utf-8"))
    base = canonical_hash(load("source_registry.json"))
    version = next(item for item in catalog["legacy_versions"] if item["registry_sha256"] == base)
    names = {
        "registry": "source_registry.json",
        "lock": "source_registry_lock.json",
        "additions": "registry_additions.json",
        "additions_lock": "registry_additions_lock.json",
    }
    for key, name in names.items():
        check(version["files"][key]["sha256"] == sha(FOLDER / name), f"the catalog holds the digest of {name}")
    check(
        json.loads(VERSION_CATALOG_LOCK_PATH.read_text(encoding="utf-8"))["catalog_sha256"] == canonical_hash(catalog), "the catalog lock matches the catalog"
    )


def test_the_additions_plan_pins_the_lock_and_the_registry_of_the_bundle() -> None:
    plan = json.loads((BUNDLE / contract.ADDITIONS_PLAN_PATH).read_bytes())
    check(plan["registry_additions_lock_sha256"] == sha(FOLDER / "registry_additions_lock.json"), "the plan pins the digest of the additions lock")
    check(plan["registry_sha256"] == canonical_hash(load("source_registry.json")), "the plan pins the rewritten registry")
    check(
        hashlib.sha256((BUNDLE / contract.ADDITIONS_PLAN_PATH).read_bytes()).hexdigest() == contract.ADDITIONS_PLAN_SHA256, "the contract holds the plan digest"
    )
