"""One-time development step: move the reference download plan to version 2, which holds no file.

The five documents of version 1 left the bootstrap (owner, Nov 12 2026). Version 2 binds the registry the repository ships, so a clean checkout loads it
without the ignored legacy archive. The version 1 plan is archived under ARCHIVE and listed as the predecessor of version 2, so captures that recorded it
keep verifying. The step writes under ROOT only, so a staging copy of the config folder takes the archive.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from scripts.acquisition.repin_registry import CONFIG, legacy_catalog
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import canonical_hash

PLAN = "reference_downloads_plan.json"
ARCHIVE = "data/acquisition_planning/acquisition_legacy_20261009/pre_seed_plan_versions_20261112"


def shipped_registry(root: Path) -> str:
    """The canonical hash of the registry that the locked version catalog names as the repository's own."""
    path = root / CONFIG / "registry_versions.json"
    catalog = json.loads(path.read_text(encoding="utf-8"))
    if json.loads(path.with_suffix(".lock.json").read_text(encoding="utf-8")).get("catalog_sha256") != canonical_hash(catalog):
        raise ValueError("The registry version catalog differs from its lock")
    return str(catalog["current_registry_sha256"])


def successor(plan: dict, registry_sha256: str) -> dict:
    """The version 2 plan of a version 1 plan: the shipped registry and the same hold, no file."""
    if plan.get("version") != 1:
        raise ValueError("Only a version 1 plan has a version 2 successor")
    return {"version": 2, "model_eligible": plan["model_eligible"], "registry_sha256": registry_sha256, "files": []}


def apply(root: Path, archive: str = ARCHIVE) -> str:
    """Write the successor plan and its lock, archive the version 1 file and update the catalog. Return the new canonical hash."""
    path = root / CONFIG / PLAN
    lock_path = path.with_suffix(".lock.json")
    before = path.read_bytes()
    plan = json.loads(before)
    if json.loads(lock_path.read_text(encoding="utf-8")).get("plan_sha256") != canonical_hash(plan):
        raise ValueError("The plan differs from its lock")
    new = successor(plan, shipped_registry(root))
    fresh = canonical_hash(new)
    path.write_bytes(encoded_json(new))
    lock_path.write_bytes(encoded_json({"plan_sha256": fresh}))
    legacy_catalog(root, {canonical_hash(plan): (path, "plan_sha256", fresh)}, {path: before}, archive)
    return fresh


def main() -> None:
    """Apply the step to the config folder under ROOT (a staging copy, never a checkout whose data folder links to another checkout)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    sys.stdout.write(json.dumps({"plan_sha256": apply(args.root)}) + "\n")


if __name__ == "__main__":
    main()
