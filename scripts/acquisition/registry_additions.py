"""Validate dated, user-approved measure additions without rewriting the hash-locked source registry.

The base registry hash is recorded in existing plans, batches and receipts, so additions live in a companion file bound to that
hash. Additions never clear gates or holds and never approve a measure for modeling.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any

from scripts.acquisition.source_registry import LOCK_PATH, REGISTRY_PATH, RegistryError, canonical_hash, index_records, load_registry, read_json, require

ADDITIONS_PATH = REGISTRY_PATH.with_name("registry_additions.json")
ADDITIONS_LOCK_PATH = REGISTRY_PATH.with_name("registry_additions_lock.json")
REVIEW_RULE = "Review additions and lock diffs together against the recorded user decision. This lock detects drift; it is not a signature or new approval."


def validate_addition(control: dict[str, Any], base_controls: dict[str, dict[str, Any]], base_sources: dict[str, dict[str, Any]]) -> None:
    """Reject an added measure control that could substitute, orphan or clear a registered control.

    Parameters
    ----------
    control : dict
        One entry from the additions file's ``measure_controls`` list.
    base_controls, base_sources : dict
        The base registry's measure controls and sources, indexed by ID.

    Raises
    ------
    RegistryError
        Through ``require`` when any rule fails.
    """
    control_id = control.get("id")
    require(isinstance(control_id, str) and control_id not in base_controls, f"Added control {control_id} collides with a registered control.")
    require(control.get("parent_id") in base_controls, f"Added control {control_id} names an unknown parent.")
    sources = control.get("source_ids")
    require(isinstance(sources, list) and bool(sources) and all(s in base_sources for s in sources), f"Added control {control_id} needs registered source IDs.")
    # A new control must start held and ungated, so adding it can never approve a measure for modeling.
    require(control.get("gate_status") == "not_run", f"Added control {control_id} cannot start with a cleared gate.")
    decision = (control.get("preserved_controls") or {}).get("current_review_decision")
    require(isinstance(decision, str) and decision.startswith("hold_"), f"Added control {control_id} must start on a hold_ decision.")
    approval = control.get("approval") or {}
    require(approval.get("decided_by") == "user", f"Added control {control_id} needs the user's approval.")
    decided_on = approval.get("decided_on")
    require(
        isinstance(decided_on, str) and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", decided_on) is not None,
        f"Added control {control_id} needs an ISO approval date.",
    )
    try:
        date.fromisoformat(str(decided_on))
    except ValueError as error:
        raise RegistryError(f"Added control {control_id} has an impossible approval date.") from error


def additions_lock(additions: dict[str, Any], registry: dict[str, Any]) -> dict[str, Any]:
    """Return the drift-detection lock for an additions document bound to the given registry."""
    controls = index_records(additions.get("measure_controls"), "id", "added measure control")
    return {
        "lock_version": 1,
        "base_registry_sha256": canonical_hash(registry),
        "additions_sha256": canonical_hash(additions),
        "control_ids": sorted(controls),
        "review_rule": REVIEW_RULE,
    }


def validate_additions(additions: dict[str, Any], lock: dict[str, Any], registry: dict[str, Any]) -> list[dict[str, Any]]:
    """Verify additions against their lock and the current base registry, returning the added controls."""
    require(additions.get("additions_version") == 1 and lock.get("lock_version") == 1, "Unsupported additions or lock version.")
    base_hash = canonical_hash(registry)
    require(additions.get("base_registry_sha256") == base_hash == lock.get("base_registry_sha256"), "Additions are bound to a different registry; review them.")
    controls = index_records(additions.get("measure_controls"), "id", "added measure control")
    base_controls = index_records(registry.get("measure_controls"), "id", "measure control")
    base_sources = index_records(registry.get("sources"), "source_id", "source")
    for control in controls.values():
        validate_addition(control, base_controls, base_sources)
    require(sorted(controls) == lock.get("control_ids"), "Added control IDs differ from the additions lock.")
    require(canonical_hash(additions) == lock.get("additions_sha256"), "Additions differ from their lock; review the change before relocking.")
    return list(controls.values())


def load_registry_additions(registry: dict[str, Any], path: Path = ADDITIONS_PATH, lock_path: Path = ADDITIONS_LOCK_PATH) -> list[dict[str, Any]]:
    """Return validated added controls; both files absent means no additions, one absent is an error."""
    initial_presence = (path.exists(), lock_path.exists())
    if initial_presence == (False, False):
        return []
    require(all(initial_presence), "Registry additions and their lock must exist together.")
    if path == ADDITIONS_PATH and lock_path == ADDITIONS_LOCK_PATH:
        from scripts.acquisition.source_registry import registry_version_paths

        current = read_json(ADDITIONS_PATH)
        if current.get("base_registry_sha256") != canonical_hash(registry):
            legacy = registry_version_paths(canonical_hash(registry))
            path, lock_path = legacy["additions"], legacy["additions_lock"]
    present = (path.exists(), lock_path.exists())
    if present == (False, False):
        return []
    require(all(present), "Registry additions and their lock must exist together.")
    try:
        return validate_additions(read_json(path), read_json(lock_path), registry)
    except (KeyError, TypeError, AttributeError) as error:
        raise RegistryError("Malformed registry additions or lock.") from error


def write_additions_lock(registry: dict[str, Any], path: Path, lock_path: Path) -> str:
    """Create the lock once after validating every addition; an identical lock is left unchanged."""
    additions = read_json(path)
    try:
        lock = additions_lock(additions, registry)
        validate_additions(additions, lock, registry)
    except (KeyError, TypeError, AttributeError) as error:
        raise RegistryError("Malformed registry additions.") from error
    if lock_path.exists():
        require(read_json(lock_path) == lock, "Refusing to replace a stale additions lock; review the diff and remove it deliberately.")
        return "Additions lock already current."
    with lock_path.open("x", encoding="utf-8") as handle:
        handle.write(canonical_json(lock))
    return "Additions lock written. Review additions and lock together."


def canonical_json(value: dict[str, Any]) -> str:
    """Return the project's deterministic, reviewable JSON text for configuration files."""
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"


def main() -> None:
    """Validate the base registry plus additions, or write the additions lock, and report the totals."""
    parser = argparse.ArgumentParser(description="Validate user-approved registry additions offline. No downloads, AWS access or gate clearance.")
    parser.add_argument("--registry", type=Path, default=REGISTRY_PATH)
    parser.add_argument("--lock", type=Path, default=LOCK_PATH)
    parser.add_argument("--additions", type=Path, default=ADDITIONS_PATH)
    parser.add_argument("--additions-lock", type=Path, default=ADDITIONS_LOCK_PATH)
    parser.add_argument("--write-lock", action="store_true", help="Create the additions lock if absent; verify it if present.")
    arguments = parser.parse_args()
    try:
        registry = load_registry(arguments.registry, arguments.lock)
        message = write_additions_lock(registry, arguments.additions, arguments.additions_lock) if arguments.write_lock else None
        added = load_registry_additions(registry, arguments.additions, arguments.additions_lock)
    except (RegistryError, OSError) as error:
        parser.error(str(error))
    if message:
        sys.stdout.write(message + "\n")
    base = len(registry["measure_controls"])
    ids = ", ".join(control["id"] for control in added) or "none"
    sys.stdout.write(f"Registry additions valid: {len(added)} user-added measure controls ({ids}); base registry {canonical_hash(registry)[:12]} unchanged.\n")
    sys.stdout.write(f"Measure controls: {base} registered + {len(added)} added = {base + len(added)}. All gates remain not_run.\n")


if __name__ == "__main__":
    main()
