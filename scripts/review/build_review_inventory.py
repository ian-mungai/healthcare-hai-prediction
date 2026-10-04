"""Merge the frozen capture inventory with a frozen amendment into one effective review inventory.

The base inventory and the amendment are only read. The output is deterministic: repeating the build with the same
inputs replaces the file with identical bytes.

Usage::

    .venv/bin/python -m scripts.review.build_review_inventory --base inputs/capture_inventory.json \
        --amendment inputs/capture_inventory_amendment1.json --output inputs/capture_inventory_effective.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from scripts.review.review_remaining import digest, write_json

FAILED = "failed_attempt_excluded"


def build(base_path: Path, amendment_path: Path) -> dict[str, Any]:
    """Return the effective inventory, or raise ``ValueError`` naming the inconsistency."""
    base = json.loads(base_path.read_text())
    amendment = json.loads(amendment_path.read_text())
    candidates: list[dict[str, Any]] = [dict(c, inventory_origin="base") for c in base["candidates"]]
    by_id = {c["snapshot_id"]: c for c in candidates}
    if len(by_id) != len(candidates):
        raise ValueError("duplicate_snapshot_id_in_base")
    for change in amendment["reclassified_candidates"]:
        target = by_id.get(change["snapshot_id"])
        if target is None:
            raise ValueError(f"reclassified_snapshot_not_in_base: {change['snapshot_id']}")
        if target["status"] != change["from"]:
            raise ValueError(f"reclassified_status_differs_from_base: {change['snapshot_id']}")
        target.update({"status": change["to"], "replaced_by": change["replaced_by"], "inventory_origin": "base_reclassified_by_amendment1"})
    for added in amendment["added_candidates"]:
        if added["snapshot_id"] in by_id:
            raise ValueError(f"added_snapshot_already_in_inventory: {added['snapshot_id']}")
        entry = dict(added, inventory_origin="amendment1")
        by_id[added["snapshot_id"]] = entry
        candidates.append(entry)
    accepted = sum(c["status"] != FAILED for c in candidates)
    if len(candidates) != amendment["candidate_count_after"] or accepted != amendment["acquired_candidates_after"]:
        raise ValueError(f"count_mismatch: {len(candidates)} entries and {accepted} accepted differ from the amendment's totals")
    candidates.sort(key=lambda c: (c["source_id"], c["snapshot_id"]))
    return {
        "version": 2,
        "scope": "Effective review inventory: frozen base inventory plus frozen amendment 1. Capture candidates, not final active datasets.",
        "base_sha256": digest(base_path),
        "amendment_sha256": digest(amendment_path),
        "candidate_count": len(candidates),
        "accepted_count": accepted,
        "failed_attempt_count": len(candidates) - accepted,
        "origin_counts": dict(sorted(Counter(c["inventory_origin"] for c in candidates).items())),
        "execution_approved": False,
        "model_eligible": False,
        "candidates": candidates,
    }


def main() -> int:
    """Build the effective inventory from its real CLI."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--amendment", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        inventory = build(args.base, args.amendment)
    except (ValueError, KeyError, json.JSONDecodeError) as error:
        sys.stderr.write(f"inventory_rejected: {type(error).__name__}: {error}\n")
        return 1
    write_json(args.output, inventory)
    sys.stdout.write(json.dumps({k: inventory[k] for k in ("candidate_count", "accepted_count", "failed_attempt_count", "origin_counts")}) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
