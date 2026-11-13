"""Write the locked plan of the ACS table-vintage units, once, offline.

The plan holds one batch for each table and vintage of the scope table in the contract (173 units). It pins no dictionary, so no network step comes first.
"""

import sys

from scripts.acquisition import census_acs_tables_contract as contract
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry

APPROVAL = "ACS 5-year county tables moved from data.census.gov exports to the Census API: one unit per table and stored vintage (owner, Nov 12 2026)."


def build(scope: dict | None = None) -> dict:
    """The locked plan for a scope table (the contract's own by default); no network access."""
    return {
        "version": 1,
        "source_id": "ACS",
        "credential_reference": "census_api_key",
        "scope": APPROVAL,
        "request_spacing_seconds": 1,
        "registry_sha256": canonical_hash(load_registry()),
        "model_eligible": False,
        "batches": contract.planned_batches(scope),
    }


def main() -> None:
    """Write the plan and its lock write-once, then prove it loads."""
    plan = build()
    write_once(contract.PLAN_PATH, encoded_json(plan))
    write_once(contract.PLAN_PATH.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    contract.load_plan()
    sys.stdout.write(f"{contract.PLAN_PATH.relative_to(REPO_ROOT)} {canonical_hash(plan)} {len(plan['batches'])} batches\n")


if __name__ == "__main__":
    main()
