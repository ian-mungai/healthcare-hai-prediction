"""Write the locked HUD ZIP-to-county plans once: the 2021 Q1 pilot and the 2021 Q2-2025 Q4 range."""

import sys

from scripts.acquisition import hud_api_contract as contract
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry

TERMS_PATH = "config/acquisition/terms_acceptance_20260928.json"


def build() -> dict:
    """Pin scope, documented fields, terms record and registry; no network access."""
    batch = {"type": 2, "query": "All", "year": 2021, "quarter": 1}
    return {
        "version": 1,
        "source_id": "HUD",
        "scope": "User decision 2026-09-28: pilot ZIP-to-county, 2021 Q1, nationwide, one request",
        "endpoint": contract.ENDPOINT,
        "credential_reference": contract.CREDENTIAL,
        "vintage": "2021Q1",
        "envelope_fields": sorted(["year", "quarter", "input", "crosswalk_type", "results"]),
        "result_fields": sorted(["zip", "geoid", "res_ratio", "bus_ratio", "oth_ratio", "tot_ratio", "city", "state"]),
        "required_state_fips": contract.STATE_FIPS,
        "terms": {"path": TERMS_PATH, "sha256": contract.digest((REPO_ROOT / TERMS_PATH).read_bytes())},
        "registry_sha256": canonical_hash(load_registry()),
        "request_spacing_seconds": 2,
        "model_eligible": False,
        "batches": [batch | {"id": contract.batch_id(batch)}],
    }


def build_range(pilot: dict) -> dict:
    """User decision 2026-09-28: 2021 Q2 through 2025 Q4, bound to the unchanged pilot plan."""
    quarters = [(y, q) for y in range(2021, 2026) for q in range(1, 5) if (2021, 2) <= (y, q) <= (2025, 4)]
    batches = []
    for year, quarter in quarters:
        batch = {"type": 2, "query": "All", "year": year, "quarter": quarter}
        batches.append(batch | {"county_geography": contract.county_geography(batch), "id": contract.batch_id(batch)})
    return pilot | {
        "version": 2,
        "scope": "User decision 2026-09-28: ZIP-to-county, 2021 Q2 through 2025 Q4, nationwide, 19 requests",
        "vintage": "2021Q2_2025Q4",
        "supplements_plan_sha256": canonical_hash(pilot),
        "batches": batches,
    }


def main() -> None:
    """Write both plans and locks write-once (the pilot must reproduce exactly), then prove they load."""
    pilot = build()
    for path, plan in [(contract.PLAN_PATH, pilot), (contract.RANGE_PLAN_PATH, build_range(pilot))]:
        write_once(path, encoded_json(plan))
        write_once(path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
        sys.stdout.write(f"{path.relative_to(REPO_ROOT)} {canonical_hash(plan)} {len(plan['batches'])} batches\n")
    contract.load_plans()


if __name__ == "__main__":
    main()
