"""Write the locked plan for the CDC WONDER county-year exports saved by hand, once."""

import sys

from scripts.acquisition import wonder_export_contract as contract
from scripts.acquisition.bls_api_contract import digest
from scripts.acquisition.hud_api_contract import STATE_FIPS
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json

TERMS_PATH = "config/acquisition/terms_acceptance_20260928.json"
MANIFEST_PATH = "data/acquisition_planning/wonder_downloads.json"


def build(manifest: dict, min_counties_per_year: int = 3100) -> dict:
    """Bind every export to its verified download hash and size; no network access."""
    batches = []
    for record in manifest["verified_downloads"]:
        batch = {
            "database": record["database"],
            "years": record["years"],
            "file_name": record["filename"],
            "sha256": record["sha256"],
            "bytes": record["bytes"],
        }
        batches.append(batch | {"id": contract.batch_id(batch)})
    return {
        "version": 1,
        "source_id": "WONDER",
        "scope": "County-year deaths, population and crude rate per 100,000 with 95% limits and standard error; "
        "two pilot exports and full exports of 1999-2020 (D76) and 2018-2024 (D158), saved by hand",
        "databases": contract.DATABASES,
        "header": contract.HEADER,
        "fixed_parameters": contract.FIXED_PARAMETERS,
        "required_state_fips": STATE_FIPS,
        "min_counties_per_year": min_counties_per_year,
        "download_manifest": MANIFEST_PATH,
        "terms": {"path": TERMS_PATH, "sha256": digest((REPO_ROOT / TERMS_PATH).read_bytes())},
        "registry_sha256": canonical_hash(load_registry()),
        "model_eligible": False,
        "batches": batches,
    }


def main() -> None:
    """Write the plan and its lock write-once from the verified download manifest, then prove it loads."""
    plan = build(read_json(REPO_ROOT / MANIFEST_PATH))
    write_once(contract.PLAN_PATH, encoded_json(plan))
    write_once(contract.PLAN_PATH.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    contract.load_plan()
    sys.stdout.write(f"{contract.PLAN_PATH.relative_to(REPO_ROOT)} {canonical_hash(plan)} {len(plan['batches'])} exports\n")


if __name__ == "__main__":
    main()
