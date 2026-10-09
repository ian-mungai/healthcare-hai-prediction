"""Write the locked plan for the 44 manually downloaded HUD ZIP-COUNTY workbooks (2010 Q1 to 2020 Q4) once."""

import argparse
import sys

from scripts.acquisition import hud_api_contract as api_contract
from scripts.acquisition import hud_xlsx_contract as contract
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require

TERMS_PATH = "config/acquisition/terms_acceptance_20260928.json"


def build(manifest: dict) -> dict:
    """Bind every quarter to its verified download hash and size; no network access."""
    records = {r["quarter"]: r for r in manifest["verified_downloads"]}
    batches = []
    for year, quarter in contract.quarters():
        record = records[f"{year}Q{quarter}"]
        require(record["filename"] == contract.file_name(year, quarter), "HUD download name differs from its quarter")
        batch = {
            "year": year,
            "quarter": quarter,
            "file_name": record["filename"],
            "sha256": record["sha256"],
            "bytes": record["bytes"],
            "county_geography": contract.county_geography(year, quarter),
        }
        batches.append(batch | {"id": contract.batch_id(batch)})
    return {
        "version": 3,
        "source_id": "HUD",
        "scope": "ZIP-COUNTY workbooks, 2010 Q1 through 2020 Q4, downloaded by hand from the HUD crosswalk files site",
        "route_url": contract.ROUTE_URL,
        "origin": contract.ORIGIN,
        "vintage": "2010Q1_2020Q4_xlsx",
        "header": contract.HEADER,
        "required_state_fips": api_contract.STATE_FIPS,
        "download_manifest": "data/acquisition_planning/hud_zip_county_2010_2020_downloads.json",
        "terms": {"path": TERMS_PATH, "sha256": api_contract.digest((REPO_ROOT / TERMS_PATH).read_bytes())},
        "registry_sha256": canonical_hash(load_registry()),
        "model_eligible": False,
        "batches": batches,
    }


def build_repeat_decision(plan: dict, year: int, quarter: int, repeats: int) -> dict:
    """Record the user's approval to drop an exact number of exactly repeated rows from one quarter's CSV."""
    batch = next(b for b in plan["batches"] if (b["year"], b["quarter"]) == (year, quarter))
    return {
        "version": 1,
        "source_id": "HUD",
        "decision": "Keep the original workbook unchanged; drop exact repeated rows from the derived CSV only and record the count",
        "plan_sha256": canonical_hash(plan),
        "quarters": [{"batch_id": batch["id"], "year": year, "quarter": quarter, "sha256": batch["sha256"], "exact_repeat_rows": repeats}],
    }


def main() -> None:
    """Write the plan and its lock write-once from the verified download manifest, then prove it loads.

    With --approve-exact-repeats, instead write the separate locked repeat decision for one quarter.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--approve-exact-repeats", nargs=3, type=int, metavar=("YEAR", "QUARTER", "ROWS"))
    args = parser.parse_args()
    if args.approve_exact_repeats:
        plan = contract.load_plan()
        decision = build_repeat_decision(plan, *args.approve_exact_repeats)
        write_once(contract.DECISION_PATH, encoded_json(decision))
        write_once(contract.DECISION_PATH.with_suffix(".lock.json"), encoded_json({"decision_sha256": canonical_hash(decision)}))
        require(
            contract.load_repeat_decisions(plan) == {decision["quarters"][0]["batch_id"]: args.approve_exact_repeats[2]}, "HUD repeat decision did not load"
        )
        sys.stdout.write(f"{contract.DECISION_PATH.relative_to(REPO_ROOT)} {canonical_hash(decision)}\n")
        return
    plan = build(read_json(REPO_ROOT / "data/acquisition_planning/hud_zip_county_2010_2020_downloads.json"))
    write_once(contract.PLAN_PATH, encoded_json(plan))
    write_once(contract.PLAN_PATH.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    contract.load_plan()
    sys.stdout.write(f"{contract.PLAN_PATH.relative_to(REPO_ROOT)} {canonical_hash(plan)} {len(plan['batches'])} batches\n")


if __name__ == "__main__":
    main()
