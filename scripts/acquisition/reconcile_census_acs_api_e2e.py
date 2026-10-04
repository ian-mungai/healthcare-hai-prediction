"""Reconcile every stored Census profile in both locked plans offline and prove a no-effects replay."""

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from scripts.acquisition import census_acs_api_contract as contract
from scripts.acquisition.collect_census_acs_api import execute
from scripts.acquisition.run_bls_api_e2e import inventory
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, read_json, require


def reconcile() -> dict:
    """Validate captures, transport caches, all storage records and unchanged local files."""
    plans = contract.load_plans()
    plan = plans[0]
    root = REPO_ROOT / "data/datasets/historical_acquisition/census_acs_api_history" / plan["vintage"]
    before, tables, objects = inventory(root), [], set()
    for plan_item, batch in [(p, b) for p in plans for b in p["batches"]]:
        branch = root / "batches" / batch["id"]
        require((branch / "completed.json").is_file(), "Census profile is not stored")
        completed = execute(plan_item, batch, root, False, False, None, {})
        envelope = read_json(branch / "transport.json")
        raw = (branch / "capture/raw/response.json").read_bytes()
        require(bytes.fromhex(envelope["body_hex"]) == raw and envelope["sha256"] == contract.digest(raw), "Census transport cache bytes differ")
        require(envelope["request"] == contract.request_for(batch) and envelope["endpoint"] == contract.ENDPOINT, "Census transport scope differs")
        require(envelope["bytes"] == len(raw) and envelope["http_status"] == 200, "Census transport provenance differs")
        proof = read_json(branch / "capture/evidence/request_response.json")
        require(envelope["retrieved_at_utc"] == proof["retrieved_at_utc"], "Census transport time differs")
        reconciliation = read_json(branch / "capture/s3_collections_reconciliation.json")
        for item in [*reconciliation["objects"], *reconciliation["manifests"]]:
            obj = item["object"]
            objects.add((obj["bucket"], obj["key"], obj["version_id"]))
        tables.append(
            {
                "table": batch["table"],
                "plan_version": plan_item["version"],
                "county_count": len(plan_item["county_ids"]),
                "batch_id": batch["id"],
                "statistics": proof["statistics"],
                "receipt_path": completed["receipt_path"],
                "receipt_sha256": completed["receipt_sha256"],
                "reconciliation_sha256": completed["reconciliation_sha256"],
                "raw_sha256": contract.digest(raw),
                "csv_sha256": fingerprint(branch / "capture/derived/observations.csv")[0],
            }
        )
    require(before == inventory(root), "Census replay changed local collection files")
    require(len(tables) == 5 and len(objects) == 35, "Census stored table/object coverage differs")
    return {
        "status": "passed",
        "checked_at_utc": datetime.now(UTC).isoformat(),
        "source_id": "ACS",
        "year": 2009,
        "window": [2005, 2009],
        "table_count": len(tables),
        "county_count_per_plan": {p["version"]: len(p["county_ids"]) for p in plans},
        "total_table_county_rows": sum(t["statistics"]["rows"] for t in tables),
        "version_verified_s3_objects": len(objects),
        "plan_sha256": [canonical_hash(p) for p in plans],
        "code_sha256": contract.code_hashes(),
        "tables": tables,
        "collection_files_checked": len(before),
        "no_effects_replay": True,
        "model_eligible": False,
        "boundary": "Complete offline reconstruction of both plans and saved exact-version S3 readback evidence; no new API/S3 requests or credential reads.",
        "known_gaps": ["DP02 publishes no values for the 78 Puerto Rico municipios; plan 2 DP02PR holds Puerto Rico social characteristics"],
        "open_reviews": [
            "definitions_by_vintage",
            "MOE_and_special_tokens",
            "five_year_window_alignment",
            "geography_and_hospital_crosswalk",
            "model_eligibility",
        ],
        "other_ACS_histories": "unchanged; existing holds remain",
    }


def main() -> None:
    """Persist immutable complete-coverage evidence to a new requested output path."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = reconcile()
    write_once(args.output, encoded_json(report))
    sys.stdout.write(
        json.dumps({k: report[k] for k in ["status", "table_count", "total_table_county_rows", "version_verified_s3_objects", "no_effects_replay"]}) + "\n"
    )


if __name__ == "__main__":
    main()
