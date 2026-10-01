"""Reconcile completed MMD exports with local bytes and saved S3 readback evidence."""

import argparse
import csv
import json
import sys
from pathlib import Path

from scripts.acquisition.capture import receipt_validator, validate_receipt
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import read_json, require


def main() -> None:
    """Emit immutable coverage evidence; no network requests or eligibility promotion."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--measure-id", required=True)
    parser.add_argument("--years", nargs="+", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = Path("data/historical_acquisition/mmd_browser_history")
    validator = receipt_validator()
    records = []
    seen = set()
    for completion_path in sorted(root.glob("*/completed.json")):
        completion = read_json(completion_path)
        if completion.get("measure_id") != args.measure_id:
            continue
        year = completion["year"]
        require(year not in seen, "Multiple completed captures for the same condition-year; review revisions.")
        seen.add(year)
        reconciliation_path = Path(completion["reconciliation_path"])
        require(reconciliation_path.resolve().is_relative_to(completion_path.parent.resolve()), "Reconciliation outside capture.")
        reconciliation = read_json(reconciliation_path)
        receipt = read_json(reconciliation_path.parent / "receipt.json")
        validate_receipt(receipt, validator, reconciliation_path.parent)
        require(receipt["snapshot_id"] == reconciliation["snapshot_id"], "Mismatched snapshot.")
        require(json.loads(receipt["lineage"]["extraction_or_query"])["measure_id"] == args.measure_id, "Wrong measure lineage.")
        selections = receipt["acquisition"]["export_selections"]
        require(selections["year"] == str(year) and len(selections) == 13, "Year or filter count differs.")
        data = [r for r in reconciliation["objects"] if r["role"] == "data"]
        require(len(data) == 1, "Expected one data file.")
        item = data[0]
        raw = reconciliation_path.parent / item["storage_path"]
        record = item["object"]
        require(fingerprint(raw) == (record["sha256"], record["byte_count"]), "Raw bytes differ from S3 evidence.")
        for entry in reconciliation["objects"] + reconciliation["manifests"]:
            obj = entry["object"]
            require(bool(obj["version_id"]) and obj["verification"] == "version_get_sha256_and_length_match", "Missing version readback.")
        with raw.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            require(reader.fieldnames == receipt["schema_profile"]["native_headers"], "Changed header.")
            ids = set()
            for row in reader:
                require(None not in row and None not in row.values(), "Invalid CSV row width.")
                require(all(row[k] == v for k, v in selections.items()), "Row selections differ.")
                require(bool(row["fips"]) and row["fips"] not in ids, "Blank or duplicate FIPS.")
                ids.add(row["fips"])
        require(len(ids) == receipt["schema_profile"]["row_count"] and bool(ids), "Row count mismatch.")
        records.append(
            {
                "year": year,
                "rows": len(ids),
                "sha256": record["sha256"],
                "bytes": record["byte_count"],
                "s3": record,
                "completion_path": str(completion_path),
                "reconciliation_path": str(reconciliation_path),
            }
        )
    require(seen == set(args.years), "Missing or unexpected completed years.")
    result = {
        "measure_id": args.measure_id,
        "offered_and_stored_years": sorted(seen),
        "not_offered_years": [],
        "records": sorted(records, key=lambda r: r["year"]),
        "all_local_checks_passed": True,
        "s3_evidence": "version-specific readback at upload; no fresh S3 requests in this reconciliation",
        "model_eligible": False,
        "schema_definition_and_geographic_universe_review": "pending",
        "script_sha256": fingerprint(Path(__file__))[0],
        "python_version": sys.version,
        "reproduce": ".venv/bin/python -m scripts.acquisition.reconcile_mmd_history " + " ".join(sys.argv[1:]),
    }
    write_once(args.output, encoded_json(result))
    sys.stdout.write(json.dumps({"output": str(args.output), "years": sorted(seen), "rows": sum(r["rows"] for r in records), "status": "passed"}) + "\n")


if __name__ == "__main__":
    main()
