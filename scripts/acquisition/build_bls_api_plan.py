"""Build deterministic BLS county candidate batches from verified stored Census references."""

import argparse
import re
import sys
from pathlib import Path

from scripts.acquisition.bls_api_contract import PLAN_PATH, batch_id, digest
from scripts.acquisition.capture import receipt_validator, validate_receipt
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require


def build(references: list[Path], vintage: str) -> dict:
    """Bind native Census IDs to source hashes without claiming BLS publication completeness."""
    counties, inputs = set(), []
    for path in sorted(references):
        receipt_path = path.parent.parent / "receipt.json"
        receipt = read_json(receipt_path)
        validate_receipt(receipt, receipt_validator(), receipt_path.parent)
        require(receipt["source"]["source_record_id"] == "ADJ", "County reference must be a captured Census adjacency source")
        raw = path.read_bytes()
        delimiter = b"|" if b"|" in raw.splitlines()[0] else b"\t"
        # Only identifier fields are decoded; historic publisher names use mixed encodings.
        rows = [line.split(delimiter) for line in raw.splitlines()]
        native = {cell.decode("ascii") for row in rows for cell in row[1:4:2] if re.fullmatch(rb"[0-9]{5}", cell)}
        require(len(native) >= 3000, "County reference is incomplete")
        counties.update(native)
        inputs.append(
            {"path": str(path.relative_to(REPO_ROOT)), "sha256": digest(raw), "url": receipt["acquisition"]["requested_url"], "county_count": len(native)}
        )
    require(bool(inputs), "At least one verified county reference is required")
    series = sorted(f"LAUCN{fips}00000000{measure:02}" for fips in counties for measure in range(3, 7))
    batches = []
    for start, end in ((2006, 2025), (1990, 2005)):
        for offset in range(0, len(series), 50):
            batch = {"series_ids": series[offset : offset + 50], "start_year": start, "end_year": end}
            batches.append(batch | {"id": batch_id(batch)})
    return {
        "version": 1,
        "source_id": "BLS",
        "vintage": vintage,
        "endpoint": "https://api.bls.gov/publicAPI/v2/timeseries/data/",
        "credential_reference": "bls_api_key",
        "request_limit_24h": 450,
        "request_spacing_seconds": 1,
        "registry_sha256": canonical_hash(load_registry()),
        "references": inputs,
        "county_count": len(counties),
        "series_count": len(series),
        "batches": batches,
        "model_eligible": False,
        "review_status": "all_existing_source_and_measure_holds_retained",
        "enumeration_status": "Census candidate union; BLS la.series returned HTTP 403; historical BLS completeness unverified",
        "classification": "Public",
        "credentials_classification": "Restricted_runtime_only",
        "quota_boundary": "Conservative local rolling 24h cap leaves 50 requests for pilots/shared usage; other clients remain unobserved",
    }


def main() -> None:
    """Write immutable plan and lock; identical inputs reproduce identical bytes."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, action="append", required=True)
    parser.add_argument("--vintage", required=True)
    args = parser.parse_args()
    plan = build([p.resolve() for p in args.reference], args.vintage)
    write_once(PLAN_PATH, encoded_json(plan))
    write_once(PLAN_PATH.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    sys.stdout.write(f"{plan['county_count']} candidate counties; {plan['series_count']} series; {len(plan['batches'])} batches\n")


if __name__ == "__main__":
    main()
