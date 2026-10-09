"""Pin 2009 profile dictionaries and county coverage from stored Census geography files."""

import argparse
import re
import sys
from dataclasses import asdict
from pathlib import Path

from scripts.acquisition import census_acs_api_contract as contract
from scripts.acquisition.capture import receipt_validator, validate_receipt
from scripts.acquisition.data_paths import current
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require
from scripts.acquisition.transport import Limits, download

REFERENCES = REPO_ROOT / "data/datasets/historical_acquisition/census_acs_api_history/20260926/references"


def reference(path: Path, url: str) -> dict:
    """Bind a repository-relative immutable input and its public origin."""
    return {"path": str(path.relative_to(REPO_ROOT)), "sha256": contract.digest(path.read_bytes()), "url": url}


def geography() -> list[tuple[list[dict], set[str]]]:
    """Per stored geography file: its references and native summary-level-050 county IDs."""
    files = sorted((REPO_ROOT / "data/datasets/historical_acquisition/acs_2009_history").glob("jobs/*/captures/ACS/*/raw/g20095*.txt"))
    require(len(files) == 53, "Expected 2009 geography files for 50 states, DC, PR and US")
    result, counties = [], set()
    for path in files:
        found = set()
        receipt_path = path.parent.parent / "receipt.json"
        receipt = read_json(receipt_path)
        validate_receipt(receipt, receipt_validator(), path.parent.parent)
        require(receipt["source"]["source_record_id"] == "ACS", "Geography source differs")
        url = receipt["acquisition"]["requested_url"]
        require(url.startswith("https://www2.census.gov/programs-surveys/acs/summary_file/2009/") and url.endswith(path.name), "Geography vintage differs")
        for line in path.read_text(encoding="latin1").splitlines():
            if line[8:13] == "05000":
                native = re.findall(r"05000US([0-9]{5})(?![0-9])", line)
                require(len(native) == 1 and native[0] not in counties, "Geography duplicate or malformed county")
                counties.add(native[0])
                found.add(native[0])
        result.append(([reference(path, url), reference(receipt_path, url)], found))
    states = {c[:2] for c in counties}
    require(len(counties) == 3221 and len(states) == 52 and sum(c.startswith("72") for c in counties) == 78, "Inspected 2009 county scope differs")
    return result


def dictionary(table: str) -> tuple[list[dict], dict]:
    """Bind one stored group dictionary and its download receipt to a batch."""
    result = read_json(REFERENCES / table / "receipt.json")
    url = f"{contract.ENDPOINT}/groups/{table}.json"
    require(result["complete"] and result["requested_url"] == result["resolved_url"] == url, "Dictionary transport differs")
    path = REPO_ROOT / current(result["path"])
    require(contract.digest(path.read_bytes()) == result["sha256"], "Dictionary bytes differ")
    ref = reference(path, url)
    variables = read_json(path)["variables"]
    batch = {"table": table, "headers": sorted([*variables, "state", "county"]), "metadata": ref}
    return [ref, reference(REFERENCES / table / "receipt.json", url)], batch | {"id": contract.batch_id(batch)}


def fetch_dictionary(table: str) -> None:
    """Download a missing group dictionary once, without a key; an existing receipt is never replaced."""
    receipt = REFERENCES / table / "receipt.json"
    if receipt.exists():
        return
    result = download(f"{contract.ENDPOINT}/groups/{table}.json", REFERENCES / table, "group.json", "json", "dictionary", Limits())
    require(result.complete and result.failure is None and not result.partial and result.http_status == 200, "Dictionary download incomplete")
    write_once(receipt, encoded_json(asdict(result) | {"path": str(result.path.relative_to(REPO_ROOT))}))


def build() -> dict:
    """Plan 1: DP02-DP05 for every 2009 county, from native summary-level-050 rows."""
    references, counties = [], set()
    for refs, found in geography():
        references.extend(refs)
        counties |= found
    batches = []
    for table in contract.TABLES:
        refs, batch = dictionary(table)
        references.extend(refs)
        batches.append(batch)
    return {
        "version": 1,
        "source_id": "ACS",
        "endpoint": contract.ENDPOINT,
        "credential_reference": "census_api_key",
        "year": 2009,
        "window": [2005, 2009],
        "vintage": "20260926",
        "county_ids": sorted(counties),
        "references": references,
        "batches": batches,
        "model_eligible": False,
        "registry_sha256": canonical_hash(load_registry()),
        "request_spacing_seconds": 1,
        "scope": "All published DP02-DP05 variables; all 2009 counties in the United States and Puerto Rico",
        "classification": "Public aggregate data; Restricted runtime-only API credential",
        "csv_null_marker": contract.NULL_MARKER,
        "review_status": "All definition, geography, precision and modeling holds retained",
        "approval": "Separate Census collector, pilot, verification and S3 acquisition",
        "county_enumeration": "Native 05000US identifiers from hash-verified stored 2009 ACS summary-file geography; no later geography substituted",
    }


def build_pr(base_plan: dict) -> dict:
    """Plan 2: DP02PR for the 78 Puerto Rico municipios, bound to the unchanged plan 1."""
    references, counties = [], set()
    for refs, found in geography():
        if any(c.startswith(contract.PUERTO_RICO) for c in found):
            require(all(c.startswith(contract.PUERTO_RICO) for c in found), "Puerto Rico geography file mixes states")
            references.extend(refs)
            counties |= found
    require(len(counties) == 78, "Inspected 2009 Puerto Rico scope differs")
    batches = []
    for table in contract.PR_TABLES:
        refs, batch = dictionary(table)
        references.extend(refs)
        batches.append(batch)
    return {
        "version": 2,
        "source_id": "ACS",
        "endpoint": contract.ENDPOINT,
        "credential_reference": "census_api_key",
        "year": 2009,
        "window": [2005, 2009],
        "vintage": base_plan["vintage"],
        "county_ids": sorted(counties),
        "references": references,
        "batches": batches,
        "model_eligible": False,
        "registry_sha256": canonical_hash(load_registry()),
        "request_spacing_seconds": 1,
        "supplements_plan_sha256": canonical_hash(base_plan),
        "scope": "All published DP02PR variables; all 78 2009 Puerto Rico municipios",
        "classification": base_plan["classification"],
        "csv_null_marker": contract.NULL_MARKER,
        "review_status": "All definition, geography, precision and modeling holds retained",
        "approval": "Add DP02PR after DP02 was found to publish no Puerto Rico values",
        "county_enumeration": "Puerto Rico subset of the plan 1 native 2009 summary-file geography; no later geography substituted",
    }


def main() -> None:
    """Write stable plans once; rerunning verifies identical bytes."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch-dictionary", action="store_true", help="download missing group dictionaries (keyless public metadata)")
    args = parser.parse_args()
    if args.fetch_dictionary:
        for table in contract.PR_TABLES:
            fetch_dictionary(table)
    plan = build()
    pr_plan = build_pr(plan)
    for path, body in [(contract.PLAN_PATH, plan), (contract.PR_PLAN_PATH, pr_plan)]:
        write_once(path, encoded_json(body))
        write_once(path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(body)}))
    contract.load_plans()
    sys.stdout.write(f"Pinned {len(plan['county_ids'])} counties for DP02-DP05 and {len(pr_plan['county_ids'])} Puerto Rico municipios for DP02PR\n")


if __name__ == "__main__":
    main()
