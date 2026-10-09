"""Write the locked plan for the ACS detailed tables behind the derived poverty and limited-English history, once."""

import sys
from pathlib import Path

from scripts.acquisition import census_acs_detailed_contract as contract
from scripts.acquisition.bls_api_contract import digest
from scripts.acquisition.data_paths import current
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require

REFERENCES = REPO_ROOT / "data/datasets/historical_acquisition/census_acs_detailed/references"
BROWSER = "data/datasets/historical_acquisition/acs_browser_history"
TABLE_YEARS = {"B17001": list(range(2010, 2017)), "B16001": list(range(2009, 2016)), "B16004": list(range(2009, 2016))}
DERIVED_YEARS = {"poverty": [2010, 2011], "english": list(range(2009, 2016))}
COMPARISONS = [("poverty", y, f"ACSST5Y{y}.S1701-Data.csv") for y in range(2012, 2017)] + [("english", y, None) for y in range(2009, 2016)]
DEFINITIONS = {
    "poverty": "C175: B17001_002E / B17001_001E x 100 (below poverty / population for whom poverty status is determined); not a published S1701 value",
    "english": "C183.02 total: B16001 'less than very well' lines / B16001_001E x 100 (age 5+); checked against B16004; not a published C16001 value",
}
APPROVAL = "Poverty 2010-2011 from B17001 (checked vs S1701 2012-2016); limited English 2009-2015 from B16001 (vs B16004)."


def reference(path: Path, url: str) -> dict:
    """Bind a repository-relative immutable input and its public origin."""
    inside = path.is_relative_to(REPO_ROOT)
    return {"path": str(path.relative_to(REPO_ROOT)) if inside else str(path), "sha256": digest(path.read_bytes()), "url": url}


def dictionary(folder: Path, table: str, year: int) -> tuple[dict, dict]:
    """One pinned group dictionary and its batch."""
    receipt = read_json(folder / f"{table}_{year}" / "receipt.json")
    url = f"{contract.endpoint(year)}/groups/{table}.json"
    require(receipt["complete"] and receipt["requested_url"] == url, "Census detailed dictionary transport differs")
    path = REPO_ROOT / current(receipt["path"])
    require(digest(path.read_bytes()) == receipt["sha256"], "Census detailed dictionary bytes differ")
    ref = reference(path, url)
    variables = read_json(path)["variables"]
    batch = {"table": table, "year": year, "headers": sorted([*variables, "state", "county"]), "metadata": ref}
    if table in {"B16001", "B16004"}:
        batch["less_lines"] = contract.less_lines(table, variables)
    return ref, batch | {"id": contract.batch_id(batch)}


def comparison_file(name: str, browser: Path) -> Path:
    """The single stored history member with this name (not a mapped copy)."""
    found = sorted(p for p in browser.rglob(name) if "history_members" in p.parts)
    require(len(found) == 1, "Census detailed comparison file not unique")
    return found[0]


def build(
    references: Path = REFERENCES,
    table_years: dict | None = None,
    derived_years: dict | None = None,
    comparisons: list | None = None,
    browser: Path | None = None,
) -> dict:
    """The locked scope; no network access."""
    refs, batches = [], []
    for table, years in (table_years or TABLE_YEARS).items():
        for year in years:
            ref, batch = dictionary(references, table, year)
            refs.append(ref)
            batches.append(batch)
    checks = []
    for kind, year, name in comparisons or COMPARISONS:
        if name is None:
            checks.append({"kind": kind, "year": year, "against": "B16004"})
            continue
        path = comparison_file(name, browser or REPO_ROOT / BROWSER)
        ref = reference(path, "stored data.census.gov export")
        refs.append(ref)
        checks.append({"kind": kind, "year": year, "file": ref})
    return {
        "version": 1,
        "source_id": "ACS",
        "credential_reference": "census_api_key",
        "scope": APPROVAL,
        "definitions": DEFINITIONS,
        "derived_years": derived_years or DERIVED_YEARS,
        "comparisons": checks,
        "references": refs,
        "batches": batches,
        "request_spacing_seconds": 1,
        "registry_sha256": canonical_hash(load_registry()),
        "model_eligible": False,
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
