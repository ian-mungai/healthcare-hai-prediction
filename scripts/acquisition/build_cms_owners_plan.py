"""Write the locked plan for the 44 CMS Hospital All Owners releases, once, from the saved catalogue entries."""

import sys

from scripts.acquisition import cms_owners_contract as contract
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require

CATALOGUE_PATH = "data/acquisition_planning/cms_owners_20260929/cms_catalogue_entries_20260929.json"
HEADER_LINES_PATH = "data/acquisition_planning/cms_owners_20260929/release_header_lines.json"
APPROVAL = "All 44 releases; organisation owner rows only to S3; each original stays on this Mac only."


def releases(catalogue: dict, registry: dict, header_lines: dict | None = None) -> list[dict]:
    """Bind every catalogue CSV to its registry route and period; check recorded header lines when given."""
    source = next(s for s in registry["sources"] if s["source_id"] == contract.SOURCE_ID)
    routes = {r["url"]: r["route_id"] for r in source["file_routes"]}
    found = []
    for dataset in catalogue["datasets"]:
        for dist in dataset["distribution"]:
            url = dist.get("downloadURL", "")
            if not url.lower().endswith(".csv"):
                continue
            require(url in routes, "CMS owners catalogue file is not a registry route")
            temporal = dist.get("temporal") or dataset["temporal"]
            start, end = temporal.split("/") if isinstance(temporal, str) else (temporal[0]["startDate"], temporal[0]["endDate"])
            release = {"route_id": routes[url], "url": url, "period_start": start, "period_end": end, "title": dist.get("title") or dataset["title"]}
            release["layout"] = contract.layout_for(start)
            if header_lines is not None:
                require(header_lines[url].split(",") == contract.HEADERS[release["layout"]], "CMS owners recorded header differs from its layout")
            found.append(release | {"id": contract.release_id(release)})
    return sorted(found, key=lambda r: r["period_start"])


def build(catalogue: dict, header_lines: dict | None = None, expected: int = 44) -> dict:
    """The locked scope; no network access."""
    registry = load_registry()
    selected = releases(catalogue, registry, header_lines)
    require(len(selected) == expected, "CMS owners release count differs from the approval")
    return {
        "version": 1,
        "source_id": contract.SOURCE_ID,
        "scope": APPROVAL,
        "headers": contract.HEADERS,
        "dropped_columns": contract.DROPPED,
        "replacement": contract.REPLACEMENT,
        "catalogue": CATALOGUE_PATH,
        "registry_sha256": canonical_hash(registry),
        "model_eligible": False,
        "releases": selected,
    }


def main() -> None:
    """Write the plan and its lock write-once, then prove it loads."""
    plan = build(read_json(REPO_ROOT / CATALOGUE_PATH), read_json(REPO_ROOT / HEADER_LINES_PATH))
    write_once(contract.PLAN_PATH, encoded_json(plan))
    write_once(contract.PLAN_PATH.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    contract.load_plan()
    sys.stdout.write(f"{contract.PLAN_PATH.relative_to(REPO_ROOT)} {canonical_hash(plan)} {len(plan['releases'])} releases\n")


if __name__ == "__main__":
    main()
