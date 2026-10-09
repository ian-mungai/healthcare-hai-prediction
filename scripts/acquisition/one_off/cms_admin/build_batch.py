"""Build the bounded batch for the 62 approved CMS Hospital CHOW and Hospital Enrollments CSV releases.

Run from the repository root:

    PYTHONPATH=. .venv/bin/python scripts/acquisition/one_off/cms_admin/build_batch.py

Writes ``batch.json`` beside this file (write-once) from the locked source registry, the planning rules and the saved
CMS catalogue entries. No network access.
"""

import json
import sys
from dataclasses import asdict

from scripts.acquisition.plan_remaining import capture_plan
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require
from scripts.acquisition.transport import Limits

HERE = REPO_ROOT / "data/acquisition_planning/cms_admin_20260929"
LICENCE = "https://www.usa.gov/government-works"
APPROVAL = "All 62 CSV releases; privacy and terms review in data/acquisition_planning/route_recheck_20260929/findings.md."


def catalogue_periods() -> dict[str, tuple[str, str]]:
    """Map each CSV download URL to its catalogue title and temporal range."""
    periods = {}
    for dataset in read_json(HERE / "cms_catalogue_entries_20260929.json")["datasets"]:
        for dist in dataset["distribution"]:
            url = dist.get("downloadURL")
            if url and url.lower().endswith(".csv"):
                periods[url] = (dist["title"], dist["temporal"])
    return periods


def build() -> dict:
    """One job per approved data route, with catalogue periods and the verified licence."""
    registry = load_registry()
    rules = read_json(REPO_ROOT / "config/acquisition/planning_rules.json")
    periods = catalogue_periods()
    jobs = []
    for source_id in ("CMS_CHOW", "ENROLL"):
        source = next(s for s in registry["sources"] if s["source_id"] == source_id)
        for route in source["file_routes"]:
            if not route["url"].lower().endswith(".csv"):
                continue
            require(route["url"] in periods, f"CMS catalogue has no period for {route['route_id']}")
            title, temporal = periods[route["url"]]
            start, end = temporal.split("/")
            plan = capture_plan(source, route, rules)
            require(plan["role"] == "data" and plan["expected_format"] == "csv", f"Unexpected route role or format: {route['route_id']}")
            plan["scope"] = f"{APPROVAL} {plan['scope']}"
            plan["release"]["publisher_release_label"] = title
            plan["measurement_periods"] = [
                {
                    "label": f"CMS catalogue period for {title}",
                    "start_date": start,
                    "end_date": end,
                    "period_type": "unknown",
                    "source_basis": "publisher_stated",
                    "target_alignment": "unknown",
                }
            ]
            plan["governance"]["license_or_terms_url"] = LICENCE
            jobs.append({"job_id": route["route_id"].replace(":", "_"), "plan": plan, "references": [], "limits": asdict(Limits())})
    require(len(jobs) == 62, f"Expected 62 approved CSV routes, found {len(jobs)}")
    return {"batch_version": 1, "registry_sha256": canonical_hash(registry), "jobs": jobs}


def main() -> None:
    """Write the batch write-once and print its identity."""
    batch = build()
    write_once(HERE / "batch.json", encoded_json(batch))
    sys.stdout.write(json.dumps({"batch_sha256": canonical_hash(batch), "jobs": len(batch["jobs"])}) + "\n")


if __name__ == "__main__":
    main()
