"""Frozen HUD USPS ZIP-to-county crosswalk scope and immutable capture validation.

Plan 1 holds the 2021 Q1 pilot (type 2, query All). Plan 2 supplements it with 2021 Q2
through 2025 Q4. Ratios keep the publisher's direction and decimal text; nothing is
inverted, renormalized or collapsed.
"""

import csv
import io
import json
import re
from decimal import Decimal
from pathlib import Path

from scripts.acquisition.bls_api_contract import code_hashes, digest
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.data_paths import current
from scripts.acquisition.legacy_versions import with_predecessors
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require

ENDPOINT = "https://www.huduser.gov/hudapi/public/usps"
CREDENTIAL = "hud_api_key"
PLAN_PATH = REPO_ROOT / "config/acquisition/hud_api_plan.json"
RANGE_PLAN_PATH = REPO_ROOT / "config/acquisition/hud_api_plan_2021q2_2025q4.json"
VERSIONS_PATH = REPO_ROOT / "config/acquisition/hud_api_code_versions.json"
CROSSWALKS = {2: "zip-county"}
RATIOS = ("res_ratio", "bus_ratio", "oth_ratio", "tot_ratio")
# Every state and DC; territories are reported, not required.
STATE_FIPS = sorted(
    f"{n:02d}"
    for n in [1, 2, 4, 5, 6, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36]
    + [37, 38, 39, 40, 41, 42, 44, 45, 46, 47, 48, 49, 50, 51, 53, 54, 55, 56]
)
SUM_TOLERANCE = Decimal("0.01")
# HUD omits these descriptive fields from 2022 Q1; accept their
# absence as a whole-response layout, never fill them in. All other fields stay required.
OPTIONAL_FIELDS = ("city", "state")
# From 2024 Q2 HUD lists some Pacific ZIPs under a two-digit area code
# with no county (American Samoa, Micronesia, Marshall Islands, Palau). Keep them as published.
TERRITORY_GEOIDS = frozenset({"60", "64", "68", "70"})
ATTRIBUTION = "This product uses the HUD User Data API but is not endorsed or certified by HUD User."


def request_for(batch: dict) -> dict:
    """Public parameters only; the bearer token travels in a header, never the query."""
    require(batch["type"] in CROSSWALKS and batch["query"] == "All", "HUD crosswalk outside approved scope")
    return {"type": str(batch["type"]), "query": batch["query"], "year": str(batch["year"]), "quarter": str(batch["quarter"])}


def batch_id(batch: dict) -> str:
    """Identify one crosswalk quarter independently of its retrieval date."""
    return canonical_hash({"endpoint": ENDPOINT, "parameters": request_for(batch)})


def quarter_period(batch: dict) -> tuple[str, str]:
    """Calendar start and end dates of the batch's USPS address quarter."""
    year, quarter = batch["year"], batch["quarter"]
    require(quarter in {1, 2, 3, 4}, "HUD quarter invalid")
    end = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}[quarter]
    return f"{year}-{3 * quarter - 2:02d}-01", f"{year}-{end}"


def county_geography(batch: dict) -> str:
    """HUD publishes 2020 Census geographies beginning with the 2023 Q1 release."""
    return "2020_census" if (batch["year"], batch["quarter"]) >= (2023, 1) else "2010_census"


def require_code(hashes: dict | None = None) -> dict:
    """Require explicit review of new captures and retain trusted historical implementations."""
    actual = code_hashes() if hashes is None else hashes
    require(bool(actual) and actual in [v["code_sha256"] for v in read_code_versions(VERSIONS_PATH)["versions"]], "Unreviewed HUD code version")
    return actual


def load_plan(path: Path | None = None) -> dict:
    """Verify the locked scope, pinned fields, terms record and base registry."""
    path = PLAN_PATH if path is None else path
    plan = read_json(path)
    require(read_json(path.with_suffix(".lock.json"))["plan_sha256"] == canonical_hash(plan), "HUD plan lock differs")
    require(plan["source_id"] == "HUD" and plan["model_eligible"] is False, "HUD plan hold differs")
    require(plan["endpoint"] == ENDPOINT and plan["credential_reference"] == CREDENTIAL, "HUD endpoint or credential differs")
    require(plan["registry_sha256"] == canonical_hash(load_registry(expected_sha256=plan["registry_sha256"])), "HUD registry differs")
    require(plan["required_state_fips"] == STATE_FIPS, "HUD state coverage rule differs")
    require(plan["request_spacing_seconds"] >= 1, "HUD request spacing below the 60 per minute limit")
    terms = plan["terms"]
    require(digest((REPO_ROOT / current(terms["path"])).read_bytes()) == terms["sha256"], "HUD terms record changed")
    record = next(d for d in read_json(REPO_ROOT / current(terms["path"]))["datasets"] if d["source_id"] == "HUD")
    require(record["terms_accepted"] is True, "HUD terms not accepted")
    require({"zip", "geoid", *RATIOS} <= set(plan["result_fields"]) and plan["result_fields"] == sorted(set(plan["result_fields"])), "HUD result fields differ")
    require(bool(plan["batches"]), "HUD plan has no batches")
    for batch in plan["batches"]:
        require(batch["id"] == batch_id(batch), "HUD batch binding differs")
    if plan["version"] == 2:
        quarters = [(b["year"], b["quarter"]) for b in plan["batches"]]
        expected = [(y, q) for y in range(2021, 2026) for q in range(1, 5) if (2021, 2) <= (y, q) <= (2025, 4)]
        require(quarters == expected, "HUD range plan quarters differ")
        require(all(b["county_geography"] == county_geography(b) for b in plan["batches"]), "HUD county geography era differs")
    else:
        require(plan["version"] == 1 and [(b["year"], b["quarter"]) for b in plan["batches"]] == [(2021, 1)], "HUD pilot plan scope differs")
    return plan


def load_plans() -> list[dict]:
    """Both locked plans, with the range plan bound to the exact pilot plan it supplements."""
    plans = [load_plan(PLAN_PATH), load_plan(RANGE_PLAN_PATH)]
    require(plans[1]["supplements_plan_sha256"] == canonical_hash(plans[0]), "HUD plan chain differs")
    return plans


def layouts(plan: dict) -> list[list[str]]:
    """The pinned field set, and the same set without the optional descriptive fields."""
    return [plan["result_fields"], [f for f in plan["result_fields"] if f not in OPTIONAL_FIELDS]]


def parse(raw: bytes) -> dict:
    """Decode JSON numbers as decimals so ratio text and precision survive unchanged."""
    try:
        payload = json.loads(raw, parse_float=Decimal, parse_int=Decimal)
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise ValueError("HUD invalid JSON response") from None
    require(isinstance(payload, dict) and set(payload) == {"data"} and isinstance(payload["data"], dict), "HUD JSON envelope changed")
    return payload["data"]


def ratio_profile(rows: list[dict]) -> dict:
    """Count ZIPs whose ratio sums are complete, absent or outside rounding tolerance."""
    sums: dict[str, dict[str, Decimal]] = {}
    for row in rows:
        totals = sums.setdefault(row["zip"], dict.fromkeys(RATIOS, Decimal(0)))
        for name in RATIOS:
            totals[name] += row[name]
    profile = {}
    for name in RATIOS:
        values = [t[name] for t in sums.values()]
        profile[name] = {
            "zips_summing_to_one": sum(abs(v - 1) <= SUM_TOLERANCE for v in values),
            "zips_with_zero_weight": sum(v == 0 for v in values),
            "zips_outside_tolerance": sum(v != 0 and abs(v - 1) > SUM_TOLERANCE for v in values),
        }
    return profile


def validate_response(raw: bytes, batch: dict, plan: dict) -> tuple[bytes, dict]:
    """Write a lossless CSV while refusing scope, schema or geographic incompleteness."""
    data = parse(raw)
    require(sorted(data) == plan["envelope_fields"], "HUD data envelope fields differ; possible pagination or schema change")
    require(str(data["year"]) == str(batch["year"]) and str(data["quarter"]) == str(batch["quarter"]), "HUD response period differs")
    require(data["input"] == batch["query"] and data["crosswalk_type"] == CROSSWALKS[batch["type"]], "HUD response scope differs")
    results = data["results"]
    require(isinstance(results, list) and bool(results), "HUD results missing")
    fields = sorted(results[0]) if isinstance(results[0], dict) else []
    require(fields in layouts(plan), "HUD result fields differ")
    seen, rows = set(), []
    for row in results:
        require(isinstance(row, dict) and sorted(row) == fields, "HUD result fields differ")
        require(all(isinstance(row[k], str) for k in fields if k not in RATIOS), "HUD text cell type changed")
        require(all(isinstance(row[k], Decimal) and 0 <= row[k] <= 1 for k in RATIOS), "HUD ratio invalid")
        require(
            re.fullmatch(r"[0-9]{5}", row["zip"]) is not None and (re.fullmatch(r"[0-9]{5}", row["geoid"]) is not None or row["geoid"] in TERRITORY_GEOIDS),
            "HUD ZIP or county identifier invalid",
        )
        key = (row["zip"], row["geoid"])
        require(key not in seen, "Duplicate ZIP-county row in HUD response")
        seen.add(key)
        rows.append(row)
    states = {g[:2] for _, g in seen}
    require(set(STATE_FIPS) <= states, "HUD state coverage incomplete")
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(fields)
    writer.writerows([str(r[k]) for k in fields] for r in sorted(rows, key=lambda r: (r["zip"], r["geoid"])))
    return output.getvalue().encode(), {
        "rows": len(rows),
        "zips": len({z for z, _ in seen}),
        "counties": len({g for _, g in seen if len(g) == 5}),
        "states_and_territories": sorted(states),
        "ratio_sums": ratio_profile(rows),
        "direction": "zip_to_county",
        "model_eligible": False,
    }


def verify_capture(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Check every artifact, public request, terms binding and preserved hold."""
    from scripts.acquisition.capture import receipt_validator

    require(not evidence_only and source["source_id"] == "HUD", "HUD storage source differs")
    plans = with_predecessors({canonical_hash(p): p for p in load_plans()})
    require(lineage["plan_sha256"] in plans, "HUD capture plan differs")
    plan = plans[lineage["plan_sha256"]]
    require(lineage["registry_sha256"] == canonical_hash(load_registry(expected_sha256=lineage["registry_sha256"])), "HUD registry differs")
    require(lineage["schema_sha256"] == canonical_hash(receipt_validator().schema), "HUD schema differs")
    require(lineage["terms_sha256"] == plan["terms"]["sha256"], "HUD terms binding differs")
    require(lineage["model_eligible"] is False and receipt["snapshot_status"] == "acquired_unvalidated", "HUD modeling hold differs")
    require_code(lineage["code_sha256"])
    matches = [b for b in plan["batches"] if b["id"] == lineage["batch_id"]]
    require(len(matches) == 1, "HUD capture batch not in its plan")
    batch = matches[0]
    acq = receipt["acquisition"]
    require(acq["requested_url"] == acq["resolved_url"] == ENDPOINT and acq["request_method"] == "GET", "HUD request endpoint differs")
    require(acq["request_parameters"] == request_for(batch) and acq["http_status"] == 200, "HUD saved request differs")
    require(acq["pagination"]["page_count"] == 1 and acq["pagination"]["termination_verified"] is True, "HUD coverage evidence differs")
    roles = {a["role"]: a for a in receipt["artifacts"]}
    require(len(roles) == len(receipt["artifacts"]) == 4 and set(roles) == {"api_page", "data", "layout", "export_receipt"}, "HUD artifact set differs")
    raw = (root / roles["api_page"]["storage_path"]).read_bytes()
    require(digest(raw) == lineage["expected_sha256"], "HUD raw hash differs")
    derived, statistics = validate_response(raw, batch, plan)
    require((root / roles["data"]["storage_path"]).read_bytes() == derived, "HUD derived CSV differs")
    require(canonical_hash(read_json(root / roles["layout"]["storage_path"])) == lineage["plan_sha256"], "HUD stored scope differs")
    proof = read_json(root / roles["export_receipt"]["storage_path"])
    require(proof["statistics"] == statistics and proof["request"] == request_for(batch), "HUD proof differs")
    require(proof["sha256"] == digest(raw) and proof["bytes"] == len(raw) and proof["http_status"] == 200, "HUD transport proof differs")
    require(proof["endpoint"] == ENDPOINT and proof["retrieved_at_utc"] == acq["retrieved_at_utc"], "HUD retrieval proof differs")
    require(
        receipt["schema_profile"]["row_count"] == statistics["rows"]
        and receipt["schema_profile"]["native_headers"] in layouts(plan)
        and derived.decode().split("\n", 1)[0].split(",") == receipt["schema_profile"]["native_headers"],
        "HUD schema profile differs",
    )
    require(receipt["governance"]["credential_reference"] == CREDENTIAL and ATTRIBUTION in receipt["governance"]["use_restrictions"], "HUD governance differs")
    start, end = quarter_period(batch)
    period = receipt["measurement_periods"]
    require(len(period) == 1 and period[0]["start_date"] == start and period[0]["end_date"] == end, "HUD measurement quarter differs")
