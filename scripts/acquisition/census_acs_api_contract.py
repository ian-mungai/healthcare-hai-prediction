"""Frozen Census 2009 profile scope, native tokens and immutable capture validation.

Plan 1 holds DP02-DP05 for every 2009 county. Plan 2 supplements it with DP02PR, the
Puerto Rico social profile, because DP02 publishes no values for Puerto Rico.
"""

import csv
import io
import json
import re
from pathlib import Path

from scripts.acquisition.bls_api_contract import code_hashes, digest
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require

ENDPOINT = "https://api.census.gov/data/2009/acs/acs5/profile"
TABLES = ["DP02", "DP03", "DP04", "DP05"]
PR_TABLES = ["DP02PR"]
PUERTO_RICO = "72"
PLAN_SCOPES = {1: TABLES, 2: PR_TABLES}
PLAN_PATH = REPO_ROOT / "config/acquisition/census_acs_api_plan.json"
PR_PLAN_PATH = REPO_ROOT / "config/acquisition/census_acs_api_plan_dp02pr.json"
VERSIONS_PATH = REPO_ROOT / "config/acquisition/census_acs_api_code_versions.json"
NULL_MARKER = "\\N"


def request_for(batch: dict) -> dict:
    """Public parameters only; group calls include estimates, margins and annotations."""
    require(batch["table"] in TABLES + PR_TABLES, "Census table outside approved scope")
    states = PUERTO_RICO if batch["table"] in PR_TABLES else "*"
    return {"get": f"group({batch['table']})", "for": "county:*", "in": f"state:{states}"}


def batch_id(batch: dict) -> str:
    """Identify one table's fixed 2009 county request independently of its retrieval date."""
    return canonical_hash({"endpoint": ENDPOINT, "parameters": request_for(batch)})


def require_code(hashes: dict | None = None) -> dict:
    """Require explicit review of new captures and retain trusted historical implementations."""
    actual = code_hashes() if hashes is None else hashes
    require(bool(actual) and actual in [v["code_sha256"] for v in read_code_versions(VERSIONS_PATH)["versions"]], "Unreviewed Census code version")
    return actual


def load_plan(path: Path | None = None) -> dict:
    """Verify frozen metadata, county enumeration and the plan version's table scope."""
    path = PLAN_PATH if path is None else path
    plan = read_json(path)
    require(read_json(path.with_suffix(".lock.json"))["plan_sha256"] == canonical_hash(plan), "Census plan lock differs")
    require(plan["source_id"] == "ACS" and plan["model_eligible"] is False, "Census plan hold differs")
    require(plan["endpoint"] == ENDPOINT and plan["credential_reference"] == "census_api_key", "Census endpoint or credential differs")
    require(plan["year"] == 2009 and plan["window"] == [2005, 2009], "Census product window differs")
    require(plan["registry_sha256"] == canonical_hash(load_registry(expected_sha256=plan["registry_sha256"])), "Census registry differs")
    ids = plan["county_ids"]
    require(ids == sorted(set(ids)) and bool(ids) and all(re.fullmatch(r"[0-9]{5}", s) for s in ids), "Census county universe differs")
    require(any(i.startswith(PUERTO_RICO) for i in ids), "Census Puerto Rico scope missing")
    if plan["version"] == 2:
        require(all(i.startswith(PUERTO_RICO) for i in ids), "Census Puerto Rico plan scope differs")
    for ref in plan["references"]:
        require(digest(Path(ref["path"]).read_bytes()) == ref["sha256"], "Census reference changed")
    require([b["table"] for b in plan["batches"]] == PLAN_SCOPES.get(plan["version"]), "Census table scope differs")
    for batch in plan["batches"]:
        require(batch["id"] == batch_id(batch) and batch["metadata"] in plan["references"], "Census batch binding differs")
        variables = read_json(Path(batch["metadata"]["path"]))["variables"]
        require(batch["headers"] == sorted([*variables, "state", "county"]), "Census metadata headers differ")
        table = batch["table"]
        require({"GEO_ID", "NAME"} <= variables.keys(), "Census metadata geography missing")
        require(
            all(v in {"GEO_ID", "NAME"} or re.fullmatch(rf"{table}_[0-9]{{4}}(?:E|M|PE|PM|EA|MA|PEA|PMA)", v) for v in variables),
            "Census metadata fields changed",
        )
        for var in variables:
            if re.fullmatch(rf"{table}_[0-9]{{4}}E", var):
                require(all(var[:-1] + suffix in variables for suffix in ("M", "PE", "PM", "EA", "MA", "PEA", "PMA")), "Census margin or annotation missing")
    return plan


def load_plans() -> list[dict]:
    """Both locked plans, with the Puerto Rico plan bound to the exact plan it supplements."""
    plans = [load_plan(PLAN_PATH), load_plan(PR_PLAN_PATH)]
    require(plans[1]["supplements_plan_sha256"] == canonical_hash(plans[0]), "Census plan chain differs")
    return plans


def validate_response(raw: bytes, batch: dict, counties: list[str]) -> tuple[bytes, dict]:
    """Reconstruct a lossless CSV while refusing any geographic or schema incompleteness."""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        raise ValueError("Census invalid JSON response") from None
    require(isinstance(payload, list) and len(payload) > 1 and isinstance(payload[0], list), "Census JSON array schema changed")
    headers = payload[0]
    require(all(isinstance(h, str) for h in headers) and len(set(headers)) == len(headers) and sorted(headers) == batch["headers"], "Census headers differ")
    positions = [headers.index(h) for h in batch["headers"]]
    seen, records = set(), []
    nulls = negative = annotations = 0
    for row in payload[1:]:
        require(isinstance(row, list) and len(row) == len(headers), "Census row width changed")
        require(all(v is None or isinstance(v, str) for v in row), "Census native cell type changed")
        require(NULL_MARKER not in row, "Census native null marker collision")
        data = dict(zip(headers, row, strict=True))
        state, county = data["state"], data["county"]
        require(isinstance(state, str) and re.fullmatch(r"[0-9]{2}", state) is not None, "Census state geography invalid")
        require(isinstance(county, str) and re.fullmatch(r"[0-9]{3}", county) is not None, "Census county geography invalid")
        identity = state + county
        require(data["GEO_ID"] == f"0500000US{identity}" and isinstance(data["NAME"], str) and bool(data["NAME"]), "Census native geography mismatch")
        require(identity not in seen, "Duplicate county in Census response")
        seen.add(identity)
        nulls += sum(v is None for v in row)
        negative += sum(isinstance(v, str) and v.startswith("-") for v in row)
        annotations += sum(h.endswith("A") and v not in {None, ""} for h, v in zip(headers, row, strict=True))
        records.append((identity, [NULL_MARKER if row[i] is None else row[i] for i in positions]))
    require(seen == set(counties), "Census county coverage differs from pinned 2009 geography")
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(batch["headers"])
    writer.writerows(row for _, row in sorted(records))
    return output.getvalue().encode(), {
        "rows": len(records),
        "columns": len(headers),
        "null_cells": nulls,
        "negative_tokens": negative,
        "nonempty_annotations": annotations,
        "puerto_rico_counties": sum(i.startswith("72") for i in seen),
        "model_eligible": False,
    }


def verify_capture(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Check every artifact, public request, exact metadata and all preserved holds."""
    from scripts.acquisition.capture import receipt_validator

    require(not evidence_only and source["source_id"] == "ACS", "Census storage source differs")
    plans = {canonical_hash(p): p for p in load_plans()}
    require(lineage["plan_sha256"] in plans, "Census capture plan differs")
    plan = plans[lineage["plan_sha256"]]
    require(lineage["registry_sha256"] == canonical_hash(load_registry(expected_sha256=lineage["registry_sha256"])), "Census registry differs")
    require(lineage["schema_sha256"] == canonical_hash(receipt_validator().schema), "Census schema differs")
    require(lineage["model_eligible"] is False and receipt["snapshot_status"] == "acquired_unvalidated", "Census modeling hold differs")
    require_code(lineage["code_sha256"])
    matches = [b for b in plan["batches"] if b["id"] == lineage["batch_id"]]
    require(len(matches) == 1, "Census capture batch not in its plan")
    batch = matches[0]
    acq = receipt["acquisition"]
    require(acq["requested_url"] == acq["resolved_url"] == ENDPOINT and acq["request_method"] == "GET", "Census request endpoint differs")
    require(acq["request_parameters"] == request_for(batch) and acq["http_status"] == 200, "Census saved request differs")
    require(acq["pagination"]["page_count"] == 1 and acq["pagination"]["termination_verified"] is True, "Census coverage evidence differs")
    roles = {a["role"]: a for a in receipt["artifacts"]}
    require(
        len(roles) == len(receipt["artifacts"]) == 5 and set(roles) == {"api_page", "data", "dictionary", "layout", "export_receipt"},
        "Census artifact set differs",
    )
    raw = (root / roles["api_page"]["storage_path"]).read_bytes()
    require(digest(raw) == lineage["expected_sha256"], "Census raw hash differs")
    derived, statistics = validate_response(raw, batch, plan["county_ids"])
    require((root / roles["data"]["storage_path"]).read_bytes() == derived, "Census derived CSV differs")
    require(digest((root / roles["dictionary"]["storage_path"]).read_bytes()) == batch["metadata"]["sha256"], "Census stored dictionary differs")
    require(read_json(root / roles["layout"]["storage_path"]) == plan, "Census stored scope differs")
    proof = read_json(root / roles["export_receipt"]["storage_path"])
    require(proof["statistics"] == statistics and proof["request"] == request_for(batch), "Census proof differs")
    require(proof["response_sha256"] == digest(raw) and proof["endpoint"] == ENDPOINT, "Census response proof differs")
    require(proof["sha256"] == digest(raw) and proof["bytes"] == len(raw) and proof["http_status"] == 200, "Census transport proof differs")
    require(proof["retrieved_at_utc"] == acq["retrieved_at_utc"], "Census retrieval timestamp differs")
    require(
        receipt["schema_profile"]["row_count"] == statistics["rows"] and receipt["schema_profile"]["native_headers"] == batch["headers"],
        "Census schema profile differs",
    )
    require(receipt["governance"]["credential_reference"] == "census_api_key", "Census credential reference differs")
    period = receipt["measurement_periods"]
    require(len(period) == 1 and period[0]["start_date"] == "2005-01-01" and period[0]["end_date"] == "2009-12-31", "Census measurement window differs")
