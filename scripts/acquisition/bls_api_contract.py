"""Validate locked BLS batch scope, native responses and reproducible capture evidence."""

import csv
import hashlib
import io
import json
import re
from pathlib import Path

from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.data_paths import current
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, read_json, require

PLAN_PATH = REPO_ROOT / "config/acquisition/bls_api_plan.json"
VERSIONS_PATH = REPO_ROOT / "config/acquisition/bls_api_code_versions.json"
FIELDS = ["seriesID", "county_fips", "measure_code", "year", "period", "periodName", "value", "footnotes"]


def digest(body: bytes) -> str:
    """Return a complete byte SHA-256."""
    return hashlib.sha256(body).hexdigest()


def code_hashes() -> dict[str, str]:
    """Bind acquisition dependencies and configuration loader, excluding test-only runners."""
    paths = sorted((REPO_ROOT / "scripts/acquisition").glob("*.py"))
    paths += [REPO_ROOT / "scripts/infrastructure/render_project_config.py"]
    return {str(p.relative_to(REPO_ROOT)): digest(p.read_bytes()) for p in paths if "e2e" not in p.name}


def require_code(hashes: dict | None = None) -> dict:
    """Accept only an explicitly reviewed implementation version."""
    actual = code_hashes() if hashes is None else hashes
    versions = read_code_versions(VERSIONS_PATH)["versions"]
    require(bool(actual) and actual in [v["code_sha256"] for v in versions], "Unreviewed BLS code version")
    return actual


def request_for(batch: dict) -> dict:
    """Return the exact public request fields; credentials are never part of this record."""
    return {
        "seriesid": batch["series_ids"],
        "startyear": str(batch["start_year"]),
        "endyear": str(batch["end_year"]),
        "annualaverage": True,
        "catalog": False,
        "calculations": False,
        "aspects": False,
    }


def batch_id(batch: dict) -> str:
    """Identify one fixed, sorted series/window request."""
    return canonical_hash(request_for(batch))


def load_plan() -> dict:
    """Check plan lock, explicit scope and immutable county enumeration inputs."""
    plan = read_json(PLAN_PATH)
    lock = read_json(PLAN_PATH.with_suffix(".lock.json"))
    require(lock["plan_sha256"] == canonical_hash(plan), "BLS plan lock differs")
    require(plan["source_id"] == "BLS" and plan["model_eligible"] is False, "BLS plan hold differs")
    require(plan["endpoint"] == "https://api.bls.gov/publicAPI/v2/timeseries/data/", "BLS endpoint differs")
    require(plan["credential_reference"] == "bls_api_key", "BLS secret reference differs")
    require(1 <= plan["request_limit_24h"] <= 450, "BLS quota must reserve shared-key headroom")
    identities = set()
    for batch in plan["batches"]:
        ids = batch["series_ids"]
        require(1 <= len(ids) <= 50 and ids == sorted(set(ids)), "BLS series scope differs")
        require(all(re.fullmatch(r"LAUCN[0-9]{5}000000000[3-6]", item) for item in ids), "BLS county series invalid")
        require(1990 <= batch["start_year"] <= batch["end_year"] <= 2025, "BLS years outside approved history")
        require(batch["end_year"] - batch["start_year"] < 20, "BLS window exceeds 20 years")
        require(batch["id"] == batch_id(batch) and batch["id"] not in identities, "BLS batch identity differs")
        identities.add(batch["id"])
    require(bool(identities), "BLS plan is empty")
    for ref in plan["references"]:
        require(digest(Path(current(ref["path"])).read_bytes()) == ref["sha256"], "BLS county reference differs")
    return plan


def validate_response(raw: bytes, batch: dict) -> tuple[bytes, dict]:
    """Preserve native tokens and footnotes while rejecting incomplete or altered scope."""
    payload = json.loads(raw)
    require(payload.get("status") == "REQUEST_SUCCEEDED", "BLS request was not successful")
    messages = payload.get("message")
    require(isinstance(messages, list) and all(isinstance(m, str) for m in messages), "BLS message schema changed")
    results = payload.get("Results")
    require(isinstance(results, dict) and isinstance(results.get("series"), list), "BLS results schema changed")
    series = results["series"]
    require(all(isinstance(s, dict) and isinstance(s.get("seriesID"), str) for s in series), "BLS series schema changed")
    require(sorted(s["seriesID"] for s in series) == batch["series_ids"], "BLS missing, duplicate or extra series")
    unavailable, recognized, records = [], [], []
    expected = {(str(y), f"M{p:02}") for y in range(batch["start_year"], batch["end_year"] + 1) for p in range(1, 14)}
    missing_values = footnoted = available_series = 0
    for item in series:
        sid, data = item["seriesID"], item.get("data")
        require(isinstance(data, list), "BLS missing data array")
        if not data and f"Series does not exist for Series {sid}" in messages:
            # Only the exact observed missing-series message is an accepted empty result.
            message = f"Series does not exist for Series {sid}"
            require(message in messages, "BLS empty series lacks reviewed absence evidence")
            recognized.append(message)
            unavailable.append({"series_id": sid, "status": "availability_hold", "message": message})
            continue
        available_series += bool(data)
        seen = set()
        for row in data:
            require(isinstance(row, dict) and {"year", "period", "periodName", "value", "footnotes"} <= set(row), "BLS row schema changed")
            require(set(row) <= {"year", "period", "periodName", "value", "footnotes", "latest"}, "BLS extra row field")
            key = (row["year"], row["period"])
            require(key in expected and key not in seen, "BLS duplicate or unrequested period")
            seen.add(key)
            value, notes = row["value"], row["footnotes"]
            require(isinstance(value, str) and re.fullmatch(r"(?:[0-9]+(?:\.[0-9]+)?|-)", value) is not None, "BLS unknown value token")
            require(isinstance(row["periodName"], str) and isinstance(notes, list), "BLS period or footnotes invalid")
            require(
                all(isinstance(n, dict) and set(n) <= {"code", "text"} and all(isinstance(v, str) for v in n.values()) for n in notes), "BLS footnotes changed"
            )
            require(value != "-" or any(n for n in notes), "BLS missing token lacks footnote")
            missing_values += value == "-"
            footnoted += any(n for n in notes)
            records.append([sid, sid[5:10], sid[-2:], *key, row["periodName"], value, json.dumps(notes, sort_keys=True, separators=(",", ":"))])
        missing = expected - seen
        for year in sorted({year for year, _period in missing}):
            # Only complete absent years with exact publisher evidence are accepted as holds.
            # Partial years still stop; no cells are fabricated and no hold is closed.
            require(not any(observed_year == year for observed_year, _period in seen), "BLS missing year-period cells; coverage review required")
            message = f"No Data Available for Series {sid} Year: {year}"
            require(message in messages, "BLS missing year lacks reviewed absence evidence")
            recognized.append(message)
            unavailable.append({"series_id": sid, "year": int(year), "status": "availability_hold", "message": message})
    require(sorted(messages) == sorted(recognized), "BLS unreviewed warning or availability message")
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(FIELDS)
    writer.writerows(sorted(records))
    return output.getvalue().encode(), {
        "rows": len(records),
        "requested_series": len(series),
        "available_series": available_series,
        "availability_holds": unavailable,
        "missing_value_rows": missing_values,
        "footnoted_rows": footnoted,
        "model_eligible": False,
    }


def verify_capture(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Rebuild the derivative and bind every storage input to a reviewed public request."""
    from scripts.acquisition.capture import receipt_validator
    from scripts.acquisition.source_registry import load_registry

    require(not evidence_only and source["source_id"] == "BLS", "BLS source or storage mode differs")
    plan = load_plan()
    require(lineage["plan_sha256"] == canonical_hash(plan), "BLS capture plan differs")
    require(lineage["registry_sha256"] == canonical_hash(load_registry(expected_sha256=lineage["registry_sha256"])), "BLS registry differs")
    require(lineage["schema_sha256"] == canonical_hash(receipt_validator().schema), "BLS schema differs")
    require(lineage["model_eligible"] is False and receipt["snapshot_status"] == "acquired_unvalidated", "BLS modeling hold differs")
    require_code(lineage["code_sha256"])
    batch = next(b for b in plan["batches"] if b["id"] == lineage["batch_id"])
    acq = receipt["acquisition"]
    require(acq["requested_url"] == acq["resolved_url"] == plan["endpoint"], "BLS transport endpoint differs")
    require(acq["request_method"] == "POST" and acq["request_parameters"] == request_for(batch), "BLS saved request differs")
    require(acq["pagination"]["page_count"] == 1 and acq["pagination"]["termination_verified"] is True, "BLS response coverage differs")
    roles = {item["role"]: item for item in receipt["artifacts"]}
    require(len(roles) == len(receipt["artifacts"]) == 3 and set(roles) == {"api_page", "data", "export_receipt"}, "BLS artifact roles differ")
    raw = (root / roles["api_page"]["storage_path"]).read_bytes()
    require(digest(raw) == lineage["expected_sha256"], "BLS raw hash differs")
    derived, statistics = validate_response(raw, batch)
    require(derived == (root / roles["data"]["storage_path"]).read_bytes(), "BLS derived CSV differs")
    proof = read_json(root / roles["export_receipt"]["storage_path"])
    require(proof["statistics"] == statistics and proof["request"] == request_for(batch), "BLS evidence differs")
    require(receipt["schema_profile"]["row_count"] == statistics["rows"], "BLS row count differs")
    require(proof["response_sha256"] == digest(raw), "BLS response evidence hash differs")
