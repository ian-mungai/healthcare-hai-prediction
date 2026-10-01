"""Locked scope, validation and derivation for ACS detailed tables used to fill S1701 and C16001 history.

User decision 2026-09-29: derive poverty % (C175) for 2010-2011 from B17001 and the "speaks English less than very
well" total (C183.02) for 2009-2015 from B16001, after exact checks against the stored S1701 (2012-2016) and C16001
(2016-2017) county files; after B16001 proved null for counties from 2016, limited English is checked against B16004 in
the same years (user decision 2026-09-29). Failure modes: data/acquisition_planning/acs_derived_history_failure_modes.md.
"""

import csv
import io
import json
import math
import re
from pathlib import Path

from scripts.acquisition.bls_api_contract import code_hashes, digest
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require

PLAN_PATH = REPO_ROOT / "config/acquisition/census_acs_detailed_plan.json"
VERSIONS_PATH = REPO_ROOT / "config/acquisition/census_acs_detailed_code_versions.json"
NULL_MARKER = "\\N"
LESS = 'less than "very well"'
# B16004 splits "less than very well" into three levels; together they are the same group.
LIMITED = ('Speak English "well"', 'Speak English "not well"', 'Speak English "not at all"')
UNIVERSE = {"B17001": "B17001_001E", "B16001": "B16001_001E", "B16004": "B16004_001E"}
MIN_COUNTIES = 3100
NUMBER = re.compile(r"[0-9]+(?:\.[0-9]+)?")


def endpoint(year: int) -> str:
    """The ACS 5-year detailed-table endpoint for one year."""
    return f"https://api.census.gov/data/{year}/acs/acs5"


def request_for(batch: dict) -> dict:
    """Public parameters only; group calls include estimates, margins and annotations."""
    require(batch["table"] in UNIVERSE, "Census detailed table outside approved scope")
    return {"get": f"group({batch['table']})", "for": "county:*", "in": "state:*"}


def batch_id(batch: dict) -> str:
    """Identify one table-year county request independently of its retrieval date."""
    return canonical_hash({"endpoint": endpoint(batch["year"]), "parameters": request_for(batch)})


def require_code(hashes: dict | None = None) -> dict:
    """Require explicit review of new captures and retain trusted historical implementations."""
    actual = code_hashes() if hashes is None else hashes
    require(bool(actual) and actual in [v["code_sha256"] for v in read_code_versions(VERSIONS_PATH)["versions"]], "Unreviewed Census detailed code version")
    return actual


def variables_of(batch: dict) -> dict:
    """The pinned group dictionary's variables."""
    return read_json(REPO_ROOT / batch["metadata"]["path"])["variables"]


def less_lines(table: str, variables: dict) -> list[str]:
    """Estimate lines for speaking English less than very well, selected by label from the year's own dictionary."""

    def limited(label: str) -> bool:
        return LESS in label if table == "B16001" else label.endswith(LIMITED)

    return sorted(k for k, v in variables.items() if re.fullmatch(rf"{table}_[0-9]{{3}}E", k) and limited(v["label"]))


def load_plan(path: Path | None = None) -> dict:
    """Verify the lock, pinned dictionaries and comparison files, and the label-selected lines."""
    path = PLAN_PATH if path is None else path
    plan = read_json(path)
    require(read_json(path.with_suffix(".lock.json"))["plan_sha256"] == canonical_hash(plan), "Census detailed plan lock differs")
    require(
        plan["source_id"] == "ACS" and plan["model_eligible"] is False and plan["credential_reference"] == "census_api_key", "Census detailed plan hold differs"
    )
    require(plan["registry_sha256"] == canonical_hash(load_registry(expected_sha256=plan["registry_sha256"])), "Census detailed registry differs")
    for ref in plan["references"]:
        require(digest((REPO_ROOT / ref["path"]).read_bytes()) == ref["sha256"], "Census detailed reference changed")
    for batch in plan["batches"]:
        require(batch["id"] == batch_id(batch) and batch["metadata"] in plan["references"], "Census detailed batch binding differs")
        variables = variables_of(batch)
        require(batch["headers"] == sorted([*variables, "state", "county"]) and {"GEO_ID", "NAME"} <= variables.keys(), "Census detailed headers differ")
        if batch["table"] in {"B16001", "B16004"}:
            require(batch["less_lines"] == less_lines(batch["table"], variables) and bool(batch["less_lines"]), "Census detailed English lines differ")
    tables = {(b["table"], b["year"]) for b in plan["batches"]}
    for check in plan["comparisons"]:
        if check["kind"] == "poverty":
            require(check["file"] in plan["references"], "Census detailed comparison file not pinned")
        else:
            require({("B16001", check["year"]), ("B16004", check["year"])} <= tables, "Census detailed English check tables missing")
    return plan


def validate_response(raw: bytes, batch: dict) -> tuple[bytes, dict]:
    """Lossless CSV of the response, refusing header, geography or duplicate defects."""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        raise ValueError("Census detailed invalid JSON response") from None
    require(isinstance(payload, list) and len(payload) > 1 and isinstance(payload[0], list), "Census detailed JSON schema changed")
    headers = payload[0]
    require(
        all(isinstance(h, str) for h in headers) and sorted(headers) == batch["headers"] and len(set(headers)) == len(headers), "Census detailed headers differ"
    )
    positions = [headers.index(h) for h in batch["headers"]]
    seen: set[str] = set()
    records = []
    for row in payload[1:]:
        require(isinstance(row, list) and len(row) == len(headers), "Census detailed row width changed")
        require(all(v is None or isinstance(v, str) for v in row) and NULL_MARKER not in row, "Census detailed native cell type changed")
        data = dict(zip(headers, row, strict=True))
        identity = f"{data['state']}{data['county']}"
        require(re.fullmatch(r"[0-9]{5}", identity) is not None and data["GEO_ID"] == f"0500000US{identity}", "Census detailed geography invalid")
        require(identity not in seen, "Duplicate county in Census detailed response")
        seen.add(identity)
        records.append((identity, [NULL_MARKER if row[i] is None else row[i] for i in positions]))
    require(len(seen) >= MIN_COUNTIES, "Census detailed county count below the floor")
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(batch["headers"])
    writer.writerows(row for _, row in sorted(records))
    return output.getvalue().encode(), {"rows": len(records), "columns": len(headers), "puerto_rico_counties": sum(i.startswith("72") for i in seen)}


def table_rows(csv_bytes: bytes) -> dict[str, dict[str, str]]:
    """Rows of a stored observations CSV, by five-digit county."""
    reader = csv.DictReader(io.StringIO(csv_bytes.decode(), newline=""))
    return {r["state"] + r["county"]: r for r in reader}


def number(value: str | None) -> float | None:
    """A published non-negative number, or None for nulls, sentinels and text."""
    return float(value) if value is not None and NUMBER.fullmatch(value) else None


def ratio(numerator: float, denominator: float, moe_num: float, moe_den: float) -> tuple[float, float]:
    """Percentage and its approximate margin (Census proportion formula; ratio form when the radicand is negative)."""
    p = numerator / denominator
    radicand = moe_num**2 - p**2 * moe_den**2
    if radicand < 0:
        radicand = moe_num**2 + p**2 * moe_den**2
    return p * 100, math.sqrt(radicand) / denominator * 100


def measure(rows: dict[str, dict[str, str]], numerators: list[str], universe: str) -> dict[str, tuple[float, float, float, float | None] | None]:
    """Per county: numerator, denominator, percentage and approximate margin (None when a margin is not a number).

    A county gets no value when any estimate or the denominator is not a published non-negative number, or is zero.
    """
    out: dict[str, tuple[float, float, float, float | None] | None] = {}
    for county, row in rows.items():
        values = [number(row[v]) for v in numerators]
        den = number(row[universe])
        if any(v is None for v in values) or den is None or den == 0:
            out[county] = None
            continue
        num = sum(v for v in values if v is not None)
        margins = [number(row[v[:-1] + "M"]) for v in [*numerators, universe]]
        if any(m is None for m in margins):
            out[county] = (num, den, num / den * 100, None)
            continue
        found = [m for m in margins if m is not None]
        pct, pct_moe = ratio(num, den, math.sqrt(sum(m**2 for m in found[:-1])), found[-1])
        out[county] = (num, den, pct, pct_moe)
    return out


def published(path: Path) -> dict[str, dict[str, str]]:
    """A stored data.census.gov county export (code row, label row, data), by five-digit county."""
    rows = list(csv.reader(io.StringIO(path.read_text(encoding="utf-8-sig"), newline="")))
    require(len(rows) > 2 and rows[0][0] == "GEO_ID", "Census comparison file layout differs")
    return {r[0].removeprefix("0500000US"): dict(zip(rows[0], r, strict=True)) for r in rows[2:] if r[0].startswith("0500000US")}


def check(kind: str, derived: dict[str, tuple[float, float, float, float | None] | None], stored: dict[str, dict[str, str]]) -> dict:
    """Exact overlap comparison with the published S1701 table; mismatches are counted, never printed."""
    require(set(derived) == set(stored), f"Census {kind} check county sets differ")
    compared = mismatched = skipped = 0
    for county, value in derived.items():
        row = stored[county]
        fields = ["S1701_C01_001E", "S1701_C02_001E", "S1701_C03_001E"]
        published_values = [number(row.get(f)) for f in fields]
        if value is None or any(v is None for v in published_values):
            skipped += 1
            continue
        compared += 1
        num, den, pct, _ = value
        ok = (den, num) == (published_values[0], published_values[1]) and abs(round(pct, 1) - (published_values[2] or 0.0)) <= 0.051
        mismatched += not ok
    return {"kind": kind, "counties_compared": compared, "counties_skipped_non_numeric": skipped, "mismatches": mismatched}


def check_tables(
    derived: dict[str, tuple[float, float, float, float | None] | None], other: dict[str, tuple[float, float, float, float | None] | None]
) -> dict:
    """Exact same-year comparison of two tables' numerator and universe; mismatches are counted, never printed."""
    require(set(derived) == set(other), "Census english check county sets differ")
    compared = mismatched = skipped = 0
    for county, value in derived.items():
        reference = other[county]
        if value is None or reference is None:
            skipped += 1
            continue
        compared += 1
        mismatched += (value[0], value[1]) != (reference[0], reference[1])
    return {"kind": "english", "against": "B16004", "counties_compared": compared, "counties_skipped_non_numeric": skipped, "mismatches": mismatched}


def derive(plan: dict, observations: dict[str, bytes]) -> tuple[dict[str, bytes], dict]:
    """Derived CSVs and the check report; stops unless every overlap check matches exactly."""
    batches = {(b["table"], b["year"]): b for b in plan["batches"]}
    report: dict = {"checks": [], "derived_counties": {}, "definition": plan["definitions"], "approximate_margins": True, "model_eligible": False}

    def values(table: str, year: int) -> dict[str, tuple[float, float, float, float | None] | None]:
        batch = batches[(table, year)]
        rows = table_rows(observations[batch["id"]])
        if table == "B17001":
            return measure(rows, ["B17001_002E"], UNIVERSE[table])
        return measure(rows, batch["less_lines"], UNIVERSE[table])

    for check_spec in plan["comparisons"]:
        kind, year = check_spec["kind"], check_spec["year"]
        if kind == "poverty":
            result = check(kind, values("B17001", year), published(REPO_ROOT / check_spec["file"]["path"]))
        else:
            result = check_tables(values("B16001", year), values("B16004", year))
        result["year"] = year
        report["checks"].append(result)
        require(result["mismatches"] == 0 and result["counties_compared"] > 0, f"Census {kind} derivation does not reproduce the published table")
    files: dict[str, bytes] = {}
    for kind, years in plan["derived_years"].items():
        table = "B17001" if kind == "poverty" else "B16001"
        output = io.StringIO(newline="")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(["year", "county", "numerator", "denominator", "percent", "percent_moe_approx", "source_table", "source_lines"])
        for year in years:
            lines = "B17001_002E" if table == "B17001" else "+".join(batches[(table, year)]["less_lines"])
            derived = values(table, year)
            report["derived_counties"][f"{kind}_{year}"] = sum(v is not None for v in derived.values())
            for county, value in sorted(derived.items()):
                cells = ["", "", "", ""] if value is None else [repr(value[0]), repr(value[1]), repr(value[2]), "" if value[3] is None else repr(value[3])]
                writer.writerow([year, county, *cells, table, lines])
        files[f"derived/{kind}_{years[0]}_{years[-1]}.csv"] = output.getvalue().encode()
    return files, report


def verify_capture(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Check one table-year capture's artifacts and bindings, or the derived snapshot's rebuild from its inputs."""
    from scripts.acquisition.capture import receipt_validator

    require(not evidence_only and source["source_id"] == "ACS", "Census detailed storage source differs")
    plan = load_plan()
    require(lineage["plan_sha256"] == canonical_hash(plan), "Census detailed capture plan differs")
    require(lineage["registry_sha256"] == canonical_hash(load_registry(expected_sha256=lineage["registry_sha256"])), "Census detailed registry differs")
    require(lineage["schema_sha256"] == canonical_hash(receipt_validator().schema), "Census detailed schema differs")
    require(lineage["model_eligible"] is False and receipt["snapshot_status"] == "acquired_unvalidated", "Census detailed modeling hold differs")
    require_code(lineage["code_sha256"])
    require(receipt["governance"]["credential_reference"] == "census_api_key", "Census detailed credential reference differs")
    if lineage["mode"] == "acs_derived":
        inputs = {}
        collection = root.parents[2]
        for batch in plan["batches"]:
            path = collection / "batches" / batch["id"] / "capture/derived/observations.csv"
            require(path.exists(), "Census derived input capture missing")
            inputs[batch["id"]] = path.read_bytes()
        require(lineage["input_sha256"] == {k: digest(v) for k, v in sorted(inputs.items())}, "Census derived inputs differ")
        files, report = derive(plan, inputs)
        paths = {a["storage_path"] for a in receipt["artifacts"]}
        require(paths == {*files, "evidence/checks.json", "references/scope.json"}, "Census derived artifact set differs")
        for name, body in files.items():
            require((root / name).read_bytes() == body, "Census derived CSV differs")
        require(read_json(root / "evidence/checks.json") == report and read_json(root / "references/scope.json") == plan, "Census derived evidence differs")
        return
    matches = [b for b in plan["batches"] if b["id"] == lineage["batch_id"]]
    require(len(matches) == 1, "Census detailed capture batch not in its plan")
    batch = matches[0]
    acq = receipt["acquisition"]
    require(
        acq["requested_url"] == endpoint(batch["year"]) and acq["request_parameters"] == request_for(batch) and acq["http_status"] == 200,
        "Census detailed request differs",
    )
    roles = {a["role"]: a for a in receipt["artifacts"]}
    require(set(roles) == {"api_page", "data", "dictionary", "layout", "export_receipt"} and len(roles) == 5, "Census detailed artifact set differs")
    raw = (root / roles["api_page"]["storage_path"]).read_bytes()
    require(digest(raw) == lineage["expected_sha256"], "Census detailed raw hash differs")
    derived, statistics = validate_response(raw, batch)
    require((root / roles["data"]["storage_path"]).read_bytes() == derived, "Census detailed CSV differs")
    require(digest((root / roles["dictionary"]["storage_path"]).read_bytes()) == batch["metadata"]["sha256"], "Census detailed dictionary differs")
    require(read_json(root / roles["layout"]["storage_path"]) == plan, "Census detailed stored scope differs")
    proof = read_json(root / roles["export_receipt"]["storage_path"])
    require(proof["statistics"] == statistics and proof["sha256"] == digest(raw) and proof["request"] == request_for(batch), "Census detailed proof differs")
    require(receipt["schema_profile"]["row_count"] == statistics["rows"], "Census detailed schema profile differs")
