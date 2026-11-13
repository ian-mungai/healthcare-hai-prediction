"""Locked scope, request shape, validation and export-layout CSV for the ACS table-vintage units served by the Census API.

One unit is one table of one vintage: 173 units for the 15 tables of the former data.census.gov county exports (subject, data profile and detailed
tables). The plan pins no group dictionary; each unit stores the dictionary it fetched and checks that the response headers equal the dictionary's variables
plus the geography columns. The derived CSV keeps the export layout (a code row, a ``Geography`` label row, county rows) with native tokens and every
annotation column, so bronze keeps its table shape. Design: data/local_keep/acs_census_api_design_20261113/design.md.
"""

import csv
import io
import json
import re
from pathlib import Path

from scripts.acquisition.bls_api_contract import digest
from scripts.acquisition.code_closure import closure_hashes
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.legacy_versions import plan_matches
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require

PLAN_PATH = REPO_ROOT / "config/acquisition/census_acs_tables_plan.json"
VERSIONS_PATH = REPO_ROOT / "config/acquisition/census_acs_tables_code_versions.json"
CODE_ENTRY = "scripts.acquisition.collect_census_acs_tables"
HOST = "https://api.census.gov"
NULL_MARKER = "\\N"
MIN_COUNTIES = 3100
GEOGRAPHY = ("GEO_ID", "NAME")
PUERTO_RICO = "72"
CONNECTICUT = "09"
LABEL_ROW = ("Geography", "Geographic Area Name")
PRODUCTS = {"subject": ("/subject", "ACSST5Y"), "profile": ("/profile", "ACSDP5Y"), "detailed": ("", "ACSDT5Y")}
# Product, then table, to the stored vintages (owner decision "ACS API vintages: the stored set only", Nov 12 2026).
SCOPE = {
    "subject": {
        "S0101": list(range(2010, 2025)),
        "S0601": list(range(2010, 2025)),
        "S1701": list(range(2012, 2025)),
        "S2503": list(range(2010, 2025)),
        "S2701": list(range(2012, 2025)),
    },
    "profile": {table: list(range(2010, 2025)) for table in ("DP02", "DP03", "DP04", "DP05")},
    "detailed": {
        "B16005": list(range(2010, 2018)),
        "B19013": list(range(2010, 2018)),
        "B25070": list(range(2010, 2018)),
        "B25091": list(range(2010, 2018)),
        "B26001": list(range(2010, 2018)),
        "C16001": [2016, 2017],
    },
}


def product_of(table: str) -> str:
    """The product of one approved table."""
    found = [product for product, tables in SCOPE.items() if table in tables]
    require(len(found) == 1, "Census tables table outside approved scope")
    return found[0]


def endpoint(product: str, year: int) -> str:
    """The ACS 5-year endpoint of one product and vintage."""
    require(product in PRODUCTS, "Census tables product outside approved scope")
    return f"{HOST}/data/{year}/acs/acs5{PRODUCTS[product][0]}"


def request_for(batch: dict) -> dict:
    """Public parameters only; a group call returns estimates, margins and annotations. The key is added by the transport at call time, never here."""
    product_of(batch["table"])
    return {"get": f"group({batch['table']})", "for": "county:*", "in": "state:*"}


def dictionary_url(batch: dict) -> str:
    """The unkeyed group dictionary of one table and vintage."""
    return f"{endpoint(batch['product'], batch['year'])}/groups/{batch['table']}.json"


def file_name(batch: dict) -> str:
    """The export-style member name, so the vintage pattern of the geography periods script matches."""
    return f"{PRODUCTS[batch['product']][1]}{batch['year']}.{batch['table']}-Data.csv"


def batch_id(batch: dict) -> str:
    """Identify one table-vintage county request independently of its retrieval date."""
    return canonical_hash({"endpoint": endpoint(batch["product"], batch["year"]), "parameters": request_for(batch)})


def queue_snapshot_id(batch: dict) -> str:
    """The deterministic snapshot ID of a queue entry: the plan version and the first 32 characters of the batch ID, no time."""
    return f"Census_ACS_Tables__v1__{batch['id'][:32]}"


def scope_batches(scope: dict | None = None) -> list[dict]:
    """The table-vintage units of a scope table: product, table and year only, in product, table and year order."""
    scope = SCOPE if scope is None else scope
    return [{"product": p, "table": t, "year": y} for p in PRODUCTS if p in scope for t in sorted(scope[p]) for y in sorted(scope[p][t])]


def planned_batches(scope: dict | None = None) -> list[dict]:
    """The plan batches of a scope table: each unit with its endpoint, export-style file name and ID."""
    out = []
    for unit in scope_batches(scope):
        batch = unit | {"endpoint": endpoint(unit["product"], unit["year"]), "file_name": file_name(unit)}
        out.append(batch | {"id": batch_id(batch)})
    return out


def collector_code_hashes(entry: str = CODE_ENTRY) -> dict[str, str]:
    """Bind the entry module of the collector and every repository module it can run, and no other file."""
    return closure_hashes(entry)


def require_code(hashes: dict | None = None, entry: str = CODE_ENTRY) -> dict:
    """Require explicit review of new captures and retain trusted historical implementations."""
    actual = collector_code_hashes(entry) if hashes is None else hashes
    require(bool(actual) and actual in [v["code_sha256"] for v in read_code_versions(VERSIONS_PATH)["versions"]], "Unreviewed Census tables code version")
    return actual


def load_plan(path: Path | None = None) -> dict:
    """Verify the lock, the credential name, the registry and that each batch equals its scope-table unit."""
    path = PLAN_PATH if path is None else path
    plan = read_json(path)
    require(read_json(path.with_suffix(".lock.json"))["plan_sha256"] == canonical_hash(plan), "Census tables plan lock differs")
    require(
        plan["source_id"] == "ACS" and plan["model_eligible"] is False and plan["credential_reference"] == "census_api_key" and plan["version"] == 1,
        "Census tables plan hold differs",
    )
    require(plan["registry_sha256"] == canonical_hash(load_registry(expected_sha256=plan["registry_sha256"])), "Census tables registry differs")
    expected = planned_batches()
    require(len(plan["batches"]) == len(expected), "Census tables scope differs")
    for batch, want in zip(plan["batches"], expected, strict=True):
        require(batch == want, "Census tables batch binding differs")
    return plan


def parse_dictionary(raw: bytes) -> dict[str, str]:
    """The variable code to label map of a group dictionary; a different shape is refused with fixed text."""
    try:
        document = json.loads(raw)
    except (ValueError, RecursionError):
        raise ValueError("Census tables dictionary invalid JSON") from None
    if not isinstance(document, dict) or not isinstance(document.get("variables"), dict):
        raise ValueError("Census tables dictionary schema changed")
    variables = document["variables"]
    require(set(GEOGRAPHY) <= variables.keys(), "Census tables dictionary schema changed")
    require(all(isinstance(v, dict) and isinstance(v.get("label"), str) for v in variables.values()), "Census tables dictionary schema changed")
    return {code: value["label"] for code, value in variables.items()}


def validate_response(raw: bytes, dictionary: bytes, batch: dict) -> tuple[bytes, dict]:
    """The export-layout CSV of one response and its statistics, refusing header, geography, cell and duplicate defects with fixed text."""
    labels = parse_dictionary(dictionary)
    try:
        payload = json.loads(raw)
    except (ValueError, RecursionError):
        raise ValueError("Census tables invalid JSON response") from None
    require(isinstance(payload, list) and len(payload) > 1 and isinstance(payload[0], list), "Census tables JSON schema changed")
    headers = payload[0]
    require(all(isinstance(h, str) for h in headers), "Census tables JSON schema changed")
    others = sorted(set(labels) - {*GEOGRAPHY, "state", "county"})
    order = [*GEOGRAPHY, *others, "state", "county"]
    require(len(set(headers)) == len(headers) and sorted(headers) == sorted(order), "Census tables headers differ")
    positions = [headers.index(h) for h in order]
    seen: set[str] = set()
    records = []
    for row in payload[1:]:
        require(isinstance(row, list) and len(row) == len(headers), "Census tables row width changed")
        require(all(v is None or isinstance(v, str) for v in row) and NULL_MARKER not in row, "Census tables native cell type changed")
        data = dict(zip(headers, row, strict=True))
        state, county = data["state"], data["county"]
        valid = isinstance(state, str) and isinstance(county, str) and re.fullmatch(r"[0-9]{2}", state) and re.fullmatch(r"[0-9]{3}", county)
        require(bool(valid) and data["GEO_ID"] == f"0500000US{state}{county}", "Census tables geography invalid")
        identity = f"{state}{county}"
        require(identity not in seen, "Duplicate county in Census tables response")
        seen.add(identity)
        records.append((data["GEO_ID"], [NULL_MARKER if row[i] is None else row[i] for i in positions]))
    require(len(seen) >= MIN_COUNTIES, "Census tables county count below the floor")
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(order)
    writer.writerow([*LABEL_ROW, *(labels[code] for code in others), "state", "county"])
    writer.writerows(row for _, row in sorted(records))
    statistics = {
        "rows": len(records),
        "columns": len(order),
        "puerto_rico_counties": sum(i.startswith(PUERTO_RICO) for i in seen),
        "connecticut_counties": sum(i.startswith(CONNECTICUT) for i in seen),
    }
    return output.getvalue().encode(), statistics


def verify_capture(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Check one capture's artifacts and bindings, rebuilding the CSV from the stored response and dictionary without a network."""
    from scripts.acquisition.capture import receipt_validator

    require(not evidence_only and source["source_id"] == "ACS", "Census tables storage source differs")
    plan = load_plan()
    require(plan_matches(plan, lineage["plan_sha256"]), "Census tables capture plan differs")
    require(lineage["registry_sha256"] == canonical_hash(load_registry(expected_sha256=lineage["registry_sha256"])), "Census tables registry differs")
    require(lineage["schema_sha256"] == canonical_hash(receipt_validator().schema), "Census tables schema differs")
    require(lineage["model_eligible"] is False and receipt["snapshot_status"] == "acquired_unvalidated", "Census tables modeling hold differs")
    require_code(lineage["code_sha256"])
    require(receipt["governance"]["credential_reference"] == "census_api_key", "Census tables credential reference differs")
    matches = [b for b in plan["batches"] if b["id"] == lineage["batch_id"]]
    require(len(matches) == 1, "Census tables capture batch not in its plan")
    batch = matches[0]
    acq = receipt["acquisition"]
    require(
        acq["requested_url"] == batch["endpoint"] and acq["request_parameters"] == request_for(batch) and acq["http_status"] == 200,
        "Census tables request differs",
    )
    roles = {a["role"]: a for a in receipt["artifacts"]}
    require(
        set(roles) == {"api_page", "data", "dictionary", "layout", "export_receipt"} and len(roles) == len(receipt["artifacts"]),
        "Census tables artifact set differs",
    )
    require(roles["data"]["stored_file_name"] == batch["file_name"], "Census tables data file name differs")
    raw = (root / roles["api_page"]["storage_path"]).read_bytes()
    stored_dictionary = (root / roles["dictionary"]["storage_path"]).read_bytes()
    require(digest(raw) == lineage["expected_sha256"], "Census tables raw hash differs")
    require(digest(stored_dictionary) == lineage["dictionary_sha256"], "Census tables dictionary differs")
    derived, statistics = validate_response(raw, stored_dictionary, batch)
    require((root / roles["data"]["storage_path"]).read_bytes() == derived, "Census tables CSV differs")
    require(canonical_hash(read_json(root / roles["layout"]["storage_path"])) == lineage["plan_sha256"], "Census tables stored scope differs")
    proof = read_json(root / roles["export_receipt"]["storage_path"])
    require(
        proof["statistics"] == statistics
        and proof["sha256"] == digest(raw)
        and proof["dictionary_sha256"] == digest(stored_dictionary)
        and proof["request"] == request_for(batch),
        "Census tables proof differs",
    )
    require(receipt["schema_profile"]["row_count"] == statistics["rows"], "Census tables schema profile differs")
