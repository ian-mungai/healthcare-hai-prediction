"""Frozen scope and content checks for CDC WONDER county-year mortality exports saved by hand.

WONDER's API serves national data only, so county results come from the query form. The measure is
deaths, population and crude rate per 100,000 with 95% confidence limits and standard error (user
decision 2026-09-28: WONDER does not offer county age-adjusted rates). Values stay exactly as
published, including suppression and reliability flags; nothing is reconstructed or recalculated.
"""

import csv
import io
import re
from pathlib import Path

from scripts.acquisition.bls_api_contract import code_hashes, digest
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.data_paths import current
from scripts.acquisition.legacy_versions import plan_matches
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require

PLAN_PATH = REPO_ROOT / "config/acquisition/wonder_export_plan.json"
VERSIONS_PATH = REPO_ROOT / "config/acquisition/wonder_export_code_versions.json"
ORIGIN = "https://wonder.cdc.gov/controller/datarequest/"
DATABASES = {
    "D76": {"title": "Underlying Cause of Death, 1999-2020", "form_url": "https://wonder.cdc.gov/ucd-icd10.html", "years": [1999, 2020]},
    "D158": {"title": "Underlying Cause of Death, 2018-2024, Single Race", "form_url": "https://wonder.cdc.gov/ucd-icd10-expanded.html", "years": [2018, 2024]},
}
HEADER = [
    "Notes",
    "County",
    "County Code",
    "Year",
    "Year Code",
    "Deaths",
    "Population",
    "Crude Rate",
    "Crude Rate Lower 95% Confidence Interval",
    "Crude Rate Upper 95% Confidence Interval",
    "Crude Rate Standard Error",
]
# The approved query, as WONDER prints it in each export's notes (pilot, 2026-09-28).
FIXED_PARAMETERS = {
    "Group By": "County; Year",
    "Show Totals": "Disabled",
    "Show Zero Values": "True",
    "Show Suppressed": "True",
    "Calculate Rates Per": "100,000",
    "Rate Options": "Default intercensal populations for years 2001-2009 (except Infant Age Groups)",
}
# Published tokens allowed in place of a number, by column.
TOKENS = {
    "Deaths": {"Suppressed", "Missing"},
    "Population": {"Not Available", "Missing"},
    "Crude Rate": {"Suppressed", "Unreliable", "Not Available", "Missing"},
}
RATE_COLUMNS = HEADER[7:]
DIVIDER = '"---"'


def require_code(hashes: dict | None = None) -> dict:
    """Require explicit review of new captures and retain trusted historical implementations."""
    actual = code_hashes() if hashes is None else hashes
    require(bool(actual) and actual in [v["code_sha256"] for v in read_code_versions(VERSIONS_PATH)["versions"]], "Unreviewed WONDER code version")
    return actual


def batch_id(batch: dict) -> str:
    """Identify one export by its database, years and exact saved bytes."""
    return canonical_hash({"database": batch["database"], "years": batch["years"], "sha256": batch["sha256"]})


def stored_name(batch: dict) -> str:
    """A storage-safe name for the original; the browser's name is kept as the original file name."""
    return f"wonder_{batch['database']}_{batch['years'][0]}_{batch['years'][1]}.txt"


def sanitize_origins(origins: list[str], database: str) -> list[str]:
    """Drop the session identifier Chrome records, and require the planned database's query page."""
    cleaned = sorted({re.sub(r";jsessionid=[^?#]*", "", url) for url in origins})
    require(bool(cleaned) and all(re.fullmatch(re.escape(ORIGIN + database) + r"(\?.*)?", url) for url in cleaned), "WONDER download origin differs")
    return [ORIGIN + database]


def load_plan(path: Path | None = None) -> dict:
    """Verify the locked scope, per-export download bindings, terms record and base registry."""
    path = PLAN_PATH if path is None else path
    plan = read_json(path)
    require(read_json(path.with_suffix(".lock.json"))["plan_sha256"] == canonical_hash(plan), "WONDER plan lock differs")
    require(plan["source_id"] == "WONDER" and plan["model_eligible"] is False and plan["version"] == 1, "WONDER plan hold differs")
    require(plan["header"] == HEADER and plan["fixed_parameters"] == FIXED_PARAMETERS and plan["databases"] == DATABASES, "WONDER plan query differs")
    require(plan["registry_sha256"] == canonical_hash(load_registry(expected_sha256=plan["registry_sha256"])), "WONDER registry differs")
    require(isinstance(plan["min_counties_per_year"], int) and plan["min_counties_per_year"] > 0, "WONDER county floor invalid")
    terms = plan["terms"]
    require(digest((REPO_ROOT / current(terms["path"])).read_bytes()) == terms["sha256"], "WONDER terms record changed")
    record = next(d for d in read_json(REPO_ROOT / current(terms["path"]))["datasets"] if d["source_id"] == "WONDER")
    require(record["terms_accepted"] is True, "WONDER terms not accepted")
    require(bool(plan["batches"]), "WONDER plan has no exports")
    for batch in plan["batches"]:
        first, last = batch["years"]
        low, high = DATABASES[batch["database"]]["years"]
        require(low <= first <= last <= high and batch["id"] == batch_id(batch), "WONDER export binding differs")
        require(re.fullmatch(r"[0-9a-f]{64}", batch["sha256"]) is not None and 0 < batch["bytes"] <= 64 * 1024**2, "WONDER export binding invalid")
    return plan


def query_record(notes: list[str]) -> dict:
    """Read the dataset, query parameters, query date and citation from the export's notes section."""
    text = [line[1:-1] if len(line) > 1 and line.startswith('"') and line.endswith('"') else line for line in notes]
    require(bool(text) and text[0].startswith("Dataset: ") and "Query Parameters:" in text, "WONDER query record missing")
    start = text.index("Query Parameters:") + 1
    end = start + text[start:].index("---")
    parameters = {}
    for line in text[start:end]:
        key, sep, value = line.partition(": ")
        require(bool(sep) and key not in parameters, "WONDER query parameters differ")
        parameters[key] = value
    date = next((line.removeprefix("Query Date: ") for line in text if line.startswith("Query Date: ")), None)
    cite = next((i for i, line in enumerate(text) if line.startswith("Suggested Citation: ")), None)
    citation = None
    if cite is not None:
        stop = cite + text[cite:].index("---") if "---" in text[cite:] else len(text)
        citation = " ".join(text[cite:stop]).removeprefix("Suggested Citation: ")
    return {"dataset": text[0].removeprefix("Dataset: "), "parameters": parameters, "query_date": date, "citation": citation}


def listed_years(value: str) -> list[int]:
    """Expand WONDER's Year/Month selection text into years; anything unrecognized stops the export."""
    years: set[int] = set()
    for token in re.split(r"[;,]\s*", value.strip()):
        match = re.fullmatch(r"(\d{4})(?:\s*-\s*(\d{4}))?", token.strip())
        if match is None:
            raise ValueError("WONDER query years differ")
        first, last = int(match[1]), int(match[2] or match[1])
        years.update(range(first, last + 1))
    return sorted(years)


def split_export(raw: bytes) -> tuple[list[list[str]], list[str]]:
    """Separate the tab-delimited table from the notes section that follows the first divider line."""
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeDecodeError:
        raise ValueError("WONDER export is not UTF-8 text") from None
    require(DIVIDER in lines, "WONDER query record missing")
    cut = lines.index(DIVIDER)
    table = list(csv.reader(lines[:cut], delimiter="\t", quotechar='"'))
    return table, lines[cut + 1 :]


def check_value(column: str, value: str) -> None:
    """Numbers as published, or a published flag allowed for that column."""
    if column in ("Deaths", "Population"):
        allowed = value in TOKENS[column] or re.fullmatch(r"[0-9]+", value) is not None
    else:
        allowed = value in (TOKENS["Crude Rate"] if column == "Crude Rate" else TOKENS["Crude Rate"] - {"Unreliable"}) or (
            re.fullmatch(r"[0-9]+(\.[0-9]+)?", value) is not None
        )
    require(allowed, f"WONDER {column} value invalid")
    require(not (column == "Deaths" and value.isdigit() and 1 <= int(value) <= 9), "WONDER export shows a count of 1-9 deaths")


def sort_key(row: list[str]) -> tuple[str, str]:
    """Order rows by county code, then year code."""
    return row[2], row[4]


def validate_export(raw: bytes, batch: dict, plan: dict) -> tuple[bytes, dict]:
    """Write a CSV of the published rows while refusing query, schema, coverage or suppression defects."""
    table, notes = split_export(raw)
    query = query_record(notes)
    database = DATABASES[batch["database"]]
    require(query["dataset"] == database["title"], "WONDER dataset differs")
    parameters = dict(query["parameters"])
    planned = list(range(batch["years"][0], batch["years"][1] + 1))
    if "Year/Month" in parameters:
        require(listed_years(parameters.pop("Year/Month")) == planned, "WONDER query years differ")
    require(parameters == plan["fixed_parameters"], "WONDER query parameters differ")
    require(bool(table) and table[0] == plan["header"], "WONDER header differs")
    seen: dict[str, set[str]] = {}
    counts: dict[str, dict[str, int]] = {c: {} for c in HEADER[5:]}
    for row in table[1:]:
        require(len(row) == len(HEADER), "WONDER row shape differs")
        values = dict(zip(HEADER, row, strict=True))
        require(values["Notes"] == "" and values["County Code"] != "", "WONDER export contains a totals or notes row")
        require(re.fullmatch(r"[0-9]{5}", values["County Code"]) is not None, "WONDER county code invalid")
        require(values["Year Code"].isdigit() and int(values["Year Code"]) in planned, "WONDER year outside the plan")
        require(values["Year"].strip() == values["Year Code"], "WONDER year differs from its year code")
        for column in HEADER[5:]:
            check_value(column, values[column])
            if not re.fullmatch(r"[0-9.]+", values[column]):
                counts[column][values[column]] = counts[column].get(values[column], 0) + 1
        codes = seen.setdefault(values["Year Code"], set())
        require(values["County Code"] not in codes, "Repeated county-year row in WONDER export")
        codes.add(values["County Code"])
    require(sorted(int(y) for y in seen) == planned, "WONDER export years differ from the plan")
    per_year = {year: len(codes) for year, codes in sorted(seen.items())}
    require(min(per_year.values()) >= plan["min_counties_per_year"], "WONDER county count below the floor")
    states = set(plan["required_state_fips"])
    require(all(states <= {c[:2] for c in codes} for codes in seen.values()), "WONDER state coverage incomplete")
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(HEADER)
    ordered = sorted(table[1:], key=sort_key)
    writer.writerows(ordered)
    return output.getvalue().encode(), {
        "rows": len(table) - 1,
        "years": planned,
        "counties_per_year": per_year,
        "flags": {column: dict(sorted(found.items())) for column, found in counts.items() if found},
        "query": query,
        "model_eligible": False,
    }


def verify_capture(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Check every artifact, the download binding, query record, terms and preserved holds."""
    from scripts.acquisition.capture import receipt_validator

    require(not evidence_only and source["source_id"] == "WONDER", "WONDER storage source differs")
    plan = load_plan()
    require(plan_matches(plan, lineage["plan_sha256"]), "WONDER capture plan differs")
    require(lineage["registry_sha256"] == canonical_hash(load_registry(expected_sha256=lineage["registry_sha256"])), "WONDER registry differs")
    require(lineage["schema_sha256"] == canonical_hash(receipt_validator().schema), "WONDER schema differs")
    require(lineage["terms_sha256"] == plan["terms"]["sha256"], "WONDER terms binding differs")
    require(lineage["model_eligible"] is False and receipt["snapshot_status"] == "acquired_unvalidated", "WONDER modeling hold differs")
    require_code(lineage["code_sha256"])
    matches = [b for b in plan["batches"] if b["id"] == lineage["batch_id"]]
    require(len(matches) == 1, "WONDER capture export not in its plan")
    batch = matches[0]
    database = DATABASES[batch["database"]]
    acq = receipt["acquisition"]
    require(acq["requested_url"] == database["form_url"] and acq["request_method"] == "browser_export" and acq["http_status"] is None, "WONDER route differs")
    raw_path = f"raw/{stored_name(batch)}"
    paths = {a["storage_path"] for a in receipt["artifacts"]}
    require(paths == {raw_path, "derived/county_year.csv", "evidence/download_proof.json", "references/scope.json"}, "WONDER artifact set differs")
    raw = (root / raw_path).read_bytes()
    require(digest(raw) == lineage["expected_sha256"] == batch["sha256"] and len(raw) == batch["bytes"], "WONDER raw hash differs")
    derived, statistics = validate_export(raw, batch, plan)
    require((root / "derived/county_year.csv").read_bytes() == derived, "WONDER derived CSV differs")
    require(canonical_hash(read_json(root / "references/scope.json")) == lineage["plan_sha256"], "WONDER stored scope differs")
    proof = read_json(root / "evidence/download_proof.json")
    require(proof["origin_urls"] == [ORIGIN + batch["database"]] and proof["file_name"] == batch["file_name"], "WONDER download proof differs")
    require(proof["sha256"] == batch["sha256"] and proof["statistics"] == statistics and proof["query"] == statistics["query"], "WONDER download proof differs")
    require(proof["created_at_utc"] == acq["retrieved_at_utc"], "WONDER download proof differs")
    require(
        receipt["schema_profile"]["row_count"] == statistics["rows"] and receipt["schema_profile"]["native_headers"] == HEADER, "WONDER schema profile differs"
    )
    terms = next(d for d in read_json(REPO_ROOT / current(plan["terms"]["path"]))["datasets"] if d["source_id"] == "WONDER")
    require(receipt["governance"]["use_restrictions"] == terms["restrictions"], "WONDER governance differs")
    period = receipt["measurement_periods"]
    first, last = batch["years"]
    require(len(period) == 1 and (period[0]["start_date"], period[0]["end_date"]) == (f"{first}-01-01", f"{last}-12-31"), "WONDER measurement years differ")
