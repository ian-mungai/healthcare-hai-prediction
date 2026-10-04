"""Bounded, approval-specific MMD API contract; no network or eligibility changes."""

import csv
import hashlib
import io
import json
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlencode, urlsplit

from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.data_paths import current
from scripts.acquisition.registry_additions import ADDITIONS_LOCK_PATH, load_registry_additions
from scripts.acquisition.source_registry import canonical_hash, load_registry, read_json, require

PLAN_PATH = Path("data/datasets/historical_acquisition/mmd_api_history/20260925/collection_plan.json")
PLAN_SHA256 = "97f72f7b82b39bfbb8afec75778fe335aceb59f57c0b5fa72c773517377fbfe4"
ADDITIONS_PLAN_PATH = Path("data/datasets/historical_acquisition/mmd_api_history/20260926/collection_plan_additions.json")
ADDITIONS_PLAN_SHA256 = "1525659d4da7ffd41ce12e4513dc2e04b0e4122a93b21439af2653e19f8656a8"
# Each capture records the hash of the plan that authorized it, so adding a plan never touches earlier captures.
PLANS = {PLAN_SHA256: PLAN_PATH, ADDITIONS_PLAN_SHA256: ADDITIONS_PLAN_PATH}
FIELDS = {"agecat", "condition", "dencat", "dual", "eligcat", "fips", "fltr", "geography", "measure", "racecat", "rate", "sexcat", "year"}
ENDPOINT = "https://data.cms.gov/data-api/v1/mmd-tool/"
PAGE_SIZE = 500000
CODE_FILES = (
    "scripts/acquisition/collect_mmd_api.py",
    "scripts/acquisition/mmd_api_contract.py",
    "scripts/acquisition/compare_mmd_api.py",
    "scripts/acquisition/storage_controls.py",
)
CURRENT_CODE_FILES = (*CODE_FILES, "scripts/acquisition/code_versions.py")
# Kept outside CODE_FILES: a file cannot pin the hash of the code that reads it.
CODE_VERSIONS_PATH = Path("config/acquisition/mmd_api_code_versions.json")


def digest(raw: bytes) -> str:
    """Return the content identity used by immutable acquisition evidence."""
    return hashlib.sha256(raw).hexdigest()


def current_code_hashes() -> dict[str, str]:
    """Fingerprint the collector implementation that would produce new evidence."""
    return {name: digest(Path(name).read_bytes()) for name in CURRENT_CODE_FILES}


def reviewed_code_versions() -> list[dict[str, str]]:
    """Return every user-approved collector version whose captures remain reusable."""
    document = read_code_versions(CODE_VERSIONS_PATH)
    versions = [entry.get("code_sha256") for entry in document.get("versions", [])]
    require(
        bool(versions) and all(isinstance(v, dict) and set(v) in (set(CODE_FILES), set(CURRENT_CODE_FILES)) for v in versions),
        "Reviewed MMD code versions are incomplete",
    )
    return versions


def load_plan(plan_sha256: str = PLAN_SHA256) -> dict:
    """Verify one narrowly authorized plan and all retained publisher references."""
    require(plan_sha256 in PLANS, "MMD authorization binding differs")
    raw = PLANS[plan_sha256].read_bytes()
    require(digest(raw) == plan_sha256, "MMD collection plan changed")
    plan = json.loads(raw)
    require(plan["endpoint"] == ENDPOINT and plan["model_eligible"] is False, "MMD scope changed")
    require(plan["registry_sha256"] == canonical_hash(load_registry(expected_sha256=plan["registry_sha256"])), "MMD registry changed")
    require(digest(Path(plan["comparison_report"]).read_bytes()) == plan["comparison_report_sha256"], "AMI parity evidence changed")
    for ref in plan["references"].values():
        path = Path(current(ref["path"]))
        require(not path.is_symlink() and digest(path.read_bytes()) == ref["sha256"], "MMD reference changed")
    if plan_sha256 == ADDITIONS_PLAN_SHA256:
        verify_additions(plan)
    return plan


def verify_additions(plan: dict) -> None:
    """Bind user-added conditions to the locked, still-valid registry additions and their holds."""
    from scripts.acquisition.source_registry import registry_version_paths

    additions_lock = ADDITIONS_LOCK_PATH
    if plan["registry_sha256"] != canonical_hash(load_registry()):
        additions_lock = registry_version_paths(plan["registry_sha256"])["additions_lock"]
    require(digest(additions_lock.read_bytes()) == plan["registry_additions_lock_sha256"], "Registry additions lock changed")
    added = {control["id"]: control for control in load_registry_additions(load_registry(expected_sha256=plan["registry_sha256"]))}
    require(
        all(
            c["measure_id"] in added
            and c["gate_status"] == added[c["measure_id"]]["gate_status"] == "not_run"
            and c["review_decision"] == added[c["measure_id"]]["preserved_controls"]["current_review_decision"]
            for c in plan["conditions"]
        ),
        "Additions plan differs from the locked registry additions",
    )


def plan_for(measure_id: str) -> tuple[str, dict]:
    """Return the hash and contents of the single approved plan that lists this measure."""
    found = [(sha, plan) for sha in PLANS for plan in [load_plan(sha)] if any(c["measure_id"] == measure_id for c in plan["conditions"])]
    require(bool(found), "Condition is not in the approved locked registry")
    require(len(found) == 1, "Condition is not in exactly one approved plan")
    return found[0]


def condition_for(plan: dict, measure_id: str, year: int) -> dict:
    """Resolve an offered branch without approving an unavailable year or addition."""
    matches = [c for c in plan["conditions"] if c["measure_id"] == measure_id]
    require(len(matches) == 1, "Condition is not in the approved locked registry")
    condition = matches[0]
    require(year in condition["available_years"], "availability_hold: approved menu does not offer this condition-year")
    require(condition["gate_status"] == "not_run" and condition["review_decision"].startswith("hold_"), "Review hold changed")
    return condition


def request_parameters(plan: dict, condition: dict, year: int) -> dict[str, str]:
    """Reproduce the independently checked CMS source crosswalk and exact filters."""
    menu = Path(current(plan["references"]["menus.js"]["path"])).read_text()
    matches = sorted(set(re.findall(r"'disp':\s*\"" + str(year) + r"\",\s*'val':\s*\"([^\"]+)\"", menu)))
    require(len(matches) == 1, "Ambiguous year code")
    code = matches[0]
    dimensions = {"population": "f", "measure": "v", "year": code, "elig": ".", "race_code": ".", "sex_code": ".", "adjust": "1", "dual": "."}
    with Path(current(plan["references"]["codebook_crosswalk.csv"]["path"])).open(newline="") as handle:
        candidates = [row for row in csv.DictReader(handle) if all(not row.get(k) or v in row[k] for k, v in dimensions.items())]
    require(bool(candidates) and candidates[0]["year"] == code, "Crosswalk first match differs from exact year")
    filters = dict(plan["filters"], condition=condition["condition_code"], geography=condition["geography"], year=code)
    return {"_source": candidates[0]["url"], **{k: ".|IS NULL" if v == "." else v for k, v in filters.items()}, "_size": str(PAGE_SIZE)}


def url_for(parameters: dict[str, str]) -> str:
    """Build only the approved public CMS endpoint, with no redirects or credentials."""
    return ENDPOINT + "?" + urlencode(parameters, quote_via=quote)


def selections_for(plan: dict, condition: dict, year: int) -> dict:
    """Retain approved labels; state-only sickle cell remains a separate geography."""
    return dict(
        plan["export_selections"],
        year=str(year),
        condition=condition["condition_label"],
        domain=condition["domain_label"],
        geography="County" if condition["geography"] == "c" else "State/Territory",
    )


def validate_rows(raw: bytes, parameters: dict) -> tuple[list[dict], dict]:
    """Reject unexpected fields, subgroups, identifiers or undocumented rate tokens."""
    rows = json.loads(raw)
    require(isinstance(rows, list) and 2 < len(rows) < PAGE_SIZE, "Empty, tiny or size-limited MMD response requires review")
    keys: set[str] = set()
    rate_tokens: dict[str, int] = {}
    for row in rows:
        require(isinstance(row, dict) and set(row) == FIELDS, "Unexpected API schema or fields")
        for key in FIELDS - {"rate", "dencat", "fips"}:
            expected = parameters[key]
            require(row[key] == expected or (expected == ".|IS NULL" and row[key] in (".", "", None)), f"Returned dimension mismatch: {key}")
        fips = row["fips"]
        width = 5 if parameters["geography"] == "c" else 2
        # CMS drops the leading zero for state codes 01-09 in 2012-2018; keep the value as published.
        pattern = r"[0-9]{4,5}" if width == 5 else r"[0-9]{2}"
        require(isinstance(fips, str) and re.fullmatch(pattern, fips) is not None, "Native FIPS width/value changed")
        padded = fips.zfill(width)
        require(padded not in keys, "Duplicate native FIPS")
        require((int(fips) >= 1000) if width == 5 else (0 < int(fips) < 100), "Unexpected geography ID")
        keys.add(padded)
        require(row["dencat"] in {"1", "2", "3", "4", "5"}, "Unknown denominator token requires review")
        token = row["rate"]
        require(isinstance(token, str), "Rate type changed")
        try:
            number = Decimal(token)
        except InvalidOperation as error:
            raise ValueError("Non-numeric rate token requires review; no imputation") from error
        require(number.is_finite() and 0 <= number <= (100000 if width == 2 else 100), "Unexpected prevalence rate")
        require(re.fullmatch(r"\d+(?:\.\d+)?", token) is not None, "Rate notation requires review")
        rate_tokens[token] = rate_tokens.get(token, 0) + 1
    return rows, {"rows": len(rows), "zero_rate_rows": rate_tokens.get("0", 0), "rate_tokens": rate_tokens, "native_fips_width": width}


def validate_transport(raw: bytes, evidence: dict, parameters: dict) -> None:
    """Check saved HTTP evidence and demonstrate honored offset plus empty termination."""
    main = evidence["main"]
    require(main["url"] == url_for(parameters) and main["status"] == 200, "Main request differs")
    require(main["sha256"] == digest(raw) and main["bytes"] == len(raw), "Main response checksum/length differs")
    require("application/json" in main["content_type"], "Main response type differs")
    rows = json.loads(raw)
    probes = evidence["probes"]
    require(len(probes) == 3, "Three completeness probes required")
    for probe, offset in zip(probes, (0, 2, len(rows)), strict=True):
        expected = dict(parameters, _size="2", _offset=str(offset))
        body = probe["body_utf8"].encode()
        require(probe["url"] == url_for(expected) and probe["status"] == 200, "Completeness probe request differs")
        require("application/json" in probe["content_type"] and digest(body) == probe["sha256"] and len(body) == probe["bytes"], "Probe bytes differ")
        require(json.loads(body) == rows[offset : offset + 2], "API offset, ordering or complete termination differs")


def reconstruct(plan: dict, condition: dict, year: int, rows: list[dict]) -> tuple[bytes, list[str]]:
    """Create a separate CSV using checked CMS formatting; never alter API JSON."""
    # Imported at execution time so storage_controls can invoke this module without an import cycle.
    from scripts.acquisition.compare_mmd_api import HEADERS, js_rate, lookup

    root = Path(current(plan["references"]["countynames.tsv"]["path"])).parent.parent
    names, _ = lookup(root, "countynames.tsv")
    urban, _ = lookup(root, "urban.tsv")
    headers = [h for h in HEADERS if condition["geography"] == "c" or h not in {"county", "urban"}]
    selections = selections_for(plan, condition, year)
    baseline = next(r for r in read_json(Path(plan["comparison_report"]))["years"] if r["year"] == year)
    original = Path(baseline["original_path"]).read_bytes()
    require(digest(original) == baseline["original_sha256"], "AMI geography baseline changed")
    baseline_blanks = {int(r["fips"]) for r in csv.DictReader(io.StringIO(original.decode())) if r["county"] == ""}
    lines = [",".join(headers)]
    for row in sorted(rows, key=lambda item: int(item["fips"])):
        fips = int(row["fips"])
        state_id = fips // 1000 if condition["geography"] == "c" else fips
        # CMS uses state code + 990 for beneficiaries whose county is unknown; the name stays blank, as in the AMI baseline.
        unknown_county = condition["geography"] == "c" and fips % 1000 == 990
        require(
            state_id in names and (fips in names or unknown_county or (condition["geography"] == "c" and fips in baseline_blanks)),
            "New missing geographic lookup requires review",
        )
        cells = selections | {
            "fips": str(fips),
            "county": names.get(fips, ""),
            "state": names[state_id],
            "urban": "Urban" if fips in urban else "Rural",
            "primary_denominator": plan["denominator_labels"][row["dencat"]],
            "analysis_value": js_rate(row["rate"]),
        }
        output = []
        for field in headers:
            value = cells[field]
            require('"' not in value and "\n" not in value and "\r" not in value, "CSV value escaping requires review")
            if field in {"condition", "primary_denominator"}:
                value = '"' + value + '"'
            else:
                require("," not in value, "Unquoted CSV comma requires review")
            output.append(value)
        lines.append(",".join(output))
    derived = "\r\n".join(lines).encode()
    parsed = list(csv.DictReader(io.StringIO(derived.decode())))
    require(len(parsed) == len(rows) and all(None not in row and None not in row.values() for row in parsed), "Derived CSV structure differs")
    require(all(all(row[k] == v for k, v in selections.items()) for row in parsed), "Derived selections differ")
    return derived, headers


def code_binding_complete(code_sha256: dict) -> bool:
    """Historical captures bind CODE_FILES; captures made since code_versions.py joined the fingerprint bind CURRENT_CODE_FILES."""
    return set(code_sha256) in (set(CODE_FILES), set(CURRENT_CODE_FILES))


def verify_capture(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Recompute MMD scope, completeness and derivation before the existing uploader."""
    require(source["source_id"] == "MMD" and not evidence_only, "Only complete approved MMD captures use this route")
    require(lineage.get("plan_sha256") in PLANS and lineage.get("mode") == "mmd_api", "MMD authorization binding differs")
    plan = load_plan(lineage["plan_sha256"])
    require(lineage.get("model_eligible") is False and lineage.get("review_decision", "").startswith("hold_"), "MMD review hold differs")
    require(code_binding_complete(lineage.get("code_sha256", {})), "MMD code binding is incomplete")
    require(lineage["code_sha256"] in reviewed_code_versions(), "MMD capture implementation changed; review before reuse")
    year = lineage["year"]
    condition = condition_for(plan, lineage["measure_id"], year)
    parameters = request_parameters(plan, condition, year)
    acquisition = receipt["acquisition"]
    require(acquisition["requested_url"] == url_for(parameters) and acquisition["resolved_url"] == url_for(parameters), "MMD endpoint/query differs")
    require(
        acquisition["request_parameters"] == parameters and acquisition["transport_mode"] == "api_response" and acquisition["request_method"] == "GET",
        "MMD transport differs",
    )
    require(acquisition["export_selections"] == selections_for(plan, condition, year), "MMD selections differ")
    require(acquisition["pagination"]["termination_verified"] is True and acquisition["pagination"]["page_count"] == 1, "MMD completeness not verified")
    roles = {a["role"]: a for a in receipt["artifacts"]}
    require(len(roles) == len(receipt["artifacts"]) == 4 and set(roles) == {"api_page", "data", "export_receipt", "layout"}, "MMD artifact set differs")
    for artifact in roles.values():
        path = root / artifact["storage_path"]
        require(path.resolve().is_relative_to(root.resolve()) and not path.is_symlink(), "Artifact escapes snapshot")
        require(digest(path.read_bytes()) == artifact["sha256"] and path.stat().st_size == artifact["byte_count"], "Artifact integrity differs")
    raw = (root / roles["api_page"]["storage_path"]).read_bytes()
    evidence = read_json(root / roles["export_receipt"]["storage_path"])
    validate_transport(raw, evidence, parameters)
    rows, _ = validate_rows(raw, parameters)
    derived, headers = reconstruct(plan, condition, year, rows)
    require(derived == (root / roles["data"]["storage_path"]).read_bytes(), "Derived CSV differs from retained raw response")
    require(roles["api_page"]["original_unchanged"] is True and roles["data"]["original_unchanged"] is False, "Original/derived lineage differs")
    require(receipt["schema_profile"]["native_headers"] == headers and receipt["schema_profile"]["row_count"] == len(rows), "Schema profile differs")
    require(receipt["governance"]["contains_pii"] is False and receipt["governance"]["contains_phi"] is False, "Privacy classification changed")
    bundle = read_json(root / roles["layout"]["storage_path"])
    require(set(bundle) == set(plan["references"]), "Reference bundle is incomplete")
    for name, reference in plan["references"].items():
        item = bundle[name]
        require(item["url"] == reference["url"] and digest(bytes.fromhex(item["body_hex"])) == reference["sha256"], "Reference bundle differs")
    require(
        lineage.get("derivation", {}).get("input_sha256") == digest(raw) and lineage["derivation"]["output_sha256"] == digest(derived),
        "Derivation hashes differ",
    )
    require(
        urlsplit(acquisition["requested_url"]).hostname == "data.cms.gov" and bool(parse_qsl(urlsplit(acquisition["requested_url"]).query)), "MMD host differs"
    )
