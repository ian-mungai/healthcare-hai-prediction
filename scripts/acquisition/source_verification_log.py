"""Build and check the data source verification log from the committed registry, lock, layout, bronze map and closeout record."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from scripts.acquisition.source_registry import REPO_ROOT, RegistryError, load_registry, read_json, require

CONFIG = REPO_ROOT / "config" / "acquisition"
LOG_PATH = CONFIG / "source_verification_log.json"
ALLOWLIST_PATH = CONFIG / "source_verification_text_allowlist.json"
BRONZE_PATH = REPO_ROOT / "config" / "lakehouse" / "bronze_tables.json"
LOG_VERSION = 1
NOT_RECORDED = "not recorded"
STATUSES = ("verified", "deferred", "validation-only", "rejected", NOT_RECORDED)
ROW_FIELDS = (
    "dataset_id",
    "title",
    "publisher",
    "collection",
    "route",
    "route_class",
    "access_and_terms",
    "data_class",
    "data_class_basis",
    "verification",
    "evidence",
    "status",
    "status_reason",
    "deferral_trigger",
    "not_recorded",
)
NESTED_FIELDS = {
    "access_and_terms": ("text", "source"),
    "verification": ("date", "basis"),
    "evidence": (
        "registry_record_sha256",
        "registry_record_pointer",
        "capture_candidates",
        "unique_snapshot_ids",
        "receipt_sha256",
        "object_version",
        "receipt_path",
    ),
}
CARE_VALIDATION = "Validation table, never a predictor (project guide, pipeline section)"
VALIDATION_ONLY = {
    "care-hac-cms": "External validation check and RQ3 mediator source, never a main-model predictor: circular with PSI-90 and HAI (goal card)",
    "care-deaths-cms": "Validation table and RQ3 mediator source, never a main-model predictor (project guide, sources section)",
    "care-hrrp-cms": CARE_VALIDATION,
    "care-vbp-cms": CARE_VALIDATION,
    "care-visits-cms": CARE_VALIDATION,
}
REJECTED = {
    "AHRF": "Excluded: not a predictor source, because its license limits sharing. The project stores no AHRF data (goal card; project guide section 8)",
    "NHSN": "Excluded: National Healthcare Safety Network facility-level data needs a formal Data Use Agreement; rejected (goal card; guide section 8)",
}
REJECTED_CLASS = {"AHRF": "Not stored", "NHSN": "Not collected"}
CARE_COMPARE_TERMS = ("CMS Care Compare", "Free, public, no Data Use Agreement (DUA)")
# Transcribed from the project guide, section 8 (data sources table): publisher and the access and terms column.
GUIDE_SECTION_8 = {
    "main-hai-pdc": CARE_COMPARE_TERMS,
    "care-general-cms": CARE_COMPARE_TERMS,
    "care-hcahps-cms": CARE_COMPARE_TERMS,
    "care-timely-cms": CARE_COMPARE_TERMS,
    "care-hac-cms": ("CMS", "Free, public, no DUA"),
    "CMS_HCRIS_PUF": ("CMS", "Free, public, quarterly, no DUA"),
    "main-cmi-ipps": ("CMS", "Free, public, annual, no DUA"),
    "SVI": ("CDC and the Agency for Toxic Substances and Disease Registry (ATSDR)", "Free, public, county and tract level, no DUA"),
    "AHRF": ("Health Resources and Services Administration (HRSA)", "Free, public, annual, county-level, no DUA"),
    "NHSN": ("CDC", "Requires a formal Data Use Agreement"),
}
GUIDE_BUILT = frozenset({"main-hai-pdc", "care-general-cms", "care-hcahps-cms", "care-timely-cms", "care-hac-cms", "CMS_HCRIS_PUF", "main-cmi-ipps", "SVI"})
# Publisher names the project guide uses for the layout's publisher folders (section 8 and its data classification text).
PUBLISHER_NAMES = {
    "cms": "CMS",
    "census": "Census Bureau",
    "bls": "BLS",
    "cdc": "CDC",
    "hud": "HUD",
    "hrsa": "HRSA",
    "onc": "ONC",
    "hhs": "HHS",
    "usda": "USDA",
    "ahrq": "AHRQ",
    "california_hcai": "Department of Health Care Access and Information (HCAI)",
}
ROUTE_CLASSES = {
    "file_download": "download URL",
    "api_fallback": "API",
    "file_plus_api_fallback": "download URL with API fallback",
    "web_export": "manual export (permitted web export)",
    "documentation_only": "reference documents only",
}
TERMS_ACCEPTANCE_SOURCES = ("HUD", "WONDER")
QUESTIONS = {
    "publisher": "Which publisher name does the project guide give for this source? The layout folder has no guide name.",
    "route_class": "Which route (API, download URL or manual) delivered this source? The registry records only an access hold.",
    "verification.date": "On what date was this source verified? Neither the registry nor the committed records give a date for it.",
    "evidence.capture_candidates": "Where is the capture count for this source recorded? The collection closeout record does not list it.",
    "evidence.unique_snapshot_ids": "Where is the unique snapshot count for this source recorded? The collection closeout record does not list it.",
    "evidence.receipt_sha256": "Which committed file should hold each source's receipt hash? The registry holds none and receipts stay under local data.",
    "evidence.object_version": "Which committed file should hold each source's S3 object version? The registry holds none and the readback stays local.",
    "evidence.receipt_path": "Which receipt path should the log name per source? The registry records none and receipts stay under local data.",
    "deferral_trigger": "What event ends the deferral of the state sources (group E)? Project fact 10 names no trigger.",
    "data_class": "Which data class does the project assign this source? Its collection has no class in the bronze table map.",
    "status": "What evidence verifies this source? Neither the closeout record nor the project guide lists it.",
}
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
PHONE = re.compile(r"(?<!\w)(?:\+?1[ .-]?)?\(?\d{3}\)?[ .-]?\d{3}[ .-]?\d{4}(?!\w)")
RECORD_SHA256 = re.compile(r"[0-9a-f]{64}")
RECORD_POINTER = re.compile(r"acquisition-manifest\.json#/source_records/\d+")
ROUTES = frozenset({*ROUTE_CLASSES, "access_hold"})


def load_inputs(root: Path = REPO_ROOT) -> dict[str, Any]:
    """Return every committed input of the log, with the registry checked against its lock."""
    config = root / "config" / "acquisition"
    registry = load_registry(config / "source_registry.json", config / "source_registry_lock.json")
    return {
        "registry": registry,
        "lock": read_json(config / "source_registry_lock.json"),
        "layout": read_json(config / "collection_layout.json"),
        "bronze": read_json(root / "config" / "lakehouse" / "bronze_tables.json"),
        "closeout": read_json(config / "collection_closeout.json"),
        "terms": read_json(config / "terms_acceptance_20260928.json"),
        "allowlist": read_json(config / ALLOWLIST_PATH.name),
        "releases": [read_json(config / name) for name in ("access_releases_20260929.json", "access_releases_20261002.json")],
    }


def collections_by_source(layout: dict[str, Any]) -> dict[str, tuple[str, str]]:
    """Map each registry source ID to its publisher folder and its collection path."""
    return {source: (group["publisher"], f"{group['publisher']}/{group['collection']}") for group in layout["groups"] for source in group["source_ids"]}


def bronze_groups(bronze: dict[str, Any]) -> dict[str, set[str]]:
    """Return the table groups each bronze collection holds."""
    groups: dict[str, set[str]] = {}
    for table in bronze["tables"]:
        groups.setdefault(table["collection"], set()).add(table["group"])
    return groups


def unresolved_bronze_collections(inputs: dict[str, Any]) -> list[str]:
    """Return a problem for each bronze collection without a collection layout entry and for each layout source the registry does not hold."""
    by_collection: dict[str, list[str]] = {}
    for source, (_, collection) in collections_by_source(inputs["layout"]).items():
        by_collection.setdefault(collection, []).append(source)
    registry_ids = {source["source_id"] for source in inputs["registry"]["sources"]}
    problems = []
    for collection in sorted(bronze_groups(inputs["bronze"])):
        sources = by_collection.get(collection)
        if not sources:
            problems.append(f"bronze collection {collection} has no entry in the collection layout")
            continue
        problems.extend(
            f"bronze collection {collection} names layout source {source}, which the registry does not hold" for source in sorted(set(sources) - registry_ids)
        )
    return problems


def bronze_source_ids(inputs: dict[str, Any]) -> list[str]:
    """Return the registry sources whose collection the bronze table map loads, in sorted order."""
    groups = bronze_groups(inputs["bronze"])
    return sorted(source for source, (_, collection) in collections_by_source(inputs["layout"]).items() if collection in groups)


def logged_source_ids(inputs: dict[str, Any]) -> list[str]:
    """Return the sources that need a row: every bronze source, the validation-only table and the rejected sources."""
    return sorted({*bronze_source_ids(inputs), *VALIDATION_ONLY, *REJECTED})


def dated_decisions(inputs: dict[str, Any]) -> dict[str, tuple[str, str]]:
    """Return the date and basis of each dated record that names a source: terms acceptance, access releases and the owner source decision."""
    dates: dict[str, tuple[str, str]] = {}
    for decision in inputs["closeout"].get("user_decisions", []):
        for source in decision.get("sources", {}):
            if "decision_date" in decision:
                dates[source] = (decision["decision_date"], "owner source decision recorded in the registry closeout")
    for source in TERMS_ACCEPTANCE_SOURCES:
        dates[source] = (inputs["terms"]["decision_date"], "owner terms acceptance record terms_acceptance_20260928.json")
    names = ("access_releases_20260929.json", "access_releases_20261002.json")
    for name, record in zip(names, inputs["releases"], strict=True):
        for release in record["releases"]:
            if release.get("released"):
                dates[release["source_id"]] = (record["recorded_at_utc"][:10], f"access release record {name}")
    return dates


def shape_problems(name: str, route: Any, record_sha256: Any, pointer: Any) -> list[str]:
    """Return the problems of the registry-derived fields that are not free text: the route, the record hash and the record pointer."""
    problems = []
    if route not in ROUTES:
        problems.append(f"{name}: route {route!r} is not on the route allowlist")
    if not (isinstance(record_sha256, str) and RECORD_SHA256.fullmatch(record_sha256)):
        problems.append(f"{name}: registry_record_sha256 is not a SHA-256 hex digest")
    if not (isinstance(pointer, str) and RECORD_POINTER.fullmatch(pointer)):
        problems.append(f"{name}: registry_record_pointer does not have the form acquisition-manifest.json#/source_records/<number>")
    return problems


def build_row(source: dict[str, Any], inputs: dict[str, Any], groups: dict[str, set[str]], dates: dict[str, tuple[str, str]]) -> dict[str, Any]:
    """Build one log row from the registry record and the committed records."""
    source_id = source["source_id"]
    publisher_slug, collection = collections_by_source(inputs["layout"])[source_id]
    guide = GUIDE_SECTION_8.get(source_id)
    allowlist = inputs["allowlist"]
    closeout = inputs["closeout"]["sources"].get(source_id)
    reference = source["research_reference"]
    route = source["preferred_route"]
    pointer = f"{reference['input']}#{reference['json_pointer']}"
    shape = shape_problems(source_id, route, reference["record_sha256"], pointer)
    require(not shape, "; ".join(shape))
    require(allowlist["titles"].get(source_id) == source["title"], f"{source_id}: the registry title is not on the allowlist {ALLOWLIST_PATH.name}")
    route_class = "not used (rejected)" if source_id in REJECTED else ROUTE_CLASSES.get(route, NOT_RECORDED)
    if guide:
        terms = {"text": guide[1], "source": "project guide section 8"}
    else:
        registry_terms = source["validation"]["reviewed_access_terms"]
        require(registry_terms in allowlist["registry_terms"], f"{source_id}: the registry access terms are not on the allowlist {ALLOWLIST_PATH.name}")
        terms = {"text": registry_terms, "source": "registry validation.reviewed_access_terms"}
    if source_id in REJECTED_CLASS:
        data_class, class_basis = REJECTED_CLASS[source_id], "project guide section 8"
    else:
        data_class = inputs["bronze"]["classification"]["collections"].get(collection, {}).get("class", NOT_RECORDED)
        class_basis = "config/lakehouse/bronze_tables.json collection class"
    date, basis = dates.get(source_id, (NOT_RECORDED, "no dated record names this source"))
    candidates = closeout["capture_candidates"] if closeout else NOT_RECORDED
    snapshots = closeout["unique_snapshot_ids"] if closeout else NOT_RECORDED
    trigger: str | None = None
    if source_id in REJECTED:
        status, reason = "rejected", REJECTED[source_id]
    elif source_id in VALIDATION_ONLY:
        status, reason = "validation-only", VALIDATION_ONLY[source_id]
    elif groups.get(collection) == {"state_sources"}:
        status, reason, trigger = "deferred", "State source (group E), deferred by project fact 10", NOT_RECORDED
    elif closeout and candidates == snapshots and isinstance(candidates, int) and candidates > 0:
        status, reason = "verified", f"Collection closeout records {candidates} capture candidates, each with its own snapshot"
    elif source_id in GUIDE_BUILT:
        status, reason = "verified", "Project guide section 8: built in bronze and silver"
    else:
        status, reason = NOT_RECORDED, "No closeout capture record and no project guide row"
    row: dict[str, Any] = {
        "dataset_id": source_id,
        "title": source["title"],
        "publisher": guide[0] if guide else PUBLISHER_NAMES.get(publisher_slug, NOT_RECORDED),
        "collection": collection,
        "route": route,
        "route_class": route_class,
        "access_and_terms": terms,
        "data_class": data_class,
        "data_class_basis": class_basis,
        "verification": {"date": date, "basis": basis},
        "evidence": {
            "registry_record_sha256": reference["record_sha256"],
            "registry_record_pointer": pointer,
            "capture_candidates": candidates,
            "unique_snapshot_ids": snapshots,
            "receipt_sha256": NOT_RECORDED,
            "object_version": NOT_RECORDED,
            "receipt_path": NOT_RECORDED,
        },
        "status": status,
        "status_reason": reason,
        "deferral_trigger": trigger,
    }
    row["not_recorded"] = not_recorded_fields(row)
    return row


def not_recorded_fields(row: dict[str, Any]) -> list[str]:
    """Return the dotted names of the row fields that hold the literal "not recorded"."""
    found: list[str] = []
    for field in ROW_FIELDS:
        value = row.get(field)
        if isinstance(value, dict):
            found.extend(f"{field}.{name}" for name in NESTED_FIELDS.get(field, ()) if value.get(name) == NOT_RECORDED)
        elif value == NOT_RECORDED and field != "dataset_id":
            found.append(field)
    return sorted(found)


def build_log(inputs: dict[str, Any]) -> dict[str, Any]:
    """Build the whole log: one row per logged source, sorted by dataset ID, plus a question for every fact marked not recorded."""
    unresolved = unresolved_bronze_collections(inputs)
    require(not unresolved, "; ".join(unresolved))
    sources = {source["source_id"]: source for source in inputs["registry"]["sources"]}
    groups = bronze_groups(inputs["bronze"])
    dates = dated_decisions(inputs)
    rows = [build_row(sources[source_id], inputs, groups, dates) for source_id in logged_source_ids(inputs)]
    used = sorted({field for row in rows for field in row["not_recorded"]})
    questions = [{"field": field, "question": QUESTIONS[field]} for field in used]
    return {
        "log_version": LOG_VERSION,
        "generated_by": "scripts/acquisition/source_verification_log.py",
        "generated_from": {"registry_sha256": inputs["lock"]["registry_sha256"], "closeout_inventory_sha256": inputs["closeout"]["inventory_sha256"]},
        "questions": questions,
        "rows": rows,
    }


def render(log: dict[str, Any]) -> str:
    """Return the committed text form of the log."""
    return json.dumps(log, indent=2, ensure_ascii=False) + "\n"


def personal_data_in(value: Any) -> bool:
    """Return whether any string inside the value looks like an email address or a phone number."""
    if isinstance(value, str):
        return bool(EMAIL.search(value) or PHONE.search(value))
    if isinstance(value, dict):
        return any(personal_data_in(item) for item in value.values())
    if isinstance(value, list):
        return any(personal_data_in(item) for item in value)
    return False


def check_row(row: dict[str, Any], record_sha256: str | None, question_fields: set[str], allowlist: dict[str, Any]) -> list[str]:
    """Return the problems of one row: unknown fields, personal data, free text off the allowlist, stale evidence and facts without a question."""
    name = str(row.get("dataset_id", "?"))
    problems = [f"{name}: unexpected field {field}" for field in sorted(set(row) - set(ROW_FIELDS))]
    for field, allowed in NESTED_FIELDS.items():
        if isinstance(row.get(field), dict):
            problems.extend(f"{name}: unexpected field {field}.{extra}" for extra in sorted(set(row[field]) - set(allowed)))
    if personal_data_in(row):
        problems.append(f"{name}: personal data (email address or phone number) in the row")
    evidence = row["evidence"] if isinstance(row.get("evidence"), dict) else {}
    problems.extend(shape_problems(name, row.get("route"), evidence.get("registry_record_sha256"), evidence.get("registry_record_pointer")))
    if allowlist["titles"].get(name) != row.get("title"):
        problems.append(f"{name}: title is not on the allowlist {ALLOWLIST_PATH.name}")
    terms = row.get("access_and_terms")
    guide_terms = {text for _, text in GUIDE_SECTION_8.values()}
    if not isinstance(terms, dict) or terms.get("text") not in {*guide_terms, *allowlist["registry_terms"]}:
        problems.append(f"{name}: terms text is not on the allowlist {ALLOWLIST_PATH.name} or the project guide table")
    if record_sha256 is not None and row.get("evidence", {}).get("registry_record_sha256") != record_sha256:
        problems.append(f"{name}: stale evidence, registry_record_sha256 no longer matches the registry record")
    if row.get("status") not in STATUSES:
        problems.append(f"{name}: status {row.get('status')!r} is not one of {', '.join(STATUSES)}")
    problems.extend(f"{name}: no question for {field}" for field in row.get("not_recorded", []) if field not in question_fields)
    if row.get("not_recorded") != not_recorded_fields(row):
        problems.append(f"{name}: not_recorded does not list the facts marked {NOT_RECORDED}")
    return problems


def check_log(log: dict[str, Any], inputs: dict[str, Any]) -> list[str]:
    """Return every problem found when the log is compared with the registry and the other inputs; an empty list is a pass."""
    problems = []
    if log.get("generated_from", {}).get("registry_sha256") != inputs["lock"]["registry_sha256"]:
        problems.append("stale evidence: generated_from.registry_sha256 no longer matches the registry lock")
    if personal_data_in(inputs["allowlist"]):
        problems.append(f"personal data (email address or phone number) in {ALLOWLIST_PATH.name}")
    unresolved = unresolved_bronze_collections(inputs)
    problems.extend(unresolved)
    rows = log.get("rows", [])
    seen = [row.get("dataset_id") for row in rows]
    problems.extend(f"duplicate row for {name}" for name in sorted({name for name in seen if seen.count(name) > 1}))
    expected = logged_source_ids(inputs)
    problems.extend(f"missing row for {name}" for name in expected if name not in seen)
    problems.extend(f"unexpected row for {name}" for name in seen if name not in expected)
    records = {source["source_id"]: source["research_reference"]["record_sha256"] for source in inputs["registry"]["sources"]}
    question_fields = {question["field"] for question in log.get("questions", [])}
    for row in rows:
        problems.extend(check_row(row, records.get(row.get("dataset_id")), question_fields, inputs["allowlist"]))
    if not unresolved and log != build_log(inputs):
        problems.append("the log differs from a fresh build; run python -m scripts.acquisition.source_verification_log")
    return problems


def check_file(path: Path, inputs: dict[str, Any]) -> list[str]:
    """Return the problems of the committed file: duplicate keys, text other than the canonical serialization, and every check_log problem."""
    try:
        log = read_json(path)
    except RegistryError as error:
        return [str(error)]
    problems = check_log(log, inputs)
    if path.read_text(encoding="utf-8") != render(log):
        problems.append("the file text is not the canonical serialization of its content; run python -m scripts.acquisition.source_verification_log")
    return problems


def write_log(log: dict[str, Any], path: Path = LOG_PATH) -> bool:
    """Write the log when its text changed, through a temporary file; return whether the file changed."""
    text = render(log)
    if path.exists() and path.read_text(encoding="utf-8") == text:
        return False
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)
    return True


def main() -> None:
    """Write the log, or with --check fail when a bronze source lacks a row or a row no longer matches the registry."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="check the committed log and change nothing")
    arguments = parser.parse_args()
    try:
        inputs = load_inputs()
    except RegistryError as error:
        parser.error(str(error))
    if not arguments.check:
        changed = write_log(build_log(inputs))
        sys.stdout.write(f"Source verification log {'written' if changed else 'unchanged'}: {LOG_PATH.relative_to(REPO_ROOT)}\n")
        return
    if not LOG_PATH.exists():
        parser.exit(1, f"Missing {LOG_PATH.relative_to(REPO_ROOT)}\n")
    problems = check_file(LOG_PATH, inputs)
    if problems:
        parser.exit(1, "Source verification log check failed:\n" + "\n".join(f"- {problem}" for problem in problems) + "\n")
    sys.stdout.write(f"Source verification log valid: {len(logged_source_ids(inputs))} rows\n")


if __name__ == "__main__":
    main()
