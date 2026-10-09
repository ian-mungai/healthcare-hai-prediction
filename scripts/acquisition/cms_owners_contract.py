"""Frozen scope and privacy checks for CMS Hospital All Owners releases (C067, C068).

Each original names individual owners, so it stays on this Mac only. S3 receives a
derived CSV of organisation owner rows, without name, title or street-address columns; kept cells are unchanged
except owner organisation or DBA names that contain an individual owner's whole name, which are redacted.
Failure modes: data/acquisition_planning/cms_owners_failure_modes.md.
"""

import csv
import io
import re
import unicodedata
from pathlib import Path

from scripts.acquisition.bls_api_contract import code_hashes, digest
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.legacy_versions import plan_matches
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require

PLAN_PATH = REPO_ROOT / "config/acquisition/cms_owners_plan.json"
VERSIONS_PATH = REPO_ROOT / "config/acquisition/cms_owners_code_versions.json"
SOURCE_ID = "CMS_OWNERS"
TERMS_URL = "https://www.usa.gov/government-works"
_COMMON = [
    "ENROLLMENT ID",
    "ASSOCIATE ID",
    "ORGANIZATION NAME",
    "ASSOCIATE ID - OWNER",
    "TYPE - OWNER",
    "ROLE CODE - OWNER",
    "ROLE TEXT - OWNER",
    "ASSOCIATION DATE - OWNER",
    "FIRST NAME - OWNER",
    "MIDDLE NAME - OWNER",
    "LAST NAME - OWNER",
    "TITLE - OWNER",
    "ORGANIZATION NAME - OWNER",
    "DOING BUSINESS AS NAME - OWNER",
    "ADDRESS LINE 1 - OWNER",
    "ADDRESS LINE 2 - OWNER",
    "CITY - OWNER",
    "STATE - OWNER",
    "ZIP CODE - OWNER",
    "PERCENTAGE OWNERSHIP",
    "CREATED FOR ACQUISITION - OWNER",
    "CORPORATION - OWNER",
    "LLC - OWNER",
    "MEDICAL PROVIDER SUPPLIER - OWNER",
    "MANAGEMENT SERVICES COMPANY - OWNER",
    "MEDICAL STAFFING COMPANY - OWNER",
    "HOLDING COMPANY - OWNER",
    "INVESTMENT FIRM - OWNER",
    "FINANCIAL INSTITUTION - OWNER",
    "CONSULTING FIRM - OWNER",
    "FOR PROFIT - OWNER",
    "NON PROFIT - OWNER",
]
# The two published layouts: through March 2025, and from April 2025 with the private-equity and REIT flags.
HEADERS = {
    "v1": [*_COMMON, "OTHER TYPE - OWNER", "OTHER TYPE TEXT - OWNER"],
    "v2": [
        *_COMMON,
        "PRIVATE EQUITY COMPANY - OWNER",
        "REIT - OWNER",
        "CHAIN HOME OFFICE - OWNER",
        "OTHER TYPE - OWNER",
        "OTHER TYPE TEXT - OWNER",
        "OWNED BY ANOTHER ORG OR IND - OWNER",
    ],
}
V2_FIRST_PERIOD = "2025-04-01"
PERSONAL = ["FIRST NAME - OWNER", "MIDDLE NAME - OWNER", "LAST NAME - OWNER", "TITLE - OWNER"]
DROPPED = [*PERSONAL, "ADDRESS LINE 1 - OWNER", "ADDRESS LINE 2 - OWNER", "CITY - OWNER", "ZIP CODE - OWNER"]
REDACTABLE = ["ORGANIZATION NAME - OWNER", "DOING BUSINESS AS NAME - OWNER"]
TEXT_COLUMNS = {"OTHER TYPE TEXT - OWNER"}
REPLACEMENT = "[REDACTED]"


def require_code(hashes: dict | None = None) -> dict:
    """Require explicit review of new captures and retain trusted historical implementations."""
    actual = code_hashes() if hashes is None else hashes
    require(bool(actual) and actual in [v["code_sha256"] for v in read_code_versions(VERSIONS_PATH)["versions"]], "Unreviewed CMS owners code version")
    return actual


def kept_columns(layout: str) -> list[str]:
    """Columns written to the derived file, in published order."""
    return [column for column in HEADERS[layout] if column not in DROPPED]


def flag_columns(layout: str) -> list[str]:
    """Published Y/N organisation-type flags."""
    header = HEADERS[layout]
    return [c for c in header[header.index("PERCENTAGE OWNERSHIP") + 1 :] if c not in TEXT_COLUMNS]


def layout_for(period_start: str) -> str:
    """The layout CMS published for a release period."""
    return "v2" if period_start >= V2_FIRST_PERIOD else "v1"


def release_id(release: dict) -> str:
    """Identify one release by its route, URL and period."""
    return canonical_hash({"route_id": release["route_id"], "url": release["url"], "period": [release["period_start"], release["period_end"]]})


def stored_name(release: dict) -> str:
    """A storage-safe local name for the original."""
    return f"hospital_all_owners_{release['period_start']}.csv"


def load_plan(path: Path | None = None) -> dict:
    """Verify the locked scope, per-release bindings and base registry."""
    path = PLAN_PATH if path is None else path
    plan = read_json(path)
    require(read_json(path.with_suffix(".lock.json"))["plan_sha256"] == canonical_hash(plan), "CMS owners plan lock differs")
    require(plan["source_id"] == SOURCE_ID and plan["model_eligible"] is False and plan["version"] == 1, "CMS owners plan hold differs")
    require(plan["headers"] == HEADERS and plan["dropped_columns"] == DROPPED and plan["replacement"] == REPLACEMENT, "CMS owners plan layout differs")
    require(plan["registry_sha256"] == canonical_hash(load_registry(expected_sha256=plan["registry_sha256"])), "CMS owners registry differs")
    source = next(s for s in load_registry(expected_sha256=plan["registry_sha256"])["sources"] if s["source_id"] == SOURCE_ID)
    routes = {r["route_id"]: r["url"] for r in source["file_routes"]}
    require(bool(plan["releases"]) and len({r["id"] for r in plan["releases"]}) == len(plan["releases"]), "CMS owners plan has no releases")
    for release in plan["releases"]:
        require(routes.get(release["route_id"]) == release["url"] and release["id"] == release_id(release), "CMS owners release binding differs")
        require(release["layout"] == layout_for(release["period_start"]), "CMS owners release layout differs")
        require(re.fullmatch(r"[0-9]{4}-[0-9]{2}-01", release["period_start"]) is not None, "CMS owners release period invalid")
    return plan


def normalized(value: str) -> str:
    """Case-, width- and space-insensitive text for transient name matching."""
    return " ".join(re.sub(r"[^\w]+", " ", unicodedata.normalize("NFKC", value)).split()).casefold()


def decode(raw: bytes) -> tuple[str, str]:
    """UTF-8 (with or without BOM) first; Windows-1252 only when UTF-8 fails."""
    try:
        return raw.decode("utf-8-sig"), "utf-8"
    except UnicodeDecodeError:
        return raw.decode("cp1252"), "cp1252"


def contains_name(text: str, names: set[tuple[str, ...]], longest: int) -> bool:
    """True when the normalized text contains any individual's whole name as consecutive words."""
    words = tuple(normalized(text).split())
    return any(words[i : i + n] in names for n in range(2, longest + 1) for i in range(len(words) - n + 1))


def derive(raw: bytes, release: dict) -> tuple[bytes, dict]:
    """Organisation owner rows only; refuse schema, type or leftover-personal-data defects."""
    text, encoding = decode(raw)
    table = list(csv.reader(io.StringIO(text, newline=""), strict=True))
    header = HEADERS[release["layout"]]
    require(bool(table) and table[0] == header, "CMS owners header differs")
    index = {column: i for i, column in enumerate(header)}
    names: set[tuple[str, ...]] = set()
    organisations: list[list[str]] = []
    individuals = 0
    for row in table[1:]:
        require(len(row) == len(header), "CMS owners row shape differs")
        kind = row[index["TYPE - OWNER"]]
        require(kind in {"I", "O"}, "CMS owners type differs")
        if kind == "I":
            individuals += 1
            first, middle, last = (normalized(row[index[c]]).split() for c in PERSONAL[:3])
            if first and last:
                names.add((*first, *last))
                if middle:
                    names.add((*first, *middle, *last))
            continue
        require(all(row[index[c]] == "" for c in PERSONAL), "CMS owners organisation row carries a personal name")
        require(row[index["ENROLLMENT ID"]] != "", "CMS owners organisation row has no enrollment ID")
        require(all(row[index[c]] in {"Y", "N", ""} for c in flag_columns(release["layout"])), "CMS owners flag value differs")
        organisations.append(row)
    require(bool(organisations), "CMS owners release has no organisation rows")
    longest = max((len(n) for n in names), default=0)
    columns = kept_columns(release["layout"])
    redacted = 0
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(columns)
    for row in organisations:
        values = dict(zip(header, row, strict=True))
        for column in REDACTABLE:
            if values[column] and names and contains_name(values[column], names, longest):
                values[column] = REPLACEMENT
                redacted += 1
        writer.writerow([values[c] for c in columns])
    flags = {c: sum(r[index[c]] == "Y" for r in organisations) for c in flag_columns(release["layout"])}
    statistics = {
        "rows_read": len(table) - 1,
        "individual_rows_dropped": individuals,
        "organisation_rows_kept": len(organisations),
        "redacted_name_cells": redacted,
        "blank_percentage_rows": sum(r[index["PERCENTAGE OWNERSHIP"]] == "" for r in organisations),
        "enrollments": len({r[index["ENROLLMENT ID"]] for r in organisations}),
        "flag_yes_counts": flags,
        "encoding": encoding,
        "layout": release["layout"],
        "model_eligible": False,
    }
    require(statistics["rows_read"] == individuals + len(organisations), "CMS owners row counts differ")
    return output.getvalue().encode(), statistics


def verify_capture(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Rebuild the derived file from the local original; check the artifact set, bindings and holds."""
    from scripts.acquisition.capture import receipt_validator

    require(not evidence_only and source["source_id"] == SOURCE_ID, "CMS owners storage source differs")
    plan = load_plan()
    require(plan_matches(plan, lineage["plan_sha256"]), "CMS owners capture plan differs")
    require(lineage["registry_sha256"] == canonical_hash(load_registry(expected_sha256=lineage["registry_sha256"])), "CMS owners registry differs")
    require(lineage["schema_sha256"] == canonical_hash(receipt_validator().schema), "CMS owners schema differs")
    require(lineage["model_eligible"] is False and receipt["snapshot_status"] == "acquired_unvalidated", "CMS owners modeling hold differs")
    require_code(lineage["code_sha256"])
    matches = [r for r in plan["releases"] if r["id"] == lineage["release_id"]]
    require(len(matches) == 1, "CMS owners capture release not in its plan")
    release = matches[0]
    paths = {a["storage_path"] for a in receipt["artifacts"]}
    require(paths == {"derived/organisation_owners.csv", "evidence/download_proof.json", "references/scope.json"}, "CMS owners artifact set differs")
    require(not (root / "raw").exists() and not (root / "audit").exists(), "CMS owners snapshot holds original bytes")
    proof = read_json(root / "evidence/download_proof.json")
    collection = root.parents[2]
    original = (collection / proof["original_path"]).resolve()
    require(
        original.is_relative_to((collection / "private_original").resolve()) and not original.is_relative_to(root.resolve()),
        "CMS owners original location differs",
    )
    raw = original.read_bytes()
    require(digest(raw) == lineage["expected_sha256"] == proof["sha256"] and len(raw) == proof["bytes"], "CMS owners original hash differs")
    derived, statistics = derive(raw, release)
    require((root / "derived/organisation_owners.csv").read_bytes() == derived, "CMS owners derived CSV differs")
    require(proof["statistics"] == statistics and proof["requested_url"] == release["url"], "CMS owners download proof differs")
    require(canonical_hash(read_json(root / "references/scope.json")) == lineage["plan_sha256"], "CMS owners stored scope differs")
    acq = receipt["acquisition"]
    require(
        acq["requested_url"] == release["url"] and acq["http_status"] == 200 and acq["retrieved_at_utc"] == proof["retrieved_at_utc"],
        "CMS owners route differs",
    )
    require(receipt["schema_profile"]["native_headers"] == kept_columns(release["layout"]), "CMS owners schema profile differs")
    require(receipt["schema_profile"]["row_count"] == statistics["organisation_rows_kept"], "CMS owners schema profile differs")
    require(receipt["governance"]["contains_pii"] is False and receipt["governance"]["license_or_terms_url"] == TERMS_URL, "CMS owners governance differs")
    period = receipt["measurement_periods"]
    require(
        len(period) == 1 and (period[0]["start_date"], period[0]["end_date"]) == (release["period_start"], release["period_end"]), "CMS owners period differs"
    )
