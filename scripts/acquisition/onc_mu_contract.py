"""Frozen scope and checks for ONC's meaningful-use attestation file (2011-2017).

All the data is public. Under plan version 2 the file is stored whole under raw/ as published; its receipt records that it names
individual clinicians, whom bronze pseudonymizes at load. The version 1 captures (hospital rows only, rebuilt from an original kept on the Mac)
are retired.
It is a separate "Medicare meaningful use" series, not the 2023-2024 Promoting Interoperability file.
Failure modes: data/acquisition_planning/onc_mu_hospital_failure_modes.md.
"""

import csv
import re
from collections import Counter
from pathlib import Path

from scripts.acquisition.code_closure import closure_hashes
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.legacy_versions import plan_matches
from scripts.acquisition.s3_store import fingerprint
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require

PLAN_PATH = REPO_ROOT / "config/acquisition/onc_mu_hospital_plan.json"
VERSIONS_PATH = REPO_ROOT / "config/acquisition/onc_mu_code_versions.json"
CODE_ENTRY = "scripts.acquisition.store_onc_mu"
SOURCE_ID = "ONC_PI"
TERMS_URL = "https://healthit.gov/data/datasets/ehr-products-used-meaningful-use-attestation/"
HOSPITAL = "Hospital"
PROVIDER_TYPES = {"Hospital", "EP"}
DROPPED = ["Specialty"]
FILE_NAME = "mu_report.csv"
PSEUDONYMIZE_AT_LOAD = "Names individual clinicians as published; bronze pseudonymizes the person fields at load."
REQUIRED_YEARS = ["2011", "2012", "2013", "2014", "2015", "2016"]
MAX_BYTES = 600 * 1024**2
MAX_SECONDS = 3600


def collector_code_hashes(entry: str = CODE_ENTRY) -> dict[str, str]:
    """Bind the entry module of the collector and every repository module it can run, and no other file."""
    return closure_hashes(entry)


def require_code(hashes: dict | None = None) -> dict:
    """Require explicit review of new captures and retain trusted historical implementations."""
    actual = collector_code_hashes() if hashes is None else hashes
    require(bool(actual) and actual in [v["code_sha256"] for v in read_code_versions(VERSIONS_PATH)["versions"]], "Unreviewed ONC meaningful-use code version")
    return actual


def file_id(plan: dict) -> str:
    """Identify the file by URL and recorded bytes."""
    return canonical_hash({"url": plan["url"], "sha256": plan["sha256"], "bytes": plan["bytes"]})


def load_plan(path: Path | None = None) -> dict:
    """Verify the locked scope, file binding and base registry."""
    path = PLAN_PATH if path is None else path
    plan = read_json(path)
    require(read_json(path.with_suffix(".lock.json"))["plan_sha256"] == canonical_hash(plan), "ONC meaningful-use plan lock differs")
    require(plan["source_id"] == SOURCE_ID and plan["model_eligible"] is False and plan["version"] == 2, "ONC meaningful-use plan hold differs")
    require(plan["registry_sha256"] == canonical_hash(load_registry(expected_sha256=plan["registry_sha256"])), "ONC meaningful-use registry differs")
    require(not {"dropped_columns", "hospital_value"} & set(plan) and "Provider_Type" in plan["header"], "ONC meaningful-use plan rules differ")
    require(plan["id"] == file_id(plan) and re.fullmatch(r"[0-9a-f]{64}", plan["sha256"]) is not None, "ONC meaningful-use file binding differs")
    require(0 < plan["bytes"] <= MAX_BYTES and plan["limits"] == {"max_bytes": MAX_BYTES, "max_seconds": MAX_SECONDS}, "ONC meaningful-use limits differ")
    return plan


def encoding_of(path: Path) -> str:
    """UTF-8 (with or without BOM) when the whole file decodes; otherwise Windows-1252."""
    try:
        with path.open(encoding="utf-8-sig") as handle:
            while handle.read(1024 * 1024):
                pass
        return "utf-8-sig"
    except UnicodeDecodeError:
        return "cp1252"


def check_raw(path: Path, plan: dict) -> dict:
    """Stream the file and check that it is the published shape; the bytes are stored unchanged."""
    encoding = encoding_of(path)
    header = plan["header"]
    index = {c: i for i, c in enumerate(header)}
    kinds: Counter[str] = Counter()
    years: Counter[str] = Counter()
    rows = 0
    with path.open(encoding=encoding, newline="") as handle:
        reader = csv.reader(handle, strict=True)
        require(next(reader, None) == header, "ONC meaningful-use header differs")
        for row in reader:
            rows += 1
            require(len(row) == len(header), "ONC meaningful-use row shape differs")
            kind = row[index["Provider_Type"]]
            require(kind in PROVIDER_TYPES, "ONC meaningful-use provider type differs")
            kinds[kind] += 1
            if kind == HOSPITAL:
                require(row[index["CCN"]] != "", "ONC meaningful-use hospital row has no CCN")
                years[row[index["Program_Year"]]] += 1
    require(all(years[y] > 0 for y in REQUIRED_YEARS), "ONC meaningful-use program years missing")
    return {
        "rows_read": rows,
        "clinician_rows": kinds["EP"],
        "hospital_rows": kinds[HOSPITAL],
        "hospital_rows_by_program_year": dict(sorted(years.items())),
        "encoding": encoding,
        "model_eligible": False,
    }


def verify_capture(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Check the artifact set, bindings and holds: a capture holds the file as published."""
    from scripts.acquisition.capture import receipt_validator

    require(not evidence_only and source["source_id"] == SOURCE_ID, "ONC meaningful-use storage source differs")
    plan = load_plan()
    require(plan_matches(plan, lineage["plan_sha256"]) and lineage["file_id"] == plan["id"], "ONC meaningful-use capture plan differs")
    require(lineage["registry_sha256"] == canonical_hash(load_registry(expected_sha256=lineage["registry_sha256"])), "ONC meaningful-use registry differs")
    require(lineage["schema_sha256"] == canonical_hash(receipt_validator().schema), "ONC meaningful-use schema differs")
    require(lineage["model_eligible"] is False and receipt["snapshot_status"] == "acquired_unvalidated", "ONC meaningful-use modeling hold differs")
    require_code(lineage["code_sha256"])
    paths = {a["storage_path"] for a in receipt["artifacts"]}
    require(lineage["plan_sha256"] == canonical_hash(plan), "ONC meaningful-use capture plan differs")
    require(paths == {f"raw/{FILE_NAME}", "evidence/download_proof.json", "references/scope.json"}, "ONC meaningful-use artifact set differs")
    require(not (root / "derived").exists(), "ONC meaningful-use snapshot holds a derived copy")
    stored = root / "raw" / FILE_NAME
    proof = read_json(root / "evidence/download_proof.json")
    require(fingerprint(stored) == (plan["sha256"], plan["bytes"]) and proof["sha256"] == plan["sha256"], "ONC meaningful-use file hash differs")
    statistics = check_raw(stored, plan)
    require(proof["statistics"] == statistics and proof["requested_url"] == plan["url"], "ONC meaningful-use download proof differs")
    require(canonical_hash(read_json(root / "references/scope.json")) == lineage["plan_sha256"], "ONC meaningful-use stored scope differs")
    acq = receipt["acquisition"]
    require(acq["requested_url"] == plan["url"] and acq["http_status"] == 200, "ONC meaningful-use route differs")
    governance = receipt["governance"]
    require(
        governance["access_class"] == "public" and governance["contains_pii"] is True and PSEUDONYMIZE_AT_LOAD in governance["use_restrictions"],
        "ONC meaningful-use governance differs",
    )
    require(
        receipt["schema_profile"]["native_headers"] == plan["header"] and receipt["schema_profile"]["row_count"] == statistics["rows_read"],
        "ONC meaningful-use schema profile differs",
    )
