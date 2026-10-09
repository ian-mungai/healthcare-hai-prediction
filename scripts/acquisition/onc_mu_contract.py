"""Frozen scope and privacy checks for ONC's meaningful-use attestation file (2011-2017), hospital rows only.

The file mixes eligible-professional rows (individual clinicians) with eligible-hospital rows, so the original stays
on this Mac only and S3 receives hospital rows without the clinician-only Specialty column.
It is a separate "Medicare meaningful use" series, not the 2023-2024 Promoting Interoperability file.
Failure modes: data/acquisition_planning/onc_mu_hospital_failure_modes.md.
"""

import csv
import io
import re
from collections import Counter
from pathlib import Path

from scripts.acquisition.bls_api_contract import code_hashes
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.legacy_versions import plan_matches
from scripts.acquisition.s3_store import fingerprint
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require

PLAN_PATH = REPO_ROOT / "config/acquisition/onc_mu_hospital_plan.json"
VERSIONS_PATH = REPO_ROOT / "config/acquisition/onc_mu_code_versions.json"
SOURCE_ID = "ONC_PI"
TERMS_URL = "https://healthit.gov/data/datasets/ehr-products-used-meaningful-use-attestation/"
HOSPITAL = "Hospital"
PROVIDER_TYPES = {"Hospital", "EP"}
DROPPED = ["Specialty"]
REQUIRED_YEARS = ["2011", "2012", "2013", "2014", "2015", "2016"]
MAX_BYTES = 600 * 1024**2
MAX_SECONDS = 3600


def require_code(hashes: dict | None = None) -> dict:
    """Require explicit review of new captures and retain trusted historical implementations."""
    actual = code_hashes() if hashes is None else hashes
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
    require(plan["source_id"] == SOURCE_ID and plan["model_eligible"] is False and plan["version"] == 1, "ONC meaningful-use plan hold differs")
    require(plan["registry_sha256"] == canonical_hash(load_registry(expected_sha256=plan["registry_sha256"])), "ONC meaningful-use registry differs")
    require(plan["dropped_columns"] == DROPPED and plan["hospital_value"] == HOSPITAL and "Specialty" in plan["header"], "ONC meaningful-use plan rules differ")
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


def derive(path: Path, plan: dict) -> tuple[bytes, dict]:
    """Stream the original; keep hospital rows without Specialty, or stop with a fixed public reason."""
    encoding = encoding_of(path)
    header = plan["header"]
    kept_columns = [c for c in header if c not in DROPPED]
    kept_index = [header.index(c) for c in kept_columns]
    index = {c: i for i, c in enumerate(header)}
    kinds: Counter[str] = Counter()
    years: Counter[str] = Counter()
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(kept_columns)
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
            if kind != HOSPITAL:
                continue
            require(row[index["Specialty"]] == "", "ONC meaningful-use hospital row carries a specialty")
            require(row[index["CCN"]] != "", "ONC meaningful-use hospital row has no CCN")
            years[row[index["Program_Year"]]] += 1
            writer.writerow([row[i] for i in kept_index])
    require(all(years[y] > 0 for y in REQUIRED_YEARS), "ONC meaningful-use program years missing")
    statistics = {
        "rows_read": rows,
        "clinician_rows_dropped": kinds["EP"],
        "hospital_rows_kept": kinds[HOSPITAL],
        "hospital_rows_by_program_year": dict(sorted(years.items())),
        "encoding": encoding,
        "model_eligible": False,
    }
    require(rows == kinds["EP"] + kinds[HOSPITAL], "ONC meaningful-use row counts differ")
    return output.getvalue().encode(), statistics


def verify_capture(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Rebuild the derived file from the local original; check the artifact set, bindings and holds."""
    from scripts.acquisition.capture import receipt_validator

    require(not evidence_only and source["source_id"] == SOURCE_ID, "ONC meaningful-use storage source differs")
    plan = load_plan()
    require(plan_matches(plan, lineage["plan_sha256"]) and lineage["file_id"] == plan["id"], "ONC meaningful-use capture plan differs")
    require(lineage["registry_sha256"] == canonical_hash(load_registry(expected_sha256=lineage["registry_sha256"])), "ONC meaningful-use registry differs")
    require(lineage["schema_sha256"] == canonical_hash(receipt_validator().schema), "ONC meaningful-use schema differs")
    require(lineage["model_eligible"] is False and receipt["snapshot_status"] == "acquired_unvalidated", "ONC meaningful-use modeling hold differs")
    require_code(lineage["code_sha256"])
    paths = {a["storage_path"] for a in receipt["artifacts"]}
    require(paths == {"derived/hospital_attestations.csv", "evidence/download_proof.json", "references/scope.json"}, "ONC meaningful-use artifact set differs")
    require(not (root / "raw").exists() and not (root / "audit").exists(), "ONC meaningful-use snapshot holds original bytes")
    proof = read_json(root / "evidence/download_proof.json")
    collection = root.parents[2]
    original = (collection / proof["original_path"]).resolve()
    require(original.is_relative_to((collection / "private_original").resolve()), "ONC meaningful-use original location differs")
    require(fingerprint(original) == (plan["sha256"], plan["bytes"]) and proof["sha256"] == plan["sha256"], "ONC meaningful-use original hash differs")
    derived, statistics = derive(original, plan)
    require((root / "derived/hospital_attestations.csv").read_bytes() == derived, "ONC meaningful-use derived CSV differs")
    require(proof["statistics"] == statistics and proof["requested_url"] == plan["url"], "ONC meaningful-use download proof differs")
    require(canonical_hash(read_json(root / "references/scope.json")) == lineage["plan_sha256"], "ONC meaningful-use stored scope differs")
    acq = receipt["acquisition"]
    require(acq["requested_url"] == plan["url"] and acq["http_status"] == 200, "ONC meaningful-use route differs")
    require(receipt["governance"]["contains_pii"] is False, "ONC meaningful-use governance differs")
    require(receipt["schema_profile"]["row_count"] == statistics["hospital_rows_kept"], "ONC meaningful-use schema profile differs")
