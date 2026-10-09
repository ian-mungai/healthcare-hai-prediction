"""Capture ONC's meaningful-use attestation file locally and store hospital rows only, with verified S3 storage.

Offline by default. ``--fetch`` downloads the original into an owner-only local folder that never enters a snapshot
or S3; ``--execute`` stores the derived CSV, download proof and scope.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

from scripts.acquisition import onc_mu_contract as contract
from scripts.acquisition.bls_api_transport import collection_lock
from scripts.acquisition.capture import base_receipt, receipt_validator, validate_receipt
from scripts.acquisition.collect_hud_api import runtime
from scripts.acquisition.collect_mmd_api import artifact
from scripts.acquisition.collection_layout import load_routes, object_prefix
from scripts.acquisition.dataset_layout import source_folder
from scripts.acquisition.legacy_versions import plan_matches
from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, upload_snapshot, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require
from scripts.acquisition.store_cms_owners import private
from scripts.acquisition.transport import Limits, download
from scripts.infrastructure.render_project_config import load_configuration

LOGGER = logging.getLogger(__name__)
ROOT = REPO_ROOT / "data/datasets/historical_acquisition/onc_mu_attestation"
FILE_NAME = "mu_report.csv"
TRANSFORMATIONS = [
    "Kept rows whose Provider_Type is Hospital; dropped eligible-professional (clinician) rows and the clinician-only Specialty column.",
    "Other kept cells unchanged; written as UTF-8 CSV with LF line endings.",
]


def completed_download(folder: Path, plan: dict) -> tuple[Path, dict] | None:
    """The first complete earlier download whose bytes match the plan."""
    for record in sorted(folder.rglob("transport.json")):
        meta = read_json(record)
        path = record.parent / meta["path"]
        if meta["complete"] and path.is_file() and not path.is_symlink() and fingerprint(path) == (plan["sha256"], plan["bytes"]):
            return path, meta
    return None


def fetch(plan: dict, root: Path, allowed: bool, opener: Any, limits: Limits | None) -> tuple[Path, dict]:
    """Reuse the verified original, or download it into private_original/ within the plan's raised limits."""
    top = root / "private_original"
    found = completed_download(top, plan) if top.exists() else None
    if found is not None:
        return found
    require(allowed, "ONC meaningful-use original not captured; run with --fetch")
    top.mkdir(parents=True, exist_ok=True)
    private(top, top)
    run = top / f"run_{len(list(top.glob('run_*'))) + 1:02}"
    bounds = limits or Limits(attempts=3, timeout_seconds=120, max_seconds=plan["limits"]["max_seconds"], max_bytes=plan["limits"]["max_bytes"])
    result = download(plan["url"], run, FILE_NAME, "csv", "data", bounds, opener=opener)
    for path in run.rglob("*"):
        private(path, top)
    require(result.complete, "ONC meaningful-use download failed")
    found = completed_download(top, plan)
    if found is None:
        raise ValueError("ONC meaningful-use original differs from the plan")
    return found


def make_receipt(plan: dict, original: Path, meta: dict, branch: Path, root: Path) -> Path:
    """Write the derived CSV, download proof, scope and receipt; the original stays outside the snapshot."""
    registry, validator = load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator()
    require(plan["registry_sha256"] == canonical_hash(registry), "ONC meaningful-use base registry changed")
    source = next(s for s in registry["sources"] if s["source_id"] == contract.SOURCE_ID)
    derived, statistics = contract.derive(original, plan)
    proof = {
        "requested_url": plan["url"],
        "resolved_url": meta["resolved_url"],
        "retrieved_at_utc": meta["retrieved_at_utc"],
        "http_status": meta["http_status"],
        "sha256": meta["sha256"],
        "bytes": meta["byte_count"],
        "original_path": original.relative_to(root).as_posix(),
        "original_retention": "This Mac only, owner-only; contains clinician NPIs; never uploaded; deleted at collection closeout after user confirmation",
        "statistics": statistics,
        "transformations": TRANSFORMATIONS,
    }
    snapshot = branch / "capture"
    files = (
        ("derived/hospital_attestations.csv", derived, "data"),
        ("evidence/download_proof.json", encoded_json(proof), "export_receipt"),
        ("references/scope.json", encoded_json(plan), "layout"),
    )
    for name, body, _ in files:
        write_once(snapshot / name, body)
    metadata = {
        "publisher": "Office of the National Coordinator for Health Information Technology",
        "release": {
            "advertised_history": "EHR Products Used for Meaningful Use Attestation, April 2011 - September 2017",
            "dataset_version": None,
            "observed_history": "Medicare meaningful-use attestations with certified products; hospital rows only; not Promoting Interoperability",
            "publisher_release_label": "MU_REPORT",
            "publisher_updated_at": None,
            "release_date": None,
            "revision_status": "unknown",
        },
        "measurement_periods": [
            {
                "label": "Program years 2011-2017 (2017 partial)",
                "start_date": "2011-01-01",
                "end_date": "2017-12-31",
                "period_type": "unknown",
                "source_basis": "publisher_stated",
                "target_alignment": "unknown",
            }
        ],
        "governance": {
            "access_class": "public",
            "contains_phi": False,
            "contains_pii": False,
            "credential_reference": None,
            "credentials_required": False,
            "license_or_terms_url": contract.TERMS_URL,
            "use_restrictions": [
                "Project-transformed subset: hospital rows only, not the official file.",
                "Medicare meaningful use, not Promoting Interoperability; do not join to C071-C074 without a definition review.",
            ],
        },
    }
    stamp = meta["retrieved_at_utc"].replace("-", "").replace(":", "")[:15] + "Z"
    receipt = base_receipt(source, metadata, f"ONC_MU_HOSPITAL__{stamp}__{plan['id'][:32]}", validator)
    receipt["acquisition"].update(
        requested_url=plan["url"],
        resolved_url=meta["resolved_url"],
        retrieved_at_utc=meta["retrieved_at_utc"],
        http_status=meta["http_status"],
        tool_name="HistoricalSourceCapture",
        tool_version="1.0.0",
        pagination={"required": False, "strategy": "One CSV", "page_count": 1, "termination_verified": True, "deduplication_keys": []},
    )
    receipt["artifacts"] = [artifact(snapshot / name, snapshot, role, i, False) for i, (name, _, role) in enumerate(files, 1)]
    columns = [c for c in plan["header"] if c not in contract.DROPPED]
    receipt["schema_profile"].update(
        encoding="utf-8",
        delimiter=",",
        native_headers=columns,
        schema_fingerprint_sha256=canonical_hash(columns),
        row_count=statistics["hospital_rows_kept"],
        parsed_row_count=statistics["hospital_rows_kept"],
        native_identifier_fields=["NPI", "CCN", "EHR_Certification_Number", "EHR_Product_CHP_Id"],
    )
    receipt["quality_profile"].update(
        parse_status="passed_with_warnings",
        blank_identifier_rows=0,
        pagination_complete=True,
        checks_passed=["recorded_original_hash", "exact_header", "provider_type_hospital_or_ep", "no_specialty_on_hospital_rows", "row_counts_add_up"],
        warnings=[
            "Acquisition checks only. Definitions, hospital identity and model eligibility remain unreviewed; C071-C074 holds kept.",
            f"Original read as {statistics['encoding']}; {statistics['clinician_rows_dropped']} clinician rows dropped.",
            "Program year 2017 is partial; program years 2018-2022 are in neither ONC file.",
            "Derived file: " + " ".join(TRANSFORMATIONS),
        ],
    )
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "mode": "onc_mu_hospital",
            "route_id": "ONC_PI:file:mu_report",
            "scope": "ONC meaningful-use attestations 2011-2017, hospital rows",
            "file_id": plan["id"],
            "plan_sha256": canonical_hash(plan),
            "registry_sha256": canonical_hash(registry),
            "schema_sha256": canonical_hash(validator.schema),
            "expected_sha256": plan["sha256"],
            "code_sha256": contract.require_code(),
            "model_eligible": False,
        }
    )
    path = snapshot / "receipt.json"
    if path.exists():
        receipt = read_json(path)
    validate_receipt(receipt, validator, snapshot)
    contract.verify_capture(receipt, source, json.loads(receipt["lineage"]["extraction_or_query"]), snapshot, False)
    write_once(path, encoded_json(receipt))
    return path


def verify_local(path: Path) -> dict:
    """Verify the derived CSV against the local original without network or mutation."""
    receipt = read_json(path)
    validate_receipt(receipt, receipt_validator(), path.parent)
    source = next(s for s in load_registry()["sources"] if s["source_id"] == contract.SOURCE_ID)
    contract.verify_capture(receipt, source, json.loads(receipt["lineage"]["extraction_or_query"]), path.parent, False)
    return receipt


def verify_storage(path: Path, receipt: dict, settings: dict) -> None:
    """Check exact local artifacts, S3 destination, manifest and immutable version evidence."""
    root = path.parent
    reconciliation = read_json(root / "s3_collections_reconciliation.json")
    registry = load_registry()
    registry_sha256 = json.loads(receipt["lineage"]["extraction_or_query"])["registry_sha256"]
    if canonical_hash(registry) != registry_sha256:
        registry = load_registry(expected_sha256=registry_sha256)
    route = load_routes(registry)[contract.SOURCE_ID]
    folder = source_folder(contract.SOURCE_ID)
    require(reconciliation["snapshot_id"] == receipt["snapshot_id"], "ONC meaningful-use storage snapshot differs")
    require(all(reconciliation[k] == v for k, v in route.items()), "ONC meaningful-use storage route differs")
    entries = reconciliation["objects"]
    roles = {a["storage_path"]: a["role"] for a in receipt["artifacts"]} | {"receipt.json": "capture_receipt"}
    require(len(entries) == len(roles) and {e["storage_path"] for e in entries} == set(roles), "ONC meaningful-use storage artifact set differs")

    def check_object(record: dict, local: Path, role: str) -> None:
        sha, size = fingerprint(local)
        prefix = object_prefix(route, folder, role, receipt["release"]["release_date"] or receipt["snapshot_id"], receipt["snapshot_id"], False)
        require(record["key"] == f"{prefix}/{sha}/{local.name}" and record["bucket"] == settings["data_bucket_name"], "ONC meaningful-use storage key differs")
        require(record["sha256"] == sha and record["byte_count"] == size, "ONC meaningful-use stored bytes differ")
        require(isinstance(record["version_id"], str) and record["version_id"] not in {"", "null"}, "ONC meaningful-use storage version missing")
        require(record["verification"] == "version_get_sha256_and_length_match", "ONC meaningful-use storage readback missing")

    for entry in entries:
        check_object(entry["object"], root / entry["storage_path"], roles[entry["storage_path"]])
    manifests = reconciliation["manifests"]
    require(len(manifests) == 1 and manifests[0]["dataset_id"] == folder, "ONC meaningful-use storage manifest set differs")
    manifest_path = root / "s3_collections" / folder / "manifest.json"
    manifest = read_json(manifest_path)
    require(
        manifest["objects"] == entries and manifest["source_id"] == contract.SOURCE_ID and manifest["snapshot_id"] == receipt["snapshot_id"],
        "ONC meaningful-use manifest differs",
    )
    require(manifest["model_eligible"] is False and manifest["snapshot_status"] == "acquired_unvalidated", "ONC meaningful-use manifest hold differs")
    check_object(manifests[0]["object"], manifest_path, "capture_receipt")


def execute(
    plan: dict, root: Path, allow_fetch: bool, upload: bool, client: AwsCli | None, outputs: dict, opener: Any = None, limits: Limits | None = None
) -> dict:
    """Run the integrated capture/storage path; a valid completion performs no writes."""
    require(canonical_hash(plan) == canonical_hash(contract.load_plan()), "ONC meaningful-use selected plan differs")
    branch = root / "batches" / plan["id"]
    receipt_path = branch / "capture/receipt.json"
    if not receipt_path.exists():
        contract.require_code()
    with collection_lock(root):
        if not receipt_path.exists():
            contract.require_code()
            original, meta = fetch(plan, root, allow_fetch, opener, limits)
            receipt_path = make_receipt(plan, original, meta, branch, root)
        receipt = verify_local(receipt_path)
        done_path = branch / "completed.json"
        if done_path.exists():
            done = read_json(done_path)
            require(done["receipt_sha256"] == fingerprint(receipt_path)[0] and done["file_id"] == plan["id"], "ONC meaningful-use completion receipt differs")
            reconciliation = receipt_path.parent / "s3_collections_reconciliation.json"
            require(done["reconciliation_sha256"] == fingerprint(reconciliation)[0], "ONC meaningful-use completion storage evidence differs")
            require(
                done["model_eligible"] is False and done["status"] == "stored" and plan_matches(plan, done["plan_sha256"]),
                "ONC meaningful-use completion hold differs",
            )
            settings = client.configuration if client is not None else load_configuration(REPO_ROOT / ".env")[0]
            verify_storage(receipt_path, receipt, settings)
            return done
        result = {
            "file_id": plan["id"],
            "rows": receipt["schema_profile"]["row_count"],
            "status": "capture_ready",
            "model_eligible": False,
            "receipt_path": str(receipt_path),
            "receipt_sha256": fingerprint(receipt_path)[0],
            "plan_sha256": canonical_hash(plan),
        }
        if upload:
            contract.require_code()
            if client is None:
                raise ValueError("ONC meaningful-use storage client missing")
            stored = upload_snapshot(receipt_path, [], client, outputs, load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator())
            verify_storage(receipt_path, receipt, client.configuration)
            result.update(
                status="stored", reconciliation_path=stored["reconciliation_path"], reconciliation_sha256=fingerprint(Path(stored["reconciliation_path"]))[0]
            )
            write_once(done_path, encoded_json(result))
        return result


def main() -> None:
    """Validate offline by default; opt into the download and S3 writes independently."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--validate-receipt", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    os.umask(0o077)
    if args.validate_receipt:
        receipt = verify_local(args.validate_receipt)
        sys.stdout.write(json.dumps({"status": "valid", "rows": receipt["schema_profile"]["row_count"], "model_eligible": False}) + "\n")
        return
    client, outputs = runtime() if args.execute else (None, {})
    try:
        result = execute(contract.load_plan(), ROOT, args.fetch, args.execute, client, outputs)
    except (ValueError, OSError, KeyError) as error:
        reason = str(error) if isinstance(error, ValueError) and str(error).startswith(("ONC", "Source is")) else type(error).__name__
        LOGGER.error(json.dumps({"status": "held", "reason": reason}))
        sys.exit(1)
    LOGGER.info(json.dumps({k: result[k] for k in ("rows", "status")}, sort_keys=True))


if __name__ == "__main__":
    main()
