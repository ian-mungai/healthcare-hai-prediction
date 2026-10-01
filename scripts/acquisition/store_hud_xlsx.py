"""Check and store the 44 manually downloaded HUD ZIP-COUNTY workbooks with immutable originals and verified S3 storage.

Offline by default. Each quarter is an independent, already-downloaded file: a quarter that fails
its checks is held and named, never partly stored, and the other quarters continue.
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from scripts.acquisition import hud_api_contract as api
from scripts.acquisition import hud_xlsx_contract as contract
from scripts.acquisition.capture import base_receipt, receipt_validator, validate_receipt
from scripts.acquisition.collect_hud_api import TERMS_URL, runtime, verify_storage
from scripts.acquisition.collect_mmd_api import artifact
from scripts.acquisition.hud_api_transport import collection_lock
from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, upload_snapshot, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require
from scripts.infrastructure.render_project_config import load_configuration

LOGGER = logging.getLogger(__name__)
XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def stage(batch: dict, downloads: Path, branch: Path) -> Path:
    """Copy the exact recorded HUD download write-once; after that the Downloads copy is not needed."""
    cached, proof = branch / "source" / batch["file_name"], branch / "source" / "download.json"
    # Both files present means staging finished; otherwise redo it (write_once accepts identical bytes).
    if cached.exists() and proof.exists():
        require(fingerprint(cached) == (batch["sha256"], batch["bytes"]), "HUD staged workbook differs from the recorded download")
        return cached
    original = downloads / batch["file_name"]
    require(original.is_file() and not original.is_symlink(), "HUD download missing or not a regular file")
    origins = contract.read_origin(original)
    require(origins == [contract.ORIGIN], "HUD download origin differs")
    body = original.read_bytes()
    require((api.digest(body), len(body)) == (batch["sha256"], batch["bytes"]), "HUD workbook differs from the recorded download")
    created = contract.download_created_at(original)
    write_once(cached, body)
    require(fingerprint(cached) == (batch["sha256"], batch["bytes"]), "HUD staged workbook differs from the recorded download")
    write_once(proof, encoded_json({"file_name": batch["file_name"], "origin_urls": origins, "created_at_utc": created}))
    return cached


def make_receipt(plan: dict, batch: dict, cached: Path, branch: Path) -> Path:
    """Write the original, a lossless CSV, download proof, scope and receipt; carry HUD restrictions and every hold."""
    registry, validator = load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator()
    require(plan["registry_sha256"] == canonical_hash(registry), "HUD base registry changed")
    source = next(s for s in registry["sources"] if s["source_id"] == "HUD")
    terms = next(d for d in read_json(REPO_ROOT / plan["terms"]["path"])["datasets"] if d["source_id"] == "HUD")
    raw = cached.read_bytes()
    derived, statistics = contract.validate_workbook(raw, batch, plan)
    download = read_json(cached.parent / "download.json")
    proof = download | {"sha256": batch["sha256"], "bytes": batch["bytes"], "saved_by": "user, Chrome", "statistics": statistics}
    snapshot = branch / "capture"
    files = (
        (f"raw/{batch['file_name']}", raw, "data", True),
        ("derived/crosswalk.csv", derived, "data", False),
        ("evidence/download_proof.json", encoded_json(proof), "export_receipt", False),
        ("references/scope.json", encoded_json(plan), "layout", False),
    )
    for name, body, _, _ in files:
        write_once(snapshot / name, body)
    quarter = f"{batch['year']}Q{batch['quarter']}"
    start, end = api.quarter_period(batch)
    geography = batch["county_geography"]
    metadata = {
        "publisher": "U.S. Department of Housing and Urban Development, Office of Policy Development and Research",
        "release": {
            "advertised_history": "Quarterly HUD USPS ZIP crosswalk files from 2010 Q1, after login to the crosswalk files site",
            "dataset_version": None,
            "observed_history": f"{quarter} ZIP-COUNTY workbook, downloaded by hand; linkage-only and geography holds preserved",
            "publisher_release_label": quarter,
            "publisher_updated_at": None,
            "release_date": None,
            "revision_status": "unknown",
        },
        "measurement_periods": [
            {
                "label": f"USPS address quarter {quarter}",
                "start_date": start,
                "end_date": end,
                "period_type": "unknown",
                "source_basis": "publisher_stated",
                "target_alignment": "unknown",
            }
        ],
        "governance": {
            "access_class": "registration_required",
            "contains_phi": False,
            "contains_pii": False,
            "credential_reference": None,
            "credentials_required": True,
            "license_or_terms_url": TERMS_URL,
            "use_restrictions": terms["restrictions"],
        },
        "export_selections": contract.selections(batch),
    }
    timestamp = datetime.fromisoformat(download["created_at_utc"]).strftime("%Y%m%dT%H%M%SZ")
    receipt = base_receipt(source | {"preferred_route": "web_export"}, metadata, f"HUD_XLSX__{timestamp}__{batch['id'][:32]}", validator)
    receipt["acquisition"].update(
        transport_mode="web_export",
        requested_url=contract.ROUTE_URL,
        resolved_url=contract.ORIGIN,
        retrieved_at_utc=download["created_at_utc"],
        http_status=None,
        request_method="manual_download",
        request_parameters={},
        tool_name="hud_crosswalk_files_manual_download",
        tool_version="1.0.0",
        pagination={
            "required": False,
            "strategy": "One workbook per quarter; exact package, header, cell types, unique rows and every state and DC checked",
            "page_count": 1,
            "termination_verified": True,
            "deduplication_keys": ["zip", "geoid"],
        },
    )
    receipt["artifacts"] = [artifact(snapshot / name, snapshot, role, i, original) for i, (name, _, role, original) in enumerate(files, 1)]
    receipt["artifacts"][0].update(media_type=XLSX, hash_scope="complete_file")
    receipt["schema_profile"].update(
        encoding="utf-8",
        delimiter=None,
        native_headers=contract.HEADER,
        schema_fingerprint_sha256=canonical_hash(contract.HEADER),
        row_count=statistics["rows"],
        parsed_row_count=statistics["rows"],
        native_identifier_fields=["zip", "geoid"],
        native_date_fields=[],
        native_missing_tokens=[],
        native_footnote_fields=[],
    )
    residential = statistics["ratio_sums"]["res_ratio"]
    receipt["quality_profile"].update(
        parse_status="passed_with_warnings",
        duplicate_candidate_key_rows=statistics.get("exact_repeat_rows_dropped", 0),
        blank_identifier_rows=0,
        pagination_complete=True,
        suppressed_or_footnoted_rows=None,
        checks_passed=[
            "recorded_download_hash_and_origin",
            "single_sheet_package",
            "exact_header_and_cell_types",
            "unique_zip_county_rows",
            "all_states_and_dc_present",
            "reproducible_csv",
        ],
        warnings=[
            "Acquisition checks only. Linkage-only review, ZIP universe completeness, geography vintage and model eligibility remain unreviewed.",
            "Ratios are ZIP-to-county; never invert them for county-to-ZIP weighting.",
            "The workbook carries no period of its own; the quarter comes from the user's selection and HUD's file name, bound to the recorded download hash.",
            "Saved by hand in Chrome from the HUD crosswalk files site; the retrieval time is the file's creation time, and no HTTP status or headers exist.",
            "The files site showed no terms; the accepted HUD API terms and attribution are carried as the conservative restriction set.",
            "This workbook layout has no city or state columns; none are filled in.",
            f"County identifiers follow {geography.replace('_', ' ')} geography per HUD's notes; do not mix eras without a crosswalk.",
            f"Residential ratio sums: {residential['zips_with_zero_weight']} ZIPs with zero weight, {residential['zips_outside_tolerance']} outside tolerance.",
            *(
                [
                    f"{statistics['exact_repeat_rows_dropped']} exact repeated rows were dropped from the derived CSV only (user decision 2026-09-28); "
                    "the stored original workbook keeps them."
                ]
                if "exact_repeat_rows_dropped" in statistics
                else []
            ),
            "Registry access_hold released by the dated user terms acceptance and the user's manual-download decision; the base registry text is unchanged.",
        ],
    )
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "mode": "hud_xlsx",
            "route_id": f"HUD:crosswalk_files_xlsx:{quarter}:20260928",
            "scope": f"HUD USPS ZIP-to-county crosswalk workbook, {quarter}, nationwide",
            "county_geography": geography,
            "batch_id": batch["id"],
            "plan_sha256": canonical_hash(plan),
            "registry_sha256": canonical_hash(registry),
            "schema_sha256": canonical_hash(validator.schema),
            "terms_sha256": plan["terms"]["sha256"],
            "expected_sha256": batch["sha256"],
            "code_sha256": api.require_code(),
            "model_eligible": False,
            **({"exact_repeat_decision_sha256": canonical_hash(read_json(contract.DECISION_PATH))} if "exact_repeat_rows_dropped" in statistics else {}),
            "fallback_reason": "API serves nationwide requests only from 2021 Q1; the files site has no fixed links, so the user downloaded by hand",
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
    """Verify the original workbook and the reconstructed crosswalk without network or mutation."""
    receipt = read_json(path)
    validate_receipt(receipt, receipt_validator(), path.parent)
    source = next(s for s in load_registry()["sources"] if s["source_id"] == "HUD")
    contract.verify_capture(receipt, source, json.loads(receipt["lineage"]["extraction_or_query"]), path.parent, False)
    return receipt


def execute(plan: dict, batch: dict, root: Path, downloads: Path, upload: bool, client: AwsCli | None, outputs: dict) -> dict:
    """Run the integrated capture/storage path for one quarter; a valid completion performs no writes."""
    require(canonical_hash(plan) == canonical_hash(contract.load_plan()) and batch in plan["batches"], "HUD selected plan or batch differs")
    branch = root / "batches" / batch["id"]
    receipt_path = branch / "capture/receipt.json"
    if not receipt_path.exists():
        api.require_code()
        receipt_path = make_receipt(plan, batch, stage(batch, downloads, branch), branch)
    receipt = verify_local(receipt_path)
    require(json.loads(receipt["lineage"]["extraction_or_query"])["batch_id"] == batch["id"], "HUD selected batch differs from receipt")
    done_path = branch / "completed.json"
    if done_path.exists():
        done = read_json(done_path)
        require(done["receipt_sha256"] == fingerprint(receipt_path)[0] and done["batch_id"] == batch["id"], "HUD completion receipt differs")
        reconciliation = receipt_path.parent / "s3_collections_reconciliation.json"
        require(done["reconciliation_sha256"] == fingerprint(reconciliation)[0], "HUD completion storage evidence differs")
        require(done["model_eligible"] is False and done["status"] == "stored" and done["plan_sha256"] == canonical_hash(plan), "HUD completion hold differs")
        require(done["rows"] == receipt["schema_profile"]["row_count"], "HUD completion row count differs")
        settings = client.configuration if client is not None else load_configuration(REPO_ROOT / ".env")[0]
        verify_storage(receipt_path, receipt, settings)
        return done
    result = {
        "batch_id": batch["id"],
        "quarter": f"{batch['year']}Q{batch['quarter']}",
        "rows": receipt["schema_profile"]["row_count"],
        "status": "capture_ready",
        "model_eligible": False,
        "receipt_path": str(receipt_path),
        "receipt_sha256": fingerprint(receipt_path)[0],
        "plan_sha256": canonical_hash(plan),
    }
    if upload:
        api.require_code()
        if client is None:
            raise ValueError("HUD storage client missing")
        stored = upload_snapshot(receipt_path, [], client, outputs, load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator())
        verify_storage(receipt_path, receipt, client.configuration)
        result.update(
            status="stored", reconciliation_path=stored["reconciliation_path"], reconciliation_sha256=fingerprint(Path(stored["reconciliation_path"]))[0]
        )
        write_once(done_path, encoded_json(result))
    return result


def run_all(plan: dict, root: Path, downloads: Path, upload: bool, client: AwsCli | None, outputs: dict) -> tuple[list[dict], list[dict]]:
    """Process every quarter independently; return completed results and held quarters with their public reason."""
    results, held = [], []
    with collection_lock(root):
        for batch in plan["batches"]:
            quarter = f"{batch['year']}Q{batch['quarter']}"
            try:
                results.append(execute(plan, batch, root, downloads, upload, client, outputs))
            except (ValueError, OSError, KeyError) as error:
                # require() messages are fixed public text; other exceptions are reported by type only.
                reason = str(error) if isinstance(error, ValueError) and str(error).startswith(("HUD", "Duplicate")) else type(error).__name__
                held.append({"quarter": quarter, "batch_id": batch["id"], "reason": reason})
    return results, held


def main() -> None:
    """Validate offline by default; opt into S3 writes with --execute."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--downloads", type=Path, default=Path.home() / "Downloads")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--validate-receipt", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.validate_receipt:
        receipt = verify_local(args.validate_receipt)
        sys.stdout.write(json.dumps({"status": "valid", "rows": receipt["schema_profile"]["row_count"], "model_eligible": False}) + "\n")
        return
    plan = contract.load_plan()
    root = REPO_ROOT / "data/historical_acquisition/hud_usps_crosswalk" / plan["vintage"]
    client, outputs = runtime() if args.execute else (None, {})
    results, held = run_all(plan, root, args.downloads, args.execute, client, outputs)
    for result in results:
        LOGGER.info(json.dumps({k: result[k] for k in ("quarter", "rows", "status")}, sort_keys=True))
    for item in held:
        LOGGER.error(json.dumps({"status": "held"} | item, sort_keys=True))
    LOGGER.info(json.dumps({"completed": len(results), "held": [h["quarter"] for h in held]}))
    sys.exit(1 if held else 0)


if __name__ == "__main__":
    main()
