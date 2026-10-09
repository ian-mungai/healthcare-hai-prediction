"""Check and store CDC WONDER county-year exports saved by hand, with immutable originals and verified S3 storage.

Offline by default. Each export is an independent, already-saved file: an export that fails its
checks is held and named, never partly stored, and the others continue.
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from scripts.acquisition import hud_xlsx_contract as download_metadata
from scripts.acquisition import wonder_export_contract as contract
from scripts.acquisition.capture import base_receipt, receipt_validator, validate_receipt
from scripts.acquisition.collect_hud_api import runtime
from scripts.acquisition.collect_mmd_api import artifact
from scripts.acquisition.collection_layout import load_routes, object_prefix
from scripts.acquisition.data_paths import current
from scripts.acquisition.dataset_layout import source_folder
from scripts.acquisition.hud_api_transport import collection_lock
from scripts.acquisition.legacy_versions import plan_matches
from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, upload_snapshot, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require
from scripts.infrastructure.render_project_config import load_configuration

LOGGER = logging.getLogger(__name__)
TERMS_URL = "https://wonder.cdc.gov/datause.html"


def stage(batch: dict, downloads: Path, branch: Path) -> Path:
    """Copy the exact recorded WONDER export write-once; after that the Downloads copy is not needed."""
    cached, proof = branch / "source" / contract.stored_name(batch), branch / "source" / "download.json"
    # Both files present means staging finished; otherwise redo it (write_once accepts identical bytes).
    if cached.exists() and proof.exists():
        require(fingerprint(cached) == (batch["sha256"], batch["bytes"]), "WONDER staged export differs from the recorded download")
        return cached
    original = downloads / batch["file_name"]
    require(original.is_file() and not original.is_symlink(), "WONDER download missing or not a regular file")
    try:
        origins = download_metadata.read_origin(original)
    except ValueError:
        raise ValueError("WONDER download origin missing") from None
    clean = contract.sanitize_origins(origins, batch["database"])
    body = original.read_bytes()
    require((contract.digest(body), len(body)) == (batch["sha256"], batch["bytes"]), "WONDER export differs from the recorded download")
    created = download_metadata.download_created_at(original)
    write_once(cached, body)
    require(fingerprint(cached) == (batch["sha256"], batch["bytes"]), "WONDER staged export differs from the recorded download")
    write_once(proof, encoded_json({"file_name": batch["file_name"], "origin_urls": clean, "created_at_utc": created}))
    return cached


def make_receipt(plan: dict, batch: dict, cached: Path, branch: Path) -> Path:
    """Write the original, a CSV of the published rows, download proof, scope and receipt; carry WONDER restrictions and every hold."""
    registry, validator = load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator()
    require(plan["registry_sha256"] == canonical_hash(registry), "WONDER base registry changed")
    source = next(s for s in registry["sources"] if s["source_id"] == "WONDER")
    terms = next(d for d in read_json(REPO_ROOT / current(plan["terms"]["path"]))["datasets"] if d["source_id"] == "WONDER")
    raw = cached.read_bytes()
    derived, statistics = contract.validate_export(raw, batch, plan)
    download = read_json(cached.parent / "download.json")
    proof = download | {"sha256": batch["sha256"], "bytes": batch["bytes"], "saved_by": "user, Chrome", "query": statistics["query"], "statistics": statistics}
    snapshot = branch / "capture"
    files = (
        (f"raw/{contract.stored_name(batch)}", raw, "data", True),
        ("derived/county_year.csv", derived, "data", False),
        ("evidence/download_proof.json", encoded_json(proof), "export_receipt", False),
        ("references/scope.json", encoded_json(plan), "layout", False),
    )
    for name, body, _, _ in files:
        write_once(snapshot / name, body)
    database = contract.DATABASES[batch["database"]]
    first, last = batch["years"]
    span = f"{first}" if first == last else f"{first}-{last}"
    metadata = {
        "publisher": "Centers for Disease Control and Prevention, National Center for Health Statistics",
        "release": {
            "advertised_history": f"{database['title']} on CDC WONDER",
            "dataset_version": None,
            "observed_history": f"{batch['database']} county-year export for {span}, saved by hand; measure and linkage holds preserved",
            "publisher_release_label": database["title"],
            "publisher_updated_at": None,
            "release_date": None,
            "revision_status": "unknown",
        },
        "measurement_periods": [
            {
                "label": f"Death years {span}",
                "start_date": f"{first}-01-01",
                "end_date": f"{last}-12-31",
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
            "license_or_terms_url": TERMS_URL,
            "use_restrictions": terms["restrictions"],
        },
        "export_selections": {"database": batch["database"], "years": batch["years"], **contract.FIXED_PARAMETERS, "saved_by": "user, Chrome"},
    }
    timestamp = datetime.fromisoformat(download["created_at_utc"]).strftime("%Y%m%dT%H%M%SZ")
    receipt = base_receipt(source, metadata, f"WONDER_EXPORT__{timestamp}__{batch['id'][:32]}", validator)
    receipt["acquisition"].update(
        transport_mode="web_export",
        requested_url=database["form_url"],
        resolved_url=contract.ORIGIN + batch["database"],
        retrieved_at_utc=download["created_at_utc"],
        http_status=None,
        request_method="browser_export",
        request_parameters={},
        tool_name="cdc_wonder_manual_export",
        tool_version="1.0.0",
        pagination={
            "required": False,
            "strategy": "One export per database and year range; exact query record, header, years, county floor and unique county-years checked",
            "page_count": 1,
            "termination_verified": True,
            "deduplication_keys": ["County Code", "Year Code"],
        },
    )
    receipt["artifacts"] = [artifact(snapshot / name, snapshot, role, i, original) for i, (name, _, role, original) in enumerate(files, 1)]
    receipt["artifacts"][0].update(media_type="text/tab-separated-values", hash_scope="complete_file", original_file_name=batch["file_name"])
    receipt["schema_profile"].update(
        encoding="utf-8",
        delimiter="\t",
        native_headers=contract.HEADER,
        schema_fingerprint_sha256=canonical_hash(contract.HEADER),
        row_count=statistics["rows"],
        parsed_row_count=statistics["rows"],
        native_identifier_fields=["County Code", "Year Code"],
        native_date_fields=["Year", "Year Code"],
        native_missing_tokens=["Suppressed", "Unreliable", "Not Available", "Missing"],
        native_footnote_fields=["Notes"],
    )
    receipt["quality_profile"].update(
        parse_status="passed_with_warnings",
        duplicate_candidate_key_rows=0,
        blank_identifier_rows=0,
        pagination_complete=True,
        suppressed_or_footnoted_rows=sum(statistics["flags"].get("Deaths", {}).values()),
        checks_passed=["recorded_download_hash_and_origin", "exact_query_record", "exact_header", "planned_years_and_county_floor", "unique_county_years"],
        warnings=[
            "Acquisition checks only. Definitions, geography changes, joins and model eligibility remain unreviewed.",
            "Crude rates per 100,000 by county of residence; WONDER offers no county age-adjusted rates.",
            "Never publish statistics based on nine or fewer deaths, including rates; never reconstruct suppressed values.",
            f"Published flags kept as text: {json.dumps(statistics['flags'], sort_keys=True)}.",
            "Saved by hand in Chrome from the WONDER query form; retrieval time is the file creation time; session identifier removed from the origin.",
        ],
    )
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "mode": "wonder_export",
            "route_id": f"WONDER:web_export:{batch['database']}:{span}:20260928",
            "scope": f"CDC WONDER {batch['database']} county-year crude mortality, {span}",
            "batch_id": batch["id"],
            "plan_sha256": canonical_hash(plan),
            "registry_sha256": canonical_hash(registry),
            "schema_sha256": canonical_hash(validator.schema),
            "terms_sha256": plan["terms"]["sha256"],
            "expected_sha256": batch["sha256"],
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
    """Verify the original export and the rebuilt CSV without network or mutation."""
    receipt = read_json(path)
    validate_receipt(receipt, receipt_validator(), path.parent)
    source = next(s for s in load_registry()["sources"] if s["source_id"] == "WONDER")
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
    route = load_routes(registry)["WONDER"]
    require(reconciliation["snapshot_id"] == receipt["snapshot_id"], "WONDER storage snapshot differs")
    require(all(reconciliation[k] == v for k, v in route.items()), "WONDER storage route differs")
    entries = reconciliation["objects"]
    roles = {a["storage_path"]: a["role"] for a in receipt["artifacts"]} | {"receipt.json": "capture_receipt"}
    require(len(entries) == len(roles) and {e["storage_path"] for e in entries} == set(roles), "WONDER storage artifact set differs")

    def check_object(record: dict, local: Path, role: str) -> None:
        sha, size = fingerprint(local)
        prefix = object_prefix(
            route, source_folder("WONDER"), role, receipt["release"]["release_date"] or receipt["snapshot_id"], receipt["snapshot_id"], False
        )
        require(record["key"] == f"{prefix}/{sha}/{local.name}" and record["bucket"] == settings["data_bucket_name"], "WONDER storage key differs")
        require(record["sha256"] == sha and record["byte_count"] == size, "WONDER stored bytes differ")
        require(isinstance(record["version_id"], str) and record["version_id"] not in {"", "null"}, "WONDER storage version missing")
        require(record["verification"] == "version_get_sha256_and_length_match", "WONDER storage readback missing")

    for entry in entries:
        check_object(entry["object"], root / entry["storage_path"], roles[entry["storage_path"]])
    manifests = reconciliation["manifests"]
    require(len(manifests) == 1 and manifests[0]["dataset_id"] == source_folder("WONDER"), "WONDER storage manifest set differs")
    manifest_path = root / "s3_collections" / source_folder("WONDER") / "manifest.json"
    manifest = read_json(manifest_path)
    require(
        manifest["objects"] == entries and manifest["source_id"] == "WONDER" and manifest["snapshot_id"] == receipt["snapshot_id"], "WONDER manifest differs"
    )
    require(manifest["model_eligible"] is False and manifest["snapshot_status"] == "acquired_unvalidated", "WONDER manifest hold differs")
    check_object(manifests[0]["object"], manifest_path, "capture_receipt")


def execute(plan: dict, batch: dict, root: Path, downloads: Path, upload: bool, client: AwsCli | None, outputs: dict) -> dict:
    """Run the integrated capture/storage path for one export; a valid completion performs no writes."""
    require(canonical_hash(plan) == canonical_hash(contract.load_plan()) and batch in plan["batches"], "WONDER selected plan or batch differs")
    branch = root / "batches" / batch["id"]
    receipt_path = branch / "capture/receipt.json"
    if not receipt_path.exists():
        contract.require_code()
        receipt_path = make_receipt(plan, batch, stage(batch, downloads, branch), branch)
    receipt = verify_local(receipt_path)
    require(json.loads(receipt["lineage"]["extraction_or_query"])["batch_id"] == batch["id"], "WONDER selected export differs from receipt")
    done_path = branch / "completed.json"
    if done_path.exists():
        done = read_json(done_path)
        require(done["receipt_sha256"] == fingerprint(receipt_path)[0] and done["batch_id"] == batch["id"], "WONDER completion receipt differs")
        reconciliation = receipt_path.parent / "s3_collections_reconciliation.json"
        require(done["reconciliation_sha256"] == fingerprint(reconciliation)[0], "WONDER completion storage evidence differs")
        require(done["model_eligible"] is False and done["status"] == "stored" and plan_matches(plan, done["plan_sha256"]), "WONDER completion hold differs")
        require(done["rows"] == receipt["schema_profile"]["row_count"], "WONDER completion row count differs")
        settings = client.configuration if client is not None else load_configuration(REPO_ROOT / ".env")[0]
        verify_storage(receipt_path, receipt, settings)
        return done
    result = {
        "batch_id": batch["id"],
        "export": f"{batch['database']} {batch['years'][0]}-{batch['years'][1]}",
        "file_name": batch["file_name"],
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
            raise ValueError("WONDER storage client missing")
        stored = upload_snapshot(receipt_path, [], client, outputs, load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator())
        verify_storage(receipt_path, receipt, client.configuration)
        result.update(
            status="stored", reconciliation_path=stored["reconciliation_path"], reconciliation_sha256=fingerprint(Path(stored["reconciliation_path"]))[0]
        )
        write_once(done_path, encoded_json(result))
    return result


def run_all(plan: dict, root: Path, downloads: Path, upload: bool, client: AwsCli | None, outputs: dict) -> tuple[list[dict], list[dict]]:
    """Process every export independently; return completed results and held exports with their public reason."""
    results, held = [], []
    with collection_lock(root):
        for batch in plan["batches"]:
            try:
                results.append(execute(plan, batch, root, downloads, upload, client, outputs))
            except (ValueError, OSError, KeyError) as error:
                # require() messages are fixed public text; other exceptions are reported by type only.
                reason = str(error) if isinstance(error, ValueError) and str(error).startswith(("WONDER", "Repeated")) else type(error).__name__
                held.append({"file_name": batch["file_name"], "batch_id": batch["id"], "reason": reason})
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
    root = REPO_ROOT / "data/datasets/historical_acquisition/wonder_county_mortality"
    client, outputs = runtime() if args.execute else (None, {})
    results, held = run_all(plan, root, args.downloads, args.execute, client, outputs)
    for result in results:
        LOGGER.info(json.dumps({k: result[k] for k in ("export", "rows", "status")}, sort_keys=True))
    for item in held:
        LOGGER.error(json.dumps({"status": "held"} | item, sort_keys=True))
    LOGGER.info(json.dumps({"completed": len(results), "held": [h["file_name"] for h in held]}))
    sys.exit(1 if held else 0)


if __name__ == "__main__":
    main()
