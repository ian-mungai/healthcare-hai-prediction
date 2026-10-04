"""Collect fixed BLS county batches with immutable raw responses and verified S3 storage."""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from scripts.acquisition import bls_api_contract as contract
from scripts.acquisition.bls_api_transport import QuotaPause, collection_lock, fetch
from scripts.acquisition.capture import base_receipt, receipt_validator, validate_receipt
from scripts.acquisition.collect_mmd_api import artifact
from scripts.acquisition.collection_layout import load_routes, object_prefix
from scripts.acquisition.process import run_command
from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, upload_snapshot, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require
from scripts.infrastructure.render_project_config import load_configuration

LOGGER = logging.getLogger(__name__)


def aws_runner(command: list[str], **kwargs: Any) -> Any:
    """Send legacy AWS adapter commands through the bounded central process launcher."""
    return run_command(command[0], command[1:], timeout=kwargs.get("timeout", 180))


def make_receipt(plan: dict, batch: dict, envelope: dict, root: Path) -> Path:
    """Write deterministic source artifacts and receipt; preserve every downstream hold."""
    registry, validator = load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator()
    require(plan["registry_sha256"] == canonical_hash(registry), "BLS base registry changed")
    source = next(s for s in registry["sources"] if s["source_id"] == "BLS")
    raw = bytes.fromhex(envelope["body_hex"])
    derived, statistics = contract.validate_response(raw, batch)
    snapshot = root / "batches" / batch["id"] / "capture"
    public_proof = {k: v for k, v in envelope.items() if k != "body_hex"}
    public_proof.update(statistics=statistics, response_sha256=contract.digest(raw))
    files = (
        ("raw/response.json", raw, "api_page", True),
        ("derived/observations.csv", derived, "data", False),
        ("evidence/request_response.json", encoded_json(public_proof), "export_receipt", False),
    )
    for name, body, _, _ in files:
        write_once(snapshot / name, body)
    timestamp = datetime.fromisoformat(envelope["retrieved_at_utc"]).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"BLS_API__{timestamp}__{batch['id'][:32]}"
    metadata = {
        "publisher": "U.S. Bureau of Labor Statistics",
        "release": {
            "advertised_history": "County LAUS history candidate request",
            "dataset_version": None,
            "observed_history": f"Explicit {batch['start_year']}-{batch['end_year']} request; availability holds preserved",
            "publisher_release_label": None,
            "publisher_updated_at": None,
            "release_date": None,
            "revision_status": "unknown",
        },
        "measurement_periods": [
            {
                "label": "Calendar years with monthly observations and published annual averages",
                "start_date": f"{batch['start_year']}-01-01",
                "end_date": f"{batch['end_year']}-12-31",
                "period_type": "calendar_year",
                "source_basis": "field_observed",
                "target_alignment": "unknown",
            }
        ],
        "governance": {
            "access_class": "public",
            "contains_phi": False,
            "contains_pii": False,
            "credential_reference": plan["credential_reference"],
            "credentials_required": True,
            "license_or_terms_url": None,
            "use_restrictions": ["Public aggregate data only; all source/measure, geography, revision and modeling holds remain."],
        },
    }
    receipt = base_receipt(source, metadata, run_id, validator)
    receipt["acquisition"].update(
        transport_mode="api_response",
        requested_url=plan["endpoint"],
        resolved_url=plan["endpoint"],
        retrieved_at_utc=envelope["retrieved_at_utc"],
        http_status=200,
        request_method="POST",
        request_parameters=contract.request_for(batch),
        tool_name="bls_api_collector",
        tool_version="1.0.0",
        pagination={
            "required": False,
            "strategy": "Explicit series/window request; response IDs and complete period grid checked",
            "page_count": 1,
            "termination_verified": True,
            "deduplication_keys": ["seriesID", "year", "period"],
        },
    )
    receipt["artifacts"] = [artifact(snapshot / name, snapshot, role, i, original) for i, (name, _, role, original) in enumerate(files, 1)]
    receipt["schema_profile"].update(
        encoding="utf-8",
        delimiter=",",
        native_headers=contract.FIELDS,
        schema_fingerprint_sha256=canonical_hash(contract.FIELDS),
        row_count=statistics["rows"],
        parsed_row_count=statistics["rows"],
        native_identifier_fields=["seriesID"],
        native_date_fields=["year", "period"],
        native_missing_tokens=["-"],
        native_footnote_fields=["footnotes"],
    )
    receipt["quality_profile"].update(
        parse_status="passed_with_warnings",
        duplicate_candidate_key_rows=0,
        blank_identifier_rows=0,
        pagination_complete=True,
        suppressed_or_footnoted_rows=statistics["footnoted_rows"],
        checks_passed=["exact_series_scope", "unique_native_periods", "complete_grid_or_explicit_missing_series", "reproducible_csv"],
        warnings=[
            "Acquisition checks only. Geography, historic completeness, vintage and model eligibility remain unreviewed.",
            "Census IDs are candidate BLS series; unavailable IDs retain availability holds.",
            "M13 and footnoted missing values remain exactly as published; no inferred zeros.",
        ],
    )
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "mode": "bls_api_v2",
            "route_id": "BLS:api_v2:20260926",
            "scope": "County LAUS four measures, monthly plus annual average",
            "batch_id": batch["id"],
            "plan_sha256": canonical_hash(plan),
            "registry_sha256": canonical_hash(registry),
            "schema_sha256": canonical_hash(validator.schema),
            "expected_sha256": contract.digest(raw),
            "code_sha256": contract.require_code(),
            "model_eligible": False,
            "fallback_reason": "Approved separate keyed API collector; immutable base registry preserved",
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
    """Verify original bytes and reconstructed observations without network or mutation."""
    receipt = read_json(path)
    validate_receipt(receipt, receipt_validator(), path.parent)
    source = next(s for s in load_registry()["sources"] if s["source_id"] == "BLS")
    contract.verify_capture(receipt, source, json.loads(receipt["lineage"]["extraction_or_query"]), path.parent, False)
    return receipt


def verify_storage(path: Path, receipt: dict, settings: dict) -> None:
    """Check exact local artifacts, S3 destination, manifests and immutable version evidence."""
    root = path.parent
    reconciliation = read_json(root / "s3_collections_reconciliation.json")
    registry = load_registry()
    registry_sha256 = json.loads(receipt["lineage"]["extraction_or_query"])["registry_sha256"]
    if canonical_hash(registry) != registry_sha256:
        registry = load_registry(expected_sha256=registry_sha256)
    route = load_routes(registry)["BLS"]
    require(reconciliation["snapshot_id"] == receipt["snapshot_id"], "BLS storage snapshot differs")
    require(all(reconciliation[k] == v for k, v in route.items()), "BLS storage route differs")
    entries = reconciliation["objects"]
    roles = {a["storage_path"]: a["role"] for a in receipt["artifacts"]} | {"receipt.json": "capture_receipt"}
    require(len(entries) == len(roles) and {e["storage_path"] for e in entries} == set(roles), "BLS storage artifact set differs")

    def check_object(record: dict, local: Path, role: str) -> None:
        sha, size = fingerprint(local)
        prefix = object_prefix(route, "bls", role, receipt["release"]["release_date"] or receipt["snapshot_id"], receipt["snapshot_id"], False)
        require(record["key"] == f"{prefix}/{sha}/{local.name}", "BLS storage key differs")
        require(record["bucket"] == settings["data_bucket_name"], "BLS storage destination differs")
        require(record["sha256"] == sha and record["byte_count"] == size, "BLS stored bytes differ")
        require(isinstance(record["version_id"], str) and record["version_id"] not in {"", "null"}, "BLS storage version missing")
        require(record["verification"] == "version_get_sha256_and_length_match", "BLS storage readback missing")

    for entry in entries:
        check_object(entry["object"], root / entry["storage_path"], roles[entry["storage_path"]])
    manifests = reconciliation["manifests"]
    require(len(manifests) == 1 and manifests[0]["dataset_id"] == "bls", "BLS storage manifest set differs")
    manifest_path = root / "s3_collections/bls/manifest.json"
    manifest = read_json(manifest_path)
    require(manifest["objects"] == entries and manifest["source_id"] == "BLS", "BLS manifest objects differ")
    require(manifest["model_eligible"] is False and manifest["snapshot_status"] == "acquired_unvalidated", "BLS manifest hold differs")
    require(manifest["snapshot_id"] == receipt["snapshot_id"], "BLS manifest snapshot differs")
    check_object(manifests[0]["object"], manifest_path, "capture_receipt")


def execute(plan: dict, batch: dict, root: Path, allow_network: bool, upload: bool, client: AwsCli | None, outputs: dict, request: Any = None) -> dict:
    """Run the integrated capture/storage path; a valid completion performs no writes."""
    require(canonical_hash(plan) == canonical_hash(contract.load_plan()) and batch in plan["batches"], "BLS selected plan or batch differs")
    branch = root / "batches" / batch["id"]
    receipt_path = branch / "capture/receipt.json"
    if not receipt_path.exists():
        contract.require_code()
        envelope = fetch(plan, batch, root, allow_network, client, request=request)
        receipt_path = make_receipt(plan, batch, envelope, root)
    receipt = verify_local(receipt_path)
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    require(
        lineage["batch_id"] == batch["id"] and receipt["acquisition"]["request_parameters"] == contract.request_for(batch),
        "BLS selected batch differs from receipt",
    )
    done_path = branch / "completed.json"
    if done_path.exists():
        done = read_json(done_path)
        require(done["receipt_sha256"] == fingerprint(receipt_path)[0] and done["batch_id"] == batch["id"], "BLS completion receipt differs")
        reconciliation = receipt_path.parent / "s3_collections_reconciliation.json"
        require(done["reconciliation_sha256"] == fingerprint(reconciliation)[0], "BLS completion storage evidence differs")
        require(done["model_eligible"] is False and done["status"] == "stored", "BLS completion hold differs")
        require(done["plan_sha256"] == canonical_hash(plan), "BLS completion plan differs")
        require(done["rows"] == receipt["schema_profile"]["row_count"], "BLS completion row count differs")
        settings = client.configuration if client is not None else load_configuration(REPO_ROOT / ".env")[0]
        verify_storage(receipt_path, receipt, settings)
        return done
    result = {
        "batch_id": batch["id"],
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
            raise ValueError("BLS storage client missing")
        stored = upload_snapshot(receipt_path, [], client, outputs, load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator())
        verify_storage(receipt_path, receipt, client.configuration)
        result.update(
            status="stored", reconciliation_path=stored["reconciliation_path"], reconciliation_sha256=fingerprint(Path(stored["reconciliation_path"]))[0]
        )
        write_once(done_path, encoded_json(result))
    return result


def runtime() -> tuple[AwsCli, dict]:
    """Load the project profile and read-only local Terraform outputs, without credentials in output."""
    settings, _ = load_configuration(REPO_ROOT / ".env")
    result = run_command("terraform", ["-chdir=infra", "output", "-json"], cwd=REPO_ROOT, timeout=30)
    require(result.returncode == 0, "Local Terraform outputs unavailable")
    return AwsCli(settings, runner=aws_runner), json.loads(result.stdout)


def main() -> None:
    """Validate offline by default; opt into BLS requests and S3 writes independently."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id")
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--limit", type=int, default=1)
    parser.add_argument("--validate-receipt", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        if args.validate_receipt:
            receipt = verify_local(args.validate_receipt)
            sys.stdout.write(json.dumps({"status": "valid", "rows": receipt["schema_profile"]["row_count"], "model_eligible": False}) + "\n")
            return
        plan = contract.load_plan()
        require(args.limit > 0, "BLS limit must be positive")
        root = REPO_ROOT / "data/datasets/historical_acquisition/bls_api_history" / plan["vintage"]
        batches = [b for b in plan["batches"] if b["id"] == args.batch_id] if args.batch_id else plan["batches"]
        require(bool(batches), "BLS batch not in locked plan")
        client: AwsCli | None = None
        outputs: dict = {}
        with collection_lock(root):
            processed = 0
            for batch in batches:
                already = (root / "batches" / batch["id"] / "completed.json").exists()
                if not already and client is None and (args.fetch or args.execute):
                    client, outputs = runtime()
                result = execute(plan, batch, root, args.fetch, args.execute, client, outputs)
                LOGGER.info(json.dumps(result, sort_keys=True))
                processed += not already
                if processed >= args.limit:
                    break
    except QuotaPause as error:
        LOGGER.info(json.dumps({"status": "quota_pause", "reason": str(error), "model_eligible": False}))
        sys.exit(3)
    except (ValueError, OSError, KeyError, TypeError, StopIteration) as error:
        # Exceptions can originate at credential-bearing boundaries; never emit their bodies.
        LOGGER.error(json.dumps({"status": "blocked", "error_type": type(error).__name__, "details": "Collector stopped; inspect public evidence offline"}))
        sys.exit(1)


if __name__ == "__main__":
    main()
