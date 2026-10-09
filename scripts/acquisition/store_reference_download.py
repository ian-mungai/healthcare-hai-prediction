"""Check and store publisher reference documents saved in a browser, with immutable originals and verified S3 storage.

Offline by default. Each file is independent: a file that fails its checks is held and named, never
partly stored, and the others continue. `--build-plan` binds reviewed requests to the saved files for
locking; it writes nothing to S3.
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from scripts.acquisition import hud_xlsx_contract as download_metadata
from scripts.acquisition import reference_download_contract as contract
from scripts.acquisition.bls_api_transport import collection_lock
from scripts.acquisition.capture import base_receipt, receipt_validator, validate_receipt
from scripts.acquisition.collect_hud_api import runtime
from scripts.acquisition.collect_mmd_api import artifact
from scripts.acquisition.collection_layout import load_routes, object_prefix
from scripts.acquisition.dataset_layout import source_folder
from scripts.acquisition.legacy_versions import plan_matches
from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, upload_snapshot, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require
from scripts.infrastructure.render_project_config import load_configuration

LOGGER = logging.getLogger(__name__)
ROOT = REPO_ROOT / "data/datasets/historical_acquisition/reference_downloads"


def stage(entry: dict, downloads: Path, branch: Path) -> Path:
    """Copy the exact planned file write-once with its origin proof; after that the Downloads copy is not needed."""
    cached, proof = branch / "source" / entry["file_name"], branch / "source" / "download.json"
    # Both files present means staging finished; otherwise redo it (write_once accepts identical bytes).
    if cached.exists() and proof.exists():
        require(fingerprint(cached) == (entry["sha256"], entry["bytes"]), "Reference staged copy differs from the plan")
        return cached
    body = contract.read_download(entry, downloads)
    require((contract.digest(body), len(body)) == (entry["sha256"], entry["bytes"]), "Reference download differs from the plan")
    created = download_metadata.download_created_at(downloads / entry["file_name"])
    write_once(cached, body)
    require(fingerprint(cached) == (entry["sha256"], entry["bytes"]), "Reference staged copy differs from the plan")
    # Only the planned URL is kept; a referring page the browser also recorded is not evidence and may be personal.
    write_once(proof, encoded_json({"file_name": entry["file_name"], "origin_url": entry["origin_url"], "created_at_utc": created}))
    return cached


def make_receipt(plan: dict, entry: dict, cached: Path, branch: Path) -> Path:
    """Write the original, its download proof and the receipt; a browser download has no HTTP status, so none is recorded."""
    registry, validator = load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator()
    source = contract.source_for(entry, registry)
    bindings = contract.bindings_for(entry, source)
    if bindings:
        # The receipt records what was collected: documentation only; the registry hold itself is unchanged.
        source = source | {"preferred_route": "documentation_only"}
    download = read_json(cached.parent / "download.json")
    proof = download | {"sha256": entry["sha256"], "bytes": entry["bytes"], "saved_by": "user, browser"}
    snapshot = branch / "capture"
    files = (
        (f"raw/{entry['file_name']}", cached.read_bytes(), entry["role"], True),
        ("evidence/download_proof.json", encoded_json(proof), "export_receipt", False),
    )
    for name, body, _, _ in files:
        write_once(snapshot / name, body)
    metadata = {
        "publisher": entry["publisher"],
        "release": {
            "advertised_history": None,
            "dataset_version": None,
            "observed_history": f"{entry['title']}, saved by hand in a browser from the publisher's URL",
            "publisher_release_label": None,
            "publisher_updated_at": None,
            "release_date": None,
            "revision_status": "unknown",
        },
        "measurement_periods": [
            {"label": None, "start_date": None, "end_date": None, "period_type": "unknown", "source_basis": "unknown", "target_alignment": "unknown"}
        ],
        "governance": {
            "access_class": "public",
            "contains_phi": False,
            "contains_pii": False,
            "credential_reference": None,
            "credentials_required": False,
            "license_or_terms_url": None,
            "use_restrictions": [
                "Publisher documentation only; no observations.",
                "Preserve all source and measure holds; no model approval.",
            ],
        },
    }
    timestamp = datetime.fromisoformat(download["created_at_utc"]).strftime("%Y%m%dT%H%M%SZ")
    receipt = base_receipt(source, metadata, f"{entry['source_id']}_REFERENCE__{timestamp}__{entry['id'][:32]}", validator)
    receipt["acquisition"].update(
        transport_mode="reference_document",
        requested_url=entry["origin_url"],
        resolved_url=entry["origin_url"],
        retrieved_at_utc=download["created_at_utc"],
        http_status=None,
        request_method="manual_download",
        tool_name="browser_reference_download",
        tool_version="1.0.0",
    )
    receipt["artifacts"] = [artifact(snapshot / name, snapshot, role, i, original) for i, (name, _, role, original) in enumerate(files, 1)]
    receipt["artifacts"][0].update(media_type=entry["media_type"], hash_scope="complete_file")
    receipt["quality_profile"].update(
        checks_passed=["planned_origin_url", "planned_sha256_and_size"],
        warnings=[
            "Reference document only; its content is not reviewed.",
            "Saved by hand in a browser; retrieval time is the file creation time; no HTTP status or headers exist for it.",
        ],
    )
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "mode": "reference_download",
            "route_id": f"{entry['source_id']}:reference_download:{entry['id'][:16]}",
            "scope": f"{entry['title']} ({entry['role']}), saved by hand from {entry['origin_url']}",
            "file_id": entry["id"],
            "plan_sha256": canonical_hash(plan),
            "registry_sha256": plan["registry_sha256"],
            "schema_sha256": canonical_hash(validator.schema),
            "expected_sha256": entry["sha256"],
            "code_sha256": contract.require_code(),
            "model_eligible": False,
            **bindings,
        }
    )
    path = snapshot / "receipt.json"
    if path.exists():
        receipt = read_json(path)
    validate_receipt(receipt, validator, snapshot)
    contract.verify_capture(receipt, contract.source_for(entry, registry), json.loads(receipt["lineage"]["extraction_or_query"]), snapshot, False)
    write_once(path, encoded_json(receipt))
    return path


def verify_local(path: Path) -> dict:
    """Verify the stored original and its proof without network or mutation."""
    receipt = read_json(path)
    validate_receipt(receipt, receipt_validator(), path.parent)
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    registry = load_registry(expected_sha256=lineage["registry_sha256"])
    source = next(s for s in registry["sources"] if s["source_id"] == receipt["source"]["source_record_id"])
    contract.verify_capture(receipt, source, lineage, path.parent, False)
    return receipt


def verify_storage(path: Path, receipt: dict, settings: dict) -> None:
    """Check exact local artifacts, S3 destination, manifest and immutable version evidence."""
    root = path.parent
    reconciliation = read_json(root / "s3_collections_reconciliation.json")
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    source_id = receipt["source"]["source_record_id"]
    route = load_routes(load_registry(expected_sha256=lineage["registry_sha256"]))[source_id]
    require(reconciliation["snapshot_id"] == receipt["snapshot_id"], "Reference storage snapshot differs")
    require(all(reconciliation[k] == v for k, v in route.items()), "Reference storage route differs")
    entries = reconciliation["objects"]
    roles = {a["storage_path"]: a["role"] for a in receipt["artifacts"]} | {"receipt.json": "capture_receipt"}
    require(len(entries) == len(roles) and {e["storage_path"] for e in entries} == set(roles), "Reference storage artifact set differs")

    def check_object(record: dict, local: Path, role: str) -> None:
        sha, size = fingerprint(local)
        prefix = object_prefix(route, source_folder(source_id), role, receipt["snapshot_id"], receipt["snapshot_id"], False)
        require(record["key"] == f"{prefix}/{sha}/{local.name}" and record["bucket"] == settings["data_bucket_name"], "Reference storage key differs")
        require(record["sha256"] == sha and record["byte_count"] == size, "Reference stored bytes differ")
        require(isinstance(record["version_id"], str) and record["version_id"] not in {"", "null"}, "Reference storage version missing")
        require(record["verification"] == "version_get_sha256_and_length_match", "Reference storage readback missing")

    for entry in entries:
        check_object(entry["object"], root / entry["storage_path"], roles[entry["storage_path"]])
    manifests = reconciliation["manifests"]
    require(len(manifests) == 1 and manifests[0]["dataset_id"] == source_folder(source_id), "Reference storage manifest set differs")
    manifest_path = root / "s3_collections" / source_folder(source_id) / "manifest.json"
    manifest = read_json(manifest_path)
    require(
        manifest["objects"] == entries and manifest["source_id"] == source_id and manifest["snapshot_id"] == receipt["snapshot_id"],
        "Reference manifest differs",
    )
    require(manifest["model_eligible"] is False and manifest["snapshot_status"] == "acquired_unvalidated", "Reference manifest hold differs")
    check_object(manifests[0]["object"], manifest_path, "capture_receipt")


def execute(plan: dict, entry: dict, root: Path, downloads: Path, upload: bool, client: AwsCli | None, outputs: dict) -> dict:
    """Run the integrated capture/storage path for one file; a valid completion performs no writes."""
    require(canonical_hash(plan) == canonical_hash(contract.load_plan()) and entry in plan["files"], "Reference selected plan or file differs")
    branch = root / "files" / entry["id"]
    receipt_path = branch / "capture/receipt.json"
    if not receipt_path.exists():
        contract.require_code()
        receipt_path = make_receipt(plan, entry, stage(entry, downloads, branch), branch)
    receipt = verify_local(receipt_path)
    require(json.loads(receipt["lineage"]["extraction_or_query"])["file_id"] == entry["id"], "Reference selected file differs from receipt")
    done_path = branch / "completed.json"
    if done_path.exists():
        done = read_json(done_path)
        require(done["receipt_sha256"] == fingerprint(receipt_path)[0] and done["file_id"] == entry["id"], "Reference completion receipt differs")
        reconciliation = receipt_path.parent / "s3_collections_reconciliation.json"
        require(done["reconciliation_sha256"] == fingerprint(reconciliation)[0], "Reference completion storage evidence differs")
        require(done["model_eligible"] is False and done["status"] == "stored" and plan_matches(plan, done["plan_sha256"]), "Reference completion hold differs")
        settings = client.configuration if client is not None else load_configuration(REPO_ROOT / ".env")[0]
        verify_storage(receipt_path, receipt, settings)
        return done
    result = {
        "file_id": entry["id"],
        "source_id": entry["source_id"],
        "file_name": entry["file_name"],
        "role": entry["role"],
        "status": "capture_ready",
        "model_eligible": False,
        "receipt_path": str(receipt_path),
        "receipt_sha256": fingerprint(receipt_path)[0],
        "plan_sha256": canonical_hash(plan),
    }
    if upload:
        contract.require_code()
        if client is None:
            raise ValueError("Reference storage client missing")
        registry = load_registry(expected_sha256=plan["registry_sha256"])
        stored = upload_snapshot(receipt_path, [], client, outputs, registry, receipt_validator())
        verify_storage(receipt_path, receipt, client.configuration)
        result.update(
            status="stored", reconciliation_path=stored["reconciliation_path"], reconciliation_sha256=fingerprint(Path(stored["reconciliation_path"]))[0]
        )
        write_once(done_path, encoded_json(result))
    return result


def run_all(plan: dict, root: Path, downloads: Path, upload: bool, client: AwsCli | None, outputs: dict) -> tuple[list[dict], list[dict]]:
    """Process every planned file independently; return completed results and held files with their public reason."""
    results, held = [], []
    with collection_lock(root):
        for entry in plan["files"]:
            try:
                results.append(execute(plan, entry, root, downloads, upload, client, outputs))
            except (ValueError, OSError, KeyError) as error:
                # require() messages are fixed public text; other exceptions are reported by type only.
                reason = str(error) if isinstance(error, ValueError) and str(error).startswith(("Reference", "Unreviewed reference")) else type(error).__name__
                held.append({"file_name": entry["file_name"], "file_id": entry["id"], "reason": reason})
    return results, held


def main() -> None:
    """Validate offline by default; write a plan for review with --build-plan; opt into S3 writes with --execute."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--downloads", type=Path, default=Path.home() / "Downloads")
    parser.add_argument("--build-plan", type=Path, help="Reviewed request list to bind to the saved files")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--validate-receipt", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.validate_receipt:
        receipt = verify_local(args.validate_receipt)
        sys.stdout.write(json.dumps({"status": "valid", "snapshot_id": receipt["snapshot_id"], "model_eligible": False}) + "\n")
        return
    if args.build_plan:
        plan = contract.build_plan(read_json(args.build_plan)["files"], args.downloads)
        write_once(contract.PLAN_PATH, encoded_json(plan))
        write_once(contract.PLAN_PATH.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
        sys.stdout.write(json.dumps({"plan": str(contract.PLAN_PATH), "files": [f["file_name"] for f in plan["files"]]}) + "\n")
        return
    plan = contract.load_plan()
    client, outputs = runtime() if args.execute else (None, {})
    results, held = run_all(plan, ROOT, args.downloads, args.execute, client, outputs)
    for result in results:
        LOGGER.info(json.dumps({k: result[k] for k in ("source_id", "file_name", "status")}, sort_keys=True))
    for item in held:
        LOGGER.error(json.dumps({"status": "held"} | item, sort_keys=True))
    LOGGER.info(json.dumps({"completed": len(results), "held": [h["file_name"] for h in held]}))
    sys.exit(1 if held else 0)


if __name__ == "__main__":
    main()
