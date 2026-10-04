"""Capture CMS Hospital All Owners releases locally and store organisation owner rows only, with verified S3 storage.

Offline by default. ``--fetch`` downloads missing originals into an owner-only local folder that never enters a
snapshot or S3; ``--execute`` stores the derived CSV, download proof and scope. Each release is independent: one
that fails is held and named, never partly stored, and the others continue.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

from scripts.acquisition import cms_owners_contract as contract
from scripts.acquisition.bls_api_transport import collection_lock
from scripts.acquisition.capture import base_receipt, receipt_validator, validate_receipt
from scripts.acquisition.collect_hud_api import runtime
from scripts.acquisition.collect_mmd_api import artifact
from scripts.acquisition.collection_layout import load_routes, object_prefix
from scripts.acquisition.dataset_layout import source_folder
from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, upload_snapshot, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require
from scripts.acquisition.transport import Limits, download
from scripts.infrastructure.render_project_config import load_configuration

LOGGER = logging.getLogger(__name__)
ROOT = REPO_ROOT / "data/datasets/historical_acquisition/cms_hospital_owners"
TRANSFORMATIONS = [
    "Kept rows with TYPE - OWNER = O; dropped name, title, street address, city and ZIP columns.",
    "Replaced owner organisation or DBA names containing an individual owner's whole name with [REDACTED].",
    "Other kept cells unchanged; written as UTF-8 CSV with LF line endings.",
]


def private(path: Path, top: Path) -> None:
    """Owner-only permissions on the original and every folder up to the private root."""
    path.chmod(0o600 if path.is_file() else 0o700)
    for parent in path.parents:
        if not parent.is_relative_to(top):
            break
        parent.chmod(0o700)


def completed_download(folder: Path, name: str) -> tuple[Path, dict] | None:
    """The first complete, hash-verified earlier download of this release, if any."""
    for record in sorted(folder.glob("run_*/attempt_*/transport.json")):
        meta = read_json(record)
        path = record.parent / name
        if meta["complete"] and path.is_file() and not path.is_symlink() and fingerprint(path) == (meta["sha256"], meta["byte_count"]):
            return path, meta
    return None


def fetch(release: dict, root: Path, allowed: bool, opener: Any, limits: Limits) -> tuple[Path, dict]:
    """Reuse a verified original, or download one into private_original/; failures stay there as failed attempts."""
    top = root / "private_original"
    folder = top / release["id"]
    name = contract.stored_name(release)
    found = completed_download(folder, name)
    if found is not None:
        return found
    require(allowed, "CMS owners original not captured; run with --fetch")
    folder.mkdir(parents=True, exist_ok=True)
    private(folder, top)
    run = folder / f"run_{len(list(folder.glob('run_*'))) + 1:02}"
    result = download(release["url"], run, name, "csv", "data", limits, opener=opener)
    for path in run.rglob("*"):
        private(path, top)
    require(result.complete, "CMS owners download failed")
    found = completed_download(folder, name)
    if found is None:
        raise ValueError("CMS owners download failed")
    return found


def make_receipt(plan: dict, release: dict, original: Path, meta: dict, branch: Path, root: Path) -> Path:
    """Write the derived CSV, download proof, scope and receipt; the original stays outside the snapshot."""
    registry, validator = load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator()
    require(plan["registry_sha256"] == canonical_hash(registry), "CMS owners base registry changed")
    source = next(s for s in registry["sources"] if s["source_id"] == contract.SOURCE_ID)
    raw = original.read_bytes()
    derived, statistics = contract.derive(raw, release)
    proof = {
        "requested_url": release["url"],
        "resolved_url": meta["resolved_url"],
        "retrieved_at_utc": meta["retrieved_at_utc"],
        "http_status": meta["http_status"],
        "media_type": meta["media_type"],
        "sha256": meta["sha256"],
        "bytes": meta["byte_count"],
        "original_path": original.relative_to(root).as_posix(),
        "original_retention": "This Mac only, owner-only; names individual owners; never uploaded; deleted at collection closeout after user confirmation",
        "statistics": statistics,
        "transformations": TRANSFORMATIONS,
    }
    snapshot = branch / "capture"
    files = (
        ("derived/organisation_owners.csv", derived, "data"),
        ("evidence/download_proof.json", encoded_json(proof), "export_receipt"),
        ("references/scope.json", encoded_json(plan), "layout"),
    )
    for name, body, _ in files:
        write_once(snapshot / name, body)
    metadata = {
        "publisher": "Centers for Medicare & Medicaid Services",
        "release": {
            "advertised_history": "Hospital All Owners, monthly releases on data.cms.gov",
            "dataset_version": None,
            "observed_history": f"{release['title']}; organisation owner rows only; C067/C068 holds preserved",
            "publisher_release_label": release["title"],
            "publisher_updated_at": None,
            "release_date": None,
            "revision_status": "unknown",
        },
        "measurement_periods": [
            {
                "label": f"CMS catalogue period for {release['title']}",
                "start_date": release["period_start"],
                "end_date": release["period_end"],
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
                "Project-transformed subset of a CMS file: organisation owner rows only, not an official CMS file.",
                "Individual owners are removed; never re-link this file to individual owners.",
            ],
        },
    }
    stamp = meta["retrieved_at_utc"].replace("-", "").replace(":", "")[:15] + "Z"
    receipt = base_receipt(source, metadata, f"CMS_OWNERS_ORG__{stamp}__{release['id'][:32]}", validator)
    receipt["acquisition"].update(
        requested_url=release["url"],
        resolved_url=meta["resolved_url"],
        retrieved_at_utc=meta["retrieved_at_utc"],
        http_status=meta["http_status"],
        tool_name="HistoricalSourceCapture",
        tool_version="1.0.0",
        pagination={"required": False, "strategy": "One CSV per monthly release", "page_count": 1, "termination_verified": True, "deduplication_keys": []},
    )
    snapshot_files = [artifact(snapshot / name, snapshot, role, i, False) for i, (name, _, role) in enumerate(files, 1)]
    receipt["artifacts"] = snapshot_files
    columns = contract.kept_columns(release["layout"])
    receipt["schema_profile"].update(
        encoding="utf-8",
        delimiter=",",
        native_headers=columns,
        schema_fingerprint_sha256=canonical_hash(columns),
        row_count=statistics["organisation_rows_kept"],
        parsed_row_count=statistics["organisation_rows_kept"],
        native_identifier_fields=["ENROLLMENT ID", "ASSOCIATE ID", "ASSOCIATE ID - OWNER"],
        native_date_fields=["ASSOCIATION DATE - OWNER"],
    )
    receipt["quality_profile"].update(
        parse_status="passed_with_warnings",
        blank_identifier_rows=0,
        pagination_complete=True,
        checks_passed=["exact_published_header", "owner_type_i_or_o", "no_personal_name_on_kept_rows", "flag_values_y_n_blank", "row_counts_add_up"],
        warnings=[
            "Acquisition checks only. Owner roll-up to hospitals, definitions and model eligibility remain unreviewed.",
            f"Original read as {statistics['encoding']}; {statistics['individual_rows_dropped']} individual owner rows dropped, "
            f"{statistics['redacted_name_cells']} organisation name cells redacted because they contain an individual owner's name.",
            "Missing percentages are not zero; one hospital may have several owner rows.",
            "Private-equity and REIT flags exist only from the April 2025 release; they are never inferred for earlier releases.",
            "Derived file: " + " ".join(TRANSFORMATIONS),
        ],
    )
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "mode": "cms_owners_org",
            "route_id": release["route_id"],
            "scope": f"CMS Hospital All Owners {release['period_start']}, organisation owner rows",
            "release_id": release["id"],
            "plan_sha256": canonical_hash(plan),
            "registry_sha256": canonical_hash(registry),
            "schema_sha256": canonical_hash(validator.schema),
            "expected_sha256": meta["sha256"],
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
    require(reconciliation["snapshot_id"] == receipt["snapshot_id"], "CMS owners storage snapshot differs")
    require(all(reconciliation[k] == v for k, v in route.items()), "CMS owners storage route differs")
    entries = reconciliation["objects"]
    roles = {a["storage_path"]: a["role"] for a in receipt["artifacts"]} | {"receipt.json": "capture_receipt"}
    require(len(entries) == len(roles) and {e["storage_path"] for e in entries} == set(roles), "CMS owners storage artifact set differs")

    def check_object(record: dict, local: Path, role: str) -> None:
        sha, size = fingerprint(local)
        prefix = object_prefix(route, folder, role, receipt["release"]["release_date"] or receipt["snapshot_id"], receipt["snapshot_id"], False)
        require(record["key"] == f"{prefix}/{sha}/{local.name}" and record["bucket"] == settings["data_bucket_name"], "CMS owners storage key differs")
        require(record["sha256"] == sha and record["byte_count"] == size, "CMS owners stored bytes differ")
        require(isinstance(record["version_id"], str) and record["version_id"] not in {"", "null"}, "CMS owners storage version missing")
        require(record["verification"] == "version_get_sha256_and_length_match", "CMS owners storage readback missing")

    for entry in entries:
        check_object(entry["object"], root / entry["storage_path"], roles[entry["storage_path"]])
    manifests = reconciliation["manifests"]
    require(len(manifests) == 1 and manifests[0]["dataset_id"] == folder, "CMS owners storage manifest set differs")
    manifest_path = root / "s3_collections" / folder / "manifest.json"
    manifest = read_json(manifest_path)
    require(
        manifest["objects"] == entries and manifest["source_id"] == contract.SOURCE_ID and manifest["snapshot_id"] == receipt["snapshot_id"],
        "CMS owners manifest differs",
    )
    require(manifest["model_eligible"] is False and manifest["snapshot_status"] == "acquired_unvalidated", "CMS owners manifest hold differs")
    check_object(manifests[0]["object"], manifest_path, "capture_receipt")


def execute(
    plan: dict,
    release: dict,
    root: Path,
    allow_fetch: bool,
    upload: bool,
    client: AwsCli | None,
    outputs: dict,
    opener: Any = None,
    limits: Limits | None = None,
) -> dict:
    """Run the integrated capture/storage path for one release; a valid completion performs no writes."""
    require(canonical_hash(plan) == canonical_hash(contract.load_plan()) and release in plan["releases"], "CMS owners selected plan or release differs")
    branch = root / "batches" / release["id"]
    receipt_path = branch / "capture/receipt.json"
    if not receipt_path.exists():
        contract.require_code()
        original, meta = fetch(release, root, allow_fetch, opener, limits or Limits())
        receipt_path = make_receipt(plan, release, original, meta, branch, root)
    receipt = verify_local(receipt_path)
    require(json.loads(receipt["lineage"]["extraction_or_query"])["release_id"] == release["id"], "CMS owners selected release differs from receipt")
    done_path = branch / "completed.json"
    if done_path.exists():
        done = read_json(done_path)
        require(done["receipt_sha256"] == fingerprint(receipt_path)[0] and done["release_id"] == release["id"], "CMS owners completion receipt differs")
        reconciliation = receipt_path.parent / "s3_collections_reconciliation.json"
        require(done["reconciliation_sha256"] == fingerprint(reconciliation)[0], "CMS owners completion storage evidence differs")
        require(
            done["model_eligible"] is False and done["status"] == "stored" and done["plan_sha256"] == canonical_hash(plan), "CMS owners completion hold differs"
        )
        settings = client.configuration if client is not None else load_configuration(REPO_ROOT / ".env")[0]
        verify_storage(receipt_path, receipt, settings)
        return done
    result = {
        "release_id": release["id"],
        "period_start": release["period_start"],
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
            raise ValueError("CMS owners storage client missing")
        stored = upload_snapshot(receipt_path, [], client, outputs, load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator())
        verify_storage(receipt_path, receipt, client.configuration)
        result.update(
            status="stored", reconciliation_path=stored["reconciliation_path"], reconciliation_sha256=fingerprint(Path(stored["reconciliation_path"]))[0]
        )
        write_once(done_path, encoded_json(result))
    return result


def run_all(
    plan: dict, root: Path, allow_fetch: bool, upload: bool, client: AwsCli | None, outputs: dict, opener: Any = None, limits: Limits | None = None
) -> tuple[list[dict], list[dict]]:
    """Process every release independently; return completed results and held releases with their public reason."""
    results, held = [], []
    with collection_lock(root):
        for release in plan["releases"]:
            try:
                results.append(execute(plan, release, root, allow_fetch, upload, client, outputs, opener, limits))
            except (ValueError, OSError, KeyError) as error:
                # require() messages are fixed public text; other exceptions are reported by type only.
                reason = str(error) if isinstance(error, ValueError) and str(error).startswith("CMS owners") else type(error).__name__
                held.append({"period_start": release["period_start"], "release_id": release["id"], "reason": reason})
    return results, held


def main() -> None:
    """Validate offline by default; opt into CMS downloads and S3 writes independently."""
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
    plan = contract.load_plan()
    client, outputs = runtime() if args.execute else (None, {})
    results, held = run_all(plan, ROOT, args.fetch, args.execute, client, outputs)
    for result in results:
        LOGGER.info(json.dumps({k: result[k] for k in ("period_start", "rows", "status")}, sort_keys=True))
    for item in held:
        LOGGER.error(json.dumps({"status": "held"} | item, sort_keys=True))
    LOGGER.info(json.dumps({"completed": len(results), "held": [h["period_start"] for h in held]}))
    sys.exit(1 if held else 0)


if __name__ == "__main__":
    main()
