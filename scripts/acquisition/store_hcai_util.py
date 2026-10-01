"""Capture the HCAI annual utilization workbooks locally and store abstracted sheet CSVs only, with verified S3 storage.

Offline by default. ``--fetch`` downloads missing originals through the reviewed signed redirect into an owner-only
local folder that never enters a snapshot or S3; ``--execute`` stores one abstracted CSV per sheet, the download proof
and the scope. Each year is independent: one that fails is held and named, and the others continue.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

from scripts.acquisition import hcai_util_contract as contract
from scripts.acquisition.bls_api_transport import collection_lock
from scripts.acquisition.capture import base_receipt, receipt_validator, validate_receipt
from scripts.acquisition.collect_hud_api import runtime
from scripts.acquisition.collect_mmd_api import artifact
from scripts.acquisition.collection_layout import load_routes, object_prefix
from scripts.acquisition.dataset_layout import source_folder
from scripts.acquisition.history_redirects import install_redirect
from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, upload_snapshot, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require
from scripts.acquisition.store_cms_owners import private
from scripts.acquisition.transport import Limits, download
from scripts.infrastructure.render_project_config import load_configuration

LOGGER = logging.getLogger(__name__)
ROOT = REPO_ROOT / "data/historical_acquisition/hcai_util_2018_2025"
TRANSFORMATIONS = [
    "One CSV per workbook sheet, cells as stored in the workbook (Excel date serials and error text kept).",
    "Facility street address and phone, administrator, preparer and revision-preparer names and parent business address replaced with [REDACTED].",
    "On rows whose licensee type is Investor - Individual, parent name, city and ZIP code also replaced.",
    "Any other cell containing an administrator, preparer or individual owner's name replaced with [REDACTED].",
]


def completed_download(folder: Path, entry: dict) -> tuple[Path, dict] | None:
    """The first complete earlier download of this year whose bytes match the plan."""
    for record in sorted(folder.rglob("transport.json")):
        meta = read_json(record)
        path = record.parent / meta["path"]
        if meta["complete"] and path.is_file() and not path.is_symlink() and fingerprint(path) == (entry["sha256"], entry["bytes"]):
            return path, meta
    return None


def fetch(entry: dict, root: Path, allowed: bool, opener: Any, limits: Limits) -> tuple[Path, dict]:
    """Reuse a verified original, or download one through the reviewed redirect into private_original/<year>/."""
    top = root / "private_original"
    folder = top / str(entry["year"])
    found = completed_download(folder, entry) if folder.exists() else None
    if found is not None:
        return found
    require(allowed, "HCAI utilization original not captured; run with --fetch")
    install_redirect(entry["url"], entry["redirect_target"])
    folder.mkdir(parents=True, exist_ok=True)
    private(folder, top)
    run = folder / f"run_{len(list(folder.glob('run_*'))) + 1:02}"
    result = download(entry["url"], run, f"hcai_util_{entry['year']}.xlsx", "xlsx", "data", limits, opener=opener)
    for path in run.rglob("*"):
        private(path, top)
    require(result.complete, "HCAI utilization download failed")
    found = completed_download(folder, entry)
    if found is None:
        raise ValueError("HCAI utilization original differs from the plan")
    return found


def make_receipt(plan: dict, entry: dict, original: Path, meta: dict, branch: Path, root: Path) -> Path:
    """Write the abstracted sheets, download proof, scope and receipt; the original stays outside the snapshot."""
    registry, validator = load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator()
    require(plan["registry_sha256"] == canonical_hash(registry), "HCAI utilization base registry changed")
    source = next(s for s in registry["sources"] if s["source_id"] == contract.SOURCE_ID)
    files, statistics = contract.derive(original.read_bytes(), entry)
    proof = {
        "requested_url": entry["url"],
        "resolved_url": meta["resolved_url"],
        "resolved_url_redacted": meta["resolved_url_redacted"],
        "retrieved_at_utc": meta["retrieved_at_utc"],
        "http_status": meta["http_status"],
        "sha256": meta["sha256"],
        "bytes": meta["byte_count"],
        "original_path": original.relative_to(root).as_posix(),
        "original_retention": "This Mac only, owner-only; names people; never uploaded; deleted at collection closeout after user confirmation",
        "statistics": statistics,
        "transformations": TRANSFORMATIONS,
    }
    snapshot = branch / "capture"
    roles = contract.roles(entry["sheets"])
    items = [(name, files[name], roles[name]) for name in roles]
    items += [("evidence/download_proof.json", encoded_json(proof), "export_receipt"), ("references/scope.json", encoded_json(plan), "layout")]
    for name, body, _ in items:
        write_once(snapshot / name, body)
    kind = "preliminary" if entry["preliminary"] else "final"
    metadata = {
        "publisher": "California Department of Health Care Access and Information",
        "release": {
            "advertised_history": "Hospital Annual Utilization Report, annual workbooks on data.chhs.ca.gov",
            "dataset_version": None,
            "observed_history": f"{entry['title']}; {kind}; abstracted sheets only; measure and linkage holds preserved",
            "publisher_release_label": entry["title"],
            "publisher_updated_at": None,
            "release_date": None,
            "revision_status": "unknown",
        },
        "measurement_periods": [
            {
                "label": f"Report year {entry['year']}",
                "start_date": f"{entry['year']}-01-01",
                "end_date": f"{entry['year']}-12-31",
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
                "Attribute HCAI; this is a project-transformed derivative, marked as modified and not official data.",
                "Personal details were removed under the user's public-business-data exception; never re-identify or publish them.",
            ],
        },
    }
    stamp = meta["retrieved_at_utc"].replace("-", "").replace(":", "")[:15] + "Z"
    # The registry route is on access hold until closeout; the route actually used is the published workbook file.
    receipt = base_receipt(source | {"preferred_route": "file_download"}, metadata, f"HCAI_UTIL_WORKBOOK__{stamp}__{entry['id'][:32]}", validator)
    receipt["acquisition"].update(
        requested_url=entry["url"],
        resolved_url=meta["resolved_url"],
        retrieved_at_utc=meta["retrieved_at_utc"],
        http_status=meta["http_status"],
        tool_name="HistoricalSourceCapture",
        tool_version="1.0.0",
        pagination={"required": False, "strategy": "One workbook per report year", "page_count": 1, "termination_verified": True, "deduplication_keys": []},
    )
    receipt["artifacts"] = [artifact(snapshot / name, snapshot, role, i, False) for i, (name, _, role) in enumerate(items, 1)]
    receipt["schema_profile"].update(
        encoding="utf-8",
        delimiter=",",
        native_headers=entry["header"],
        schema_fingerprint_sha256=canonical_hash(entry["header"]),
        row_count=statistics["hospitals"]["Page 1-6"],
        parsed_row_count=statistics["hospitals"]["Page 1-6"],
        native_identifier_fields=["FAC_NO", "LICENSE_NO"] if "LICENSE_NO" in entry["header"] else ["FAC_NO"],
    )
    receipt["quality_profile"].update(
        parse_status="passed_with_warnings",
        pagination_complete=True,
        checks_passed=["recorded_original_hash", "sheet_names", "exact_data_header", "personal_columns_reviewed", "no_contact_details_after_redaction"],
        warnings=[
            "Acquisition checks only. Definitions by year, hospital identity, joins and model eligibility remain unreviewed.",
            f"Redacted cells: {statistics['redacted_cells']}; copied-name cells: {statistics['copied_name_cells']}; "
            f"individual-owner rows: {statistics['individual_owner_rows']}.",
            "Rows 2-4 of the data sheets are publisher label rows, not hospitals; dates are Excel serial numbers.",
            *(["Preliminary data: HCAI will revise it; a replacement file needs a new plan."] if entry["preliminary"] else []),
            "Derived files: " + " ".join(TRANSFORMATIONS),
        ],
    )
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "mode": "hcai_util_workbook",
            "route_id": f"HCAI_UTIL:workbook:{entry['year']}:{entry['resource_id']}",
            "scope": f"HCAI hospital annual utilization {entry['year']} ({kind}), abstracted sheets",
            "year_id": entry["id"],
            "plan_sha256": canonical_hash(plan),
            "registry_sha256": canonical_hash(registry),
            "schema_sha256": canonical_hash(validator.schema),
            "access_release_sha256": plan["access_release"]["sha256"],
            "expected_sha256": entry["sha256"],
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
    """Verify every abstracted sheet against the local original without network or mutation."""
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
    require(reconciliation["snapshot_id"] == receipt["snapshot_id"], "HCAI utilization storage snapshot differs")
    require(all(reconciliation[k] == v for k, v in route.items()), "HCAI utilization storage route differs")
    entries = reconciliation["objects"]
    roles = {a["storage_path"]: a["role"] for a in receipt["artifacts"]} | {"receipt.json": "capture_receipt"}
    require(len(entries) == len(roles) and {e["storage_path"] for e in entries} == set(roles), "HCAI utilization storage artifact set differs")

    def check_object(record: dict, local: Path, role: str) -> None:
        sha, size = fingerprint(local)
        prefix = object_prefix(route, folder, role, receipt["release"]["release_date"] or receipt["snapshot_id"], receipt["snapshot_id"], False)
        require(record["key"] == f"{prefix}/{sha}/{local.name}" and record["bucket"] == settings["data_bucket_name"], "HCAI utilization storage key differs")
        require(record["sha256"] == sha and record["byte_count"] == size, "HCAI utilization stored bytes differ")
        require(isinstance(record["version_id"], str) and record["version_id"] not in {"", "null"}, "HCAI utilization storage version missing")
        require(record["verification"] == "version_get_sha256_and_length_match", "HCAI utilization storage readback missing")

    for entry in entries:
        check_object(entry["object"], root / entry["storage_path"], roles[entry["storage_path"]])
    manifests = reconciliation["manifests"]
    require(len(manifests) == 1 and manifests[0]["dataset_id"] == folder, "HCAI utilization storage manifest set differs")
    manifest_path = root / "s3_collections" / folder / "manifest.json"
    manifest = read_json(manifest_path)
    require(
        manifest["objects"] == entries and manifest["source_id"] == contract.SOURCE_ID and manifest["snapshot_id"] == receipt["snapshot_id"],
        "HCAI utilization manifest differs",
    )
    require(manifest["model_eligible"] is False and manifest["snapshot_status"] == "acquired_unvalidated", "HCAI utilization manifest hold differs")
    check_object(manifests[0]["object"], manifest_path, "capture_receipt")


def execute(
    plan: dict, entry: dict, root: Path, allow_fetch: bool, upload: bool, client: AwsCli | None, outputs: dict, opener: Any = None, limits: Limits | None = None
) -> dict:
    """Run the integrated capture/storage path for one year; a valid completion performs no writes."""
    require(canonical_hash(plan) == canonical_hash(contract.load_plan()) and entry in plan["years"], "HCAI utilization selected plan or year differs")
    branch = root / "batches" / entry["id"]
    receipt_path = branch / "capture/receipt.json"
    if not receipt_path.exists():
        contract.require_code()
        original, meta = fetch(entry, root, allow_fetch, opener, limits or Limits())
        receipt_path = make_receipt(plan, entry, original, meta, branch, root)
    receipt = verify_local(receipt_path)
    require(json.loads(receipt["lineage"]["extraction_or_query"])["year_id"] == entry["id"], "HCAI utilization selected year differs from receipt")
    done_path = branch / "completed.json"
    if done_path.exists():
        done = read_json(done_path)
        require(done["receipt_sha256"] == fingerprint(receipt_path)[0] and done["year_id"] == entry["id"], "HCAI utilization completion receipt differs")
        reconciliation = receipt_path.parent / "s3_collections_reconciliation.json"
        require(done["reconciliation_sha256"] == fingerprint(reconciliation)[0], "HCAI utilization completion storage evidence differs")
        require(
            done["model_eligible"] is False and done["status"] == "stored" and done["plan_sha256"] == canonical_hash(plan),
            "HCAI utilization completion hold differs",
        )
        settings = client.configuration if client is not None else load_configuration(REPO_ROOT / ".env")[0]
        verify_storage(receipt_path, receipt, settings)
        return done
    result = {
        "year_id": entry["id"],
        "year": entry["year"],
        "hospitals": receipt["schema_profile"]["row_count"],
        "status": "capture_ready",
        "model_eligible": False,
        "receipt_path": str(receipt_path),
        "receipt_sha256": fingerprint(receipt_path)[0],
        "plan_sha256": canonical_hash(plan),
    }
    if upload:
        contract.require_code()
        if client is None:
            raise ValueError("HCAI utilization storage client missing")
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
    """Process every year independently; return completed results and held years with their public reason."""
    results, held = [], []
    with collection_lock(root):
        for entry in plan["years"]:
            try:
                results.append(execute(plan, entry, root, allow_fetch, upload, client, outputs, opener, limits))
            except (ValueError, OSError, KeyError) as error:
                # require() messages are fixed public text; other exceptions are reported by type only.
                reason = str(error) if isinstance(error, ValueError) and str(error).startswith(("HCAI", "Source is")) else type(error).__name__
                held.append({"year": entry["year"], "year_id": entry["id"], "reason": reason})
    return results, held


def main() -> None:
    """Validate offline by default; opt into HCAI downloads and S3 writes independently."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--validate-receipt", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    os.umask(0o077)
    if args.validate_receipt:
        receipt = verify_local(args.validate_receipt)
        sys.stdout.write(json.dumps({"status": "valid", "hospitals": receipt["schema_profile"]["row_count"], "model_eligible": False}) + "\n")
        return
    plan = contract.load_plan()
    client, outputs = runtime() if args.execute else (None, {})
    results, held = run_all(plan, ROOT, args.fetch, args.execute, client, outputs)
    for result in results:
        LOGGER.info(json.dumps({k: result[k] for k in ("year", "hospitals", "status")}, sort_keys=True))
    for item in held:
        LOGGER.error(json.dumps({"status": "held"} | item, sort_keys=True))
    LOGGER.info(json.dumps({"completed": len(results), "held": [h["year"] for h in held]}))
    sys.exit(1 if held else 0)


if __name__ == "__main__":
    main()
