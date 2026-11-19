"""Check and store the AHRQ Compendium hospital linkage files the owner saved in a browser, with immutable originals and verified S3 storage.

Offline and read-only by default: without ``--execute`` nothing is written (no lock, staged copy, receipt or folder).
Each file is independent: a file that fails its checks is held and named, never partly stored, and the others continue.
``--build-plan`` binds the reviewed request list to the saved files for locking; it writes nothing to S3. The same model as
the reference-document download, with a data role for the six linkage workbooks and the AHRQ access hold released by one bound record.
"""

import argparse
import json
import logging
import re
import sys
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path
from typing import cast

from scripts.acquisition import ahrq_compendium_contract as contract
from scripts.acquisition import hud_xlsx_contract as download_metadata
from scripts.acquisition.bls_api_transport import collection_lock
from scripts.acquisition.capture import base_receipt, receipt_validator, validate_receipt
from scripts.acquisition.collect_hud_api import runtime
from scripts.acquisition.collect_mmd_api import artifact
from scripts.acquisition.collection_layout import capture_root, load_routes, object_prefix
from scripts.acquisition.dataset_layout import source_folder
from scripts.acquisition.legacy_versions import plan_matches
from scripts.acquisition.s3_store import AwsCli, component, encoded_json, fingerprint, upload_snapshot, write_once
from scripts.acquisition.single_copy import load_policy
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require
from scripts.infrastructure.render_project_config import load_configuration

LOGGER = logging.getLogger(__name__)
ROOT = REPO_ROOT / "data/datasets/historical_acquisition/ahrq_compendium"


def planned_download(entry: dict, downloads: Path) -> bytes:
    """Read a saved file and require the planned origin, hash and size; nothing is written."""
    body = contract.read_download(entry, downloads)
    require((contract.digest(body), len(body)) == (entry["sha256"], entry["bytes"]), "AHRQ download differs from the plan")
    return body


def stage(entry: dict, downloads: Path, branch: Path) -> Path:
    """Copy the exact planned file write-once with its origin proof; after that the Downloads copy is not needed."""
    cached, proof = branch / "source" / entry["file_name"], branch / "source" / "download.json"
    # Both files present means staging finished; otherwise redo it (write_once accepts identical bytes).
    if cached.exists() and proof.exists():
        require(fingerprint(cached) == (entry["sha256"], entry["bytes"]), "AHRQ staged copy differs from the plan")
        return cached
    body = planned_download(entry, downloads)
    created = download_metadata.download_created_at(downloads / entry["file_name"])
    write_once(cached, body)
    require(fingerprint(cached) == (entry["sha256"], entry["bytes"]), "AHRQ staged copy differs from the plan")
    # Only the planned URL is kept; a referring page the browser also recorded is not evidence.
    write_once(proof, encoded_json({"file_name": entry["file_name"], "origin_url": entry["origin_url"], "created_at_utc": created}))
    return cached


def make_receipt(plan: dict, entry: dict, cached: Path, branch: Path) -> Path:
    """Write the original, its download proof and the receipt; a browser download has no HTTP status, so none is recorded."""
    registry, validator = load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator()
    source = contract.source_of(registry)
    download = read_json(cached.parent / "download.json")
    proof = download | {"sha256": entry["sha256"], "bytes": entry["bytes"], "saved_by": "owner, browser"}
    snapshot = branch / "capture"
    files = (
        (f"raw/{entry['file_name']}", cached.read_bytes(), entry["role"], True),
        ("evidence/download_proof.json", encoded_json(proof), "export_receipt", False),
    )
    for name, body, _, _ in files:
        write_once(snapshot / name, body)
    label = (
        f"AHRQ Compendium hospital linkage file, {entry['vintage']} vintage" if entry["role"] == "data" else f"{entry['title']} ({entry['vintage']} vintage)"
    )
    metadata = {
        "publisher": entry["publisher"],
        "release": {
            "advertised_history": None,
            "dataset_version": entry["vintage"],
            "observed_history": f"{entry['title']}, saved by hand in a browser from the publisher's URL",
            "publisher_release_label": label,
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
            "use_restrictions": contract.USE_RESTRICTIONS,
        },
    }
    timestamp = datetime.fromisoformat(download["created_at_utc"]).strftime("%Y%m%dT%H%M%SZ")
    receipt = base_receipt(source | {"preferred_route": "file_download"}, metadata, f"AHRQ_SYSTEM__{timestamp}__{entry['id'][:32]}", validator)
    receipt["acquisition"].update(
        transport_mode="download" if entry["role"] == "data" else "reference_document",
        requested_url=entry["origin_url"],
        resolved_url=entry["origin_url"],
        retrieved_at_utc=download["created_at_utc"],
        http_status=None,
        request_method="manual_download",
        tool_name="browser_ahrq_download",
        tool_version="1.0.0",
    )
    receipt["artifacts"] = [artifact(snapshot / name, snapshot, role, i, original) for i, (name, _, role, original) in enumerate(files, 1)]
    receipt["artifacts"][0].update(media_type=entry["media_type"], hash_scope="complete_file")
    receipt["quality_profile"].update(
        checks_passed=["planned_origin_url", "planned_sha256_and_size", "file_signature"],
        warnings=[
            "Acquisition checks only. Sheets, columns, reference dates and model eligibility remain unreviewed.",
            "Saved by hand in a browser; retrieval time is the file creation time; no HTTP status or headers exist for it.",
            "AHRQ publishes no terms text for these files; the evidence checked is the terms record in the receipt lineage, and no reuse license is inferred.",
            "The registry access hold was lifted for this capture by the owner's recorded release.",
        ],
    )
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "mode": contract.MODE,
            "route_id": f"{entry['source_id']}:manual_download:{entry['id'][:16]}",
            "scope": f"{entry['title']} ({entry['role']}), saved by hand from {entry['origin_url']}",
            "file_id": entry["id"],
            "plan_sha256": canonical_hash(plan),
            "registry_sha256": plan["registry_sha256"],
            "schema_sha256": canonical_hash(validator.schema),
            "expected_sha256": entry["sha256"],
            "code_sha256": contract.require_code(),
            "access_release_sha256": plan["access_release"]["sha256"],
            "terms_record": contract.TERMS_RECORD,
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
    """Verify the stored original and its proof without network or mutation."""
    receipt = read_json(path)
    validate_receipt(receipt, receipt_validator(), path.parent)
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    registry = load_registry(expected_sha256=lineage["registry_sha256"])
    contract.verify_capture(receipt, contract.source_of(registry), lineage, path.parent, False)
    return receipt


CONTENT_KEY = re.compile(r"(bronze_objects|bronze_evidence)/sha256/[0-9a-f]{64}")
READ_OPERATIONS = frozenset({"head-object", "get-object"})


class PrefixGuard:
    """Hand the storage writer a client that refuses any write outside this collection's prefix before the request is sent.

    A key is allowed under the collection's own prefix, or as the content-addressed key the single-copy policy selects; every other key, and every write
    operation other than a create-only object put, is refused.
    """

    def __init__(self, client: AwsCli) -> None:
        self.client = client

    def __getattr__(self, name: str) -> object:
        return getattr(self.client, name)

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
        args = arguments or []
        if "--key" in args and operation not in READ_OPERATIONS:
            key = args[args.index("--key") + 1]
            root = capture_root()
            own = key.startswith(f"{root}{contract.PUBLISHER_PREFIX}")
            content = key.startswith(root) and CONTENT_KEY.fullmatch(key.removeprefix(root)) is not None and bool(self.client.storage_policy["enabled"])
            require(operation == "put-object" and (own or content), "AHRQ upload outside its collection prefix")
        else:
            require(
                operation in READ_OPERATIONS or not operation.startswith(("put-", "delete-", "copy-", "restore-")), "AHRQ upload outside its collection prefix"
            )
        return self.client.call(service, operation, arguments)


def selected_policy(client: AwsCli | None) -> dict:
    """The single-copy policy the storage writer selects: the client's, or the committed one when replaying without a client."""
    return client.storage_policy if client is not None else load_policy()


def verify_storage(path: Path, receipt: dict, settings: dict, policy: dict) -> None:
    """Check exact local artifacts, S3 destination under the writer's policy, manifest and immutable version evidence."""
    root = path.parent
    reconciliation = read_json(root / "s3_collections_reconciliation.json")
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    route = load_routes(load_registry(expected_sha256=lineage["registry_sha256"]))[contract.SOURCE_ID]
    folder = source_folder(contract.SOURCE_ID)
    require(reconciliation["snapshot_id"] == receipt["snapshot_id"], "AHRQ storage snapshot differs")
    require(all(reconciliation[k] == v for k, v in route.items()), "AHRQ storage route differs")
    entries = reconciliation["objects"]
    roles = {a["storage_path"]: a["role"] for a in receipt["artifacts"]} | {"receipt.json": "capture_receipt"}
    require(len(entries) == len(roles) and {e["storage_path"] for e in entries} == set(roles), "AHRQ storage artifact set differs")

    def check_object(record: dict, local: Path, role: str, manifest: bool = False) -> None:
        sha, size = fingerprint(local)
        single = policy["enabled"] and not manifest
        prefix = object_prefix(
            route, folder, "transport_audit" if single and role == "capture_receipt" else role, receipt["snapshot_id"], receipt["snapshot_id"], False
        )
        allowed = {f"{prefix}/{sha}/{component(local.name)}"}
        if single:
            scope = "evidence_objects" if prefix.removeprefix(capture_root()).split("/")[2] == "audit" else "objects"
            known = policy[scope].get(sha)
            content = "bronze_evidence" if scope == "evidence_objects" else "bronze_objects"
            allowed = {f"{capture_root()}{content}/sha256/{sha}"} | ({known["key"]} if known else set())
            require(known is None or record["key"] != known["key"] or record["version_id"] == known["version_id"], "AHRQ storage key differs")
        require(record["key"] in allowed and record["bucket"] == settings["data_bucket_name"], "AHRQ storage key differs")
        require(record["sha256"] == sha and record["byte_count"] == size, "AHRQ stored bytes differ")
        require(isinstance(record["version_id"], str) and record["version_id"] not in {"", "null"}, "AHRQ storage version missing")
        require(record["verification"] == "version_get_sha256_and_length_match", "AHRQ storage readback missing")

    for entry in entries:
        check_object(entry["object"], root / entry["storage_path"], roles[entry["storage_path"]])
    manifests = reconciliation["manifests"]
    require(len(manifests) == 1 and manifests[0]["dataset_id"] == folder, "AHRQ storage manifest set differs")
    manifest_path = root / "s3_collections" / folder / "manifest.json"
    manifest = read_json(manifest_path)
    require(
        manifest["objects"] == entries and manifest["source_id"] == contract.SOURCE_ID and manifest["snapshot_id"] == receipt["snapshot_id"],
        "AHRQ manifest differs",
    )
    require(manifest["model_eligible"] is False and manifest["snapshot_status"] == "acquired_unvalidated", "AHRQ manifest hold differs")
    check_object(manifests[0]["object"], manifest_path, "capture_receipt", True)


def execute(plan: dict, entry: dict, root: Path, downloads: Path, upload: bool, client: AwsCli | None, outputs: dict, read_only: bool = False) -> dict:
    """Run the integrated capture/storage path for one file; a valid completion, or a read-only run, performs no writes."""
    require(canonical_hash(plan) == canonical_hash(contract.load_plan()) and entry in plan["files"], "AHRQ selected plan or file differs")
    capture_root()  # a root outside the two allowed forms is refused before any effect
    branch = root / "files" / entry["id"]
    receipt_path = branch / "capture/receipt.json"
    if not receipt_path.exists():
        contract.require_code()
        if read_only:
            planned_download(entry, downloads)
            return {
                "file_id": entry["id"],
                "source_id": entry["source_id"],
                "file_name": entry["file_name"],
                "role": entry["role"],
                "vintage": entry["vintage"],
                "status": "download_verified",
                "model_eligible": False,
                "plan_sha256": canonical_hash(plan),
            }
        receipt_path = make_receipt(plan, entry, stage(entry, downloads, branch), branch)
    receipt = verify_local(receipt_path)
    require(json.loads(receipt["lineage"]["extraction_or_query"])["file_id"] == entry["id"], "AHRQ selected file differs from receipt")
    done_path = branch / "completed.json"
    if done_path.exists():
        done = read_json(done_path)
        require(done["receipt_sha256"] == fingerprint(receipt_path)[0] and done["file_id"] == entry["id"], "AHRQ completion receipt differs")
        reconciliation = receipt_path.parent / "s3_collections_reconciliation.json"
        require(done["reconciliation_sha256"] == fingerprint(reconciliation)[0], "AHRQ completion storage evidence differs")
        require(done["model_eligible"] is False and done["status"] == "stored" and plan_matches(plan, done["plan_sha256"]), "AHRQ completion hold differs")
        settings = client.configuration if client is not None else load_configuration(REPO_ROOT / ".env")[0]
        verify_storage(receipt_path, receipt, settings, selected_policy(client))
        return done
    result = {
        "file_id": entry["id"],
        "source_id": entry["source_id"],
        "file_name": entry["file_name"],
        "role": entry["role"],
        "vintage": entry["vintage"],
        "status": "capture_ready",
        "model_eligible": False,
        "receipt_path": str(receipt_path),
        "receipt_sha256": fingerprint(receipt_path)[0],
        "plan_sha256": canonical_hash(plan),
    }
    if upload:
        contract.require_code()
        if client is None:
            raise ValueError("AHRQ storage client missing")
        registry = load_registry(expected_sha256=plan["registry_sha256"])
        stored = upload_snapshot(receipt_path, [], cast(AwsCli, PrefixGuard(client)), outputs, registry, receipt_validator())
        verify_storage(receipt_path, receipt, client.configuration, client.storage_policy)
        result.update(
            status="stored", reconciliation_path=stored["reconciliation_path"], reconciliation_sha256=fingerprint(Path(stored["reconciliation_path"]))[0]
        )
        write_once(done_path, encoded_json(result))
    return result


def run_all(
    plan: dict, root: Path, downloads: Path, upload: bool, client: AwsCli | None, outputs: dict, read_only: bool = False
) -> tuple[list[dict], list[dict]]:
    """Process every planned file independently; return completed results and held files with their public reason."""
    results, held = [], []
    with nullcontext() if read_only else collection_lock(root):
        for entry in plan["files"]:
            try:
                results.append(execute(plan, entry, root, downloads, upload, client, outputs, read_only))
            except (ValueError, OSError, KeyError) as error:
                # require() messages are fixed public text; other exceptions are reported by type only.
                reason = str(error) if isinstance(error, ValueError) and str(error).startswith(("AHRQ", "Unreviewed AHRQ")) else type(error).__name__
                held.append({"file_name": entry["file_name"], "file_id": entry["id"], "reason": reason})
    return results, held


def main() -> None:
    """Validate offline and read-only by default; write a plan for review with --build-plan; opt into every write, including S3, with --execute."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--downloads", type=Path, default=Path.home() / "Downloads" / "ahrq_compendium")
    parser.add_argument("--build-plan", action="store_true", help="Bind the reviewed request list to the saved files; writes the plan and its lock")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--validate-receipt", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.validate_receipt:
        receipt = verify_local(args.validate_receipt)
        sys.stdout.write(json.dumps({"status": "valid", "snapshot_id": receipt["snapshot_id"], "model_eligible": False}) + "\n")
        return
    if args.build_plan:
        plan = contract.build_plan(read_json(contract.REQUESTS_PATH)["files"], args.downloads)
        write_once(contract.PLAN_PATH, encoded_json(plan))
        write_once(contract.PLAN_PATH.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
        sys.stdout.write(json.dumps({"plan": str(contract.PLAN_PATH), "files": [f["file_name"] for f in plan["files"]]}) + "\n")
        return
    plan = contract.load_plan()
    client, outputs = runtime() if args.execute else (None, {})
    results, held = run_all(plan, ROOT, args.downloads, args.execute, client, outputs, not args.execute)
    for result in results:
        LOGGER.info(json.dumps({k: result[k] for k in ("file_name", "vintage", "status")}, sort_keys=True))
    for item in held:
        LOGGER.error(json.dumps({"status": "held"} | item, sort_keys=True))
    LOGGER.info(json.dumps({"completed": len(results), "held": [h["file_name"] for h in held]}))
    sys.exit(1 if held else 0)


if __name__ == "__main__":
    main()
