"""Collect the locked ACS table-vintage units (15 tables, 173 units) from the Census API and store each as an export-layout CSV with its raw response.

Offline by default. ``--fetch`` requests missing units with the runtime-only Census key; ``--execute`` stores each verified capture. Each unit is independent.
"""

import argparse
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from scripts.acquisition import census_acs_tables_contract as contract
from scripts.acquisition.bls_api_transport import collection_lock
from scripts.acquisition.capture import base_receipt, receipt_validator, validate_receipt
from scripts.acquisition.census_acs_tables_transport import fetch
from scripts.acquisition.collect_census_acs_api import runtime
from scripts.acquisition.collect_mmd_api import artifact
from scripts.acquisition.collection_layout import load_routes, object_prefix
from scripts.acquisition.legacy_versions import plan_matches
from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, upload_snapshot, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require
from scripts.infrastructure.render_project_config import load_configuration

LOGGER = logging.getLogger(__name__)
ROOT = REPO_ROOT / "data/datasets/historical_acquisition/census_acs_tables"
FOOTNOTE = re.compile(r"_[0-9A-Z]+[EM]A$")


def derived_name(batch: dict) -> str:
    """The export-style member name of one unit's CSV."""
    return contract.file_name(batch)


def finish_receipt(receipt: dict, snapshot: Path, source: dict) -> Path:
    """Validate, verify and write the receipt write-once."""
    path = snapshot / "receipt.json"
    if path.exists():
        receipt = read_json(path)
    validate_receipt(receipt, receipt_validator(), snapshot)
    contract.verify_capture(receipt, source, json.loads(receipt["lineage"]["extraction_or_query"]), snapshot, False)
    write_once(path, encoded_json(receipt))
    return path


def make_receipt(plan: dict, batch: dict, envelope: dict, root: Path) -> Path:
    """Raw JSON, export-layout CSV, dictionary, proof and scope for one table-vintage unit."""
    registry, validator = load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator()
    require(plan["registry_sha256"] == canonical_hash(registry), "Census tables base registry changed")
    source = next(s for s in registry["sources"] if s["source_id"] == "ACS")
    transport = root / "batches" / batch["id"] / "transport"
    raw, group = (transport / "response.json").read_bytes(), (transport / "group.json").read_bytes()
    derived, statistics = contract.validate_response(raw, group, batch)
    snapshot = root / "batches" / batch["id"] / "capture"
    proof = envelope | {"statistics": statistics}
    files = (
        ("raw/response.json", raw, "api_page", True),
        (f"derived/{derived_name(batch)}", derived, "data", False),
        ("evidence/request_response.json", encoded_json(proof), "export_receipt", False),
        ("references/group.json", group, "dictionary", True),
        ("references/scope.json", encoded_json(plan), "layout", False),
    )
    for name, body, _, _ in files:
        write_once(snapshot / name, body)
    headers = derived.decode().split("\n", 1)[0].split(",")
    metadata = {
        "publisher": "U.S. Census Bureau",
        "release": {
            **contract.release_text(batch),
            "dataset_version": None,
            "publisher_release_label": None,
            "publisher_updated_at": None,
            "release_date": None,
            "revision_status": "unknown",
        },
        "measurement_periods": [contract.period(batch["year"])],
        "governance": contract.GOVERNANCE,
    }
    stamp = datetime.fromisoformat(envelope["retrieved_at_utc"]).strftime("%Y%m%dT%H%M%SZ")
    receipt = base_receipt(source, metadata, f"Census_ACS_Tables__{stamp}__{batch['id'][:32]}", validator)
    receipt["acquisition"].update(
        transport_mode="api_response",
        requested_url=batch["endpoint"],
        resolved_url=batch["endpoint"],
        retrieved_at_utc=envelope["retrieved_at_utc"],
        http_status=200,
        request_method="GET",
        request_parameters=contract.request_for(batch),
        tool_name=contract.TOOL_NAME,
        tool_version="1.0.0",
        pagination={
            "required": False,
            "strategy": "Group query; exact header and unique counties checked",
            "page_count": 1,
            "termination_verified": True,
            "deduplication_keys": ["state", "county"],
        },
    )
    receipt["artifacts"] = [artifact(snapshot / name, snapshot, role, i, original) for i, (name, _, role, original) in enumerate(files, 1)]
    receipt["schema_profile"].update(
        encoding="utf-8",
        delimiter=",",
        native_headers=headers,
        schema_fingerprint_sha256=canonical_hash(headers),
        row_count=statistics["rows"],
        parsed_row_count=statistics["rows"],
        native_identifier_fields=["GEO_ID", "state", "county"],
        native_date_fields=[],
        native_missing_tokens=[contract.NULL_MARKER],
        native_footnote_fields=[h for h in headers if FOOTNOTE.search(h)],
    )
    receipt["quality_profile"].update(
        parse_status="passed_with_warnings",
        duplicate_candidate_key_rows=0,
        blank_identifier_rows=0,
        pagination_complete=True,
        checks_passed=["exact_headers_against_dictionary", "unique_counties", "county_floor", "reproducible_csv"],
        warnings=[
            "Acquisition checks only. Geography, vintage comparability and model eligibility remain unreviewed.",
            "Raw nulls and special tokens retained; CSV null marker is \\N, distinct from empty strings.",
        ],
    )
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "mode": "census_acs_tables",
            "route_id": contract.route_id(batch),
            "scope": contract.scope_text(batch),
            "batch_id": batch["id"],
            "plan_sha256": canonical_hash(plan),
            "registry_sha256": canonical_hash(registry),
            "schema_sha256": canonical_hash(validator.schema),
            "expected_sha256": envelope["sha256"],
            "dictionary_sha256": envelope["dictionary_sha256"],
            "code_sha256": contract.require_code(),
            "model_eligible": False,
        }
    )
    return finish_receipt(receipt, snapshot, source)


def verify_local(path: Path) -> dict:
    """Verify one capture without network or mutation."""
    receipt = read_json(path)
    validate_receipt(receipt, receipt_validator(), path.parent)
    source = next(s for s in load_registry()["sources"] if s["source_id"] == "ACS")
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
    route = load_routes(registry)["ACS"]
    require(
        reconciliation["snapshot_id"] == receipt["snapshot_id"] and all(reconciliation[k] == v for k, v in route.items()),
        "Census tables storage route differs",
    )
    entries = reconciliation["objects"]
    roles = {a["storage_path"]: a["role"] for a in receipt["artifacts"]} | {"receipt.json": "capture_receipt"}
    require(len(entries) == len(roles) and {e["storage_path"] for e in entries} == set(roles), "Census tables storage artifact set differs")

    def check_object(record: dict, local: Path, role: str) -> None:
        sha, size = fingerprint(local)
        prefix = object_prefix(route, "acs", role, receipt["release"]["release_date"] or receipt["snapshot_id"], receipt["snapshot_id"], False)
        require(record["key"] == f"{prefix}/{sha}/{local.name}" and record["bucket"] == settings["data_bucket_name"], "Census tables storage key differs")
        require(record["sha256"] == sha and record["byte_count"] == size, "Census tables stored bytes differ")
        require(isinstance(record["version_id"], str) and record["version_id"] not in {"", "null"}, "Census tables storage version missing")
        require(record["verification"] == "version_get_sha256_and_length_match", "Census tables storage readback missing")

    for entry in entries:
        check_object(entry["object"], root / entry["storage_path"], roles[entry["storage_path"]])
    manifests = reconciliation["manifests"]
    require(len(manifests) == 1 and manifests[0]["dataset_id"] == "acs", "Census tables storage manifest set differs")
    manifest_path = root / "s3_collections/acs/manifest.json"
    manifest = read_json(manifest_path)
    require(manifest["objects"] == entries and manifest["snapshot_id"] == receipt["snapshot_id"], "Census tables manifest differs")
    require(manifest["model_eligible"] is False and manifest["snapshot_status"] == "acquired_unvalidated", "Census tables manifest hold differs")
    check_object(manifests[0]["object"], manifest_path, "capture_receipt")


def store(plan: dict, identity: str, receipt_path: Path, upload: bool, client: AwsCli | None, outputs: dict) -> dict:
    """Store one verified snapshot once; a completed one only re-verifies."""
    receipt = verify_local(receipt_path)
    require(json.loads(receipt["lineage"]["extraction_or_query"])["batch_id"] == identity, "Census tables capture differs from the selected unit")
    done_path = receipt_path.parent.parent / "completed.json"
    if done_path.exists():
        done = read_json(done_path)
        require(done["receipt_sha256"] == fingerprint(receipt_path)[0] and done["id"] == identity, "Census tables completion receipt differs")
        reconciliation = receipt_path.parent / "s3_collections_reconciliation.json"
        require(done["reconciliation_sha256"] == fingerprint(reconciliation)[0], "Census tables completion storage evidence differs")
        require(
            done["status"] == "stored" and done["model_eligible"] is False and plan_matches(plan, done["plan_sha256"]), "Census tables completion hold differs"
        )
        require(done["rows"] == receipt["schema_profile"]["row_count"], "Census tables completion row count differs")
        settings = client.configuration if client is not None else load_configuration(REPO_ROOT / ".env")[0]
        verify_storage(receipt_path, receipt, settings)
        return done
    result = {
        "id": identity,
        "rows": receipt["schema_profile"]["row_count"],
        "status": "capture_ready",
        "model_eligible": False,
        "plan_sha256": canonical_hash(plan),
        "receipt_path": str(receipt_path),
        "receipt_sha256": fingerprint(receipt_path)[0],
    }
    if upload:
        contract.require_code()
        if client is None:
            raise ValueError("Census tables storage client missing")
        stored = upload_snapshot(receipt_path, [], client, outputs, load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator())
        verify_storage(receipt_path, receipt, client.configuration)
        result.update(status="stored", reconciliation_sha256=fingerprint(Path(stored["reconciliation_path"]))[0])
        write_once(done_path, encoded_json(result))
    return result


def execute(plan: dict, batch: dict, root: Path, allow_network: bool, upload: bool, client: AwsCli | None, outputs: dict, request: Any = None) -> dict:
    """Capture and store one unit; a valid completion performs no writes."""
    require(canonical_hash(plan) == canonical_hash(contract.load_plan()) and batch in plan["batches"], "Census tables selected plan or batch differs")
    receipt_path = root / "batches" / batch["id"] / "capture/receipt.json"
    if not receipt_path.exists():
        contract.require_code()
        envelope = fetch(plan, batch, root, allow_network, client, request)
        receipt_path = make_receipt(plan, batch, envelope, root)
    return store(plan, batch["id"], receipt_path, upload, client, outputs) | {"table": batch["table"], "year": batch["year"]}


def run_all(
    plan: dict, root: Path, allow_network: bool, upload: bool, client: AwsCli | None, outputs: dict, request: Any = None
) -> tuple[list[dict], list[dict]]:
    """Every unit independently; a held unit never stops the others."""
    results, held = [], []
    with collection_lock(root):
        for batch in plan["batches"]:
            try:
                results.append(execute(plan, batch, root, allow_network, upload, client, outputs, request))
            except (ValueError, OSError, KeyError, TypeError) as error:
                # Errors can originate at credential-bearing boundaries; only fixed public text is kept.
                reason = str(error) if isinstance(error, ValueError) and str(error).startswith(("Census", "Duplicate", "Source is")) else type(error).__name__
                held.append({"table": batch["table"], "year": batch["year"], "reason": reason})
    return results, held


def main() -> None:
    """Validate offline by default; opt into Census requests and S3 writes independently."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--validate-receipt", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.validate_receipt:
        receipt = verify_local(args.validate_receipt)
        sys.stdout.write(json.dumps({"status": "valid", "rows": receipt["schema_profile"]["row_count"], "model_eligible": False}) + "\n")
        return
    plan = contract.load_plan()
    client, outputs = runtime() if (args.fetch or args.execute) else (None, {})
    results, held = run_all(plan, ROOT, args.fetch, args.execute, client, outputs)
    for result in results:
        LOGGER.info(json.dumps({k: result[k] for k in ("table", "year", "rows", "status")}, sort_keys=True))
    for item in held:
        LOGGER.error(json.dumps({"status": "held"} | item, sort_keys=True))
    LOGGER.info(json.dumps({"completed": len(results), "held": len(held)}))
    sys.exit(1 if held else 0)


if __name__ == "__main__":
    main()
