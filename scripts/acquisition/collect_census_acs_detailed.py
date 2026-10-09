"""Collect locked ACS detailed tables (B17001, B16001) and store derived poverty and limited-English history.

Offline by default. ``--fetch`` requests missing table-years with the runtime-only Census key; ``--execute`` stores
each capture and, once every capture and overlap check passes, the derived snapshot. Each capture is independent.
"""

import argparse
import json
import logging
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition import census_acs_detailed_contract as contract
from scripts.acquisition import redownload_controls as controls
from scripts.acquisition.bls_api_transport import RetryableRequest, collection_lock
from scripts.acquisition.capture import base_receipt, receipt_validator, validate_receipt
from scripts.acquisition.census_acs_api_transport import request_json
from scripts.acquisition.collect_census_acs_api import runtime
from scripts.acquisition.collect_mmd_api import artifact
from scripts.acquisition.collection_layout import load_routes, object_prefix
from scripts.acquisition.data_paths import current
from scripts.acquisition.legacy_versions import plan_hashes, plan_matches
from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, upload_snapshot, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require
from scripts.infrastructure.render_project_config import load_configuration

LOGGER = logging.getLogger(__name__)
ROOT = REPO_ROOT / "data/datasets/historical_acquisition/census_acs_detailed"
GOVERNANCE = {
    "access_class": "public",
    "contains_phi": False,
    "contains_pii": False,
    "credential_reference": "census_api_key",
    "credentials_required": True,
    "license_or_terms_url": None,
    "use_restrictions": ["Public aggregate data only; all source/measure, geography, revision and modeling holds remain."],
}


def live_request(plan: dict, batch: dict, client: AwsCli) -> bytes:
    """One keyed group request for the batch's year."""
    require(plan["credential_reference"] == "census_api_key", "Census detailed request scope differs; secret not read")
    return request_json(contract.endpoint(batch["year"]), contract.request_for(batch), client)


def fetch(plan: dict, batch: dict, root: Path, allow_network: bool, client: AwsCli | None, request: Callable | None, sleep: Callable = time.sleep) -> dict:
    """Reuse a cached successful response, or request it with three bounded attempts for transient failures."""
    path = root / "batches" / batch["id"] / "transport.json"
    if path.exists():
        envelope = read_json(path)
        raw = bytes.fromhex(envelope["body_hex"])
        require(
            envelope["request"] == contract.request_for(batch) and envelope["endpoint"] == contract.endpoint(batch["year"]), "Census detailed cache differs"
        )
        require(envelope["sha256"] == contract.digest(raw) and envelope["bytes"] == len(raw), "Census detailed cached response differs")
        return envelope
    if not allow_network or client is None:
        raise ValueError("Census detailed response missing; --fetch required")
    action = request or live_request
    for attempt in range(3):
        controls.begin(batch["id"])
        try:
            raw = controls.returned(action, plan, batch, client)
            break
        except RetryableRequest:
            if attempt == 2:
                raise
            sleep(2 ** (attempt + 1))
    envelope = {
        "endpoint": contract.endpoint(batch["year"]),
        "request": contract.request_for(batch),
        "retrieved_at_utc": datetime.now(UTC).isoformat(),
        "sha256": contract.digest(raw),
        "bytes": len(raw),
        "body_hex": raw.hex(),
        "http_status": 200,
    }
    write_once(path, encoded_json(envelope))
    sleep(plan["request_spacing_seconds"])
    return envelope


def finish_receipt(receipt: dict, snapshot: Path, source: dict) -> Path:
    """Validate, verify and write the receipt write-once."""
    path = snapshot / "receipt.json"
    if path.exists():
        receipt = read_json(path)
    validate_receipt(receipt, receipt_validator(), snapshot)
    contract.verify_capture(receipt, source, json.loads(receipt["lineage"]["extraction_or_query"]), snapshot, False)
    write_once(path, encoded_json(receipt))
    return path


def period(year: int) -> dict:
    """The five-year survey window ending in the table year."""
    return {
        "label": "Five-year survey estimate window",
        "start_date": f"{year - 4}-01-01",
        "end_date": f"{year}-12-31",
        "period_type": "multi_year",
        "source_basis": "field_observed",
        "target_alignment": "unknown",
    }


def make_receipt(plan: dict, batch: dict, envelope: dict, root: Path) -> Path:
    """Raw JSON, lossless CSV, dictionary, proof and scope for one table-year."""
    registry, validator = load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator()
    require(plan["registry_sha256"] == canonical_hash(registry), "Census detailed base registry changed")
    source = next(s for s in registry["sources"] if s["source_id"] == "ACS")
    raw = bytes.fromhex(envelope["body_hex"])
    derived, statistics = contract.validate_response(raw, batch)
    snapshot = root / "batches" / batch["id"] / "capture"
    proof = {k: v for k, v in envelope.items() if k != "body_hex"} | {"statistics": statistics}
    files = (
        ("raw/response.json", raw, "api_page", True),
        ("derived/observations.csv", derived, "data", False),
        ("evidence/request_response.json", encoded_json(proof), "export_receipt", False),
        ("references/group.json", (REPO_ROOT / current(batch["metadata"]["path"])).read_bytes(), "dictionary", True),
        ("references/scope.json", encoded_json(plan), "layout", False),
    )
    for name, body, _, _ in files:
        write_once(snapshot / name, body)
    metadata = {
        "publisher": "U.S. Census Bureau",
        "release": {
            "advertised_history": f"{batch['year'] - 4}-{batch['year']} ACS 5-Year Estimates, detailed tables",
            "dataset_version": None,
            "observed_history": f"{batch['year']} {batch['table']} all-fields county request; input to derived history",
            "publisher_release_label": None,
            "publisher_updated_at": None,
            "release_date": None,
            "revision_status": "unknown",
        },
        "measurement_periods": [period(batch["year"])],
        "governance": GOVERNANCE,
    }
    stamp = datetime.fromisoformat(envelope["retrieved_at_utc"]).strftime("%Y%m%dT%H%M%SZ")
    receipt = base_receipt(source, metadata, f"Census_Detailed__{stamp}__{batch['id'][:32]}", validator)
    receipt["acquisition"].update(
        transport_mode="api_response",
        requested_url=contract.endpoint(batch["year"]),
        resolved_url=contract.endpoint(batch["year"]),
        retrieved_at_utc=envelope["retrieved_at_utc"],
        http_status=200,
        request_parameters=contract.request_for(batch),
        tool_name="census_acs_detailed_collector",
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
        native_headers=batch["headers"],
        schema_fingerprint_sha256=canonical_hash(batch["headers"]),
        row_count=statistics["rows"],
        parsed_row_count=statistics["rows"],
        native_identifier_fields=["GEO_ID", "state", "county"],
        native_missing_tokens=[contract.NULL_MARKER],
    )
    receipt["quality_profile"].update(
        parse_status="passed_with_warnings",
        duplicate_candidate_key_rows=0,
        pagination_complete=True,
        checks_passed=["exact_headers", "unique_counties", "county_floor", "reproducible_csv"],
        warnings=["Acquisition checks only. Geography, vintage comparability and model eligibility remain unreviewed."],
    )
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "mode": "census_acs_detailed",
            "route_id": f"ACS:detailed_api:{batch['table']}:{batch['year']}",
            "scope": f"{batch['year']} {batch['table']} five-year county table",
            "batch_id": batch["id"],
            "plan_sha256": canonical_hash(plan),
            "registry_sha256": canonical_hash(registry),
            "schema_sha256": canonical_hash(validator.schema),
            "expected_sha256": contract.digest(raw),
            "code_sha256": contract.require_code(),
            "model_eligible": False,
        }
    )
    return finish_receipt(receipt, snapshot, source)


def derived_id(plan: dict) -> str:
    """Identify the derived snapshot by its plan."""
    return canonical_hash({"derived": canonical_hash(plan)})


def make_derived(plan: dict, root: Path) -> Path:
    """Derived CSVs, the overlap check report and scope, built only from verified local captures."""
    registry, validator = load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator()
    source = next(s for s in registry["sources"] if s["source_id"] == "ACS")
    inputs, parents = {}, []
    for batch in plan["batches"]:
        capture = root / "batches" / batch["id"] / "capture"
        inputs[batch["id"]] = (capture / "derived/observations.csv").read_bytes()
        parents.append(read_json(capture / "receipt.json")["snapshot_id"])
    files, report = contract.derive(plan, inputs)
    snapshot = root / "batches" / derived_id(plan) / "capture"
    items = [(name, body, "data") for name, body in files.items()]
    items += [("evidence/checks.json", encoded_json(report), "export_receipt"), ("references/scope.json", encoded_json(plan), "layout")]
    for name, body, _ in items:
        write_once(snapshot / name, body)
    first = min(min(v) for v in plan["derived_years"].values())
    last = max(max(v) for v in plan["derived_years"].values())
    metadata = {
        "publisher": "U.S. Census Bureau (derived by this project)",
        "release": {
            "advertised_history": "Derived from ACS 5-year B17001 and B16001; not published S1701 or C16001 values",
            "dataset_version": None,
            "observed_history": "Poverty % 2010-2011 and limited-English total 2009-2015, after exact overlap checks",
            "publisher_release_label": None,
            "publisher_updated_at": None,
            "release_date": None,
            "revision_status": "unknown",
        },
        "measurement_periods": [period(first) | {"label": f"Five-year windows ending {first}-{last}", "end_date": f"{last}-12-31"}],
        "governance": GOVERNANCE,
    }
    receipt = base_receipt(source, metadata, f"Census_Derived__{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}__{derived_id(plan)[:32]}", validator)
    receipt["acquisition"].update(
        transport_mode="api_response",
        requested_url="https://api.census.gov/data/",
        resolved_url=None,
        http_status=None,
        tool_name="census_acs_detailed_derivation",
        tool_version="1.0.0",
    )
    receipt["artifacts"] = [artifact(snapshot / name, snapshot, role, i, False) for i, (name, _, role) in enumerate(items, 1)]
    receipt["lineage"]["parent_snapshot_ids"] = sorted(set(parents))
    receipt["quality_profile"].update(
        parse_status="passed_with_warnings",
        checks_passed=["overlap_poverty_matches_s1701", "overlap_english_matches_c16001", "labels_selected_from_dictionaries"],
        warnings=[
            "Derived values, not published tables. " + " ".join(plan["definitions"].values()),
            "Margins of error are approximate (Census proportion formula).",
            "Uninsured (C179) cannot be filled for 2010-2011; no 5-year insurance table exists before 2012.",
        ],
    )
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "mode": "acs_derived",
            "route_id": "ACS:derived:B17001_B16001",
            "scope": "Derived C175 poverty 2010-2011 and C183.02 limited English 2009-2015",
            "plan_sha256": canonical_hash(plan),
            "registry_sha256": canonical_hash(registry),
            "schema_sha256": canonical_hash(validator.schema),
            "input_sha256": {k: contract.digest(v) for k, v in sorted(inputs.items())},
            "code_sha256": contract.require_code(),
            "model_eligible": False,
        }
    )
    return finish_receipt(receipt, snapshot, source)


def verify_local(path: Path) -> dict:
    """Verify one capture or the derived snapshot without network or mutation."""
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
        "Census detailed storage route differs",
    )
    entries = reconciliation["objects"]
    roles = {a["storage_path"]: a["role"] for a in receipt["artifacts"]} | {"receipt.json": "capture_receipt"}
    require(len(entries) == len(roles) and {e["storage_path"] for e in entries} == set(roles), "Census detailed storage artifact set differs")

    def check_object(record: dict, local: Path, role: str) -> None:
        sha, size = fingerprint(local)
        prefix = object_prefix(route, "acs", role, receipt["release"]["release_date"] or receipt["snapshot_id"], receipt["snapshot_id"], False)
        require(record["key"] == f"{prefix}/{sha}/{local.name}" and record["bucket"] == settings["data_bucket_name"], "Census detailed storage key differs")
        require(record["sha256"] == sha and record["byte_count"] == size, "Census detailed stored bytes differ")
        require(isinstance(record["version_id"], str) and record["version_id"] not in {"", "null"}, "Census detailed storage version missing")
        require(record["verification"] == "version_get_sha256_and_length_match", "Census detailed storage readback missing")

    for entry in entries:
        check_object(entry["object"], root / entry["storage_path"], roles[entry["storage_path"]])
    manifests = reconciliation["manifests"]
    require(len(manifests) == 1 and manifests[0]["dataset_id"] == "acs", "Census detailed storage manifest set differs")
    manifest_path = root / "s3_collections/acs/manifest.json"
    manifest = read_json(manifest_path)
    require(manifest["objects"] == entries and manifest["snapshot_id"] == receipt["snapshot_id"], "Census detailed manifest differs")
    require(manifest["model_eligible"] is False and manifest["snapshot_status"] == "acquired_unvalidated", "Census detailed manifest hold differs")
    check_object(manifests[0]["object"], manifest_path, "capture_receipt")


def store(plan: dict, identity: str, receipt_path: Path, upload: bool, client: AwsCli | None, outputs: dict) -> dict:
    """Store one verified snapshot once; a completed one only re-verifies."""
    receipt = verify_local(receipt_path)
    done_path = receipt_path.parent.parent / "completed.json"
    if done_path.exists():
        done = read_json(done_path)
        require(done["receipt_sha256"] == fingerprint(receipt_path)[0] and done["id"] == identity, "Census detailed completion receipt differs")
        reconciliation = receipt_path.parent / "s3_collections_reconciliation.json"
        require(done["reconciliation_sha256"] == fingerprint(reconciliation)[0], "Census detailed completion storage evidence differs")
        require(
            done["status"] == "stored" and done["model_eligible"] is False and plan_matches(plan, done["plan_sha256"]),
            "Census detailed completion hold differs",
        )
        settings = client.configuration if client is not None else load_configuration(REPO_ROOT / ".env")[0]
        verify_storage(receipt_path, receipt, settings)
        return done
    result = {
        "id": identity,
        "rows": receipt["schema_profile"]["row_count"],
        "status": "capture_ready",
        "model_eligible": False,
        "plan_sha256": canonical_hash(plan),
    }
    result |= {"receipt_path": str(receipt_path), "receipt_sha256": fingerprint(receipt_path)[0]}
    if upload:
        contract.require_code()
        if client is None:
            raise ValueError("Census detailed storage client missing")
        stored = upload_snapshot(receipt_path, [], client, outputs, load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator())
        verify_storage(receipt_path, receipt, client.configuration)
        result.update(status="stored", reconciliation_sha256=fingerprint(Path(stored["reconciliation_path"]))[0])
        write_once(done_path, encoded_json(result))
    return result


def execute(plan: dict, batch: dict, root: Path, allow_network: bool, upload: bool, client: AwsCli | None, outputs: dict, request: Any = None) -> dict:
    """Capture and store one table-year; a valid completion performs no writes."""
    require(canonical_hash(plan) == canonical_hash(contract.load_plan()) and batch in plan["batches"], "Census detailed selected plan or batch differs")
    receipt_path = root / "batches" / batch["id"] / "capture/receipt.json"
    if not receipt_path.exists():
        contract.require_code()
        envelope = fetch(plan, batch, root, allow_network, client, request)
        receipt_path = make_receipt(plan, batch, envelope, root)
    return store(plan, batch["id"], receipt_path, upload, client, outputs) | {"table": batch["table"], "year": batch["year"]}


def execute_derived(plan: dict, root: Path, upload: bool, client: AwsCli | None, outputs: dict) -> dict:
    """Build and store the derived snapshot once every input capture exists and every check passes."""
    require(canonical_hash(plan) == canonical_hash(contract.load_plan()), "Census detailed selected plan differs")
    # A derived snapshot made under an earlier plan keeps that plan's identity (failure mode S2).
    identities = [canonical_hash({"derived": recorded}) for recorded in plan_hashes(plan)]
    identity = next((item for item in identities if (root / "batches" / item / "capture/receipt.json").exists()), identities[0])
    receipt_path = root / "batches" / identity / "capture/receipt.json"
    if not receipt_path.exists():
        contract.require_code()
        receipt_path = make_derived(plan, root)
    return store(plan, identity, receipt_path, upload, client, outputs) | {"table": "derived", "year": None}


def run_all(
    plan: dict, root: Path, allow_network: bool, upload: bool, client: AwsCli | None, outputs: dict, request: Any = None
) -> tuple[list[dict], list[dict]]:
    """Every table-year independently, then the derived snapshot if nothing was held."""
    results, held = [], []
    with collection_lock(root):
        for batch in plan["batches"]:
            try:
                results.append(execute(plan, batch, root, allow_network, upload, client, outputs, request))
            except (ValueError, OSError, KeyError, TypeError) as error:
                # Errors can originate at credential-bearing boundaries; only fixed public text is kept.
                reason = str(error) if isinstance(error, ValueError) and str(error).startswith(("Census", "Duplicate", "Source is")) else type(error).__name__
                held.append({"table": batch["table"], "year": batch["year"], "reason": reason})
        if not held:
            try:
                results.append(execute_derived(plan, root, upload, client, outputs))
            except (ValueError, OSError, KeyError) as error:
                reason = str(error) if isinstance(error, ValueError) and str(error).startswith(("Census", "Source is")) else type(error).__name__
                held.append({"table": "derived", "year": None, "reason": reason})
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
