"""Local immutable captures; S3 upload and model eligibility are separate steps."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, cast

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry

from scripts.acquisition.api_fallbacks import api_specification, check_bls, check_il_page
from scripts.acquisition.source_registry import LOCK_PATH, REGISTRY_PATH, canonical_hash, load_registry, read_json
from scripts.acquisition.transport import CaptureError, Download, Limits, download, validate_request

SCHEMA_PATH = REGISTRY_PATH.with_name("snapshot_contract.schema.json")


def receipt_validator(schema_path: Path = SCHEMA_PATH) -> Draft202012Validator:
    """Load and validate the receipt schema, returning its Draft 2020-12 validator."""
    schema = read_json(schema_path)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker(), registry=Registry())


def validate_receipt(receipt: dict, validator: Draft202012Validator, snapshot_root: Path | None = None) -> None:
    """Reject inconsistent receipt metadata and, when supplied, verify local artifact bytes."""
    errors = sorted(validator.iter_errors(receipt), key=lambda error: str(list(error.path)))
    if errors:
        raise CaptureError(f"Receipt schema validation failed at {list(errors[0].path)}: {errors[0].validator}")
    states = {
        "acquired_unvalidated": "pending",
        "evidence_only_partial": "evidence_only",
        "validated_raw_snapshot": "accepted_raw_snapshot",
        "rejected": "rejected",
    }
    if receipt["verification"]["status"] != states[receipt["snapshot_status"]]:
        raise CaptureError("Snapshot and verification statuses are inconsistent.")
    if receipt["snapshot_status"] == "validated_raw_snapshot":
        raise CaptureError("Promotion requires a separate post-storage review; capture validation cannot approve it.")
    for period in receipt["measurement_periods"]:
        if period["start_date"] and period["end_date"] and period["start_date"] > period["end_date"]:
            raise CaptureError("Measurement start date follows its end date.")
    identities, paths = set(), set()
    for artifact in receipt["artifacts"]:
        path = PurePosixPath(artifact["storage_path"])
        if path.is_absolute() or ".." in path.parts or "\\" in str(path) or not path.parts:
            raise CaptureError("Artifact paths must stay inside the snapshot directory.")
        if artifact["artifact_id"] in identities or str(path) in paths or path.name != artifact["stored_file_name"]:
            raise CaptureError("Artifact identities and paths must be unique and consistent.")
        identities.add(artifact["artifact_id"])
        paths.add(str(path))
        if receipt["snapshot_status"] in {"acquired_unvalidated", "validated_raw_snapshot"} and artifact["completeness"] != "complete":
            raise CaptureError("Incomplete artifacts cannot be promoted to complete captures.")
        if snapshot_root is not None:
            local_path = snapshot_root / str(path)
            if not local_path.resolve().is_relative_to(snapshot_root.resolve()) or not local_path.is_file():
                raise CaptureError("Artifact is missing or resolves outside its snapshot.")
            with local_path.open("rb") as handle:
                digest = hashlib.file_digest(handle, "sha256").hexdigest()
            if digest != artifact["sha256"] or local_path.stat().st_size != artifact["byte_count"]:
                raise CaptureError("Artifact hash or byte count differs from its receipt.")


def base_receipt(source: dict, plan: dict, snapshot_id: str, validator: Draft202012Validator) -> dict:
    """Return initial receipt metadata with the source's unresolved checks preserved."""
    # receipt_validator loads an object through read_json, not a boolean JSON Schema.
    schema = cast(dict[str, Any], validator.schema)
    return {
        "contract_version": "1.0.0",
        "project": schema["properties"]["project"]["const"],
        "snapshot_id": snapshot_id,
        "snapshot_status": "acquired_unvalidated",
        "source": {
            "source_record_id": source["source_id"],
            "title": source["title"],
            "publisher": plan.get("publisher"),
            "official_landing_url": source["official_landing_url"],
            "original_source_family_ids": source["original_source_family_ids"],
            "linked_measure_ids": source["linked_measure_ids"],
        },
        "acquisition": {
            "preferred_route": source["preferred_route"],
            "transport_mode": "download",
            "requested_url": source["official_landing_url"],
            "resolved_url": None,
            "retrieved_at_utc": datetime.now(UTC).isoformat(),
            "http_status": None,
            "request_method": "GET",
            "request_parameters": {},
            "export_selections": plan.get("export_selections", {}),
            "tool_name": "local_source_capture",
            "tool_version": "1.0.0",
            "pagination": {"required": False, "strategy": None, "page_count": None, "termination_verified": None, "deduplication_keys": []},
        },
        "release": plan["release"],
        "measurement_periods": plan["measurement_periods"],
        "artifacts": [],
        "schema_profile": {
            "encoding": None,
            "delimiter": None,
            "native_headers": [],
            "schema_fingerprint_sha256": None,
            "row_count": None,
            "parsed_row_count": None,
            "expected_grain": source.get("validation", {}).get("expected_native_grain"),
            "native_identifier_fields": [],
            "candidate_join_keys": [],
            "native_date_fields": [],
            "native_missing_tokens": [],
            "native_footnote_fields": [],
        },
        "quality_profile": {
            "parse_status": "not_attempted",
            "duplicate_candidate_key_rows": None,
            "blank_identifier_rows": None,
            "suppressed_or_footnoted_rows": None,
            "pagination_complete": None,
            "checks_passed": [],
            "checks_failed": [],
            "warnings": ["Capture validation only; dataset schemas, clinical meaning, history and model eligibility remain unreviewed."],
        },
        "governance": plan["governance"],
        "lineage": {"parent_snapshot_ids": [], "extraction_or_query": None, "raw_transformations": [], "supersedes_snapshot_id": None},
        "verification": {
            "status": "pending",
            "verified_at_utc": None,
            "verified_by": None,
            "contract_validation": "passed",
            "notes": [
                "Unknown metadata remains null. No period was inferred from a filename.",
                "HTTP completeness does not establish historical dataset completeness or permission for model use.",
            ],
        },
    }


def artifact_record(result: Download, root: Path, role: str, index: int) -> dict:
    """Return receipt metadata for a captured payload relative to its snapshot root."""
    return {
        "artifact_id": f"artifact_{index:04}",
        "role": role,
        "original_file_name": result.original_file_name,
        "stored_file_name": result.path.name,
        "storage_path": result.path.relative_to(root).as_posix(),
        "media_type": result.media_type,
        "compression": None,
        "byte_count": result.byte_count,
        "sha256": result.sha256,
        "hash_scope": "partial_response" if result.partial else "complete_response",
        "completeness": "complete" if result.complete else ("partial" if result.partial else "unknown"),
        "partial_reason": result.failure,
        "original_unchanged": True,
        "archive_members": [],
    }


def prepare_capture(plan: dict, sources: dict, validator: Draft202012Validator) -> tuple:
    """Validate the plan and source permissions; return the approved request and initial receipt."""
    fields = {
        "source_id",
        "mode",
        "route_id",
        "expected_format",
        "file_name",
        "role",
        "publisher",
        "scope",
        "release",
        "measurement_periods",
        "governance",
        "export_selections",
        "expected_sha256",
        "fallback_reason",
        "series_ids",
        "start_year",
        "end_year",
        "expected_periods",
        "lineage_bindings",
    }
    if set(plan) - fields or not isinstance(plan.get("scope"), str) or not plan["scope"].strip():
        raise CaptureError("Capture plans require an explicit scope and cannot contain unrecognized options.")
    candidates = {source["source_id"]: source for source in sources["sources"]}
    source = candidates.get(plan.get("source_id"))
    # A held source may supply a reference document only with a release binding; storage rechecks the binding.
    held_reference = bool(plan.get("lineage_bindings")) and plan.get("mode", "file") == "file" and plan.get("role", "data") not in {"data", "api_page"}
    if source is None or (source["preferred_route"] == "access_hold" and not held_reference):
        raise CaptureError("Source is unknown or remains on access hold.")
    if source["preferred_route"] == "access_hold":
        # The receipt records what was collected: documentation only; the registry hold itself is unchanged.
        source = source | {"preferred_route": "documentation_only"}
    governance = plan["governance"]
    if governance.get("access_class") != "public" or governance.get("credentials_required") is not False:
        raise CaptureError("This downloader supports reviewed public unauthenticated routes only.")
    if governance.get("contains_phi") is not False or governance.get("contains_pii") is not False:
        raise CaptureError("Record the public aggregate-data privacy assessment before capture.")
    mode = plan.get("mode", "file")
    if mode not in {"file", "bls_api", "il_directory"}:
        raise CaptureError("Unapproved acquisition mode.")
    role = plan.get("role", "data") if mode == "file" else "api_page"
    if source["preferred_route"] == "documentation_only" and role not in {"dictionary", "methodology", "layout"}:
        raise CaptureError("Reference-only sources cannot supply numeric observations.")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", source["source_id"]):
        raise CaptureError("Invalid source identifier for local storage.")
    body = None
    route = {}
    if mode == "file":
        routes = [route for route in source["file_routes"] if route["route_id"] == plan.get("route_id")]
        if len(routes) != 1:
            raise CaptureError("Select exactly one approved source file route.")
        route = routes[0]
        if route.get("route_type") == "file_index" or route.get("status") in {"access_failed", "HTTP_403_file_route_unverified"}:
            raise CaptureError("This route is a discovery index or failed access evidence, not a downloadable artifact.")
        url, expected_format = route["url"], plan["expected_format"]
        if route.get("route_type") == "permitted_export" and not plan.get("export_selections"):
            raise CaptureError("Record permitted export selections before capture.")
    else:
        url, body = api_specification(source, plan)
        expected_format = "json"
    expected_hash = plan.get("expected_sha256")
    if expected_hash is not None and (not isinstance(expected_hash, str) or not re.fullmatch(r"[a-f0-9]{64}", expected_hash)):
        raise CaptureError("Expected SHA-256 must contain 64 lowercase hexadecimal digits.")
    snapshot_id = f"{source['source_id']}__{datetime.now(UTC):%Y%m%dT%H%M%SZ}__{uuid.uuid4().hex}"
    validate_request(url, plan["file_name"] if mode == "file" else "page_0001.json", expected_format, role)
    receipt = base_receipt(source, plan, snapshot_id, validator)
    # Validate publisher metadata before network access or creating a snapshot directory.
    placeholder = Download(Path("payload.bin"), url, None, receipt["acquisition"]["retrieved_at_utc"], None, 0, "0" * 64, None, True, None, False, 1)
    receipt["artifacts"] = [artifact_record(placeholder, Path("."), role, 1)]
    validate_receipt(receipt, validator)
    return source, mode, role, route, url, expected_format, body, expected_hash, receipt


def capture(plan: dict, output_root: Path, sources: dict, validator: Draft202012Validator, limits: Limits | None = None, opener: Any = None) -> Path:
    """Capture bounded source bytes and return the validated local receipt path."""
    limits = limits or Limits()
    source, mode, role, route, url, expected_format, body, expected_hash, receipt = prepare_capture(plan, sources, validator)
    from scripts.acquisition.source_registry import require_collection_scope

    require_collection_scope(source["source_id"])
    snapshot_id = receipt["snapshot_id"]
    snapshot_root = output_root / source["source_id"] / snapshot_id
    snapshot_root.mkdir(parents=True, exist_ok=False)
    receipt["artifacts"] = []
    downloads, page_log, baseline, failure = [], [], None, None
    seen: set[str] = set()
    initial_url = url
    for page in range(1, limits.max_pages + 1):
        file_name = plan["file_name"] if mode == "file" else f"page_{page:04}.json"
        result = download(url, snapshot_root / "audit" / f"request_{page:04}", file_name, expected_format, role, limits, body, opener)
        page_log.append(
            {
                "requested_url": url,
                "request_method": "GET" if body is None else "POST",
                "request_body": body,
                "retrieved_at_utc": result.retrieved_at_utc,
                "resolved_url": result.resolved_url,
                "http_status": result.http_status,
                "sha256": result.sha256,
                "byte_count": result.byte_count,
                "attempts": result.attempt,
                "resolved_url_redacted": result.resolved_url_redacted,
            }
        )
        if result.resolved_url_redacted:
            receipt["verification"]["notes"].append("Exact publisher signed redirect verified; all query parameters omitted from persisted metadata.")
        following = None
        if result.complete and mode != "file":
            try:
                payload = read_json(result.path)
                if mode == "bls_api":
                    failure = check_bls(payload, plan)
                else:
                    baseline, following = check_il_page(payload, page, baseline, seen)
                    if following and page == limits.max_pages:
                        failure = "pagination_limit_before_termination"
            except (ValueError, TypeError, KeyError) as error:
                failure = f"api_validation_{type(error).__name__}"
            if failure:
                result = replace(result, complete=False, partial=True, failure=failure)
        if result.complete and expected_hash is not None and mode == "file" and result.sha256 != expected_hash:
            result = replace(result, complete=False, failure="expected_hash_mismatch")
        if result.complete:
            raw = snapshot_root / "raw" / file_name
            raw.parent.mkdir(exist_ok=True)
            os.link(result.path, raw)
            result = replace(result, path=raw)
        downloads.append(result)
        artifact = artifact_record(result, snapshot_root, role, page)
        if mode == "file" and result.complete:
            artifact["hash_scope"] = "complete_file"
        if expected_format in {"zip", "xlsx", "docx", "gzip"}:
            artifact["compression"] = "gzip" if expected_format == "gzip" else "zip"
        receipt["artifacts"].append(artifact)
        if not result.complete or following is None:
            break
        url = following
    final = downloads[-1]
    failure = final.failure
    if failure:
        receipt["snapshot_status"] = "evidence_only_partial" if final.partial else "rejected"
        receipt["verification"]["status"] = "evidence_only" if final.partial else "rejected"
        receipt["quality_profile"]["checks_failed"].append(failure)
    receipt["acquisition"].update(
        {
            "requested_url": initial_url,
            "resolved_url": downloads[0].resolved_url,
            "retrieved_at_utc": downloads[0].retrieved_at_utc,
            "http_status": downloads[0].http_status,
            "request_method": "POST" if body is not None else "GET",
            "request_parameters": body or {},
        }
    )
    if mode != "file":
        complete = all(item.complete for item in downloads)
        receipt["acquisition"]["transport_mode"] = "api_response"
        strategy = "Explicit BLS series/year batch" if mode == "bls_api" else "Illinois numbered pages"
        receipt["acquisition"]["pagination"] = {
            "required": True,
            "strategy": strategy,
            "page_count": len(downloads),
            "termination_verified": complete,
            "deduplication_keys": ["seriesID", "year", "period"] if mode == "bls_api" else ["entity_id"],
        }
        receipt["quality_profile"]["pagination_complete"] = complete
        receipt["verification"]["notes"].append(plan["fallback_reason"])
    elif source["preferred_route"] == "documentation_only":
        receipt["acquisition"]["transport_mode"] = "reference_document"
    elif route.get("route_type") == "permitted_export":
        receipt["acquisition"]["transport_mode"] = "web_export"
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "route_id": plan.get("route_id"),
            "mode": mode,
            "scope": plan.get("scope"),
            "requests": page_log,
            "expected_periods": plan.get("expected_periods"),
            "expected_sha256": expected_hash,
            "fallback_reason": plan.get("fallback_reason"),
            "registry_sha256": canonical_hash(sources),
            "schema_sha256": canonical_hash(validator.schema),
            **plan.get("lineage_bindings", {}),
        }
    )
    if not failure:
        receipt["quality_profile"]["checks_passed"] = ["complete_http_transport", "expected_container_or_payload", "sha256_recorded"]
    validate_receipt(receipt, validator, snapshot_root)
    receipt_path = snapshot_root / "receipt.json"
    with receipt_path.open("x", encoding="utf-8") as handle:
        json.dump(receipt, handle, indent=2, ensure_ascii=True, allow_nan=False)
        handle.write("\n")
    return receipt_path


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description="Capture an explicitly selected approved source locally. No S3 upload or model eligibility clearance.")
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--output-root", type=Path, default=REGISTRY_PATH.parents[2] / "data" / "datasets" / "snapshots")
    parser.add_argument("--registry", type=Path, default=REGISTRY_PATH)
    parser.add_argument("--lock", type=Path, default=LOCK_PATH)
    parser.add_argument("--schema", type=Path, default=SCHEMA_PATH)
    parser.add_argument("--validate-receipt", type=Path)
    arguments = parser.parse_args()
    try:
        validator = receipt_validator(arguments.schema)
        if arguments.validate_receipt:
            validate_receipt(read_json(arguments.validate_receipt), validator, arguments.validate_receipt.parent)
            sys.stdout.write("Receipt schema and local artifact integrity verified. Model eligibility remains unresolved." + "\n")
        elif arguments.plan:
            receipt_path = capture(read_json(arguments.plan), arguments.output_root, load_registry(arguments.registry, arguments.lock), validator)
            status = read_json(receipt_path)["snapshot_status"]
            sys.stdout.write(str(f"{status}: {receipt_path}") + "\n")
            if status != "acquired_unvalidated":
                raise CaptureError("Capture is evidence-only or rejected; inspect its receipt before retrying.")
        else:
            parser.error("Supply --plan or --validate-receipt.")
    except (ValueError, OSError, KeyError, TypeError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
