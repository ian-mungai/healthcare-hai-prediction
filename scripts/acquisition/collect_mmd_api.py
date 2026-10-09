"""Acquire approved MMD condition-years and reuse immutable, verified S3 storage."""

import argparse
import http.client
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from scripts.acquisition import redownload_controls as controls
from scripts.acquisition.capture import base_receipt, receipt_validator, validate_receipt
from scripts.acquisition.cli_tools import executable, run_argv
from scripts.acquisition.data_paths import current
from scripts.acquisition.mmd_api_contract import (
    FIELDS,
    PLAN_PATH,
    condition_for,
    current_code_hashes,
    digest,
    plan_for,
    reconstruct,
    request_parameters,
    reviewed_code_versions,
    selections_for,
    url_for,
    validate_rows,
    validate_transport,
    verify_capture,
)
from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, upload_snapshot, write_once
from scripts.acquisition.source_registry import canonical_hash, load_registry, read_json, require
from scripts.infrastructure.render_project_config import REPO_ROOT, load_configuration

LOGGER = logging.getLogger(__name__)
ROOT = PLAN_PATH.parent
TEMPLATE = Path("data/datasets/historical_acquisition/mmd_browser_history/6c65677929ca2363c5b0736eb0b9ebf9a45d2f00648225dc5ad60fc2859ba94e/plan.json")


def fetch(root: Path, name: str, url: str, allow_network: bool) -> tuple[bytes, dict]:
    """Capture a bounded CMS response, preserving completed requests on retry."""
    body_path, meta_path = root / f"{name}.json", root / f"{name}_metadata.json"
    if meta_path.exists():
        meta = read_json(meta_path)
        body = body_path.read_bytes()
        require(meta["url"] == url and meta["sha256"] == digest(body) and meta["bytes"] == len(body), "Saved transport differs")
        return body, meta
    require(allow_network, "Missing response; --fetch is required")
    require(not body_path.exists(), "Unreceipted response requires provenance review")
    parsed = urlsplit(url)
    require(parsed.scheme == "https" and parsed.netloc == "data.cms.gov" and parsed.path == "/data-api/v1/mmd-tool/", "Unapproved API endpoint")
    connection = http.client.HTTPSConnection("data.cms.gov", timeout=25)
    try:
        controls.begin(name)
        connection.request("GET", parsed.path + "?" + parsed.query, headers={"Accept": "application/json"})
        response = connection.getresponse()
        controls.response_status(response)
        content_type = response.getheader("Content-Type", "")
        require(response.status == 200 and "application/json" in content_type, f"MMD response status/type failed: {response.status}")
        body = controls.read(response, 16 * 1024**2 + 1)
        require(0 < len(body) <= 16 * 1024**2 and isinstance(json.loads(body), list), "Invalid or oversized MMD response")
        meta = {
            "url": url,
            "status": response.status,
            "content_type": content_type,
            "retrieved_at_utc": datetime.now(UTC).isoformat(),
            "sha256": digest(body),
            "bytes": len(body),
        }
        write_once(body_path, body)
        write_once(meta_path, encoded_json(meta))
        return body, meta
    finally:
        connection.close()


def artifact(path: Path, snapshot: Path, role: str, index: int, original: bool) -> dict:
    """Describe raw and derived files honestly, without transforming original bytes."""
    sha, size = fingerprint(path)
    return {
        "artifact_id": f"artifact_{index:04}",
        "role": role,
        "original_file_name": path.name if original else None,
        "stored_file_name": path.name,
        "storage_path": path.relative_to(snapshot).as_posix(),
        "media_type": "text/csv" if path.suffix == ".csv" else "application/json",
        "compression": None,
        "byte_count": size,
        "sha256": sha,
        "hash_scope": "complete_response" if original else "complete_file",
        "completeness": "complete",
        "partial_reason": None,
        "original_unchanged": original,
        "archive_members": [],
    }


def require_reviewed_code() -> dict[str, str]:
    """Stop unreviewed collector code before it requests, writes or uploads anything."""
    code_hashes = current_code_hashes()
    require(code_hashes in reviewed_code_versions(), "Unreviewed MMD collector version; pass the E2E and list it before capturing")
    return code_hashes


def reuse_capture(branch: Path) -> Path:
    """Return a year's verified existing capture; a rerun never writes a second snapshot."""
    receipt_path = Path(current(read_json(branch / "capture_ready.json")["receipt_path"]))
    receipt = read_json(receipt_path)
    validate_receipt(receipt, receipt_validator(), receipt_path.parent)
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    source = next(s for s in load_registry(expected_sha256=lineage["registry_sha256"])["sources"] if s["source_id"] == "MMD")
    verify_capture(receipt, source, json.loads(receipt["lineage"]["extraction_or_query"]), receipt_path.parent, False)
    return receipt_path


def capture(measure_id: str, year: int, allow_network: bool, root: Path | None = None) -> Path:
    """Capture one offered branch and recheck every storage condition before upload; ``root`` selects a fresh state root."""
    plan_sha256, config = plan_for(measure_id)
    condition = condition_for(config, measure_id, year)
    parameters = request_parameters(config, condition, year)
    branch = (ROOT if root is None else root) / measure_id.replace(".", "_").lower() / str(year)
    # A finished year is only re-verified, never recaptured, so it needs no new evidence and no gate.
    if (branch / "capture_ready.json").exists():
        return reuse_capture(branch)
    # Scope errors surface first; unreviewed code then stops before any request or file write.
    code_hashes = require_reviewed_code()
    raw, main = fetch(branch / "transport", "main", url_for(parameters), allow_network)
    rows, stats = validate_rows(raw, parameters)
    probes = []
    for offset in (0, 2, len(rows)):
        body, metadata = fetch(branch / "transport", f"probe_{offset}", url_for(dict(parameters, _size="2", _offset=str(offset))), allow_network)
        probes.append(metadata | {"body_utf8": body.decode()})
    evidence = {"main": main, "probes": probes, "statistics": stats, "model_eligible": False}
    validate_transport(raw, evidence, parameters)
    derived, headers = reconstruct(config, condition, year, rows)
    timestamp = datetime.fromisoformat(main["retrieved_at_utc"]).strftime("%Y%m%dT%H%M%SZ")
    run_hash = digest(encoded_json({"raw_sha256": digest(raw), "code_sha256": code_hashes}))
    run_id = f"MMD_API_{measure_id.replace('.', '_')}_{year}__{timestamp}__{run_hash[:32]}"
    snapshot = branch / "captures" / run_id
    file_stem = f"mmd_ffs_{'county' if condition['geography'] == 'c' else 'state'}_{measure_id.lower().replace('.', '_')}_prevalence_{year}"
    raw_path, csv_path = snapshot / "raw" / (file_stem + ".json"), snapshot / "derived" / (file_stem + ".csv")
    proof_path, reference_path = snapshot / "evidence" / "export_evidence.json", snapshot / "references" / "cms_reference_bundle.json"
    bundle = {
        name: {"url": ref["url"], "sha256": ref["sha256"], "body_hex": Path(current(ref["path"])).read_bytes().hex()}
        for name, ref in config["references"].items()
    }
    for path, content in ((raw_path, raw), (csv_path, derived), (proof_path, encoded_json(evidence)), (reference_path, encoded_json(bundle))):
        write_once(path, content)
    registry, validator = load_registry(expected_sha256=config["registry_sha256"]), receipt_validator()
    source = next(s for s in registry["sources"] if s["source_id"] == "MMD")
    plan = read_json(TEMPLATE)
    scope = f"{measure_id} {year} FFS {condition['geography']} prevalence; API raw plus derived CSV; all clinical/geographic/model holds retained"
    plan.update(mode="mmd_api", route_id="MMD:api:20260925", scope=scope, export_selections=selections_for(config, condition, year))
    plan["measurement_periods"] = [
        {
            "label": f"Selected calendar year {year}; condition-specific historical claims lookback remains unreviewed",
            "start_date": f"{year}-01-01",
            "end_date": f"{year}-12-31",
            "period_type": "calendar_year",
            "source_basis": "field_observed",
            "target_alignment": "unknown",
        }
    ]
    plan["release"]["observed_history"] = f"Full public API response captured for selected {measure_id} calendar year {year}; publication/revision date unknown"
    receipt = base_receipt(source, plan, run_id, validator)
    receipt["acquisition"].update(
        transport_mode="api_response",
        requested_url=url_for(parameters),
        resolved_url=url_for(parameters),
        retrieved_at_utc=main["retrieved_at_utc"],
        http_status=200,
        request_method="GET",
        request_parameters=parameters,
        tool_name="approved_mmd_api_collector",
        tool_version="1.0.0",
        pagination={
            "required": False,
            "strategy": "Browser size 500000; offset 0/2 probes match and offset row_count is empty",
            "page_count": 1,
            "termination_verified": True,
            "deduplication_keys": ["fips"],
        },
    )
    receipt["artifacts"] = [
        artifact(path, snapshot, role, i, original)
        for i, (path, role, original) in enumerate(
            ((raw_path, "api_page", True), (csv_path, "data", False), (proof_path, "export_receipt", False), (reference_path, "layout", False)),
            1,
        )
    ]
    receipt["schema_profile"].update(
        encoding="utf-8",
        delimiter=",",
        native_headers=headers,
        schema_fingerprint_sha256=canonical_hash(headers),
        row_count=len(rows),
        parsed_row_count=len(rows),
        native_identifier_fields=["fips"],
        native_date_fields=["year"],
    )
    receipt["quality_profile"].update(
        parse_status="passed_with_warnings",
        duplicate_candidate_key_rows=0,
        blank_identifier_rows=0,
        pagination_complete=True,
        checks_passed=["exact_raw_schema_and_dimensions", "unique_native_fips", "actual_offset_and_termination", "derived_csv_consistency"],
        warnings=[
            "Acquisition integrity only. All source/measure definition, geography, suppression and modeling holds remain.",
            "Raw API JSON preserves native FIPS and tokens. CSV is a separately derived publisher-shaped representation, not a browser-saved original.",
            "Published zero can represent suppressed small numerators. Never infer clinical absence or recover hidden counts.",
            "Population is selected by source crosswalk/request and is absent from API rows. Unknown nonnumeric rate tokens stop this collector.",
            "CSV profile describes derived columns; raw API schema is recorded in extraction lineage. Source lookback/release versions remain pending.",
            "Known missing county labels from the same-year AMI export remain blank; native API IDs are intact and geography review stays pending.",
        ],
    )
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "mode": "mmd_api",
            "route_id": "MMD:api:20260925",
            "scope": scope,
            "measure_id": measure_id,
            "year": year,
            "plan_sha256": plan_sha256,
            "registry_sha256": canonical_hash(registry),
            "schema_sha256": canonical_hash(validator.schema),
            "expected_sha256": digest(raw),
            "requests": [main],
            "native_api_fields": sorted(FIELDS),
            "fallback_reason": "User authorized MMD-only API collection after 12-year AMI parity; native browser Save remained blocked",
            "derivation": {
                "input_sha256": digest(raw),
                "output_sha256": digest(derived),
                "description": "Approved labels, CMS lookups, denominator bands and browser CSV formatting; original JSON unchanged",
            },
            "code_sha256": code_hashes,
            "review_decision": condition["review_decision"],
            "model_eligible": False,
        }
    )
    receipt_path = snapshot / "receipt.json"
    if receipt_path.exists():
        receipt = read_json(receipt_path)
    validate_receipt(receipt, validator, snapshot)
    verify_capture(receipt, source, json.loads(receipt["lineage"]["extraction_or_query"]), snapshot, False)
    write_once(receipt_path, encoded_json(receipt))
    write_once(
        branch / "capture_ready.json", encoded_json({"receipt_path": str(receipt_path), "rows": len(rows), "sha256": digest(raw), "model_eligible": False})
    )
    return receipt_path


def execute(measure_id: str, year: int, allow_network: bool, upload: bool, root: Path | None = None) -> dict:
    """Run a complete local or live pilot through the existing storage guard."""
    path = capture(measure_id, year, allow_network, root)
    completed = path.parents[2] / "completed.json"
    if completed.exists():
        done = read_json(completed)
        require(current(done["receipt_path"]) == str(path), "Completed capture differs from the ready capture")
        return done
    receipt = read_json(path)
    result = {"measure_id": measure_id, "year": year, "receipt_path": str(path), "rows": receipt["schema_profile"]["row_count"], "model_eligible": False}
    if upload:
        require_reviewed_code()
        settings, _ = load_configuration(REPO_ROOT / ".env")
        outputs = json.loads(run_argv([executable("terraform"), "-chdir=infra", "output", "-json"], check=True, capture_output=True, text=True).stdout)
        stored = upload_snapshot(path, [], AwsCli(settings), outputs, load_registry(), receipt_validator())
        result.update(reconciliation_path=stored["reconciliation_path"], status="stored")
        write_once(path.parents[2] / "completed.json", encoded_json(result))
    else:
        result["status"] = "capture_ready"
    return result


def main() -> None:
    """Use --fetch for permitted reads and --execute for authorized verified storage."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--measure-id", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--validate-receipt", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.validate_receipt:
        receipt = read_json(args.validate_receipt)
        require(json.loads(receipt["lineage"]["extraction_or_query"])["measure_id"] == args.measure_id, "Validation measure differs")
        require(json.loads(receipt["lineage"]["extraction_or_query"])["year"] == args.year, "Validation year differs")
        validate_receipt(receipt, receipt_validator(), args.validate_receipt.parent)
        source = next(s for s in load_registry()["sources"] if s["source_id"] == "MMD")
        verify_capture(receipt, source, json.loads(receipt["lineage"]["extraction_or_query"]), args.validate_receipt.parent, False)
        result = {"status": "valid", "model_eligible": False}
    else:
        result = execute(args.measure_id, args.year, args.fetch, args.execute)
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
