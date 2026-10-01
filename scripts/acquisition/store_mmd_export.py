"""Validate a browser-saved MMD CSV and reuse the existing immutable S3 workflow."""

import argparse
import csv
import json
import plistlib
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from scripts.acquisition.capture import base_receipt, receipt_validator, validate_receipt
from scripts.acquisition.cli_tools import executable, run_argv
from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, upload_snapshot, write_once
from scripts.acquisition.source_registry import canonical_hash, load_registry, read_json, require
from scripts.infrastructure.render_project_config import REPO_ROOT, load_configuration


def main() -> None:
    """Check exact caller-observed selections, preserve raw bytes and verify S3 storage."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--download", type=Path, required=True)
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--condition", required=True)
    parser.add_argument("--measure-id", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--domain", required=True)
    parser.add_argument("--geography", choices=["County", "State/Territory"], required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    require(2012 <= args.year <= 2023, "Year outside approved history.")
    registry = load_registry()
    source = next(s for s in registry["sources"] if s["source_id"] == "MMD")
    template = read_json(args.template)
    validator = receipt_validator()
    validate_receipt(template, validator, args.template.parent)
    require(template["source"]["source_record_id"] == "MMD", "Wrong template source.")
    require(args.measure_id in {m["id"] for m in registry["measure_controls"]}, "Condition is not in locked base registry.")
    require(args.download.is_file() and not args.download.is_symlink(), "Download missing or symlinked.")
    digest, size = fingerprint(args.download)
    require(0 < size <= 16 * 1024**2, "Download exceeds bounds.")
    metadata = run_argv([executable("xattr"), "-px", "com.apple.metadata:kMDItemWhereFroms", str(args.download)], check=True, capture_output=True).stdout
    origins = plistlib.loads(bytes.fromhex(metadata))
    require(bool(origins) and all(urlsplit(u).scheme == "https" and urlsplit(u).hostname == "data.cms.gov" for u in origins), "Non-CMS origin.")
    selections = dict(template["acquisition"]["export_selections"])
    selections.update(year=str(args.year), condition=args.condition, domain=args.domain, geography=args.geography)
    require(len(selections) == 13, "Expected all 13 approved selections.")
    with args.download.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        headers = reader.fieldnames
        require(headers == template["schema_profile"]["native_headers"], "Export headers changed; review required.")
        identifiers: set[str] = set()
        for row in reader:
            require(None not in row and None not in row.values(), "Malformed CSV row.")
            require(all(row[k] == v for k, v in selections.items()), "Export differs from approved selections.")
            require(bool(row["fips"]) and row["fips"] not in identifiers, "Blank or duplicate native FIPS.")
            identifiers.add(row["fips"])
    require(bool(identifiers), "Empty export.")
    root = Path("data/historical_acquisition/mmd_browser_history") / digest
    scope = f"{args.measure_id} {args.year} FFS {args.geography} prevalence; exact browser selections; definition and universe review pending."
    plan = read_json(args.template.parents[3] / "plan.json")
    plan.update(expected_sha256=digest, file_name=args.download.name, export_selections=selections, scope=scope)
    write_once(root / "plan.json", encoded_json(plan))
    run_id = "MMD__" + datetime.fromtimestamp(args.download.stat().st_mtime, UTC).strftime("%Y%m%dT%H%M%SZ") + "__" + digest[:32]
    snapshot = root / "captures" / "MMD" / run_id
    raw = snapshot / "raw" / args.download.name
    write_once(raw, args.download.read_bytes())
    require(fingerprint(raw) == (digest, size) == fingerprint(args.download), "Source bytes changed during capture.")
    receipt = base_receipt(source, plan, run_id, validator)
    receipt["acquisition"].update(
        transport_mode="web_export",
        request_method="browser_export",
        tool_name="publisher_browser_export",
        retrieved_at_utc=datetime.fromtimestamp(args.download.stat().st_mtime, UTC).isoformat(),
    )
    artifact = dict(template["artifacts"][0])
    artifact.update(
        byte_count=size, sha256=digest, original_file_name=args.download.name, stored_file_name=args.download.name, storage_path="raw/" + args.download.name
    )
    receipt["artifacts"] = [artifact]
    receipt["schema_profile"] = dict(template["schema_profile"])
    receipt["schema_profile"].update(row_count=len(identifiers), parsed_row_count=len(identifiers))
    receipt["quality_profile"] = dict(template["quality_profile"])
    receipt["quality_profile"]["warnings"] = [
        "Capture integrity only; retain all source and measure holds, suppression and native identifiers. No model approval.",
        "Browser-saved public aggregate CSV; retrieval timestamp is local file mtime; HTTP completion time/status unavailable.",
        "County/state universe and year-specific definitions remain unreviewed; raw bytes are unchanged.",
    ]
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "route_id": plan["route_id"],
            "mode": "file",
            "scope": scope,
            "requests": [],
            "expected_sha256": digest,
            "registry_sha256": canonical_hash(registry),
            "schema_sha256": canonical_hash(validator.schema),
            "browser_origin": origins,
            "measure_id": args.measure_id,
        }
    )
    validate_receipt(receipt, validator, snapshot)
    path = snapshot / "receipt.json"
    write_once(path, encoded_json(receipt))
    result = {"receipt_path": str(path), "sha256": digest, "bytes": size, "rows": len(identifiers), "model_eligible": False}
    write_once(root / "capture_ready.json", encoded_json(result))
    if args.execute:
        settings, _ = load_configuration(REPO_ROOT / ".env")
        outputs = json.loads(run_argv([executable("terraform"), "-chdir=infra", "output", "-json"], check=True, capture_output=True, text=True).stdout)
        stored = upload_snapshot(path, [], AwsCli(settings), outputs, registry, validator)
        completion = {
            "source_id": "MMD",
            "measure_id": args.measure_id,
            "year": args.year,
            "scope": scope,
            "reconciliation_path": stored["reconciliation_path"],
            "model_eligible": False,
        }
        write_once(root / "completed.json", encoded_json(completion))
        result.update(completion)
    sys.stdout.write(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
