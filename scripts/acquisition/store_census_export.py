"""Pin a reviewed Census browser export to its publisher URL and store its contents."""

import argparse
import csv
import json
import plistlib
import re
import sys
import zipfile
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from scripts.acquisition.capture import capture, receipt_validator, validate_receipt
from scripts.acquisition.cli_tools import executable, run_argv
from scripts.acquisition.collection_layout import load_routes, object_prefix
from scripts.acquisition.history_routes import extend_registry, map_captured_archive
from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, preflight, store_file, upload_snapshot, verify_version, write_once
from scripts.acquisition.source_registry import (
    LOCK_PATH,
    RegistryError,
    canonical_hash,
    load_registry,
    read_json,
    registry_version_paths,
    require,
    validate_registry,
)
from scripts.acquisition.transport import Limits
from scripts.infrastructure.render_project_config import REPO_ROOT, load_configuration


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--download", type=Path, required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--years", type=int, nargs="+", required=True)
    args = parser.parse_args()
    metadata = run_argv([executable("xattr"), "-px", "com.apple.metadata:kMDItemWhereFroms", str(args.download)], check=True, capture_output=True).stdout
    origins = plistlib.loads(bytes.fromhex(metadata))
    url = origins[0]
    parts = urlsplit(url)
    require(parts.scheme == "https" and parts.hostname == "data.census.gov", "Export must come from Census.")
    require(parts.path == "/api/access/table/download" and set(parse_qs(parts.query)) == {"download_id"}, "Unexpected export route.")
    require(bool(re.fullmatch(r"(?:[BC]\d{5}|S\d{4}|DP\d{2})", args.table)), "Invalid table ID.")
    digest, size = fingerprint(args.download)
    require(size <= 64 * 1024**2, "Export exceeds the 64 MiB review bound.")
    observed: dict[int, list[str]] = {}
    with zipfile.ZipFile(args.download) as archive:
        require(sum(item.file_size for item in archive.infolist()) <= 1024**3, "Expanded size exceeds review bound.")
        require(len(archive.infolist()) <= 10000 and archive.testzip() is None, "Invalid archive count or CRC.")
        names = archive.namelist()
        require(len(set(names)) == len(names), "Duplicate archive member names.")
        for name in names:
            match = re.fullmatch(r"ACS(?:ST|DT|DP)5Y(\d{4})\." + args.table + r"-(Data\.csv|Column-Metadata\.csv|Table-Notes\.txt)", name)
            if match is None:
                raise RegistryError("Unexpected Census export member.")
            year = int(match[1])
            observed.setdefault(year, []).append(match[2])
        require(set(observed) == set(args.years), "Export vintages differ from selected vintages.")
        for members in observed.values():
            require(set(members) == {"Data.csv", "Column-Metadata.csv", "Table-Notes.txt"}, "Missing data or companion reference.")
    selections = {
        "table": args.table,
        "product": "ACS 5-Year Estimates",
        "vintages": sorted(args.years),
        "geography": "All Counties within United States and Puerto Rico",
        "fields": "all",
        "browser_origin": origins,
        "browser_download_sha256": digest,
    }
    root = Path("data/datasets/historical_acquisition/acs_browser_history") / digest
    write_once(root / "export_selections.json", encoded_json(selections))
    candidate = {
        "source_id": "ACS",
        "url": url,
        "format": "zip",
        "role": "data",
        "route_type": "permitted_export",
        "label": args.table + " selected five-year vintages",
        "expected_sha256": digest,
        "max_bytes": 64 * 1024**2,
        "scope": json.dumps(selections, sort_keys=True),
        "evidence": selections,
    }
    base, base_lock = load_registry(), read_json(LOCK_PATH)
    if (root / "registry.json").exists() or (root / "registry_lock.json").exists():
        saved = read_json(root / "registry.json")
        validate_registry(saved, read_json(root / "registry_lock.json"))
        parent = saved["historical_extension"]["parent_registry_sha256"]
        if canonical_hash(base) != parent:
            paths = registry_version_paths(parent)
            base, base_lock = load_registry(expected_sha256=parent), read_json(paths["lock"])
    registry, lock, jobs = extend_registry(base, base_lock, [candidate], read_json(Path("config/acquisition/planning_rules.json")))
    plan = jobs[0]["plan"]
    plan["export_selections"] = selections
    write_once(root / "registry.json", encoded_json(registry))
    write_once(root / "registry_lock.json", encoded_json(lock))
    write_once(root / "plan.json", encoded_json(plan))
    validator = receipt_validator()
    receipts = sorted(root.glob("captures/*/*/receipt.json"))
    path = next((p for p in receipts if read_json(p)["snapshot_status"] == "acquired_unvalidated"), None)
    if path is None:
        path = capture(plan, root / "captures", registry, validator, Limits(max_bytes=64 * 1024**2, attempts=1))
    receipt = read_json(path)
    require(receipt["snapshot_status"] == "acquired_unvalidated", str(receipt["quality_profile"]["checks_failed"]))
    validate_receipt(receipt, validator, path.parent)
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    require(
        lineage["registry_sha256"] == canonical_hash(registry) and lineage["schema_sha256"] == canonical_hash(validator.schema),
        "Receipt differs from locked registry or contract.",
    )
    require(lineage["route_id"] == plan["route_id"] and lineage["scope"] == plan["scope"] and lineage["mode"] == "file", "Receipt lineage differs.")
    require(
        receipt["source"]["source_record_id"] == "ACS"
        and receipt["acquisition"]["requested_url"] == url
        and receipt["acquisition"]["export_selections"] == selections,
        "Receipt export identity differs.",
    )
    require(
        len(receipt["artifacts"]) == 1 and (receipt["artifacts"][0]["sha256"], receipt["artifacts"][0]["byte_count"]) == (digest, size),
        "Captured archive differs from browser export.",
    )
    mapping = map_captured_archive(path, registry)
    row_counts = {}
    for member in mapping["members"]:
        name = member["member_chain"][-1]
        data = name.endswith("-Data.csv")
        member.update(
            role="data" if data else "reference",
            dataset_id=("acs_" + args.table.lower()) if data else None,
            mapping_basis="Exact Census export member suffix, table identity and selected vintages; clinical and period review pending.",
        )
        if name.endswith("-Column-Metadata.csv"):
            member["reference_kind"] = "dictionary_candidate"
        if data:
            local = next((path.parent / "history_members").rglob(name))
            with local.open(newline="", encoding="utf-8-sig") as handle:
                reader = csv.reader(handle)
                header, labels = next(reader), next(reader)
                require(header[:2] == ["GEO_ID", "NAME"] and labels[0] == "Geography", "Unexpected export headers.")
                identifiers = set()
                for row in reader:
                    require(len(row) == len(header) and bool(re.fullmatch(r"0500000US\d{5}", row[0])), "Invalid row width or non-county geography.")
                    require(row[0] not in identifiers, "Duplicate county identifier.")
                    identifiers.add(row[0])
                require(len(identifiers) > 3000, "County count below sanity floor; investigate export scope.")
                row_counts[name] = len(identifiers)
    bindings = [
        {
            "data": member["member_chain"][-1],
            "dictionary": member["member_chain"][-1].replace("-Data.csv", "-Column-Metadata.csv"),
            "notes": member["member_chain"][-1].replace("-Data.csv", "-Table-Notes.txt"),
            "status": "publisher_bundled_dictionary_candidate_applicability_pending",
        }
        for member in mapping["members"]
        if member["role"] == "data"
    ]
    review = {
        "review_version": 2,
        "selections": selections,
        "county_rows_per_export": row_counts,
        "complete_archive_crc": True,
        "publisher_redownload_matches_browser_sha256": True,
        "vintage_and_member_selection_match": True,
        "county_universe_validation": "pending_vintage_specific_universe_comparison",
        "dictionary_bindings": bindings,
        "model_eligible": False,
        "remaining_checks": [
            "county_universe_completeness",
            "exact_measure_definition_by_vintage",
            "five_year_window_alignment",
            "geography_boundary_changes",
            "MOE_and_suppression_codes",
            "required_field_coverage",
        ],
    }
    review_path = root / "transport_review_v2.json"
    write_once(review_path, encoded_json(review))
    settings, _ = load_configuration(REPO_ROOT / ".env")
    outputs = json.loads(run_argv([executable("terraform"), "-chdir=infra", "output", "-json"], check=True, capture_output=True, text=True).stdout)
    client = AwsCli(settings)
    completed_path = root / "completed.json"
    if completed_path.exists():
        result = read_json(completed_path)
        reconciliation_path = Path(result["reconciliation_path"])
        require(reconciliation_path.resolve() == (path.parent / "s3_collections_reconciliation.json").resolve(), "Completion points outside its capture.")
        require(result["table"] == args.table and result["vintages"] == sorted(args.years), "Completion selections differ.")
        reconciliation = read_json(reconciliation_path)
        require(
            reconciliation["snapshot_id"] == receipt["snapshot_id"]
            and reconciliation["publisher"] == "census"
            and reconciliation["collection"] == "acs"
            and bool(reconciliation["objects"]),
            "Reconciliation snapshot or collection differs.",
        )
        require(
            len(reconciliation["manifests"]) == 1 and reconciliation["manifests"][0]["dataset_id"] == "acs_" + args.table.lower(),
            "Missing or unexpected table manifest.",
        )
        manifest_path = path.parent / "s3_collections" / ("acs_" + args.table.lower()) / "manifest.json"
        manifest_record = reconciliation["manifests"][0]["object"]
        require(fingerprint(manifest_path) == (manifest_record["sha256"], manifest_record["byte_count"]), "Local manifest differs from stored bytes.")
        manifest = read_json(manifest_path)
        require(
            manifest["snapshot_id"] == receipt["snapshot_id"]
            and manifest["source_id"] == "ACS"
            and manifest["dataset_id"] == "acs_" + args.table.lower()
            and manifest["registry_sha256"] == canonical_hash(registry)
            and manifest["objects"] == reconciliation["objects"],
            "Manifest lineage or object inventory differs.",
        )
        expected_members = {tuple(member["member_chain"]): (member["sha256"], member["bytes"], member["role"]) for member in mapping["members"]}
        actual_members = [entry for entry in reconciliation["objects"] if entry.get("member_chain")]
        require(len(actual_members) == len(expected_members), "Mapped member inventory is incomplete.")
        require(
            {tuple(entry["member_chain"]): (entry["object"]["sha256"], entry["object"]["byte_count"], entry["role"]) for entry in actual_members}
            == expected_members,
            "Stored members differ from captured archive.",
        )
        preflight(client, outputs)
        verified = set()
        for entry in reconciliation["objects"] + reconciliation["manifests"]:
            record = entry.get("object", entry)
            prefix = object_prefix(
                load_routes(registry)["ACS"],
                "acs_" + args.table.lower(),
                entry.get("role", "capture_receipt"),
                receipt["release"]["release_date"] or receipt["snapshot_id"],
                receipt["snapshot_id"],
                False,
            )
            require(
                record["bucket"] == settings["data_bucket_name"]
                and record["key"].startswith("census/acs/")
                and record["key"].startswith(prefix + "/")
                and record["sha256"] in record["key"].split("/"),
                "Object belongs to another destination, role or capture.",
            )
            identity = (record["key"], record["version_id"])
            if identity not in verified:
                verify_version(client, *identity, record["sha256"], record["byte_count"])
                verified.add(identity)
    else:
        result = upload_snapshot(path, [], client, outputs, registry, validator, archive_mapping=mapping)
    review_record, _ = store_file(client, review_path, f"census/acs/audit/capture_id={receipt['snapshot_id']}/{fingerprint(review_path)[0]}/{review_path.name}")
    completion = {
        "status": "stored_unvalidated",
        "source_id": "ACS",
        "table": args.table,
        "vintages": sorted(args.years),
        "reconciliation_path": result["reconciliation_path"],
        "transport_review_sha256": canonical_hash(review),
        "review_object": review_record,
        "model_eligible": False,
    }
    if not completed_path.exists():
        write_once(completed_path, encoded_json(completion))
    write_once(root / "completed_review_v2.json", encoded_json(completion))
    sys.stdout.write(str(json.dumps({"table": args.table, "vintages": sorted(args.years), "status": "stored_unvalidated"})) + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
