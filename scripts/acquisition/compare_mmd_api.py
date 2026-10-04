"""Compare twelve authorized AMI API snapshots with immutable CMS browser exports.

This local verification runner never uploads, changes acquisition plans or clears
review gates. Network reads require --fetch; offline replay verifies saved hashes.
"""

import argparse
import csv
import hashlib
import http.client
import io
import json
import logging
import math
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlencode, urlsplit

from scripts.acquisition.data_paths import current
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import read_json, require

LOGGER = logging.getLogger(__name__)
LIMIT = 16 * 1024**2
HEADERS = [
    "population",
    "year",
    "geography",
    "measure",
    "adjustment",
    "analysis",
    "domain",
    "condition",
    "primary_sex",
    "primary_age",
    "primary_dual",
    "fips",
    "county",
    "state",
    "urban",
    "primary_race",
    "primary_eligibility",
    "primary_denominator",
    "analysis_value",
]


def verify_file(path: Path, digest: str) -> bytes:
    """Read a regular file only when its immutable checksum matches."""
    require(path.is_file() and not path.is_symlink(), f"Missing or symlinked input: {path}")
    raw = path.read_bytes()
    require(hashlib.sha256(raw).hexdigest() == digest, f"Checksum mismatch: {path}")
    return raw


def year_code(root: Path, year: int) -> str:
    """Read CMS's actual menu code instead of assuming calendar-year encoding."""
    source = (root / "references/menus.js").read_text()
    matches = sorted(set(re.findall(r"'disp':\s*\"" + str(year) + r"\",\s*'val':\s*\"([^\"]+)\"", source)))
    require(len(matches) == 1, f"Ambiguous or missing menu code for {year}")
    return matches[0]


def request_for_year(root: Path, manifest: dict, year: int) -> tuple[str, dict[str, str], list[str]]:
    """Reproduce the CMS first-match crosswalk and literal filter construction."""
    code = year_code(root, year)
    dimensions = {"population": "f", "measure": "v", "year": code, "elig": ".", "race_code": ".", "sex_code": ".", "adjust": "1", "dual": "."}
    with (root / "references/codebook_crosswalk.csv").open(newline="") as handle:
        candidates = [r for r in csv.DictReader(handle) if all(not r.get(k) or v in r[k] for k, v in dimensions.items())]
    require(bool(candidates), f"No CMS source for {year}")
    chosen = candidates[0]
    require(chosen["year"] == code and chosen["population"] == "f" and chosen["measure"] == "v", "Unsafe crosswalk first match")
    filters = dict(manifest["filters"], year=code)
    require(filters["condition"] == "2" and filters["geography"] == "c", "Only county AMI comparison is authorized")
    query = {"_source": chosen["url"]} | {key: ".|IS NULL" if value == "." else value for key, value in filters.items()} | {"_size": "500000"}
    url = manifest["endpoint"] + "?" + urlencode(query, quote_via=quote)
    return url, filters, [r["url"] for r in candidates]


def api_bytes(root: Path, year: int, url: str, fetch: bool) -> tuple[bytes, dict]:
    """Retain one complete response or verify its saved URL and hash on replay."""
    target = root / "api_raw" / f"{year}.json"
    metadata_path = root / "api_raw" / f"{year}_receipt.json"
    if metadata_path.exists():
        metadata = read_json(metadata_path)
        stored_url, planned_url = urlsplit(metadata["url"]), urlsplit(url)
        require(stored_url[:3] == planned_url[:3] and sorted(parse_qsl(stored_url.query)) == sorted(parse_qsl(planned_url.query)), "Stored API request differs")
        return verify_file(target, metadata["sha256"]), metadata
    require(fetch, f"No captured response for {year}; --fetch is required")
    require(not target.exists(), f"Unreceipted response exists for {year}; review its provenance")
    parsed = urlsplit(url)
    require(parsed.scheme == "https" and bool(parsed.hostname) and parsed.username is None and parsed.password is None, "Invalid HTTPS endpoint")
    connection = http.client.HTTPSConnection(parsed.hostname or "", timeout=25)
    try:
        connection.request("GET", parsed.path + "?" + parsed.query, headers={"Accept": "application/json"})
        response = connection.getresponse()
        require(response.status == 200, f"API HTTP status {response.status} for {year}")
        content_type = response.getheader("Content-Type", "")
        require("application/json" in content_type, "API response is not JSON")
        body = response.read(LIMIT + 1)
        require(0 < len(body) <= LIMIT, "API response exceeds size bound")
        require(isinstance(json.loads(body), list), "API response must be an array")
        metadata = {
            "url": url,
            "status": response.status,
            "content_type": content_type,
            "retrieved_at_utc": datetime.now(UTC).isoformat(),
            "sha256": hashlib.sha256(body).hexdigest(),
            "bytes": len(body),
        }
        write_once(target, body)
        write_once(metadata_path, encoded_json(metadata))
        return body, metadata
    finally:
        connection.close()


def js_rate(value: str | int | float | None) -> str:
    """Represent observed API rates as the browser parseFloat/CSV path does."""
    match = re.match(r"^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?", str(value).lstrip())
    if not match:
        return "NaN"
    number = float(match[0])
    require(math.isfinite(number), "Non-finite numeric rate")
    if number == 0:
        return "0"
    if number.is_integer() and abs(number) < 1e21:
        return str(int(number))
    result = repr(number)
    return re.sub(r"e([+-])0+(\d+)", r"e\1\2", result)


def lookup(root: Path, name: str) -> tuple[dict[int, str], dict]:
    """Reproduce CMS map-set ordering, reporting repeated keys and decoding loss."""
    text = (root / "references" / name).read_bytes().decode("utf-8", errors="replace")
    rows = list(csv.DictReader(io.StringIO(text), delimiter="\t"))
    grouped: dict[int, list[str]] = {}
    for row in rows:
        grouped.setdefault(int(row["id"]), []).append(row["name"])
    result = {int(row["id"]): row["name"] for row in rows}
    audit = {
        "duplicate_ids_in_source_order": {str(key): values for key, values in grouped.items() if len(values) > 1},
        "utf8_replacement_characters": text.count("\ufffd"),
        "handling": "CMS d3.map.set uses last row; urban labels are unused, only ID membership matters",
    }
    return result, audit


def reconstruct(
    rows: list[dict], filters: dict[str, str], selections: dict[str, str], names: dict[int, str], urban: dict[int, str], labels: dict
) -> tuple[bytes, dict]:
    """Build the browser-shaped CSV independently from raw API and CMS lookups."""
    ids: set[int] = set()
    derived: dict[int, list[str]] = {}
    suppressed = 0
    excluded = []
    missing_fields: set[str] = set()
    denominator_counts: dict[str, int] = {}
    for row in rows:
        require(isinstance(row, dict) and {"fips", "rate", "dencat", "condition"} <= row.keys(), "Missing API value/key fields")
        for key, expected in filters.items():
            if key not in row:
                missing_fields.add(key)
                continue
            # CMS's .|IS NULL branch serializes older aggregate dimensions as empty strings.
            require(row[key] == expected or (expected == "." and row[key] in (None, "")), f"Returned filter mismatch: {key}")
        raw_id = str(row["fips"])
        require(bool(re.fullmatch(r"\d+", raw_id)), "Invalid native API FIPS")
        fips = int(raw_id)
        require(fips not in ids, "Duplicate FIPS would be overwritten in browser")
        ids.add(fips)
        if fips < 1000:
            excluded.append(raw_id)
            continue
        dencat = str(row["dencat"])
        denominator = labels.get(dencat, "undefined")
        denominator_counts[dencat] = denominator_counts.get(dencat, 0) + 1
        value = js_rate(row["rate"])
        suppressed += value == "NaN"
        cells = dict(selections) | {
            "fips": str(fips),
            "county": names.get(fips, ""),
            "state": names.get(fips // 1000, ""),
            "urban": "Urban" if fips in urban else "Rural",
            "primary_denominator": denominator,
            "analysis_value": value,
        }
        derived[fips] = [cells[key] for key in HEADERS]
    lines = [",".join(HEADERS)]
    for fips in sorted(derived):
        row_cells = derived[fips].copy()
        for index in (7, 17):
            row_cells[index] = '"' + row_cells[index] + '"'
        lines.append(",".join(row_cells))
    stats = {
        "api_rows": len(rows),
        "csv_rows": len(derived),
        "excluded_native_ids": excluded,
        "missing_api_dimensions": sorted(missing_fields),
        "suppressed_or_non_numeric_rates": suppressed,
        "raw_denominator_categories": denominator_counts,
        "returned_dimension_values": {key: sorted({str(row[key]) for row in rows if key in row}) for key in filters},
    }
    return "\r\n".join(lines).encode(), stats


def compare_year(root: Path, manifest: dict, item: dict, fetch: bool, names: dict[int, str], urban: dict[int, str]) -> dict:
    """Check capture integrity, all cells and the complete reconstructed bytes."""
    year = item["year"]
    receipt_path = Path(current(item["receipt_path"]))
    receipt = read_json(receipt_path)
    artifacts = [r for r in receipt["artifacts"] if r["role"] == "data"]
    require(len(artifacts) == 1, "Expected one saved browser CSV")
    artifact = artifacts[0]
    require(artifact["sha256"] == item["sha256"], "Coverage and receipt disagree")
    original_path = receipt_path.parent / artifact["storage_path"]
    require(original_path.resolve().is_relative_to(receipt_path.parent.resolve()), "Original escapes capture directory")
    original = verify_file(original_path, item["sha256"])
    require(len(original) == artifact["byte_count"], "Original size mismatch")
    selections = dict(manifest["export_selections"], year=str(year))
    require(selections == receipt["acquisition"]["export_selections"], "Approved selections differ from receipt")
    reader = csv.DictReader(io.StringIO(original.decode("utf-8-sig")))
    require(reader.fieldnames == HEADERS, "Original headers differ")
    expected = list(reader)
    require(len(expected) == item["rows"], "Original row count differs")
    require(all(all(r[k] == v for k, v in selections.items()) for r in expected), "Original selections differ")
    require(len({r["fips"] for r in expected}) == len(expected), "Original duplicate FIPS")
    url, filters, candidates = request_for_year(root, manifest, year)
    raw, api_receipt = api_bytes(root, year, url, fetch)
    generated, stats = reconstruct(json.loads(raw), filters, selections, names, urban, manifest["denominator_labels"])
    generated_path = root / "reconstructed" / f"mmd_ffs_county_ami_prevalence_{year}.csv"
    write_once(generated_path, generated)
    observed = list(csv.DictReader(io.StringIO(generated.decode())))
    old_by_id, new_by_id = ({r["fips"]: r for r in group} for group in (expected, observed))
    missing, extra = sorted(old_by_id.keys() - new_by_id.keys()), sorted(new_by_id.keys() - old_by_id.keys())
    mismatch_count = 0
    examples: list[dict[str, str]] = []
    for key in sorted(old_by_id.keys() & new_by_id.keys()):
        for column in HEADERS:
            if old_by_id[key][column] != new_by_id[key][column]:
                mismatch_count += 1
                if len(examples) < 20:
                    examples.append({"fips": key, "column": column, "browser": old_by_id[key][column], "api_derived": new_by_id[key][column]})
    unchanged = fingerprint(original_path)[0] == item["sha256"]
    same_order = [r["fips"] for r in expected] == [r["fips"] for r in observed]
    passed = not missing and not extra and mismatch_count == 0 and generated == original and unchanged
    return {
        "year": year,
        "status": "passed" if passed else "failed",
        "original_path": str(original_path),
        "original_sha256": item["sha256"],
        "reconstructed_path": str(generated_path),
        "reconstructed_sha256": hashlib.sha256(generated).hexdigest(),
        "api_receipt": api_receipt,
        "crosswalk_candidates_in_order": candidates,
        "filters": filters,
        "csv_rows": len(expected),
        "all_13_selections_match": True,
        "missing_native_fips": missing,
        "extra_native_fips": extra,
        "mismatched_cells": mismatch_count,
        "mismatch_examples": examples,
        "same_row_order": same_order,
        "byte_identical": generated == original,
        "original_unchanged": unchanged,
        **stats,
    }


def run(args: argparse.Namespace) -> dict:
    """Execute twelve authorized comparisons and produce repeatable local evidence."""
    root = args.manifest.parent
    manifest = read_json(args.manifest)
    require(manifest["scope"] == "C258.01 API comparison only" and manifest["model_eligible"] is False, "Invalid scope or model gate")
    for name, reference in manifest["references"].items():
        verify_file(root / "references" / name, reference["sha256"])
    coverage = json.loads(verify_file(Path(manifest["coverage_path"]), manifest["coverage_sha256"]))
    require([r["year"] for r in coverage["years"]] == list(range(2012, 2024)), "Expected twelve complete ordered years")
    selected_years = args.years or list(range(2012, 2024))
    require(set(selected_years) <= set(range(2012, 2024)), "Year outside approved comparison")
    names, names_audit = lookup(root, "countynames.tsv")
    urban, urban_audit = lookup(root, "urban.tsv")
    results = []
    for item in coverage["years"]:
        if item["year"] not in selected_years:
            continue
        try:
            result = compare_year(root, manifest, item, args.fetch, names, urban)
        except (KeyError, TypeError, ValueError, OSError, http.client.HTTPException) as error:
            result = {"year": item["year"], "status": "failed", "error": str(error)}
        results.append(result)
        LOGGER.info("year=%s status=%s byte_identical=%s", item["year"], result["status"], result.get("byte_identical"))
    return {
        "scope": manifest["scope"],
        "created_at_utc": manifest["created_at_utc"],
        "python_version": sys.version,
        "script_sha256": fingerprint(Path(__file__))[0],
        "manifest_sha256": fingerprint(args.manifest)[0],
        "status": "passed" if all(r["status"] == "passed" for r in results) else "failed",
        "years": results,
        "complete_12_year_coverage": len(results) == 12,
        "reference_observations": {"countynames.tsv": names_audit, "urban.tsv": urban_audit},
        "model_eligible": False,
        "further_api_collection_authorized": False,
        "verification_boundary": (
            "Live API capture and independent reconstruction versus twelve saved browser exports; offline replay uses exact retained responses."
        ),
        "limits": [
            "No S3 uploads or fresh S3 verification",
            "No clinical, definition or modeling clearance",
            "No other conditions or source APIs tested",
            "Completeness established relative to saved browser exports, not unpublished CMS records",
            "No downloaded JavaScript executed",
        ],
        "reproduce": f".venv/bin/python -m scripts.acquisition.compare_mmd_api --manifest {args.manifest} --report {root / 'replay.json'}",
    }


def main() -> None:
    """Run a bounded comparison; --fetch is the only network-enabled mode."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--years", type=int, nargs="+")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        report = run(args)
    except (KeyError, TypeError, ValueError, OSError, http.client.HTTPException) as error:
        report = {"status": "failed", "stage": "input_validation", "error": str(error), "model_eligible": False}
    write_once(args.report, encoded_json(report))
    LOGGER.info("report=%s status=%s", args.report, report["status"])
    if report["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
