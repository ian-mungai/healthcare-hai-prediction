"""Check whether six Alaska county codes in Provider of Services rows appear as an SSA or FIPS code in county crosswalks (WV3-002).

The primary source is the CMS FY 2020 final rule county-to-CBSA crosswalk. The three NBER SSA-to-FIPS files are supporting evidence (built from CMS files).
The files are fetched into a new local folder and never committed. The check is offline analysis: nothing is loaded into bronze.

    .venv/bin/python -m scripts.acquisition.one_off.reference_documents.check_alaska_county_codes --output data/reference_checks/<date>/alaska_crosswalks
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import zipfile
from pathlib import Path
from typing import Any

from scripts.acquisition.one_off.reference_documents.fetch_support import FetchError, file_sha256, run_fetch
from scripts.acquisition.transport import CaptureError

CODES = ("02040", "02080", "02120", "02140", "02210", "02260")
NAMES_SOURCE = "nber_msabea"
CMS_BASE = "https://www.cms.gov/medicare/medicare-fee-for-service-payment/acuteinpatientpps/downloads/"
NBER_BASE = "https://data.nber.org/ssa-fips-state-county-crosswalk/"
SOURCES: tuple[dict[str, Any], ...] = (
    {
        "id": "cms_fy2020_county_cbsa",
        "role": "primary",
        "url": CMS_BASE + "fy2020-fr-county-cbsa-crosswalk.zip",
        "file_name": "fy2020-fr-county-cbsa-crosswalk.zip",
        "format": "zip",
        "media_types": {"application/zip"},
        "member": "FY 2020 FR County to CBSA Crosswalk.txt",
        "delimiter": "\t",
        "columns": {"county": "County Name", "state": "State", "ssa": "SSACD", "fips": "FIPS County Code"},
    },
    {
        "id": "nber_msabea",
        "role": "supporting",
        "url": NBER_BASE + "msabea.csv",
        "file_name": "msabea.csv",
        "format": "csv",
        "media_types": {"text/csv"},
        "member": None,
        "delimiter": ",",
        "columns": {"county": "county", "state": "abbr", "ssa": "ssa", "fips": "fips"},
    },
    {
        "id": "nber_2011",
        "role": "supporting",
        "url": NBER_BASE + "2011/ssa_fips_state_county2011.csv",
        "file_name": "ssa_fips_state_county2011.csv",
        "format": "csv",
        "media_types": {"text/csv"},
        "member": None,
        "delimiter": ",",
        "columns": {"county": "county", "state": "state", "ssa": "ssacounty", "fips": "fipscounty"},
    },
    {
        "id": "nber_2020",
        "role": "supporting",
        "url": NBER_BASE + "2020/ssa_fips_state_county2020.csv",
        "file_name": "ssa_fips_state_county2020.csv",
        "format": "csv",
        "media_types": {"text/csv"},
        "member": None,
        "delimiter": ",",
        "columns": {"county": "county", "state": "state", "ssa": "ssacd", "fips": "fipscounty"},
    },
)


def read_rows(source: dict[str, Any], path: Path) -> list[dict[str, str]]:
    """Return the crosswalk rows as dictionaries; a missing member or column fails instead of reading a different layout."""
    if source["member"] is None:
        raw = path.read_bytes()
    else:
        try:
            with zipfile.ZipFile(path) as archive:
                raw = archive.read(source["member"])
        except KeyError as error:
            raise FetchError(f"{source['id']}: archive member {source['member']} is missing") from error
    reader = csv.DictReader(io.StringIO(raw.decode("latin-1"), newline=""), delimiter=source["delimiter"])
    missing = sorted(set(source["columns"].values()) - set(reader.fieldnames or []))
    if missing:
        raise FetchError(f"{source['id']}: column layout differs, missing {missing}")
    return list(reader)


def check_source(source: dict[str, Any], path: Path) -> dict[str, Any]:
    """Return where each of the six codes appears as an SSA or FIPS code in one file."""
    columns = source["columns"]
    rows = read_rows(source, path)
    found: dict[str, dict[str, list[dict[str, str]]]] = {}
    for row in rows:
        ssa, fips = (row[columns["ssa"]] or "").strip(), (row[columns["fips"]] or "").strip()
        for code in CODES:
            for kind, value in (("as_ssa", ssa), ("as_fips", fips)):
                if value == code:
                    entry = {"county": row[columns["county"]], "state": row[columns["state"]], "ssa": ssa, "fips": fips}
                    found.setdefault(code, {}).setdefault(kind, []).append(entry)
    return {
        "id": source["id"],
        "role": source["role"],
        "file_name": source["file_name"],
        "sha256": file_sha256(path),
        "bytes": path.stat().st_size,
        "rows": len(rows),
        "alaska_rows": sum(1 for row in rows if (row[columns["state"]] or "").strip() == "AK"),
        "codes_as_ssa": sorted(code for code, kinds in found.items() if "as_ssa" in kinds),
        "codes_as_fips": sorted(code for code, kinds in found.items() if "as_fips" in kinds),
        "found": dict(sorted(found.items())),
    }


def historical_names(sources: list[dict[str, Any]]) -> dict[str, Any]:
    """Return each code's county names from the rows of ``NAMES_SOURCE`` whose SSA column equals the code; a code that file lacks gets an empty list."""
    source = next(item for item in sources if item["id"] == NAMES_SOURCE)
    return {
        "source": NAMES_SOURCE,
        "names": {code: sorted({row["county"] for row in source["found"].get(code, {}).get("as_ssa", [])}) for code in CODES},
    }


def build_report(output: Path, receipt: dict[str, Any]) -> dict[str, Any]:
    """Check every accepted file and return the report; it holds no time, so a rerun on the same files gives the same bytes."""
    sources = [check_source(source, output / entry["path"]) for source, entry in zip(SOURCES, receipt["files"], strict=True)]
    return {
        "codes_checked": list(CODES),
        "match_rule": "exact five-digit string in the SSA or FIPS county column",
        "historical_names": historical_names(sources),
        "sources": sources,
        "summary": {
            "sources_with_any_code": [item["id"] for item in sources if item["found"]],
            "primary_source_has_any_code": any(item["found"] for item in sources if item["role"] == "primary"),
            "sources_with_all_six_codes_as_ssa": [item["id"] for item in sources if item["codes_as_ssa"] == list(CODES)],
            "sources_with_all_six_codes_as_fips": [item["id"] for item in sources if item["codes_as_fips"] == list(CODES)],
        },
    }


def write_report(path: Path, report: dict[str, Any]) -> None:
    """Write the report once; an existing report must be identical."""
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != text:
            raise FetchError(f"{path} differs from this run's report; use a new output folder")
        return
    with path.open("x", encoding="utf-8") as handle:
        handle.write(text)


def main(argv: list[str] | None = None) -> int:
    """Fetch the four files into a new folder, write and print the report and return 0 when every file was accepted."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, required=True, help="A new empty folder; a folder with a receipt is rechecked offline.")
    args = parser.parse_args(argv)
    specs = [{key: source[key] for key in ("url", "file_name", "format", "media_types")} for source in SOURCES]
    try:
        receipt, requested = run_fetch("alaska_county_crosswalks", specs, args.output)
        if not receipt["all_accepted"]:
            rejected = [
                {key: entry[key] for key in ("file_name", "reason", "http_status", "media_type")} for entry in receipt["files"] if entry["status"] != "accepted"
            ]
            sys.stdout.write(json.dumps({"requests_made": requested, "all_accepted": False, "rejected": rejected}, indent=2) + "\n")
            return 1
        report = build_report(args.output, receipt)
        write_report(args.output / "report.json", report)
    except (FetchError, CaptureError, OSError, zipfile.BadZipFile) as error:
        parser.error(str(error))
    sys.stdout.write(json.dumps({"requests_made": requested, "all_accepted": True, **report}, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
