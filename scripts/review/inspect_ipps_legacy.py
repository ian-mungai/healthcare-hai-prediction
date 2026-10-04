"""Build a hash-bound legacy IPPS layout plan and profile every record without emitting row values.

Use ``build-plan --source-root ROOT --inventory INVENTORY --output PLAN``, then
``profile --source-root ROOT --plan PLAN --output REPORT``. Output is local review evidence, not staging data.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import sys
import zipfile
from collections import Counter
from pathlib import Path, PurePosixPath
from typing import Any

import pdfplumber
import xlrd
from xlrd.biffh import XLRDError

from scripts.review.review_remaining import digest, safe_path, write_json

YEARS = tuple(range(1994, 2002))
LIMIT = 32 << 20
FIELD = re.compile(r"^(\d+)(?:-(\d+)\s*|\s+)(\$?\d+\.\d*)\s*(.+)$")
NUMBER = re.compile(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)")
# These cells are physically split across page boundaries in the publisher PDF itself (not just text extraction).
PATCHES: dict[int, list[tuple[int, str, int | None, int, str]]] = {
    1994: [(162, "$1.", None, 1, "Reclassification status")],
    1997: [(96, "8.2", 103, 2, "Hospital-Specific Rate"), (229, "4.", 232, 6, "Mileage to Nearest Hospital")],
    1998: [(96, "8.2", 103, 2, "Hospital-Specific Rate"), (209, "9.7", 217, 6, "Capital Wage Index")],
    1999: [(96, "8.2", 103, 2, "Hospital-Specific Rate"), (209, "9.7", 217, 6, "Capital Wage Index")],
    2000: [(137, "8.2", None, 2, "Hospital-Specific Rate"), (253, "9.7", None, 5, "Operating Wage Index")],
}


class ReviewError(ValueError):
    """A safe error category that contains no source row values."""


def checked_artifact(root: Path, spec: dict[str, Any]) -> bytes:
    """Verify the pinned receipt and exact artifact; refuse changed bytes or unsafe paths."""
    receipt = safe_path(root, spec["receipt"])
    if not receipt.is_file() or digest(receipt) != spec["receipt_sha256"]:
        raise ReviewError("receipt_mismatch")
    matches = [a for a in json.loads(receipt.read_text())["artifacts"] if a["stored_file_name"] == spec["name"]]
    if len(matches) != 1 or matches[0]["sha256"] != spec["sha256"]:
        raise ReviewError("receipt_binding_mismatch")
    path = safe_path(root, str(receipt.parent.relative_to(root) / matches[0]["storage_path"]))
    if not path.is_file():
        raise ReviewError("artifact_missing")
    if path.stat().st_size != matches[0]["byte_count"] or digest(path) != spec["sha256"]:
        raise ReviewError("artifact_mismatch")
    if path.stat().st_size > LIMIT:
        raise ReviewError("artifact_over_limit")
    return path.read_bytes()


def member_bytes(data: bytes, name: str, expected_sha: str | None = None) -> bytes:
    """Read only the bound archive member, without extracting to disk."""
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or "\\" in name:
        raise ReviewError("unsafe_member")
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        matches = [info for info in archive.infolist() if info.filename == name]
        if len(matches) != 1:
            raise ReviewError("member_missing_or_duplicate")
        info = matches[0]
        if info.flag_bits & 1 or info.file_size > LIMIT:
            raise ReviewError("member_refused")
        result = archive.read(info)
    if expected_sha is not None and hashlib.sha256(result).hexdigest() != expected_sha:
        raise ReviewError("member_mismatch")
    return result


def field_spec(start: int, fmt: str, end: int | None, page: int, title: str, evidence: str) -> dict[str, Any]:
    """Use the published start and SAS width; retain disagreeing printed end positions explicitly."""
    width = int(fmt.lstrip("$").split(".")[0])
    return {"start": start, "width": width, "format": fmt, "declared_end": end, "page": page, "title": title, "evidence": evidence}


def layout_fields(data: bytes, year: int) -> list[dict[str, Any]]:
    """Extract the publisher's field rows, with explicit repairs for physically interleaved PDF cells."""
    fields: dict[int, dict[str, Any]] = {}
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for page_number, page in enumerate(pdf.pages, 1):
            for line in (page.extract_text() or "").splitlines():
                match = FIELD.match(line)
                if match is None:
                    continue
                start, end, fmt, title = match.groups()
                if int(start) > 400 or int(fmt.lstrip("$").split(".")[0]) > 40:
                    continue
                spec = field_spec(int(start), fmt, int(end) if end else None, page_number, title, "publisher_table_row")
                if int(start) in fields:
                    raise ReviewError("duplicate_layout_position")
                fields[int(start)] = spec
    for start, fmt, end, patch_page, title in PATCHES.get(year, []):
        if start not in fields:
            fields[start] = field_spec(start, fmt, end, patch_page, title, "publisher_cell_split_across_lines_or_pages")
    if len(fields) < 30:
        raise ReviewError("incomplete_layout")
    return [fields[start] for start in sorted(fields)]


def build_plan(root: Path, inventory: Path) -> dict[str, Any]:
    """Freeze the eight captured dictionary/data pairs without fetching or changing sources."""
    pairs: dict[int, dict[str, Any]] = {year: {} for year in YEARS}
    for candidate in json.loads(inventory.read_text())["candidates"]:
        if candidate["source_id"] != "CMS_IPPS":
            continue
        receipt_path = safe_path(root, candidate["receipt"])
        if not receipt_path.is_file() or digest(receipt_path) != candidate["receipt_sha256"]:
            raise ReviewError("receipt_mismatch")
        for artifact in json.loads(receipt_path.read_text())["artifacts"]:
            match = re.fullmatch(r"(pubfil|impfil)(9[4-9]|0[01])\.(pdf|zip)", artifact["stored_file_name"], re.I)
            if match is None:
                continue
            kind, short, _ = match.groups()
            year = 1900 + int(short) if int(short) >= 94 else 2000 + int(short)
            spec = {
                "receipt": candidate["receipt"],
                "receipt_sha256": candidate["receipt_sha256"],
                "name": artifact["stored_file_name"],
                "sha256": artifact["sha256"],
            }
            key = "dictionary" if kind.lower() == "pubfil" else "data"
            if key in pairs[year]:
                raise ReviewError("ambiguous_year_capture")
            pairs[year][key] = spec
    for year, pair in pairs.items():
        if set(pair) != {"dictionary", "data"}:
            raise ReviewError("missing_year_pair")
        pair["year"] = year
        pair["fields"] = layout_fields(checked_artifact(root, pair["dictionary"]), year)
        pair["member"] = f"IMPFIL{str(year)[-2:]}.{'xls' if year == 2001 else 'txt'}"
        content = member_bytes(checked_artifact(root, pair["data"]), pair["member"])
        pair["member_sha256"] = hashlib.sha256(content).hexdigest()
        pair["mode"] = "workbook" if year == 2001 else "fixed_width"
    return {"version": 1, "inventory_sha256": digest(inventory), "model_eligible": False, "years": list(pairs.values())}


def category(value: str) -> str:
    """Classify only; arbitrary row values are never retained in reports."""
    if not value:
        return "empty"
    if re.fullmatch(r"\.(?:[A-Z_])?", value):
        return "sas_missing"
    return "numeric" if NUMBER.fullmatch(value) else "text"


def fixed_profile(data: bytes, fields: list[dict[str, Any]]) -> dict[str, Any]:
    """Apply all published field widths to every row and expose layout/grain issues as counts."""
    ends = [field["start"] - 1 + field["width"] for field in fields]
    if not fields or any(field["start"] < 1 or field["width"] < 1 for field in fields):
        raise ReviewError("invalid_layout")
    covered: set[int] = set()
    for field, end in zip(fields, ends, strict=True):
        span = set(range(field["start"] - 1, end))
        if covered & span:
            raise ReviewError("overlapping_layout")
        covered.update(span)
    provider_fields = [f for f in fields if "provider number" in f["title"].lower()]
    if len(provider_fields) != 1:
        raise ReviewError("provider_field_missing_or_ambiguous")
    provider = provider_fields[0]
    columns: list[Counter[str]] = [Counter() for _ in fields]
    providers: Counter[str] = Counter()
    lengths: Counter[int] = Counter()
    rows = short = uncovered = controls = malformed = leading_zero = 0
    lines = data.decode("cp1252").splitlines()
    for index, line in enumerate(lines):
        if index == len(lines) - 1 and line == "\x1a":
            controls += 1
            continue
        rows += 1
        lengths[len(line)] += 1
        short += len(line) < max(ends)
        uncovered += any(char.strip() and position not in covered for position, char in enumerate(line))
        value = line[provider["start"] - 1 : provider["start"] - 1 + provider["width"]].strip()
        malformed += re.fullmatch(r"\d{6}", value) is None
        leading_zero += value.startswith("0")
        providers[value] += 1
        for field, stat in zip(fields, columns, strict=True):
            value = line[field["start"] - 1 : field["start"] - 1 + field["width"]].strip()
            stat[category(value)] += 1
    return {
        "mode": "fixed_width",
        "rows": rows,
        "line_lengths": dict(sorted(lengths.items())),
        "terminal_dos_markers": controls,
        "short_rows": short,
        "rows_with_nonblank_unmapped_bytes": uncovered,
        "provider_malformed": malformed,
        "provider_leading_zero_rows": leading_zero,
        "provider_duplicate_rows": sum(n - 1 for n in providers.values()),
        "fields": [
            {
                **field,
                "categories": dict(sorted(stat.items())),
                "printed_range_disagrees_with_format": field["declared_end"] is not None and field["declared_end"] != end,
            }
            for field, stat, end in zip(fields, columns, ends, strict=True)
        ],
    }


def workbook_profile(data: bytes) -> dict[str, Any]:
    """Review FY 2001 native cells; do not apply byte offsets to a workbook."""
    book = xlrd.open_workbook(file_contents=data)
    sheets = []
    for sheet in book.sheets():
        headers = [str(v) for v in sheet.row_values(0)]
        if headers.count("PROV") != 1:
            raise ReviewError("workbook_provider_header_missing")
        key = headers.index("PROV")
        counts: Counter[str] = Counter()
        columns: list[Counter[str]] = [Counter() for _ in headers]
        leading_zero = malformed = errors = numeric_provider_cells = 0
        for index in range(1, sheet.nrows):
            cells = sheet.row(index)
            provider = cells[key]
            numeric_provider_cells += provider.ctype == xlrd.XL_CELL_NUMBER
            value = str(provider.value).strip()
            counts[value] += 1
            leading_zero += value.startswith("0")
            malformed += re.fullmatch(r"\d{6}", value) is None
            for cell, stat in zip(cells, columns, strict=True):
                errors += cell.ctype == xlrd.XL_CELL_ERROR
                stat[category(str(cell.value).strip())] += 1
        sheets.append(
            {
                "rows": sheet.nrows - 1,
                "columns": len(headers),
                "provider_duplicate_rows": sum(n - 1 for n in counts.values()),
                "provider_leading_zero_rows": leading_zero,
                "provider_malformed": malformed,
                "excel_error_cells": errors,
                "numeric_provider_cells": numeric_provider_cells,
                "headers": [{"name": header, "categories": dict(sorted(stat.items()))} for header, stat in zip(headers, columns, strict=True)],
            }
        )
    book.release_resources()
    return {"mode": "workbook", "rows": sum(s["rows"] for s in sheets), "sheets": sheets}


def profile(root: Path, plan: Path) -> dict[str, Any]:
    """Reverify all immutable bindings before using the frozen positions."""
    results = []
    for pair in json.loads(plan.read_text())["years"]:
        checked_artifact(root, pair["dictionary"])
        data = member_bytes(checked_artifact(root, pair["data"]), pair["member"], pair["member_sha256"])
        result = workbook_profile(data) if pair["mode"] == "workbook" else fixed_profile(data, pair["fields"])
        results.append({"year": pair["year"], "dictionary_sha256": pair["dictionary"]["sha256"], "member_sha256": pair["member_sha256"], **result})
    return {"version": 1, "code_sha256": digest(Path(__file__)), "plan_sha256": digest(plan), "model_eligible": False, "years": results}


def main() -> int:
    """Run the real CLI and report safe failure classes."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("build-plan", "profile"):
        sub = commands.add_parser(command)
        sub.add_argument("--source-root", type=Path, required=True)
        sub.add_argument("--output", type=Path, required=True)
        sub.add_argument("--inventory" if command == "build-plan" else "--plan", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = build_plan(args.source_root.resolve(), args.inventory) if args.command == "build-plan" else profile(args.source_root.resolve(), args.plan)
        write_json(args.output, result)
    except (ReviewError, ValueError, KeyError, OSError, zipfile.BadZipFile, XLRDError) as error:
        message = str(error) if isinstance(error, ReviewError) else type(error).__name__
        sys.stderr.write(f"IPPS review refused: {message}\n")
        return 1
    sys.stdout.write(json.dumps({"years": len(result["years"]), "model_eligible": False}) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
