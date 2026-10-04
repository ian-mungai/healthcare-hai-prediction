"""Read every tabular member of the selected sources in full and record structure, categories and grain.

Archives are opened in memory (one nesting level) and never extracted. Delimited and fixed-width text, ``.xlsx`` and
``.xls`` sheets are read row by row; PDFs get page and text counts. Output holds header names that pass a
contact-pattern filter, counts and checksums only, never cell values.

Usage (the interpreter needs openpyxl, pypdf and xlrd)::

    PYTHONPATH=.review_dependencies BUNDLED_PYTHON -m scripts.review.inspect_members --source-root SOURCE_ROOT \
        --inventory data/schema_review/2026_09_30/inputs/capture_inventory_effective.json --output members_run1
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import re
import sys
import zipfile
from collections import Counter
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path, PurePosixPath
from typing import Any

import openpyxl
import xlrd
from pypdf import PdfReader
from pypdf.errors import PdfReadError
from xlrd.biffh import XLRDError

from scripts.review.inspect_hai_archives import MEMBER_LIMIT, is_resource_fork, normalize, safe_text, sha256
from scripts.review.review_remaining import digest, safe_path, value_counts, write_json

SOURCES = ("ACS", "CMS_HCRIS_PUF", "CMS_IPPS", "CMS_OCCMIX", "HCAI_FINANCE", "HPSA", "NY", "SAHIE", "main-cmi-ipps")
KEYS = {
    "CMS_HCRIS_PUF": (("rpt_rec_num",), ("provider_ccn", "fiscal_year_begin_date")),
    "SAHIE": (("year", "statefips", "countyfips", "geocat", "agecat", "racecat", "sexcat", "iprcat"),),
}
IDENTIFIER = re.compile(
    r"(^|_)(provider|prov|provno|ccn|provider_number|provider_ccn|fips|geoid|cbsa|logrecno|rpt_rec_num|statefips|countyfips|oshpd_id|facility_number)(_|$)"
)
TEXT_SUFFIXES = {".csv", ".txt", ".dat", ".tsv", ".prn"}
EXCEL_ERRORS = {"#DIV/0!", "#REF!", "#N/A", "#VALUE!", "#NAME?", "#NUM!", "#NULL!"}
HEADER_SCAN = 120
log = logging.getLogger("inspect_members")


def decode(data: bytes) -> tuple[str, str]:
    """UTF-16 when a byte-order mark says so, else UTF-8 (BOM allowed), then CP1252 with replacement characters."""
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        return data.decode("utf-16", errors="replace"), "utf-16"
    try:
        return data.decode("utf-8-sig"), "utf-8-sig"
    except UnicodeDecodeError:
        return data.decode("cp1252", errors="replace"), "cp1252"


def is_numeric_text(value: str) -> bool:
    """Numbers as text, allowing thousands separators, percent signs and currency."""
    return bool(re.fullmatch(r"[-+(]?[$]?\d[\d,]*(\.\d+)?%?\)?|[-+]?\.\d+|[-+]?\d+(\.\d+)?[eE][-+]?\d+", value.strip()))


@lru_cache(maxsize=1 << 18)
def categories(value: str) -> tuple[tuple[str, int], ...]:
    """Category labels for one cell, identical to the first-pass audit's rules; plain digit strings take a fast path."""
    if value.isdigit():
        return (("observed", 1), ("numeric", 1), *((("leading_zero", 1),) if len(value) > 1 and value[0] == "0" else ()))
    stat: list[Counter[str]] = [Counter()]
    value_counts([value], stat)
    return tuple(stat[0].items())


def count_row(values: list[str], stats: list[Counter[str]]) -> None:
    """Add one row's cell categories to the per-column counters."""
    for value, stat in zip(values, stats, strict=True):
        stat.update(dict(categories(value)))


def hcris_periods(names: list[str], data: list[list[str]], member: str) -> dict[str, Any] | None:
    """Cost-report periods against the federal fiscal year in the file name (Oct 1 of the prior year to Sep 30)."""
    match = re.search(r"CostReport_(\d{4})", member)
    positions = {normalize(name): index for index, name in enumerate(names)}
    if match is None or not {"fiscal_year_begin_date", "fiscal_year_end_date", "provider_ccn"} <= positions.keys():
        return None
    year = int(match.group(1))
    lengths: Counter[str] = Counter()
    outside = unparseable = 0
    reports: Counter[str] = Counter()
    for row in data:
        try:
            begin = datetime.strptime(row[positions["fiscal_year_begin_date"]].strip(), "%m/%d/%Y").date()
            end = datetime.strptime(row[positions["fiscal_year_end_date"]].strip(), "%m/%d/%Y").date()
        except ValueError:
            unparseable += 1
            continue
        outside += not date(year - 1, 10, 1) <= begin <= date(year, 9, 30)
        days = (end - begin + timedelta(days=1)).days
        lengths["under_360_days" if days < 360 else "360_to_370_days" if days <= 370 else "over_370_days"] += 1
        reports[row[positions["provider_ccn"]].strip()] += 1
    return {
        "file_federal_fiscal_year": year,
        "begin_outside_file_fiscal_year": outside,
        "unparseable_dates": unparseable,
        "period_lengths": dict(sorted(lengths.items())),
        "ccns_with_several_reports": sum(count > 1 for count in reports.values()),
    }


def find_header(rows: list[list[str]], width: int) -> int | None:
    """Index of the first row, within the first rows, that reads as a header rather than data or a title."""
    for index, row in enumerate(rows[:HEADER_SCAN]):
        cells = [cell.strip() for cell in row if cell.strip()]
        if len(cells) >= max(2, width // 2) and sum(not is_numeric_text(cell) for cell in cells) >= 0.8 * len(cells):
            return index
    return None


def identifier_stats(values: list[str]) -> dict[str, Any]:
    """Shape and uniqueness of an identifier-like column, without its values."""
    present = [value.strip() for value in values if value.strip()]
    counts = Counter(present)
    return {
        "non_blank": len(present),
        "distinct": len(counts),
        "rows_repeating_a_value": len(present) - len(counts),
        "lengths": dict(sorted(Counter(str(len(value)) for value in counts).items(), key=lambda item: int(item[0]))),
        "leading_zero_values": sum(value.startswith("0") and value.isdigit() and len(value) > 1 for value in counts),
        "values_with_letters": sum(any(ch.isalpha() for ch in value) for value in counts),
        "float_suffix_values": sum(value.endswith(".0") for value in counts),
    }


def key_duplicates(names: list[str], data: list[list[str]], source: str) -> list[dict[str, Any]]:
    """Test each configured candidate key whose columns are all present."""
    positions = {normalize(name): index for index, name in enumerate(names)}
    results: list[dict[str, Any]] = []
    for key in KEYS.get(source, ()):
        if not all(part in positions for part in key):
            results.append({"key": list(key), "status": "columns_absent"})
            continue
        tuples = [tuple(row[positions[part]].strip() for part in key) for row in data]
        results.append({"key": list(key), "status": "tested", "rows": len(tuples), "duplicate_rows": len(tuples) - len(set(tuples))})
    return results


def profile_rows(rows: list[list[str]], source: str, member: str = "") -> dict[str, Any]:
    """Shape, header, per-column categories, identifiers and candidate keys for one table."""
    rows = [row for row in rows if any(cell.strip() for cell in row)]
    if not rows:
        return {"status": "empty_table"}
    widths = Counter(len(row) for row in rows)
    width = widths.most_common(1)[0][0]
    header_index = find_header(rows, width)
    header = rows[header_index] if header_index is not None else []
    body = rows[header_index + 1 :] if header_index is not None else rows
    names = [str(position) for position in range(width)] if header_index is None else [*header[:width], *[""] * (width - len(header))]
    data = [row for row in body if len(row) == width]
    stats: list[Counter[str]] = [Counter() for _ in range(width)]
    errors = 0
    for row in data:
        count_row(row, stats)
        errors += sum(cell.strip() in EXCEL_ERRORS for cell in row)
    columns = []
    for index, name in enumerate(names):
        column: dict[str, Any] = {
            "name": safe_text(name, 120) or ("<blank>" if not name.strip() else "<withheld>"),
            "counts": dict(sorted(stats[index].items())),
        }
        if (header_index is not None and IDENTIFIER.search(normalize(name))) or (header_index is None and index == 0):
            column["identifier"] = identifier_stats([row[index] for row in data])
        columns.append(column)
    return {
        "status": "table",
        "header_row_index": header_index,
        "title_rows_above_header": header_index or 0,
        "rows_after_header": len(body),
        "data_rows_at_modal_width": len(data),
        "other_width_rows": len(body) - len(data),
        "width_counts": dict(sorted((str(k), v) for k, v in widths.items())),
        "excel_error_cells": errors,
        "duplicate_header_names": len(names) - len(set(names)) if header_index is not None else None,
        "columns": columns,
        "candidate_keys": key_duplicates(names, data, source) if header_index is not None else [],
        "reporting_periods": hcris_periods(names, data, member) if source == "CMS_HCRIS_PUF" else None,
    }


def profile_text(data: bytes, source: str, member: str) -> dict[str, Any]:
    """Delimited text as a table; whitespace-separated columns as a positional table; anything else as fixed-width text."""
    text, encoding = decode(data)
    if "\x00" in text[:65536]:
        return {"status": "binary_content", "encoding": encoding}
    replaced = text.count("\ufffd")
    try:
        delimiter: str | None = csv.Sniffer().sniff(text[:65536], delimiters=",\t|;").delimiter
    except csv.Error:
        # Sniffing fails on files that open with prose (SAHIE); a .csv extension still means commas.
        delimiter = "," if member.lower().endswith(".csv") else None
    if delimiter is None:
        lines = [line for line in text.splitlines() if line.strip()]
        tokens = Counter(len(line.split()) for line in lines)
        common = tokens.most_common(1)[0] if tokens else (0, 0)
        if common[0] >= 2 and common[1] >= 0.95 * len(lines):
            rows = [line.split() for line in lines]
            return {"encoding": encoding, "delimiter": "whitespace", "replacement_characters": replaced, **profile_rows(rows, source, member)}
        lengths = Counter(len(line) for line in lines)
        modal = lengths.most_common(1)[0] if lengths else (0, 0)
        return {
            "status": "fixed_width_or_free_text",
            "encoding": encoding,
            "replacement_characters": replaced,
            "non_blank_lines": len(lines),
            "modal_line_length": modal[0],
            "lines_at_modal_length": modal[1],
            "distinct_line_lengths": len(lengths),
        }
    try:
        rows = list(csv.reader(io.StringIO(text, newline=""), delimiter=delimiter))
    except csv.Error as error:
        return {"status": "parse_failed", "error_type": type(error).__name__, "encoding": encoding}
    return {"encoding": encoding, "delimiter": delimiter, "replacement_characters": replaced, **profile_rows(rows, source, member)}


def cell_text(value: Any) -> str:
    """Render a cached workbook value as text for categorization only."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).upper()
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)


def profile_xlsx(data: bytes, source: str) -> dict[str, Any]:
    """Every worksheet of an Office Open XML workbook, cached values only."""
    book = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True, keep_links=False)
    try:
        sheets = {}
        for sheet in book.worksheets:
            rows = [[cell_text(value) for value in row] for row in sheet.iter_rows(values_only=True)]
            sheets[safe_text(sheet.title, 60) or "<withheld>"] = {"declared_dimensions": sheet.calculate_dimension(), **profile_rows(rows, source)}
        return {"status": "workbook", "sheets": sheets}
    finally:
        book.close()


def profile_xls(data: bytes, source: str) -> dict[str, Any]:
    """Every worksheet of a legacy Excel workbook."""
    book = xlrd.open_workbook(file_contents=data, on_demand=True)
    try:
        sheets = {}
        for index in range(book.nsheets):
            sheet = book.sheet_by_index(index)
            rows = [[cell_text(sheet.cell_value(r, c)) for c in range(sheet.ncols)] for r in range(sheet.nrows)]
            sheets[safe_text(sheet.name, 60) or "<withheld>"] = profile_rows(rows, source)
            book.unload_sheet(index)
        return {"status": "workbook", "sheets": sheets}
    finally:
        book.release_resources()


def profile_pdf(data: bytes) -> dict[str, Any]:
    """Page count, extractable text and pages that need visual review."""
    reader = PdfReader(io.BytesIO(data))
    chars = [len((page.extract_text() or "").strip()) for page in reader.pages]
    return {"status": "pdf", "pages": len(chars), "text_characters": sum(chars), "pages_without_text": sum(count == 0 for count in chars)}


def profile_member(name: str, data: bytes, source: str) -> dict[str, Any]:
    """Dispatch on content signature first, then the member's extension."""
    suffix = PurePosixPath(name).suffix.lower()
    head = data[:8].lstrip()
    if not data:
        return {"status": "empty_file"}
    try:
        if head.startswith(b"%PDF-"):
            return profile_pdf(data)
        if data[:1024].lstrip().lower().startswith((b"<!doctype html", b"<html")):
            return {"status": "html_content"}
        if head.startswith(bytes.fromhex("d0cf11e0a1b11ae1")):
            return profile_xls(data, source)
        if head.startswith(b"PK") and suffix in {".xlsx", ".xlsm"}:
            return profile_xlsx(data, source)
        if suffix in TEXT_SUFFIXES:
            return profile_text(data, source, name)
    except (ValueError, KeyError, OSError, EOFError, zipfile.BadZipFile, XLRDError, PdfReadError) as error:
        return {"status": "parse_failed", "error_type": type(error).__name__}
    return {"status": "listed_only", "extension": suffix}


class MemberReview:
    """Distinct members across every archive; identical bytes are profiled once."""

    def __init__(self) -> None:
        self.members: dict[str, dict[str, Any]] = {}

    def add(self, source: str, location: str, name: str, data: bytes) -> str:
        member_sha = sha256(data)
        if member_sha not in self.members:
            self.members[member_sha] = {"bytes": len(data), "profile": profile_member(name, data, source), "locations": []}
        self.members[member_sha]["locations"].append({"source_id": source, "location": location})
        return member_sha

    def walk(self, source: str, label: str, data: bytes, depth: int = 0) -> list[str]:
        """Profile the members of one archive; returns findings."""
        findings: list[str] = []
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            for info in sorted((i for i in archive.infolist() if not i.is_dir()), key=lambda i: i.filename):
                path = PurePosixPath(info.filename)
                if is_resource_fork(info.filename):
                    continue
                if path.is_absolute() or ".." in path.parts or info.flag_bits & 1 or info.file_size > MEMBER_LIMIT:
                    findings.append(f"member_refused:{safe_text(info.filename, 200) or '<withheld>'}")
                    continue
                content = archive.read(info)
                location = f"{label}!{info.filename}"
                if path.suffix.lower() == ".zip":
                    if depth == 0:
                        findings.extend(self.walk(source, location, content, depth + 1))
                    else:
                        findings.append(f"nested_beyond_one_level:{location}")
                    continue
                self.add(source, location, info.filename, content)
        return findings


def review_capture(root: Path, candidate: dict[str, Any], review: MemberReview) -> dict[str, Any]:
    """Verify one capture, then profile each verified artifact or its archive members."""
    result: dict[str, Any] = {"snapshot_id": candidate["snapshot_id"], "inventory_status": candidate["status"], "artifacts": []}
    receipt_path = safe_path(root, candidate["receipt"])
    if not receipt_path.is_file() or digest(receipt_path) != candidate["receipt_sha256"]:
        return {**result, "review_status": "receipt_drift"}
    for artifact in json.loads(receipt_path.read_text())["artifacts"]:
        path = receipt_path.parent / artifact["storage_path"]
        entry: dict[str, Any] = {"role": artifact["role"], "name": artifact["stored_file_name"], "sha256": artifact["sha256"]}
        if not path.is_file():
            entry["status"] = "artifact_not_local"
        elif path.stat().st_size != artifact["byte_count"] or digest(path) != artifact["sha256"]:
            entry["status"] = "artifact_mismatch"
        elif path.suffix.lower() == ".zip":
            entry.update({"status": "archive_walked", "findings": review.walk(candidate["source_id"], artifact["stored_file_name"], path.read_bytes())})
        elif artifact["role"] in {"api_page", "export_receipt", "layout", "dictionary"} and path.suffix.lower() == ".json":
            entry["status"] = "json_metadata_listed"
        else:
            entry.update(
                {"status": "member_profiled", "member_sha256": review.add(candidate["source_id"], artifact["stored_file_name"], path.name, path.read_bytes())}
            )
        result["artifacts"].append(entry)
    return {**result, "review_status": "reviewed"}


def summarize(members: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Counts by source and member status, attributing each distinct member to every source that holds it."""
    status: dict[str, Counter[str]] = {}
    for member in members.values():
        for source in sorted({location["source_id"] for location in member["locations"]}):
            status.setdefault(source, Counter())[member["profile"]["status"]] += 1
    return {source: dict(sorted(counts.items())) for source, counts in sorted(status.items())}


def main() -> int:
    """Run the member review from its real CLI."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sources", nargs="+", default=list(SOURCES))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    csv.field_size_limit(64 << 20)
    root = args.source_root.resolve()
    candidates = [c for c in json.loads(args.inventory.read_text())["candidates"] if c["source_id"] in set(args.sources)]
    review = MemberReview()
    captures: dict[str, list[dict[str, Any]]] = {}
    for index, candidate in enumerate(sorted(candidates, key=lambda c: (c["source_id"], c["snapshot_id"])), start=1):
        if candidate["source_id"] not in captures:
            log.info("source %s", candidate["source_id"])
        captures.setdefault(candidate["source_id"], []).append(review_capture(root, candidate, review))
        if index % 25 == 0:
            log.info("capture %d of %d", index, len(candidates))
    for source, entries in sorted(captures.items()):
        shas = {location_sha for location_sha, member in review.members.items() if any(loc["source_id"] == source for loc in member["locations"])}
        write_json(args.output / f"{source}.json", {"source_id": source, "captures": entries, "members": {sha: review.members[sha] for sha in sorted(shas)}})
    report = {
        "version": 1,
        "inventory_sha256": digest(args.inventory),
        "code_sha256": digest(Path(__file__)),
        "sources": sorted(captures),
        "captures": {source: dict(sorted(Counter(e["review_status"] for e in entries).items())) for source, entries in sorted(captures.items())},
        "artifact_status": dict(sorted(Counter(a["status"] for entries in captures.values() for e in entries for a in e["artifacts"]).items())),
        "distinct_members": len(review.members),
        "member_status_by_source": summarize(review.members),
        "model_eligible": False,
    }
    write_json(args.output / "report.json", report)
    sys.stdout.write(json.dumps({"distinct_members": report["distinct_members"], "artifact_status": report["artifact_status"]}) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
