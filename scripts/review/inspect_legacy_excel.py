"""Check every legacy XLS cell locally without exposing cell values or sheet names."""

from __future__ import annotations

import argparse
import io
import json
from collections import Counter
from importlib.metadata import version
from pathlib import Path
from typing import Any

import xlrd
from xlrd.biffh import XLRDError
from xlrd.compdoc import CompDocError
from xlrd.sheet import Sheet

from scripts.review.profile_sources import NUMERIC
from scripts.review.review_remaining import digest, safe_path, write_json

CELL_TYPES = {0: "empty", 1: "text", 2: "numeric_cached", 3: "date_serial", 4: "boolean", 5: "excel_error", 6: "styled_blank"}
SPECIAL_TEXT = {"(x)", "(d)", "(s)", "-", "--", "n/a", "na", "suppressed", "unreliable", "not available", "missing"}
NUMERIC_SENTINELS = {-666666666, -888888888, -999999999, -222222222, -333333333, -555555555}
OLE_SIGNATURE = bytes.fromhex("d0cf11e0a1b11ae1")


def text_counts(value: str, counts: Counter[str]) -> None:
    """Count only known structural categories; never retain arbitrary text."""
    stripped = value.strip()
    if stripped.casefold() in SPECIAL_TEXT:
        counts["special_text_token"] += 1
    if stripped.isdigit() and len(stripped) > 1 and stripped.startswith("0"):
        counts["leading_zero_digit_text"] += 1
    if NUMERIC.fullmatch(stripped):
        counts["numeric_as_text"] += 1
    if stripped != value:
        counts["outer_whitespace_text"] += 1


def sheet_profile(sheet: Sheet, index: int, book: xlrd.book.Book) -> dict[str, Any]:
    """Traverse all explicit cells and account for every implicit empty position."""
    columns: list[Counter[str]] = [Counter() for _ in range(sheet.ncols)]
    categories: Counter[str] = Counter()
    excel_errors: Counter[str] = Counter()
    error_positions: list[dict[str, Any]] = []
    for row in range(sheet.nrows):
        for col, cell in enumerate(sheet.row(row)):
            columns[col][CELL_TYPES[cell.ctype]] += 1
            if cell.ctype == xlrd.XL_CELL_TEXT:
                text_counts(str(cell.value), categories)
            elif cell.ctype == xlrd.XL_CELL_ERROR:
                error_name = xlrd.error_text_from_code.get(int(cell.value), "unknown_error_code")
                excel_errors[error_name] += 1
                error_positions.append({"row": row + 1, "column": col + 1, "error": error_name})
            elif cell.ctype == xlrd.XL_CELL_NUMBER and isinstance(cell.value, (int, float)):
                categories["zero_numeric_cached"] += cell.value == 0
                categories["candidate_numeric_sentinel"] += cell.value in NUMERIC_SENTINELS
                fmt = book.format_map.get(book.xf_list[sheet.cell_xf_index(row, col)].format_key)
                if fmt is not None and len(fmt.format_str) > 1 and set(fmt.format_str) == {"0"}:
                    categories["zero_padded_numeric_display"] += 1
    for counts in columns:
        counts["empty"] += sheet.nrows - sum(counts.values())
    total: Counter[str] = Counter()
    for counts in columns:
        total.update(counts)
    if sum(total.values()) != sheet.nrows * sheet.ncols:
        raise ValueError("cell_extent_does_not_reconcile")
    return {
        "sheet_ordinal": index + 1,
        "rows": sheet.nrows,
        "columns": sheet.ncols,
        "cell_types": dict(total),
        "structural_categories": dict(categories),
        "cached_excel_errors": dict(excel_errors),
        "cached_error_positions": error_positions,
        "column_cell_types": [{"column_ordinal": i + 1, "types": dict(counts)} for i, counts in enumerate(columns)],
    }


def workbook_profile(path: Path) -> dict[str, Any]:
    """Parse all worksheets as data; macros and formulas are never executed."""
    with path.open("rb") as handle:
        if handle.read(8) != OLE_SIGNATURE:
            return {"status": "unsupported_signature"}
    captured_warnings = io.StringIO()
    book = xlrd.open_workbook(str(path), on_demand=True, formatting_info=True, ragged_rows=True, logfile=captured_warnings)
    try:
        sheets = []
        for index in range(book.nsheets):
            sheet = book.sheet_by_index(index)
            sheets.append(sheet_profile(sheet, index, book))
            book.unload_sheet(index)
        return {
            "status": "legacy_excel_all_cells_checked",
            "date_system": book.datemode,
            "biff_version": book.biff_version,
            "sheets": sheets,
            "reader_warning_lines": len(captured_warnings.getvalue().splitlines()),
            "limits": "Every cell position accounted for. Cached values only; no formula recalculation, table-boundary or semantic/eligibility approval.",
        }
    finally:
        book.release_resources()


def inspect(profile_root: Path, source_root: Path, output: Path) -> dict[str, Any]:
    """Recheck all selected immutable XLS artifacts and retain failures explicitly."""
    results: list[dict[str, Any]] = []
    cache: dict[str, dict[str, Any]] = {}
    for profile_file in sorted(profile_root.glob("*.json")):
        source = json.loads(profile_file.read_text())
        for candidate in source["candidates"]:
            for artifact in candidate.get("artifacts", []):
                if Path(artifact["path"]).suffix.lower() != ".xls":
                    continue
                path = safe_path(source_root, artifact["path"])
                if not path.is_file():
                    result = {"status": "artifact_missing"}
                elif artifact.get("integrity") != "verified" or digest(path) != artifact["sha256"]:
                    result = {"status": "artifact_drifted"}
                elif artifact["sha256"] in cache:
                    result = cache[artifact["sha256"]]
                else:
                    try:
                        result = workbook_profile(path)
                    except (OSError, ValueError, IndexError, KeyError, XLRDError, CompDocError) as error:
                        result = {"status": "legacy_excel_parse_failed", "error_type": type(error).__name__}
                    cache[artifact["sha256"]] = result
                results.append({"source_id": source["source_id"], "path": artifact["path"], "sha256": artifact["sha256"], "result": result})
    report = {
        "code_sha256": digest(Path(__file__)),
        "xlrd_version": version("xlrd"),
        "results": results,
        "unique_parsed_hashes": len(cache),
        "statuses": dict(Counter(r["result"]["status"] for r in results)),
        "model_eligible": False,
        "limits": "Local XLS artifacts from frozen capture profiles only, including repeated captures; no archive-member or live S3 check.",
    }
    write_json(output, report)
    return report


def main() -> None:
    """Run complete-cell legacy workbook structure checks from the real CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profiles", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    inspect(args.profiles, args.source_root.resolve(), args.output)


if __name__ == "__main__":
    main()
