"""Summarize the full-row member review of the 15 sampled sources into one deterministic per-source report.

Reads the per-source JSON written by ``profile_sampled_sources`` and counts rows, header variants, column
categories, identifier shapes and grain-key duplicates. Like its input, the summary holds counts and header names,
never cell values.

Usage::

    python -m scripts.review.summarize_sampled_sources --profiles sampled_run1 --output sampled_summary.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from scripts.review.review_remaining import digest, write_json

LIST_LIMIT = 20


def tables(profile: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Every table inside a member profile: the profile itself, or each worksheet of a workbook."""
    if profile.get("status") == "table":
        return [("", profile)]
    if profile.get("status") == "workbook":
        return [(name, sheet) for name, sheet in sorted(profile["sheets"].items()) if sheet.get("status") == "table"]
    return []


def summarize_source(document: dict[str, Any]) -> dict[str, Any]:
    """Counts for one source across its distinct members."""
    member_status: Counter[str] = Counter()
    headers: Counter[tuple[str, ...]] = Counter()
    column_tables: Counter[str] = Counter()
    category_columns: dict[str, Counter[str]] = {"special_token": Counter(), "numeric_sentinel": Counter(), "threshold_or_topcode": Counter()}
    all_blank: Counter[str] = Counter()
    identifiers: dict[str, Counter[str]] = {}
    keys: list[dict[str, Any]] = []
    totals: Counter[str] = Counter()
    for member_sha, member in sorted(document["members"].items()):
        profile = member["profile"]
        member_status[profile.get("status", "unknown")] += 1
        totals["replacement_characters"] += profile.get("replacement_characters", 0) or 0
        for sheet, table in tables(profile):
            totals["tables"] += 1
            totals["data_rows"] += table.get("data_rows_at_modal_width", 0)
            totals["other_width_rows"] += table.get("other_width_rows", 0)
            totals["title_rows_above_header"] += table.get("title_rows_above_header", 0)
            totals["excel_error_cells"] += table.get("excel_error_cells", 0)
            totals["headerless_tables"] += table.get("header_row_index") is None
            names = tuple(column["name"] for column in table["columns"])
            headers[names] += 1
            for column in table["columns"]:
                counts = column["counts"]
                column_tables[column["name"]] += 1
                for category, counter in category_columns.items():
                    if counts.get(category):
                        counter[column["name"]] += counts[category]
                if counts.get("observed") and counts.get("blank") == counts.get("observed"):
                    all_blank[column["name"]] += 1
                if "identifier" in column:
                    shape = identifiers.setdefault(column["name"], Counter())
                    ident = column["identifier"]
                    for length, count in ident["lengths"].items():
                        shape[f"length_{length}"] += count
                    for field in ("leading_zero_values", "values_with_letters", "float_suffix_values", "rows_repeating_a_value"):
                        shape[field] += ident[field]
            for key in table.get("candidate_keys", []):
                keys.append({"member": member_sha[:12], "sheet": sheet, **key})
    variable = sorted(name for name, count in column_tables.items() if count != totals["tables"])
    key_summary: dict[str, Any] = {}
    for key in keys:
        entry = key_summary.setdefault("+".join(key["key"]), Counter())
        entry[key["status"]] += 1
        if key["status"] == "tested":
            entry["rows"] += key["rows"]
            entry["duplicate_rows"] += key["duplicate_rows"]
            entry["tables_with_duplicates"] += key["duplicate_rows"] > 0
    return {
        "captures": len(document["captures"]),
        "capture_status": dict(sorted(Counter(c["review_status"] for c in document["captures"]).items())),
        "artifact_status": dict(sorted(Counter(a["status"] for c in document["captures"] for a in c["artifacts"]).items())),
        "distinct_members": len(document["members"]),
        "member_status": dict(sorted(member_status.items())),
        "totals": dict(sorted(totals.items())),
        "header_variants": len(headers),
        "distinct_column_names": len(column_tables),
        "columns_not_in_every_table": {"count": len(variable), "names": variable[:LIST_LIMIT]},
        "columns_with": {category: {"count": len(counter), "top": dict(counter.most_common(LIST_LIMIT))} for category, counter in category_columns.items()},
        "columns_entirely_blank_in_some_table": {"count": len(all_blank), "names": sorted(all_blank)[:LIST_LIMIT]},
        "identifier_shapes": {name: dict(sorted(shape.items())) for name, shape in sorted(identifiers.items())},
        "grain_keys": {name: dict(sorted(counter.items())) for name, counter in sorted(key_summary.items())},
    }


def main() -> int:
    """Run the summary from its real CLI."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profiles", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = json.loads((args.profiles / "report.json").read_text())
    sources = {path.stem: summarize_source(json.loads(path.read_text())) for path in sorted(args.profiles.glob("*.json")) if path.name != "report.json"}
    write_json(
        args.output,
        {
            "version": 1,
            "code_sha256": digest(Path(__file__)),
            "profiles_report_sha256": digest(args.profiles / "report.json"),
            "member_review_code_sha256": report["code_sha256"],
            "inventory_sha256": report["inventory_sha256"],
            "sources": sources,
            "model_eligible": False,
        },
    )
    sys.stdout.write(json.dumps({s: v["totals"].get("data_rows", 0) for s, v in sources.items()}) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
