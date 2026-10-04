"""Fully profile collected HUD crosswalk and WONDER CSVs against frozen receipts."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from scripts.review.profile_sources import ColumnStats, describe
from scripts.review.review_remaining import digest, safe_path, write_json


def profile_source(root: Path, inventory: Path, source: str) -> dict[str, Any]:
    """Inspect every row, preserving publisher/database periods as separate partitions."""
    candidates = [c for c in json.loads(inventory.read_text())["candidates"] if c["source_id"] == source]
    files: list[dict[str, Any]] = []
    headers: Counter[tuple[str, ...]] = Counter()
    columns: dict[str, ColumnStats] = defaultdict(ColumnStats)
    for candidate in sorted(candidates, key=lambda c: c["receipt"]):
        receipt_path = safe_path(root, candidate["receipt"])
        if digest(receipt_path) != candidate["receipt_sha256"]:
            raise ValueError("receipt changed from pinned inventory")
        receipt = json.loads(receipt_path.read_text())
        artifacts = [a for a in receipt["artifacts"] if a["role"] == "data" and a["storage_path"].endswith(".csv")]
        if len(artifacts) != 1:
            raise ValueError("expected exactly one reviewed data CSV")
        artifact = artifacts[0]
        path = receipt_path.parent / artifact["storage_path"]
        if digest(path) != artifact["sha256"]:
            raise ValueError("CSV differs from pinned receipt")
        seen: set[tuple[str, ...]] = set()
        rows = duplicates = blank_identifiers = 0
        years: Counter[str] = Counter()
        geos: set[str] = set()
        special: Counter[str] = Counter()
        ratio_bounds = 0
        zip_sums: dict[str, float] = defaultdict(float)
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            header = tuple(reader.fieldnames or [])
            headers[header] += 1
            for row in reader:
                if None in row or any(v is None for v in row.values()):
                    raise ValueError("ragged CSV row")
                rows += 1
                for name, value in row.items():
                    columns[name].add(value)
                if source == "HUD":
                    key = (row["zip"], row["geoid"])
                    geo = row["geoid"]
                    year = receipt["measurement_periods"][0]["start_date"][:4]
                    for name in ("res_ratio", "bus_ratio", "oth_ratio", "tot_ratio"):
                        ratio_bounds += not 0 <= float(row[name]) <= 1
                    zip_sums[row["zip"]] += float(row["tot_ratio"])
                else:
                    key = (row["County Code"].strip(), row["Year"].strip())
                    geo = row["County Code"]
                    year = row["Year"]
                    for name in ("Deaths", "Population", "Crude Rate"):
                        if row[name] in {"Suppressed", "Unreliable", "Not Applicable", "Missing", ""}:
                            special[f"{name}:{row[name] or 'blank'}"] += 1
                duplicates += key in seen
                seen.add(key)
                blank_identifiers += any(not v for v in key)
                years[year] += 1
                geos.add(geo)
        files.append(
            {
                "receipt": candidate["receipt"],
                "sha256": artifact["sha256"],
                "rows": rows,
                "duplicate_grain_rows": duplicates,
                "blank_key_rows": blank_identifiers,
                "rows_by_year": dict(sorted(years.items())),
                "distinct_geographies": len(geos),
                "ratio_out_of_bounds_cells": ratio_bounds,
                "zip_total_ratio_sum_outside_0_999_to_1_001": sum(not 0.999 <= v <= 1.001 for v in zip_sums.values()),
                "flags": dict(special),
                "periods": receipt["measurement_periods"],
            }
        )
    identifiers = {"zip", "geoid", "County Code", "Year Code"}
    return {
        "source": source,
        "code_sha256": digest(Path(__file__)),
        "numeric_classifier_sha256": digest(Path(__file__).with_name("profile_sources.py")),
        "files": files,
        "rows": sum(f["rows"] for f in files),
        "duplicate_grain_rows_within_files": sum(f["duplicate_grain_rows"] for f in files),
        "header_variants": [{"columns": list(h), "files": n} for h, n in sorted(headers.items())],
        "columns": {k: describe(v, k in identifiers) for k, v in sorted(columns.items())},
        "model_eligible": False,
        "limits": (
            "Local CSV integrity and all rows checked. WONDER overlapping database exports are separate series; "
            "uniqueness across databases is not asserted. Definitions and geography require publisher/reference reconciliation."
        ),
    }


def main() -> None:
    """Run the complete two-source profile with value examples removed."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    for source in ("HUD", "WONDER"):
        result = profile_source(args.source_root.resolve(), args.inventory, source)
        # Names/tokens can include arbitrary publisher values; retain counts without examples.
        for stats in result["columns"].values():
            stats.pop("top_tokens", None)
        write_json(args.output / f"{source}.json", result)


if __name__ == "__main__":
    main()
