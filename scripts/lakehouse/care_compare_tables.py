"""Generate the bronze table map entries for the Care Compare tables other than HAI, one table per file family.

Run from the repository root with read-only S3 access; it rewrites only the `care_compare` group in the table map:

    .venv/bin/python -m scripts.lakehouse.care_compare_tables

Care Compare stores each table under two kinds of dataset ID: legacy captures (`cms_legacy_<family>`, plain file
names) and current captures (`cms_<socrata id>`, `<id>_<date>_<family>.csv` or bare `<id>.csv`). Each dataset ID is
assigned to one family by normalizing its file names: the ID and date prefix, release dates, fiscal years, reporting
periods and edition words are not part of the family [155]. Every family becomes one table selected by its dataset IDs;
datasets an existing table already selects and excluded datasets get none [156] [157]. Failure modes 150 to 159:
data/lakehouse_planning/bronze_gaps_20261003/failure_modes.md.
"""

from __future__ import annotations

import json
import os
import re
import sys
from collections.abc import Iterable
from typing import Any

GROUP = "care_compare"
COLLECTION = "cms/hospitals"
# State-agency e-mail and phone contacts stay out of bronze until a privacy check clears them [157].
EXCLUDED = frozenset({"cms_legacy_hospitals_casper_aspen_contacts"})
ID_PREFIX = re.compile(r"^[a-z0-9]{4}-[a-z0-9]{4}(?:_\d{4}-\d{2}-\d{2})?(?:_|$)", re.IGNORECASE)
MONTHS = "january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec"
# Applied in order to the lower-case, underscore-separated stem; each removes a release marker, never a measure word.
MARKERS = (
    re.compile(r"^fy_?\d{4}_"),  # fiscal-year prefix: FY2019_..., FY_2021_...
    re.compile(rf"_(?:{MONTHS})_\d{{4}}(?:_\d{{1,2}}_\d{{1,2}}_\d{{2,4}})?(?=_|$)"),  # month and year, with a stamp date
    re.compile(r"_\d{4}_\d{2}_\d{2}(?=_|$)"),  # ISO date: _2018_11_30
    re.compile(r"_\d{1,2}_\d{1,2}_(?:\d{4}|\d{2})(?=_|$)"),  # US date: _11_09_2018, _11_29_18
    re.compile(r"_pr\d{2}q\d_\d{2}q\d$"),  # reporting period: _pr17q3_18q2
    re.compile(r"_(?:revised|updated|production(?:_file)?)(?=_|$)"),  # edition words
    re.compile(r"(?<=^cjr)_py\d+(?=_)"),  # CJR performance year
)
FORMAT_BY_SUFFIX = {"csv": "csv", "xlsx": "sheet_rows", "xls": "sheet_rows"}
# Datasets whose files the preflight found to be UTF-16 with tab separators get a table of their own [163].
UTF16_TAB = {"suffix": "utf16", "encodings": ["utf-16"], "delimiter": "\t"}
OVERRIDES: dict[str, dict[str, Any]] = {
    "cms_legacy_fy2017_percent_change_in_medicare_payments_2018_12_03": UTF16_TAB,
    "cms_legacy_fy2018_percent_change_in_medicare_payments_2019_11_22": UTF16_TAB,
    "cms_legacy_u625_zae7": UTF16_TAB,
}


# The stored Care Compare dictionaries, as the HAI tables link them; they give names and published types only [108] [165].
DICTIONARY_TABLE = "cms_hospitals_dictionaries"
DICTIONARY_FILES = "(?i)Data_?Dictionary|DataDictionary"


class FamilyError(ValueError):
    """A dataset cannot be assigned to exactly one family and format."""


def stem(file_name: str) -> str:
    """Return a file name without its folder or extension."""
    return file_name.rsplit("/", 1)[-1].rsplit(".", 1)[0]


def family(file_name: str) -> str | None:
    """Return a file's family, or None when its name is only a dataset ID [155]."""
    text = re.sub(r"[^a-z0-9]+", "_", ID_PREFIX.sub("", stem(file_name)).lower()).strip("_")
    for marker in MARKERS:
        text = marker.sub("", text)
    return text.strip("_") or None


def section_pattern(name: str) -> str:
    """Return a section pattern for a family: its words in order, any separators between them, release text after [164]."""
    words = [re.escape(word) for word in name.split("_") if word]
    return r"(?i)^(?:FY_?\d{4}[^A-Za-z0-9]*)?" + r"[^A-Za-z0-9]*".join(words) + r"(?:[^A-Za-z0-9].*)?$"


def assign(datasets: dict[str, list[str]], existing: Iterable[str], excluded: Iterable[str] = EXCLUDED) -> dict[str, tuple[str, str]]:
    """Map each dataset ID to (family, format); refuse a dataset whose files give two families or formats.

    Datasets stored only under bare ID names (`yv7e-xc69.csv`) take the family that named files give the same ID, and
    keep the ID itself when no named file exists.
    """
    skip = set(existing) | set(excluded)
    by_id: dict[str, set[str]] = {}
    for names in datasets.values():
        for file_name in names:
            match = ID_PREFIX.match(stem(file_name))
            name = family(file_name)
            if match and name:
                by_id.setdefault(match.group(0)[:9].lower(), set()).add(name)
    assigned: dict[str, tuple[str, str]] = {}
    for dataset_id in sorted(datasets):
        if dataset_id in skip:
            continue
        names = datasets[dataset_id]
        families = {name for name in (family(file_name) for file_name in names) if name}
        if not families:
            ids = {match.group(0)[:9].lower() for match in (ID_PREFIX.match(stem(file_name)) for file_name in names) if match}
            families = set().union(*(by_id.get(socrata_id, set()) for socrata_id in ids)) if ids else set()
        formats = {FORMAT_BY_SUFFIX.get(file_name.rsplit(".", 1)[-1].lower(), "") for file_name in names}
        if len(families) > 1 or len(formats) != 1 or "" in formats:
            raise FamilyError(f"{dataset_id}: its files give {sorted(families)} families and {sorted(formats)} formats")
        # A dataset stored only under a bare ID that no named file explains keeps its ID as the family.
        name = families.pop() if families else re.sub(r"^cms_(?:legacy_)?(?:pdc_s3_hos_data_)?", "", dataset_id)
        assigned[dataset_id] = (name, formats.pop())
    return assigned


def table_entries(
    assigned: dict[str, tuple[str, str]], datasets: dict[str, list[str]], overrides: dict[str, dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    """Return one table entry per family, format and reading override, sorted, selecting its dataset IDs."""
    overrides = OVERRIDES if overrides is None else overrides
    tables: dict[str, dict[str, Any]] = {}
    for dataset_id, (name, file_format) in sorted(assigned.items()):
        override = overrides.get(dataset_id, {})
        table = f"cms_cc_{name}" if file_format == "csv" else f"cms_cc_{name}_{file_format}"
        if override:
            table = f"{table}_{override['suffix']}"
        entry = tables.setdefault(table, {"table": table, "group": GROUP, "collection": COLLECTION, "dataset_ids": [], "format": file_format})
        if file_format == "csv":
            # Lossless options for variants that differ by release; files without them read as before [160] [161].
            entry.update(unnamed_headers=True, end_of_file_marker=True, trailing_blank_lines=True)
            entry.update({key: value for key, value in override.items() if key != "suffix"} or {"encodings": ["utf-8", "cp1252"], "byte_order_mark": True})
            entry["dictionary"] = {"table": DICTIONARY_TABLE, "file_pattern": DICTIONARY_FILES, "section_pattern": section_pattern(name)}
        entry["dataset_ids"].append(dataset_id)
    return [tables[name] for name in sorted(tables)]


def main() -> int:
    """Discover the unselected Care Compare datasets read-only and rewrite the care_compare group in the table map."""
    from scripts.lakehouse import bronze
    from scripts.lakehouse.catalog import deployment

    settings = deployment()
    os.environ.setdefault("AWS_PROFILE", settings["aws_profile"])
    os.environ.setdefault("AWS_REGION", settings["aws_region"])
    config = bronze.load_table_map()
    others = [table["table"] for table in config["tables"] if table["collection"] == COLLECTION and table.get("group") != GROUP]
    inputs, unselected = bronze.discover(bronze.S3Storage(), settings["data_bucket_name"], config, others, bronze.load_retired())
    # Only data tables claim a dataset; document tables read other roles from the same datasets.
    data_tables = {table["table"] for table in config["tables"] if table["roles"] == ["data"]}
    existing = {item["dataset_id"] for item in inputs if item["table"] in data_tables}
    datasets: dict[str, list[str]] = {}
    for entry in unselected:
        if entry["collection"] == COLLECTION and not entry.get("identical_copy") and not entry.get("retired"):
            datasets.setdefault(entry["dataset_id"], []).append(entry["file_name"])
    try:
        shared = sorted(set(datasets) & existing)
        if shared:
            raise FamilyError(f"{shared[0]}: an existing table selects part of this dataset ({len(shared)} such datasets)")
        entries = table_entries(assign(datasets, existing), datasets)
    except FamilyError as error:
        sys.stderr.write(f"care compare tables: {error}\n")
        return 1
    raw = json.loads(bronze.TABLE_MAP.read_text())
    raw["tables"] = [table for table in raw["tables"] if table.get("group") != GROUP] + entries
    bronze.TABLE_MAP.write_text(json.dumps(raw, indent=2) + "\n")
    excluded = sorted(dataset_id for dataset_id in datasets if dataset_id in EXCLUDED)
    sys.stdout.write(f"{len(entries)} care_compare tables from {len(datasets) - len(excluded)} datasets; excluded: {', '.join(excluded) or 'none'}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
