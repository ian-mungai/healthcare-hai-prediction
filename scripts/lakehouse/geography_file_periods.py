"""Generate the vintages and periods of the county-context files that staging reads (failure modes 403, 410, 416 and 427).

Run from the repository root with read-only S3 access and the local acquisition records:

    .venv/bin/python -m scripts.lakehouse.geography_file_periods           # rewrite the seed
    .venv/bin/python -m scripts.lakehouse.geography_file_periods --check   # rebuild and compare with the committed seed

A HUD ZIP-to-county file takes the one publisher-stated USPS quarter its capture receipt records. A hospital service
area file takes the one catalog coverage its acquisition job plan records. RUCC, RUCA and county adjacency captures
record no period, so each of their files takes the vintage reviewed below for its exact published file name; the
vintage is the publisher's reference year, not a publication date. An ACS file takes the year in its publisher file name
(ACSDP5Y2023..., acsdt5y2023-...), with the five-year period ending that year. An SVI file takes the edition year its
capture receipt's release label names, as does a PLACES file; a WONDER file takes its database name and publisher-stated period
from its receipt. SAIPE, SAHIE and MMD files take the year in their publisher file names (est23all.txt,
sahie_2023.csv, mmd_ffs_county_c258_02_prevalence_2023.csv); the one MMD file named mmd_data.csv takes its reviewed year.
A BLS capture takes its capture date, the revision vintage of the series it holds, and an HPSA or MUA capture its capture
date, the day its statuses are as of. A loaded file with no period or vintage, or with two, stops the run. Failure modes:
plans/group_c_20261006/failure_modes_c1.md to failure_modes_c5.md.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SEED = REPO_ROOT / "dbt/seeds/geography_file_periods.csv"
DATASETS = REPO_ROOT / "data/datasets"
QUARTER_RECEIPTS = "historical_acquisition/hud_usps_crosswalk/*/batches/*/capture/receipt.json"
COVERAGE_JOBS = "acquisition_batches/*/jobs/HSA_file_*/job.json"
QUARTER_TABLE = "hud_zip_county"
COVERAGE_TABLE = "cms_hsa_csv"
EDITION_RECEIPTS = ("*/*/jobs/SVI_*/captures/SVI/*/receipt.json", "*/*/jobs/PLACES_*/captures/PLACES/*/receipt.json")
EDITION_TABLES = ("svi", "places")
DATABASE_RECEIPTS = "historical_acquisition/wonder_county_mortality/batches/*/capture/receipt.json"
DATABASE_TABLE = "wonder_county_mortality"
EDITION = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")
ACS_FILE = re.compile(r"ACS(?:DP|ST|DT)5Y(\d{4})\.(?:DP\d{2}|[BCS]\d{4,5})-Data\.csv|acsdt5y(\d{4})-[bc]\d{5}\.dat")
ACS_EXPORTS = ("dp02", "dp03", "dp04", "dp05", "s0101", "s0601", "s1701", "s2503", "s2701", "b16005", "b19013", "b25070", "b25091", "b26001", "c16001")
ACS_SUMMARIES = ("b16005", "b19013", "b25070", "b25091", "b26001", "c16001")
ACS_TABLES = (*(f"acs_{table}" for table in ACS_EXPORTS), *(f"acs_summary_{table}" for table in ACS_SUMMARIES))
SAIPE_FILE = re.compile(r"est(\d{2})(?:all|-[a-z]{2})\.(?:txt|dat)")
SAHIE_FILE = re.compile(r"sahie[-_](\d{4})\.csv")
MMD_FILE = re.compile(r"mmd_ffs_(?:county|state)_(?:c258_\d{2}|ami)_prevalence_(\d{4})\.csv")
CAPTURE_DATE = re.compile(r"__(\d{4})(\d{2})(\d{2})T\d{6}Z__")
NAMED_YEAR_TABLES = ("saipe_text_lines", "sahie", "cms_mmd_csv")
NAMED_YEAR_FILES = {"saipe_text_lines": SAIPE_FILE, "sahie": SAHIE_FILE, "cms_mmd_csv": MMD_FILE}
CAPTURE_TABLES = ("bls_laus", "hrsa_hpsa_detail", "hrsa_mua_detail")
TEMPORAL = re.compile(r'"catalog_temporal": "(\d{4}-\d{2}-\d{2})/(\d{4}-\d{2}-\d{2})"')
# Reviewed against each file's published name and, where present, its year-bearing headers (RUCC_2013,
# Primary RUCA Code 2010, countyfips20). Two vintages in one workbook are listed together.
REVIEWED_VINTAGES: dict[tuple[str, str], str] = {
    ("county_adjacency", "county_adjacency2023.txt"): "2023",
    # Its only rows are 2023 data (by inspection); the name carries no year.
    ("cms_mmd_csv", "mmd_data.csv"): "2023",
    ("county_adjacency", "county_adjacency2024.txt"): "2024",
    ("county_adjacency", "county_adjacency2025.txt"): "2025",
    ("county_adjacency", "county_adjacency2026.txt"): "2026",
    ("county_adjacency_2010_text_lines", "county_adjacency2010.txt"): "2010",
    ("rucc", "2023-rural-urban-continuum-codes.csv"): "2023",
    ("rucc_sheet_rows", "1974-rural-urban-continuum-codes.xls"): "1974",
    ("rucc_sheet_rows", "1983-and-1993-rural-urban-continuum-codes.xls"): "1983 1993",
    ("rucc_sheet_rows", "1993-rural-urban-continuum-codes.xls"): "1993",
    ("rucc_sheet_rows", "2003-rural-urban-continuum-codes.xls"): "2003",
    ("rucc_sheet_rows", "2003-rural-urban-continuum-codes-codes-for-puerto-rico.xls"): "2003",
    ("rucc_sheet_rows", "2013-rural-urban-continuum-codes.xls"): "2013",
    ("rucc_sheet_rows", "2023-rural-urban-continuum-codes.xlsx"): "2023",
    ("ruca_tracts_2020", "2020-rural-urban-commuting-area-codes-census-tracts.csv"): "2020",
    ("ruca_zip_2020", "2020-rural-urban-commuting-area-codes-zip-codes.csv"): "2020",
    ("ruca_zip_2010", "2010-rural-urban-commuting-area-codes-zip-code-file.csv"): "2010",
    ("ruca_sheet_rows", "1990-rural-urban-commuting-area-codes.xls"): "1990",
    ("ruca_sheet_rows", "2000-rural-urban-commuting-area-codes.xls"): "2000",
    ("ruca_sheet_rows", "2010-rural-urban-commuting-area-codes-revised-732019.xlsx"): "2010",
    ("ruca_sheet_rows", "2010-rural-urban-commuting-area-codes-zip-code-file.xlsx"): "2010",
    ("ruca_sheet_rows", "2020-rural-urban-commuting-area-codes-census-tracts.xlsx"): "2020",
    ("ruca_sheet_rows", "2020-rural-urban-commuting-area-codes-zip-codes.xlsx"): "2020",
    ("cms_geographic_variation_csv", "2014-2024_Original_Medicare_Geographic_Variation_Public_Use_File.csv"): "2014-2024",
}
# Each table once: an MMD file is dated by its name or, for mmd_data.csv, by its reviewed vintage.
TABLES = tuple(
    dict.fromkeys(
        (
            QUARTER_TABLE,
            COVERAGE_TABLE,
            *EDITION_TABLES,
            DATABASE_TABLE,
            *CAPTURE_TABLES,
            *NAMED_YEAR_TABLES,
            *ACS_TABLES,
            *sorted({table for table, _ in REVIEWED_VINTAGES}),
        )
    )
)
COLUMNS = ("member_sha256", "bronze_table", "release_id", "file_name", "vintage", "period_start", "period_end", "period_basis")


class PeriodError(ValueError):
    """A loaded geography file has no recorded period or vintage, or two."""


def receipt_quarter(receipt: Mapping[str, object]) -> tuple[str, str, str] | None:
    """Return a HUD receipt's quarter label and publisher-stated period, or None when it states none."""
    listed = receipt.get("measurement_periods")
    stated = {
        (str(period["start_date"]), str(period["end_date"]))
        for period in (listed if isinstance(listed, list) else [])
        if isinstance(period, dict) and period.get("source_basis") == "publisher_stated" and period.get("start_date") and period.get("end_date")
    }
    if not stated:
        return None
    if len(stated) > 1:
        raise PeriodError(f"release {receipt.get('snapshot_id')} records more than one publisher period")
    start, end = stated.pop()
    release = receipt.get("release")
    label = str(release.get("publisher_release_label") or "") if isinstance(release, dict) else ""
    if not label:
        raise PeriodError(f"release {receipt.get('snapshot_id')} records a period but no quarter label")
    return label, start, end


def receipt_quarters(root: Path = DATASETS) -> dict[str, tuple[str, str, str]]:
    """Return each HUD capture's quarter label and period from its receipt, keyed by release ID."""
    quarters: dict[str, tuple[str, str, str]] = {}
    for path in sorted(root.glob(QUARTER_RECEIPTS)):
        receipt = json.loads(path.read_text())
        found = receipt_quarter(receipt)
        if found is not None and quarters.setdefault(str(receipt["snapshot_id"]), found) != found:
            raise PeriodError(f"release {receipt['snapshot_id']} has two recorded quarters")
    return quarters


def receipt_editions(root: Path = DATASETS) -> dict[str, str]:
    """Return each SVI or PLACES capture's edition year from its receipt's release label, keyed by release ID [427] [453]."""
    editions: dict[str, str] = {}
    for path in sorted(path for pattern in EDITION_RECEIPTS for path in root.glob(pattern)):
        receipt = json.loads(path.read_text())
        release = receipt.get("release")
        label = str(release.get("publisher_release_label") or "") if isinstance(release, dict) else ""
        years = set(EDITION.findall(label))
        if len(years) > 1:
            raise PeriodError(f"release {receipt.get('snapshot_id')} names more than one edition year")
        if years:
            editions[str(receipt["snapshot_id"])] = years.pop()
    return editions


def receipt_databases(root: Path = DATASETS) -> dict[str, tuple[str, str, str]]:
    """Return each WONDER capture's database (its publisher release label) and publisher-stated period, keyed by release ID [453]."""
    databases: dict[str, tuple[str, str, str]] = {}
    for path in sorted(root.glob(DATABASE_RECEIPTS)):
        receipt = json.loads(path.read_text())
        found = receipt_quarter(receipt)
        if found is not None:
            databases[str(receipt["snapshot_id"])] = found
    return databases


def job_coverage(root: Path = DATASETS) -> dict[str, tuple[str, str]]:
    """Return each hospital service area capture's catalog coverage from its job plan, keyed by release ID."""
    coverage: dict[str, tuple[str, str]] = {}
    for job in sorted(root.glob(COVERAGE_JOBS)):
        plan = json.loads(job.read_text())["plan"]
        labels = [period.get("label") or "" for period in plan.get("measurement_periods", [])]
        found = {match.groups() for label in labels if (match := TEMPORAL.search(label))}
        if not found:
            continue
        if len(found) > 1:
            raise PeriodError(f"job {job.parent.name} records more than one catalog coverage")
        start, end = found.pop()
        for capture in sorted((job.parent / "captures/HSA").glob("*")):
            if coverage.setdefault(capture.name, (start, end)) != (start, end):
                raise PeriodError(f"release {capture.name} has two catalog coverages")
    return coverage


def file_row(
    item: Mapping[str, str],
    quarters: Mapping[str, tuple[str, str, str]],
    coverage: Mapping[str, tuple[str, str]],
    editions: Mapping[str, str],
    databases: Mapping[str, tuple[str, str, str]],
) -> dict[str, str]:
    """Return one loaded file's seed row; a file without a period or vintage stops the run [403] [410] [416] [427]."""
    table, release, name = item["table"], item["release_id"], item["file_name"]
    row = {"member_sha256": item["sha256"], "bronze_table": table, "release_id": release, "file_name": name}
    if table == QUARTER_TABLE:
        if release not in quarters:
            raise PeriodError(f"{table} file {name} (release {release}) has no recorded quarter")
        label, start, end = quarters[release]
        return row | {"vintage": label, "period_start": start, "period_end": end, "period_basis": "receipt_publisher_quarter"}
    if table == COVERAGE_TABLE:
        if release not in coverage:
            raise PeriodError(f"{table} file {name} (release {release}) has no recorded catalog coverage")
        start, end = coverage[release]
        return row | {"vintage": start[:4], "period_start": start, "period_end": end, "period_basis": "job_catalog_coverage"}
    if table == DATABASE_TABLE:
        if release not in databases:
            raise PeriodError(f"{table} file {name} (release {release}) has no recorded database and period")
        database, start, end = databases[release]
        return row | {"vintage": database, "period_start": start, "period_end": end, "period_basis": "receipt_database_period"}
    if table in EDITION_TABLES:
        if release not in editions:
            raise PeriodError(f"{table} file {name} (release {release}) has no recorded edition")
        return row | {"vintage": editions[release], "period_start": "", "period_end": "", "period_basis": "receipt_edition_year"}
    if table in NAMED_YEAR_TABLES and (table, name) not in REVIEWED_VINTAGES:
        match = NAMED_YEAR_FILES[table].fullmatch(name)
        if not match:
            raise PeriodError(f"{table} file {name} (release {release}) has no publisher year in its name")
        digits = match.group(1)
        year = int(digits) if len(digits) == 4 else (1900 if int(digits) >= 89 else 2000) + int(digits)
        return row | {"vintage": str(year), "period_start": f"{year}-01-01", "period_end": f"{year}-12-31", "period_basis": "publisher_file_name"}
    if table in CAPTURE_TABLES:
        match = CAPTURE_DATE.search(release)
        if not match:
            raise PeriodError(f"{table} file {name} (release {release}) has no capture date")
        captured = "-".join(match.groups())
        return row | {"vintage": captured, "period_start": "", "period_end": "", "period_basis": "capture_date"}
    if table in ACS_TABLES:
        match = ACS_FILE.fullmatch(name)
        if not match:
            raise PeriodError(f"{table} file {name} (release {release}) has no publisher vintage in its name")
        year = int(match.group(1) or match.group(2))
        return row | {"vintage": str(year), "period_start": f"{year - 4}-01-01", "period_end": f"{year}-12-31", "period_basis": "publisher_file_name"}
    if (table, name) not in REVIEWED_VINTAGES:
        raise PeriodError(f"{table} file {name} (release {release}) has no reviewed vintage")
    return row | {"vintage": REVIEWED_VINTAGES[(table, name)], "period_start": "", "period_end": "", "period_basis": "reviewed_file_name"}


def rows_for(
    loaded: Iterable[Mapping[str, str]],
    quarters: Mapping[str, tuple[str, str, str]],
    coverage: Mapping[str, tuple[str, str]],
    editions: Mapping[str, str] | None = None,
    databases: Mapping[str, tuple[str, str, str]] | None = None,
) -> list[dict[str, str]]:
    """Return one seed row per loaded file; copies of one file must agree, and the row keeps the first release ID."""
    rows: dict[tuple[str, str], dict[str, str]] = {}
    for item in sorted(loaded, key=lambda entry: (entry["table"], entry["sha256"], entry["release_id"])):
        row = file_row(item, quarters, coverage, editions or {}, databases or {})
        kept = rows.setdefault((row["bronze_table"], row["member_sha256"]), row)
        dated = ("vintage", "period_start", "period_end")
        if tuple(kept[column] for column in dated) != tuple(row[column] for column in dated):
            raise PeriodError(f"{row['bronze_table']} file {row['file_name']} has copies with two periods")
    return sorted(rows.values(), key=lambda row: (row["bronze_table"], row["vintage"], row["member_sha256"]))


def loaded_files() -> list[dict[str, str]]:
    """Read the loaded geography files from the S3 manifests, read-only, as bronze discovers them."""
    from scripts.lakehouse import bronze
    from scripts.lakehouse.catalog import deployment

    settings = deployment()
    os.environ.setdefault("AWS_PROFILE", settings["aws_profile"])
    os.environ.setdefault("AWS_REGION", settings["aws_region"])
    inputs, _ = bronze.discover(bronze.S3Storage(), settings["data_bucket_name"], bronze.load_table_map(), TABLES, bronze.load_retired())
    return [{"table": item["table"], "sha256": item["sha256"], "release_id": item["release_id"], "file_name": item["file_name"]} for item in inputs]


def build() -> list[dict[str, str]]:
    """Return the seed rows from storage, the receipts and the job plans."""
    return rows_for(loaded_files(), receipt_quarters(), job_coverage(), receipt_editions(), receipt_databases())


def as_csv(rows: Iterable[Mapping[str, str]]) -> str:
    """Return the rows as the seed's CSV text."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def main() -> int:
    """Rebuild the geography file periods and write or check the committed seed."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="compare with the committed seed instead of writing it")
    args = parser.parse_args()
    try:
        text = as_csv(build())
    except PeriodError as error:
        sys.stderr.write(f"geography file periods: {error}\n")
        return 1
    if args.check:
        same = SEED.exists() and SEED.read_text() == text
        sys.stdout.write(f"geography file periods: {'committed seed reproduced' if same else 'committed seed differs from storage'}\n")
        return 0 if same else 1
    SEED.write_text(text)
    sys.stdout.write(f"geography file periods: {text.count(chr(10)) - 1} files\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
