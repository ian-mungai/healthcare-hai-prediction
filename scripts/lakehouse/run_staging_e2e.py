"""E2E check of the staging copy, label, twin and sheet models (failure modes 166 to 173, 177 to 188, 267 to 278).

Run from the repository root with Docker running:

    .venv/bin/python -m scripts.lakehouse.run_staging_e2e            # fixture cases only
    .venv/bin/python -m scripts.lakehouse.run_staging_e2e --real     # then the real bronze tables, built twice

Each fixture case writes a small bronze database, runs ``dbt build`` against it in the analytics image and compares the
models with expectations computed here, independently of the SQL. Each case runs on its own copy of the dbt project,
whose label and twin seeds are written by the real generator (``ipps_file_labels``) from the fixture's names. Six cases
must fail one named dbt test each: a name clash under one release, copies with different row counts, a stale label
hold, an object with two checksums, an unheld label conflict and an unlabelled copy. The real stage checks that the
generator reproduces the committed seeds, builds the models from the catalog twice and reconciles them with bronze.

Failure modes: ``data/lakehouse_planning/staging_dedup_20261003/failure_modes.md`` and
``data/lakehouse_planning/staging_families_20261003/failure_modes.md`` and
``data/lakehouse_planning/sheet_selection_20261005/failure_modes.md``. The report in ``data/e2e/staging/`` holds
outcomes and counts, never data values or credentials.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import shutil
import sys
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import catalog, ipps_file_labels
from scripts.process import run_command

REPO_ROOT = catalog.REPO_ROOT
OUT = REPO_ROOT / "data/analytics/dbt"
CASES = OUT / "e2e"
REPORTS = REPO_ROOT / "data/e2e/staging"
CONTAINER_OUT = "/workspace/out"
TIMEOUT = 7200
TABLES = (
    "cms_hai_hospital",
    "cms_hai_state",
    "cms_hai_national",
    "cms_hospital_cost_reports",
    "cms_ipps_text_lines",
    "cms_ipps_sheet_rows",
    "cms_ipps_sas",
    "cms_occupational_mix_text_lines",
    "cms_occupational_mix_text_lines_utf16",
    "cms_occupational_mix_sheet_rows",
)
HELD = {
    "61a3cfb84973b2997ca60b2ebdce129005a9267d452db0ee984d9ca1eefacc88": "BRZ-016",
    "83b9668d22a23b40def50b64725f9674dcaa821d0b2db5e43215e672cad69a71": "BRZ-016",
}
NESTED_DATE = re.compile(r".*(\d{4}-\d{2}-\d{2})\.zip!")
HAI_FILE = "Healthcare_Associated_Infections-Hospital.csv"


@dataclass(frozen=True)
class Stored:
    """One loaded object in a fixture bronze table."""

    table: str
    key: str
    release: str
    member: str
    sha: str
    rows: int
    snapshot: str = "fixture-snapshot"
    second_sha: str | None = None
    # Text lines, or "sheet:cell|cell" rows for a workbook; empty gives generated values.
    content: tuple[str, ...] = ()
    # Bronze also holds this copy's rows although it is not the loaded copy, as before one copy per file [207].
    force_loaded: bool = False


def sha(label: str) -> str:
    """Return a readable 64-character fake checksum."""
    return (label * 64)[:64]


# Displayed values in the text file (4 decimals, "$1,234.50") against full precision in the workbook [189]; data rows align
# by position after the title and header rows [190].
TWIN_TEXT = (
    "FY 2021 IPPS Impact File - Final Rule\t\t",
    "Provider\tName\tCMI\tPayment\tShare",
    '010001\t"Alpha, Hospital "\t1.2346\t"$1,234.50"\t1.58%',
    '010002\tBeta\t0.5\t"$60,591,269.77"\t0.22%',
)
TWIN_SHEET = (
    "Variable Descriptions:Variable|Meaning",
    "Data:Provider|Name|CMI|Payment|Share",
    "Data:10001|Alpha, Hospital|1.23456789|1234.4978|0.0158371255",
    "Data:10002|Beta|0.5|60591269.765|0.0021846031",
)
OCCMIX_TEXT = ("Provider,Wage", "010001,3.5", "010002,4.0")
# The BRZ-016 text held under two names; its workbook twin agrees with it [268].
HELD_TEXT = ("Provider\tCMI", "010001\t1.5")
# A final-rule workbook whose correction-notice sheet has the text's row count but other values [267] [269].
FR_CN_SHEET = (
    "FR 2024:Provider|Name|CMI|Payment|Share",
    "FR 2024:10001|Alpha, Hospital|1.23456789|1234.4978|0.0158371255",
    "FR 2024:10002|Beta|0.5|60591269.765|0.0021846031",
    "CN 2024:Provider|Name|CMI|Payment|Share",
    "CN 2024:10001|Alpha, Hospital|1.3|1300|0.016",
    "CN 2024:10002|Beta|0.6|60000000|0.0022",
)
HAI_2021 = (
    "010001|HAI_1_SIR|01/01/2019|12/31/2019|0.5",
    "010001|HAI_2_SIR|01/01/2019|12/31/2019|0.7",
    "010002|HAI_1_SIR|01/01/2019|12/31/2019|0.9",
)
HAI_2026_MAY = ("010001|HAI_1_SIR|01/01/2019|12/31/2019|0.6", "010001|HAI_1_SIR|01/01/2025|12/31/2025|1.1")
# A letter-suffixed facility ID is kept as text; a date that does not parse is held, never guessed [233].
HAI_2026_AUG = (
    "010001|HAI_1_SIR|01/01/2025|12/31/2025|1.2",
    "010003|HAI_1_SIR|01/01/2025|12/31/2025|0.4",
    "01000F|HAI_1_SIR|01/01/2025|12/31/2025|0.3",
    "010004|HAI_1_SIR|13/45/2025|12/31/2025|0.2",
)
HAI_STATE = ("AL|HAI_1_SIR|01/01/2019|12/31/2019|0.8", "AL|HAI_2_SIR|01/01/2019|12/31/2019|0.9")
HAI_NATIONAL = ("|HAI_1_SIR|01/01/2019|12/31/2019|1.0",)
HAI_TABLES = {"cms_hai_hospital": "facility_id", "cms_hai_state": "state", "cms_hai_national": None}
BASE = (
    # HAI: a release republished byte for byte [171]; the year-to-date archive dated by capture [170]; the canonical
    # copy is the smallest object key, here the archive copy, never the earliest or latest capture [167].
    # HAI rows are "entity|measure|start|end|score". A later release revises a value; the latest dated file wins [231].
    Stored("cms_hai_hospital", "h01", "2021-01-27", HAI_FILE, sha("a1"), 3, content=HAI_2021),
    Stored("cms_hai_hospital", "h02", "2021-03-31", HAI_FILE, sha("a1"), 3, content=HAI_2021),
    Stored("cms_hai_hospital", "h03", "2026-05-13", HAI_FILE, sha("a2"), 2, content=HAI_2026_MAY),
    Stored("cms_hai_hospital", "h00", "2026-08-19", f"hospitals_2026-05-13.zip!{HAI_FILE}", sha("a2"), 2, content=HAI_2026_MAY),
    Stored("cms_hai_hospital", "h04", "2026-08-19", f"hospitals_2026-08-13.zip!{HAI_FILE}", sha("a3"), 4, content=HAI_2026_AUG),
    # A notes file packed with the HAI tables has no key [235]; another file on the same date conflicts for one key [232].
    Stored("cms_hai_hospital", "h05", "2026-08-19", "readme_bundle.zip!Notes.csv", sha("a4"), 1, content=("||||",)),
    Stored("cms_hai_hospital", "h07", "2026-08-13", "HAI_Hospital_Supplement.csv", sha("a5"), 1, content=("010003|HAI_1_SIR|01/01/2025|12/31/2025|0.45",)),
    Stored("cms_hai_state", "s01", "2021-01-27", "Healthcare_Associated_Infections-State.csv", sha("b1"), 2, content=HAI_STATE),
    Stored("cms_hai_national", "n01", "2021-01-27", "Healthcare_Associated_Infections-National.csv", sha("b2"), 1, content=HAI_NATIONAL),
    # Cost reports: one file captured in two snapshots on the same day.
    Stored("cms_hospital_cost_reports", "c01", "CMS_HCRIS_PUF__20260924T040326Z__aa", "CostReport_2023_Final.csv", sha("c1"), 3, "snap-aa"),
    Stored("cms_hospital_cost_reports", "c02", "CMS_HCRIS_PUF__20260924T042740Z__bb", "CostReport_2023_Final.csv", sha("c1"), 3, "snap-bb"),
    Stored("cms_hospital_cost_reports", "c03", "CMS_HCRIS_PUF__20260924T040326Z__aa", "CostReport_2022_Final.csv", sha("c2"), 2, "snap-aa"),
    # IPPS: the two BRZ-016 files under conflicting names, held [172]; one ordinary file.
    Stored("cms_ipps_text_lines", "i01", "CMS_IPPS__a", "FY 2019 IPPS Proposed Rule Impact File.txt", next(iter(HELD)), 2, content=HELD_TEXT),
    Stored(
        "cms_ipps_text_lines",
        "i02",
        "CMS_IPPS__a",
        "FY 2019 IPPS Proposed Rule Impact File (Variable Descriptions).txt",
        next(iter(HELD)),
        2,
        content=HELD_TEXT,
    ),
    # Its workbook twin: the text is held, so no selected text covers the data sheet and it is kept [268]; a sheet with no
    # data rows cannot be compared and is kept [274].
    Stored(
        "cms_ipps_sheet_rows",
        "w05",
        "CMS_IPPS__a",
        "FY 2019 IPPS Proposed Rule Impact File.xlsx",
        sha("n2"),
        3,
        content=("FY19 NPRM:Provider|CMI", "FY19 NPRM:10001|1.5", "Variable Descriptions:Variable|Meaning"),
    ),
    Stored("cms_ipps_text_lines", "i03", "CMS_IPPS__b", "FY 2020 Correction Notice Impact File.txt", list(HELD)[1], 3),
    Stored("cms_ipps_text_lines", "i04", "CMS_IPPS__c", "FY 2019 IPPS FR and CN Impact File (CN data).txt", list(HELD)[1], 3),
    # Twins that agree [185] [189] to [192]: a title line, CSV quoting, a leading zero, $ and separators, displayed decimals,
    # percentages, a value exactly at the half-unit boundary and a space inside quotes [200].
    Stored("cms_ipps_text_lines", "i05", "CMS_IPPS__c", "FY 2021 Final Rule Impact File.txt", sha("d1"), 4, content=TWIN_TEXT),
    Stored("cms_ipps_sheet_rows", "w01", "CMS_IPPS__c", "FY 2021 Final Rule Impact File.xlsx", sha("d2"), 4, content=TWIN_SHEET),
    # A twin pair that agrees on the final-rule sheet; the correction-notice sheet matches no selected text and is kept [267].
    Stored("cms_ipps_text_lines", "i11", "CMS_IPPS__m", "FY 2024 Final Rule Impact File.txt", sha("l1"), 4, content=TWIN_TEXT),
    Stored("cms_ipps_sheet_rows", "w06", "CMS_IPPS__m", "FY 2024 Final Rule Impact File.xlsx", sha("l2"), 6, content=FR_CN_SHEET),
    # Twins whose text file is fixed-width, with thousands separators: not compared, the workbook is preferred [186] [193].
    Stored(
        "cms_ipps_text_lines",
        "i06",
        "CMS_IPPS__c",
        "FY 2022 Final Rule Impact File.txt",
        sha("d4"),
        3,
        content=("PROV  NAME   CMI    WAGES", "010001ALPHA 1.2345 $60,884,976.48", "010002BETA  0.5000 $1,000.00"),
    ),
    Stored(
        "cms_ipps_sheet_rows",
        "w02",
        "CMS_IPPS__c",
        "FY 2022 Final Rule Impact File.xlsx",
        sha("d5"),
        2,
        content=("Data:10001|ALPHA|1.2345", "Data:10002|BETA|0.5"),
    ),
    Stored("cms_ipps_sas", "x01", "CMS_IPPS__d", "prds_hosp10_yr2019.sas7bdat", sha("d3"), 2),
    # Occupational mix: one capture packs the same file in two nested archives, neither dated [170].
    # A UTF-16 text file has its own table and still pairs with its workbook [199].
    Stored(
        "cms_occupational_mix_text_lines_utf16",
        "u01",
        "CMS_OCCMIX__b",
        "FY_2017_FINAL_provoccmix.zip!FY_2017_FR_provoccmix_06302016.txt",
        sha("g1"),
        2,
        "CMS_OCCMIX__u",
        content=("PROV\tWAGE", "010001\t$28.20"),
    ),
    Stored(
        "cms_occupational_mix_sheet_rows",
        "v02",
        "CMS_OCCMIX__b",
        "FY_2017_FINAL_provoccmix.zip!FY_2017_FR_provoccmix_06302016.xlsx",
        sha("g2"),
        2,
        "CMS_OCCMIX__u",
        content=("Data:PROV|WAGE", "Data:010001|28.199739"),
    ),
    # Dates as m/d/yyyy in the text and Excel serial numbers or ISO timestamps in the workbook agree [202].
    Stored(
        "cms_occupational_mix_text_lines",
        "m03",
        "CMS_OCCMIX__d",
        "test9mc040513.txt",
        sha("h1"),
        3,
        "CMS_OCCMIX__d",
        content=("PROV\tFROM\tTO\tAHW", "010001\t01/01/2010\t12/31/2010\t28.1252", "010005\t01/04/2010\t12/28/2010\t28.5869"),
    ),
    Stored(
        "cms_occupational_mix_sheet_rows",
        "v03",
        "CMS_OCCMIX__d",
        "test9mc040513.xls",
        sha("h2"),
        3,
        "CMS_OCCMIX__d",
        content=("Sheet1:PROV|FROM|TO|AHW", "Sheet1:010001|40179.0|40543.0|28.125204115", "Sheet1:010005|2010-01-04T00:00:00|40540.0|28.586945499"),
    ),
    # A tab-separated header over space-separated data is fixed-width: not compared, the workbook is used [201].
    Stored(
        "cms_ipps_text_lines",
        "i09",
        "CMS_IPPS__g",
        "FY_2012_FINAL_CMI.TXT",
        sha("j1"),
        3,
        "CMS_IPPS__g",
        content=("PROV\tCMI", "010001 01.695408", "010005 01.236252"),
    ),
    Stored(
        "cms_ipps_sheet_rows",
        "w03",
        "CMS_IPPS__g",
        "FY_2012_FINAL_CMI.xlsx",
        sha("j2"),
        3,
        "CMS_IPPS__g",
        content=("Data:PROV|CMI", "Data:010001|1.695408", "Data:010005|1.236252"),
    ),
    # A text file and workbook under different names, paired by a reviewed override, agree like a same-name pair [218] [220].
    Stored("cms_ipps_text_lines", "i10", "CMS_IPPS__h", "FY 2023 Final Rule Impact File.txt", sha("k1"), 4, "CMS_IPPS__h", content=TWIN_TEXT),
    Stored("cms_ipps_sheet_rows", "w04", "CMS_IPPS__h", "IMPACT_FY23_FR_PUF.xlsx", sha("k2"), 4, "CMS_IPPS__h", content=TWIN_SHEET),
    # An occupational-mix workbook whose second sheet is published as its own text file in the same release: each sheet is
    # covered by the selected text that matches it [270] [276].
    Stored("cms_occupational_mix_text_lines", "m04", "CMS_OCCMIX__p", "FY26_S3_PUF.txt", sha("p1"), 2, "CMS_OCCMIX__p", content=("PROV\tS3", "010001\t100")),
    Stored(
        "cms_occupational_mix_text_lines", "m05", "CMS_OCCMIX__p", "FY26_OccMix_PUF.txt", sha("p3"), 2, "CMS_OCCMIX__p", content=("PROV\tOM", "010001\t0.5")
    ),
    Stored(
        "cms_occupational_mix_sheet_rows",
        "v04",
        "CMS_OCCMIX__p",
        "FY26_S3_PUF.xlsx",
        sha("p2"),
        4,
        "CMS_OCCMIX__p",
        content=("S-3 Data:PROV|S3", "S-3 Data:010001|100", "OccMix Data:PROV|OM", "OccMix Data:010001|0.5"),
    ),
    # The text twin is comma-separated and one value differs from its workbook: the pair differs and both are held [187].
    Stored("cms_occupational_mix_text_lines", "m01", "CMS_OCCMIX__a", "PUFs.zip!AHW_by_Provider.zip!provcbsaahw.txt", sha("e1"), 3, content=OCCMIX_TEXT),
    Stored("cms_occupational_mix_text_lines", "m02", "CMS_OCCMIX__a", "PUFs.zip!provcbsaahw.zip!provcbsaahw.txt", sha("e1"), 3, content=OCCMIX_TEXT),
    Stored(
        "cms_occupational_mix_sheet_rows",
        "v01",
        "CMS_OCCMIX__a",
        "PUFs.zip!provcbsaahw.xlsx",
        sha("e2"),
        3,
        content=("Sheet1:Provider|Wage", "Sheet1:10001|3.6", "Sheet1:10002|4"),
    ),
)
# Each failing case changes the base fixture, or drops label rows, and names the one dbt test that must catch it.
FAILING: dict[str, tuple[str, tuple[Stored, ...], frozenset[str]]] = {
    "name_clash": ("assert_no_release_name_clash", (*BASE, Stored("cms_hai_hospital", "h06", "2021-01-27", HAI_FILE, sha("a9"), 2)), frozenset()),
    # Bronze holds rows of a copy the copies table lists as not loaded [207] [209].
    "copy_rows_in_bronze": (
        "assert_file_copies_match_bronze_objects",
        tuple(replace(item, force_loaded=True) if item.key == "c02" else item for item in BASE),
        frozenset(),
    ),
    "stale_hold": ("assert_label_holds_name_stored_files", tuple(item for item in BASE if item.sha != list(HELD)[1]), frozenset()),
    "two_checksums": (
        "assert_file_copies_one_checksum_per_object",
        tuple(replace(item, second_sha=sha("f1")) if item.key == "c03" else item for item in BASE),
        frozenset(),
    ),
    # Copies of one file named for two fiscal years, with no owner hold [181].
    "unheld_conflict": (
        "assert_label_conflicts_are_held",
        (
            *BASE,
            Stored("cms_ipps_text_lines", "i07", "CMS_IPPS__e", "FY 2017 Final Rule Impact File.txt", sha("d9"), 1),
            Stored("cms_ipps_text_lines", "i08", "CMS_IPPS__f", "FY 2018 Final Rule Impact File.txt", sha("d9"), 1),
        ),
        frozenset(),
    ),
    # The label map misses one loaded copy [182].
    "unlabelled_copy": ("assert_file_labels_cover_copies", BASE, frozenset({"i05"})),
}
# Reviewed pairs under different names, as committed in the overrides file [220].
FIXTURE_RENAMED = ((sha("k1"), sha("k2")),)
# The container name the fixture objects stand in, so names without a year still get one [178].
FIXTURE_CONTAINER = "FY_2021_fixture.zip"
TEXT_TABLES = {"cms_ipps_text_lines", "cms_occupational_mix_text_lines", "cms_occupational_mix_text_lines_utf16"}
SHEET_TABLES = {"cms_ipps_sheet_rows", "cms_occupational_mix_sheet_rows"}


FIXTURE_COLUMNS = (
    "bronze_table",
    "_object_key",
    "_source_id",
    "_snapshot_id",
    "_dataset_id",
    "_release_id",
    "_s3_key",
    "_s3_version_id",
    "_member_path",
    "_member_sha256",
    "_row_number",
    "value",
    "line_text",
    "sheet_name",
    "cells_text",
    "facility_id",
    "provider_id",
    "state",
    "measure_id",
    "start_date",
    "end_date",
    "measure_start_date",
    "measure_end_date",
    "score",
    "footnote",
    "measure_name",
)
# Splits the fixture CSV (path in the fixture_csv variable) into the bronze tables; fixed text, never built from values.
FIXTURE_SQL = """CREATE SCHEMA bronze;
CREATE TABLE fixture AS
    SELECT * REPLACE (_row_number::BIGINT AS _row_number)
    FROM read_csv(getvariable('fixture_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
CREATE TABLE bronze.cms_hai_hospital AS
    SELECT * EXCLUDE (bronze_table, line_text, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'cms_hai_hospital';
CREATE TABLE bronze.cms_hai_state AS
    SELECT * EXCLUDE (bronze_table, line_text, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'cms_hai_state';
CREATE TABLE bronze.cms_hai_national AS
    SELECT * EXCLUDE (bronze_table, line_text, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'cms_hai_national';
CREATE TABLE bronze.cms_hospital_cost_reports AS
    SELECT * EXCLUDE (bronze_table, line_text, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'cms_hospital_cost_reports';
CREATE TABLE bronze.cms_ipps_sas AS
    SELECT * EXCLUDE (bronze_table, line_text, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'cms_ipps_sas';
CREATE TABLE bronze.cms_ipps_text_lines AS
    SELECT * EXCLUDE (bronze_table, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'cms_ipps_text_lines';
CREATE TABLE bronze.cms_occupational_mix_text_lines AS
    SELECT * EXCLUDE (bronze_table, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'cms_occupational_mix_text_lines';
CREATE TABLE bronze.cms_occupational_mix_text_lines_utf16 AS
    SELECT * EXCLUDE (bronze_table, sheet_name, cells_text) FROM fixture WHERE bronze_table = 'cms_occupational_mix_text_lines_utf16';
CREATE TABLE bronze.cms_ipps_sheet_rows AS
    SELECT * EXCLUDE (bronze_table, line_text, cells_text), _row_number AS sheet_row, string_split(cells_text, '|') AS cells
    FROM fixture WHERE bronze_table = 'cms_ipps_sheet_rows';
CREATE TABLE bronze.cms_occupational_mix_sheet_rows AS
    SELECT * EXCLUDE (bronze_table, line_text, cells_text), _row_number AS sheet_row, string_split(cells_text, '|') AS cells
    FROM fixture WHERE bronze_table = 'cms_occupational_mix_sheet_rows';
DROP TABLE fixture;
CREATE TABLE bronze.stored_copies AS
    SELECT * REPLACE (byte_count::BIGINT AS byte_count, loaded::BOOLEAN AS loaded, retired::BOOLEAN AS retired)
    FROM read_csv(getvariable('copies_csv'), header = true, all_varchar = true, delim = ',', quote = '"', escape = '"');
"""
COPY_COLUMNS = (
    "table_name",
    "_object_key",
    "sha256",
    "s3_key",
    "s3_version_id",
    "source_id",
    "snapshot_id",
    "dataset_id",
    "release_id",
    "release_partition",
    "manifest_key",
    "manifest_version_id",
    "member_path",
    "file_name",
    "byte_count",
    "loaded",
    "retired",
)


def loaded_keys(objects: Iterable[Stored]) -> set[str]:
    """Return the copies bronze loads: the smallest object key of each table's file [205]."""
    canonical: dict[tuple[str, str], str] = {}
    for item in objects:
        file = (item.table, item.sha)
        canonical[file] = min(canonical.get(file, item.key), item.key)
    return set(canonical.values())


def copies_csv(objects: Iterable[Stored]) -> str:
    """Return the copies table as CSV: every stored copy with its lineage and whether bronze loads it [206]."""
    items = list(objects)
    loaded = loaded_keys(items)
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(COPY_COLUMNS)
    for item in items:
        file_name = item.member.rsplit("!", 1)[-1].rsplit("/", 1)[-1]
        writer.writerow(
            (item.table, item.key, item.sha, f"fixture/{item.key}.zip", f"v-{item.key}", "fixture", item.snapshot, "fixture_dataset", item.release)
            + ("", "fixture/manifest.json", "v-manifest", item.member, file_name, item.rows * 10, str(item.key in loaded).lower(), "false")
        )
    return buffer.getvalue()


def fixture_csv(objects: Iterable[Stored]) -> str:
    """Return the fixture rows as CSV: the provenance columns, one data column and the text or sheet content."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(FIXTURE_COLUMNS)
    items = list(objects)
    loaded = loaded_keys(items)
    for item in items:
        if item.key not in loaded and not item.force_loaded:
            continue
        if item.content and len(item.content) != item.rows:
            raise ValueError(f"fixture {item.key}: {len(item.content)} content rows but rows={item.rows}")
        for row in range(1, item.rows + 1):
            # The same bytes give the same rows; the second checksum, when set, marks the object's last row.
            checksum = item.second_sha if item.second_sha and row == item.rows else item.sha
            value = f"{item.sha[:6]}-{row}"
            text = item.content[row - 1] if item.content else value
            sheet, _, cells = text.partition(":") if item.table in SHEET_TABLES and item.content else ("Sheet1", "", value)
            # HAI content fills the HAI columns under the newer layout's names; empty parts stay null.
            hai = {"facility_id": "", "state": "", "measure_id": "", "start_date": "", "end_date": "", "score": ""}
            if item.table in HAI_TABLES and item.content:
                entity, measure, start, end, score = text.split("|")
                entity_column = HAI_TABLES[item.table]
                if entity_column:
                    hai[entity_column] = entity
                hai.update(measure_id=measure, start_date=start, end_date=end, score=score)
            writer.writerow(
                (item.table, item.key, "fixture", item.snapshot, "fixture_dataset", item.release, f"fixture/{item.key}.zip", f"v-{item.key}", item.member)
                + (checksum, row, value, text, sheet, cells)
                + (hai["facility_id"], "", hai["state"], hai["measure_id"], hai["start_date"], hai["end_date"], "", "", hai["score"], "", "")
            )
    return buffer.getvalue()


def publication(item: Stored) -> tuple[str, str]:
    """Return the expected publication release and its source [170]."""
    if "!" not in item.member:
        return item.release, "release_partition"
    nested = NESTED_DATE.match(item.member)
    return (nested.group(1), "nested_archive") if nested else (item.release, "container")


def expected(objects: Iterable[Stored]) -> dict[str, Any]:
    """Compute the copy and file models' expected content from the fixture, independently of the dbt SQL."""
    items = list(objects)
    canonical: dict[tuple[str, str], str] = {}
    for item in items:
        file = (item.table, item.sha)
        canonical[file] = min(canonical.get(file, item.key), item.key)
    # Only the loaded copy has rows in bronze; the other copies are listed without a row count [209].
    copies = sorted(
        (item.table, item.key, item.sha, *publication(item), str(canonical[(item.table, item.sha)] == item.key).lower())
        + (str(item.rows) if canonical[(item.table, item.sha)] == item.key else "",)
        for item in items
    )
    files: dict[tuple[str, str], dict[str, Any]] = {}
    for item in items:
        entry = files.setdefault((item.table, item.sha), {"copies": 0, "rows": item.rows, "releases": set()})
        entry["copies"] += 1
        entry["releases"].add(publication(item)[0])
    file_rows = sorted(
        (table, digest, canonical[(table, digest)], str(entry["copies"]), str(entry["rows"]), "|".join(sorted(entry["releases"])), HELD.get(digest, ""))
        for (table, digest), entry in files.items()
    )
    rows = {table: sum(entry["rows"] for (name, _), entry in files.items() if name == table) for table in TABLES}
    return {"copies": copies, "files": file_rows, "rows": rows}


COPIES_SQL = (
    "SELECT bronze_table, object_key, member_sha256, publication_release, release_source, is_canonical::VARCHAR, coalesce(row_count::VARCHAR, '') "
    "FROM stg_bronze__file_copies ORDER BY ALL;"
)
FILES_SQL = (
    "SELECT bronze_table, member_sha256, canonical_object_key, copy_count::VARCHAR, row_count::VARCHAR, "
    "array_to_string(publication_releases, '|'), coalesce(label_hold_issue, '') FROM stg_bronze__files ORDER BY ALL;"
)


WINDOWS_SQL = (
    "SELECT level, entity_id, measure_id, window_start::VARCHAR, window_end::VARCHAR, score, left(member_sha256, 2) FROM ("
    "SELECT 'hospital' AS level, * FROM int_hai_hospital_windows UNION ALL BY NAME "
    "SELECT 'state' AS level, * FROM int_hai_state_windows UNION ALL BY NAME "
    "SELECT 'national' AS level, * FROM int_hai_national_windows) ORDER BY ALL;"
)
HOLDS_SQL = "SELECT bronze_table, coalesce(entity_id, ''), coalesce(measure_id, ''), hold_reason, row_count::VARCHAR FROM int_hai_window_holds ORDER BY ALL;"
TWINS_SQL = (
    "SELECT left(text_sha256, 2), left(workbook_sha256, 2), text_layout, twin_status, coalesce(left(preferred_sha256, 2), '') "
    "FROM stg_bronze__twin_comparison ORDER BY ALL;"
)
SELECTION_SQL = (
    "SELECT left(member_sha256, 2), has_label_conflict::VARCHAR, is_label_held::VARCHAR, is_twin_excluded::VARCHAR, is_selected::VARCHAR "
    "FROM stg_bronze__file_selection WHERE NOT is_selected ORDER BY ALL;"
)
# Sheets of twin-excluded workbooks; every sheet of a selected workbook is file_selected [273].
SHEETS_SQL = (
    "SELECT left(member_sha256, 2), sheet_name, sheet_status, coalesce(left(covering_sha256, 2), ''), is_selected::VARCHAR "
    "FROM stg_bronze__sheet_selection WHERE sheet_status <> 'file_selected' ORDER BY ALL;"
)
VIEW_ROWS_SQL = "SELECT getvariable('checked_table'), count(*)::VARCHAR FROM query_table(getvariable('checked_table'));\n"
BRONZE_COUNTS_SQL = (
    "WITH o AS (SELECT _object_key, any_value(_member_sha256) AS sha, count(*) AS n FROM query_table(getvariable('checked_table')) GROUP BY 1), "
    "d AS (SELECT DISTINCT sha, n FROM o) "
    "SELECT getvariable('checked_table'), (SELECT count(*) FROM o)::VARCHAR, (SELECT count(*) FROM d)::VARCHAR, (SELECT sum(n) FROM d)::VARCHAR;\n"
)


def per_table(query: str, prefix: str) -> str:
    """Return the query once per table, each run after setting the checked_table variable to the prefixed table name."""
    return "".join(f"SET VARIABLE checked_table = '{prefix}{table}';\n{query}" for table in TABLES)


def unprefixed(rows: list[list[str]], prefix: str) -> dict[str, list[str]]:
    """Key query rows by table name without its prefix."""
    return {row[0].removeprefix(prefix): row[1:] for row in rows}


def compose_run(service_args: list[str], extra_env: dict[str, str]) -> tuple[int, str, str]:
    """Run one container of the analytics-dbt service and return its exit code and output."""
    env_flags = [flag for name, value in extra_env.items() for flag in ("-e", f"{name}={value}")]
    args = ["compose", "--project-directory", str(REPO_ROOT), "-f", str(REPO_ROOT / "docker-compose.yaml"), "--env-file", str(catalog.COMPOSE_ENV)]
    args += ["--profile", "query", "run", "--rm", "-T", "--quiet-pull", *env_flags, *service_args]
    result = run_command("docker", args, cwd=REPO_ROOT, env=catalog.system_environment(), timeout=TIMEOUT)
    return result.returncode, result.stdout, result.stderr


def duckdb_csv(database: str, sql: str, init: str | None = None) -> list[list[str]]:
    """Query a database in the case folder with the DuckDB CLI and return the rows without the header."""
    init_flags = ["-init", init] if init else []
    code, stdout, stderr = compose_run(["--entrypoint", "duckdb", "analytics-dbt", database, *init_flags, "-csv", "-noheader", "-c", sql], {})
    if code:
        raise RuntimeError(f"duckdb query failed: {stderr.strip().splitlines()[-1:] or ['no output']}")
    return [row for row in csv.reader(io.StringIO(stdout)) if row]


def install_packages() -> None:
    """Install the dbt packages from dbt/package-lock.yml into the output mount; the project mount stays read-only."""
    code, stdout, stderr = compose_run(["analytics-dbt", "deps"], {})
    if code:
        raise SystemExit(f"dbt deps failed ({code}): {(stdout + stderr)[-2000:]}")


def dbt_build(case: str, target: str, project: bool = False) -> tuple[int, dict[str, str]]:
    """Run dbt build for a case, on its own project copy when asked, and return the exit code and each node's status."""
    case_dir = f"{CONTAINER_OUT}/e2e/{case}"
    env = {"STAGING_E2E_CASE": case_dir, "DBT_TARGET_PATH": f"{case_dir}/target", "DBT_LOG_PATH": f"{case_dir}/logs"}
    flags = ["--project-dir", f"{case_dir}/project", "--profiles-dir", f"{case_dir}/project"] if project else []
    # A build that dies leaves no results of its own; never read the previous run's.
    (CASES / case / "target/run_results.json").unlink(missing_ok=True)
    code, _, _ = compose_run(["analytics-dbt", "build", "--target", target, *flags], env)
    results_path = CASES / case / "target/run_results.json"
    if not results_path.exists():
        return code, {}
    results = json.loads(results_path.read_text())
    return code, {item["unique_id"].split(".")[2]: item["status"] for item in results["results"]}


def generator_objects(objects: Iterable[Stored]) -> list[ipps_file_labels.Stored]:
    """Return the fixture's IPPS and occupational-mix objects as the label generator sees them."""
    return [
        ipps_file_labels.Stored(item.table, item.key, item.sha, item.snapshot, FIXTURE_CONTAINER, tuple(item.member.split("!")))
        for item in objects
        if item.table in ipps_file_labels.TABLES
    ]


def fixture_project(case_dir: Path, objects: tuple[Stored, ...], unlabelled: frozenset[str]) -> None:
    """Copy the dbt project into the case and write its label and twin seeds with the real generator functions."""
    project = case_dir / "project"
    shutil.copytree(REPO_ROOT / "dbt", project)
    stored = generator_objects(objects)
    rows = [row for row in ipps_file_labels.labels(stored, {}) if row["object_key"] not in unlabelled]
    (project / "seeds/ipps_occmix_copy_labels.csv").write_text(ipps_file_labels.as_csv(rows, ipps_file_labels.LABEL_COLUMNS))
    twin_rows = ipps_file_labels.twins(stored, FIXTURE_RENAMED)
    (project / "seeds/ipps_occmix_twins.csv").write_text(ipps_file_labels.as_csv(twin_rows, ipps_file_labels.TWIN_COLUMNS))


def run_fixture(case: str, objects: tuple[Stored, ...], unlabelled: frozenset[str] = frozenset()) -> tuple[int, dict[str, str]]:
    """Write a case's fixture bronze database and project copy and build the models against them."""
    case_dir = CASES / case
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True)
    fixture_project(case_dir, objects, unlabelled)
    (case_dir / "bronze.csv").write_text(fixture_csv(objects))
    (case_dir / "copies.csv").write_text(copies_csv(objects))
    variables = f"SET VARIABLE fixture_csv = '{CONTAINER_OUT}/e2e/{case}/bronze.csv';\nSET VARIABLE copies_csv = '{CONTAINER_OUT}/e2e/{case}/copies.csv';\n"
    (case_dir / "bronze.sql").write_text(variables + FIXTURE_SQL)
    database = f"{CONTAINER_OUT}/e2e/{case}/fixture_lakehouse.duckdb"
    code, _, stderr = compose_run(["--entrypoint", "duckdb", "analytics-dbt", database, "-c", f".read {CONTAINER_OUT}/e2e/{case}/bronze.sql"], {})
    if code:
        raise RuntimeError(f"fixture {case} failed: {stderr.strip().splitlines()[-1:] or ['no output']}")
    return dbt_build(case, "fixture", project=True)


def model_outputs(case: str) -> dict[str, Any]:
    """Read a fixture case's copy and file models and each staging view's row count; a missing model is an error entry."""
    try:
        return read_models(case)
    except RuntimeError as error:
        return {"error": str(error)}


def read_models(case: str) -> dict[str, Any]:
    """Read a fixture case's copy and file models and each staging view's row count."""
    database = f"{CONTAINER_OUT}/e2e/{case}/staging.duckdb"
    attach = CASES / case / "attach.sql"
    attach.write_text(f"ATTACH '{CONTAINER_OUT}/e2e/{case}/fixture_lakehouse.duckdb' AS lakehouse (READ_ONLY);\n")
    init = f"{CONTAINER_OUT}/e2e/{case}/attach.sql"
    rows = {table: int(values[0]) for table, values in unprefixed(duckdb_csv(database, per_table(VIEW_ROWS_SQL, "stg_"), init), "stg_").items()}
    return {
        "copies": [tuple(row) for row in duckdb_csv(database, COPIES_SQL)],
        "files": [tuple(row) for row in duckdb_csv(database, FILES_SQL)],
        "rows": rows,
        "twins": [tuple(row) for row in duckdb_csv(database, TWINS_SQL)],
        "selection": [tuple(row) for row in duckdb_csv(database, SELECTION_SQL)],
        "sheets": [tuple(row) for row in duckdb_csv(database, SHEETS_SQL)],
        "windows": [tuple(row) for row in duckdb_csv(database, WINDOWS_SQL)],
        "holds": [tuple(row) for row in duckdb_csv(database, HOLDS_SQL)],
    }


def refuses(action: Any, fragment: str) -> bool:
    """Return whether the action raises the generator's error with the fragment in its message."""
    try:
        action()
    except ipps_file_labels.LabelError as error:
        return fragment in str(error)
    return False


def fixture_scenarios() -> dict[str, bool]:
    """Run every fixture case and return each check's outcome."""
    checks: dict[str, bool] = {}
    want = expected(BASE)
    code, statuses = run_fixture("base", BASE)
    checks["base_build_passes"] = code == 0 and bool(statuses) and all(status in ("pass", "success") for status in statuses.values())
    base = model_outputs("base")
    checks["copies_match_expected"] = [tuple(row) for row in want["copies"]] == base.get("copies")
    checks["files_match_expected"] = [tuple(row) for row in want["files"]] == base.get("files")
    checks["view_rows_match_distinct_files"] = want["rows"] == base.get("rows")
    # Twins: the quoted, zero-padded, rounded pair agrees and prefers its text file; the fixed-width pair prefers its
    # workbook; the comma pair with a changed value differs and keeps neither [184] to [187].
    checks["twins_match_expected"] = base.get("twins") == [
        ("61", "n2", "tab", "agree", "61"),
        ("d1", "d2", "tab", "agree", "d1"),
        ("d4", "d5", "fixed_width", "not_compared", "d5"),
        ("e1", "e2", "comma", "differ", ""),
        ("g1", "g2", "tab", "agree", "g1"),
        ("h1", "h2", "tab", "agree", "h1"),
        ("j1", "j2", "fixed_width", "not_compared", "j2"),
        ("k1", "k2", "tab", "agree", "k1"),
        ("l1", "l2", "tab", "agree", "l1"),
        ("p1", "p2", "tab", "agree", "p1"),
    ]
    # Not selected: the two held BRZ-016 files (their copies conflict) and the excluded twins [172] [181] [187].
    checks["selection_matches_expected"] = base.get("selection") == [
        ("61", "true", "true", "false", "false"),
        ("83", "true", "true", "false", "false"),
        ("d2", "false", "false", "true", "false"),
        ("d4", "false", "false", "true", "false"),
        ("e1", "false", "false", "true", "false"),
        ("e2", "false", "false", "true", "false"),
        ("g2", "false", "false", "true", "false"),
        ("h2", "false", "false", "true", "false"),
        ("j1", "false", "false", "true", "false"),
        ("k2", "false", "false", "true", "false"),
        ("l2", "false", "false", "true", "false"),
        ("n2", "false", "false", "true", "false"),
        ("p2", "false", "false", "true", "false"),
    ]
    # Sheets of twin-excluded workbooks: covered only by a selected text of the same release whose data rows all match;
    # otherwise kept, except the compared sheet of a pair that differs [267] to [274].
    checks["sheets_match_expected"] = base.get("sheets") == [
        ("d2", "Data", "covered", "d1", "false"),
        ("d2", "Variable Descriptions", "kept", "", "true"),
        ("e2", "Sheet1", "twin_differs", "", "false"),
        ("g2", "Data", "covered", "g1", "false"),
        ("h2", "Sheet1", "covered", "h1", "false"),
        ("k2", "Data", "covered", "k1", "false"),
        ("k2", "Variable Descriptions", "kept", "", "true"),
        ("l2", "CN 2024", "kept", "", "true"),
        ("l2", "FR 2024", "covered", "l1", "false"),
        ("n2", "FY19 NPRM", "kept", "", "true"),
        ("n2", "Variable Descriptions", "kept", "", "true"),
        ("p2", "OccMix Data", "covered", "p3", "false"),
        ("p2", "S-3 Data", "covered", "p1", "false"),
    ]
    # [219] [218] A file in two pairs, or a renamed pair from two captures, is refused by the generator.
    stored = generator_objects(BASE)
    checks["generator_refuses_file_in_two_pairs"] = refuses(
        lambda: ipps_file_labels.twins(stored, (*FIXTURE_RENAMED, (sha("d4"), sha("d2")))), "two twin pairs"
    )
    checks["generator_refuses_pair_across_captures"] = refuses(lambda: ipps_file_labels.twins(stored, ((sha("k1"), sha("j2")),)), "one capture")
    # [230] to [235] One row per HAI measurement window, from the latest dated file; conflicts and unreadable rows held.
    checks["hai_windows_match_expected"] = base.get("windows") == [
        ("hospital", "010001", "HAI_1_SIR", "2019-01-01", "2019-12-31", "0.6", "a2"),
        ("hospital", "010001", "HAI_1_SIR", "2025-01-01", "2025-12-31", "1.2", "a3"),
        ("hospital", "010001", "HAI_2_SIR", "2019-01-01", "2019-12-31", "0.7", "a1"),
        ("hospital", "010002", "HAI_1_SIR", "2019-01-01", "2019-12-31", "0.9", "a1"),
        ("hospital", "01000F", "HAI_1_SIR", "2025-01-01", "2025-12-31", "0.3", "a3"),
        ("national", "US", "HAI_1_SIR", "2019-01-01", "2019-12-31", "1.0", "b2"),
        ("state", "AL", "HAI_1_SIR", "2019-01-01", "2019-12-31", "0.8", "b1"),
        ("state", "AL", "HAI_2_SIR", "2019-01-01", "2019-12-31", "0.9", "b1"),
    ]
    checks["hai_holds_match_expected"] = base.get("holds") == [
        ("cms_hai_hospital", "", "", "no_key", "1"),
        ("cms_hai_hospital", "010003", "HAI_1_SIR", "same_date_conflict", "2"),
        ("cms_hai_hospital", "010004", "HAI_1_SIR", "unparsed_date", "1"),
    ]
    code, _ = run_fixture("base_again", BASE)
    checks["rebuild_identical"] = code == 0 and "error" not in base and model_outputs("base_again") == base
    code, _ = run_fixture("reversed_order", tuple(reversed(BASE)))
    checks["load_order_independent"] = code == 0 and "error" not in base and model_outputs("reversed_order") == base
    for case, (test, objects, unlabelled) in FAILING.items():
        code, statuses = run_fixture(case, objects, unlabelled)
        checks[f"{case}_fails_{test}"] = code != 0 and statuses.get(test) == "fail"
    return checks


REAL_ATTACH = """.output /dev/null
SET autoinstall_known_extensions = false;
LOAD iceberg;
LOAD httpfs;
LOAD aws;
CREATE SECRET lakehouse_s3 (TYPE s3, PROVIDER credential_chain, CHAIN 'config', PROFILE getenv('AWS_PROFILE'), REGION getenv('AWS_REGION'));
CREATE SECRET lakehouse_catalog (
    TYPE iceberg,
    CLIENT_ID getenv('DBT_ENV_SECRET_POLARIS_CLIENT_ID'),
    CLIENT_SECRET getenv('DBT_ENV_SECRET_POLARIS_CLIENT_SECRET'),
    OAUTH2_SERVER_URI 'http://polaris:8181/api/catalog/v1/oauth/tokens',
    OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL'
);
ATTACH 'hai_lakehouse' AS lakehouse (
    TYPE iceberg, ENDPOINT 'http://polaris:8181/api/catalog', SECRET lakehouse_catalog, ACCESS_DELEGATION_MODE 'none', READ_ONLY
);
.output stdout
"""


def real_stage() -> dict[str, Any]:
    """Build the models from the catalog twice and reconcile them with bronze."""
    outcome: dict[str, Any] = {"checks": {}, "counts": {}}
    stored = ipps_file_labels.collect()
    rebuilt_labels = ipps_file_labels.as_csv(ipps_file_labels.labels(stored, ipps_file_labels.load_overrides()), ipps_file_labels.LABEL_COLUMNS)
    rebuilt_twins = ipps_file_labels.as_csv(ipps_file_labels.twins(stored, ipps_file_labels.load_renamed()), ipps_file_labels.TWIN_COLUMNS)
    outcome["checks"]["generator_reproduces_seeds"] = (
        ipps_file_labels.LABELS_SEED.read_text() == rebuilt_labels and ipps_file_labels.TWINS_SEED.read_text() == rebuilt_twins
    )
    database = f"{CONTAINER_OUT}/staging.duckdb"
    (OUT / "real_attach.sql").write_text(REAL_ATTACH)
    init = f"{CONTAINER_OUT}/real_attach.sql"
    builds = []
    for run in ("real", "real_again"):
        (CASES / run).mkdir(parents=True, exist_ok=True)
        code, statuses = dbt_build(run, "lakehouse")
        failed = sorted(name for name, status in statuses.items() if status not in ("pass", "success"))
        outcome["checks"][f"{run}_build_passes"] = code == 0 and bool(statuses) and not failed
        outcome[f"{run}_not_passing"] = failed
        builds.append([row[:6] for row in duckdb_csv(database, FILES_SQL)])
    outcome["checks"]["real_rebuild_identical"] = builds[0] == builds[1]
    status_sql = "SELECT text_layout || ' ' || twin_status, count(*)::VARCHAR FROM stg_bronze__twin_comparison GROUP BY 1 ORDER BY 1;"
    outcome["twin_statuses"] = dict(duckdb_csv(database, status_sql))
    held_sql = (
        "SELECT 'held ' || is_label_held || ', twin excluded ' || is_twin_excluded, count(*)::VARCHAR "
        "FROM stg_bronze__file_selection WHERE NOT is_selected GROUP BY 1 ORDER BY 1;"
    )
    outcome["not_selected"] = dict(duckdb_csv(database, held_sql))
    sheet_sql = "SELECT sheet_status, count(*)::VARCHAR FROM stg_bronze__sheet_selection GROUP BY 1 ORDER BY 1;"
    outcome["sheet_statuses"] = dict(duckdb_csv(database, sheet_sql))
    # BRZ-016: the FY 2020 correction-notice sheet and the FY 2019 proposed-rule sheets are selected [267] [268] [274].
    brz016_sql = (
        "SELECT left(member_sha256, 10) || ' ' || sheet_name, sheet_status FROM stg_bronze__sheet_selection "
        "WHERE (left(member_sha256, 10) = 'ac5ffd206e' AND sheet_name = 'CN 2020') "
        "OR (left(member_sha256, 10) = '6907b7e751' AND sheet_name IN ('FY19 NPRM', 'Variable Descriptions')) ORDER BY 1;"
    )
    outcome["checks"]["brz016_sheets_kept"] = duckdb_csv(database, brz016_sql) == [
        ["6907b7e751 FY19 NPRM", "kept"],
        ["6907b7e751 Variable Descriptions", "kept"],
        ["ac5ffd206e CN 2020", "kept"],
    ]
    bronze = unprefixed(duckdb_csv(database, per_table(BRONZE_COUNTS_SQL, "lakehouse.bronze."), init), "lakehouse.bronze.")
    # Bronze loads one copy per file, so its objects equal the distinct files; the copies table lists every copy [204] [206].
    model_sql = (
        "SELECT f.bronze_table, count(*)::VARCHAR, count(*)::VARCHAR, sum(f.row_count)::VARCHAR, sum(f.copy_count)::VARCHAR "
        "FROM stg_bronze__files f GROUP BY 1 ORDER BY 1;"
    )
    models = {row[0]: row[1:] for row in duckdb_csv(database, model_sql)}
    copies_sql = "SELECT table_name, count(*)::VARCHAR FROM lakehouse.bronze.stored_copies GROUP BY 1 ORDER BY 1;"
    stored_copies = dict(duckdb_csv(database, copies_sql, init))
    views = {table: values[0] for table, values in unprefixed(duckdb_csv(database, per_table(VIEW_ROWS_SQL, "stg_"), init), "stg_").items()}
    for table in TABLES:
        objects, distinct, rows = bronze[table]
        copies = stored_copies.get(table, "0")
        outcome["counts"][table] = {
            "objects": int(objects),
            "distinct_files": int(distinct),
            "distinct_rows": int(rows),
            "copies": int(copies),
            "view_rows": int(views[table]),
        }
        outcome["checks"][f"{table}_reconciles"] = models.get(table) == [objects, distinct, rows, copies] and views[table] == rows
    return outcome


def main() -> int:
    """Run the fixture cases, then the real stage when asked, and write the report."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--real", action="store_true", help="also build from the real bronze tables, twice")
    args = parser.parse_args()
    catalog.up()
    install_packages()
    report: dict[str, Any] = {"started_at": datetime.now(UTC).isoformat(timespec="seconds"), "image": "hai-analytics:duckdb1.5.6-dbt1.11.15"}
    report["fixture"] = fixture_scenarios()
    if args.real:
        report["real"] = real_stage()
    checks = dict(report["fixture"]) | (report["real"]["checks"] if args.real else {})
    report["passed"] = sum(checks.values())
    report["total"] = len(checks)
    results = CASES / "base/target/run_results.json"
    if results.exists():
        report["dbt_version"] = json.loads(results.read_text())["metadata"]["dbt_version"]
    REPORTS.mkdir(parents=True, exist_ok=True)
    path = REPORTS / f"report_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.json"
    path.write_text(json.dumps(report, indent=2) + "\n")
    for name, ok in checks.items():
        sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}\n")
    sys.stdout.write(f"staging e2e: {report['passed']} of {report['total']} passed; report {path.relative_to(REPO_ROOT)}\n")
    return 0 if report["passed"] == report["total"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
