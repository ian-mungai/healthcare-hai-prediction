"""End-to-end test of the manifest-driven bronze loader with real Spark and Iceberg against a synthetic S3.

Run from the repository root; the catalog script starts it inside the Spark container without the Polaris service:

    .venv/bin/python -m scripts.lakehouse.catalog job bronze_e2e

Every scenario uses synthetic manifests and files held by an in-memory stand-in for versioned S3 and a throwaway
local Iceberg catalog under the container's scratch folder, so it never touches S3 or the real catalog. Failure modes
are in plans/bronze_hai_20261002/ (1 to 35), bronze_manifest_20261002/ (36 to 53),
bronze_ipps_occmix_20261002/ (68 to 82), bronze_dictionary_20261002/ (83 to 96), publisher_dictionaries_20261002/
(97 to 112), bronze_county_20261002/ (113 to 131), bronze_remaining_20261003/ (132 to 149) and
bronze_gaps_20261003/ (150 to 159) and bronze_utf16_20261003/ (194 to 198); numbers in brackets.
The report under data/e2e/bronze/ holds outcomes only.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import shutil
import sys
import tempfile
import zipfile
from collections.abc import Callable
from datetime import UTC, date, datetime
from functools import partial
from pathlib import Path
from typing import Any

from scripts.lakehouse import bronze, care_compare_tables, dictionary, file_readers
from scripts.lakehouse.session import spark_session

REPORT_ROOT = Path("data/e2e/bronze")
BUCKET = "synthetic-bucket"
# Edge cases Spark's CSV reader must keep exactly as Python's csv module reads them [43].
EDGE_ROWS = [
    ["010001", "Hospital, Inc.", 'Say "hi"', "", "Not Available"],
    ["45005A", "Line one\r\nline two", "  padded  ", "--", "N/A"],
    ["000123", "***", "", " ", "0.000"],
]
EDGE_HEADER = ["Facility ID", "Facility Name", "Note", "Score", "Footnote"]


class FakeStorage:
    """Versioned objects in memory, standing in for S3: list current keys, get or download one exact version."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.downloads: list[Path] = []

    def put(self, key: str, data: bytes) -> str:
        version = f"v-{hashlib.sha256(key.encode() + data).hexdigest()[:12]}"
        self.objects[(key, version)] = data
        return version

    def list_keys(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        if bucket != BUCKET:
            raise KeyError(bucket)
        return sorted((key, version) for key, version in self.objects if key.startswith(prefix))

    def list_current(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        return self.list_keys(bucket, prefix)

    def get(self, bucket: str, key: str, version_id: str) -> bytes:
        return self.objects[(key, version_id)]

    def download(self, bucket: str, key: str, version_id: str, path: Path) -> None:
        path.write_bytes(self.objects[(key, version_id)])
        self.downloads.append(path)


def csv_bytes(header: list[str], rows: list[list[str]]) -> bytes:
    """Write CRLF CSV the way the publishers do: quoted only where needed."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buffer.getvalue().encode()


def store(
    storage: FakeStorage,
    collection: str,
    dataset: str,
    snapshot: str,
    members: list[tuple[list[str] | None, str, bytes]],
    release: str = "2026-01-01",
    role: str = "data",
) -> str:
    """Store data objects and their manifest under the collection, as the acquisition storage step does."""
    objects = []
    for chain, name, data in members:
        digest = hashlib.sha256(data).hexdigest()
        key = f"{collection}/datasets/{dataset}/release_date=2026-01-01/{digest}/{name}"
        version = storage.put(key, data)
        entry: dict[str, Any] = {
            "role": role,
            "dataset_id": dataset,
            "object": {"bucket": BUCKET, "key": key, "version_id": version, "sha256": digest, "byte_count": len(data)},
        }
        if chain is not None:
            entry.update({"member_chain": chain, "member_sha256": digest})
        objects.append(entry)
    objects.append({"role": "capture_receipt", "dataset_id": dataset, "object": {"bucket": BUCKET, "key": f"{collection}/manifests/x/receipt.json"}})
    manifest = {"snapshot_id": snapshot, "source_id": dataset.upper(), "dataset_id": dataset, "release_id": release, "objects": objects}
    base = f"{collection}/manifests/release_date={release}/{snapshot}/{dataset}"
    storage.put(f"{base}/receipt.json", b'{"not": "a manifest"}')
    storage.put(f"{base}/manifest.json", json.dumps(manifest).encode())
    return base


def pdf_text(value: str) -> str:
    """Escape a string for a PDF literal; the en dash is WinAnsi byte 0x96."""
    escaped = value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    return escaped.replace("\u2013", "\\226")


def pdf_document(pages: list[dict[str, Any]]) -> bytes:
    """Write a minimal PDF: each page may hold one ruled table (rows of cells) and loose text lines, in Helvetica."""
    objects = ["<< /Type /Catalog /Pages 2 0 R >>", "", "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>"]
    kids = []
    for page in pages:
        commands = []
        rows = page.get("table", [])
        if rows:
            # Wide enough for each column's longest text, so no character falls outside its cell.
            widths = [max(60, 5 * max(len(row[column]) for row in rows) + 10) for column in range(len(rows[0]))]
            top, height, left = 740, 22, 40
            edges = [left + sum(widths[:index]) for index in range(len(widths) + 1)]
            for index in range(len(rows) + 1):
                commands.append(f"{left} {top - index * height} m {edges[-1]} {top - index * height} l S")
            for edge in edges:
                commands.append(f"{edge} {top} m {edge} {top - len(rows) * height} l S")
            for number, row in enumerate(rows):
                for column, cell in enumerate(row):
                    if cell:
                        commands.append(f"BT /F1 8 Tf {edges[column] + 3} {top - number * height - 14} Td ({pdf_text(cell)}) Tj ET")
        for number, line in enumerate(page.get("lines", [])):
            commands.append(f"BT /F1 9 Tf 40 {200 - number * 14} Td ({pdf_text(line)}) Tj ET")
        stream = "\n".join(commands).encode("latin-1")
        objects.append(f"<< /Length {len(stream)} >>\nstream\n" + stream.decode("latin-1") + "\nendstream")
        objects.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 3 0 R >> >> /Contents {len(objects)} 0 R >>")
        kids.append(f"{len(objects)} 0 R")
    objects[1] = f"<< /Type /Pages /Kids [{' '.join(kids)}] /Count {len(kids)} >>"
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for number, body in enumerate(objects, 1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n{body}\nendobj\n".encode("latin-1")
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
    out += "".join(f"{offset:010d} 00000 n \n" for offset in offsets).encode()
    out += f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    return bytes(out)


def table_map(path: Path, tables: list[dict[str, Any]]) -> Path:
    """Write a synthetic table map."""
    path.write_text(json.dumps({"version": 1, "namespace": "bronze", "tables": tables}))
    return path


def expect_error(action: Callable[[], object], fragment: str) -> bool:
    """Return True only when the action raises BronzeError mentioning the fragment."""
    try:
        action()
    except bronze.BronzeError as error:
        return fragment in str(error)
    return False


# Workbook written with openpyxl; expected rows after dropping trailing empty cells [72] [73].
EXPECTED_SHEET_ROWS = [
    ("Data", 1, 1, ["PROV", "CMI", "FLAG", "DATE"], ["s", "s", "s", "s"]),
    ("Data", 1, 2, ["010001", "1.2345", "TRUE", "2026-01-02T00:00:00"], ["s", "n", "b", "d"]),
    ("Data", 1, 3, [None, "0.1"], [None, "n"]),
    ("Data", 1, 4, [], []),
    ("Data", 1, 5, ["42"], ["n"]),
    ("Notes", 2, 1, ["note, with comma", "#N/A"], ["s", "e"]),
]


def write_workbook(path: Path) -> None:
    """Write a two-sheet workbook with text, numbers, a boolean, a date, an error, gaps and an empty row."""
    from openpyxl import Workbook

    book = Workbook()
    data = book.active
    data.title = "Data"
    data.append(["PROV", "CMI", "FLAG", "DATE"])
    data.append(["010001", 1.2345, True, date(2026, 1, 2)])  # Excel has no date-only type: it reads back as midnight
    data["B3"] = 0.1
    data["A5"] = 42
    notes = book.create_sheet("Notes")
    notes["A1"] = "note, with comma"
    notes["B1"] = "#N/A"
    book.save(path)


def trim(values: list[str | None] | None) -> list[str | None]:
    """Drop trailing empty cells, which readers pad differently."""
    values = list(values or [])
    while values and values[-1] is None:
        values.pop()
    return values


def trim_to(types: list[str | None] | None, values: list[str | None] | None) -> list[str | None]:
    """Cut a row's cell types to the length of its trimmed values."""
    return list(types or [])[: len(trim(values))]


def scenarios(spark: Any, root: Path) -> dict[str, bool]:
    """Run every scenario and return its outcome."""
    results: dict[str, bool] = {}
    storage = FakeStorage()
    scratch = root / "scratch"
    scratch.mkdir()
    hai = "(?:^|/)(?:Healthcare[ _]Associated[ _]Infections ?- ?Hospital|(?:[a-z0-9_]*_)?77hc[-_]ibv8)\\.csv$"
    layout_2019 = csv_bytes(["Provider ID", "Score"], [["010001", "Not Available"]])
    # A different file in the 2019 layout: identical bytes would load once [204].
    layout_2019_other = csv_bytes(["Provider ID", "Score"], [["010002", "Not Available"]])
    layout_2024 = csv_bytes(["Facility ID", "City/Town", "Score"], [["010001", "X", "--"], ["020002", "Y", "0.5"]])
    store(storage, "cms/hospitals", "legacy_hai", "snap_a", [(["h.zip", "Healthcare Associated Infections - Hospital.csv"], "h.csv", layout_2019)])
    store(
        storage,
        "cms/hospitals",
        "hai_ids",
        "snap_b",
        [(["77hc-ibv8.csv"], "77hc-ibv8.csv", layout_2024), (["pdc_s3_hos_data_77hc_ibv8.csv"], "p.csv", layout_2019_other)],
    )
    store(storage, "cms/hospitals", "other", "snap_c", [(["PCH_HEALTHCARE_ASSOCIATED_INFECTIONS_HOSPITAL.csv"], "pch.csv", layout_2019)])
    store(storage, "cms/provider_of_services", "cms_pos", "snap_d", [(None, "Hospital_and_other.DATA.Q1.csv", csv_bytes(EDGE_HEADER, EDGE_ROWS))])
    store(storage, "cms/provider_of_services", "cms_pos", "snap_e", [(None, "Hospital_and_other.DATA.Q2.csv", csv_bytes(EDGE_HEADER, EDGE_ROWS[:1]))])
    store(storage, "cms/ipps", "cms_ipps", "snap_f", [(["impact.xlsx"], "impact.xlsx", b"PK\x03\x04 not really excel")])
    tables: list[dict[str, Any]] = [
        {
            "table": "hai_hospital",
            "group": "hai",
            "collection": "cms/hospitals",
            "member_pattern": hai,
            "format": "csv",
            "expected_distinct_files": 3,
            "expected_distinct_rows": 4,
        },
        {"table": "provider_of_services", "group": "structure", "collection": "cms/provider_of_services", "dataset_ids": ["cms_pos"], "format": "csv"},
    ]
    config = bronze.load_table_map(table_map(root / "map.json", tables))

    # [37] [38] [39] Only manifest.json files under the mapped collection are read; unselected data objects are reported.
    inputs, unselected = bronze.discover(storage, BUCKET, config, ["hai_hospital", "provider_of_services"])
    hospital = [item for item in inputs if item["table"] == "hai_hospital"]
    results["manifests_select_named_and_id_only_files_and_skip_pch"] = len(hospital) == 3 and len(inputs) == 5
    results["unselected_objects_reported"] = [entry["file_name"] for entry in unselected] == ["PCH_HEALTHCARE_ASSOCIATED_INFECTIONS_HOSPITAL.csv"]
    results["lineage_names_manifest_version"] = all(
        item["manifest_key"].endswith("/manifest.json") and item["manifest_version_id"].startswith("v-") for item in inputs
    )
    overlap: list[dict[str, Any]] = [
        *tables,
        {"table": "all_hospitals", "group": "hai", "collection": "cms/hospitals", "member_pattern": "\\.csv$", "format": "csv"},
    ]
    results["overlapping_tables_refused"] = expect_error(
        lambda: bronze.discover(storage, BUCKET, bronze.load_table_map(table_map(root / "overlap.json", overlap)), ["hai_hospital", "all_hospitals"]),
        "two tables",
    )
    wrong: list[dict[str, Any]] = [{"table": "ipps", "group": "structure", "collection": "cms/ipps", "dataset_ids": ["cms_ipps"], "format": "csv"}]
    results["format_mismatch_refused"] = expect_error(
        lambda: bronze.discover(storage, BUCKET, bronze.load_table_map(table_map(root / "wrong.json", wrong)), ["ipps"]), "format"
    )
    results["unknown_format_refused"] = expect_error(
        lambda: bronze.load_table_map(table_map(root / "bad.json", [{**tables[1], "format": "parquet"}])), "format"
    )

    # [42] [43] [45] Spark reads the file; values and row numbers equal Python's csv module exactly.
    summary = bronze.load_objects(spark, inputs, storage, "bronze", scratch)
    edge = spark.table("bronze.provider_of_services").where("_snapshot_id = 'snap_d'").orderBy("_row_number").collect()
    loaded = [[row["facility_id"], row["facility_name"], row["note"], row["score"], row["footnote"]] for row in edge]
    results["spark_values_equal_python_csv"] = loaded == EDGE_ROWS and [row["_row_number"] for row in edge] == [1, 2, 3]
    results["row_counts_match_each_file"] = summary["provider_of_services"]["rows"] == 4 and summary["hai_hospital"]["rows"] == 4

    # [19] [20] Header layouts form one table; a column a file lacks stays null, never an empty string.
    hai_rows = spark.table("bronze.hai_hospital").collect()
    results["header_layouts_union_with_nulls"] = sum(row["provider_id"] is not None for row in hai_rows) == 2 and all(
        row["city_town"] is None for row in hai_rows if row["provider_id"]
    )

    # [46] Scratch copies are removed after each commit.
    results["scratch_files_removed"] = bool(storage.downloads) and not any(path.exists() for path in storage.downloads)

    # [21] [22] [23] A rerun replaces each object's rows in place.
    before = sorted(json.dumps(row.asDict(), sort_keys=True, default=str) for row in spark.table("bronze.provider_of_services").collect())
    bronze.load_objects(spark, inputs, storage, "bronze", scratch)
    after = sorted(json.dumps(row.asDict(), sort_keys=True, default=str) for row in spark.table("bronze.provider_of_services").collect())
    results["rerun_replaces_rows_without_duplicates"] = before == after

    # [41] Verification compares distinct files and rows with the table map's expected counts.
    checks = bronze.verify(spark, config, inputs, "bronze")
    results["verification_matches_expected_counts"] = checks["hai_hospital"]["passed"] and checks["provider_of_services"]["passed"]
    strict = bronze.load_table_map(table_map(root / "strict.json", [{**tables[0], "expected_distinct_rows": 99}, tables[1]]))
    results["verification_reports_a_count_mismatch"] = not bronze.verify(spark, strict, inputs, "bronze")["hai_hospital"]["passed"]

    # [9] [17] [18] [44] Checksums, ragged rows, invalid UTF-8 and byte-order marks fail before anything is written.
    snapshots = spark.table("bronze.provider_of_services.snapshots").count()
    target = next(item for item in inputs if item["table"] == "provider_of_services")
    results["checksum_mismatch_stops_before_write"] = (
        expect_error(lambda: bronze.load_objects(spark, [dict(target, sha256="0" * 64)], storage, "bronze", scratch), "SHA-256")
        and spark.table("bronze.provider_of_services.snapshots").count() == snapshots
    )
    for name, data, fragment in (
        ("ragged", b"A,B\r\n1,2,3\r\n", "fields"),
        ("utf8", b"A,B\r\n\xff,2\r\n", "UTF-8"),
        ("bom", b"\xef\xbb\xbfA,B\r\n1,2\r\n", "byte-order"),
    ):
        path = scratch / f"{name}.csv"
        path.write_bytes(data)
        results[f"validation_rejects_{name}"] = expect_error(partial(bronze.validate_csv, path), fragment)
    # [50] [51] Windows-1252 decodes only where the table allows it; undefined bytes and UTF-8-only tables still fail by name.
    cp1252 = csv_bytes(["Facility Name", "Note"], [["Saint Mary\u2019s", "A \u2013 B"]]).decode().encode("cp1252")
    store(storage, "cms/hospital_ownership", "cms_chow", "snap_g", [(None, "CHOW.csv", cp1252)])
    store(storage, "cms/hospital_ownership", "enroll", "snap_h", [(None, "ENROLL.csv", cp1252)])
    encoded: list[dict[str, Any]] = [
        {
            "table": "chow",
            "group": "structure",
            "collection": "cms/hospital_ownership",
            "dataset_ids": ["cms_chow"],
            "format": "csv",
            "encodings": ["utf-8", "cp1252"],
        },
        {"table": "enroll", "group": "structure", "collection": "cms/hospital_ownership", "dataset_ids": ["enroll"], "format": "csv"},
    ]
    encoded_inputs, _ = bronze.discover(storage, BUCKET, bronze.load_table_map(table_map(root / "encoded.json", encoded)), ["chow", "enroll"])
    bronze.load_objects(spark, [item for item in encoded_inputs if item["table"] == "chow"], storage, "bronze", scratch)
    chow = spark.table("bronze.chow").collect()
    results["cp1252_decoded_where_allowed"] = [(row["facility_name"], row["note"], row["_source_encoding"]) for row in chow] == [
        ("Saint Mary\u2019s", "A \u2013 B", "cp1252")
    ]
    results["utf8_only_table_refuses_cp1252_by_name"] = expect_error(
        lambda: bronze.load_objects(spark, [item for item in encoded_inputs if item["table"] == "enroll"], storage, "bronze", scratch), "ENROLL.csv"
    )
    undefined = scratch / "undefined.csv"
    undefined.write_bytes(b"A,B\r\n\x81,2\r\n")
    results["undefined_byte_fails_in_every_encoding"] = expect_error(partial(bronze.validate_csv, undefined, None, ("utf-8", "cp1252")), "not valid")
    results["unknown_encoding_refused"] = expect_error(
        lambda: bronze.load_table_map(table_map(root / "enc_bad.json", [{**encoded[1], "encodings": ["latin-1"]}])), "encoding"
    )

    # [52] A storage error while loading an object names the table, snapshot and file.
    class RefusingStorage(FakeStorage):
        def download(self, bucket: str, key: str, version_id: str, path: Path) -> None:
            raise OSError("refused")

    refusing = RefusingStorage()
    refusing.objects = storage.objects
    chow_item = next(item for item in encoded_inputs if item["table"] == "chow")
    results["storage_error_names_the_file"] = expect_error(
        lambda: bronze.load_objects(spark, [chow_item], refusing, "bronze", scratch), "chow snap_g CHOW.csv: OSError"
    )
    # [68] [69] [70] [71] Text files load one row per line with its terminator and rebuild byte for byte.
    text_cp1252 = b"PROV  NAME\tX\r\n010001  Saint Mary\x92s \x96 East\r\n\r\nlone\rcarriage\n  padded  \x0c form feed\x1a"
    store(storage, "cms/file_formats", "text_a", "snap_t1", [(["IMPFIL94.TXT"], "IMPFIL94.TXT", text_cp1252)])
    store(storage, "cms/file_formats", "text_b", "snap_t2", [(["impact.csv"], "impact.csv", b"a,b\nc,d\n")])
    store(storage, "cms/file_formats", "text_c", "snap_t3", [(["bad.txt"], "bad.txt", b"A\x81B\n")])
    store(storage, "cms/file_formats", "text_d", "snap_t4", [(["empty.txt"], "empty.txt", b"")])
    workbook = scratch / "fixture.xlsx"
    write_workbook(workbook)
    store(storage, "cms/file_formats", "sheet_a", "snap_x1", [(["impact.xlsx"], "impact.xlsx", workbook.read_bytes())])
    formats: list[dict[str, Any]] = [
        {
            "table": "file_text_lines",
            "group": "files",
            "collection": "cms/file_formats",
            "member_pattern": "(?i)\\.(txt|csv)$",
            "format": "text_lines",
            "encodings": ["utf-8", "cp1252"],
        },
        {"table": "file_sheet_rows", "group": "files", "collection": "cms/file_formats", "member_pattern": "(?i)\\.xlsx?$", "format": "sheet_rows"},
    ]
    file_inputs, _ = bronze.discover(storage, BUCKET, bronze.load_table_map(table_map(root / "formats.json", formats)), ["file_text_lines", "file_sheet_rows"])
    good = [item for item in file_inputs if item["snapshot_id"] in {"snap_t1", "snap_t2", "snap_x1"}]
    bronze.load_objects(spark, good, storage, "bronze", scratch)
    lines = spark.table("bronze.file_text_lines").where("_snapshot_id = 'snap_t1'").orderBy("_row_number").collect()
    rebuilt = b"".join((row["line_text"] + row["line_terminator"]).encode(row["_source_encoding"]) for row in lines)
    results["text_lines_rebuild_byte_for_byte"] = rebuilt == text_cp1252 and len(lines) == 6 and lines[0]["_source_encoding"] == "cp1252"
    results["text_lines_keep_padding_and_control_characters"] = lines[5]["line_text"] == "  padded  \x0c form feed\x1a" and lines[5]["line_terminator"] == ""
    trailing = spark.table("bronze.file_text_lines").where("_snapshot_id = 'snap_t2'").orderBy("_row_number").collect()
    results["final_terminator_adds_no_phantom_line"] = [(row["line_text"], row["line_terminator"]) for row in trailing] == [("a,b", "\n"), ("c,d", "\n")]
    by_snapshot = {item["snapshot_id"]: item for item in file_inputs}
    results["text_undefined_byte_refused_by_name"] = expect_error(
        lambda: bronze.load_objects(spark, [by_snapshot["snap_t3"]], storage, "bronze", scratch), "bad.txt: the file is not valid"
    )
    results["empty_text_file_refused"] = expect_error(lambda: bronze.load_objects(spark, [by_snapshot["snap_t4"]], storage, "bronze", scratch), "empty")
    # [72] [73] Excel rows keep sheet, row position, each cell's stored value as text and its type.
    sheet_rows = spark.table("bronze.file_sheet_rows").orderBy("_row_number").collect()
    seen = [(row["sheet_name"], row["sheet_index"], row["sheet_row"], trim(row["cells"]), trim_to(row["cell_types"], row["cells"])) for row in sheet_rows]
    results["excel_cells_keep_text_and_type"] = seen == EXPECTED_SHEET_ROWS
    results["colliding_headers_fail"] = expect_error(lambda: bronze.column_names(["Facility ID", "Facility_ID"]), "collide")
    # [81] Headers that begin with an underscore (SAS automatic variables) map to u_ and never reach a lineage name.
    results["underscore_headers_map_clear_of_lineage"] = bronze.column_names(["_row_number", "_NAME_", "Name"]) == ["u_row_number", "u_name", "name"]
    # [82] SAS names map by lowercasing only, so names that differ by a trailing underscore stay apart.
    results["sas_names_keep_trailing_underscores"] = bronze.sas_column_names(["E_A_1", "E_A_1_", "_NAME_"]) == ["e_a_1", "e_a_1_", "u_name_"]
    results["underscore_collisions_refused"] = expect_error(lambda: bronze.column_names(["_NAME_", "u name"]), "collide")
    mapping = {(row["original_header"], row["column_name"]) for row in spark.table("bronze.column_map").collect()}
    results["column_map_records_original_headers"] = ("City/Town", "city_town") in mapping and ("Facility ID", "facility_id") in mapping
    results.update(dictionary_scenarios(spark, root, storage, scratch, formats))
    results.update(publisher_dictionary_scenarios(spark, root, storage, scratch))
    results.update(county_scenarios(spark, root, storage, scratch))
    results.update(remaining_scenarios(spark, root, storage, scratch))
    return results


# Two editions of an owners-style dictionary with spacer columns, a page break, a repeated header and an en dash [102]-[106].
OWNERS_HEADER = ["", "Term Name", "", "Variable Name", "", "Description", "Type"]
EDITION_OLD: list[dict[str, Any]] = [
    {
        "table": [
            OWNERS_HEADER,
            ["Enrollment ID", "", "", "ENROLLMENT ID", "", "Old text.", "CHAR"],
            ["Associate ID", "", "", "ASSOCIATE ID", "", "PAC ID.", "CHAR"],
        ]
    }
]
# The newer edition uses the CMS three-cell layout: each header label sits one cell right of its row values [112].
SHIFTED_HEADER = ["", "Term Name", "", "", "Variable Name", "", "", "Description", "", "", "Type", ""]
EDITION_NEW: list[dict[str, Any]] = [
    {
        "table": [
            ["Owners Data Dictionary", "", "", "", "", "", "", "", "", "", "", ""],
            SHIFTED_HEADER,
            ["Enrollment ID", "", "", "ENROLLMENT ID", "", "", "New text", "", "", "CHAR", "", ""],
        ]
    },
    {
        "table": [
            SHIFTED_HEADER,
            ["", "", "", "", "", "", "continued.", "", "", "", "", ""],
            ["Owner Non-Profit Flag", "", "", "NON PROFIT \u2013 OWNER", "", "", "Flag.", "", "", "CHAR", "", ""],
        ],
        "lines": ["Page 2 of 2"],
    },
]
# A Care Compare-style file table: types and column names, no per-column description [108].
CARE_COMPARE: list[dict[str, Any]] = [
    {
        "table": [
            ["Table", "HAI - Hospital"],
            ["File Name", "HEALTHCARE_ASSOCIATED_INFECTIONS-"],
            ["", "HOSPITAL.CSV"],
            ["Data Type", "Column Name - CSV"],
            ["Char(6)", "Facility ID"],
            ["Char(13)", "Score"],
            ["Table", "Other file"],
            ["File Name", "OTHER.CSV"],
            ["Data Type", "Column Name - CSV"],
            ["Num(8)", "Score"],
        ]
    },
    {"lines": ["Text-only layout page", "FIELD  START  LENGTH"]},
]


def publisher_dictionary_scenarios(spark: Any, root: Path, storage: FakeStorage, scratch: Path) -> dict[str, bool]:
    """Dictionary PDFs load as published rows; descriptions come from the newest edition that defines a column [97] to [110]."""
    results: dict[str, bool] = {}
    owners = csv_bytes(["ENROLLMENT ID", "ASSOCIATE ID", "NON PROFIT - OWNER", "EXTRA COLUMN"], [["1", "2", "Y", "x"]])
    store(storage, "cms/owners", "cms_owners", "snap_o1", [(None, "owners.csv", owners)], release="2026-02-01")
    store(storage, "cms/owners", "cms_owners", "snap_d1", [(None, "Owners_Data_Dictionary.pdf", pdf_document(EDITION_OLD))], "2022-01-01", "dictionary")
    store(storage, "cms/owners", "cms_owners", "snap_d2", [(None, "Owners_Data_Dictionary.pdf", pdf_document(EDITION_NEW))], "2024-01-01", "methodology")
    store(
        storage,
        "cms/owners",
        "cms_owners",
        "snap_d3",
        [(None, "Owners_Data_Dictionary_no_extension", pdf_document([*EDITION_OLD, {"lines": ["copy"]}]))],
        "2022-01-01",
        "dictionary",
    )
    care = pdf_document(CARE_COMPARE)
    store(
        storage,
        "cms/hospitals",
        "cms_dictionaries",
        "snap_c1",
        [(["a.zip", "HOSPITAL_Data_Dictionary.pdf"], "HOSPITAL_Data_Dictionary.pdf", care)],
        "2025-01-01",
        "reference",
    )
    store(
        storage,
        "cms/hospitals",
        "cms_dictionaries",
        "snap_c2",
        [(["b.zip", "HOSPITAL_Data_Dictionary.pdf"], "HOSPITAL_Data_Dictionary.pdf", care)],
        "2025-04-01",
        "reference",
    )
    # [111] The same archive snapshot lists its dictionary in a second dataset's manifest under the same member path.
    store(
        storage,
        "cms/hospitals",
        "cms_other_dataset",
        "snap_c1",
        [(["a.zip", "HOSPITAL_Data_Dictionary.pdf"], "HOSPITAL_Data_Dictionary.pdf", care)],
        "2025-01-01",
        "dictionary",
    )
    roles = ["dictionary", "methodology", "reference"]
    tables: list[dict[str, Any]] = [
        {
            "table": "owners",
            "group": "pub",
            "collection": "cms/owners",
            "dataset_ids": ["cms_owners"],
            "format": "csv",
            "dictionary": {"table": "owners_dictionaries", "file_pattern": "(?i)owners_data_dictionary"},
        },
        {
            "table": "owners_dictionaries",
            "group": "pub",
            "collection": "cms/owners",
            "member_pattern": "(?i)data_dictionary",
            "format": "pdf_rows",
            "roles": roles,
        },
        {
            "table": "care_dictionaries",
            "group": "pub",
            "collection": "cms/hospitals",
            "member_pattern": "(?i)HOSPITAL_Data_Dictionary\\.pdf$",
            "format": "pdf_rows",
            "roles": roles,
        },
        {
            "table": "hai_hospital",
            "group": "hai",
            "collection": "cms/hospitals",
            "member_pattern": "(?:^|/)(?:Healthcare[ _]Associated[ _]Infections ?- ?Hospital|(?:[a-z0-9_]*_)?77hc[-_]ibv8)\\.csv$",
            "format": "csv",
            "dictionary": {
                "table": "care_dictionaries",
                "file_pattern": "(?i)HOSPITAL_Data_Dictionary",
                "section_pattern": "(?i)^HEALTHCARE_ASSOCIATED_INFECTIONS-HOSPITAL\\.CSV$",
            },
        },
    ]
    config = bronze.load_table_map(table_map(root / "publisher.json", tables))
    inputs, _ = bronze.discover(storage, BUCKET, config, ["owners", "owners_dictionaries", "care_dictionaries"])
    by_table: dict[str, list[dict[str, Any]]] = {}
    for item in inputs:
        by_table.setdefault(item["table"], []).append(item)
    # [101] Data tables never take dictionary objects; dictionary tables take only their roles.
    results["data_table_skips_dictionary_roles"] = [item["file_name"] for item in by_table["owners"]] == ["owners.csv"]
    # [99] [100] One copy per distinct file, chosen by content; a name without an extension is accepted.
    results["shared_member_across_manifests_loads_once"] = sum(item["snapshot_id"] == "snap_c1" for item in inputs) == 1
    results["identical_copies_load_once"] = len(by_table["care_dictionaries"]) == 1 and len(by_table["owners_dictionaries"]) == 3
    results["pdf_found_by_content_not_extension"] = "Owners_Data_Dictionary_no_extension" in {item["file_name"] for item in by_table["owners_dictionaries"]}
    summary = bronze.load_objects(spark, inputs, storage, "bronze", scratch)
    rows = spark.table("bronze.owners_dictionaries").where("_snapshot_id = 'snap_d2'").orderBy("_row_number").collect()
    # [97] [110] Table rows and text lines are both kept, with the reader recorded.
    kinds = {row["row_kind"] for row in rows}
    texts = [row["cells"][0] for row in rows if row["row_kind"] == "text"]
    results["pdf_tables_and_text_kept"] = kinds == {"table", "text"} and "Page 2 of 2" in texts and summary["owners_dictionaries"]["rows"] > 0
    results["en_dash_read_as_published"] = any("NON PROFIT \u2013 OWNER" in (cell or "") for row in rows for cell in row["cells"])
    results["reader_recorded"] = {row["reader"] for row in rows} == {"pdfplumber 0.11.10"}
    # [98] Non-PDF and text-free files are refused by name.
    store(storage, "cms/bad", "bad", "snap_b1", [(None, "bad_Data_Dictionary.pdf", b"hello")], "2022-01-01", "dictionary")
    store(storage, "cms/bad", "bad", "snap_b2", [(None, "scan_Data_Dictionary.pdf", pdf_document([{}]))], "2022-01-01", "dictionary")
    bad_map = bronze.load_table_map(table_map(root / "bad.json", [{**tables[1], "table": "bad_dictionaries", "collection": "cms/bad"}]))
    bad_inputs, _ = bronze.discover(storage, BUCKET, bad_map, ["bad_dictionaries"])
    by_name = {item["file_name"]: item for item in bad_inputs}
    results["non_pdf_refused_by_name"] = expect_error(
        lambda: bronze.load_objects(spark, [by_name["bad_Data_Dictionary.pdf"]], storage, "bronze", scratch), "bad_Data_Dictionary.pdf: not a PDF"
    )
    results["text_free_pdf_refused"] = expect_error(
        lambda: bronze.load_objects(spark, [by_name["scan_Data_Dictionary.pdf"]], storage, "bronze", scratch), "no text"
    )
    # [102] to [108] Descriptions from the newest edition, continuation joined, exact matches only, types kept apart.
    dictionary.build(spark, config, ["owners", "hai_hospital"], "bronze")
    described = {row["column_name"]: row.asDict() for row in spark.table("bronze_dictionary.owners").collect()}
    enrollment = described["enrollment_id"]
    results["newest_edition_with_continuation"] = (enrollment["description"], enrollment["description_variants"]) == ("New text continued.", 2)
    results["description_names_file_and_edition"] = enrollment["description_source"] == "publisher dictionary Owners_Data_Dictionary.pdf (2024-01-01)"
    results["older_edition_fills_its_columns"] = described["associate_id"]["description"] == "PAC ID."
    results["dash_variants_match_exactly"] = described["non_profit_owner"]["description"] == "Flag."
    results["unmatched_column_has_no_description"] = described["extra_column"]["description"] is None
    results["publisher_type_kept"] = enrollment["publisher_type"] == "CHAR"
    care_rows = {row["column_name"]: row.asDict() for row in spark.table("bronze_dictionary.hai_hospital").collect()}
    results["type_only_dictionary_gives_type_not_description"] = (care_rows["facility_id"]["publisher_type"], care_rows["facility_id"]["description"]) == (
        "Char(6)",
        None,
    )
    # [108] Types come from the HAI file's own section, not another file's column of the same name.
    results["type_from_the_file_section"] = care_rows["score"]["publisher_type"] == "Char(13)"
    return results


BOM = b"\xef\xbb\xbf"
EXPORT_HEADER = ["GEO_ID", "NAME", "DP02_0001E"]
EXPORT_LABELS = ["Geography", "Geographic Area Name", "Estimate!!HOUSEHOLDS BY TYPE!!Total households"]
SAHIE = b"Filename:  sahie_2022.csv\nCreated:   12JUL24  02:44\n \nyear,version,statefips,countyfips,\n2022,1,01,001,\n"
GROUP_JSON = {"variables": {"DP02_0001E": {"label": "Estimate!!HOUSEHOLDS BY TYPE!!Total households", "concept": "SOCIAL", "predicateType": "int"}}}


def county_scenarios(spark: Any, root: Path, storage: FakeStorage, scratch: Path) -> dict[str, bool]:
    """County-context layouts: byte-order marks, label rows, pipes, preambles, capture selectors and CSV and JSON dictionaries [113] to [127]."""
    results: dict[str, bool] = {}
    export = BOM + csv_bytes(EXPORT_HEADER, [EXPORT_LABELS, ["0500000US01001", "Autauga County, Alabama", "22308"]])
    no_labels = BOM + csv_bytes(EXPORT_HEADER, [["0500000US01003", "Baldwin County, Alabama", "87190"]])
    store(storage, "census/acs", "acs_dp02", "ACS_EXPORT__a", [(["x.zip", "ACSDP5Y2024.DP02-Data.csv"], "ACSDP5Y2024.DP02-Data.csv", export)])
    store(storage, "census/acs", "acs_dp03", "ACS_EXPORT__b", [(["y.zip", "ACSDP5Y2024.DP03-Data.csv"], "ACSDP5Y2024.DP03-Data.csv", no_labels)])
    summary = b"#GEO_ID|B16005_001E|B16005_001M\n0500000US01001|100|5\n"
    store(storage, "census/acs", "acs", "ACS_SUMMARY__c", [(None, "acsdt5y2018-b16005.dat", summary)])
    metadata = BOM + csv_bytes(["Column Name", "Label"], [["GEO_ID", "Geography"], ["B16005_001E", "Estimate!!Total:"]])
    store(storage, "census/acs", "acs_b16005", "ACS_EXPORT__d", [(["z.zip", "ACSDT5Y2018.B16005-Column-Metadata.csv"], "m.csv", metadata)], role="reference")
    profile = csv_bytes(["DP02_0001E", "state", "county"], [["22308", "01", "001"]])
    detailed = csv_bytes(["B16001_001E", "state", "county"], [["55000", "01", "001"]])
    store(storage, "census/acs", "acs", "Census_API__e", [(None, "observations.csv", profile)])
    store(storage, "census/acs", "acs", "Census_Detailed__f", [(None, "observations.csv", detailed)])
    store(storage, "census/acs", "acs", "Census_API__e", [(None, "group.json", json.dumps(GROUP_JSON).encode())], role="dictionary")
    store(storage, "census/sahie", "sahie", "SAHIE__g", [(None, "sahie_2022.csv", SAHIE)])
    store(storage, "census/sahie", "sahie", "SAHIE__h", [(None, "sahie_2023.csv", b"Filename: none\n2023,1,01,001,\n")])
    store(storage, "census/county_adjacency", "adj", "ADJ__i", [(None, "county_adjacency2024.txt", b"County Name|County GEOID\nAutauga County, AL|01001\n")])
    store(storage, "cdc/wonder_mortality", "wonder", "WONDER__j", [(None, "wonder_help_ucd.html", b"<html><body>Deaths</body></html>\n")], role="dictionary")
    places = csv_bytes(["MeasureID", "Measure full name"], [["ACCESS2", "Current lack of health insurance"]])
    store(storage, "cdc/places", "places", "PLACES__k", [(None, "rows.csv", places)], role="methodology")
    roles = ["dictionary", "methodology", "reference"]
    first = {"first_label": "Geography"}
    tables: list[dict[str, Any]] = [
        {
            "table": "acs_dp02",
            "group": "c",
            "collection": "census/acs",
            "dataset_ids": ["acs_dp02"],
            "format": "csv",
            "byte_order_mark": True,
            "label_row": first,
        },
        {
            "table": "acs_dp03",
            "group": "c",
            "collection": "census/acs",
            "dataset_ids": ["acs_dp03"],
            "format": "csv",
            "byte_order_mark": True,
            "label_row": first,
        },
        {
            "table": "acs_summary_b16005",
            "group": "c",
            "collection": "census/acs",
            "member_pattern": "^acsdt5y\\d{4}-b16005\\.dat$",
            "format": "csv",
            "delimiter": "|",
            "dictionary": {"table": "acs_documents_csv", "file_pattern": "-Column-Metadata\\.csv$"},
        },
        {
            "table": "acs_api_profile",
            "group": "c",
            "collection": "census/acs",
            "member_pattern": "^observations\\.csv$",
            "snapshot_pattern": "^Census_API__",
            "format": "csv",
            "dictionary": {"table": "acs_documents_json", "file_pattern": "^group\\.json$"},
        },
        {
            "table": "acs_api_detailed",
            "group": "c",
            "collection": "census/acs",
            "member_pattern": "^observations\\.csv$",
            "snapshot_pattern": "^Census_Detailed__",
            "format": "csv",
        },
        {
            "table": "acs_documents_csv",
            "group": "c",
            "collection": "census/acs",
            "member_pattern": "\\.csv$",
            "format": "csv_rows",
            "byte_order_mark": True,
            "roles": roles,
        },
        {"table": "acs_documents_json", "group": "c", "collection": "census/acs", "member_pattern": "\\.json$", "format": "json_variables", "roles": roles},
        {
            "table": "sahie",
            "group": "c",
            "collection": "census/sahie",
            "member_pattern": "^sahie_\\d{4}\\.csv$",
            "format": "csv",
            "preamble": {"header_pattern": "^year,"},
            "unnamed_headers": True,
        },
        {
            "table": "county_adjacency",
            "group": "c",
            "collection": "census/county_adjacency",
            "member_pattern": "^county_adjacency20\\d\\d\\.txt$",
            "format": "csv",
            "delimiter": "|",
        },
        {
            "table": "wonder_documents_text",
            "group": "c",
            "collection": "cdc/wonder_mortality",
            "member_pattern": "\\.html$",
            "format": "text_lines",
            "roles": roles,
        },
        {"table": "places_documents_csv", "group": "c", "collection": "cdc/places", "member_pattern": "\\.csv$", "format": "csv_rows", "roles": roles},
    ]
    config = bronze.load_table_map(table_map(root / "county.json", tables))
    names = [table["table"] for table in tables]
    inputs, unselected = bronze.discover(storage, BUCKET, config, names)
    by_table: dict[str, list[dict[str, Any]]] = {}
    for item in inputs:
        by_table.setdefault(item["table"], []).append(item)
    # [119] Same-named files from different captures go to the table whose snapshot pattern matches.
    results["snapshot_pattern_splits_same_named_files"] = [item["snapshot_id"] for item in by_table["acs_api_profile"]] == ["Census_API__e"] and [
        item["snapshot_id"] for item in by_table["acs_api_detailed"]
    ] == ["Census_Detailed__f"]
    loose = [*tables, {"table": "acs_api_any", "group": "c", "collection": "census/acs", "member_pattern": "^observations\\.csv$", "format": "csv"}]
    results["snapshot_overlap_still_refused"] = expect_error(
        lambda: bronze.discover(storage, BUCKET, bronze.load_table_map(table_map(root / "loose.json", loose)), ["acs_api_profile", "acs_api_any"]),
        "two tables",
    )
    results["county_inputs_all_selected"] = not [entry for entry in unselected if entry["collection"].startswith(("census/", "cdc/"))]
    failing = {"acs_dp03", "sahie"}
    loadable = [item for item in inputs if item["table"] not in failing and item["snapshot_id"] != "SAHIE__h"]
    bronze.load_objects(spark, loadable, storage, "bronze", scratch)
    # [113] [114] The mark is removed, the label row becomes labels, and row numbers count data rows only.
    rows = spark.table("bronze.acs_dp02").collect()
    results["byte_order_mark_removed_and_label_row_kept_as_labels"] = [(row["geo_id"], row["dp02_0001e"], row["_row_number"]) for row in rows] == [
        ("0500000US01001", "22308", 1)
    ]
    labels = {row["column_name"]: row["original_label"] for row in spark.table("bronze.column_map").where("table_name = 'acs_dp02'").collect()}
    results["labels_in_column_map"] = labels == dict(zip(["geo_id", "name", "dp02_0001e"], EXPORT_LABELS, strict=True))
    # [115] A file without its label row is refused by name instead of losing a data row.
    dp03 = by_table["acs_dp03"]
    results["missing_label_row_refused"] = expect_error(lambda: bronze.load_objects(spark, dp03, storage, "bronze", scratch), "label row")
    # [113] Without the declared option a byte-order mark is still refused.
    plain = bronze.load_table_map(table_map(root / "plain.json", [{**tables[0], "byte_order_mark": False, "label_row": first}]))
    plain_inputs, _ = bronze.discover(storage, BUCKET, plain, ["acs_dp02"])
    results["undeclared_byte_order_mark_refused"] = expect_error(
        lambda: bronze.load_objects(spark, plain_inputs, storage, "bronze", scratch), "byte-order mark"
    )
    # [116] [120] Pipe files named .dat or .txt load as columns.
    summary_rows = spark.table("bronze.acs_summary_b16005").collect()
    results["pipe_dat_loads_columns"] = [(row["geo_id"], row["b16005_001e"], row["b16005_001m"]) for row in summary_rows] == [("0500000US01001", "100", "5")]
    adjacency = spark.table("bronze.county_adjacency").collect()
    results["pipe_txt_loads_columns"] = [(row["county_name"], row["county_geoid"]) for row in adjacency] == [("Autauga County, AL", "01001")]
    # [117] [118] Preamble lines are stored with their numbers; the trailing empty header gets a positional name.
    sahie = [item for item in by_table["sahie"] if item["snapshot_id"] == "SAHIE__g"]
    bronze.load_objects(spark, sahie, storage, "bronze", scratch)
    sahie_rows = spark.table("bronze.sahie").collect()
    results["preamble_skipped_and_unnamed_header_kept"] = [(row["year"], row["countyfips"], row["unnamed_5"], row["_row_number"]) for row in sahie_rows] == [
        ("2022", "001", "", 1)
    ]
    preamble = spark.table("bronze.file_preambles").where("table_name = 'sahie'").orderBy("line_number").collect()
    results["preamble_lines_stored"] = [(row["line_number"], row["line_text"]) for row in preamble] == [
        (1, "Filename:  sahie_2022.csv"),
        (2, "Created:   12JUL24  02:44"),
        (3, " "),
    ]
    missing = [item for item in by_table["sahie"] if item["snapshot_id"] == "SAHIE__h"]
    results["missing_header_after_preamble_refused"] = expect_error(lambda: bronze.load_objects(spark, missing, storage, "bronze", scratch), "header pattern")
    results["undeclared_empty_header_refused"] = expect_error(lambda: bronze.column_names(["year", ""]), "no letters or digits")
    # [121] [123] HTML loads as text lines; CSV documents keep every row's cells, header included.
    html = spark.table("bronze.wonder_documents_text").collect()
    results["html_document_loads_as_text_lines"] = [row["line_text"] for row in html] == ["<html><body>Deaths</body></html>"]
    cells = [list(row["cells"]) for row in spark.table("bronze.places_documents_csv").orderBy("_row_number").collect()]
    results["csv_document_rows_kept_as_cells"] = cells == [["MeasureID", "Measure full name"], ["ACCESS2", "Current lack of health insurance"]]
    variables = spark.table("bronze.acs_documents_json").collect()
    results["json_variables_stored"] = [(row["variable_name"], row["attributes"]["label"], row["attributes"]["predicateType"]) for row in variables] == [
        ("DP02_0001E", GROUP_JSON["variables"]["DP02_0001E"]["label"], "int")
    ]
    store(storage, "census/bad_json", "acs", "ACS_BAD__l", [(None, "group.json", b'{"fields": {}}')], role="dictionary")
    bad_json = bronze.load_table_map(table_map(root / "bad_json.json", [{**tables[6], "collection": "census/bad_json"}]))
    bad_inputs, _ = bronze.discover(storage, BUCKET, bad_json, ["acs_documents_json"])
    results["json_without_variables_refused"] = expect_error(lambda: bronze.load_objects(spark, bad_inputs, storage, "bronze", scratch), "variables")
    # [114] [124] [125] [127] Descriptions from label rows, CSV column metadata and JSON variable labels.
    dictionary.build(spark, config, ["acs_dp02", "acs_summary_b16005", "acs_api_profile"], "bronze")
    dp02 = {row["column_name"]: row.asDict() for row in spark.table("bronze_dictionary.acs_dp02").collect()}
    results["label_row_gives_description"] = (dp02["dp02_0001e"]["description"], dp02["dp02_0001e"]["description_source"]) == (
        EXPORT_LABELS[2],
        dictionary.PUBLISHER,
    )
    summary_dictionary = {row["column_name"]: row.asDict() for row in spark.table("bronze_dictionary.acs_summary_b16005").collect()}
    results["column_metadata_csv_gives_description"] = (
        summary_dictionary["b16005_001e"]["description"] == "Estimate!!Total:"
        and summary_dictionary["b16005_001e"]["description_source"] == "publisher dictionary ACSDT5Y2018.B16005-Column-Metadata.csv (2026-01-01)"
        and summary_dictionary["b16005_001m"]["description"] is None
    )
    profile_dictionary = {row["column_name"]: row.asDict() for row in spark.table("bronze_dictionary.acs_api_profile").collect()}
    results["json_label_gives_description_and_type"] = (
        profile_dictionary["dp02_0001e"]["description"],
        profile_dictionary["dp02_0001e"]["publisher_type"],
    ) == (
        GROUP_JSON["variables"]["DP02_0001E"]["label"],
        "int",
    )
    # [130] Undoubled inner quotes in a label row and in a document line are read by the declared rule; data rows stay strict.
    s0601 = BOM + (b'"GEO_ID","S0601_C01_025E",\r\n"Geography","Total!!Speak English "very well"",\r\n"0500000US01001","12.5",\r\n')
    store(storage, "census/quotes", "acs_s0601", "ACS_EXPORT__m", [(["q.zip", "ACSST5Y2010.S0601-Data.csv"], "s.csv", s0601)])
    s0601_metadata = BOM + b'"Column Name","Label"\r\n"S0601_C01_025E","Total!!Speak English "very well""\r\n'
    store(
        storage,
        "census/quotes",
        "acs_s0601",
        "ACS_EXPORT__m",
        [(["q.zip", "ACSST5Y2010.S0601-Column-Metadata.csv"], "c.csv", s0601_metadata)],
        role="reference",
    )
    quoted = {"byte_order_mark": True, "quoted_fields": True}
    quote_tables: list[dict[str, Any]] = [
        {
            "table": "acs_s0601",
            "group": "c",
            "collection": "census/quotes",
            "dataset_ids": ["acs_s0601"],
            "format": "csv",
            "label_row": first,
            "unnamed_headers": True,
            **quoted,
        },
        {
            "table": "acs_quote_documents",
            "group": "c",
            "collection": "census/quotes",
            "member_pattern": "\\.csv$",
            "format": "csv_rows",
            "roles": roles,
            **quoted,
        },
    ]
    quote_config = bronze.load_table_map(table_map(root / "quotes.json", quote_tables))
    quote_inputs, _ = bronze.discover(storage, BUCKET, quote_config, ["acs_s0601", "acs_quote_documents"])
    bronze.load_objects(spark, quote_inputs, storage, "bronze", scratch)
    s0601_rows = spark.table("bronze.acs_s0601").collect()
    s0601_labels = {row["column_name"]: row["original_label"] for row in spark.table("bronze.column_map").where("table_name = 'acs_s0601'").collect()}
    results["undoubled_quotes_in_label_row_kept"] = (
        [(row["geo_id"], row["s0601_c01_025e"], row["unnamed_3"]) for row in s0601_rows] == [("0500000US01001", "12.5", "")]
        and s0601_labels["s0601_c01_025e"] == 'Total!!Speak English "very well"'
        and s0601_labels["unnamed_3"] == ""
    )
    quote_cells = [list(row["cells"]) for row in spark.table("bronze.acs_quote_documents").orderBy("_row_number").collect()]
    results["undoubled_quotes_in_document_kept"] = quote_cells == [["Column Name", "Label"], ["S0601_C01_025E", 'Total!!Speak English "very well"']]
    strict = bronze.load_table_map(table_map(root / "strict.json", [{**quote_tables[0], "quoted_fields": False}]))
    strict_inputs, _ = bronze.discover(storage, BUCKET, strict, ["acs_s0601"])
    results["undeclared_undoubled_quotes_refused"] = expect_error(
        lambda: bronze.load_objects(spark, strict_inputs, storage, "bronze", scratch), "not valid CSV"
    )
    results["unquoted_field_breaks_the_rule"] = file_readers.split_quoted_line('"a",b"c"', ",") is None and file_readers.split_quoted_line(
        '"a","b "c"",', ","
    ) == [
        "a",
        'b "c"',
        "",
    ]
    # [126] A data table may not link to a text-lines document table.
    linked = [{**tables[9]}, {"table": "wonder", "group": "c", "collection": "cdc/wonder_mortality", "member_pattern": "\\.csv$", "format": "csv"}]
    linked[1]["dictionary"] = {"table": "wonder_documents_text", "file_pattern": "html"}
    results["text_dictionary_link_refused"] = expect_error(lambda: bronze.load_table_map(table_map(root / "linked.json", linked)), "dictionary")
    return results


# A Word body: a paragraph with a tab and a line break, an empty paragraph, a two-row table (one cell holding two
# paragraphs), a content control holding a paragraph, and tab stops in paragraph properties that are not text [133].
W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
DOCX_BODY = (
    '<w:p><w:pPr><w:tabs><w:tab w:val="left" w:pos="720"/></w:tabs></w:pPr>'
    '<w:r><w:t>Nurse</w:t><w:tab/><w:t xml:space="preserve">vacancy </w:t><w:br/><w:t>rate</w:t></w:r></w:p>'
    "<w:p/>"
    "<w:tbl><w:tr><w:tc><w:p><w:r><w:t>Hospital</w:t></w:r></w:p></w:tc><w:tc><w:p><w:r><w:t>Rate</w:t></w:r></w:p></w:tc></w:tr>"
    "<w:tr><w:tc><w:p><w:r><w:t>A</w:t></w:r></w:p><w:p><w:r><w:t>B</w:t></w:r></w:p></w:tc><w:tc><w:p/></w:tc></w:tr></w:tbl>"
    "<w:sdt><w:sdtContent><w:p><w:hyperlink><w:r><w:t>Linked</w:t></w:r></w:hyperlink><w:del><w:r><w:delText>gone</w:delText></w:r></w:del></w:p>"
    "</w:sdtContent></w:sdt>"
    "<w:sectPr/>"
)
EXPECTED_DOCX_ROWS = [
    ("paragraph", 1, None, None, ["Nurse\tvacancy \nrate"]),
    ("paragraph", 2, None, None, [""]),
    ("table", 3, 1, 1, ["Hospital", "Rate"]),
    ("table", 3, 1, 2, ["A\nB", ""]),
    ("paragraph", 4, None, None, ["Linked"]),
]


def docx_bytes(body: str, doctype: str = "", part: str = "word/document.xml") -> bytes:
    """Write a minimal Word package with the given body, optionally with a DOCTYPE or under another part name."""
    import zipfile

    document = f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>{doctype}<w:document xmlns:w="{W_NS}"><w:body>{body}</w:body></w:document>'
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", '<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"/>')
        archive.writestr(part, document)
    return buffer.getvalue()


def remaining_scenarios(spark: Any, root: Path, storage: FakeStorage, scratch: Path) -> dict[str, bool]:
    """Remaining sources: macro workbooks, Word documents, JSON documents, report PDFs, package parts, layouts and end-of-file markers [132]-[145]."""
    results: dict[str, bool] = {}
    workbook = root / "pivot.xlsm"
    write_workbook(workbook)
    upper = root / "upper.XLSX"
    write_workbook(upper)
    # A different archive comment keeps the two workbooks distinct files; identical bytes would load once [204].
    with zipfile.ZipFile(upper, "a") as archive:
        archive.comment = b"upper"
    store(
        storage,
        "state/finance",
        "ca",
        "CA__a",
        [
            (None, "pivot.xlsm", workbook.read_bytes()),
            (["pivot.xlsm", "[Content_Types].xml"], "member__Content_Types_.xml", b"<Types/>"),
            (["pivot.xlsm", "xl/printerSettings/printerSettings1.bin"], "printerSettings1.bin", b"\x00\x01"),
            (["pivot.xlsm", "xl/_rels/workbook.xml.rels"], "workbook.xml.rels", b"<Relationships/>"),
            (None, "HPSA.XLSX", upper.read_bytes()),
        ],
    )
    store(storage, "state/finance", "ca", "CA__b", [(None, "vacancy.docx", docx_bytes(DOCX_BODY))])
    store(storage, "state/finance", "ca", "CA__c", [(None, "datapackage.json", b'{\n  "name": "hafd",\n  "resources": []\n}\n')])
    report = pdf_document([{"table": [["Hospital", "RN FTE"], ["A", "12.5"]], "lines": ["Hospital nurse staffing 2014"]}])
    store(storage, "state/finance", "ca", "CA__d", [(None, "2014HNSS_Appendices.pdf", report)])
    store(storage, "state/finance", "ca", "CA__e", [(None, "columns.json", b'[{"name": "beds"}]')], role="methodology")
    page = csv_bytes(["FAC_ID", "BEDS"], [["106010735", "120"]])
    nonresp = csv_bytes(["FAC_ID", "REASON"], [["106010739", "closed"]])
    store(storage, "state/util", "hcai_util", "UTIL__f", [(None, "page_1_6.csv", page), (None, "nonresp_1_6.csv", nonresp)])
    roles = ["dictionary", "methodology", "reference"]
    tables: list[dict[str, Any]] = [
        {"table": "finance_sheet_rows", "group": "r", "collection": "state/finance", "member_pattern": "(?i)\\.(xlsx?|xlsm)$", "format": "sheet_rows"},
        {"table": "finance_documents_docx", "group": "r", "collection": "state/finance", "member_pattern": "(?i)\\.docx$", "format": "docx_rows"},
        {
            "table": "finance_text_lines",
            "group": "r",
            "collection": "state/finance",
            "member_pattern": "\\.json$",
            "format": "text_lines",
        },
        {"table": "finance_reports_pdf", "group": "r", "collection": "state/finance", "member_pattern": "(?i)\\.pdf$", "format": "pdf_rows"},
        {
            "table": "finance_documents_text",
            "group": "r",
            "collection": "state/finance",
            "member_pattern": "\\.json$",
            "format": "text_lines",
            "roles": roles,
        },
        {"table": "util_pages", "group": "r", "collection": "state/util", "member_pattern": "^page_\\d+_\\d+\\.csv$", "format": "csv"},
        {"table": "util_nonresponse", "group": "r", "collection": "state/util", "member_pattern": "^nonresp_\\d+_\\d+\\.csv$", "format": "csv"},
    ]
    config = bronze.load_table_map(table_map(root / "remaining.json", tables))
    names = [table["table"] for table in tables]
    inputs, unselected = bronze.discover(storage, BUCKET, config, names)
    by_table: dict[str, list[str]] = {}
    for item in inputs:
        by_table.setdefault(item["table"], []).append(item["file_name"])
    # [137] Package parts stay unselected and are listed by name; the whole workbook is selected.
    parts = sorted(entry["file_name"] for entry in unselected if entry["collection"] == "state/finance")
    results["package_parts_left_unselected"] = parts == ["[Content_Types].xml", "printerSettings1.bin", "workbook.xml.rels"]
    # [132] [141] Macro-enabled and upper-case workbooks select and load as sheet rows, like .xlsx.
    results["xlsm_and_upper_case_workbooks_selected"] = sorted(by_table.get("finance_sheet_rows", [])) == ["HPSA.XLSX", "pivot.xlsm"]
    bronze.load_objects(spark, inputs, storage, "bronze", scratch)
    sheet_rows = spark.table("bronze.finance_sheet_rows").where("_snapshot_id = 'CA__a'").collect()
    by_file: dict[str, list[Any]] = {}
    for row in sheet_rows:
        by_file.setdefault(row["_s3_key"].rsplit("/", 1)[-1], []).append(row)
    for name in ("pivot.xlsm", "HPSA.XLSX"):
        ordered = sorted(by_file.get(name, []), key=lambda row: row["_row_number"])
        seen = [(row["sheet_name"], row["sheet_index"], row["sheet_row"], trim(row["cells"]), trim_to(row["cell_types"], row["cells"])) for row in ordered]
        results[f"{'xlsm' if name.endswith('xlsm') else 'upper_case_xlsx'}_cells_kept"] = seen == EXPECTED_SHEET_ROWS
    # [133] Word paragraphs and table rows keep document order, tabs and breaks; tab stops and deleted text are not text.
    docx_rows = spark.table("bronze.finance_documents_docx").orderBy("_row_number").collect()
    results["docx_blocks_kept_in_order"] = [
        (row["block_kind"], row["block_index"], row["table_index"], row["table_row"], list(row["cells"])) for row in docx_rows
    ] == EXPECTED_DOCX_ROWS
    # [134] Non-packages, packages without a document part and DOCTYPE declarations are refused by name.
    hostile = '<!DOCTYPE w:document [<!ENTITY x "y">]>'
    for label, data in (
        ("not_zip", b"PK-not-really"),
        ("no_document_part", docx_bytes("<w:p/>", part="word/other.xml")),
        ("doctype", docx_bytes("<w:p/>", doctype=hostile)),
    ):
        path = root / f"{label}.docx"
        path.write_bytes(data)
        try:
            file_readers.read_docx_rows(path, root / f"{label}.jsonl")
            refused = False
        except file_readers.ReaderError as error:
            refused = {"not_zip": "not a Word package", "no_document_part": "word/document.xml", "doctype": "DOCTYPE"}[label] in str(error)
        results[f"docx_{label}_refused"] = refused
    # [135] A JSON document that is not a variable list loads as text lines, as published.
    json_lines = spark.table("bronze.finance_text_lines").orderBy("_row_number").collect()
    results["json_document_loads_as_text_lines"] = [row["line_text"] for row in json_lines] == ["{", '  "name": "hafd",', '  "resources": []', "}"]
    methodology = spark.table("bronze.finance_documents_text").collect()
    results["json_methodology_loads_as_text_lines"] = [row["line_text"] for row in methodology] == ['[{"name": "beds"}]']
    # [136] A report PDF held as data loads in a data-role PDF table with its table rows and text.
    report_rows = spark.table("bronze.finance_reports_pdf").orderBy("_row_number").collect()
    results["report_pdf_loads_as_data"] = [list(row["cells"]) for row in report_rows if row["row_kind"] == "table"] == [
        ["Hospital", "RN FTE"],
        ["A", "12.5"],
    ] and any(row["cells"] == ["Hospital nurse staffing 2014"] for row in report_rows)
    # [138] Two layouts in one dataset load to their own tables.
    pages = spark.table("bronze.util_pages").collect()
    nonresponse = spark.table("bronze.util_nonresponse").collect()
    results["two_layouts_split_by_member_pattern"] = [(row["fac_id"], row["beds"]) for row in pages] == [("106010735", "120")] and [
        (row["fac_id"], row["reason"]) for row in nonresponse
    ] == [("106010739", "closed")]
    # [139] A single-document manifest without an objects list stops discovery by name.
    storage.put("state/cdph/manifests/doc/manifest.json", json.dumps({"source_id": "CDPH", "role": "reference", "sha256": "0" * 64}).encode())
    cdph = bronze.load_table_map(
        table_map(root / "cdph.json", [{"table": "cdph", "group": "r", "collection": "state/cdph", "member_pattern": "\\.pdf$", "format": "pdf_rows"}])
    )
    results["single_document_manifest_refused"] = expect_error(lambda: bronze.discover(storage, BUCKET, cdph, ["cdph"]), "not a storage manifest")
    # [145] A declared end-of-file marker on the last line is counted, not loaded; anywhere else it is refused.
    hsa = csv_bytes(["MEDICARE_PROV_NUM", "TOTAL_CASES"], [["010001", "*"], ["010005", "12"]])
    store(storage, "cms/hsa", "hsa", "HSA__g", [(None, "Hospital_Service_Area_2024.csv", hsa + b"\x1a")])
    store(storage, "cms/hsa", "hsa", "HSA__h", [(None, "Hospital_Service_Area_2023.csv", hsa + b"\x1a\r\n010009,3\r\n")])
    hsa_table = {"table": "hsa", "group": "r", "collection": "cms/hsa", "member_pattern": "\\.csv$", "format": "csv", "end_of_file_marker": True}
    hsa_config = bronze.load_table_map(table_map(root / "hsa.json", [hsa_table]))
    hsa_inputs, _ = bronze.discover(storage, BUCKET, hsa_config, ["hsa"])
    clean = [item for item in hsa_inputs if item["snapshot_id"] == "HSA__g"]
    summary = bronze.load_objects(spark, clean, storage, "bronze", scratch)
    hsa_rows = spark.table("bronze.hsa").orderBy("_row_number").collect()
    results["end_of_file_marker_counted_not_loaded"] = [(row["medicare_prov_num"], row["total_cases"]) for row in hsa_rows] == [
        ("010001", "*"),
        ("010005", "12"),
    ] and summary["hsa"].get("end_of_file_markers") == 1
    after = [item for item in hsa_inputs if item["snapshot_id"] == "HSA__h"]
    results["line_after_end_of_file_marker_refused"] = expect_error(
        lambda: bronze.load_objects(spark, after, storage, "bronze", scratch), "follows the end-of-file marker"
    )
    plain_hsa = bronze.load_table_map(table_map(root / "hsa_plain.json", [{**hsa_table, "end_of_file_marker": False}]))
    plain_hsa_inputs = [item for item in bronze.discover(storage, BUCKET, plain_hsa, ["hsa"])[0] if item["snapshot_id"] == "HSA__g"]
    results["undeclared_end_of_file_marker_refused"] = expect_error(
        lambda: bronze.load_objects(spark, plain_hsa_inputs, storage, "bronze", scratch), "has 1 fields"
    )
    results["end_of_file_marker_option_only_for_csv"] = expect_error(
        lambda: bronze.load_table_map(table_map(root / "hsa_rows.json", [{**hsa_table, "format": "csv_rows"}])), "csv tables only"
    )
    results.update(retired_scenarios(spark, root, storage, scratch))
    results.update(gap_scenarios(spark, root, storage, scratch))
    results.update(utf16_scenarios(spark, root, storage, scratch))
    results.update(copies_scenarios(spark, root, storage, scratch))
    results.update(supersession_scenarios(spark, root, storage, scratch))
    results.update(removed_table_scenarios(spark, root, storage, scratch))
    return results


def stored_identity(collection: str, dataset: str, name: str, data: bytes) -> dict[str, str]:
    """Return the key, version and SHA-256 that store() gives a data object."""
    digest = hashlib.sha256(data).hexdigest()
    key = f"{collection}/datasets/{dataset}/release_date=2026-01-01/{digest}/{name}"
    return {"key": key, "version_id": f"v-{hashlib.sha256(key.encode() + data).hexdigest()[:12]}", "sha256": digest}


def retired_map(path: Path, entries: list[dict[str, str]]) -> Path:
    """Write a synthetic retired-objects list."""
    path.write_text(json.dumps({"version": 1, "objects": entries}))
    return path


def retired_scenarios(spark: Any, root: Path, storage: FakeStorage, scratch: Path) -> dict[str, bool]:
    """Objects deleted for privacy but still listed in manifests: skipped only when listed, matched exactly [146] to [148]."""
    results: dict[str, bool] = {}
    kept = csv_bytes(["FAC_NO", "BEDS"], [["106010735", "120"]])
    gone = csv_bytes(["FAC_NO", "BEDS"], [["106010739", "80"]])
    store(storage, "state/retired", "ca", "CA__r", [(None, "CA_file_001.csv", kept), (None, "CA_file_002.csv", gone)])
    retired = stored_identity("state/retired", "ca", "CA_file_002.csv", gone)
    del storage.objects[(retired["key"], retired["version_id"])]
    table = {"table": "retired_csv", "group": "r", "collection": "state/retired", "member_pattern": "\\.csv$", "format": "csv"}
    config = bronze.load_table_map(table_map(root / "retired.json", [table]))
    listed = bronze.load_retired(retired_map(root / "retired_list.json", [retired]))
    # [146] A listed object is skipped and reported as retired; the rest loads.
    inputs, unselected = bronze.discover(storage, BUCKET, config, ["retired_csv"], listed)
    results["retired_object_skipped"] = [item["file_name"] for item in inputs] == ["CA_file_001.csv"]
    results["retired_object_reported"] = [(entry["file_name"], entry.get("retired")) for entry in unselected] == [("CA_file_002.csv", True)]
    bronze.load_objects(spark, inputs, storage, "bronze", scratch)
    rows = spark.table("bronze.retired_csv").collect()
    results["rest_of_collection_loads"] = [(row["fac_no"], row["beds"]) for row in rows] == [("106010735", "120")]
    # [146] Without the list the missing object is still selected, so the load would stop on it.
    plain, _ = bronze.discover(storage, BUCKET, config, ["retired_csv"])
    results["unlisted_missing_object_still_selected"] = sorted(item["file_name"] for item in plain) == ["CA_file_001.csv", "CA_file_002.csv"]
    # [147] An entry whose SHA-256 differs from the manifest's is refused; malformed lists are refused before discovery.
    wrong = bronze.load_retired(retired_map(root / "retired_wrong.json", [{**retired, "sha256": "0" * 64}]))
    results["retired_checksum_mismatch_refused"] = expect_error(lambda: bronze.discover(storage, BUCKET, config, ["retired_csv"], wrong), "SHA-256")
    results["malformed_retired_list_refused"] = expect_error(
        lambda: bronze.load_retired(retired_map(root / "retired_bad.json", [{**retired, "sha256": "xyz"}])), "64 hex"
    ) and expect_error(lambda: bronze.load_retired(retired_map(root / "retired_twice.json", [retired, retired])), "repeats")
    # [148] An entry under a discovered collection that matches no manifest object is refused; other collections are not checked.
    stale_entry = {**retired, "key": retired["key"].replace("CA_file_002", "CA_file_009")}
    stale = bronze.load_retired(retired_map(root / "retired_stale.json", [retired, stale_entry]))
    results["stale_retired_entry_refused"] = expect_error(
        lambda: bronze.discover(storage, BUCKET, config, ["retired_csv"], stale), "matches no manifest object"
    )
    elsewhere = bronze.load_retired(retired_map(root / "retired_elsewhere.json", [retired, {**stale_entry, "key": "other/collection/x.csv"}]))
    results["other_collection_entries_ignored"] = len(bronze.discover(storage, BUCKET, config, ["retired_csv"], elsewhere)[0]) == 1
    return results


def dictionary_scenarios(spark: Any, root: Path, storage: FakeStorage, scratch: Path, formats: list[dict[str, Any]]) -> dict[str, bool]:
    """One dictionary per bronze table: published columns only, counted within the files that carry them [83] to [94]."""
    results: dict[str, bool] = {}
    layouts = "cms/layouts"
    store(storage, layouts, "lay_a", "snap_l1", [(None, "a.csv", csv_bytes(["Provider ID", "Score"], [["010001", ""], ["020002", "5"]]))])
    store(storage, layouts, "lay_b", "snap_l2", [(None, "b.csv", csv_bytes(["Provider_ID", "Score", "City"], [["030003", "", "X"]]))], release="2026-03-01")
    store(storage, layouts, "lay_c", "snap_l3", [(None, "c.csv", csv_bytes(["Note"], [["n1"], ["n2"]]))], release="CMS_LAY__20260924T050025Z__abc")
    entry = {"table": "layouts", "group": "layouts", "collection": layouts, "dataset_ids": ["lay_a", "lay_b", "lay_c"], "format": "csv"}
    config = bronze.load_table_map(table_map(root / "dictionary.json", [entry, *formats]))
    inputs, _ = bronze.discover(storage, BUCKET, config, ["layouts"])
    by_snapshot = {item["snapshot_id"]: item for item in inputs}
    bronze.load_objects(spark, [by_snapshot["snap_l1"], by_snapshot["snap_l2"]], storage, "bronze", scratch)

    def rows(table: str) -> dict[str, dict[str, Any]]:
        return {row["column_name"]: row.asDict() for row in spark.table(f"bronze_dictionary.{table}").collect()}

    # [91] A file loaded after a build appears only after the rebuild.
    dictionary.build(spark, config, ["layouts"], "bronze")
    results["dictionary_lists_only_loaded_columns"] = "note" not in rows("layouts")
    bronze.load_objects(spark, [by_snapshot["snap_l3"]], storage, "bronze", scratch)
    # [90] A column map row whose object is not in the bronze table is ignored.
    stray = [("gone_object", "layouts", 1, "Gone Header", "provider_id", None)]
    spark.createDataFrame(stray, bronze.COLUMN_MAP_SCHEMA).writeTo("bronze.column_map").overwritePartitions()
    # [85] [96] A publisher label stored beside its header becomes the description; other columns stay without one.
    score_object = by_snapshot["snap_l1"]["object_key"]
    labelled = [(score_object, "layouts", 1, "Provider ID", "provider_id", None), (score_object, "layouts", 2, "Score", "score", "Measure score")]
    spark.createDataFrame(labelled, bronze.COLUMN_MAP_SCHEMA).writeTo("bronze.column_map").overwritePartitions()
    # [89] A column no file carries any more stays listed with zero files.
    spark.sql("ALTER TABLE bronze.layouts ADD COLUMNS (orphan STRING)")
    counts = dictionary.build(spark, config, ["layouts", "file_text_lines", "file_sheet_rows"], "bronze")
    layout = rows("layouts")
    # [83] Lineage columns never appear; every published column does, in the bronze table's order.
    published = [name for name in spark.table("bronze.layouts").columns if name not in bronze.LINEAGE]
    ordered = sorted(layout.values(), key=lambda row: row["position"])
    results["dictionary_excludes_lineage"] = [row["column_name"] for row in ordered] == published and counts["layouts"] == len(published)
    results["dictionary_rebuilt_after_new_file"] = layout["note"]["files"] == 1
    # [84] Every published header behind a column is kept.
    results["dictionary_keeps_header_variants"] = layout["provider_id"]["original_headers"] == ["Provider ID", "Provider_ID"]
    results["dictionary_ignores_stray_column_map_rows"] = "Gone Header" not in layout["provider_id"]["original_headers"]
    # [86] [87] Counts cover only the files that carry the column; nulls and empty values are separate.
    city, score = layout["city"], layout["score"]
    results["counts_only_files_with_the_column"] = (city["files"], city["rows"], city["null_rows"], city["empty_rows"]) == (1, 1, 0, 0)
    results["empty_and_null_counted_apart"] = (score["rows"], score["null_rows"], score["empty_rows"]) == (3, 0, 2) and score["missing_share"] == 2 / 3
    results["publisher_label_is_the_description"] = (score["description"], score["description_source"]) == ("Measure score", "publisher variable label")
    results["no_description_invented"] = layout["provider_id"]["description"] is None and layout["provider_id"]["description_source"] is None
    # [88] Release ranges only from dates; capture IDs are not shown as releases.
    provider = layout["provider_id"]
    results["release_range_from_dates"] = (provider["first_release"], provider["last_release"]) == ("2026-01-01", "2026-03-01")
    results["capture_ids_not_shown_as_releases"] = layout["note"]["first_release"] is None and layout["note"]["last_release"] is None
    orphan = layout["orphan"]
    results["orphaned_column_listed_with_zero_files"] = (orphan["files"], orphan["rows"], orphan["missing_share"]) == (0, 0, None)
    # [85] [87] Storage-format columns carry the format's own description and count every file; empty arrays are empty.
    lines, sheets = rows("file_text_lines"), rows("file_sheet_rows")
    results["format_columns_described_by_format"] = all(
        row["description"] and row["description_source"] == "bronze storage format" and row["original_headers"] is None
        for row in [*lines.values(), *sheets.values()]
    )
    results["format_columns_count_every_file"] = (lines["line_text"]["files"], lines["line_text"]["rows"], lines["line_text"]["empty_rows"]) == (2, 8, 1)
    results["empty_cell_arrays_counted_as_empty"] = (sheets["cells"]["rows"], sheets["cells"]["empty_rows"], sheets["cells"]["null_rows"]) == (6, 1, 0)
    # [91] [92] [94] A rebuild gives the same rows, in any batch size.
    before = sorted(json.dumps(row, sort_keys=True) for row in layout.values())
    dictionary.build(spark, config, ["layouts"], "bronze", batch=2)
    results["rebuild_is_idempotent_in_batches"] = before == sorted(json.dumps(row, sort_keys=True) for row in rows("layouts").values())
    # [93] Names outside the table map or missing from bronze are refused before anything is written.
    snapshots = spark.table("bronze_dictionary.layouts.snapshots").count()
    missing = bronze.load_table_map(table_map(root / "missing.json", [entry, {**entry, "table": "never_loaded", "dataset_ids": ["none"]}]))
    results["unknown_table_refused"] = expect_error(lambda: dictionary.build(spark, config, ["layouts", "not_mapped"], "bronze"), "not_mapped")
    results["unloaded_table_refused_before_writing"] = (
        expect_error(lambda: dictionary.build(spark, missing, ["layouts", "never_loaded"], "bronze"), "never_loaded")
        and spark.table("bronze_dictionary.layouts.snapshots").count() == snapshots
    )
    return results


def gap_scenarios(spark: Any, root: Path, storage: FakeStorage, scratch: Path) -> dict[str, bool]:
    """Gap fixes: pruning a narrowed selection, single-document manifests and Care Compare families [150] to [158]."""
    results: dict[str, bool] = {}
    # [150] [151] A table whose selection narrows drops the object it no longer selects; other tables are untouched.
    other = csv_bytes(["PRVDR_NUM", "FAC_NAME"], [["010001", "General"]])
    clia = csv_bytes(["PRVDR_NUM", "CLIA_LAB"], [["01D0000001", "Lab"]])
    store(storage, "cms/pos", "cms_pos", "POS__a", [(None, "POS_OTHER_DEC22.csv", other), (None, "PQWB.POSQ.CLIA.DATA.MAR25.csv", clia)])
    wide = {"table": "pos", "group": "g", "collection": "cms/pos", "dataset_ids": ["cms_pos"], "format": "csv"}
    wide_config = bronze.load_table_map(table_map(root / "pos_wide.json", [wide]))
    wide_inputs, _ = bronze.discover(storage, BUCKET, wide_config, ["pos"])
    bronze.load_objects(spark, wide_inputs, storage, "bronze", scratch)
    loaded_before = spark.table("bronze.pos").select("_object_key").distinct().count()
    narrow = [
        {"table": "pos", "group": "g", "collection": "cms/pos", "member_pattern": "(?i)^(?!PQWB\\.POSQ\\.CLIA\\.).*\\.csv$", "format": "csv"},
        {
            "table": "pos_clia",
            "group": "g",
            "collection": "cms/pos",
            "member_pattern": "(?i)^PQWB\\.POSQ\\.CLIA\\.DATA\\.[A-Z]{3}\\d{2}\\.csv$",
            "format": "csv",
        },
    ]
    narrow_config = bronze.load_table_map(table_map(root / "pos_narrow.json", narrow))
    narrow_inputs, _ = bronze.discover(storage, BUCKET, narrow_config, ["pos", "pos_clia"])
    bronze.load_objects(spark, narrow_inputs, storage, "bronze", scratch)
    pruned = bronze.prune(spark, narrow_config, ["pos", "pos_clia"], narrow_inputs, "bronze")
    checks = bronze.verify(spark, narrow_config, narrow_inputs, "bronze")
    results["narrowed_selection_pruned"] = (
        loaded_before == 2
        and pruned == {"pos": 1, "pos_clia": 0}
        and checks["pos"]["passed"]
        and checks["pos"]["objects"] == 1
        and checks["pos_clia"]["passed"]
    )
    stale_map = spark.table("bronze.column_map").where("table_name = 'pos' AND original_header = 'CLIA_LAB'").count()
    results["pruned_object_column_map_removed"] = stale_map == 0
    results["clia_rows_in_own_table"] = [(row["prvdr_num"], row["clia_lab"]) for row in spark.table("bronze.pos_clia").collect()] == [("01D0000001", "Lab")]
    untouched = spark.table("bronze.hsa").count()
    bronze.prune(spark, narrow_config, ["pos"], narrow_inputs, "bronze")
    results["tables_outside_run_untouched"] = spark.table("bronze.hsa").count() == untouched
    results["table_without_inputs_not_emptied"] = bronze.prune(spark, narrow_config, ["pos"], [], "bronze") == {"pos": 0} and (
        spark.table("bronze.pos").count() == 1
    )
    # [153] [154] A single-document manifest resolves to the one stored object under its SHA-256 and loads as a document.
    document = pdf_document([{"lines": ["Infection prevention program plan"]}])
    digest = hashlib.sha256(document).hexdigest()
    storage.put(f"state/plan/references/capture_id=CDPH_x/{digest}/plan_redacted.pdf", document)
    single = {
        "source_id": "CDPH",
        "role": "reference",
        "sha256": digest,
        "byte_count": len(document),
        "original_file_name": "Plan_Redacted.pdf",
        "scope_exception": {"allowed_document_sha256": digest, "allowed_role": "reference"},
    }
    storage.put("state/plan/manifests/capture_id=CDPH_x/CDPH_x/m1/manifest.json", json.dumps(single).encode())
    plan_table = {
        "table": "plan_documents_pdf",
        "group": "g",
        "collection": "state/plan",
        "member_pattern": "(?i)\\.pdf$",
        "format": "pdf_rows",
        "roles": ["reference"],
    }
    plan_config = bronze.load_table_map(table_map(root / "plan.json", [plan_table]))
    plan_inputs, _ = bronze.discover(storage, BUCKET, plan_config, ["plan_documents_pdf"])
    results["single_document_resolved"] = [(item["snapshot_id"], item["file_name"], item["sha256"]) for item in plan_inputs] == [
        ("CDPH_x", "plan_redacted.pdf", digest)
    ]
    bronze.load_objects(spark, plan_inputs, storage, "bronze", scratch)
    results["single_document_loaded"] = any(row["cells"] == ["Infection prevention program plan"] for row in spark.table("bronze.plan_documents_pdf").collect())
    for label, change, fragment in (
        ("scope_mismatch", {"scope_exception": {"allowed_document_sha256": "0" * 64, "allowed_role": "reference"}}, "scope exception"),
        ("missing_object", {"sha256": "1" * 64, "scope_exception": {"allowed_document_sha256": "1" * 64, "allowed_role": "reference"}}, "no stored object"),
    ):
        collection = f"state/plan_{label}"
        storage.put(f"{collection}/references/capture_id=CDPH_y/{digest}/plan_redacted.pdf", document)
        storage.put(f"{collection}/manifests/capture_id=CDPH_y/CDPH_y/m1/manifest.json", json.dumps({**single, **change}).encode())
        variant = bronze.load_table_map(table_map(root / f"{label}.json", [{**plan_table, "collection": collection}]))
        results[f"single_document_{label}_refused"] = expect_error(partial(bronze.discover, storage, BUCKET, variant, ["plan_documents_pdf"]), fragment)
    storage.put(f"state/plan_twice/references/capture_id=CDPH_z/{digest}/a.pdf", document)
    storage.put(f"state/plan_twice/references/capture_id=CDPH_z/{digest}/b.pdf", document)
    storage.put("state/plan_twice/manifests/capture_id=CDPH_z/CDPH_z/m1/manifest.json", json.dumps(single).encode())
    twice = bronze.load_table_map(table_map(root / "twice.json", [{**plan_table, "collection": "state/plan_twice"}]))
    results["single_document_two_objects_refused"] = expect_error(lambda: bronze.discover(storage, BUCKET, twice, ["plan_documents_pdf"]), "2 stored objects")
    # [155] Families: the dataset-ID prefix, dates, fiscal years, periods and edition words are not part of the family.
    family = care_compare_tables.family
    results["family_names_normalized"] = (
        family("Timely_and_Effective_Care-Hospital.csv")
        == family("dgck-syfz_2026-07-22_Timely_and_Effective_Care-Hospital.csv")
        == family("Timely and Effective Care - Hospital.csv")
        == "timely_and_effective_care_hospital"
        and family("HVBP_Safety_11_09_2018.csv") == family("hvbp_safety.csv") == "hvbp_safety"
        and family("FY2017_Net_Change_in_Base_Op_DRG_Payment_Amt_2018-11-30.csv") == family("FY2024_Net_Change_in_Base_Op_DRG_Payment_Amt.csv")
        and family("ASC_CCN_PR17Q3_18Q2.csv") == "asc_ccn"
        and family("Data_Updates_April_2021.csv") == family("data_updates_october_2019_updated.csv") == "data_updates"
        and family("DOD_TE_January_2019_Production_11-29-18.csv") == family("DOD_TE_July_2019_Production.csv") == "dod_te"
        and family("IPFQR_QualityMeasures_Facility_revised.csv") == family("IPFQR_QualityMeasures_Facility.csv")
        and family("3n5g-6b7f.csv") is None
        and family("Complications_and_Deaths-Hospital.csv") != family("Complications_and_Deaths-State.csv")
    )
    datasets = {
        "cms_legacy_timely_and_effective_care_hospital": ["Timely_and_Effective_Care-Hospital.csv"],
        "cms_yv7e_xc69": ["yv7e-xc69.csv", "yv7e-xc69_2026-07-22_Timely_and_Effective_Care-Hospital.csv"],
        "cms_legacy_pdc_s3_hos_data_yv7e_xc69": ["yv7e-xc69.csv"],
        "cms_legacy_zz99_zz99": ["zz99-zz99.csv"],
        "cms_legacy_hai": ["Healthcare_Associated_Infections-Hospital.csv"],
        "cms_legacy_contacts": ["Hospitals_CASPER_ASPEN_Contacts.csv"],
        "cms_legacy_book": ["Measure_Dates.xlsx"],
    }
    assigned = care_compare_tables.assign(datasets, existing={"cms_legacy_hai"}, excluded={"cms_legacy_contacts"})
    entries = {entry["table"]: entry for entry in care_compare_tables.table_entries(assigned, datasets)}
    results["legacy_and_id_datasets_share_a_table"] = entries["cms_cc_timely_and_effective_care_hospital"]["dataset_ids"] == [
        "cms_legacy_pdc_s3_hos_data_yv7e_xc69",
        "cms_legacy_timely_and_effective_care_hospital",
        "cms_yv7e_xc69",
    ]
    results["unnamed_id_keeps_its_id"] = entries["cms_cc_zz99_zz99"]["dataset_ids"] == ["cms_legacy_zz99_zz99"]
    results["existing_and_excluded_datasets_skipped"] = not any(
        set(entry["dataset_ids"]) & {"cms_legacy_hai", "cms_legacy_contacts"} for entry in entries.values()
    )
    results["workbook_family_gets_sheet_rows"] = entries["cms_cc_measure_dates_sheet_rows"]["format"] == "sheet_rows"
    results["generator_is_deterministic"] = care_compare_tables.table_entries(assigned, datasets) == care_compare_tables.table_entries(
        care_compare_tables.assign(datasets, existing={"cms_legacy_hai"}, excluded={"cms_legacy_contacts"}), datasets
    )
    split = {"cms_legacy_mixed": ["HCAHPS-Hospital.csv", "HCAHPS-State.csv"]}
    try:
        care_compare_tables.assign(split, existing=set(), excluded=set())
        results["dataset_with_two_families_refused"] = False
    except care_compare_tables.FamilyError as error:
        results["dataset_with_two_families_refused"] = "cms_legacy_mixed" in str(error)
    # [161] Blank lines after the last row are counted, not loaded; a blank line before data is still refused.
    blank = csv_bytes(["CCN", "SCORE"], [["010001", "7"]])
    store(storage, "cms/blank", "asc", "ASC__b", [(None, "ASC_NATIONAL_pr18q1_18q4.csv", blank + b"\r\n")])
    store(storage, "cms/blank", "asc", "ASC__c", [(None, "ASC_STATE_pr18q1_18q4.csv", blank + b"\r\n010005,9\r\n")])
    blank_table = {"table": "asc_blank", "group": "g", "collection": "cms/blank", "member_pattern": "\\.csv$", "format": "csv", "trailing_blank_lines": True}
    blank_config = bronze.load_table_map(table_map(root / "blank.json", [blank_table]))
    blank_inputs, _ = bronze.discover(storage, BUCKET, blank_config, ["asc_blank"])
    clean_blank = [item for item in blank_inputs if item["snapshot_id"] == "ASC__b"]
    blank_summary = bronze.load_objects(spark, clean_blank, storage, "bronze", scratch)
    results["trailing_blank_line_counted_not_loaded"] = [(row["ccn"], row["score"]) for row in spark.table("bronze.asc_blank").collect()] == [
        ("010001", "7")
    ] and blank_summary["asc_blank"].get("trailing_blank_lines") == 1
    inner = [item for item in blank_inputs if item["snapshot_id"] == "ASC__c"]
    results["blank_line_before_data_refused"] = expect_error(lambda: bronze.load_objects(spark, inner, storage, "bronze", scratch), "follows a blank line")
    plain_blank = bronze.load_table_map(table_map(root / "blank_plain.json", [{**blank_table, "trailing_blank_lines": False}]))
    plain_blank_inputs = [item for item in bronze.discover(storage, BUCKET, plain_blank, ["asc_blank"])[0] if item["snapshot_id"] == "ASC__b"]
    results["undeclared_trailing_blank_line_refused"] = expect_error(
        lambda: bronze.load_objects(spark, plain_blank_inputs, storage, "bronze", scratch), "has 0 fields"
    )
    # [162] A UTF-16 tab-separated file loads when the table declares UTF-16 alone; UTF-16 never shares a table.
    utf16 = "% Change\tNumber of Hospitals\r\n0.5%\t12\r\n".encode("utf-16")
    store(storage, "cms/utf16", "pct", "PCT__d", [(None, "FY2017_Percent_Change_in_Medicare_Payments_2018-12-03.csv", utf16)])
    utf16_table = {
        "table": "pct_utf16",
        "group": "g",
        "collection": "cms/utf16",
        "member_pattern": "\\.csv$",
        "format": "csv",
        "encodings": ["utf-16"],
        "delimiter": "\t",
    }
    utf16_config = bronze.load_table_map(table_map(root / "utf16.json", [utf16_table]))
    utf16_inputs, _ = bronze.discover(storage, BUCKET, utf16_config, ["pct_utf16"])
    bronze.load_objects(spark, utf16_inputs, storage, "bronze", scratch)
    utf16_rows = spark.table("bronze.pct_utf16").collect()
    results["utf16_tab_file_loads"] = [(row["change"], row["number_of_hospitals"], row["_source_encoding"]) for row in utf16_rows] == [("0.5%", "12", "utf-16")]
    results["utf16_mixed_encodings_refused"] = expect_error(
        lambda: bronze.load_table_map(table_map(root / "utf16_mixed.json", [{**utf16_table, "encodings": ["utf-8", "utf-16"]}])), "encodings"
    )
    # [163] Listed datasets go to a table of their own with their reading options; the rest of the family keeps the CSV table.
    family_sets = {
        "cms_legacy_fy2017_pct": ["FY2017_Percent_Change_in_Medicare_Payments_2018-12-03.csv"],
        "cms_legacy_fy2019_pct": ["FY2019_Percent_Change_in_Medicare_Payments.csv"],
    }
    overrides = {"cms_legacy_fy2017_pct": {"suffix": "utf16", "encodings": ["utf-16"], "delimiter": "\t"}}
    split_entries = {
        entry["table"]: entry for entry in care_compare_tables.table_entries(care_compare_tables.assign(family_sets, set(), set()), family_sets, overrides)
    }
    results["override_datasets_get_own_table"] = (
        split_entries["cms_cc_percent_change_in_medicare_payments_utf16"]["dataset_ids"] == ["cms_legacy_fy2017_pct"]
        and split_entries["cms_cc_percent_change_in_medicare_payments_utf16"]["encodings"] == ["utf-16"]
        and split_entries["cms_cc_percent_change_in_medicare_payments_utf16"]["delimiter"] == "\t"
        and split_entries["cms_cc_percent_change_in_medicare_payments"]["dataset_ids"] == ["cms_legacy_fy2019_pct"]
    )
    # [164] Each CSV family links to the Care Compare dictionaries by an anchored section pattern built from its words.
    linked = {
        entry["table"]: entry
        for entry in care_compare_tables.table_entries(
            care_compare_tables.assign(datasets, existing={"cms_legacy_hai"}, excluded={"cms_legacy_contacts"}), datasets
        )
    }
    section = re.compile(linked["cms_cc_timely_and_effective_care_hospital"]["dictionary"]["section_pattern"])
    results["family_sections_matched"] = (
        all(
            section.search(name)
            for name in ("TIMELY_AND_EFFECTIVE_CARE-HOSPITAL.CSV", "TIMELYANDEFFECTIVECARE-HOSPITAL.CSV", "Timely_and_Effective_Care-Hospital_11_09_2018.csv")
        )
        and not any(
            section.search(name)
            for name in ("PCH_TIMELY_AND_EFFECTIVE_CARE-HOSPITAL.CSV", "TIMELY_AND_EFFECTIVE_CARE-STATE.CSV", "TIMELY_AND_EFFECTIVE_CARE-HOSPITALS.CSV")
        )
        and linked["cms_cc_timely_and_effective_care_hospital"]["dictionary"]["table"] == "cms_hospitals_dictionaries"
        and "dictionary" not in linked["cms_cc_measure_dates_sheet_rows"]
    )
    fiscal = re.compile(care_compare_tables.section_pattern("net_change_in_base_op_drg_payment_amt"))
    results["fiscal_year_section_matched"] = bool(fiscal.search("FY2020_NET_CHANGE_IN_BASE_OP_DRG_PAYMENT_AMT.CSV")) and not fiscal.search(
        "FY2020_NET_CHANGE_IN_BASE_OP_DRG_PAYMENT_"
    )
    return results


def utf16_scenarios(spark: Any, root: Path, storage: FakeStorage, scratch: Path) -> dict[str, bool]:
    """UTF-16 files: refused by single-byte tables, read by a UTF-16 table, split from their snapshot's other files [194] to [198]."""
    results: dict[str, bool] = {}
    utf16_text = b"\xff\xfe" + "PROV\tWAGE\r\n010001\t$28.20\r\n".encode("utf-16-le")
    plain_text = "PUF description\r\nWages and hours\r\n".encode("cp1252")
    names = ("FY_2017_FR_OccMix_PUF.txt", "FY_2017_FR_S3_OCCMIX_PUF description.txt")
    store(storage, "cms/occmix_utf16", "cms_occmix", "OCCMIX__u", [(None, names[0], utf16_text), (None, names[1], plain_text)])
    single_byte = {"table": "occmix_text", "group": "g", "collection": "cms/occmix_utf16", "member_pattern": "(?i)\\.txt$", "format": "text_lines"}
    wide = bronze.load_table_map(table_map(root / "utf16_wide.json", [{**single_byte, "encodings": ["utf-8", "cp1252"]}]))
    wide_inputs, _ = bronze.discover(storage, BUCKET, wide, ["occmix_text"])
    # [194] [195] A single-byte table refuses the UTF-16 file by name and says what to declare.
    results["utf16_mark_refused_by_cp1252_table"] = expect_error(
        lambda: bronze.load_objects(spark, wide_inputs, storage, "bronze", scratch), "UTF-16 byte-order mark"
    ) and expect_error(lambda: bronze.load_objects(spark, wide_inputs, storage, "bronze", scratch), names[0])
    results["utf16_refusal_names_the_fix"] = expect_error(lambda: bronze.load_objects(spark, wide_inputs, storage, "bronze", scratch), "encodings as utf-16")
    # [194] A NUL byte in a single-byte text file is refused too.
    store(storage, "cms/occmix_nul", "cms_occmix", "OCCMIX__n", [(None, "nul.txt", b"A\x00B\r\nC\r\n")])
    nul_table = {**single_byte, "table": "nul_text", "collection": "cms/occmix_nul", "encodings": ["utf-8", "cp1252"]}
    nul_config = bronze.load_table_map(table_map(root / "utf16_nul.json", [nul_table]))
    nul_inputs, _ = bronze.discover(storage, BUCKET, nul_config, ["nul_text"])
    results["nul_byte_refused_by_text_table"] = expect_error(lambda: bronze.load_objects(spark, nul_inputs, storage, "bronze", scratch), "NUL byte")
    # [194] A CSV table refuses a UTF-16 mark it does not declare.
    utf16_csv = b"\xff\xfe" + "PROV,WAGE\r\n010001,28.20\r\n".encode("utf-16-le")
    store(storage, "cms/occmix_csv16", "cms_occmix", "OCCMIX__c", [(None, "wages.csv", utf16_csv)])
    csv_table = {
        "table": "csv16",
        "group": "g",
        "collection": "cms/occmix_csv16",
        "member_pattern": "(?i)\\.csv$",
        "format": "csv",
        "encodings": ["utf-8", "cp1252"],
    }
    csv_config = bronze.load_table_map(table_map(root / "utf16_csv.json", [csv_table]))
    csv_inputs, _ = bronze.discover(storage, BUCKET, csv_config, ["csv16"])
    results["utf16_mark_refused_by_cp1252_csv_table"] = expect_error(
        lambda: bronze.load_objects(spark, csv_inputs, storage, "bronze", scratch), "UTF-16 byte-order mark"
    )
    # [196] [198] The UTF-16 table takes exactly the named file; the single-byte table keeps the snapshot's other file.
    pattern = "FY_2017_FR_OccMix_PUF"
    split = [
        {**single_byte, "member_pattern": f"(?i)^(?!{pattern}\\.txt$).*\\.txt$", "encodings": ["utf-8", "cp1252"]},
        {
            **single_byte,
            "table": "occmix_text_utf16",
            "member_pattern": f"(?i)^{pattern}\\.txt$",
            "snapshot_pattern": "^OCCMIX__u$",
            "encodings": ["utf-16"],
        },
    ]
    split_config = bronze.load_table_map(table_map(root / "utf16_split.json", split))
    split_inputs, _ = bronze.discover(storage, BUCKET, split_config, ["occmix_text", "occmix_text_utf16"])
    try:
        bronze.load_objects(spark, split_inputs, storage, "bronze", scratch)
    except bronze.BronzeError:
        return {**results, "utf16_table_reads_text": False, "single_byte_table_keeps_other_files": False}
    checks = bronze.verify(spark, split_config, split_inputs, "bronze")
    utf16_rows = spark.table("bronze.occmix_text_utf16").orderBy("_row_number").collect()
    plain_rows = spark.table("bronze.occmix_text").orderBy("_row_number").collect()
    results["utf16_table_reads_text"] = (
        [row["line_text"] for row in utf16_rows] == ["PROV\tWAGE", "010001\t$28.20"]
        and all(row["_source_encoding"] == "utf-16" for row in utf16_rows)
        and checks["occmix_text_utf16"]["passed"]
    )
    results["single_byte_table_keeps_other_files"] = [row["line_text"] for row in plain_rows] == ["PUF description", "Wages and hours"] and (
        checks["occmix_text"]["passed"]
    )
    return results


def copies_scenarios(spark: Any, root: Path, storage: FakeStorage, scratch: Path) -> dict[str, bool]:
    """A file stored more than once loads once, from the copy with the smallest object key; every copy is listed [204] to [208]."""
    results: dict[str, bool] = {}
    same = csv_bytes(["CCN", "SCORE"], [["010001", "1.5"], ["010002", "2.5"]])
    other = csv_bytes(["CCN", "SCORE"], [["010003", "3.5"]])
    same_sha = hashlib.sha256(same).hexdigest()
    store(storage, "cms/copies", "first", "COPY__a", [(["a.zip", "scores.csv"], "scores.csv", same)])
    store(storage, "cms/copies", "second", "COPY__b", [(["b.zip", "scores_again.csv"], "scores_again.csv", same)])
    store(storage, "cms/copies", "third", "COPY__c", [(None, "scores.csv", same), (None, "other.csv", other)])
    store(storage, "cms/copies", "fourth", "COPY__d", [(None, "scores_retired.csv", same)])
    retired = stored_identity("cms/copies", "fourth", "scores_retired.csv", same)
    del storage.objects[(retired["key"], retired["version_id"])]
    table = {"table": "copy_scores", "group": "c", "collection": "cms/copies", "member_pattern": "\\.csv$", "format": "csv"}
    config = bronze.load_table_map(table_map(root / "copies.json", [table]))
    listed = bronze.load_retired(retired_map(root / "copies_retired.json", [retired]))
    inputs, unselected = bronze.discover(storage, BUCKET, config, ["copy_scores"], listed)
    copies = [entry["copy"] for entry in unselected if entry.get("identical_copy")]
    # [204] Every table loads one copy per SHA-256, with no option to ask for it.
    results["copies_load_once_by_default"] = len(inputs) == 2 and len(copies) == 2 and len({item["sha256"] for item in inputs}) == 2
    # [205] The kept copy is the one with the smallest object key, whatever the listing order.
    candidates = sorted(item["object_key"] for item in [*inputs, *copies] if item["sha256"] == same_sha)
    kept = next(item for item in inputs if item["sha256"] == same_sha)
    results["kept_copy_has_smallest_object_key"] = kept["object_key"] == candidates[0] and len(candidates) == 3
    results["distinct_files_option_refused"] = expect_error(
        lambda: bronze.load_table_map(table_map(root / "copies_flag.json", [{**table, "distinct_files": True}])), "distinct_files"
    )
    # [208] Copies loaded before the change are pruned.
    bronze.load_objects(spark, [*inputs, *copies], storage, "bronze", scratch)
    pruned = bronze.prune(spark, config, ["copy_scores"], inputs, "bronze")
    loaded = {row["_object_key"] for row in spark.table("bronze.copy_scores").select("_object_key").distinct().collect()}
    results["previous_copies_pruned"] = pruned == {"copy_scores": 2} and loaded == {item["object_key"] for item in inputs}
    # [206] Every copy is listed with its lineage, loaded or not, retired copies included; a rerun replaces the rows.
    bronze.write_copies(spark, "bronze", ["copy_scores"], inputs, unselected)
    bronze.write_copies(spark, "bronze", ["copy_scores"], inputs, unselected)
    rows = spark.table("bronze.stored_copies").where("table_name = 'copy_scores'").collect()
    same_rows = [row for row in rows if row["sha256"] == same_sha]
    results["every_copy_listed"] = (
        len(rows) == 5
        and len(same_rows) == 4
        and sum(row["loaded"] for row in same_rows) == 1
        and len({row["s3_key"] for row in same_rows}) == 4
        and {row["file_name"] for row in same_rows} == {"scores.csv", "scores_again.csv", "scores_retired.csv"}
    )
    results["retired_copy_listed_not_loaded"] = [(row["loaded"], row["retired"]) for row in rows if row["file_name"] == "scores_retired.csv"] == [(False, True)]
    # [207] Verification checks the copies against the loaded rows.
    check = bronze.verify(spark, config, inputs, "bronze")["copy_scores"]
    results["verification_checks_copies"] = check["passed"] and check["one_loaded_copy_per_file"] and check["loaded_copies_match_rows"]
    wrong = [entry for entry in unselected if entry.get("copy") is not copies[0]]
    bronze.write_copies(spark, "bronze", ["copy_scores"], [*inputs, copies[0]], wrong)
    tampered = bronze.verify(spark, config, inputs, "bronze")["copy_scores"]
    results["verification_catches_two_loaded_copies"] = not tampered["passed"] and not tampered["one_loaded_copy_per_file"]
    bronze.write_copies(spark, "bronze", ["copy_scores"], inputs, unselected)
    # [222] A column-map file that also holds rows of kept copies is rewritten, not refused, and keeps those rows.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS shared")
    spark.sql("CREATE TABLE shared.column_map (" + bronze.COLUMN_MAP_SCHEMA + ") USING iceberg TBLPROPERTIES ('format-version'='2')")
    bronze.load_objects(spark, [*inputs, *copies], storage, "shared", scratch)
    mapped = [(item["object_key"], "copy_scores", 1, "CCN", "ccn", None) for item in [*inputs, *copies]]
    spark.createDataFrame(mapped, bronze.COLUMN_MAP_SCHEMA).coalesce(1).writeTo("shared.column_map").overwritePartitions()
    shared_pruned = bronze.prune(spark, config, ["copy_scores"], inputs, "shared")
    remaining = {row["_object_key"] for row in spark.table("shared.column_map").collect()}
    results["prune_rewrites_shared_side_files"] = shared_pruned == {"copy_scores": 2} and remaining == {item["object_key"] for item in inputs}
    # [223] Side rows left behind after their data rows were pruned (a stopped run) are removed by the next run.
    orphan = [("0" * 32, "copy_scores", 1, "CCN", "ccn", None)]
    spark.createDataFrame(orphan, bronze.COLUMN_MAP_SCHEMA).writeTo("shared.column_map").append()
    bronze.prune(spark, config, ["copy_scores"], inputs, "shared")
    after = {row["_object_key"] for row in spark.table("shared.column_map").collect()}
    results["prune_removes_orphaned_side_rows"] = after == {item["object_key"] for item in inputs}
    return results


def corrected(storage: FakeStorage, base: str, changes: list[dict[str, str]], extra: dict[str, Any] | None = None, name: str = "corrected") -> dict[str, str]:
    """Store a manifest that supersedes the one under base, with the named role changes; return its identity."""
    key = f"{base}/manifest.json"
    (version,) = [found for stored_key, found in storage.list_keys(BUCKET, key) if stored_key == key]
    raw = storage.get(BUCKET, key, version)
    manifest = json.loads(raw)
    named = {(change["key"], change["version_id"]): change for change in changes}
    for item in manifest["objects"]:
        change = named.get((item["object"].get("key", ""), item["object"].get("version_id", "")))
        if change and item["role"] == change["from_role"]:
            item["role"] = change["to_role"]
    manifest.update(extra or {})
    manifest["supersedes"] = {"key": key, "version_id": version, "sha256": hashlib.sha256(raw).hexdigest()}
    manifest["correction"] = {"issue": "BRZ-011", "changes": changes}
    body = json.dumps(manifest).encode()
    new_key = f"{base.rsplit('/', 1)[0]}/{name}/manifest.json"
    return {"key": new_key, "version_id": storage.put(new_key, body), "sha256": hashlib.sha256(body).hexdigest()}


def supersession_scenarios(spark: Any, root: Path, storage: FakeStorage, scratch: Path) -> dict[str, bool]:
    """A corrected manifest replaces the one it supersedes; a wrong or ambiguous supersession is refused [249] to [252]."""
    results: dict[str, bool] = {}
    # The publisher ends the header, and only the header, with a delimiter [256].
    detail = b"HPSA Name,HPSA ID,\r\nExample Area,1234567890\r\nOther Area,1234567891\r\n"

    def setup(collection: str) -> tuple[str, list[dict[str, str]], dict[str, Any]]:
        base = store(storage, collection, "hpsa", "HPSA__s", [(None, "DETAIL.csv", detail)], role="dictionary")
        identity = stored_identity(collection, "hpsa", "DETAIL.csv", detail)
        change = [{"key": identity["key"], "version_id": identity["version_id"], "from_role": "dictionary", "to_role": "data"}]
        tables = [
            {
                "table": f"{collection.replace('/', '_')}_documents",
                "group": "s",
                "collection": collection,
                "member_pattern": "\\.csv$",
                "format": "csv_rows",
                "roles": ["dictionary"],
            },
            {
                "table": f"{collection.replace('/', '_')}_detail",
                "group": "s",
                "collection": collection,
                "member_pattern": "^DETAIL\\.csv$",
                "format": "csv",
                "header_trailing_delimiter": True,
            },
        ]
        config = bronze.load_table_map(table_map(root / f"{collection.replace('/', '_')}.json", tables))
        return base, change, config

    def tables_of(config: dict[str, Any]) -> list[str]:
        return [table["table"] for table in config["tables"]]

    # [249] Before the correction the file is a document; after it, only the corrected manifest is read.
    base, change, config = setup("hrsa/supersede")
    before, _ = bronze.discover(storage, BUCKET, config, tables_of(config))
    corrected(storage, base, change)
    after, _ = bronze.discover(storage, BUCKET, config, tables_of(config))
    results["superseded_manifest_skipped"] = [item["table"] for item in before] == ["hrsa_supersede_documents"] and [item["table"] for item in after] == [
        "hrsa_supersede_detail"
    ]
    # [252] The corrected file loads as a data table with its records.
    bronze.load_objects(spark, after, storage, "bronze", scratch)
    rows = spark.table("bronze.hrsa_supersede_detail").collect()
    results["corrected_file_loads_as_data"] = sorted(row["hpsa_id"] for row in rows) == ["1234567890", "1234567891"]
    # [256] Without the option the extra header delimiter is refused; with it, a non-empty last header or a wider row is.
    path = scratch / "trailing_header.csv"
    path.write_bytes(detail)
    results["trailing_header_delimiter_needs_option"] = expect_error(lambda: bronze.read_csv(path, None, ["utf-8"], {}), "fields; the header has 3")
    found = bronze.read_csv(path, None, ["utf-8"], {"header_trailing_delimiter": True})
    results["trailing_header_delimiter_dropped"] = found["headers"] == ["HPSA Name", "HPSA ID"] and found["rows"] == 2
    path.write_bytes(b"HPSA Name,HPSA ID\r\nExample Area,1234567890\r\n")
    results["trailing_header_delimiter_requires_empty_last_header"] = expect_error(
        lambda: bronze.read_csv(path, None, ["utf-8"], {"header_trailing_delimiter": True}), "empty"
    )
    path.write_bytes(b"HPSA Name,HPSA ID,\r\nExample Area,1234567890,\r\n")
    results["trailing_header_delimiter_rows_keep_width"] = expect_error(
        lambda: bronze.read_csv(path, None, ["utf-8"], {"header_trailing_delimiter": True}), "fields; the header has 2"
    )
    results["trailing_header_delimiter_csv_only"] = expect_error(
        lambda: bronze.load_table_map(
            table_map(
                root / "trailing_rows.json",
                [{"table": "t_rows", "group": "s", "collection": "x/y", "member_pattern": "x", "format": "csv_rows", "header_trailing_delimiter": True}],
            )
        ),
        "header_trailing_delimiter",
    )
    # [250] A supersedes entry that matches no stored manifest, or differs in SHA-256, is refused.
    base, change, config = setup("hrsa/supersede_missing")
    target = corrected(storage, base, change)
    raw = json.loads(storage.get(BUCKET, target["key"], target["version_id"]))
    raw["supersedes"]["sha256"] = "0" * 64
    storage.objects[(target["key"], target["version_id"])] = json.dumps(raw).encode()
    results["supersedes_wrong_hash_refused"] = expect_error(lambda: bronze.discover(storage, BUCKET, config, tables_of(config)), "supersedes")
    raw["supersedes"]["key"] = f"{base}/gone/manifest.json"
    storage.objects[(target["key"], target["version_id"])] = json.dumps(raw).encode()
    results["supersedes_missing_manifest_refused"] = expect_error(lambda: bronze.discover(storage, BUCKET, config, tables_of(config)), "supersedes")
    # [250] Two manifests superseding one, or a superseding manifest that is itself superseded, are refused.
    base, change, config = setup("hrsa/supersede_twice")
    corrected(storage, base, change, name="first")
    corrected(storage, base, change, name="second")
    results["two_superseders_refused"] = expect_error(lambda: bronze.discover(storage, BUCKET, config, tables_of(config)), "supersede")
    base, change, config = setup("hrsa/supersede_chain")
    corrected(storage, base, change, name="first")
    chain_base = f"{base.rsplit('/', 1)[0]}/first"
    corrected(storage, chain_base, [], name="second")
    results["superseded_superseder_refused"] = expect_error(lambda: bronze.discover(storage, BUCKET, config, tables_of(config)), "supersede")
    # [251] A superseding manifest with any change beyond the named roles is refused.
    base, change, config = setup("hrsa/supersede_extra")
    corrected(storage, base, change, extra={"release_id": "2099-01-01"})
    results["extra_change_refused"] = expect_error(lambda: bronze.discover(storage, BUCKET, config, tables_of(config)), "beyond")
    base, change, config = setup("hrsa/supersede_unnamed")
    corrected(storage, base, [{**change[0], "to_role": "reference"}])
    raw_key = f"{base.rsplit('/', 1)[0]}/corrected/manifest.json"
    (version,) = [found for key, found in storage.list_keys(BUCKET, raw_key) if key == raw_key]
    raw = json.loads(storage.get(BUCKET, raw_key, version))
    raw["correction"]["changes"][0]["to_role"] = "data"
    storage.objects[(raw_key, version)] = json.dumps(raw).encode()
    results["unnamed_role_change_refused"] = expect_error(lambda: bronze.discover(storage, BUCKET, config, tables_of(config)), "beyond")
    return results


def removed_table_scenarios(spark: Any, root: Path, storage: FakeStorage, scratch: Path) -> dict[str, bool]:
    """A table taken out of the map is dropped with its dictionary and side rows; nothing else changes [257] to [260]."""
    from scripts.lakehouse import dictionary

    results: dict[str, bool] = {}
    store(storage, "x/removal", "docs", "RM__a", [(None, "drop_me.csv", csv_bytes(["A"], [["1"]])), (None, "keep_me.csv", csv_bytes(["A"], [["2"]]))])
    drop = {"table": "removal_drop", "group": "rm", "collection": "x/removal", "member_pattern": "^drop_me\\.csv$", "format": "csv"}
    keep = {"table": "removal_keep", "group": "rm", "collection": "x/removal", "member_pattern": "^keep_me\\.csv$", "format": "csv"}
    both = bronze.load_table_map(table_map(root / "removal_both.json", [drop, keep]))
    inputs, unselected = bronze.discover(storage, BUCKET, both, ["removal_drop", "removal_keep"])
    bronze.load_objects(spark, inputs, storage, "bronze", scratch)
    bronze.write_copies(spark, "bronze", ["removal_drop", "removal_keep"], inputs, unselected)
    dictionary.build(spark, both, ["removal_drop", "removal_keep"], "bronze")
    kept_only = bronze.load_table_map(table_map(root / "removal_kept.json", [keep]))
    listed = root / "removed_tables.json"
    listed.write_text(json.dumps({"version": 1, "tables": [{"table": "removal_drop", "reason": "synthetic", "decision": "synthetic"}]}))

    def side_tables(name: str) -> dict[str, int]:
        return {side: spark.table(f"bronze.{side}").where(f"table_name = '{name}'").count() for side in ("column_map", "stored_copies")}

    # [258] A listed table still in the map, a bad name or a repeat is refused before anything is dropped.
    results["removed_table_still_mapped_refused"] = expect_error(lambda: bronze.load_removed(listed, both), "still in the table map")
    bad = root / "removed_bad.json"
    bad.write_text(json.dumps({"version": 1, "tables": [{"table": "x; DROP", "reason": "r", "decision": "d"}]}))
    results["removed_table_bad_name_refused"] = expect_error(lambda: bronze.load_removed(bad, kept_only), "identifier")
    twice = root / "removed_twice.json"
    twice.write_text(json.dumps({"version": 1, "tables": [{"table": "removal_drop", "reason": "r", "decision": "d"}] * 2}))
    results["removed_table_repeat_refused"] = expect_error(lambda: bronze.load_removed(twice, kept_only), "twice")
    before_keep = side_tables("removal_keep")
    # [257] The table, its dictionary and its side rows go.
    outcome = bronze.drop_removed(spark, bronze.load_removed(listed, kept_only), "bronze")
    results["removed_table_dropped"] = (
        outcome == {"removal_drop": "dropped"}
        and not spark.catalog.tableExists("bronze.removal_drop")
        and not spark.catalog.tableExists(f"{dictionary.NAMESPACE}.removal_drop")
        and side_tables("removal_drop") == {"column_map": 0, "stored_copies": 0}
    )
    # [260] Every other table keeps its rows, dictionary and side rows.
    results["other_tables_untouched"] = (
        spark.table("bronze.removal_keep").count() == 1
        and spark.catalog.tableExists(f"{dictionary.NAMESPACE}.removal_keep")
        and side_tables("removal_keep") == before_keep
        and before_keep["column_map"] > 0
    )
    # [259] A rerun reports the table as absent and changes nothing.
    results["removed_table_rerun_absent"] = bronze.drop_removed(spark, bronze.load_removed(listed, kept_only), "bronze") == {"removal_drop": "absent"}
    return results


def main() -> int:
    """Run the scenarios in a throwaway catalog and write an outcomes-only report."""
    root = Path(tempfile.mkdtemp(prefix="bronze_e2e_"))
    spark = spark_session("bronze-e2e", warehouse=root / "warehouse")
    try:
        results = scenarios(spark, root)
    finally:
        spark.stop()
        shutil.rmtree(root, ignore_errors=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report = {"run_at_utc": stamp, "inputs": "synthetic only", "scenarios": results, "passed": sum(results.values()), "total": len(results)}
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    (REPORT_ROOT / f"report_{stamp}.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    for name, passed in sorted(results.items()):
        sys.stdout.write(f"{'PASS' if passed else 'FAIL'} {name}\n")
    sys.stdout.write(f"{report['passed']} of {report['total']} scenarios passed\n")
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
