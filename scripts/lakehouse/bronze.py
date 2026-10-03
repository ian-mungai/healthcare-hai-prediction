"""Load stored source files into bronze Iceberg tables, exactly as published, from the S3 manifests alone.

Run from the repository root; the catalog script starts it inside the Spark container with the Polaris catalog:

    .venv/bin/python -m scripts.lakehouse.catalog job bronze -- --group hai
    .venv/bin/python -m scripts.lakehouse.catalog job bronze -- --group hospital_structure

``config/lakehouse/bronze_tables.json`` names each bronze table, the S3 collection it reads, the dataset IDs or member
name pattern that select its files, the file format and any expected counts. For each table the loader lists every
``manifest.json`` under the collection's ``manifests/`` prefix, reads each by exact version and selects its ``data``
objects. Each object is downloaded by key and version into a scratch folder, checked against the manifest's SHA-256
and size, validated as strict UTF-8 CSV in one streaming pass and read by Spark with an all-text schema, so every
value stays exactly as published. Each object's rows replace that object's partition in one Iceberg commit and carry
lineage back to the manifest and the stored file; ``bronze.column_map`` records each published header, and any SAS
variable label, beside its column. After the counts pass, each loaded table's dictionary in ``bronze_dictionary`` is
rebuilt (``scripts/lakehouse/dictionary.py``). Only S3 and the committed table map are read, so a clean checkout can
rebuild bronze. A table may declare how its CSV is laid out (delimiter, byte-order mark, label row, preamble, unnamed
headers) and narrow its files by capture with a snapshot pattern. Failure modes: data/lakehouse_planning/
bronze_hai_20261002/, bronze_manifest_20261002/ and bronze_county_20261002/.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import os
import re
import sys
import tempfile
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from scripts.lakehouse.file_readers import ReaderError, parse_line

TABLE_MAP = Path("config/lakehouse/bronze_tables.json")
# Objects the privacy deletion removed from S3 that manifests still list: skipped by exact key, version and SHA-256 [146].
RETIRED = Path("config/lakehouse/retired_objects.json")
# A manifest with these fields and no objects list records one document by checksum [153].
SINGLE_DOCUMENT = frozenset({"role", "sha256", "byte_count", "source_id"})
DEPLOYMENT = Path("infra/deployment.auto.tfvars.json")
SCRATCH = Path(tempfile.gettempdir()) / "bronze_scratch"
REPORT_ROOT = Path("data/e2e/bronze_load")
# Format -> file extensions. text_lines, sheet_rows and sas store files losslessly without interpretation [68]-[80].
# pdf_rows has no extension list: PDFs are recognized by content, since some are stored without one [99].
# CSV tables accept .txt and .dat names; their content must still pass strict CSV validation [120] [121].
# Text lines also keep JSON documents that are not variable lists, sheet rows read macro-enabled workbooks without
# running them, and docx_rows keeps Word paragraphs and table rows [132] [133] [135].
FORMATS: dict[str, tuple[str, ...]] = {
    "csv": (".csv", ".txt", ".dat"),
    "text_lines": (".txt", ".csv", ".dat", ".html", ".json"),
    "sheet_rows": (".xlsx", ".xls", ".xlsm"),
    "sas": (".sas7bdat",),
    "pdf_rows": (),
    "csv_rows": (".csv",),
    "json_variables": (".json",),
    "docx_rows": (".docx",),
}
# Formats a data table's dictionary may be: their rows hold names and descriptions in cells or attributes [124] [126].
DICTIONARY_FORMATS = ("pdf_rows", "csv_rows", "sheet_rows", "json_variables")
CSV_OPTIONS = ("delimiter", "byte_order_mark", "label_row", "preamble", "unnamed_headers", "quoted_fields", "end_of_file_marker", "trailing_blank_lines")
PREAMBLE_LIMIT = 500
# The DOS end-of-file marker some older exporters append after the final line [145].
EOF_MARKER = "\x1a"
# Manifest roles a table may load; data tables load data only [101].
ROLES = ("data", "dictionary", "methodology", "reference")
# Columns a format always has; csv and sas tables take their columns from each file's header.
FIXED_COLUMNS = {
    "text_lines": (("line_text", "STRING"), ("line_terminator", "STRING")),
    "sheet_rows": (
        ("sheet_name", "STRING"),
        ("sheet_index", "INT"),
        ("sheet_row", "INT"),
        ("date_system", "STRING"),
        ("cells", "ARRAY<STRING>"),
        ("cell_types", "ARRAY<STRING>"),
    ),
    "pdf_rows": (
        ("row_kind", "STRING"),
        ("page", "INT"),
        ("table_index", "INT"),
        ("table_row", "INT"),
        ("cells", "ARRAY<STRING>"),
        ("reader", "STRING"),
    ),
    "csv_rows": (("cells", "ARRAY<STRING>"),),
    "json_variables": (("variable_name", "STRING"), ("attributes", "MAP<STRING,STRING>")),
    "docx_rows": (("block_kind", "STRING"), ("block_index", "INT"), ("table_index", "INT"), ("table_row", "INT"), ("cells", "ARRAY<STRING>")),
}
# UTF-8 is always tried first; a table opts into Windows-1252 when its publisher writes it (failure mode 50).
ENCODINGS = ("utf-8", "cp1252", "utf-16")
# Publisher labels (SAS variable labels) sit beside their header; other formats leave them null [96].
COLUMN_MAP_SCHEMA = "_object_key STRING, table_name STRING, position INT, original_header STRING, column_name STRING, original_label STRING"
# Lines before a declared CSV header are kept here with their numbers [117].
PREAMBLE_SCHEMA = "_object_key STRING, table_name STRING, line_number INT, line_text STRING"
LINEAGE = (
    "_object_key",
    "_source_id",
    "_snapshot_id",
    "_dataset_id",
    "_release_id",
    "_manifest_key",
    "_manifest_version_id",
    "_manifest_sha256",
    "_s3_key",
    "_s3_version_id",
    "_member_path",
    "_member_sha256",
    "_release_partition",
    "_source_encoding",
    "_row_number",
)


class BronzeError(ValueError):
    """An input, validation or count check failed; the message names the object, never its values."""


class Storage(Protocol):
    """The S3 operations the loader needs: list current keys, get or download one exact version."""

    def list_keys(self, bucket: str, prefix: str) -> list[tuple[str, str]]: ...

    def list_current(self, bucket: str, prefix: str) -> list[tuple[str, str]]: ...

    def get(self, bucket: str, key: str, version_id: str) -> bytes: ...

    def download(self, bucket: str, key: str, version_id: str, path: Path) -> None: ...


def load_table_map(path: Path = TABLE_MAP) -> dict[str, Any]:
    """Read and validate the table map: unique names, one selector per table and a supported format."""
    config = json.loads(path.read_text())
    names = [table["table"] for table in config["tables"]]
    if len(set(names)) != len(names):
        raise BronzeError("the table map repeats a table name")
    for table in config["tables"]:
        if not re.fullmatch(r"[a-z][a-z0-9_]*", table["table"]) or not re.fullmatch(r"[a-z0-9_]+/[a-z0-9_]+", table["collection"]):
            raise BronzeError(f"{table['table']}: table names are lowercase identifiers and collections are publisher/collection")
        if table["format"] not in FORMATS:
            raise BronzeError(f"{table['table']}: format {table['format']!r} is not supported; supported: {', '.join(FORMATS)}")
        if ("dataset_ids" in table) == ("member_pattern" in table):
            raise BronzeError(f"{table['table']}: give exactly one selector, dataset_ids or member_pattern")
        if "member_pattern" in table:
            re.compile(table["member_pattern"])
        encodings = table.setdefault("encodings", ["utf-8"])
        # UTF-16 stands alone: a UTF-16 file decodes without error as Windows-1252, so the two never share a table [162].
        eight_bit = bool(encodings) and encodings[0] == "utf-8" and all(name in ("utf-8", "cp1252") for name in encodings)
        if not (eight_bit or encodings == ["utf-16"]) or len(set(encodings)) != len(encodings):
            raise BronzeError(f"{table['table']}: encodings must be utf-8 optionally followed by cp1252, or utf-16 alone")
        roles = table.setdefault("roles", ["data"])
        if not roles or any(role not in ROLES for role in roles) or ("data" in roles) != (roles == ["data"]):
            raise BronzeError(f"{table['table']}: roles are either data alone or any of {', '.join(ROLES[1:])}")
        table.setdefault("distinct_files", False)
        check_csv_options(table)
    by_name = {table["table"]: table for table in config["tables"]}
    for table in config["tables"]:
        if "dictionary" in table:
            target = by_name.get(table["dictionary"].get("table", ""))
            if target is None or target["format"] not in DICTIONARY_FORMATS:
                raise BronzeError(f"{table['table']}: its dictionary must name a {', '.join(DICTIONARY_FORMATS)} table in the map")
            re.compile(table["dictionary"]["file_pattern"])
            re.compile(table["dictionary"].get("section_pattern", ""))
    return dict(config)


def check_csv_options(table: dict[str, Any]) -> None:
    """Refuse layout options a table's format cannot use, or that are malformed [113] to [119]."""
    name, file_format = table["table"], table["format"]
    if "snapshot_pattern" in table:
        re.compile(table["snapshot_pattern"])
    if "delimiter" in table and (file_format not in {"csv", "csv_rows"} or len(table["delimiter"]) != 1 or table["delimiter"] in '"\r\n'):
        raise BronzeError(f"{name}: a delimiter is one character other than a quote or line break, for csv or csv_rows tables")
    for key in ("byte_order_mark", "quoted_fields"):
        if key in table and (file_format not in {"csv", "csv_rows"} or not isinstance(table[key], bool)):
            raise BronzeError(f"{name}: {key} is true or false, for csv or csv_rows tables")
    if any(key in table for key in ("label_row", "preamble", "unnamed_headers", "end_of_file_marker", "trailing_blank_lines")) and file_format != "csv":
        raise BronzeError(f"{name}: label_row, preamble, unnamed_headers, end_of_file_marker and trailing_blank_lines apply to csv tables only")
    for key in ("end_of_file_marker", "trailing_blank_lines"):
        if key in table and not isinstance(table[key], bool):
            raise BronzeError(f"{name}: {key} is true or false")
    if "label_row" in table and not (isinstance(table["label_row"], dict) and isinstance(table["label_row"].get("first_label"), str)):
        raise BronzeError(f"{name}: label_row names the label row's first cell as first_label")
    if "preamble" in table:
        if not (isinstance(table["preamble"], dict) and isinstance(table["preamble"].get("header_pattern"), str)):
            raise BronzeError(f"{name}: preamble names the header line's pattern as header_pattern")
        re.compile(table["preamble"]["header_pattern"])
    if "unnamed_headers" in table and not isinstance(table["unnamed_headers"], bool):
        raise BronzeError(f"{name}: unnamed_headers is true or false")


def select(table: dict[str, Any], dataset_id: str, file_name: str, snapshot_id: str = "") -> bool:
    """Return whether a data object belongs to the table; a snapshot pattern narrows either selector [119]."""
    if "snapshot_pattern" in table and re.search(table["snapshot_pattern"], snapshot_id) is None:
        return False
    if "dataset_ids" in table:
        return dataset_id in table["dataset_ids"]
    return re.search(table["member_pattern"], file_name) is not None


def single_document(storage: Storage, bucket: str, collection: str, manifest_key: str, manifest: dict[str, Any]) -> dict[str, Any]:
    """Read a single-document manifest as a storage manifest with one object, found by its SHA-256 [153] [154].

    The object is the one current version under the collection with the SHA-256 as a path segment; a scope exception,
    when present, must allow this exact document and role. The download is checked against the SHA-256 and size later.
    """
    digest, role = manifest["sha256"], manifest["role"]
    scope = manifest.get("scope_exception")
    if scope is not None and (scope.get("allowed_document_sha256") != digest or scope.get("allowed_role") != role):
        raise BronzeError(f"{manifest_key}: the scope exception does not allow this document and role")
    found = [(key, version) for key, version in storage.list_current(bucket, f"{collection}/") if f"/{digest}/" in key and "/manifests/" not in key]
    if len(found) != 1:
        raise BronzeError(
            f"{manifest_key}: no stored object has the document's SHA-256" if not found else f"{manifest_key}: {len(found)} stored objects share it"
        )
    key, version = found[0]
    capture = next((part.split("=", 1)[1] for part in manifest_key.split("/") if part.startswith("capture_id=")), "")
    if not capture:
        raise BronzeError(f"{manifest_key}: a single-document manifest needs a capture_id path segment")
    stored = {
        "role": role,
        "dataset_id": manifest.get("document_id", ""),
        "object": {"bucket": bucket, "key": key, "version_id": version, "sha256": digest, "byte_count": manifest["byte_count"]},
    }
    return {"snapshot_id": capture, "source_id": manifest["source_id"], "release_id": capture, "objects": [stored]}


def load_retired(path: Path = RETIRED) -> dict[tuple[str, str], str]:
    """Read the retired-objects list as (key, version ID) -> SHA-256; refuse a malformed or repeated entry [147].

    A missing file means nothing is retired, so a missing object still stops the load [146].
    """
    if not path.exists():
        return {}
    retired: dict[tuple[str, str], str] = {}
    for entry in json.loads(path.read_text())["objects"]:
        key, version, digest = entry.get("key"), entry.get("version_id"), entry.get("sha256")
        if not (isinstance(key, str) and key and isinstance(version, str) and version):
            raise BronzeError("a retired entry needs a key and a version ID")
        if not (isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest)):
            raise BronzeError(f"{key}: a retired entry's SHA-256 must be 64 hex digits")
        if (key, version) in retired:
            raise BronzeError(f"{key}: the retired list repeats an entry")
        retired[(key, version)] = digest
    return retired


def discover(
    storage: Storage, bucket: str, config: dict[str, Any], names: Iterable[str], retired: dict[tuple[str, str], str] | None = None
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """List the chosen tables' inputs from the S3 manifests; return them with the unselected data objects.

    Objects on the retired list are skipped and reported as retired, matched on key, version and SHA-256; every list
    entry under a discovered collection must match a manifest object [146] to [148].
    """
    retired = retired or {}
    tables = {table["table"]: table for table in config["tables"]}
    chosen = [tables[name] for name in names]
    by_collection: dict[str, list[dict[str, Any]]] = {}
    for table in chosen:
        by_collection.setdefault(table["collection"], []).append(table)
    inputs: list[dict[str, Any]] = []
    unselected: list[dict[str, Any]] = []
    for collection, group in sorted(by_collection.items()):
        listed = {identity: digest for identity, digest in retired.items() if identity[0].startswith(f"{collection}/")}
        matched: set[tuple[str, str]] = set()
        for manifest_key, manifest_version in storage.list_keys(bucket, f"{collection}/manifests/"):
            if not manifest_key.endswith("/manifest.json"):
                continue
            raw = storage.get(bucket, manifest_key, manifest_version)
            manifest = json.loads(raw)
            if "objects" not in manifest and set(manifest) >= SINGLE_DOCUMENT:
                manifest = single_document(storage, bucket, collection, manifest_key, manifest)
            if not {"objects", "snapshot_id", "source_id"} <= set(manifest):
                raise BronzeError(f"{manifest_key}: not a storage manifest")
            for stored in manifest["objects"]:
                role = stored.get("role")
                identity = (stored["object"].get("key", ""), stored["object"].get("version_id", ""))
                if identity in listed:
                    if listed[identity] != stored["object"].get("sha256"):
                        raise BronzeError(f"{identity[0]}: the retired list's SHA-256 differs from the manifest's")
                    matched.add(identity)
                    retired_chain = stored.get("member_chain") or [identity[0].rsplit("/", 1)[-1]]
                    unselected.append(
                        {
                            "collection": collection,
                            "dataset_id": stored.get("dataset_id", ""),
                            "file_name": retired_chain[-1].rsplit("/", 1)[-1],
                            "role": role,
                            "retired": True,
                        }
                    )
                    continue
                candidates = [table for table in group if role in table["roles"]]
                if not candidates:
                    continue
                target = stored["object"]
                chain = stored.get("member_chain") or [target["key"].rsplit("/", 1)[-1]]
                file_name = chain[-1].rsplit("/", 1)[-1]
                matches = [table for table in candidates if select(table, stored.get("dataset_id", ""), chain[-1], manifest["snapshot_id"])]
                if len(matches) > 1:
                    raise BronzeError(f"{target['key']}: selected by two tables ({', '.join(table['table'] for table in matches)})")
                if not matches:
                    if role == "data":
                        unselected.append({"collection": collection, "dataset_id": stored.get("dataset_id", ""), "file_name": file_name})
                    continue
                table = matches[0]
                if FORMATS[table["format"]] and not file_name.lower().endswith(FORMATS[table["format"]]):
                    raise BronzeError(f"{table['table']}: {file_name!r} does not have the declared {table['format']} format")
                if not target.get("version_id") or target.get("sha256") != stored.get("member_sha256", target.get("sha256")):
                    raise BronzeError(f"{target['key']}: the manifest gives no S3 version or inconsistent checksums")
                partition = next((part for part in target["key"].split("/") if part.startswith(("release_date=", "capture_id="))), "")
                inputs.append(
                    {
                        "table": table["table"],
                        "object_key": hashlib.sha256(f"{manifest['snapshot_id']}\x00{json.dumps(chain)}".encode()).hexdigest()[:32],
                        "source_id": manifest["source_id"],
                        "snapshot_id": manifest["snapshot_id"],
                        "dataset_id": stored.get("dataset_id", ""),
                        "release_id": str(manifest.get("release_id") or ""),
                        "manifest_key": manifest_key,
                        "manifest_version_id": manifest_version,
                        "manifest_sha256": hashlib.sha256(raw).hexdigest(),
                        "bucket": target["bucket"],
                        "key": target["key"],
                        "version_id": target["version_id"],
                        "sha256": target["sha256"],
                        "byte_count": target["byte_count"],
                        "member_chain": chain,
                        "release_partition": partition,
                        "file_name": file_name,
                        "encodings": list(table["encodings"]),
                        "format": table["format"],
                        **{key: table[key] for key in CSV_OPTIONS if key in table},
                    }
                )
        stale = sorted(identity[0] for identity in set(listed) - matched)
        if stale:
            raise BronzeError(f"{stale[0]}: the retired list entry matches no manifest object ({len(stale)} such entries)")
    inputs.sort(key=lambda item: (item["table"], item["snapshot_id"], json.dumps(item["member_chain"]), item["manifest_key"]))
    # One archive snapshot can list a shared member (its dictionary) in each dataset's manifest: same bytes load once [111].
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for item in inputs:
        first = unique.setdefault((item["table"], item["object_key"]), item)
        if first is not item and first["sha256"] != item["sha256"]:
            raise BronzeError("two stored objects share one snapshot and member chain")
    inputs = list(unique.values())
    # Tables that ask for it load one copy of each distinct file; the others are reported as identical copies [100].
    distinct = {table["table"] for table in chosen if table["distinct_files"]}
    seen: set[tuple[str, str]] = set()
    kept = []
    for item in inputs:
        if item["table"] in distinct and (item["table"], item["sha256"]) in seen:
            unselected.append(
                {
                    "collection": item["key"].split("/", 2)[0] + "/" + item["key"].split("/", 2)[1],
                    "dataset_id": item["dataset_id"],
                    "file_name": item["file_name"],
                    "identical_copy": True,
                }
            )
            continue
        seen.add((item["table"], item["sha256"]))
        kept.append(item)
    inputs = kept
    unselected.sort(key=lambda entry: (entry["collection"], entry["dataset_id"], entry["file_name"]))
    return inputs, unselected


def validate_csv(path: Path, jsonl: Path | None = None, encodings: Iterable[str] = ("utf-8",)) -> tuple[list[str], int, str]:
    """Stream strict comma-separated CSV with one header row; return the header, data row count and encoding."""
    found = read_csv(path, jsonl, encodings, {})
    return found["headers"], found["rows"], found["encoding"]


def read_csv(path: Path, jsonl: Path | None, encodings: Iterable[str], options: dict[str, Any]) -> dict[str, Any]:
    """Stream strict CSV once per allowed encoding until one decodes; optionally write rows to JSON Lines.

    Returns the header, labels, data row count, encoding and any preamble lines. Refuses an undeclared byte-order mark,
    bytes that no allowed encoding defines, malformed quoting and rows that differ from the header width. Python's
    ``csv`` module is the exact reader: Spark's own CSV reader turns empty fields into nulls and quoted CRLF into LF.
    A declared mark is removed and the rest read as UTF-8 [113]; a declared label row, preamble and delimiter follow
    the table map [114] to [117]. A declared end-of-file marker (0x1A) is accepted only as the last line [145], and
    declared blank lines only after the last row [161].
    """
    with path.open("rb") as raw:
        marked = raw.read(3) == b"\xef\xbb\xbf"
    if marked and not options.get("byte_order_mark"):
        raise BronzeError("the file starts with a UTF-8 byte-order mark")
    failures = []
    for encoding in ("utf-8-sig",) if marked else encodings:
        try:
            found = _stream_rows(path, jsonl, encoding, options)
        except UnicodeDecodeError as error:
            failures.append(f"{encoding.upper()} at byte {error.start}")
            continue
        if len(set(found["headers"])) != len(found["headers"]):
            raise BronzeError("the header repeats a column name")
        return {**found, "encoding": encoding}
    raise BronzeError(f"the file is not valid in any allowed encoding ({'; '.join(failures)})")


def _stream_rows(path: Path, jsonl: Path | None, encoding: str, options: dict[str, Any]) -> dict[str, Any]:
    """Read every row strictly in one encoding, checking widths and writing JSON Lines when asked."""
    rows = markers = blanks = 0
    preamble: list[str] = []
    sink = jsonl.open("w", encoding="ascii") if jsonl is not None else None
    try:
        with path.open(encoding=encoding, errors="strict", newline="") as handle:
            source: Iterator[str] = iter(handle)
            if "preamble" in options:
                pattern = re.compile(options["preamble"]["header_pattern"])
                while True:
                    line = handle.readline()
                    if not line or len(preamble) >= PREAMBLE_LIMIT:
                        raise BronzeError(f"no line within the first {PREAMBLE_LIMIT} matches the header pattern")
                    if pattern.search(line.rstrip("\r\n")):
                        break
                    preamble.append(line.rstrip("\r\n"))
                source = itertools.chain([line], handle)
            delimiter = options.get("delimiter", ",")
            reader = csv.reader(source, strict=True, delimiter=delimiter)
            headers = next(reader, None)
            if not headers:
                raise BronzeError("the file has no header row")
            labels: list[str | None] = [None] * len(headers)
            if "label_row" in options:
                first = options["label_row"]["first_label"]
                # The label row is read as one physical line, so a declared rule can read undoubled quotes [130].
                label_line = next(source, None)
                try:
                    row = parse_line(label_line.rstrip("\r\n"), delimiter, options.get("quoted_fields", False)) if label_line is not None else None
                except ReaderError as error:
                    raise BronzeError(str(error).replace("the file", "the label row")) from None
                if row is None or len(row) != len(headers) or row[0] != first:
                    raise BronzeError(f"the label row is missing: the second row must start with {first!r} and match the header's width")
                labels = list(row)
            offset = len(preamble) + (2 if "label_row" in options else 1)
            for number, row in enumerate(reader, offset + 1):
                if markers:
                    raise BronzeError(f"line {number} follows the end-of-file marker")
                if not row and options.get("trailing_blank_lines"):
                    blanks += 1
                    continue
                if blanks:
                    raise BronzeError(f"line {number} follows a blank line")
                if row == [EOF_MARKER] and options.get("end_of_file_marker"):
                    markers += 1
                    continue
                if len(row) != len(headers):
                    raise BronzeError(f"line {number} has {len(row)} fields; the header has {len(headers)}")
                rows += 1
                if sink is not None:
                    sink.write(json.dumps({"r": rows, "v": row}, ensure_ascii=True) + "\n")
    except csv.Error as error:
        raise BronzeError(f"the file is not valid CSV ({error})") from None
    finally:
        if sink is not None:
            sink.close()
    return {"headers": headers, "labels": labels, "rows": rows, "preamble": preamble, "end_of_file_markers": markers, "trailing_blank_lines": blanks}


def column_names(headers: list[str], unnamed: bool = False) -> list[str]:
    """Map published headers to bronze column names mechanically; refuse empty or colliding results.

    A header that begins with an underscore (SAS automatic variables such as ``_NAME_``) maps to ``u_`` plus the rest,
    so no published header can take a lineage column's name (failure mode 81). A table that declares unnamed headers
    names an empty header ``unnamed_<position>`` (failure mode 118).
    """
    names = []
    for position, header in enumerate(headers, 1):
        name = re.sub(r"[^0-9a-z]+", "_", header.strip().lower()).strip("_")
        if not name and unnamed and not header.strip():
            names.append(f"unnamed_{position}")
            continue
        if not name:
            raise BronzeError(f"header {header!r} has no letters or digits")
        if header.strip().startswith("_"):
            names.append(f"u_{name}")
        else:
            names.append(f"c_{name}" if name[0].isdigit() else name)
    if len(set(names)) != len(names):
        raise BronzeError("two published headers collide on one bronze column name")
    return names


def sas_column_names(names: list[str]) -> list[str]:
    """Map SAS variable names by lowercasing only; a leading underscore gets a ``u`` prefix (failure mode 82)."""
    mapped = []
    for name in names:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise BronzeError(f"SAS variable {name!r} is not an identifier")
        lowered = name.lower()
        mapped.append(f"u{lowered}" if lowered.startswith("_") else lowered)
    if len(set(mapped)) != len(mapped):
        raise BronzeError("two SAS variable names collide on one bronze column name")
    return mapped


def sha256_file(path: Path) -> tuple[str, int]:
    """Hash a file in chunks and return its SHA-256 and size."""
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def ensure_table(spark: Any, namespace: str, table: str, file_format: str = "csv") -> None:
    """Create the namespace, the bronze table (with the format's fixed columns) and the column map if missing."""
    if not re.fullmatch(r"[a-z_]+", namespace) or not re.fullmatch(r"[a-z][a-z0-9_]*", table):
        raise BronzeError("namespace and table names are lowercase identifiers")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    lineage = ", ".join(f"{name} {'BIGINT' if name == '_row_number' else 'STRING'}" for name in LINEAGE)
    fixed = "".join(f", {name} {kind}" for name, kind in FIXED_COLUMNS.get(file_format, ()))
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {namespace}.{table} ({lineage}{fixed}) USING iceberg PARTITIONED BY (_object_key) TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {namespace}.column_map ({COLUMN_MAP_SCHEMA}) USING iceberg "
        "PARTITIONED BY (_object_key) TBLPROPERTIES ('format-version'='2')"
    )
    if "original_label" not in spark.table(f"{namespace}.column_map").columns:
        spark.sql(f"ALTER TABLE {namespace}.column_map ADD COLUMNS (original_label STRING)")
    existing = spark.table(f"{namespace}.{table}").columns
    added = [name for name in LINEAGE if name not in existing]
    if added:
        spark.sql(f"ALTER TABLE {namespace}.{table} ADD COLUMNS ({', '.join(f'{name} STRING' for name in added)})")


def read_rows(spark: Any, jsonl: Path, names: list[str]) -> Any:
    """Read the validated rows from JSON Lines with an explicit schema: every value text, numbered in file order."""
    from pyspark.sql.functions import col

    frame = spark.read.schema("r BIGINT, v ARRAY<STRING>").option("mode", "FAILFAST").json(jsonl.as_uri())
    return frame.select(col("r").alias("_row_number"), *[col("v")[index].alias(name) for index, name in enumerate(names)])


def read_fixed_frame(spark: Any, jsonl: Path, schema: str, columns: Iterable[tuple[str, str]]) -> Any:
    """Read stored fixed-column rows from JSON Lines with an explicit schema, numbered in file order."""
    from pyspark.sql.functions import col

    frame = spark.read.schema(schema).option("mode", "FAILFAST").json(jsonl.as_uri())
    return frame.select(col("r").alias("_row_number"), *[col(key).alias(name) for name, key in columns])


def read_sheet_frame(spark: Any, jsonl: Path) -> Any:
    """Read stored worksheet rows from JSON Lines with an explicit schema, numbered in workbook order."""
    from scripts.lakehouse.file_readers import SHEET_COLUMNS, SHEET_SCHEMA

    return read_fixed_frame(spark, jsonl, SHEET_SCHEMA, SHEET_COLUMNS)


def parse_object(spark: Any, item: dict[str, Any], path: Path, rows_path: Path) -> tuple[Any, int, str, list[str], list[str | None], dict[str, int], list[str]]:
    """Validate one downloaded file in its format; return its Spark rows, count, encoding, headers, labels, counts and preamble."""
    from scripts.lakehouse import file_readers

    file_format = item.get("format", "csv")
    try:
        if file_format == "csv":
            found = read_csv(path, rows_path, item["encodings"], {key: item[key] for key in CSV_OPTIONS if key in item})
            names = column_names(found["headers"], item.get("unnamed_headers", False))
            stats = {key: found[key] for key in ("end_of_file_markers", "trailing_blank_lines") if found[key]}
            return read_rows(spark, rows_path, names), found["rows"], found["encoding"], found["headers"], found["labels"], stats, found["preamble"]
        if file_format == "csv_rows":
            rows, encoding = file_readers.read_csv_rows(
                path, rows_path, item["encodings"], item.get("delimiter", ","), item.get("byte_order_mark", False), item.get("quoted_fields", False)
            )
            return read_fixed_frame(spark, rows_path, file_readers.CSV_ROWS_SCHEMA, file_readers.CSV_ROWS_COLUMNS), rows, encoding, [], [], {}, []
        if file_format == "json_variables":
            rows = file_readers.read_json_variables(path, rows_path)
            return read_fixed_frame(spark, rows_path, file_readers.JSON_SCHEMA, file_readers.JSON_COLUMNS), rows, "utf-8", [], [], {}, []
        if file_format == "sas":
            headers, labels, rows, encoding = file_readers.read_sas(path, rows_path)
            return read_rows(spark, rows_path, sas_column_names(headers)), rows, encoding, headers, labels, {}, []
        if file_format == "text_lines":
            rows, encoding = file_readers.read_text_lines(path, rows_path, item["encodings"])
            return read_rows(spark, rows_path, list(file_readers.TEXT_COLUMNS)), rows, encoding, [], [], {}, []
        if file_format == "docx_rows":
            rows, stats = file_readers.read_docx_rows(path, rows_path)
            return read_fixed_frame(spark, rows_path, file_readers.DOCX_SCHEMA, file_readers.DOCX_COLUMNS), rows, "docx", [], [], stats, []
        if file_format == "pdf_rows":
            rows, stats = file_readers.read_pdf_rows(path, rows_path)
            return read_fixed_frame(spark, rows_path, file_readers.PDF_SCHEMA, file_readers.PDF_COLUMNS), rows, "pdf", [], [], stats, []
        rows, encoding, stats = file_readers.read_sheet_rows(path, rows_path)
        return read_sheet_frame(spark, rows_path), rows, encoding, [], [], stats, []
    except file_readers.ReaderError as error:
        raise BronzeError(str(error)) from None


def load_object(spark: Any, item: dict[str, Any], storage: Storage, namespace: str, scratch: Path) -> tuple[int, dict[str, int]]:
    """Load one object and return its rows and format counts; any failure names its table, snapshot and file (51)."""
    try:
        return _load_object(spark, item, storage, namespace, scratch)
    except BronzeError as error:
        raise BronzeError(f"{item['table']} {item['snapshot_id']} {item['file_name']}: {error}") from None
    except Exception as error:
        # Storage, network and Spark errors name the file too (failure mode 52); the original stays chained.
        raise BronzeError(f"{item['table']} {item['snapshot_id']} {item['file_name']}: {type(error).__name__}: {error}") from error


def _load_object(spark: Any, item: dict[str, Any], storage: Storage, namespace: str, scratch: Path) -> tuple[int, dict[str, int]]:
    """Download, check and validate one object, then replace its rows and column map, each in one Iceberg commit."""
    from pyspark.sql.functions import lit

    scratch.mkdir(parents=True, exist_ok=True)
    # Keep the published extension: the Excel readers choose their parser by it.
    path = scratch / f"{item['object_key']}{Path(item['file_name']).suffix.lower()}"
    rows_path = scratch / f"{item['object_key']}.jsonl"
    try:
        storage.download(item["bucket"], item["key"], item["version_id"], path)
        if sha256_file(path) != (item["sha256"], item["byte_count"]):
            raise BronzeError(f"{item['snapshot_id']} {item['table']}: the bytes read do not match the recorded SHA-256 and size")
        target = f"{namespace}.{item['table']}"
        ensure_table(spark, namespace, item["table"], item.get("format", "csv"))
        frame, rows, encoding, headers, labels, stats, preamble = parse_object(spark, item, path, rows_path)
        names = [name for name in frame.columns if name != "_row_number"]
        if frame.count() != rows:
            raise BronzeError(f"{item['snapshot_id']} {item['table']}: Spark read a different row count from the validation pass")
        lineage = {
            "_object_key": item["object_key"],
            "_source_id": item["source_id"],
            "_snapshot_id": item["snapshot_id"],
            "_dataset_id": item["dataset_id"],
            "_release_id": item["release_id"],
            "_manifest_key": item["manifest_key"],
            "_manifest_version_id": item["manifest_version_id"],
            "_manifest_sha256": item["manifest_sha256"],
            "_s3_key": item["key"],
            "_s3_version_id": item["version_id"],
            "_member_path": "!".join(item["member_chain"]),
            "_member_sha256": item["sha256"],
            "_release_partition": item["release_partition"],
            "_source_encoding": encoding,
        }
        existing = spark.table(target).columns
        added = [name for name in names if name not in existing]
        if added:
            spark.sql(f"ALTER TABLE {target} ADD COLUMNS ({', '.join(f'{name} STRING' for name in added)})")
        # Line columns up with the table: lineage first; a column this file lacks stays null, never an empty string.
        ordered = []
        for name in [*existing, *added]:
            if name in lineage:
                ordered.append(lit(lineage[name]).alias(name))
            elif name in frame.columns:
                ordered.append(frame[name])
            else:
                ordered.append(lit(None).cast("string").alias(name))
        frame.select(*ordered).writeTo(target).overwritePartitions()
        if headers:
            mapping = [
                (item["object_key"], item["table"], position, header, name, label)
                for position, (header, name, label) in enumerate(zip(headers, names, labels, strict=True), 1)
            ]
            spark.createDataFrame(mapping, COLUMN_MAP_SCHEMA).writeTo(f"{namespace}.column_map").overwritePartitions()
        if "preamble" in item:
            write_preamble(spark, namespace, item, preamble)
        return rows, stats
    finally:
        path.unlink(missing_ok=True)
        rows_path.unlink(missing_ok=True)


def write_preamble(spark: Any, namespace: str, item: dict[str, Any], lines: list[str]) -> None:
    """Replace one object's preamble lines with their numbers; an object without any keeps none [117]."""
    from pyspark.sql.functions import col, lit

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {namespace}.file_preambles ({PREAMBLE_SCHEMA}) USING iceberg "
        "PARTITIONED BY (_object_key) TBLPROPERTIES ('format-version'='2')"
    )
    rows = [(item["object_key"], item["table"], number, text) for number, text in enumerate(lines, 1)]
    # Replace exactly this object's rows, also when it now has none.
    spark.createDataFrame(rows, PREAMBLE_SCHEMA).writeTo(f"{namespace}.file_preambles").overwrite(col("_object_key") == lit(item["object_key"]))


def load_objects(spark: Any, inputs: list[dict[str, Any]], storage: Storage, namespace: str, scratch: Path = SCRATCH) -> dict[str, dict[str, int]]:
    """Load every input object in order and return per-table object and row counts."""
    summary: dict[str, dict[str, int]] = {}
    for item in inputs:
        rows, stats = load_object(spark, item, storage, namespace, scratch)
        counts = summary.setdefault(item["table"], {"objects": 0, "rows": 0})
        counts["objects"] += 1
        counts["rows"] += rows
        for key, value in stats.items():
            counts[key] = counts.get(key, 0) + value
    return summary


def verify(spark: Any, config: dict[str, Any], inputs: list[dict[str, Any]], namespace: str) -> dict[str, dict[str, Any]]:
    """Recount each loaded table against its inputs and the table map's expected distinct files and rows."""
    from pyspark.sql import functions

    results: dict[str, dict[str, Any]] = {}
    for table in config["tables"]:
        keys = {item["object_key"] for item in inputs if item["table"] == table["table"]}
        if not keys:
            continue
        frame = spark.table(f"{namespace}.{table['table']}")
        loaded = {row["_object_key"] for row in frame.select("_object_key").distinct().collect()}
        distinct = frame.groupBy("_member_sha256").agg(functions.max("_row_number").alias("rows"))
        check: dict[str, Any] = {
            "objects": len(loaded),
            "objects_match_inputs": loaded == keys,
            "rows": frame.count(),
            "distinct_files": distinct.count(),
            "distinct_rows": int(distinct.agg(functions.sum("rows")).collect()[0][0] or 0),
        }
        expectations = [check["objects_match_inputs"]]
        for field in ("distinct_files", "distinct_rows"):
            if f"expected_{field}" in table:
                check[f"{field}_match_expected"] = check[field] == table[f"expected_{field}"]
                expectations.append(check[f"{field}_match_expected"])
        check["passed"] = all(expectations)
        results[table["table"]] = check
    return results


def prune(spark: Any, config: dict[str, Any], names: Iterable[str], inputs: list[dict[str, Any]], namespace: str) -> dict[str, int]:
    """Drop each run table's partitions that its inputs no longer select, with their column-map and preamble rows [150].

    Only the named tables are touched; a table with no inputs in this run is left as it is and counts 0.
    """
    pruned: dict[str, int] = {}
    tables = {table["table"] for table in config["tables"]}
    for name in names:
        keys = {item["object_key"] for item in inputs if item["table"] == name}
        pruned[name] = 0
        if not keys or name not in tables or not spark.catalog.tableExists(f"{namespace}.{name}"):
            continue
        loaded = {row["_object_key"] for row in spark.table(f"{namespace}.{name}").select("_object_key").distinct().collect()}
        stale = sorted(loaded - keys)
        if not stale:
            continue
        # An empty overwrite with a filter deletes the matching rows in one commit, with no SQL text built from values.
        from pyspark.sql.functions import col

        target = spark.table(f"{namespace}.{name}")
        target.limit(0).writeTo(f"{namespace}.{name}").overwrite(col("_object_key").isin(stale))
        for side in ("column_map", "file_preambles"):
            if spark.catalog.tableExists(f"{namespace}.{side}"):
                rows = spark.table(f"{namespace}.{side}")
                rows.limit(0).writeTo(f"{namespace}.{side}").overwrite((col("table_name") == name) & col("_object_key").isin(stale))
        pruned[name] = len(stale)
    return pruned


class S3Storage:
    """Read-only S3 access with the project's AWS profile."""

    def __init__(self) -> None:
        import boto3

        self.client = boto3.Session(profile_name=os.environ["AWS_PROFILE"], region_name=os.environ["AWS_REGION"]).client("s3")

    def list_keys(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        """Return each current manifest key with its version ID from one paginated version listing."""
        keys = []
        for page in self.client.get_paginator("list_object_versions").paginate(Bucket=bucket, Prefix=prefix):
            for entry in page.get("Versions", []):
                if entry["IsLatest"] and entry["Key"].endswith("/manifest.json"):
                    keys.append((entry["Key"], entry["VersionId"]))
        return sorted(keys)

    def list_current(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        """Return every current object key under the prefix with its version ID."""
        keys: list[tuple[str, str]] = []
        for page in self.client.get_paginator("list_object_versions").paginate(Bucket=bucket, Prefix=prefix):
            keys.extend((entry["Key"], entry["VersionId"]) for entry in page.get("Versions", []) if entry["IsLatest"])
        return sorted(keys)

    def get(self, bucket: str, key: str, version_id: str) -> bytes:
        return bytes(self.client.get_object(Bucket=bucket, Key=key, VersionId=version_id)["Body"].read())

    def download(self, bucket: str, key: str, version_id: str, path: Path) -> None:
        self.client.download_file(bucket, key, str(path), ExtraArgs={"VersionId": version_id})


def main() -> int:
    """Load the chosen group or tables into the Polaris catalog and write a counts-only report."""
    from scripts.lakehouse.session import JOB_MEMORY, spark_session

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    choice = parser.add_mutually_exclusive_group(required=True)
    choice.add_argument("--group")
    choice.add_argument("--tables", help="comma-separated table names")
    args = parser.parse_args()
    config = load_table_map()
    names = [table["table"] for table in config["tables"] if table.get("group") == args.group] if args.group else args.tables.split(",")
    if not names or any(name not in {table["table"] for table in config["tables"]} for name in names):
        sys.stderr.write("bronze: unknown group or table\n")
        return 1
    bucket = str(json.loads(DEPLOYMENT.read_text())["data_bucket_name"])
    storage = S3Storage()
    try:
        inputs, unselected = discover(storage, bucket, config, names, load_retired())
    except BronzeError as error:
        sys.stderr.write(f"bronze: {error}\n")
        return 1
    from scripts.lakehouse import dictionary

    spark = spark_session("bronze", memory=JOB_MEMORY)
    dictionaries: dict[str, int] = {}
    pruned: dict[str, int] = {}
    try:
        summary = load_objects(spark, inputs, storage, config["namespace"])
        pruned = prune(spark, config, names, inputs, config["namespace"])
        checks = verify(spark, config, inputs, config["namespace"])
        # Dictionaries follow every passing load, so they never describe an older bronze table [91].
        if all(check["passed"] for check in checks.values()):
            dictionaries = dictionary.build(spark, config, sorted(checks), config["namespace"])
    except BronzeError as error:
        sys.stderr.write(f"bronze: {error}\n")
        return 1
    finally:
        spark.stop()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    unloaded: dict[str, int] = {}
    for entry in unselected:
        kind = " identical copy" if entry.get("identical_copy") else f" retired {entry['role']}" if entry.get("retired") else ""
        label = f"{entry['collection']} {entry['dataset_id']} .{entry['file_name'].rsplit('.', 1)[-1].lower()}{kind}"
        unloaded[label] = unloaded.get(label, 0) + 1
    passed = all(check["passed"] for check in checks.values())
    report = {
        "run_at_utc": stamp,
        "tables": names,
        "inputs": len(inputs),
        "loaded": summary,
        "checks": checks,
        "pruned": {name: count for name, count in pruned.items() if count},
        "dictionaries": dictionaries,
        "unselected": unloaded,
        "passed": passed,
    }
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    (REPORT_ROOT / f"report_{stamp}.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(json.dumps(report, sort_keys=True) + "\n")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
