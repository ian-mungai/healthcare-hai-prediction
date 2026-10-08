"""Build one data dictionary per bronze table, describing only the columns that came from the published data.

Run from the repository root; the catalog script starts it inside the Spark container with the Polaris catalog. The
bronze job also rebuilds the dictionaries of the tables it loads:

    .venv/bin/python -m scripts.lakehouse.catalog job dictionary -- --all
    .venv/bin/python -m scripts.lakehouse.catalog job dictionary -- --tables cms_hai_state,cms_ipps_sas

``bronze_dictionary.<table>`` describes ``bronze.<table>``: one row per published column in the bronze table's order,
with its bronze type, every published header behind it, a description only where the publisher gave one (SAS variable
labels and label rows, or the publisher's data dictionary loaded as PDF, CSV, Excel or JSON variable rows) or the
bronze format defines the column, the published type where the dictionary gives one, the files that carry it, the
release range when releases are dates, and its rows, nulls and empty values counted only within those files. Lineage
columns are left out. Each dictionary is replaced whole in one Iceberg commit, so reruns are safe. Failure modes 83 to
96: plans/bronze_dictionary_20261002/failure_modes.md; 97 to 112: publisher_dictionaries_20261002/;
113 to 129: bronze_county_20261002/.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from typing import Any

from scripts.lakehouse import bronze

NAMESPACE = "bronze_dictionary"
# Columns counted per Spark aggregation; the SAS table alone has 5,583 [94].
BATCH = 400
SCHEMA = (
    "position INT, column_name STRING, data_type STRING, original_headers ARRAY<STRING>, description STRING, "
    "description_source STRING, description_variants INT, publisher_type STRING, files INT, first_release STRING, "
    "last_release STRING, rows BIGINT, null_rows BIGINT, empty_rows BIGINT, missing_share DOUBLE"
)
PUBLISHER = "publisher variable label"
FORMAT = "bronze storage format"
# The bronze format's own definition of its storage columns [85].
FORMAT_DESCRIPTIONS = {
    "text_lines": {
        "line_text": "One line of the file exactly as published, without its line terminator.",
        "line_terminator": "The line's terminator as published: CRLF, LF or CR, or empty for a final line without one.",
    },
    "sheet_rows": {
        "sheet_name": "Worksheet name as published.",
        "sheet_index": "Worksheet position in the workbook, starting at 1.",
        "sheet_row": "Row number in the worksheet, starting at 1; empty rows keep their numbers.",
        "date_system": "The workbook's date system, 1900 or 1904, needed to convert date serial numbers.",
        "cells": "Each cell's stored value as text, in column order; empty cells are null and trailing empty cells are dropped.",
        "cell_types": "Each cell's type as the reader reports it (s text, n number, b boolean, d or date for dates, e error), aligned with cells.",
    },
    "pdf_rows": {
        "row_kind": "table for a row of a table the reader found, text for a line of the page's text.",
        "page": "Page number in the PDF, starting at 1.",
        "table_index": "Table position on the page, starting at 1; null for text lines.",
        "table_row": "Row number in the table, or line number on the page for text lines, starting at 1.",
        "cells": "The table row's cells as the reader returns them, or the text line as one cell; empty cells are null or empty.",
        "reader": "The PDF reader and version that produced the row.",
    },
    "csv_rows": {
        "cells": "One CSV row's cells as published, header rows included, in file order.",
    },
    "json_variables": {
        "variable_name": "The variable's name as the publisher's variables object gives it.",
        "attributes": "The variable's attributes as published (label, concept, predicate type and others); non-text values as JSON text.",
    },
    "docx_rows": {
        "block_kind": "paragraph for a body paragraph, table for a row of a table, in document order.",
        "block_index": "Position of the paragraph or table in the document body, starting at 1; a table's rows share its number.",
        "table_index": "Table number in the document, starting at 1; null for paragraphs.",
        "table_row": "Row number in the table, starting at 1; null for paragraphs.",
        "cells": "Each table cell's text, or the paragraph's text as one cell; tabs and line breaks kept, empty text kept as empty.",
    },
}
DICTIONARY = "publisher dictionary"
# Header labels that mark a dictionary table's name, description and type columns, after name_key [102].
NAME_LABELS = {"variable name", "term name", "column name", "column name csv", "field name", "data element", "element name"}
# A "Label" column is the publisher's description too (ACS column metadata) [125].
DESCRIPTION_LABELS = {"description", "definition", "variable description", "field description", "label"}
TYPE_LABELS = {"type", "data type"}
RELEASE_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


def name_key(text: str) -> str:
    """Normalize a published name for exact matching: case, dashes, underscores, slashes and spacing only [105]."""
    folded = unicodedata.normalize("NFKC", text).lower()
    folded = re.sub(r"[\u2010-\u2015_/-]+", " ", folded)
    return re.sub(r"[^0-9a-z]+", " ", folded).strip()


def edition_of(release_id: str | None, snapshot_id: str) -> str:
    """Return a dictionary edition's date: its release date, or else the date it was captured [106]."""
    if release_id and RELEASE_DATE.fullmatch(release_id):
        return release_id
    stamp = re.search(r"__(\d{4})(\d{2})(\d{2})T", snapshot_id)
    return "-".join(stamp.groups()) if stamp else ""


def parse_entries(rows: list[list[str | None]]) -> list[dict[str, Any]]:
    """Turn one dictionary file's table rows into entries of names, description and type [102] [103] [104] [108]."""
    entries: list[dict[str, Any]] = []
    header: list[tuple[int, str]] | None = None
    header_keys: list[str] = []
    # Care Compare dictionaries name each file's section in "File Name" rows, sometimes split over several rows [108].
    section: list[str] = []
    collecting = False
    for cells in rows:
        keys = [name_key(cell or "") for cell in cells]
        first = keys[0] if keys else ""
        if first == "file name":
            section, collecting = [(cell or "").strip() for cell in cells[1:] if (cell or "").strip()], True
            continue
        if collecting and not first and any(keys[1:]):
            section += [(cell or "").strip() for cell in cells[1:] if (cell or "").strip()]
            continue
        collecting = False
        if first in {"table", "description"}:
            if first == "table":
                section = []
            continue
        roles = [("name" if key in NAME_LABELS else "description" if key in DESCRIPTION_LABELS else "type" if key in TYPE_LABELS else "other") for key in keys]
        if "name" in roles and ("description" in roles or "type" in roles):
            header = [(index, role) for index, (key, role) in enumerate(zip(keys, roles, strict=True)) if key]
            header_keys = keys
            continue
        if header is None or keys == header_keys or len(cells) <= header[-1][0]:
            continue
        values: dict[str, list[str]] = {"name": [], "description": [], "type": []}
        filled = [(index, " ".join((cell or "").split())) for index, cell in enumerate(cells) if (cell or "").strip()]
        if len(filled) == len(header):
            # As many values as labels: map them in order, whatever their cell offsets [112].
            pairs = [(role, value) for (_, role), (_, value) in zip(header, filled, strict=True)]
        else:
            # Otherwise each value belongs to the nearest label at or to its right [112].
            pairs = []
            for number, (stop, role) in enumerate(header):
                start = header[number - 1][0] + 1 if number else 0
                pairs += [(role, value) for index, value in filled if start <= index <= stop][:1]
        for role, value in pairs:
            if role in values:
                values[role].append(value)
        if values["name"]:
            entries.append(
                {
                    "names": values["name"],
                    "description": " ".join(values["description"]) or None,
                    "type": " ".join(values["type"]) or None,
                    "section": "".join(section) or None,
                }
            )
        elif values["description"] and entries and entries[-1]["description"]:
            entries[-1]["description"] += " " + " ".join(values["description"])
    return entries


def dictionary_files(spark: Any, namespace: str, table: str, pattern: re.Pattern[str]) -> list[dict[str, Any]]:
    """Return each matching dictionary file with its rows in reading order, as cells or as variables [124]."""
    from pyspark.sql import functions as F

    frame = spark.table(f"{namespace}.{table}")
    columns = set(frame.columns)
    if "variable_name" in columns:
        order, kind = ["_row_number"], "variables"
        frame = frame.select("_object_key", "_member_path", "_release_id", "_snapshot_id", "_row_number", "variable_name", "attributes")
    elif "row_kind" in columns:
        order, kind = ["page", "table_index", "table_row"], "cells"
        frame = frame.where(F.col("row_kind") == "table").select("_object_key", "_member_path", "_release_id", "_snapshot_id", *order, "cells")
    elif "sheet_row" in columns:
        order, kind = ["sheet_index", "sheet_row"], "cells"
        frame = frame.select("_object_key", "_member_path", "_release_id", "_snapshot_id", *order, "cells")
    else:
        order, kind = ["_row_number"], "cells"
        frame = frame.select("_object_key", "_member_path", "_release_id", "_snapshot_id", "_row_number", "cells")
    files: dict[str, dict[str, Any]] = {}
    for row in frame.collect():
        name = row["_member_path"].split("!")[-1].rsplit("/", 1)[-1]
        if not pattern.search(name):
            continue
        entry = files.setdefault(row["_object_key"], {"file": name, "edition": edition_of(row["_release_id"], row["_snapshot_id"]), "rows": [], "kind": kind})
        value = (row["variable_name"], dict(row["attributes"] or {})) if kind == "variables" else list(row["cells"] or [])
        entry["rows"].append((tuple(row[key] for key in order), value))
    for entry in files.values():
        entry["rows"] = [value for _, value in sorted(entry["rows"], key=lambda item: item[0])]
    return list(files.values())


def publisher_definitions(spark: Any, namespace: str, dictionary: dict[str, str]) -> dict[str, list[dict[str, Any]]]:
    """Return each normalized name's definitions from the dictionary table's files that match the pattern [106] [107]."""
    pattern = re.compile(dictionary["file_pattern"])
    section = re.compile(dictionary["section_pattern"]) if "section_pattern" in dictionary else None
    definitions: dict[str, list[dict[str, Any]]] = {}
    for item in dictionary_files(spark, namespace, dictionary["table"], pattern):
        if item["kind"] == "variables":
            # A variables list gives each name its label and predicate type directly [124].
            entries = [
                {"names": [name], "description": attributes.get("label"), "type": attributes.get("predicateType"), "section": None}
                for name, attributes in item["rows"]
            ]
        else:
            entries = parse_entries(item["rows"])
        for entry in entries:
            if section and not section.search(entry["section"] or ""):
                continue
            for name in entry["names"]:
                definitions.setdefault(name_key(name), []).append({**entry, "file": item["file"], "edition": item["edition"]})
    return definitions


def publisher_description(found: list[dict[str, Any]]) -> tuple[str | None, str | None, int | None, str | None]:
    """Return the newest edition's description and source, the count of distinct texts, and the newest published type [106]."""
    described = sorted((entry for entry in found if entry["description"]), key=lambda entry: (entry["edition"], entry["file"]))
    typed = sorted((entry for entry in found if entry["type"]), key=lambda entry: (entry["edition"], entry["file"]))
    published_type = typed[-1]["type"] if typed else None
    if not described:
        return None, None, None, published_type
    newest = described[-1]
    return newest["description"], f"{DICTIONARY} {newest['file']} ({newest['edition']})", len({entry["description"] for entry in described}), published_type


def published_columns(spark: Any, namespace: str, table: str) -> list[tuple[str, str]]:
    """Return the bronze table's columns and types in order, without lineage columns [83]."""
    return [(field.name, field.dataType.simpleString()) for field in spark.table(f"{namespace}.{table}").schema.fields if field.name not in bronze.LINEAGE]


def object_counts(spark: Any, namespace: str, table: str, columns: list[tuple[str, str]], batch: int) -> dict[str, dict[str, Any]]:
    """Count each object's rows and each column's nulls and empty values, a batch of columns at a time [87] [94]."""
    from pyspark.sql import functions as F

    frame = spark.table(f"{namespace}.{table}")
    counts: dict[str, dict[str, Any]] = {}
    for row in frame.groupBy("_object_key").agg(F.count(F.lit(1)).alias("rows"), F.first("_release_id").alias("release")).collect():
        counts[row["_object_key"]] = {"rows": int(row["rows"]), "release": row["release"], "null": {}, "empty": {}}
    for start in range(0, len(columns), batch):
        aggregates = []
        for index, (name, kind) in enumerate(columns[start : start + batch]):
            value = F.col(f"`{name}`")
            empty = F.size(value) == 0 if kind.startswith("array") else value == "" if kind == "string" else F.lit(False)
            aggregates.append(F.count(F.when(value.isNull(), 1)).alias(f"n{index}"))
            aggregates.append(F.count(F.when(empty, 1)).alias(f"e{index}"))
        for row in frame.groupBy("_object_key").agg(*aggregates).collect():
            entry = counts[row["_object_key"]]
            for index, (name, _) in enumerate(columns[start : start + batch]):
                entry["null"][name] = int(row[f"n{index}"])
                entry["empty"][name] = int(row[f"e{index}"])
    return counts


def column_sources(spark: Any, namespace: str, table: str, objects: set[str]) -> dict[str, dict[str, Any]]:
    """Return each column's objects, published headers and labels from the column map, for objects in the table [84] [90]."""
    sources: dict[str, dict[str, Any]] = {}
    mapped = spark.table(f"{namespace}.column_map").where(f"table_name = '{table}'").collect()
    for row in mapped:
        if row["_object_key"] not in objects:
            continue
        entry = sources.setdefault(row["column_name"], {"objects": set(), "headers": set(), "labels": set()})
        entry["objects"].add(row["_object_key"])
        entry["headers"].add(row["original_header"])
        if row["original_label"]:
            entry["labels"].add(row["original_label"])
    return sources


def describe(spark: Any, namespace: str, table: str, config: dict[str, Any], batch: int) -> list[tuple[Any, ...]]:
    """Return the dictionary rows for one bronze table."""
    file_format = config["format"]
    definitions = publisher_definitions(spark, namespace, config["dictionary"]) if "dictionary" in config else {}
    columns = published_columns(spark, namespace, table)
    counts = object_counts(spark, namespace, table, columns, batch)
    fixed = file_format in bronze.FIXED_COLUMNS
    sources = {} if fixed else column_sources(spark, namespace, table, set(counts))
    rows = []
    for position, (name, kind) in enumerate(columns, 1):
        headers: list[str] | None
        description: str | None
        source: str | None
        variants: int | None = None
        published_type: str | None = None
        if fixed:
            # Storage-format columns are in every file and described by the format itself [85] [86].
            objects, headers, description, source = set(counts), None, FORMAT_DESCRIPTIONS[file_format][name], FORMAT
        else:
            entry = sources.get(name, {"objects": set(), "headers": set(), "labels": set()})
            labels = sorted(entry["labels"])
            objects, headers = entry["objects"], sorted(entry["headers"])
            description, source = (" | ".join(labels), PUBLISHER) if labels else (None, None)
            found = [definition for key in sorted({name_key(header) for header in headers} | {name_key(name)}) for definition in definitions.get(key, [])]
            if found and not labels:
                description, source, variants, published_type = publisher_description(found)
            elif found:
                published_type = publisher_description(found)[3]
        releases = [counts[key]["release"] for key in objects]
        dated = bool(releases) and all(release and RELEASE_DATE.fullmatch(release) for release in releases)
        total = sum(counts[key]["rows"] for key in objects)
        nulls = sum(counts[key]["null"][name] for key in objects)
        empty = sum(counts[key]["empty"][name] for key in objects)
        rows.append(
            (
                position,
                name,
                kind,
                headers,
                description,
                source,
                variants,
                published_type,
                len(objects),
                min(releases) if dated else None,
                max(releases) if dated else None,
                total,
                nulls,
                empty,
                (nulls + empty) / total if total else None,
            )
        )
    return rows


def build(spark: Any, config: dict[str, Any], names: list[str], namespace: str, batch: int = BATCH) -> dict[str, int]:
    """Rebuild the named tables' dictionaries, each in one commit; refuse every bad name before writing any [92] [93]."""
    tables = {table["table"]: table for table in config["tables"]}
    loaded = {row["tableName"] for row in spark.sql(f"SHOW TABLES IN {namespace}").collect()}
    for name in names:
        if name not in tables:
            raise bronze.BronzeError(f"{name}: not in the table map")
        needed = [name, tables[name]["dictionary"]["table"]] if "dictionary" in tables[name] else [name]
        for table in needed:
            if table not in loaded:
                raise bronze.BronzeError(f"{table}: not loaded in {namespace}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    written = {}
    for name in names:
        rows = describe(spark, namespace, name, tables[name], batch)
        spark.createDataFrame(rows, SCHEMA).coalesce(1).writeTo(f"{NAMESPACE}.{name}").using("iceberg").tableProperty("format-version", "2").createOrReplace()
        written[name] = len(rows)
    return written


def main() -> int:
    """Rebuild the chosen dictionaries in the Polaris catalog and print the column counts."""
    from scripts.lakehouse.session import job_memory, spark_session

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    choice = parser.add_mutually_exclusive_group(required=True)
    choice.add_argument("--all", action="store_true")
    choice.add_argument("--tables", help="comma-separated table names")
    args = parser.parse_args()
    config = bronze.load_table_map()
    names = [table["table"] for table in config["tables"]] if args.all else args.tables.split(",")
    spark = spark_session("bronze-dictionary", memory=job_memory())
    try:
        written = build(spark, config, names, config["namespace"])
    except bronze.BronzeError as error:
        sys.stderr.write(f"dictionary: {error}\n")
        return 1
    finally:
        spark.stop()
    sys.stdout.write(json.dumps({"dictionaries": written}, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
