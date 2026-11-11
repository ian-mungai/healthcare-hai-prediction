"""Person-field policy at the bronze load boundary.

All the data is public and raw/ keeps each published file whole. A bronze table whose files can name people has a reviewed policy in
``config/lakehouse/person_policies.json``. The loader applies it to the rows it reads, before anything reaches Spark: direct identifier columns are dropped,
identifiers and names become keyed pseudonyms and free-text cells that hold a person named in the same file are replaced. The treatments, the normalization
and the HMAC-SHA-256 are those of ``scripts/acquisition/pseudonymize.py``, so the same person gets the same pseudonym there and here.

A file is protected only through a layout the policy lists. A header that matches a policy is transformed. A header that matches a listed unchanged layout
(a stored version 1 capture, already filtered at acquisition) loads as it is after a guard checks its rows. Any other header stops the load. The key is
read from Parameter Store at use, held in memory and never written or logged; every error carries a fixed code and no value.
"""

from __future__ import annotations

import json
import os
import re
import unicodedata
from collections.abc import Iterator
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from botocore.config import Config
from botocore.exceptions import BotoCoreError

from scripts import pseudonym_core as pseudonymize
from scripts.pseudonym_core import KeyReader, PseudonymizationError

POLICY_PATH = Path("config/lakehouse/person_policies.json")


class PersonFieldError(ValueError):
    """The policy cannot be applied. The message holds a fixed code or a table name, never a source value or a key."""


@dataclass(frozen=True)
class Protected:
    """The header the loader reads after protection and the counts of what the policy did."""

    headers: list[str]
    stats: dict[str, int]


@lru_cache(maxsize=4)
def read_policies(path: Path) -> dict[str, Any]:
    """Read the policy file and validate every policy and guard with the pseudonymization rules; a bad file stops the load."""
    document = json.loads(path.read_text())
    if set(document) != {"version", "key_version", "tables"} or document["version"] != 1 or not pseudonymize.KEY_VERSION.fullmatch(document["key_version"]):
        raise PersonFieldError("person policy file invalid")
    for table, entry in document["tables"].items():
        if "blocked" in entry:
            if set(entry) != {"blocked"} or not isinstance(entry["blocked"], str):
                raise PersonFieldError(f"{table}: person policy entry invalid")
            continue
        if set(entry) == {"reason", "sheet_cells"}:
            check_sheet_rule(table, entry["sheet_cells"])
            continue
        if set(entry) != {"reason", "policies", "unchanged"} or not entry["policies"]:
            raise PersonFieldError(f"{table}: person policy entry invalid")
        try:
            for policy in entry["policies"]:
                pseudonymize.validate_policy(policy)
            for layout in entry["unchanged"]:
                pseudonymize.validate_condition(layout["guard"], layout["headers"])
        except (PseudonymizationError, KeyError, TypeError) as error:
            raise PersonFieldError(f"{table}: person policy invalid ({error})") from None
    return dict(document)


def load_policies(path: Path = POLICY_PATH) -> dict[str, Any]:
    """The per-table entries of the reviewed policy file."""
    return dict(read_policies(path)["tables"])


def refuse_blocked(table: str, path: Path = POLICY_PATH) -> None:
    """Stop a load into a table that has no reviewed policy yet."""
    entry = load_policies(path).get(table)
    if entry is not None and "blocked" in entry:
        raise PersonFieldError(f"{table}: no reviewed person-field policy; {entry['blocked']}")


def parameter_store_reader() -> KeyReader:
    """A reader of the pseudonym key over the task's own identity."""
    import boto3

    try:
        return pseudonymize.parameter_store_reader(boto3.client("ssm", config=Config(**pseudonymize.CLIENT_OPTIONS)))
    except BotoCoreError:
        raise PseudonymizationError("aws_settings_invalid") from None


def stored_rows(rows_path: Path, headers: list[str]) -> Iterator[tuple[int, dict[str, str]]]:
    """Yield (row number, cells by header) for each line of the JSON Lines rows the CSV reader wrote."""
    with rows_path.open(encoding="ascii") as handle:
        for line in handle:
            record = json.loads(line)
            yield record["r"], dict(zip(headers, record["v"], strict=True))


def check_guard(guard: dict, headers: list[str], rows_path: Path, table: str) -> None:
    """Every row of an unchanged layout must hold the guard's selecting value, so a clinician or an individual cannot enter through a filtered layout."""
    for _, row in stored_rows(rows_path, headers):
        if pseudonymize.selects(guard, row[guard["column"]]) is not True:
            raise PersonFieldError(f"{table}: a row of the unchanged layout fails its guard")


def transform_rows(policy: dict, key_version: str, reader: KeyReader, headers: list[str], rows_path: Path) -> Protected:
    """Rewrite the JSON Lines rows through the policy and return the new header and counts; the key is read first and the rows are replaced atomically."""
    key = pseudonymize.load_key(key_version, reader)
    scan = pseudonymize.scan_rows(policy, (row for _, row in stored_rows(rows_path, headers)))
    statistics, seen = pseudonymize.new_statistics(policy)
    staging = rows_path.with_name(rows_path.name + ".protected")
    try:
        with staging.open("w", encoding="ascii") as sink:
            for number, row in stored_rows(rows_path, headers):
                cells = pseudonymize.convert_row(policy, key, key_version, row, scan, statistics, seen)
                sink.write(json.dumps({"r": number, "v": cells}, ensure_ascii=True) + "\n")
        os.replace(staging, rows_path)
    finally:
        staging.unlink(missing_ok=True)
    stats = {"person_rows": statistics["rows"], "person_cells_blanked": statistics["blanked"], "person_cells_scrubbed": statistics["scrubbed"]}
    stats.update({f"person_{name}": count for name, count in statistics["written"].items()})
    return Protected(pseudonymize.output_headers(policy), stats)


def protect(table: str, headers: list[str], labels: list[str | None], rows_path: Path, keys: KeyReader | None, path: Path = POLICY_PATH) -> Protected | None:
    """Apply the table's policy to the rows at ``rows_path``; None when the table has no policy or the layout loads unchanged.

    Any error leaves the caller to remove the rows file, which still holds the published values.
    """
    document = read_policies(path)
    entry = document["tables"].get(table)
    if entry is None:
        return None
    refuse_blocked(table, path)
    for policy in entry["policies"]:
        if policy["headers"] == headers:
            if any(label is not None for label in labels):
                raise PersonFieldError(f"{table}: a label row cannot be protected")
            return transform_rows(policy, document["key_version"], keys if keys is not None else parameter_store_reader(), headers, rows_path)
    for layout in entry["unchanged"]:
        if layout["headers"] == headers:
            check_guard(layout["guard"], headers, rows_path, table)
            return None
    raise PersonFieldError(f"{table}: the header matches no reviewed layout of the person-field policy")


EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+")
PHONE = re.compile(r"(?<!\d)(?:\(\d{3}\)\s?|\d{3}[-.\s])\d{3}[-.\s]\d{4}(?!\d)")


def check_sheet_columns(rule: dict, header: list[str]) -> None:
    """Every personal-looking column is replaced or reviewed as not personal, and every column the rules name is present."""
    pattern = re.compile(rule["personal_pattern"])
    reviewed = {*rule["redact_columns"], *rule["not_personal_columns"]}
    if any(pattern.search(column.upper()) and column not in reviewed for column in header):
        raise PersonFieldError("unreviewed personal-looking column in a data sheet")
    owner = rule["individual_owner"]
    needed = {*rule["redact_columns"], *rule["name_columns"], *owner["redact_columns"], owner["column"], owner["name_column"]}
    if not needed <= set(header):
        raise PersonFieldError("a column named by the sheet person rules is missing from a data sheet")
    known = owner.get("known_types")
    if not isinstance(known, list) or not known or not all(isinstance(item, str) for item in known) or owner["equals"] not in known:
        raise PersonFieldError("the sheet person rules do not list the reviewed owner types")
    if len({owner_type_key(item) for item in known}) != len(known):
        raise PersonFieldError("two reviewed owner types read the same once their spelling is normalized")


def check_sheet_rule(table: str, rule: dict) -> None:
    """The reviewed layouts of a workbook table: unique sheet names holding the data sheets, and a complete header that passes the column review."""
    layouts = rule.get("layouts")
    if not isinstance(layouts, list) or not layouts:
        raise PersonFieldError(f"{table}: person policy entry invalid (no reviewed workbook layout)")
    for layout in layouts:
        sheets, header = layout.get("sheets"), layout.get("header")
        valid = (
            set(layout) == {"layout_id", "years", "sheets", "header"}
            and isinstance(sheets, list)
            and isinstance(header, list)
            and len(set(sheets)) == len(sheets)
            and len(set(header)) == len(header)
            and set(rule["data_sheets"]) <= set(sheets)
        )
        if not valid:
            raise PersonFieldError(f"{table}: person policy entry invalid (workbook layout)")
        try:
            check_sheet_columns(rule, header)
        except PersonFieldError as error:
            raise PersonFieldError(f"{table}: {error}") from None


def owner_type_key(value: str | None) -> str:
    """The owner type in one spelling: compatibility-normalized, case-folded, with every run of white space (a no-break space too) as one space."""
    return " ".join(unicodedata.normalize("NFKC", value or "").split()).casefold()


def is_individual_owner(owner: dict, row: dict[str, str | None]) -> bool:
    """True for a row whose owner cells are protected: an individual owner, or a row with no owner type.

    A blank, null or white space type cannot show that the owner is an organisation, so the row is read like the individual type: its owner name joins the
    scrub names and its owner name, city and ZIP are replaced (empty cells stay empty). A type that is neither blank nor in the reviewed set stops the load.
    """
    found = owner_type_key(row.get(owner["column"]))
    if not found or found == owner_type_key(owner["equals"]):
        return True
    if found not in {owner_type_key(item) for item in owner["known_types"]}:
        raise PersonFieldError("a data row holds an owner type outside the reviewed set")
    return False


def is_empty(value: str | None) -> bool:
    return value is None or value == ""


def reviewed_layout(rule: dict, records: list[dict]) -> dict:
    """The one reviewed layout whose sheet names and data sheet headers the workbook holds exactly; any other workbook stops the load.

    Cells beyond the reviewed header must be empty in the header row and in every data row. The label rows keep the reviewed exception.
    """
    found = list(dict.fromkeys(record["s"] for record in records))
    candidates = [layout for layout in rule["layouts"] if layout["sheets"] == found]
    if not candidates:
        raise PersonFieldError("the workbook sheets differ from every reviewed layout")
    top = {record["s"]: record["c"] for record in records if record["w"] == 1 and record["s"] in rule["data_sheets"]}
    layout = next((c for c in candidates if all(top.get(sheet, [])[: len(c["header"])] == c["header"] for sheet in rule["data_sheets"])), None)
    if layout is None:
        raise PersonFieldError("the data sheet header matches no reviewed layout")
    width, label_rows = len(layout["header"]), rule["label_rows"]
    for record in records:
        checked = record["s"] in rule["data_sheets"] and (record["w"] == 1 or record["w"] > 1 + label_rows)
        if checked and not all(is_empty(value) for value in record["c"][width:]):
            raise PersonFieldError("a data sheet holds a nonempty cell beyond the header of its reviewed layout")
    return dict(layout)


def name_forms(value: str | None) -> set[tuple[str, ...]]:
    """The scrub word sequences of one name cell: the whole name and, when it has a middle part, the first and last words. A null or blank cell is no name."""
    if value is None or not value.strip():
        return set()
    words = pseudonymize.scrub_name_words(value)
    if not words:
        return set()
    return {words, (words[0], words[-1])} if len(words) > 2 else {words}


def sheet_names(rule: dict, records: list[dict], headers: dict[str, list[str]]) -> set[tuple[str, ...]]:
    """Every administrator, preparer and individual owner name in the data rows, as scrub word sequences."""
    owner = rule["individual_owner"]
    names: set[tuple[str, ...]] = set()
    for record in records:
        header = headers.get(record["s"])
        if header is None or record["w"] <= 1 + rule["label_rows"]:
            continue
        row = dict(zip(header, record["c"], strict=False))
        sources = [row.get(column) for column in rule["name_columns"]]
        if is_individual_owner(owner, row):
            sources.append(row.get(owner["name_column"]))
        for value in sources:
            names.update(name_forms(value))
    try:
        pseudonymize.refuse_short_values(names, set())
    except PseudonymizationError as error:
        raise PersonFieldError(f"a person name is too short to match safely ({error})") from None
    return names


def protect_sheets(table: str, rows_path: Path, path: Path = POLICY_PATH) -> dict[str, int]:
    """Replace the person cells of a workbook's JSON Lines sheet rows in place and return the counts; {} when the table has no sheet policy.

    Only a workbook whose sheets and data sheet headers match a reviewed layout loads; any other sheet, header or cell beyond the header stops the load
    before the file is touched. A replaced cell keeps its position and type, so the cell arrays stay aligned. Cells are replaced, not pseudonymized, because
    the sheet row shape has no column for a pseudonym. Any error leaves the caller to remove the rows file.
    """
    entry = read_policies(path)["tables"].get(table)
    if entry is None:
        return {}
    refuse_blocked(table, path)
    rule = entry.get("sheet_cells")
    if rule is None:
        return {}
    replacement = rule["replacement"]
    records = [json.loads(line) for line in rows_path.read_text(encoding="ascii").splitlines()]
    layout = reviewed_layout(rule, records)
    headers = {sheet: list(layout["header"]) for sheet in rule["data_sheets"]}
    index = pseudonymize.index_names(sheet_names(rule, records, headers))
    owner = rule["individual_owner"]
    stats = {"person_cells_replaced": 0, "person_individual_owner_rows": 0}
    for record in records:
        header = headers.get(record["s"])
        data_row = header is not None and record["w"] > 1 + rule["label_rows"]
        row = dict(zip(header, record["c"], strict=False)) if header is not None else {}
        individual = data_row and is_individual_owner(owner, row)
        stats["person_individual_owner_rows"] += int(individual)
        targets = {*rule["redact_columns"], *(owner["redact_columns"] if individual else [])} if data_row and header is not None else set()
        cells = record["c"]
        for position, value in enumerate(cells):
            text = value or ""
            column = header[position] if header is not None and position < len(header) else ""
            if value == replacement or not text.strip():
                continue
            if (column in targets and data_row) or EMAIL.search(text) or PHONE.search(text) or (index and pseudonymize.contains_name(text, index)):
                cells[position] = replacement
                stats["person_cells_replaced"] += 1
    staging = rows_path.with_name(rows_path.name + ".protected")
    try:
        staging.write_text("".join(json.dumps(r, ensure_ascii=True) + "\n" for r in records), encoding="ascii")
        os.replace(staging, rows_path)
    finally:
        staging.unlink(missing_ok=True)
    return stats
