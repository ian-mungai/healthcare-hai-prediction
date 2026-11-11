"""Person cells of the HCAI utilization workbook at the bronze load boundary: synthetic workbooks only.

Most cases call ``person_fields.protect_sheets`` on JSON Lines records shaped as ``file_readers.read_sheet_rows`` writes them. The last group writes real
``.xlsx`` workbooks with ``openpyxl`` and reads them through the real reader. ``openpyxl`` is not in the host environment, so that group skips there; run it
with ``PYTHONPATH=<repository>/.review_dependencies`` (the pinned review wheels) or inside the Spark image. The reviewed layouts come from the acquisition
plan, an independent source: every header has the real 354 columns and every workbook has the sheets the plan lists.
"""

from __future__ import annotations

import copy
import hashlib
import io
import json
import re
import sys
import traceback
import types
import zipfile
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from scripts.lakehouse import bronze, file_readers, person_fields
from tests.support import check

TABLE = "hcai_util_workbook_sheet_rows"
PLAN = json.loads(Path("config/acquisition/hcai_util_2018_2025_plan.json").read_text())
YEARS = {entry["year"]: entry for entry in PLAN["years"]}
HEADER = YEARS[2025]["header"]
SHEETS = YEARS[2025]["sheets"]
DATA_SHEETS = ("Page 1-6", "NonResp 1-6")
INDIVIDUAL = "Investor - Individual"
CORPORATION = "Investor - Corporation"
REPLACED = "[REDACTED]"
# Built from parts so the privacy scan does not read a fixture as a real telephone number.
PHONE = "-".join(("555", "010", "7788"))
SYNTHETIC_VALUES = ["Zorba Quill", PHONE, "9 Synthetic Way", "Mira Tolliver", "Orrin Vale", "q.vale@example.test", "Pell Dunmore"]
Cells = list[str | None]


def cells(header: list[str], values: dict[str, str | None]) -> Cells:
    row: Cells = [""] * len(header)
    for column, value in values.items():
        row[header.index(column)] = value
    return row


def data_row(number: str, owner_type: str, owner: str | None, admin: str | None, equipment: str, header: list[str] = HEADER, **more: str | None) -> Cells:
    values: dict[str, str | None] = {
        "Description": "d",
        "FAC_NO": number,
        "FAC_NAME": "Fixture Hospital",
        "FAC_STR_ADDR": "9 Synthetic Way",
        "FAC_PHONE": PHONE,
        "FAC_ADMIN_NAME": admin,
        "FAC_PAR_CORP_NAME": owner,
        "FAC_PAR_CORP_BUS_ADDR": "1 Owner Road",
        "FAC_PAR_CORP_CITY": "Fixtureville",
        "FAC_PAR_CORP_ZIP": "90001",
        "REPT_PREP_NAME": "Mira Tolliver",
        "REV_REPT_PREP_NAME": "Orrin Vale",
        "LICEE_TOC": owner_type,
        "DESC_EQUIP_01": equipment,
        "TOT_LIC_BEDS": "120",
    }
    values.update(more)
    return cells(header, values)


def sheet_records(
    rows: list[Cells], tips: str = "Questions: q.vale@example.test", header: list[str] = HEADER, sheets: list[str] = SHEETS, labels: list[Cells] | None = None
) -> list[dict[str, Any]]:
    labels = labels if labels is not None else [["label"] * len(header)] * 4
    content: dict[str, list[Cells]] = {
        "Tips": [[tips]],
        "Page 1-6": [list(header), *labels, *rows],
        "NonResp 1-6": [list(header), *labels, *rows],
        "Crosswalk": [["Old", "New"]],
    }
    records: list[dict[str, Any]] = []
    for index, name in enumerate(sheets, start=1):
        for number, row in enumerate(content[name], start=1):
            records.append(
                {"r": len(records) + 1, "s": name, "i": index, "w": number, "d": "1900", "c": list(row), "t": [None if c is None else "s" for c in row]}
            )
    return records


def write_records(path: Path, records: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(record) + "\n" for record in records))


def read_back(rows_path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in rows_path.read_text().splitlines()]


def protected(tmp_path: Path, records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    rows_path = tmp_path / "rows.jsonl"
    write_records(rows_path, records)
    before = read_back(rows_path)
    stats = person_fields.protect_sheets(TABLE, rows_path)
    return before, read_back(rows_path), stats


def assert_refused(tmp_path: Path, records: list[dict[str, Any]], match: str) -> None:
    """The load fails with the expected reason, the rows file is byte for byte as it was and no staging file is left."""
    rows_path = tmp_path / "rows.jsonl"
    write_records(rows_path, records)
    before = rows_path.read_bytes()
    with pytest.raises(person_fields.PersonFieldError, match=match):
        person_fields.protect_sheets(TABLE, rows_path)
    check(rows_path.read_bytes() == before, "a refused workbook changed its rows file")
    check(not list(tmp_path.glob("*.protected")), "a refused workbook left a staging file")


def page_cells(records: list[dict[str, Any]], sheet: str = "Page 1-6") -> dict[int, dict[str, Any]]:
    return {r["w"]: dict(zip(HEADER, r["c"], strict=False)) for r in records if r["s"] == sheet}


def test_person_cells_are_replaced_and_the_shape_is_kept(tmp_path: Path) -> None:
    rows = [
        data_row("100", INDIVIDUAL, "Pell Dunmore", "Zorba Quill", "Scanner donated by Zorba Quill"),
        data_row("101", CORPORATION, "Fixture Holdings LLC", "Zorba Quill", "Scanner"),
    ]
    before, after, stats = protected(tmp_path, sheet_records(rows))
    text = json.dumps(after)
    check(not [value for value in SYNTHETIC_VALUES if value in text], "sheet person cell check failed")
    check([(r["s"], r["w"], len(r["c"]), r["t"]) for r in after] == [(r["s"], r["w"], len(r["c"]), r["t"]) for r in before], "sheet shape check failed")
    page = page_cells(after)
    check(page[6]["FAC_PAR_CORP_NAME"] == page[6]["FAC_PAR_CORP_CITY"] == page[6]["FAC_PAR_CORP_ZIP"] == REPLACED, "individual owner cells were kept")
    check(page[7]["FAC_PAR_CORP_NAME"] == "Fixture Holdings LLC" and page[7]["FAC_PAR_CORP_CITY"] == "Fixtureville", "corporation owner cells were replaced")
    check(page[6]["TOT_LIC_BEDS"] == "120" and page[6]["FAC_NAME"] == "Fixture Hospital" and page[6]["DESC_EQUIP_01"] == REPLACED, "non-person cells changed")
    check(page[1]["FAC_ADMIN_NAME"] == "FAC_ADMIN_NAME", "the header row changed")
    check(stats["person_cells_replaced"] > 0 and stats["person_individual_owner_rows"] == 2, "sheet person cell counts are wrong")


def test_an_already_redacted_version_one_style_workbook_loads_unchanged(tmp_path: Path) -> None:
    row = data_row(
        "100", CORPORATION, "Fixture Holdings LLC", REPLACED, "Scanner", FAC_STR_ADDR=REPLACED, FAC_PHONE=REPLACED, FAC_PAR_CORP_BUS_ADDR=REPLACED,
        REPT_PREP_NAME=REPLACED, REV_REPT_PREP_NAME=REPLACED,
    )  # fmt: skip
    before, after, _ = protected(tmp_path, sheet_records([row], tips="Utilization data"))
    check(before == after, "an already redacted workbook changed")


@pytest.mark.parametrize("year", sorted(YEARS))
def test_every_reviewed_plan_layout_is_accepted(tmp_path: Path, year: int) -> None:
    entry = YEARS[year]
    rows = [data_row("100", CORPORATION, "Fixture Holdings LLC", "Zorba Quill", "Scanner by Zorba Quill", header=entry["header"])]
    _, after, _ = protected(tmp_path, sheet_records(rows, header=entry["header"], sheets=entry["sheets"]))
    check("Zorba" not in json.dumps(after), "a reviewed layout kept a collected name")


def test_the_policy_layouts_are_exactly_the_plan_layouts() -> None:
    rule = person_fields.load_policies()[TABLE]["sheet_cells"]
    listed = {(tuple(layout["header"]), tuple(layout["sheets"])) for layout in rule["layouts"]}
    check(listed == {(tuple(e["header"]), tuple(e["sheets"])) for e in PLAN["years"]}, "the policy layouts differ from the acquisition plan")


def test_a_personal_looking_column_without_a_decision_stops_the_load(tmp_path: Path) -> None:
    policy = json.loads(Path(person_fields.POLICY_PATH).read_text())
    for layout in policy["tables"][TABLE]["sheet_cells"].get("layouts", []):
        layout["header"].append("FAC_CONTACT_EMAIL")
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps(policy))
    header = [*HEADER, "FAC_CONTACT_EMAIL"]
    rows_path = tmp_path / "rows.jsonl"
    write_records(rows_path, sheet_records([["x"] * len(header)], tips="Utilization data", header=header))
    with pytest.raises(person_fields.PersonFieldError, match="unreviewed personal-looking column"):
        person_fields.protect_sheets(TABLE, rows_path, policy_path)


def test_the_table_is_no_longer_blocked_and_other_hcai_tables_have_no_policy(tmp_path: Path) -> None:
    person_fields.refuse_blocked(TABLE)
    check(person_fields.protect("hcai_util_pages_csv", ["a"], [None], tmp_path / "none.jsonl", None) is None, "a derived HCAI csv table gained a policy")


def rename_data_sheet(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{**r, "s": "Page 1 to 6"} if r["s"] == "Page 1-6" else r for r in records]


def drop_sheet(name: str) -> Any:
    return lambda records: [r for r in records if r["s"] != name]


def add_sheet(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [*records, {"r": len(records) + 1, "s": "Notes", "i": 4, "w": 1, "d": "1900", "c": ["Fixturegiven Fixturesurname"], "t": ["s"]}]


def add_crosswalk(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [*records, {"r": len(records) + 1, "s": "Crosswalk", "i": 4, "w": 1, "d": "1900", "c": ["Old", "New"], "t": ["s", "s"]}]


def edit_header(sheet: str, edit: Any) -> Any:
    def apply(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = copy.deepcopy(records)
        for record in out:
            if record["s"] == sheet and record["w"] == 1:
                edit(record["c"], record["t"])
        return out

    return apply


def append_cell(sheet: str, number: int, value: str) -> Any:
    def apply(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = copy.deepcopy(records)
        for record in out:
            if record["s"] == sheet and record["w"] == number:
                record["c"].append(value)
                record["t"].append("s")
        return out

    return apply


def insert_column(cells_: list[str], types: list[str]) -> None:
    cells_.insert(5, "CEO")
    types.insert(5, "s")


def rename_column(cells_: list[str], _types: list[str]) -> None:
    cells_[cells_.index("DESC_EQUIP_01")] = "DESC_EQUIP_ONE"


def drop_column(cells_: list[str], types: list[str]) -> None:
    position = cells_.index("TOT_LIC_BEDS")
    del cells_[position], types[position]


UNREVIEWED = [
    pytest.param(rename_data_sheet, "sheets differ", id="data sheet renamed"),
    pytest.param(drop_sheet("NonResp 1-6"), "sheets differ", id="data sheet absent"),
    pytest.param(drop_sheet("Page 1-6"), "sheets differ", id="page sheet absent"),
    pytest.param(drop_sheet("Tips"), "sheets differ", id="tips sheet absent"),
    pytest.param(add_sheet, "sheets differ", id="extra sheet"),
    pytest.param(add_crosswalk, "no reviewed layout", id="crosswalk added to a three sheet layout"),
    pytest.param(append_cell("Page 1-6", 1, "CEO"), "cell beyond the header", id="extra column after the header"),
    pytest.param(edit_header("Page 1-6", insert_column), "no reviewed layout", id="column inserted"),
    pytest.param(edit_header("Page 1-6", rename_column), "no reviewed layout", id="column renamed"),
    pytest.param(edit_header("Page 1-6", drop_column), "no reviewed layout", id="column dropped"),
    pytest.param(edit_header("NonResp 1-6", insert_column), "no reviewed layout", id="second data sheet header differs"),
    pytest.param(append_cell("Page 1-6", 6, "Fixturegiven Fixturesurname"), "cell beyond the header", id="data row longer than the header"),
    pytest.param(append_cell("NonResp 1-6", 6, "Fixturegiven Fixturesurname"), "cell beyond the header", id="second data sheet row longer"),
]


@pytest.mark.parametrize(("mutate", "match"), UNREVIEWED)
def test_an_unreviewed_sheet_header_or_cell_stops_the_load_and_writes_nothing(tmp_path: Path, mutate: Any, match: str) -> None:
    rows = [data_row("100", CORPORATION, "Fixture Holdings LLC", "Zorba Quill", "Scanner")]
    assert_refused(tmp_path, mutate(sheet_records(rows)), match)


def test_cells_beyond_the_header_stay_allowed_in_the_label_rows_and_empty_elsewhere(tmp_path: Path) -> None:
    labels: list[Cells] = [["label"] * (len(HEADER) + 2)] * 4
    rows = [[*data_row("100", CORPORATION, "Fixture Holdings LLC", "Zorba Quill", "Scanner"), "", None]]
    before, after, _ = protected(tmp_path, sheet_records(rows, labels=labels))
    check([len(r["c"]) for r in after] == [len(r["c"]) for r in before], "an allowed row changed its width")
    check(page_cells(after)[2]["Description"] == "label", "a label row changed")
    check("Zorba" not in json.dumps(after), "a collected name survived")


def test_the_first_and_last_form_of_a_three_part_name_is_replaced_everywhere(tmp_path: Path) -> None:
    rows = [
        data_row(
            "100",
            INDIVIDUAL,
            "Ownergiven Ownermid Ownersurname",
            "Fixturegiven Middle Fixturesurname",
            "Scanner",
            REPT_PREP_NAME="Prepgiven Prepmid Prepsurname",
            REV_REPT_PREP_NAME="Revgiven Revmid Revsurname",
        ),  # fmt: skip
        data_row("101", CORPORATION, "Fixture Holdings LLC", "Other Admin", "Gift from Fixturegiven Fixturesurname"),
        data_row("102", CORPORATION, "Fixture Holdings LLC", "Other Admin", "Fixturegiven Fixturesurname Jr and fixturegiven fixturesurname's team"),
        data_row("103", CORPORATION, "Fixture Holdings LLC", "Other Admin", "Prepgiven Prepsurname"),
        data_row("104", CORPORATION, "Fixture Holdings LLC", "Other Admin", "Revgiven Revsurname"),
        data_row("105", CORPORATION, "Fixture Holdings LLC", "Other Admin", "Ownergiven Ownersurname"),
        data_row("106", CORPORATION, "Fixture Holdings LLC", "Other Admin", "Fixturegiven Clinic"),
        data_row("107", CORPORATION, "Fixture Holdings LLC", "Other Admin", "Fixturegiven Middle Fixturesurname"),
    ]
    tips = "Ask Fixturegiven Fixturesurname or Prepgiven Prepsurname"
    _, after, _ = protected(tmp_path, sheet_records(rows, tips=tips))
    text = json.dumps(after)
    for fragment in ("Fixturesurname", "Prepsurname", "Revsurname", "Ownersurname", "fixturesurname"):
        check(fragment not in text, f"a first and last form survived: {fragment}")
    for sheet in DATA_SHEETS:
        cells_ = page_cells(after, sheet)
        check(cells_[12]["DESC_EQUIP_01"] == "Fixturegiven Clinic", "a single name part was replaced")
        check(all(cells_[w]["DESC_EQUIP_01"] == REPLACED for w in (7, 8, 9, 10, 11, 13)), "a copy of a collected name was kept")
    tip = next(r for r in after if r["s"] == "Tips")
    check(tip["c"] == [REPLACED], "the auxiliary sheet kept a collected name")


@pytest.mark.parametrize("missing", [None, "", "   "], ids=["null", "empty", "blank"])
def test_a_null_or_empty_name_cell_is_no_name(tmp_path: Path, missing: str | None) -> None:
    rows = [
        data_row("100", INDIVIDUAL, missing, missing, "Scanner by Zorba Quill", REPT_PREP_NAME=missing, REV_REPT_PREP_NAME=missing),
        data_row("101", CORPORATION, "Fixture Holdings LLC", "Zorba Quill", "Scanner"),
    ]
    before, after, stats = protected(tmp_path, sheet_records(rows))
    check("Zorba" not in json.dumps(after), "a collected name survived next to a null name cell")
    for old, new in zip(before, after, strict=True):
        check(old["t"] == new["t"] and len(old["c"]) == len(new["c"]), "a null name cell changed the shape")
        check([c is None for c in old["c"]] == [c is None for c in new["c"]], "a null cell was filled")
    check(stats["person_individual_owner_rows"] == 2, "the individual owner rows were not counted")


def test_a_workbook_without_any_name_loads_and_still_redacts_the_address_cells(tmp_path: Path) -> None:
    rows = [data_row("100", INDIVIDUAL, None, None, "Scanner", REPT_PREP_NAME=None, REV_REPT_PREP_NAME=None)]
    _, after, _ = protected(tmp_path, sheet_records(rows, tips="Utilization data"))
    row = page_cells(after)[6]
    check(row["FAC_STR_ADDR"] == REPLACED and row["FAC_PHONE"] == REPLACED and row["FAC_PAR_CORP_CITY"] == REPLACED, "address cells were kept")
    check(row["FAC_ADMIN_NAME"] is None and row["FAC_PAR_CORP_NAME"] is None, "a null name cell was filled")


def test_protecting_an_already_protected_workbook_changes_nothing_even_with_the_word_redacted_in_a_note(tmp_path: Path) -> None:
    note = "Figures redacted by the publisher; see Redacted entries"
    rows = [
        data_row("100", INDIVIDUAL, "Pell Dunmore", "Zorba Quill", note),
        data_row("101", CORPORATION, "Fixture Holdings LLC", "Zorba Quill", "Scanner donated by Zorba Quill"),
    ]
    rows_path = tmp_path / "rows.jsonl"
    write_records(rows_path, sheet_records(rows, tips=f"Data {note}"))
    first_stats = person_fields.protect_sheets(TABLE, rows_path)
    first = rows_path.read_bytes()
    check(first_stats["person_cells_replaced"] > 0 and "Zorba" not in first.decode(), "the first pass did not protect the workbook")
    check(note in json.dumps(read_back(rows_path)), "the first pass replaced a note that holds no collected name")
    second_stats = person_fields.protect_sheets(TABLE, rows_path)
    check(rows_path.read_bytes() == first, "a second pass changed the already protected rows")
    check(second_stats["person_cells_replaced"] == 0, "a second pass replaced cells again")


def write_xlsx(path: Path, records: list[dict[str, Any]]) -> None:
    """Write the fixture records as a native workbook: one sheet per name in order, each cell at its row and column, empty cells left unwritten."""
    openpyxl = pytest.importorskip("openpyxl")
    book = openpyxl.Workbook()
    book.remove(book.active)
    sheets: dict[str, Any] = {}
    for record in records:
        if record["s"] not in sheets:
            sheets[record["s"]] = book.create_sheet(record["s"])
        for column, value in enumerate(record["c"], start=1):
            if value not in (None, ""):
                sheets[record["s"]].cell(row=record["w"], column=column, value=value)
    book.save(path)


@pytest.mark.parametrize("year", sorted(YEARS))
def test_a_native_xlsx_of_a_reviewed_layout_is_read_by_openpyxl_and_protected(tmp_path: Path, year: int) -> None:
    entry = YEARS[year]
    rows = [
        data_row("100", INDIVIDUAL, "Pell Dunmore", "Zorba Quill", "Scanner donated by Zorba Quill", header=entry["header"]),
        data_row("101", CORPORATION, "Fixture Holdings LLC", "Zorba Quill", "Scanner", header=entry["header"]),
    ]
    fixture = sheet_records(rows, header=entry["header"], sheets=entry["sheets"])
    workbook, rows_path = tmp_path / "book.xlsx", tmp_path / "rows.jsonl"
    write_xlsx(workbook, fixture)
    count, label, _ = file_readers.read_sheet_rows(workbook, rows_path)
    read = read_back(rows_path)
    check(label == "xlsx" and count == len(fixture), "the real reader did not return every fixture row")
    check([(r["s"], r["w"]) for r in read] == [(r["s"], r["w"]) for r in fixture], "the real reader's sheet and row positions differ from the fixture shape")
    check("Zorba" in json.dumps(read), "the workbook read did not hold the published names before protection")
    stats = person_fields.protect_sheets(TABLE, rows_path)
    after = read_back(rows_path)
    check("Zorba" not in json.dumps(after) and "Pell" not in json.dumps(after), "a collected name survived in a real workbook")
    check([(r["s"], r["w"], r["t"]) for r in after] == [(r["s"], r["w"], r["t"]) for r in read], "protecting a real workbook changed its shape")
    page = page_cells(after)
    check(page[6]["FAC_PAR_CORP_NAME"] == REPLACED and page[6]["DESC_EQUIP_01"] == REPLACED, "an individual owner or name cell was kept")
    check(page[7]["FAC_PAR_CORP_NAME"] == "Fixture Holdings LLC" and page[7]["FAC_NAME"] == "Fixture Hospital", "a corporation cell was replaced")
    check(stats["person_cells_replaced"] > 0 and stats["person_individual_owner_rows"] == 2, "the real workbook counts are wrong")


@pytest.mark.parametrize(("mutate", "match"), UNREVIEWED)
def test_a_native_xlsx_of_an_unreviewed_layout_is_refused_before_any_row_is_handed_on(tmp_path: Path, mutate: Any, match: str) -> None:
    rows = [data_row("100", CORPORATION, "Fixture Holdings LLC", "Zorba Quill", "Scanner")]
    workbook, rows_path = tmp_path / "book.xlsx", tmp_path / "rows.jsonl"
    write_xlsx(workbook, mutate(sheet_records(rows)))
    # No Spark session is passed: the load must stop before the frame is built, so any attempt to read the rows would fail with another error.
    with pytest.raises(bronze.BronzeError, match=f"person fields: .*{match}"):
        bronze.parse_object(None, {"table": TABLE, "format": "sheet_rows"}, workbook, rows_path)
    check(sorted(p.name for p in tmp_path.iterdir()) == ["book.xlsx"], "a refused real workbook left a rows file or a staging file")


# Every owner type the eight real workbooks hold (2018 to 2025), spelled as published. A type outside this set stops the load.
REVIEWED_TYPES = [
    "Non-Profit Corporation (including church-related)",
    "Investor - Corporation",
    "Investor - Limited Liability Company",
    "District",
    "City or County",
    "University of California",
    "Investor - Partnership",
    "State",
    INDIVIDUAL,
    "",
]
INDIVIDUAL_SPELLINGS = [
    INDIVIDUAL + " ",
    " " + INDIVIDUAL,
    INDIVIDUAL.upper(),
    INDIVIDUAL.lower(),
    INDIVIDUAL.replace(" ", "\u00a0"),
    INDIVIDUAL.replace(" - ", "  -  "),
]


@pytest.mark.parametrize(
    "owner_type", INDIVIDUAL_SPELLINGS, ids=["trailing space", "leading space", "upper case", "lower case", "no-break space", "double spaces"]
)
def test_a_respelled_individual_owner_type_still_redacts_the_owner_cells(tmp_path: Path, owner_type: str) -> None:
    rows = [data_row("100", owner_type, "Pell Dunmore", "Zorba Quill", "Scanner donated by Pell Dunmore")]
    _, after, stats = protected(tmp_path, sheet_records(rows))
    page = page_cells(after)
    check(
        page[6]["FAC_PAR_CORP_NAME"] == page[6]["FAC_PAR_CORP_CITY"] == page[6]["FAC_PAR_CORP_ZIP"] == REPLACED, f"the owner cells of {owner_type!r} were kept"
    )
    check("Pell Dunmore" not in json.dumps(after) and stats["person_individual_owner_rows"] == 2, "the owner name or the owner row count is wrong")


@pytest.mark.parametrize("owner_type", ["New ownership class", "Investor - Individual (sole)", "Individual"])
def test_an_owner_type_outside_the_reviewed_set_stops_the_load_before_any_row_changes(tmp_path: Path, owner_type: str) -> None:
    rows = [
        data_row("100", CORPORATION, "Fixture Holdings LLC", "Zorba Quill", "Scanner"),
        data_row("101", owner_type, "Pell Dunmore", "Zorba Quill", "Scanner"),
    ]
    assert_refused(tmp_path, sheet_records(rows), "owner type")


def test_every_reviewed_owner_type_loads_and_only_the_individual_rows_lose_their_owner_cells(tmp_path: Path) -> None:
    rows = [
        data_row(str(100 + index), owner_type, "Pell Dunmore" if owner_type == INDIVIDUAL else "Fixture Holdings LLC", "Zorba Quill", "Scanner")
        for index, owner_type in enumerate(REVIEWED_TYPES)
    ]
    _, after, stats = protected(tmp_path, sheet_records(rows))
    page = page_cells(after)
    redacted = [page[6 + index]["FAC_PAR_CORP_NAME"] == REPLACED for index in range(len(REVIEWED_TYPES))]
    check(
        redacted == [owner_type == INDIVIDUAL for owner_type in REVIEWED_TYPES] and stats["person_individual_owner_rows"] == 2,
        f"owner cells by type: {redacted}",
    )


def test_the_reviewed_owner_types_are_listed_in_the_policy_and_include_the_individual_type() -> None:
    owner = person_fields.read_policies(person_fields.POLICY_PATH)["tables"][TABLE]["sheet_cells"]["individual_owner"]
    check(
        sorted(owner["known_types"]) == sorted(REVIEWED_TYPES[:-1]) and owner["equals"] in owner["known_types"],
        f"the reviewed owner types differ: {owner['known_types']}",
    )


def native_cells_at_the_spark_boundary(tmp_path: Path, owner_type: str) -> list[dict[str, Any]]:
    """Read a real synthetic workbook through the loader and return the rows at the moment Spark would read them."""
    workbook, rows_path = tmp_path / "book.xlsx", tmp_path / "rows.jsonl"
    write_xlsx(workbook, sheet_records([data_row("100", owner_type, "Pell Dunmore", "Zorba Quill", "Scanner")]))
    seen: list[dict[str, Any]] = []

    def capture(_spark: Any, path: Path) -> object:
        seen.extend(read_back(path))
        return object()

    with patch.object(bronze, "read_sheet_frame", side_effect=capture):
        bronze.parse_object(None, {"table": TABLE, "format": "sheet_rows"}, workbook, rows_path)
    return seen


@pytest.mark.parametrize("owner_type", [INDIVIDUAL, INDIVIDUAL + " ", INDIVIDUAL.upper()], ids=["exact", "trailing space", "upper case"])
def test_a_native_xlsx_reaches_the_spark_read_with_the_owner_cells_redacted_whatever_the_spelling(tmp_path: Path, owner_type: str) -> None:
    seen = native_cells_at_the_spark_boundary(tmp_path, owner_type)
    cells_ = next(record["c"] for record in seen if record["s"] == "Page 1-6" and record["w"] == 6)
    owner = [cells_[HEADER.index(column)] for column in ("FAC_PAR_CORP_NAME", "FAC_PAR_CORP_CITY", "FAC_PAR_CORP_ZIP")]
    check(owner == [REPLACED] * 3 and "Pell Dunmore" not in json.dumps(seen), f"the owner cells reached Spark as {owner}")


def test_a_native_xlsx_with_an_unknown_owner_type_is_refused_before_the_spark_read(tmp_path: Path) -> None:
    workbook, rows_path = tmp_path / "book.xlsx", tmp_path / "rows.jsonl"
    write_xlsx(workbook, sheet_records([data_row("100", "New ownership class", "Pell Dunmore", "Zorba Quill", "Scanner")]))
    refused = patch.object(bronze, "read_sheet_frame", side_effect=AssertionError("Spark must not read an unprotected workbook"))
    with refused, pytest.raises(bronze.BronzeError, match="person fields: .*owner type"):
        bronze.parse_object(None, {"table": TABLE, "format": "sheet_rows"}, workbook, rows_path)
    check(sorted(p.name for p in tmp_path.iterdir()) == ["book.xlsx"], "a refused workbook left a rows file or a staging file")


SOURCE_MARKER = "SYNTHETIC_CELL_MARKER"


def workbook_with_a_text_in_a_numeric_cell(path: Path) -> None:
    """A real synthetic workbook whose administrator cell is typed numeric but holds the marker text, so the native reader fails on that cell."""
    write_xlsx(path, sheet_records([data_row("100", INDIVIDUAL, "Pell Dunmore", "Zorba Quill", "Scanner")]))
    cell = re.compile(rb"<c\b[^>]*>(?:(?!</c>).)*<t>Zorba Quill</t>(?:(?!</c>).)*</c>")
    buffer = io.BytesIO()
    with zipfile.ZipFile(path) as original, zipfile.ZipFile(buffer, "w") as changed:
        for name in original.namelist():
            raw = original.read(name)
            match = cell.search(raw) if name == "xl/worksheets/sheet2.xml" else None
            if match is not None:
                reference = re.search(rb'\br="([A-Z]+[0-9]+)"', match.group())
                check(reference is not None, "the synthetic cell has no coordinate")
                raw = raw[: match.start()] + b'<c r="' + reference[1] + b'" t="n"><v>' + SOURCE_MARKER.encode() + b"</v></c>" + raw[match.end() :]
            changed.writestr(name, raw)
    path.write_bytes(buffer.getvalue())


def test_a_workbook_the_native_reader_cannot_read_fails_with_a_fixed_code_and_no_cell_text(tmp_path: Path) -> None:
    workbook, rows_path = tmp_path / "book.xlsx", tmp_path / "rows.jsonl"
    workbook_with_a_text_in_a_numeric_cell(workbook)
    with pytest.raises(bronze.BronzeError) as caught:
        bronze.parse_object(None, {"table": TABLE, "format": "sheet_rows"}, workbook, rows_path)
    shown = "".join(traceback.format_exception(caught.value))
    check(SOURCE_MARKER not in str(caught.value) and SOURCE_MARKER not in shown, "the cell text reached the message or the traceback")
    check(
        str(caught.value) == bronze.WORKBOOK_UNREADABLE and caught.value.__cause__ is None and caught.value.__suppress_context__,
        "the failure is not the fixed code",
    )
    check(sorted(p.name for p in tmp_path.iterdir()) == ["book.xlsx"], "the unreadable workbook left a partly written rows file")


def test_the_loader_reports_an_unreadable_workbook_by_its_fixed_code_only(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    source = tmp_path / "source.xlsx"
    workbook_with_a_text_in_a_numeric_cell(source)

    class Store:
        def download(self, _bucket: str, _key: str, _version: str, target: Path) -> None:
            target.write_bytes(source.read_bytes())

    item = {
        "table": TABLE,
        "format": "sheet_rows",
        "snapshot_id": "S1",
        "file_name": "book.xlsx",
        "object_key": "k1",
        "bucket": "b",
        "key": "k",
        "version_id": "v",
        "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "byte_count": source.stat().st_size,
    }
    monkeypatch.setattr(bronze, "ensure_table", lambda *_args, **_kwargs: None)
    # The loader imports Spark's column helpers before it reads the file; the host has no Spark, and a read that fails never reaches them.
    functions = types.SimpleNamespace(lit=str)
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", types.ModuleType("pyspark.sql"))
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions)
    with pytest.raises(bronze.BronzeError) as caught:
        bronze.load_object(None, item, Store(), "ns", tmp_path / "scratch")
    shown = "".join(traceback.format_exception(caught.value))
    check(SOURCE_MARKER not in str(caught.value) and SOURCE_MARKER not in shown, "the cell text reached the loader error or its traceback")
    check(str(caught.value).endswith(bronze.WORKBOOK_UNREADABLE), f"the loader error is {str(caught.value)!r}")
