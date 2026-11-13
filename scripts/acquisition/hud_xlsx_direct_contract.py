"""Reader for HUD's direct static ZIP-COUNTY workbooks (https://www.huduser.gov/portal/datasets/usps/ZIP_COUNTY_<MMYYYY>.xlsx), a tested option.

Nothing in the plans, the queue, the collectors or capture mode uses this module (owner decision, Nov 13 2026: the 44 HUD workbooks stay manual units).
It reads the Excel export HUD serves as a static file (shared strings, uppercase header, exponent number text), which ``hud_xlsx_contract`` rejects
because that module reads only the web app's re-export of the manual downloads. Both layouts carry the same keys and ratio values: on the 2020 Q4 pair
the 54,194 keys are equal and the 216,776 ratio values differ by at most 6e-17. Codes and ratio text stay exactly as published; nothing is inverted,
renormalized or collapsed. ``python -m scripts.acquisition.run_hud_xlsx_direct_e2e --output <file>`` proves the reader on synthetic workbooks and, when
given ``--pair-direct`` and ``--pair-stored``, on the real pair.
"""

import csv
import io
import re
import zipfile

from scripts.acquisition import hud_api_contract as api
from scripts.acquisition import hud_xlsx_contract as manual
from scripts.acquisition.source_registry import require

HEADER = manual.HEADER
DIRECT_HEADER = ["ZIP", "COUNTY", "RES_RATIO", "BUS_RATIO", "OTH_RATIO", "TOT_RATIO"]
# The members the 2020 Q4 file has (profile read Nov 13 2026). A member outside both sets stops the quarter.
REQUIRED_MEMBERS = frozenset(
    {
        "[Content_Types].xml",
        "_rels/.rels",
        "xl/_rels/workbook.xml.rels",
        "xl/sharedStrings.xml",
        "xl/styles.xml",
        "xl/workbook.xml",
        "xl/worksheets/sheet1.xml",
    }
)
OPTIONAL_MEMBERS = frozenset(
    {"docProps/app.xml", "docProps/core.xml", "xl/printerSettings/printerSettings1.bin", "xl/theme/theme1.xml", "xl/worksheets/_rels/sheet1.xml.rels"}
)
MAX_FILE_BYTES = manual.MAX_FILE_BYTES
MAX_SHEET_BYTES = manual.MAX_SHEET_BYTES
MAX_STRINGS_BYTES = 8 * 1024**2
SHEET_NAME = re.compile(rb'<sheet name="([^"<>&]*)"')
STRINGS = re.compile(
    rb'<\?xml [^<>]*\?>\s*<sst [^<>]*? count="([0-9]+)" uniqueCount="([0-9]+)">(?P<items>(?:<si><t>[0-9A-Za-z_]*</t></si>)*)</sst>\s*',
)
STRING_ITEM = re.compile(rb"<si><t>([0-9A-Za-z_]*)</t></si>")
ROW = re.compile(rb'<row r="([0-9]+)"(?: [a-zA-Z0-9:]+="[^"<>]*")*>(.*?)</row>', re.S)
SHARED_CELL = re.compile(rb'<c r="([A-Z]+[0-9]+)"(?: s="[0-9]+")? t="s"><v>([0-9]+)</v></c>')
NUMBER_CELL = manual.NUMBER_CELL


def file_name(year: int, quarter: int) -> str:
    """HUD names each static file by the quarter's last month and year."""
    return f"ZIP_COUNTY_{manual.MONTHS[quarter]}{year}.xlsx"


def sheet_name(year: int, quarter: int) -> str:
    """The worksheet of each file carries the file's name without its extension."""
    return file_name(year, quarter).removesuffix(".xlsx")


def read_strings(body: bytes) -> list[str]:
    """The shared string table: plain text items only, with its count bound to its content."""
    found = STRINGS.fullmatch(body)
    if found is None:
        raise ValueError("HUD workbook shared strings differ")
    items = [item.decode("ascii") for item in STRING_ITEM.findall(found["items"])]
    require(int(found[2]) == len(items), "HUD workbook shared strings differ")
    return items


def row_cells(content: bytes, number: int, strings: list[str]) -> list[str]:
    """Match a row's cells exactly; header and codes must be shared strings, ratios plain numbers."""
    values, position = [], 0
    for column in range(len(HEADER)):
        require(position < len(content), "HUD workbook row shape differs")
        text = number == 1 or column < 2
        match = manual.matched(SHARED_CELL if text else NUMBER_CELL, content, position, "HUD workbook cell type differs")
        require(match[1] == f"{'ABCDEF'[column]}{number}".encode(), "HUD workbook row shape differs")
        if text:
            index = int(match[2])
            require(index < len(strings), "HUD workbook shared strings differ")
            values.append(strings[index])
        else:
            values.append(match[2].decode("ascii"))
        position = match.end()
    require(position == len(content), "HUD workbook row shape differs")
    return values


def workbook_rows(raw: bytes, expected_sheet: str) -> list[list[str]]:
    """Read the single worksheet's rows as published text, after package, name and size checks.

    The worksheet is matched against the publisher's exact machine-generated layout rather than parsed as general XML: every byte of the sheet data
    must belong to a recognized row and cell, so any other construct (formulas, inline strings, entities, extra columns) stops the quarter.
    """
    require(len(raw) <= MAX_FILE_BYTES, "HUD workbook exceeds its size bound")
    try:
        archive = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile:
        raise ValueError("HUD download is not a workbook package") from None
    with archive:
        names = [info.filename for info in archive.infolist()]
        require(len(names) == len(set(names)) and REQUIRED_MEMBERS <= set(names) <= REQUIRED_MEMBERS | OPTIONAL_MEMBERS, "HUD workbook package members differ")
        require(archive.testzip() is None, "HUD workbook member CRC differs")
        book = manual.xml_bytes(archive, "xl/workbook.xml", 1024**2)
        require(book.count(b"<sheet ") == 1, "HUD workbook must contain exactly one worksheet")
        named = SHEET_NAME.search(book)
        require(named is not None and named[1].decode("ascii", "replace") == expected_sheet, "HUD workbook sheet name differs")
        strings = read_strings(manual.xml_bytes(archive, "xl/sharedStrings.xml", MAX_STRINGS_BYTES))
        body = manual.xml_bytes(archive, "xl/worksheets/sheet1.xml", MAX_SHEET_BYTES)
    sheet = manual.SHEET.fullmatch(body)
    if sheet is None or b"<row" in sheet["outside"] + sheet["after"]:
        raise ValueError("HUD workbook row shape differs")
    data, position = sheet["data"], 0
    rows: list[list[str]] = []
    while position < len(data):
        match = manual.matched(ROW, data, position, "HUD workbook row shape differs")
        require(match[1] == str(len(rows) + 1).encode(), "HUD workbook row shape differs")
        rows.append(row_cells(match[2], len(rows) + 1, strings))
        position = match.end()
    return rows


def validate_workbook(raw: bytes, batch: dict, plan: dict) -> tuple[bytes, dict]:
    """Write a lossless CSV in the API captures' layout while refusing schema, identity or coverage defects.

    BATCH names the quarter (``year``, ``quarter``) and PLAN the required states (``required_state_fips``). No exact repeated row is accepted.
    """
    table = workbook_rows(raw, sheet_name(batch["year"], batch["quarter"]))
    require(bool(table) and table[0] == DIRECT_HEADER, "HUD workbook header differs")
    require(len(table) > 1, "HUD workbook has no data rows")
    seen: set[tuple[str, str]] = set()
    rows = []
    for values in table[1:]:
        row = dict(zip(HEADER, values, strict=True))
        require(
            re.fullmatch(r"[0-9]{5}", row["zip"]) is not None and re.fullmatch(r"[0-9]{5}", row["geoid"]) is not None, "HUD ZIP or county identifier invalid"
        )
        parsed = {name: manual.ratio(row[name]) for name in api.RATIOS}
        key = (row["zip"], row["geoid"])
        require(key not in seen, "Duplicate ZIP-county row in HUD workbook")
        seen.add(key)
        rows.append((row, parsed))
    states = {g[:2] for _, g in seen}
    require(set(plan["required_state_fips"]) <= states, "HUD state coverage incomplete")
    fields = sorted(HEADER)
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(fields)
    writer.writerows([r[k] for k in fields] for r, _ in sorted(rows, key=lambda item: (item[0]["zip"], item[0]["geoid"])))
    statistics = {
        "rows": len(rows),
        "zips": len({z for z, _ in seen}),
        "counties": len({g for _, g in seen}),
        "states_and_territories": sorted(states),
        "ratio_sums": api.ratio_profile([{"zip": r["zip"], **p} for r, p in rows]),
        "direction": "zip_to_county",
        "model_eligible": False,
    }
    return output.getvalue().encode(), statistics
