"""Repeatable synthetic check of the HUD direct-file workbook reader (a tested option, wired into no plan, queue or collector), with no network access.

The synthetic workbooks follow the packaging of HUD's static ZIP_COUNTY_<MMYYYY>.xlsx files (shared strings, uppercase header, exponent number text), which the
manual-download reader of ``hud_xlsx_contract`` rejects. An optional pair check reads one real direct file and the stored workbook of the same quarter and
proves that both give the same keys and ratio values.
"""

import argparse
import io
import json
import sys
import zipfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from scripts.acquisition import hud_api_contract as api_contract
from scripts.acquisition import hud_xlsx_contract as manual_contract
from scripts.acquisition import hud_xlsx_direct_contract as contract
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import require

MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
RELATIONSHIPS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
SMALL = "7.0057581999999993E-2"
LARGE = "0.66666666699999999"
PROLOG = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r\n'


def rows(year: int, quarter: int) -> list[list[str]]:
    """Crosswalk-shaped rows in HUD's direct layout: uppercase header, one ZIP per state, a split ZIP with exponent text and a business-only ZIP."""
    body = [[f"{i:05d}", f"{state}001", "1", "1", "0", "1"] for i, state in enumerate(api_contract.STATE_FIPS, 1)]
    body[0][2:] = [SMALL, "0", "0", SMALL]
    body.append([body[0][0], "01003", LARGE, "1", "0", LARGE])
    body.append(["99999", "02013", "0", "1", "0", "1"])
    body.append([f"{year % 100:03d}{quarter:02d}", "72001", "1", "0", "0", "1"])
    return [list(contract.DIRECT_HEADER), *body]


def shared_strings(table: list[list[str]], kinds: dict[tuple[int, int], str] | None = None, *, rich: bool = False) -> tuple[bytes, dict[str, int]]:
    """The shared string table of a table's text cells in first-use order, the way Excel writes it."""
    index: dict[str, int] = {}
    references = 0
    for r, values in enumerate(table, 1):
        for c, value in enumerate(values):
            if (kinds or {}).get((r, c), "shared" if r == 1 or c < 2 else "number") == "shared":
                references += 1
                index.setdefault(value, len(index))
    items = "".join(f"<si><r><t>{v}</t></r></si>" if rich else f"<si><t>{v}</t></si>" for v in index)
    head = f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r\n<sst xmlns="{MAIN}" count="{references}" uniqueCount="{len(index)}">'
    return (head + items + "</sst>").encode(), index


def sheet(table: list[list[str]], index: dict[str, int], kinds: dict[tuple[int, int], str] | None = None, prolog: str = "") -> bytes:
    """Serialize rows as HUD's static files do: header and codes as shared strings, ratios as plain numbers, Excel row attributes."""
    kinds = kinds or {}
    out = []
    for r, values in enumerate(table, 1):
        cells = []
        for c, value in enumerate(values):
            kind = kinds.get((r, c), "shared" if r == 1 or c < 2 else "number")
            ref = f"{'ABCDEFGH'[c]}{r}"
            if kind == "shared":
                cells.append(f'<c r="{ref}" s="1" t="s"><v>{index[value]}</v></c>')
            elif kind == "inline":
                cells.append(f'<c r="{ref}" s="1" t="inlineStr"><is><t>{value}</t></is></c>')
            elif kind == "formula":
                cells.append(f'<c r="{ref}" s="2"><f>1/1</f><v>{value}</v></c>')
            else:
                cells.append(f'<c r="{ref}" s="2"><v>{value}</v></c>')
        out.append(f'<row r="{r}" spans="1:{len(values)}" x14ac:dyDescent="0.25">{"".join(cells)}</row>')
    head = (
        f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r\n<worksheet xmlns="{MAIN}" xmlns:r="{RELATIONSHIPS}" '
        f'xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006" mc:Ignorable="x14ac" '
        f'xmlns:x14ac="http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac">{prolog}<dimension ref="A1:F{len(table)}"/>'
        '<sheetFormatPr defaultRowHeight="15" x14ac:dyDescent="0.25"/><cols><col min="1" max="1" width="6" style="1" customWidth="1"/></cols>'
    )
    tail = '<pageMargins left="0.75" right="0.75" top="1" bottom="1" header="0.5" footer="0.5"/><pageSetup orientation="portrait" r:id="rId1"/></worksheet>'
    return (head + f"<sheetData>{''.join(out)}</sheetData>" + tail).encode()


def workbook(
    table: list[list[str]],
    *,
    name: str = "ZIP_COUNTY_122020",
    kinds: dict | None = None,
    prolog: str = "",
    sheets: int = 1,
    extra: dict[str, bytes] | None = None,
    drop: tuple[str, ...] = (),
    rich: bool = False,
    strings: bytes | None = None,
) -> bytes:
    """A synthetic package with the member set of HUD's static files."""
    text, index = shared_strings(table, kinds, rich=rich)
    listed = "".join(f'<sheet name="{name if i == 1 else name + str(i)}" sheetId="{i}" r:id="rId{i}"/>' for i in range(1, sheets + 1))
    members = {
        "[Content_Types].xml": b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r\n<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"/>',
        "_rels/.rels": b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r\n<Relationships/>',
        "docProps/app.xml": b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r\n<Properties/>',
        "docProps/core.xml": b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r\n<coreProperties/>',
        "xl/_rels/workbook.xml.rels": b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r\n<Relationships/>',
        "xl/printerSettings/printerSettings1.bin": b"\x00\x01",
        "xl/sharedStrings.xml": strings if strings is not None else text,
        "xl/styles.xml": f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r\n<styleSheet xmlns="{MAIN}"/>'.encode(),
        "xl/theme/theme1.xml": b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r\n<theme/>',
        "xl/workbook.xml": (f'{PROLOG}<workbook xmlns="{MAIN}" xmlns:r="{RELATIONSHIPS}"><sheets>{listed}</sheets></workbook>').encode(),
        "xl/worksheets/_rels/sheet1.xml.rels": b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r\n<Relationships/>',
        "xl/worksheets/sheet1.xml": sheet(table, index, kinds, prolog),
    } | (extra or {})
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for member, body in members.items():
            if member not in drop:
                archive.writestr(zipfile.ZipInfo(member, date_time=(1980, 1, 1, 0, 0, 0)), body, zipfile.ZIP_DEFLATED)
    return buffer.getvalue()


def synthetic_plan(year: int = 2020, quarter: int = 4) -> tuple[dict, dict]:
    """The two facts the reader takes from a plan and a batch: the required states and the quarter."""
    return {"required_state_fips": api_contract.STATE_FIPS}, {"year": year, "quarter": quarter}


def manual_csv(raw: bytes) -> tuple[bytes, dict]:
    """The stored manual-download reader's CSV and statistics, with no exact-repeat decision in play."""
    plan = {"header": manual_contract.HEADER, "required_state_fips": api_contract.STATE_FIPS, "batches": []}
    original = manual_contract.DECISION_PATH
    manual_contract.DECISION_PATH = Path("no_exact_repeat_decision.json")
    try:
        return manual_contract.validate_workbook(raw, {"id": ""}, plan)
    finally:
        manual_contract.DECISION_PATH = original


def reader_scenarios(scenario: Any) -> None:
    """The direct layout is read, every other layout, cell type and content defect stops the quarter with a fixed message."""
    plan, batch = synthetic_plan()
    raw = workbook(rows(2020, 4), name=contract.sheet_name(2020, 4))

    def reads_direct_layout() -> None:
        csv_bytes, statistics = contract.validate_workbook(raw, batch, plan)
        require(csv_bytes.startswith(b"bus_ratio,geoid,oth_ratio,res_ratio,tot_ratio,zip\n"), "Derived layout differs from the API captures")
        require(SMALL.encode() in csv_bytes and LARGE.encode() in csv_bytes and b",00001\n" in csv_bytes, "Ratio text or leading zeros lost")
        require(statistics["rows"] == len(api_contract.STATE_FIPS) + 3 and statistics["direction"] == "zip_to_county", "Rows or direction differ")

    scenario("direct_layout_read_into_the_api_csv_layout", reads_direct_layout)

    def manual_reader_still_refuses() -> None:
        manual_contract.workbook_rows(raw)

    scenario("manual_reader_refuses_the_direct_layout", manual_reader_still_refuses, "package members differ")

    def same_values_as_manual_layout() -> None:
        table = rows(2020, 4)
        lowered = [[v for v in manual_contract.HEADER], *table[1:]]
        direct, _ = contract.validate_workbook(raw, batch, plan)
        manual, _ = manual_csv(manual_workbook(lowered))
        require(csv_values(direct) == csv_values(manual), "Direct and manual layouts give different values")

    scenario("same_values_as_the_manual_layout", same_values_as_manual_layout)

    def check(**changes: Any) -> None:
        table = rows(2020, 4)
        mutate = changes.pop("mutate", None)
        if mutate:
            mutate(table)
        sheet_title = changes.pop("name", contract.sheet_name(2020, 4))
        contract.validate_workbook(changes.pop("raw", None) or workbook(table, name=sheet_title, **changes), batch, plan)

    for name, kwargs, error in [
        ("duplicate_row", {"mutate": lambda t: t.append(list(t[1]))}, "Duplicate ZIP-county"),
        ("missing_state", {"mutate": lambda t: t.__delitem__(slice(51, 52))}, "state coverage"),
        ("extra_column", {"mutate": lambda t: [r.append("x") for r in t]}, "row shape"),
        ("short_row", {"mutate": lambda t: t[3].pop()}, "row shape"),
        ("lowercase_header", {"mutate": lambda t: t.__setitem__(0, list(manual_contract.HEADER))}, "header differs"),
        ("header_order", {"mutate": lambda t: t.__setitem__(0, [t[0][1], t[0][0], *t[0][2:]])}, "header differs"),
        ("inline_string_cell", {"kinds": {(3, 0): "inline"}}, "cell type"),
        ("formula_cell", {"kinds": {(3, 2): "formula"}}, "cell type"),
        ("numeric_zip", {"kinds": {(3, 0): "number"}}, "cell type"),
        ("text_ratio", {"kinds": {(3, 2): "shared"}}, "cell type"),
        ("short_county", {"mutate": lambda t: t[3].__setitem__(1, "1001")}, "identifier invalid"),
        ("ratio_above_one", {"mutate": lambda t: t[3].__setitem__(5, "1.5")}, "ratio invalid"),
        ("ratio_not_a_number", {"mutate": lambda t: t[3].__setitem__(5, "NaN")}, "ratio invalid"),
        ("second_sheet", {"sheets": 2}, "one worksheet"),
        ("other_quarters_sheet_name", {"name": "ZIP_COUNTY_092020"}, "sheet name differs"),
        ("extra_member", {"extra": {"xl/vbaProject.bin": b"x"}}, "package members differ"),
        ("missing_shared_strings", {"drop": ("xl/sharedStrings.xml",)}, "package members differ"),
        ("rich_text_string", {"rich": True}, "shared strings differ"),
        ("entity_declaration", {"prolog": '<!DOCTYPE x [<!ENTITY a "b">]>'}, "declarations"),
        ("header_only", {"mutate": lambda t: t.__delitem__(slice(1, None))}, "no data rows"),
        ("not_a_workbook", {"raw": b"<html>Login</html>"}, "not a workbook package"),
    ]:
        scenario(name, lambda kwargs=kwargs: check(**dict(kwargs)), error)

    def oversized() -> None:
        original = contract.MAX_SHEET_BYTES
        contract.MAX_SHEET_BYTES = 100
        try:
            check()
        finally:
            contract.MAX_SHEET_BYTES = original

    scenario("expanded_size_bound", oversized, "size bound")


def manual_workbook(table: list[list[str]]) -> bytes:
    """A workbook in the manual-download layout (inline strings, lowercase header) for the equal-values comparison."""
    from scripts.acquisition import run_hud_xlsx_e2e as manual

    return manual.workbook(table)


def csv_values(body: bytes) -> list[tuple]:
    """Each derived row as (zip, county, four ratios as exact decimals), so equal values compare equal whatever their text."""
    lines = body.decode().splitlines()
    header = lines[0].split(",")
    found = []
    for line in lines[1:]:
        row = dict(zip(header, line.split(","), strict=True))
        found.append((row["zip"], row["geoid"], *(Decimal(row[name]) for name in api_contract.RATIOS)))
    return found


def exercise(pair: tuple[Path, Path] | None = None) -> list[dict]:
    """Run every scenario against synthetic workbooks, and against the real pair when it is given."""
    scenarios: list[dict] = []

    def scenario(name: str, action: Any, error: str | None = None) -> None:
        try:
            action()
            scenarios.append({"name": name, "passed": error is None, "observed": "success", "expected": error or "success"})
        except (ValueError, OSError, KeyError, TypeError, StopIteration, zipfile.BadZipFile) as failure:
            scenarios.append(
                {"name": name, "passed": error is not None and error in str(failure), "observed": str(failure)[:200], "expected": error or "success"}
            )

    reader_scenarios(scenario)
    if pair is not None:
        summary = pair_scenarios(scenario, *pair)
        scenarios.append({"name": "real_pair_tally", "passed": bool(summary), "observed": json.dumps(summary, sort_keys=True), "expected": "tally recorded"})
    return scenarios


def pair_scenarios(scenario: Any, direct: Path, stored: Path) -> dict[str, Any]:
    """One real quarter in both packagings: the same keys and ratio values to within double precision, with the exact tallies recorded."""
    summary: dict[str, Any] = {}

    def same_keys_and_values() -> None:
        raw = direct.read_bytes()
        plan, batch = synthetic_plan(2020, 4)
        new, new_stats = contract.validate_workbook(raw, batch, plan)
        old, old_stats = manual_csv(stored.read_bytes())
        new_rows, old_rows = csv_values(new), csv_values(old)
        require(
            [r[:2] for r in new_rows] == [r[:2] for r in old_rows] and new_stats["rows"] == old_stats["rows"], "Direct and stored files differ in their keys"
        )
        gaps = [abs(a - b) for x, y in zip(new_rows, old_rows, strict=True) for a, b in zip(x[2:], y[2:], strict=True)]
        summary.update(rows=new_stats["rows"], cells=len(gaps), max_abs_difference=str(max(gaps)), cells_with_a_difference=sum(g != 0 for g in gaps))
        require(max(gaps) < Decimal("1e-15"), "Direct and stored ratio values differ beyond double precision")
        require(new_stats["zips"] == old_stats["zips"] and new_stats["counties"] == old_stats["counties"], "Direct and stored files differ in their coverage")

    scenario("real_pair_same_keys_and_ratio_values", same_keys_and_values)
    return summary


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pair-direct", type=Path, help="a real direct static file of one quarter (2020 Q4)")
    parser.add_argument("--pair-stored", type=Path, help="the stored manual-download workbook of the same quarter")
    args = parser.parse_args()
    pair = (args.pair_direct, args.pair_stored) if args.pair_direct and args.pair_stored else None
    scenarios = exercise(pair)
    passed = all(s["passed"] for s in scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "feature": "HUD ZIP-COUNTY direct static file reader and capture (2010 Q1 to 2020 Q4)",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_hud_xlsx_direct_e2e --output {args.output}",
        "boundary": "Synthetic workbooks built in memory, plus the real 2020 Q4 pair only when given; no network, HUD or AWS access.",
        "cleanup": "Nothing to clean up: no process, folder or file besides the artifact.",
        "scenarios": scenarios,
    }
    write_once(args.output, encoded_json(report))
    sys.stdout.write(
        json.dumps({"status": report["status"], "passed": sum(s["passed"] for s in scenarios), "total": len(scenarios), "artifact": str(args.output)}) + "\n"
    )
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
