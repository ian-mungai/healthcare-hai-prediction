"""Frozen scope and content checks for the manually downloaded HUD ZIP-COUNTY workbooks, 2010 Q1 to 2020 Q4.

The API serves nationwide requests only from 2021 Q1, so the user downloaded these quarters by hand
from the HUD crosswalk files site. Each workbook is bound to its recorded download hash. Codes and
ratio text stay exactly as published; nothing is inverted, renormalized or collapsed.
"""

import csv
import io
import plistlib
import re
import sys
import zipfile
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from scripts.acquisition import hud_api_contract as api
from scripts.acquisition.process import run_command
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require

PLAN_PATH = REPO_ROOT / "config/acquisition/hud_xlsx_plan_2010q1_2020q4.json"
# User decision 2026-09-28: 2014 Q2 repeats 49,140 rows exactly; store the original, drop exact repeats from the CSV only.
DECISION_PATH = REPO_ROOT / "config/acquisition/hud_xlsx_exact_repeats_2014q2.json"
ROUTE_URL = "https://www.huduser.gov/apps/public/uspscrosswalk/home"
ORIGIN = "https://www.huduser.gov/"
HEADER = ["zip", "geoid", "res_ratio", "bus_ratio", "oth_ratio", "tot_ratio"]
# The member set every published 2010-2020 workbook has (content profile, 2026-09-28).
MEMBERS = frozenset(
    {
        "[Content_Types].xml",
        "_rels/.rels",
        "docProps/app.xml",
        "docProps/core.xml",
        "xl/_rels/workbook.xml.rels",
        "xl/comments1.xml",
        "xl/drawings/vmlDrawing1.vml",
        "xl/sharedStrings.xml",
        "xl/styles.xml",
        "xl/workbook.xml",
        "xl/worksheets/_rels/sheet1.xml.rels",
        "xl/worksheets/sheet1.xml",
    }
)
MAX_FILE_BYTES = 16 * 1024**2
MAX_SHEET_BYTES = 64 * 1024**2
SHEET = re.compile(rb"<\?xml [^<>]*\?>\s*<worksheet [^<>]*>(?P<outside>.*?)<sheetData>(?P<data>.*)</sheetData>(?P<after>.*)</worksheet>\s*", re.S)
ROW = re.compile(rb'<row r="([0-9]+)"(?: [a-zA-Z]+="[^"<>]*")*>(.*?)</row>', re.S)
TEXT_CELL = re.compile(rb'<c r="([A-Z]+[0-9]+)"(?: s="[0-9]+")? t="inlineStr"><is><t>([0-9A-Za-z_]*)</t></is></c>')
NUMBER_CELL = re.compile(rb'<c r="([A-Z]+[0-9]+)"(?: s="[0-9]+")?><v>([^<>&]*)</v></c>')
NUMBER = re.compile(r"[0-9]+(?:\.[0-9]+)?(?:[Ee][-+]?[0-9]+)?")
MONTHS = {1: "03", 2: "06", 3: "09", 4: "12"}


def quarters() -> list[tuple[int, int]]:
    """Every approved quarter, in order."""
    return [(y, q) for y in range(2010, 2021) for q in range(1, 5)]


def file_name(year: int, quarter: int) -> str:
    """HUD names each download by the quarter's last month and year."""
    return f"ZIP-COUNTY_{MONTHS[quarter]}{year}.xlsx"


def county_geography(year: int, quarter: int) -> str:
    """HUD's notes: 2000 Census counties through 2011 Q4, 2010 Census counties from 2012 Q1."""
    return "2010_census" if (year, quarter) >= (2012, 1) else "2000_census"


def batch_id(batch: dict) -> str:
    """Identify one quarter's workbook by its route, period and exact downloaded bytes."""
    return canonical_hash({"route": ROUTE_URL, "type": "ZIP-COUNTY", "year": batch["year"], "quarter": batch["quarter"], "sha256": batch["sha256"]})


def selections(batch: dict) -> dict:
    """The choices the user made on the files site for this quarter."""
    ordinal = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th"}[batch["quarter"]]
    return {"crosswalk_type": "ZIP-COUNTY", "data_year_and_quarter": f"{ordinal} Quarter {batch['year']}", "saved_by": "user, Chrome"}


def load_plan(path: Path | None = None) -> dict:
    """Verify the locked scope, per-quarter download bindings, terms record and base registry."""
    path = PLAN_PATH if path is None else path
    plan = read_json(path)
    require(read_json(path.with_suffix(".lock.json"))["plan_sha256"] == canonical_hash(plan), "HUD workbook plan lock differs")
    require(plan["source_id"] == "HUD" and plan["model_eligible"] is False and plan["version"] == 3, "HUD workbook plan hold differs")
    require(plan["route_url"] == ROUTE_URL and plan["origin"] == ORIGIN and plan["header"] == HEADER, "HUD workbook route or header differs")
    require(plan["registry_sha256"] == canonical_hash(load_registry(expected_sha256=plan["registry_sha256"])), "HUD registry differs")
    require(plan["required_state_fips"] == api.STATE_FIPS, "HUD state coverage rule differs")
    terms = plan["terms"]
    require(api.digest((REPO_ROOT / terms["path"]).read_bytes()) == terms["sha256"], "HUD terms record changed")
    record = next(d for d in read_json(REPO_ROOT / terms["path"])["datasets"] if d["source_id"] == "HUD")
    require(record["terms_accepted"] is True, "HUD terms not accepted")
    require([(b["year"], b["quarter"]) for b in plan["batches"]] == quarters(), "HUD workbook quarters differ")
    for batch in plan["batches"]:
        require(batch["id"] == batch_id(batch) and batch["file_name"] == file_name(batch["year"], batch["quarter"]), "HUD workbook batch binding differs")
        require(batch["county_geography"] == county_geography(batch["year"], batch["quarter"]), "HUD county geography era differs")
        require(re.fullmatch(r"[0-9a-f]{64}", batch["sha256"]) is not None and 0 < batch["bytes"] <= MAX_FILE_BYTES, "HUD workbook binding invalid")
    return plan


def read_origin(path: Path) -> list[str]:
    """The download origin macOS browsers record on saved files."""
    result = run_command("xattr", ["-px", "com.apple.metadata:kMDItemWhereFroms", str(path)], timeout=30)
    require(result.returncode == 0 and bool(result.stdout.strip()), "HUD download origin missing")
    origins = plistlib.loads(bytes.fromhex("".join(result.stdout.split())))
    require(isinstance(origins, list) and all(isinstance(u, str) for u in origins), "HUD download origin missing")
    return origins


def download_created_at(path: Path) -> str:
    """When macOS created the browser download (file birth time), as UTC ISO 8601."""
    if sys.platform != "darwin":
        raise OSError("Browser download creation time is recorded only on macOS")
    return datetime.fromtimestamp(path.stat().st_birthtime, UTC).isoformat()


def xml_bytes(archive: zipfile.ZipFile, name: str, limit: int) -> bytes:
    """Read one bounded package member and refuse declarations, comments and CDATA."""
    require(archive.getinfo(name).file_size <= limit, "HUD workbook member exceeds its size bound")
    body = archive.read(name)
    require(len(body) <= limit, "HUD workbook member exceeds its size bound")
    require(b"<!" not in body, "HUD workbook contains XML declarations")
    return body


def matched(pattern: re.Pattern[bytes], content: bytes, position: int, message: str) -> re.Match[bytes]:
    """Match at an exact position or stop the quarter with a fixed public message."""
    match = pattern.match(content, position)
    if match is None:
        raise ValueError(message)
    return match


def row_cells(content: bytes, number: int) -> list[str]:
    """Match a row's cells exactly; codes and headers must be inline text, ratios plain numbers."""
    values, position = [], 0
    for column in range(len(HEADER)):
        require(position < len(content), "HUD workbook row shape differs")
        match = matched(TEXT_CELL if number == 1 or column < 2 else NUMBER_CELL, content, position, "HUD workbook cell type differs")
        require(match[1] == f"{'ABCDEF'[column]}{number}".encode(), "HUD workbook row shape differs")
        values.append(match[2].decode("ascii"))
        position = match.end()
    require(position == len(content), "HUD workbook row shape differs")
    return values


def workbook_rows(raw: bytes) -> list[list[str]]:
    """Read the single worksheet's rows as published text, after package and size checks.

    The worksheet is matched against the publisher's exact machine-generated layout rather than parsed
    as general XML: every byte of the sheet data must belong to a recognized row and cell, so any
    other construct (formulas, shared strings, entities, extra columns) stops the quarter.
    """
    require(len(raw) <= MAX_FILE_BYTES, "HUD workbook exceeds its size bound")
    try:
        archive = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile:
        raise ValueError("HUD download is not a workbook package") from None
    with archive:
        names = [info.filename for info in archive.infolist()]
        require(len(names) == len(set(names)) and set(names) == MEMBERS, "HUD workbook package members differ")
        require(archive.testzip() is None, "HUD workbook member CRC differs")
        book = xml_bytes(archive, "xl/workbook.xml", 1024**2)
        require(book.count(b"<sheet ") == 1, "HUD workbook must contain exactly one worksheet")
        strings = xml_bytes(archive, "xl/sharedStrings.xml", 1024**2)
        require(b'uniqueCount="0"' in strings and b"<si" not in strings, "HUD workbook uses shared strings")
        body = xml_bytes(archive, "xl/worksheets/sheet1.xml", MAX_SHEET_BYTES)
    sheet = SHEET.fullmatch(body)
    if sheet is None or b"<row" in sheet["outside"] + sheet["after"]:
        raise ValueError("HUD workbook row shape differs")
    data, position = sheet["data"], 0
    rows: list[list[str]] = []
    while position < len(data):
        match = matched(ROW, data, position, "HUD workbook row shape differs")
        require(match[1] == str(len(rows) + 1).encode(), "HUD workbook row shape differs")
        rows.append(row_cells(match[2], len(rows) + 1))
        position = match.end()
    return rows


def ratio(text: str) -> Decimal:
    """Parse ratio text exactly for range and sum checks; the text itself is what gets stored."""
    try:
        value = Decimal(text) if NUMBER.fullmatch(text) else Decimal(-1)
    except InvalidOperation:
        value = Decimal(-1)
    require(0 <= value <= 1, "HUD ratio invalid")
    return value


def load_repeat_decisions(plan: dict) -> dict[str, int]:
    """Approved exact-repeat counts by batch ID; no record means no quarter may repeat a row."""
    if not DECISION_PATH.exists():
        return {}
    decision = read_json(DECISION_PATH)
    require(read_json(DECISION_PATH.with_suffix(".lock.json"))["decision_sha256"] == canonical_hash(decision), "HUD repeat decision lock differs")
    require(decision["source_id"] == "HUD" and decision["plan_sha256"] == canonical_hash(plan), "HUD repeat decision binding differs")
    batches = {b["id"]: b for b in plan["batches"]}
    allowed = {}
    for item in decision["quarters"]:
        batch = batches.get(item["batch_id"])
        require(
            batch is not None and batch["sha256"] == item["sha256"] and (batch["year"], batch["quarter"]) == (item["year"], item["quarter"]),
            "HUD repeat decision binding differs",
        )
        require(isinstance(item["exact_repeat_rows"], int) and item["exact_repeat_rows"] > 0, "HUD repeat decision binding differs")
        allowed[item["batch_id"]] = item["exact_repeat_rows"]
    return allowed


def validate_workbook(raw: bytes, batch: dict, plan: dict) -> tuple[bytes, dict]:
    """Write a lossless CSV in the API captures' layout while refusing schema, identity or coverage defects."""
    table = workbook_rows(raw)
    require(bool(table) and table[0] == plan["header"], "HUD workbook header differs")
    require(len(table) > 1, "HUD workbook has no data rows")
    allowed = load_repeat_decisions(plan).get(batch.get("id", ""))
    seen: dict[tuple[str, str], list[str]] = {}
    rows, repeats = [], 0
    for values in table[1:]:
        row = dict(zip(HEADER, values, strict=True))
        # No territory-level codes appear in 2010-2020; only five-digit county codes are accepted.
        require(
            re.fullmatch(r"[0-9]{5}", row["zip"]) is not None and re.fullmatch(r"[0-9]{5}", row["geoid"]) is not None, "HUD ZIP or county identifier invalid"
        )
        parsed = {name: ratio(row[name]) for name in api.RATIOS}
        key = (row["zip"], row["geoid"])
        if key in seen and allowed is not None and seen[key] == values:
            repeats += 1
            continue
        require(key not in seen, "Duplicate ZIP-county row in HUD workbook")
        seen[key] = values
        rows.append((row, parsed))
    require(allowed is None or repeats == allowed, "HUD exact repeat count differs from the approved decision")
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
    if allowed is not None:
        statistics["exact_repeat_rows_dropped"] = repeats
    return output.getvalue().encode(), statistics


def verify_capture(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Check every artifact, the download binding, terms, geography era and preserved holds."""
    from scripts.acquisition.capture import receipt_validator

    require(not evidence_only and source["source_id"] == "HUD", "HUD storage source differs")
    plan = load_plan()
    require(lineage["plan_sha256"] == canonical_hash(plan), "HUD capture plan differs")
    require(lineage["registry_sha256"] == canonical_hash(load_registry(expected_sha256=lineage["registry_sha256"])), "HUD registry differs")
    require(lineage["schema_sha256"] == canonical_hash(receipt_validator().schema), "HUD schema differs")
    require(lineage["terms_sha256"] == plan["terms"]["sha256"], "HUD terms binding differs")
    require(lineage["model_eligible"] is False and receipt["snapshot_status"] == "acquired_unvalidated", "HUD modeling hold differs")
    api.require_code(lineage["code_sha256"])
    matches = [b for b in plan["batches"] if b["id"] == lineage["batch_id"]]
    require(len(matches) == 1, "HUD capture batch not in its plan")
    batch = matches[0]
    require(lineage["county_geography"] == batch["county_geography"], "HUD county geography era differs")
    decided = batch["id"] in load_repeat_decisions(plan)
    bound = canonical_hash(read_json(DECISION_PATH)) if decided else None
    require(lineage.get("exact_repeat_decision_sha256") == bound, "HUD repeat decision binding differs")
    acq = receipt["acquisition"]
    require(acq["requested_url"] == ROUTE_URL and acq["request_method"] == "manual_download" and acq["http_status"] is None, "HUD download route differs")
    require(acq["export_selections"] == selections(batch), "HUD download selections differ")
    paths = {a["storage_path"]: a for a in receipt["artifacts"]}
    expected = {f"raw/{batch['file_name']}", "derived/crosswalk.csv", "evidence/download_proof.json", "references/scope.json"}
    require(set(paths) == expected and len(receipt["artifacts"]) == 4, "HUD artifact set differs")
    raw = (root / f"raw/{batch['file_name']}").read_bytes()
    require(api.digest(raw) == lineage["expected_sha256"] == batch["sha256"] and len(raw) == batch["bytes"], "HUD raw hash differs")
    derived, statistics = validate_workbook(raw, batch, plan)
    require((root / "derived/crosswalk.csv").read_bytes() == derived, "HUD derived CSV differs")
    require(read_json(root / "references/scope.json") == plan, "HUD stored scope differs")
    proof = read_json(root / "evidence/download_proof.json")
    require(proof["origin_urls"] == [ORIGIN] and proof["file_name"] == batch["file_name"], "HUD download proof differs")
    require(proof["sha256"] == batch["sha256"] and proof["bytes"] == batch["bytes"] and proof["statistics"] == statistics, "HUD download proof differs")
    require(proof["created_at_utc"] == acq["retrieved_at_utc"], "HUD download proof differs")
    require(
        receipt["schema_profile"]["row_count"] == statistics["rows"] and receipt["schema_profile"]["native_headers"] == HEADER, "HUD schema profile differs"
    )
    terms = next(d for d in read_json(REPO_ROOT / plan["terms"]["path"])["datasets"] if d["source_id"] == "HUD")
    require(receipt["governance"]["use_restrictions"] == terms["restrictions"], "HUD governance differs")
    start, end = api.quarter_period(batch)
    period = receipt["measurement_periods"]
    require(len(period) == 1 and period[0]["start_date"] == start and period[0]["end_date"] == end, "HUD measurement quarter differs")
