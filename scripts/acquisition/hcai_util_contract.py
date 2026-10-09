"""Frozen scope, workbook reading and privacy abstraction for the HCAI annual utilization workbooks, 2018-2025.

The public-business-data exception for 2012-2017 was extended to these years. Each
original names administrators and report preparers, so it stays on this Mac only; S3 receives one abstracted CSV
per sheet. Failure modes: data/acquisition_planning/hcai_util_2018_2025_failure_modes.md.
"""

import csv
import io
import re
import unicodedata
import zipfile
from html.parser import HTMLParser
from pathlib import Path

from scripts.acquisition.bls_api_contract import code_hashes, digest
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.legacy_versions import bound_record, plan_matches
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require

PLAN_PATH = REPO_ROOT / "config/acquisition/hcai_util_2018_2025_plan.json"
VERSIONS_PATH = REPO_ROOT / "config/acquisition/hcai_util_code_versions.json"
RELEASE_PATH = "config/acquisition/access_releases_20260929.json"
SOURCE_ID = "HCAI_UTIL"
TERMS_URL = "https://data.chhs.ca.gov/pages/terms"
REPLACEMENT = "[REDACTED]"
REDACT = ["FAC_STR_ADDR", "FAC_PHONE", "FAC_ADMIN_NAME", "FAC_PAR_CORP_BUS_ADDR", "REPT_PREP_NAME", "REV_REPT_PREP_NAME"]
NAME_SOURCES = ["FAC_ADMIN_NAME", "REPT_PREP_NAME", "REV_REPT_PREP_NAME"]
NOT_PERSONAL = ["FAC_NAME", "FAC_PAR_CORP_NAME", "EMSA_TRAUMA_DESIGNATION", "EMSA_TRAUMA_DESIGNATION_PEDIATRIC"]
PERSONAL_PATTERN = re.compile(r"NAME|PHONE|FAX|MAIL|ADDR|PREP|CONTACT|TITLE|SIGN")
INDIVIDUAL_OWNER = "Investor - Individual"
OWNER_REDACT = ["FAC_PAR_CORP_NAME", "FAC_PAR_CORP_CITY", "FAC_PAR_CORP_ZIP"]
DATA_SHEETS = ["Page 1-6", "NonResp 1-6"]
LABEL_ROWS = 4  # Rows 2-5: database label, Page, Column and Line.
MIN_HOSPITALS = 400
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+")
PHONE = re.compile(r"(?<!\d)(?:\(\d{3}\)\s?|\d{3}[-.\s])\d{3}[-.\s]\d{4}(?!\d)")
MAX_BYTES = 64 * 1024**2
MAX_MEMBERS = 100


def require_code(hashes: dict | None = None) -> dict:
    """Require explicit review of new captures and retain trusted historical implementations."""
    actual = code_hashes() if hashes is None else hashes
    require(bool(actual) and actual in [v["code_sha256"] for v in read_code_versions(VERSIONS_PATH)["versions"]], "Unreviewed HCAI utilization code version")
    return actual


def year_id(entry: dict) -> str:
    """Identify one year by its resource, URL and recorded original."""
    return canonical_hash({"year": entry["year"], "url": entry["url"], "sha256": entry["sha256"], "bytes": entry["bytes"]})


def slug(sheet: str) -> str:
    """A storage-safe file name for a sheet."""
    return re.sub(r"[^a-z0-9]+", "_", sheet.lower()).strip("_")


def load_plan(path: Path | None = None) -> dict:
    """Verify the locked scope, per-year bindings, access release and base registry."""
    path = PLAN_PATH if path is None else path
    plan = read_json(path)
    require(read_json(path.with_suffix(".lock.json"))["plan_sha256"] == canonical_hash(plan), "HCAI utilization plan lock differs")
    require(plan["source_id"] == SOURCE_ID and plan["model_eligible"] is False and plan["version"] == 1, "HCAI utilization plan hold differs")
    rules = {"redact": REDACT, "not_personal": NOT_PERSONAL, "individual_owner_redact": OWNER_REDACT, "replacement": REPLACEMENT}
    require(all(plan[k] == v for k, v in rules.items()), "HCAI utilization plan rules differ")
    require(plan["registry_sha256"] == canonical_hash(load_registry(expected_sha256=plan["registry_sha256"])), "HCAI utilization registry differs")
    release = plan["access_release"]
    require(
        release["path"] == RELEASE_PATH and digest((REPO_ROOT / release["path"]).read_bytes()) == release["sha256"], "HCAI utilization access release changed"
    )
    records = [r for r in read_json(REPO_ROOT / release["path"])["releases"] if r["source_id"] == SOURCE_ID]
    require(len(records) == 1 and records[0]["released"] is True, "HCAI utilization access not released")
    require(bool(plan["years"]) and len({y["id"] for y in plan["years"]}) == len(plan["years"]), "HCAI utilization plan has no years")
    for entry in plan["years"]:
        require(entry["id"] == year_id(entry) and re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]) is not None, "HCAI utilization year binding differs")
        require(0 < entry["bytes"] <= MAX_BYTES and 2018 <= entry["year"] <= 2025, "HCAI utilization year binding invalid")
        require(set(DATA_SHEETS) <= set(entry["sheets"]) and entry["header"][:2] == ["Description", "FAC_NO"], "HCAI utilization year layout differs")
    return plan


class _Markup(HTMLParser):
    """Event reader for workbook XML: no entity expansion, no network, no DTD."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.events: list[tuple[str, str, dict[str, str]]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.events.append(("start", tag.rsplit(":", 1)[-1], {k: v or "" for k, v in attrs}))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_endtag(self, tag: str) -> None:
        self.events.append(("end", tag.rsplit(":", 1)[-1], {}))

    def handle_data(self, data: str) -> None:
        self.events.append(("data", data, {}))


def events(body: bytes) -> list[tuple[str, str, dict[str, str]]]:
    """Parse one bounded workbook part after refusing declarations, comments and CDATA."""
    require(b"<!" not in body, "HCAI workbook contains XML declarations")
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("HCAI workbook part is not UTF-8") from None
    parser = _Markup()
    parser.feed(text)
    parser.close()
    return parser.events


def shared_strings(body: bytes) -> list[str]:
    """Shared strings as stored; phonetic runs are not part of the value."""
    strings: list[str] = []
    current: list[str] | None = None
    depth_t, phonetic = 0, 0
    for kind, name, _ in events(body):
        if kind == "start" and name == "si":
            current = []
        elif kind == "end" and name == "si":
            require(current is not None, "HCAI workbook shared strings differ")
            strings.append("".join(current or []))
            current = None
        elif kind == "start" and name == "rph":
            phonetic += 1
        elif kind == "end" and name == "rph":
            phonetic -= 1
        elif kind == "start" and name == "t":
            depth_t += 1
        elif kind == "end" and name == "t":
            depth_t -= 1
        elif kind == "data" and depth_t and not phonetic and current is not None:
            current.append(name)
    return strings


def column_number(reference: str) -> int:
    """1-based column number of a cell reference such as AB12."""
    letters = re.fullmatch(r"([A-Z]{1,3})[0-9]+", reference)
    require(letters is not None, "HCAI workbook cell reference differs")
    number = 0
    for char in letters[1] if letters else "":
        number = number * 26 + ord(char) - 64
    return number


def sheet_rows(body: bytes, strings: list[str]) -> list[list[str]]:
    """Dense rows of published cell text; formulas and unknown cell types stop the year."""
    rows: dict[int, dict[int, str]] = {}
    row = cell = None
    kind_of = ""
    parts: list[str] = []
    capture = False
    for kind, name, attrs in events(body):
        if kind == "start" and name == "row":
            row = int(attrs["r"])
            require(row not in rows, "HCAI workbook row repeated")
            rows[row] = {}
        elif kind == "start" and name == "c":
            require(row is not None, "HCAI workbook cell outside a row")
            cell, kind_of, parts = column_number(attrs["r"]), attrs.get("t", "n").lower(), []
            require(kind_of in {"s", "n", "e", "str", "inlinestr", "b"}, "HCAI workbook cell type differs")
        elif kind == "start" and name == "f":
            raise ValueError("HCAI workbook contains formulas")
        elif kind == "start" and name in {"v", "t"} and cell is not None:
            capture = True
        elif kind == "end" and name in {"v", "t"}:
            capture = False
        elif kind == "data" and capture:
            parts.append(name)
        elif kind == "end" and name == "c" and row is not None and cell is not None:
            value = "".join(parts)
            if kind_of == "s":
                require(value.isdigit() and int(value) < len(strings), "HCAI workbook shared string missing")
                value = strings[int(value)]
            require(cell not in rows[row], "HCAI workbook cell repeated")
            rows[row][cell] = value
            cell = None
    if not rows:
        return []
    width = max((max(cells) for cells in rows.values() if cells), default=0)
    return [[rows.get(r, {}).get(c, "") for c in range(1, width + 1)] for r in range(1, max(rows) + 1)]


def read_workbook(raw: bytes) -> dict[str, list[list[str]]]:
    """Every sheet's rows in workbook order, after package and size checks."""
    require(0 < len(raw) <= MAX_BYTES, "HCAI workbook exceeds its size bound")
    try:
        archive = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile:
        raise ValueError("HCAI download is not a workbook package") from None
    with archive:
        infos = archive.infolist()
        names = [i.filename for i in infos]
        require(len(names) == len(set(names)) and len(names) <= MAX_MEMBERS, "HCAI workbook package members differ")
        require(sum(i.file_size for i in infos) <= 8 * MAX_BYTES and archive.testzip() is None, "HCAI workbook package members differ")

        def part(name: str) -> bytes:
            require(name in names, "HCAI workbook part missing")
            return archive.read(name)

        strings = shared_strings(part("xl/sharedStrings.xml")) if "xl/sharedStrings.xml" in names else []
        targets = {a["id"]: a["target"] for k, n, a in events(part("xl/_rels/workbook.xml.rels")) if k == "start" and n == "relationship"}
        book: dict[str, list[list[str]]] = {}
        for kind, name, attrs in events(part("xl/workbook.xml")):
            if kind == "start" and name == "sheet":
                target = targets[attrs["r:id"]].lstrip("/")
                target = target if target.startswith("xl/") else f"xl/{target}"
                require(attrs["name"] not in book, "HCAI workbook sheet repeated")
                book[attrs["name"]] = sheet_rows(part(target), strings)
    return book


def normalized(value: str) -> str:
    """Case-, width- and punctuation-insensitive words for transient name matching."""
    return " ".join(re.sub(r"[^\w]+", " ", unicodedata.normalize("NFKC", value)).split()).casefold()


def contains_name(text: str, names: set[tuple[str, ...]], longest: int) -> bool:
    """True when the text contains any collected name as consecutive whole words."""
    words = tuple(normalized(text).split())
    return any(words[i : i + n] in names for n in range(2, longest + 1) for i in range(len(words) - n + 1))


def name_forms(value: str) -> set[tuple[str, ...]]:
    """A person's full name and first-plus-last name as word tuples; single words are not matched."""
    words = tuple(normalized(value).split())
    if len(words) < 2 or not any(c.isalpha() for c in words[0]) or not any(c.isalpha() for c in words[-1]):
        return set()
    return {words, (words[0], words[-1])}


def check_columns(header: list[str]) -> None:
    """Every personal-looking column is either redacted or reviewed as not personal."""
    for column in header:
        if PERSONAL_PATTERN.search(column.upper()):
            require(column in REDACT or column in NOT_PERSONAL, "HCAI unreviewed personal-looking column")
    require(all(c in header for c in [*REDACT, *OWNER_REDACT, "LICEE_TOC", "FAC_NO"]), "HCAI redaction column missing")


def derive(raw: bytes, entry: dict) -> tuple[dict[str, bytes], dict]:
    """One abstracted CSV per sheet, or a fixed public reason to stop the year."""
    book = read_workbook(raw)
    require(list(book) == entry["sheets"], "HCAI workbook sheets differ")
    names: set[tuple[str, ...]] = set()
    redacted = owner_rows = copied = contacts = 0
    hospital_counts: dict[str, int] = {}
    for sheet in DATA_SHEETS:
        rows = book[sheet]
        header = rows[0][: len(entry["header"])]
        require(header == entry["header"], "HCAI data header differs")
        check_columns(header)
        index = {c: i for i, c in enumerate(header)}
        for number, row in enumerate(rows):
            require(all(v == "" for v in row[len(header) :]) or 1 <= number <= LABEL_ROWS, "HCAI cell beyond the header")
            if number <= LABEL_ROWS:
                continue
            owned = [row[index["FAC_PAR_CORP_NAME"]]] if row[index["LICEE_TOC"]] == INDIVIDUAL_OWNER else []
            for value in [row[index[c]] for c in NAME_SOURCES] + owned:
                names.update(name_forms(value))
    longest = max((len(n) for n in names), default=0)
    files: dict[str, bytes] = {}
    for sheet, rows in book.items():
        data = sheet in DATA_SHEETS
        index = {c: i for i, c in enumerate(rows[0][: len(entry["header"])])} if data else {}
        hospitals = 0
        out = []
        for number, source_row in enumerate(rows):
            row = list(source_row)
            if data and number > LABEL_ROWS:
                hospitals += row[index["FAC_NO"]].strip() != ""
                owner = row[index["LICEE_TOC"]] == INDIVIDUAL_OWNER
                owner_rows += owner
                for column in REDACT + (OWNER_REDACT if owner else []):
                    if row[index[column]].strip():
                        row[index[column]] = REPLACEMENT
                        redacted += 1
            for i, value in enumerate(row):
                if value and value != REPLACEMENT and names and contains_name(value, names, longest):
                    row[i] = REPLACEMENT
                    copied += 1
                elif EMAIL.search(value) or PHONE.search(value):
                    # Contact details typed into other fields (for example an equipment description) are removed too.
                    row[i] = REPLACEMENT
                    contacts += 1
            out.append(row)
        if data:
            hospital_counts[sheet] = hospitals
        buffer = io.StringIO(newline="")
        csv.writer(buffer, lineterminator="\n").writerows(out)
        files[f"sheets/{slug(sheet)}.csv"] = buffer.getvalue().encode()
    require(hospital_counts["Page 1-6"] >= entry["min_hospitals"], "HCAI data sheet has too few hospitals")
    statistics = {
        "redacted_cells": redacted,
        "individual_owner_rows": owner_rows,
        "copied_name_cells": copied,
        "contact_detail_cells": contacts,
        "hospitals": hospital_counts,
        "preliminary": entry["preliminary"],
        "model_eligible": False,
    }
    return files, statistics


def roles(sheets: list[str]) -> dict[str, str]:
    """Receipt role of each derived sheet file."""
    kinds = {"Tips": "methodology", "Crosswalk": "layout"}
    return {f"sheets/{slug(s)}.csv": kinds.get(s, "data") for s in sheets}


def verify_capture(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Rebuild every sheet from the local original; check the artifact set, bindings, release and holds."""
    from scripts.acquisition.capture import receipt_validator

    require(not evidence_only and source["source_id"] == SOURCE_ID, "HCAI utilization storage source differs")
    plan = load_plan()
    require(plan_matches(plan, lineage["plan_sha256"]), "HCAI utilization capture plan differs")
    require(lineage["registry_sha256"] == canonical_hash(load_registry(expected_sha256=lineage["registry_sha256"])), "HCAI utilization registry differs")
    require(lineage["schema_sha256"] == canonical_hash(receipt_validator().schema), "HCAI utilization schema differs")
    # The receipt binds the release record current at capture; an archived predecessor still counts (failure mode S5).
    bound = bound_record(REPO_ROOT / plan["access_release"]["path"], lineage["access_release_sha256"])
    require(bound is not None, "HCAI utilization access release binding differs")
    require(lineage["model_eligible"] is False and receipt["snapshot_status"] == "acquired_unvalidated", "HCAI utilization modeling hold differs")
    require_code(lineage["code_sha256"])
    matches = [y for y in plan["years"] if y["id"] == lineage["year_id"]]
    require(len(matches) == 1, "HCAI utilization capture year not in its plan")
    entry = matches[0]
    expected = {*roles(entry["sheets"]), "evidence/download_proof.json", "references/scope.json"}
    require({a["storage_path"] for a in receipt["artifacts"]} == expected, "HCAI utilization artifact set differs")
    require(not (root / "raw").exists() and not (root / "audit").exists(), "HCAI utilization snapshot holds original bytes")
    proof = read_json(root / "evidence/download_proof.json")
    collection = root.parents[2]
    original = (collection / proof["original_path"]).resolve()
    require(original.is_relative_to((collection / "private_original").resolve()), "HCAI utilization original location differs")
    raw = original.read_bytes()
    require(
        digest(raw) == entry["sha256"] == lineage["expected_sha256"] == proof["sha256"] and len(raw) == entry["bytes"], "HCAI utilization original hash differs"
    )
    files, statistics = derive(raw, entry)
    for name, body in files.items():
        require((root / name).read_bytes() == body, "HCAI utilization derived sheet differs")
    require(proof["statistics"] == statistics and proof["requested_url"] == entry["url"], "HCAI utilization download proof differs")
    require(canonical_hash(read_json(root / "references/scope.json")) == lineage["plan_sha256"], "HCAI utilization stored scope differs")
    acq = receipt["acquisition"]
    require(acq["requested_url"] == entry["url"] and acq["http_status"] == 200, "HCAI utilization route differs")
    require(
        receipt["governance"]["contains_pii"] is False and receipt["governance"]["license_or_terms_url"] == TERMS_URL, "HCAI utilization governance differs"
    )
    period = receipt["measurement_periods"]
    year = entry["year"]
    require(len(period) == 1 and (period[0]["start_date"], period[0]["end_date"]) == (f"{year}-01-01", f"{year}-12-31"), "HCAI utilization period differs")
