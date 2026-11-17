"""Frozen scope, workbook reading and checks for the HCAI annual utilization workbooks, 2018-2025.

All the data is public. Under plan version 2 each workbook is stored whole under raw/ as published; its receipt records that it names
administrators and report preparers, whose cells bronze pseudonymizes at load (keyed, in place: the sheets have no pseudonym column).
The version 1 captures (one abstracted CSV per sheet, rebuilt from an original kept on the Mac) are retired.
Failure modes: data/acquisition_planning/hcai_util_2018_2025_failure_modes.md.
"""

import io
import re
import zipfile
from html.parser import HTMLParser
from pathlib import Path

from scripts.acquisition.bls_api_contract import digest
from scripts.acquisition.code_closure import closure_hashes
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.legacy_versions import bound_record, plan_matches, predecessor_plan
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require

PLAN_PATH = REPO_ROOT / "config/acquisition/hcai_util_2018_2025_plan.json"
VERSIONS_PATH = REPO_ROOT / "config/acquisition/hcai_util_code_versions.json"
CODE_ENTRY = "scripts.acquisition.store_hcai_util"
RELEASE_PATH = "config/acquisition/access_releases_20260929.json"
SOURCE_ID = "HCAI_UTIL"
TERMS_URL = "https://data.chhs.ca.gov/pages/terms"
PSEUDONYMIZE_AT_LOAD = "Names administrators and report preparers as published; bronze pseudonymizes the person cells at load."
# The restriction wording of the version 2 plan from before bronze pseudonymized the person cells: a capture bound to that plan carries it.
LEGACY_REDACT_AT_LOAD = "Names administrators and report preparers as published; bronze redacts the person cells at load ([REDACTED])."
INDIVIDUAL_OWNER = "Investor - Individual"
DATA_SHEETS = ["Page 1-6", "NonResp 1-6"]
LABEL_ROWS = 4  # Rows 2-5: database label, Page, Column and Line.
MIN_HOSPITALS = 400
MAX_BYTES = 64 * 1024**2
MAX_MEMBERS = 100


def collector_code_hashes(entry: str = CODE_ENTRY) -> dict[str, str]:
    """Bind the entry module of the collector and every repository module it can run, and no other file."""
    return closure_hashes(entry)


def require_code(hashes: dict | None = None) -> dict:
    """Require explicit review of new captures and retain trusted historical implementations."""
    actual = collector_code_hashes() if hashes is None else hashes
    require(bool(actual) and actual in [v["code_sha256"] for v in read_code_versions(VERSIONS_PATH)["versions"]], "Unreviewed HCAI utilization code version")
    return actual


def year_id(entry: dict) -> str:
    """Identify one year by its resource, URL and recorded original."""
    return canonical_hash({"year": entry["year"], "url": entry["url"], "sha256": entry["sha256"], "bytes": entry["bytes"]})


def stored_name(entry: dict) -> str:
    """The workbook's file name in the capture."""
    return f"hcai_util_{entry['year']}.xlsx"


def load_plan(path: Path | None = None) -> dict:
    """Verify the locked scope, per-year bindings, access release and base registry."""
    path = PLAN_PATH if path is None else path
    plan = read_json(path)
    require(read_json(path.with_suffix(".lock.json"))["plan_sha256"] == canonical_hash(plan), "HCAI utilization plan lock differs")
    require(plan["source_id"] == SOURCE_ID and plan["model_eligible"] is False and plan["version"] == 2, "HCAI utilization plan hold differs")
    require(not {"redact", "not_personal", "individual_owner_redact", "replacement"} & set(plan), "HCAI utilization plan rules differ")
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


def check_raw(raw: bytes, entry: dict) -> dict:
    """Check that a workbook is the published shape and count its hospitals; the bytes are stored unchanged."""
    book = read_workbook(raw)
    require(list(book) == entry["sheets"], "HCAI workbook sheets differ")
    hospital_counts: dict[str, int] = {}
    for sheet in DATA_SHEETS:
        rows = book[sheet]
        header = rows[0][: len(entry["header"])]
        require(header == entry["header"], "HCAI data header differs")
        index = {c: i for i, c in enumerate(header)}
        for number, row in enumerate(rows):
            require(all(v == "" for v in row[len(header) :]) or 1 <= number <= LABEL_ROWS, "HCAI cell beyond the header")
        hospital_counts[sheet] = sum(row[index["FAC_NO"]].strip() != "" for row in rows[LABEL_ROWS + 1 :])
    require(hospital_counts["Page 1-6"] >= entry["min_hospitals"], "HCAI data sheet has too few hospitals")
    return {"hospitals": hospital_counts, "preliminary": entry["preliminary"], "model_eligible": False}


def verify_capture(receipt: dict, source: dict, lineage: dict, root: Path, evidence_only: bool) -> None:
    """Check that the workbook is stored whole under raw/ as published, and check its artifact set, bindings, release and holds."""
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
    paths = {a["storage_path"] for a in receipt["artifacts"]}
    if paths == {f"raw/{stored_name(entry)}", "evidence/download_proof.json", "references/scope.json"}:
        # A capture bound to an archived predecessor plan is read as that plan: its year and its restriction wording are the predecessor's.
        captured_plan = predecessor_plan(plan, lineage["plan_sha256"]) or plan
        require(entry in captured_plan["years"], "HCAI utilization capture year not in its plan")
        restriction = LEGACY_REDACT_AT_LOAD if "bronze redacts the person cells" in captured_plan["scope"] else PSEUDONYMIZE_AT_LOAD
        require(not (root / "sheets").exists(), "HCAI utilization snapshot holds a derived copy")
        raw = (root / "raw" / stored_name(entry)).read_bytes()
        proof = read_json(root / "evidence/download_proof.json")
        require(
            digest(raw) == entry["sha256"] == lineage["expected_sha256"] == proof["sha256"] and len(raw) == entry["bytes"],
            "HCAI utilization workbook hash differs",
        )
        require(proof["statistics"] == check_raw(raw, entry) and proof["requested_url"] == entry["url"], "HCAI utilization download proof differs")
        require(canonical_hash(read_json(root / "references/scope.json")) == lineage["plan_sha256"], "HCAI utilization stored scope differs")
        acq = receipt["acquisition"]
        require(acq["requested_url"] == entry["url"] and acq["http_status"] == 200, "HCAI utilization route differs")
        governance = receipt["governance"]
        require(
            governance["access_class"] == "public"
            and governance["contains_pii"] is True
            and restriction in governance["use_restrictions"]
            and governance["license_or_terms_url"] == TERMS_URL,
            "HCAI utilization governance differs",
        )
    else:
        require(False, "HCAI utilization artifact set differs")
    period = receipt["measurement_periods"]
    year = entry["year"]
    require(len(period) == 1 and (period[0]["start_date"], period[0]["end_date"]) == (f"{year}-01-01", f"{year}-12-31"), "HCAI utilization period differs")
