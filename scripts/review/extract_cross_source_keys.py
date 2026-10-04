"""Extract hospital, county and ZIP join keys from every source that carries them, per source and period.

Receipts and artifact bytes are verified against the frozen inventory first. Delimited text is streamed row by
row; workbooks are read sheet by sheet; archives are opened in memory one level deep. Each key is normalized by
explicit rules (padding, float suffix, geography prefix) and every rule application is counted. The key sets hold
public identifiers only (CCN, county FIPS, ZIP); the report holds counts. Nothing is joined here: the companion
``analyze_cross_source_keys`` compares the sets.

Usage (the interpreter needs openpyxl, pypdf and xlrd)::

    PYTHONPATH=.review_dependencies BUNDLED_PYTHON -m scripts.review.extract_cross_source_keys --source-root SOURCE_ROOT \
        --inventory data/schema_review/2026_09_30/inputs/capture_inventory_effective.json --output keys_run1
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import re
import sys
import zipfile
from collections import Counter, defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

import openpyxl
import xlrd
from xlrd.biffh import XLRDError

from scripts.review.inspect_hai_archives import SOURCE as HAI_SOURCE
from scripts.review.inspect_hai_archives import Review, column_roles, load_table, review_capture, row_period
from scripts.review.inspect_hai_archives import read_table as read_hai_table
from scripts.review.inspect_members import cell_text
from scripts.review.review_remaining import digest, safe_path, write_json

log = logging.getLogger("extract_cross_source_keys")
MEMBER_LIMIT = 1 << 30
HEADER_SCAN = 120
YEAR = re.compile(r"(?<!\d)((?:19|20)\d\d)(?!\d)")
MONTHS = "jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec"
CCN = re.compile(r"^[0-9A-Z]{6}$")
NESTED = "<nested archive not opened>"
NUMBER_TOKEN = re.compile(r"^-?\d+(?:\.\d+)?$")


def normalize(name: str) -> str:
    """Lower-case a header and collapse punctuation, so ``#GEO_ID`` and ``''ZIP_CODE''`` compare as names."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


@dataclass(frozen=True)
class Key:
    """One key of a source: its kind, the header names that carry it, or a state and county pair to combine."""

    kind: str
    columns: tuple[str, ...] = ()
    compose: tuple[str, str] | None = None


@dataclass(frozen=True)
class Spec:
    """How keys, periods and row subsets are read from one source."""

    keys: tuple[Key, ...]
    pairs: tuple[tuple[str, str], ...] = ()
    year_columns: tuple[str, ...] = ()
    name_rules: tuple[str, ...] = ()
    period_rule: str | None = None
    row_filter: tuple[str, frozenset[str]] | None = None
    subset: tuple[str, frozenset[str], str] | None = None
    headerless: tuple[int, str, str] | None = None
    fixed_width: bool = False
    skip_names: str | None = None
    roles: frozenset[str] = frozenset({"data"})
    table_subset: tuple[str, str] | None = None
    notes: tuple[str, ...] = field(default=())


FY = (r"(?:fy|FY)[-_ ]?((?:19|20)\d\d)", r"(?:fy|FY)[-_ ]?(\d\d)(?!\d)")
SPECS: dict[str, Spec] = {
    "CMS_HCRIS_PUF": Spec((Key("hospital", ("provider_ccn",)), Key("zip", ("zip_code",))), pairs=(("hospital", "zip"),)),
    "CMS_POS": Spec(
        (Key("hospital", ("prvdr_num",)), Key("zip", ("zip_cd",)), Key("county", compose=("fips_state_cd", "fips_cnty_cd"))),
        pairs=(("hospital", "county"), ("hospital", "zip")),
        name_rules=(rf"(?:{MONTHS})[a-z]*[_.-]?(\d\d)(?!\d)",),
        subset=("prvdr_ctgry_cd", frozenset({"01", "1"}), "hospital_category_01"),
    ),
    "main-cmi-ipps": Spec(
        (Key("hospital", ("provider_no", "provider_number", "provider_id", "prov", "provider")),),
        name_rules=(*FY, r"(?:fn|pn|fr)(\d\d)(?!\d)", r"cmip(\d\d)(?!\d)"),
        headerless=(0, "hospital", r"^(?:[0-9A-Z]{6}|\d{5})$"),
        table_subset=(r"cmi", "cmi_tables"),
    ),
    "CMS_IPPS": Spec(
        (Key("hospital", ("provider_number", "prov", "provider_no")), Key("county", ("fips_county_code",))),
        pairs=(("hospital", "county"),),
        name_rules=(*FY, r"pufr?(\d\d)(?!\d)", r"imppuf(\d\d)(?!\d)", r"pubfil(\d\d)(?!\d)", r"imp(?:fil|ctf)(\d\d)(?!\d)", r"(?:fr|nprm)(\d\d)(?!\d)"),
    ),
    "CMS_OCCMIX": Spec((Key("hospital", ("prov",)),), name_rules=FY),
    "CMS_MEDICARE_PROVIDER": Spec((Key("hospital", ("rndrng_prvdr_ccn",)), Key("zip", ("rndrng_prvdr_zip5",))), name_rules=(r"DY(\d\d)(?!\d)",)),
    "CMS-MUP-DRG": Spec((Key("hospital", ("rndrng_prvdr_ccn",)), Key("zip", ("rndrng_prvdr_zip5",))), name_rules=(r"DY(\d\d)(?!\d)",)),
    "ENROLL": Spec((Key("hospital", ("ccn",)), Key("zip", ("zip_code",)))),
    "CMS_CHOW": Spec((Key("hospital", ("ccn_buyer", "ccn_seller")),)),
    "CMS_OWNERS": Spec((Key("hospital", ("ccn",)), Key("zip", ("zip_code",)))),
    "ONC_PI": Spec((Key("hospital", ("ccn", "facility_id")), Key("zip", ("zip", "zip_code")))),
    "HHS": Spec(
        (Key("hospital", ("ccn",)), Key("zip", ("zip",)), Key("county", ("fips_code",))),
        pairs=(("hospital", "county"), ("hospital", "zip")),
        year_columns=("collection_week",),
    ),
    "HSA": Spec((Key("hospital", ("medicare_prov_num",)), Key("zip", ("zip_cd_of_residence",)))),
    "HUD": Spec((Key("zip", ("zip",)), Key("county", ("county", "geoid"))), pairs=(("zip", "county"),), period_rule=r"_(\d\d)((?:19|20)\d\d)(?!\d)"),
    "RUCA": Spec((Key("zip", ("zip_code", "zipcode", "zip_code_2010", "zipa")), Key("county", ("countyfips20", "state_county_fips_code", "fipsst_cnty")))),
    "RUCC": Spec((Key("county", ("fips", "fips_code", "fips_codes")),)),
    "ADJ": Spec((Key("county", ("county_geoid",)),), headerless=(1, "county", r"^\d{5}$")),
    "MMD": Spec((Key("county", ("fips",)),), year_columns=("year",), row_filter=("geography", frozenset({"County"}))),
    "BLS": Spec((Key("county", ("county_fips",)),), year_columns=("year",)),
    "WONDER": Spec((Key("county", ("county_code",)),), year_columns=("year_code", "year")),
    "PLACES": Spec((Key("county", ("locationid",)),), year_columns=("year",)),
    "SVI": Spec((Key("county", ("fips", "stcnty", "stcofips")),)),
    "SAIPE": Spec((Key("county"),), name_rules=(r"est(\d\d)(?!\d)",), fixed_width=True),
    "SAHIE": Spec((Key("county", compose=("statefips", "countyfips")),), year_columns=("year",)),
    "ACS": Spec(
        (Key("county", compose=("state", "county")), Key("county", ("geo_id", "county"))),
        year_columns=("year",),
        name_rules=(r"acsdt\dy((?:19|20)\d\d)",),
        skip_names=r"^\d{5}[a-z]{2}\d{7}\.zip$",
        notes=("2009 summary-file sequence archives are headerless and out of scope; the 2009 county universe comes from the 2009 profiles.",),
    ),
    "HPSA": Spec(
        (Key("county", ("common_state_county_fips_code", "state_and_county_federal_information_processing_standard_code")),),
        roles=frozenset({"data", "dictionary"}),
        skip_names=r".*METADATA",
        notes=("The detail CSV is labeled as a dictionary in its receipt; it is read as data here.", "XXXXX marks rows without a county code."),
    ),
    "MUA": Spec(
        (Key("county", ("state_and_county_federal_information_processing_standard_code",)),),
        roles=frozenset({"data", "dictionary"}),
        skip_names=r".*METADATA",
        notes=("The detail CSV is labeled as a dictionary in its receipt; it is read as data here.", "XXXXX marks rows without a county code."),
    ),
    "CMS_GV": Spec((Key("county", ("bene_geo_cd",)),), year_columns=("year",), row_filter=("bene_geo_lvl", frozenset({"County"}))),
}
OUT_OF_SCOPE = {
    "CA": "own facility numbers (HCAI); no CCN",
    "HCAI_FINANCE": "own facility numbers (HCAI); no CCN",
    "HCAI_UTIL": "own facility numbers (HCAI); no CCN",
    "IL": "state report card; own identifiers",
    "TX": "state staffing study PDFs",
    "VT": "state staffing and budget PDFs",
    "MA": "state staffing document",
    "MARY": "state staffing documents",
    "MISS": "state association documents",
    "IROQ": "regional association documents",
    "CHIA": "state documents",
    "NY": "state report PDF",
    "NSI": "national report PDF",
    "HRSA": "survey documentation",
    "PSI13": "measure documentation PDF",
    "PSI90": "measure documentation PDF",
    "CCW-ALGORITHMS": "reference document",
    "CMS-MUP-DICTIONARIES": "reference documents",
}


class Normalizer:
    """Canonical key forms with a count of every rule applied, per source."""

    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()

    def _base(self, value: str) -> str:
        text = value.strip().strip("'\"").strip()
        if re.fullmatch(r"\d+\.0", text):
            self.counts["float_suffix_stripped"] += 1
            text = text[:-2]
        return text

    def county(self, value: str) -> str | None:
        text = self._base(value).upper()
        if not text:
            return None
        if "US" in text:
            level, _, rest = text.partition("US")
            if not level.startswith("050"):
                self.counts["county_other_geography_level_dropped"] += 1
                return None
            self.counts["county_prefix_parsed"] += 1
            text = rest
        if not text.isdigit():
            self.counts["county_invalid"] += 1
            return "INVALID"
        if len(text) == 4:
            self.counts["county_padded_4_to_5"] += 1
            text = "0" + text
        if len(text) != 5:
            self.counts["county_invalid"] += 1
            return "INVALID"
        return text

    def compose(self, state: str, county: str) -> str | None:
        state, county = self._base(state), self._base(county)
        if not state and not county:
            return None
        if not (state.isdigit() and county.isdigit() and len(state) <= 2 and len(county) <= 3):
            self.counts["county_invalid"] += 1
            return "INVALID"
        self.counts["county_composed"] += 1
        return state.zfill(2) + county.zfill(3)

    def hospital(self, value: str) -> str | None:
        text = self._base(value).upper()
        if not text:
            return None
        if text.isdigit() and len(text) == 5:
            self.counts["hospital_padded_5_to_6"] += 1
            text = "0" + text
        if not CCN.match(text):
            self.counts["hospital_invalid"] += 1
            return "INVALID"
        return text

    def zip(self, value: str) -> str | None:
        text = self._base(value)
        if not text:
            return None
        if re.fullmatch(r"\d{5}-", text):
            self.counts["zip_trailing_hyphen_removed"] += 1
            text = text[:5]
        elif re.fullmatch(r"\d{5}-?\d{4}", text):
            self.counts["zip_plus4_truncated"] += 1
            text = text[:5]
        if text.isdigit() and len(text) in (3, 4):
            self.counts["zip_padded_to_5"] += 1
            text = text.zfill(5)
        if not re.fullmatch(r"\d{5}", text):
            self.counts["zip_invalid"] += 1
            return "INVALID"
        return text


class Collector:
    """Key sets per source, kind, subset and period, plus table-level evidence."""

    def __init__(self) -> None:
        self.sets: dict[tuple[str, str, str, str], set[str]] = defaultdict(set)
        self.tables: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.normalizers: dict[str, Normalizer] = defaultdict(Normalizer)
        self.period_basis: dict[str, Counter[str]] = defaultdict(Counter)

    def add(self, source: str, kind: str, subset: str, period: str, value: str) -> None:
        self.sets[(source, kind, subset, period)].add(value)


def year_from(text: str, rules: tuple[str, ...]) -> str | None:
    """First configured rule (then any four-digit year) that matches; two-digit years become 19xx or 20xx.

    Hexadecimal capture hashes in stored names (``ACS_history_2f5dc77692d1c...``) are removed first, so their digits
    are never read as a year.
    """
    text = re.sub(r"[0-9a-f]{12,}", " ", text)
    for rule in (*rules, YEAR.pattern):
        match = re.search(rule, text, flags=re.IGNORECASE)
        if match:
            year = match.group(1)
            return year if len(year) == 4 else ("19" if int(year) > 70 else "20") + year
    return None


def row_year(value: str) -> str | None:
    """Year of a row's period cell (``2024``, ``2024 ``, ``2021/03/05``, ``2021-03-05T00:00``)."""
    match = YEAR.search(value)
    return match.group(1) if match else None


def encoding_of(head: bytes) -> str:
    """UTF-16 by byte-order mark, UTF-8 when the first bytes decode (a character cut at the end is allowed), else CP1252."""
    if head.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    try:
        head.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        return "utf-8-sig" if error.start > len(head) - 4 else "cp1252"
    return "utf-8-sig"


def text_rows(handle: Any, head: bytes, fixed_width: bool) -> Iterator[list[str]]:
    """Rows of a delimited or fixed-width text stream; undecodable bytes become replacement characters."""
    encoding = encoding_of(head)
    stream = io.TextIOWrapper(handle, encoding=encoding, errors="replace", newline="")
    if fixed_width:
        for line in stream:
            yield [line.rstrip("\r\n")]
        return
    sample = head.decode(encoding, errors="replace")
    try:
        delimiter = csv.Sniffer().sniff(sample[:65536], delimiters=",\t|;").delimiter
    except csv.Error:
        delimiter = ","
    yield from csv.reader(stream, delimiter=delimiter)


def workbook_tables(data: bytes, suffix: str) -> Iterator[tuple[str, Iterator[list[str]]]]:
    """Sheets of a workbook as row iterators of cell text."""
    if data[:8] == bytes.fromhex("d0cf11e0a1b11ae1"):
        book = xlrd.open_workbook(file_contents=data, on_demand=True)
        try:
            for index in range(book.nsheets):
                sheet = book.sheet_by_index(index)
                yield sheet.name, ([cell_text(sheet.cell_value(r, c)) for c in range(sheet.ncols)] for r in range(sheet.nrows))
                book.unload_sheet(index)
        finally:
            book.release_resources()
        return
    workbook = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True, keep_links=False)
    try:
        for worksheet in workbook.worksheets:
            yield worksheet.title, ([cell_text(v) for v in row] for row in worksheet.iter_rows(values_only=True))
    finally:
        workbook.close()


def wanted_columns(spec: Spec) -> set[str]:
    """Every header name that identifies a key-bearing header row."""
    names = {c for key in spec.keys for c in key.columns}
    names |= {c for key in spec.keys if key.compose for c in key.compose}
    return names


def resolve(spec: Spec, header: list[str]) -> dict[str, Any] | None:
    """Column positions of the spec's keys, period, filter and subset in one header row; None when no key matches."""
    positions: dict[str, int] = {}
    for index, name in enumerate(header):
        positions.setdefault(normalize(name), index)
    keys: dict[str, tuple[int, ...]] = {}
    for key in spec.keys:
        if key.kind in keys:
            continue
        if key.compose and all(part in positions for part in key.compose):
            keys[key.kind] = (positions[key.compose[0]], positions[key.compose[1]])
        else:
            found = next((positions[c] for c in key.columns if c in positions), None)
            if found is not None:
                keys[key.kind] = (found,)
    if not keys:
        return None
    return {
        "keys": keys,
        "year": next((positions[c] for c in spec.year_columns if c in positions), None),
        "filter": positions.get(spec.row_filter[0]) if spec.row_filter else None,
        "subset": positions.get(spec.subset[0]) if spec.subset else None,
        "columns": {kind: [header[i] for i in index] for kind, index in keys.items()},
    }


def collect_table(source: str, spec: Spec, label: str, rows: Iterator[list[str]], period: str, collector: Collector) -> dict[str, Any]:
    """Collect keys from one table; the header is the first scanned row that names a configured key column."""
    normalizer = collector.normalizers[source]
    evidence: dict[str, Any] = {"table": label, "period_from_name": period}
    buffered: list[list[str]] = []
    layout = None
    headerless = False
    split_whitespace = False
    wanted = wanted_columns(spec)
    for row in rows:
        if row == [NESTED]:
            return {**evidence, "status": "nested_archive_not_opened"}
        buffered.append(row)
        if spec.fixed_width:
            break
        if any(normalize(cell) in wanted for cell in row):
            layout = resolve(spec, row)
            buffered = []
            break
        if len(buffered) >= HEADER_SCAN:
            break
    if spec.fixed_width:
        layout = {"keys": {"county": ()}, "year": None, "filter": None, "subset": None, "columns": {"county": ["fixed positions 1-2 and 4-6"]}}
    elif layout is None and spec.headerless is not None:
        column, kind, pattern = spec.headerless
        # Space-aligned text sniffs as a single column; its fields are then separated by whitespace.
        split_whitespace = bool(buffered) and all(len(r) <= 1 for r in buffered[:50])
        candidates = [r[0].split() if split_whitespace and r else r for r in buffered[:50]]
        sample = [r[column].strip() for r in candidates if len(r) > column and r[column].strip()]
        if sample and sum(bool(re.match(pattern, v)) for v in sample) >= 0.9 * len(sample):
            layout = {"keys": {kind: (column,)}, "year": None, "filter": None, "subset": None, "columns": {kind: [f"position {column}"]}}
            headerless = True
    if layout is None:
        # Drain the rest so a table without keys is still counted.
        rows_seen = len(buffered) + sum(1 for _ in rows)
        return {**evidence, "status": "no_key_columns", "rows": rows_seen}
    evidence.update({"status": "headerless_keys" if headerless else "keys_read", "key_columns": layout["columns"]})
    counts: Counter[str] = Counter()
    years: Counter[str] = Counter()
    rows_iter = iter(buffered) if (headerless or spec.fixed_width) else iter(())
    table_subset = spec.table_subset[1] if spec.table_subset and re.search(spec.table_subset[0], label, flags=re.IGNORECASE) else None
    for row in _chain(rows_iter, rows):
        if split_whitespace:
            row = row[0].split() if row else row
        if not any(cell.strip() for cell in row):
            continue
        counts["rows"] += 1
        if (
            layout["filter"] is not None
            and spec.row_filter is not None
            and (layout["filter"] >= len(row) or row[layout["filter"]].strip() not in spec.row_filter[1])
        ):
            counts["rows_filtered_out"] += 1
            continue
        year = period
        if layout["year"] is not None and layout["year"] < len(row):
            year = row_year(row[layout["year"]]) or "unknown"
        years[year] += 1
        subsets = ["all", table_subset] if table_subset else ["all"]
        if layout["subset"] is not None and spec.subset is not None and layout["subset"] < len(row) and row[layout["subset"]].strip() in spec.subset[1]:
            subsets.append(spec.subset[2])
        values: dict[str, str] = {}
        for kind, index in layout["keys"].items():
            if spec.fixed_width:
                line = row[0]
                if len(line) < 6:
                    counts["fixed_width_short_line"] += 1
                    continue
                value = normalizer.compose(line[0:2], line[3:6])
            elif any(i >= len(row) for i in index):
                counts[f"{kind}_short_row"] += 1
                continue
            elif len(index) == 2:
                value = normalizer.compose(row[index[0]], row[index[1]])
            else:
                value = getattr(normalizer, kind)(row[index[0]])
            if value is None:
                counts[f"{kind}_blank"] += 1
                continue
            if value == "INVALID":
                counts[f"{kind}_invalid"] += 1
                continue
            values[kind] = value
            for subset in subsets:
                collector.add(source, kind, subset, year, value)
        for left, right in spec.pairs:
            if left in values and right in values:
                for subset in subsets:
                    collector.add(source, f"{left}|{right}", subset, year, f"{values[left]}|{values[right]}")
    evidence.update({"counts": dict(sorted(counts.items())), "periods": dict(sorted(years.items()))})
    return evidence


def _chain(first: Iterator[list[str]], second: Iterator[list[str]]) -> Iterator[list[str]]:
    yield from first
    yield from second


def artifact_tables(name: str, opener: Any, size: int, spec: Spec, depth: int = 0) -> Iterator[tuple[str, Iterator[list[str]]]]:
    """Tables inside one file or archive member, dispatched by content signature and suffix."""
    suffix = PurePosixPath(name).suffix.lower()
    with opener() as handle:
        head = handle.read(65536)
    if head.startswith(b"%PDF-") or suffix in {".pdf", ".json", ".docx", ".doc", ".html", ".xml"}:
        return
    if head.startswith(b"PK") and suffix == ".zip":
        if depth > 1 or size > MEMBER_LIMIT:
            yield name, iter([[NESTED]])
            return
        with opener() as handle:
            data = handle.read()
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            for info in sorted((i for i in archive.infolist() if not i.is_dir()), key=lambda i: i.filename):
                path = PurePosixPath(info.filename)
                if "__MACOSX" in path.parts or path.name.startswith("._") or path.is_absolute() or ".." in path.parts:
                    continue
                if info.file_size > MEMBER_LIMIT:
                    yield f"{name}!{info.filename}", iter([["<member over size limit>"]])
                    continue

                def member_opener(info: zipfile.ZipInfo = info, archive: zipfile.ZipFile = archive) -> Any:
                    return archive.open(info)

                yield from ((f"{name}!{label}", rows) for label, rows in artifact_tables(info.filename, member_opener, info.file_size, spec, depth + 1))
        return
    if head[:8] == bytes.fromhex("d0cf11e0a1b11ae1") or (head.startswith(b"PK") and suffix in {".xlsx", ".xlsm"}):
        with opener() as handle:
            data = handle.read()
        for sheet, rows in workbook_tables(data, suffix):
            yield f"{name}#{sheet}", rows
        return
    handle = opener()
    try:
        yield name, text_rows(handle, head, spec.fixed_width)
    finally:
        handle.close()


def extract_source(root: Path, source: str, spec: Spec, candidates: list[dict[str, Any]], collector: Collector) -> dict[str, Any]:
    """Verify and read every data artifact of one source; identical bytes are read once."""
    statuses: Counter[str] = Counter()
    seen: set[str] = set()
    for candidate in candidates:
        receipt_path = safe_path(root, candidate["receipt"])
        if not receipt_path.is_file() or digest(receipt_path) != candidate["receipt_sha256"]:
            statuses["receipt_drift"] += 1
            continue
        receipt = json.loads(receipt_path.read_text())
        acquisition = receipt.get("acquisition") or {}
        url = str(acquisition.get("requested_url") or "")
        measured = [p.get("end_date") for p in receipt.get("measurement_periods") or [] if isinstance(p, dict) and p.get("end_date")]
        params = acquisition.get("request_parameters") or {}
        for artifact in (a for a in receipt["artifacts"] if a["role"] in spec.roles):
            if artifact["sha256"] in seen:
                statuses["duplicate_bytes_read_once"] += 1
                continue
            name = artifact["stored_file_name"]
            if spec.skip_names and re.match(spec.skip_names, name):
                statuses["out_of_scope_by_name"] += 1
                continue
            path = receipt_path.parent / artifact["storage_path"]
            if not path.is_file():
                statuses["artifact_not_local"] += 1
                continue
            if path.stat().st_size != artifact["byte_count"] or digest(path) != artifact["sha256"]:
                statuses["artifact_mismatch"] += 1
                continue
            seen.add(artifact["sha256"])
            statuses["artifact_read"] += 1
            for label, rows in artifact_tables(name, lambda path=path: path.open("rb"), artifact["byte_count"], spec):
                member = label.rsplit("!", 1)[-1]
                if spec.period_rule and (match := re.search(spec.period_rule, member)):
                    period, basis = f"{match.group(2)}-{match.group(1)}", "name_period_rule"
                elif spec.period_rule and len(measured) == 1 and re.match(r"\d{4}-\d\d", str(measured[0])):
                    period, basis = str(measured[0])[:7], "receipt_measurement_period"
                elif spec.period_rule and str(params.get("year", "")).isdigit() and str(params.get("quarter", "")).isdigit():
                    period, basis = f"{params['year']}-{int(params['quarter']) * 3:02d}", "request_parameters"
                elif (year := year_from(name, spec.name_rules)) or (year := year_from(member, spec.name_rules)):
                    period, basis = year, "name"
                elif year := year_from(url, (r"year=((?:19|20)\d\d)", r"/data/((?:19|20)\d\d)/")):
                    period, basis = year, "request_url"
                else:
                    period, basis = "unknown", "none"
                collector.period_basis[source][basis] += 1
                try:
                    collector.tables[source].append({"artifact_sha256": artifact["sha256"], **collect_table(source, spec, label, rows, period, collector)})
                except (csv.Error, zipfile.BadZipFile, ValueError, KeyError, OSError, EOFError, XLRDError) as error:
                    collector.tables[source].append({"artifact_sha256": artifact["sha256"], "table": label, "status": f"read_failed:{type(error).__name__}"})
    return dict(sorted(statuses.items()))


def extract_hai(root: Path, candidates: list[dict[str, Any]], collector: Collector) -> dict[str, Any]:
    """Facility and ZIP keys of every distinct HAI hospital table, by measurement window (start/end dates)."""
    review = Review()
    statuses = Counter(review_capture(root, c, review)["review_status"] for c in candidates)
    normalizer = collector.normalizers[HAI_SOURCE]
    for table_sha, table in sorted(review.tables.items()):
        if table.get("class") != "hai_hospital":
            continue
        _, header, rows = read_hai_table(load_table(review.table_locators[table_sha]))
        roles = column_roles(header)
        zip_index = next((i for i, h in enumerate(header) if normalize(h) == "zip_code"), None)
        counts: Counter[str] = Counter()
        periods: Counter[str] = Counter()
        for row in rows:
            if len(row) != len(header):
                counts["ragged_rows"] += 1
                continue
            # HAI periods are measurement windows; rolling windows that end in the same year stay separate.
            year = row_period(row, roles)
            periods[year] += 1
            ccn = normalizer.hospital(row[roles["facility"]])
            if ccn in (None, "INVALID"):
                counts["hospital_invalid_or_blank"] += 1
                continue
            collector.add(HAI_SOURCE, "hospital", "listed", year, ccn)
            measure = row[roles["measure_id"]].strip().replace("-", "_").upper()
            if measure.endswith("SIR") and NUMBER_TOKEN.match(row[roles["score"]].strip()):
                collector.add(HAI_SOURCE, "hospital", "numeric_sir", year, ccn)
            if zip_index is not None:
                code = normalizer.zip(row[zip_index])
                if code not in (None, "INVALID"):
                    collector.add(HAI_SOURCE, "zip", "listed", year, code)
                    collector.add(HAI_SOURCE, "hospital|zip", "listed", year, f"{ccn}|{code}")
        collector.tables[HAI_SOURCE].append(
            {
                "table_sha256": table_sha,
                "releases": sorted(review.table_releases[table_sha]),
                "counts": dict(sorted(counts.items())),
                "periods": dict(sorted(periods.items())),
            }
        )
    return dict(sorted(statuses.items()))


def table_order(table: dict[str, Any]) -> tuple[str, str]:
    """Deterministic order of table evidence."""
    return (str(table.get("table", "")), str(table.get("artifact_sha256", table.get("table_sha256", ""))))


def build(root: Path, inventory_path: Path, include_hai: bool = True) -> tuple[dict[str, Any], dict[str, Any]]:
    """Extract every configured source and return the report and the key sets."""
    candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in json.loads(inventory_path.read_text())["candidates"]:
        if candidate["status"] == "candidate_not_authorized_for_execution":
            candidates[candidate["source_id"]].append(candidate)
    for group in candidates.values():
        group.sort(key=lambda c: c["snapshot_id"])
    collector = Collector()
    statuses: dict[str, dict[str, Any]] = {}
    if include_hai:
        statuses[HAI_SOURCE] = extract_hai(root, candidates[HAI_SOURCE], collector)
        log.info("%s done", HAI_SOURCE)
    for source, spec in sorted(SPECS.items()):
        statuses[source] = extract_source(root, source, spec, candidates[source], collector)
        log.info("%s done: %s", source, statuses[source])
    keysets: dict[str, Any] = {}
    summary: dict[str, Any] = defaultdict(lambda: defaultdict(dict))
    for (source, kind, subset, period), values in sorted(collector.sets.items()):
        keysets.setdefault(source, {}).setdefault(kind, {}).setdefault(subset, {})[period] = sorted(values)
        summary[source][f"{kind}:{subset}"][period] = len(values)
    report: dict[str, Any] = {
        "version": 1,
        "inventory_sha256": digest(inventory_path),
        "code_sha256": digest(Path(__file__)),
        "sources": {
            source: {
                "artifact_status": statuses[source],
                "tables": sorted(collector.tables[source], key=table_order),
                "normalization_counts": dict(sorted(collector.normalizers[source].counts.items())),
                "period_basis": dict(sorted(collector.period_basis[source].items())),
                "distinct_keys": {k: dict(sorted(v.items())) for k, v in sorted(summary[source].items())},
                "notes": list(SPECS[source].notes) if source in SPECS else [],
            }
            for source in sorted(statuses)
        },
        "out_of_scope": dict(sorted(OUT_OF_SCOPE.items())),
        "model_eligible": False,
        "limits": [
            "Offline local bytes only; no S3 or publisher requests.",
            "Key sets hold public identifiers only; they prove identifier overlap, not that records describe the same entity-period.",
            "A table whose header does not name a configured key column is counted as no_key_columns and not read further.",
            "No hold is cleared and nothing is joined.",
        ],
    }
    keysets_doc = {"version": 1, "inventory_sha256": report["inventory_sha256"], "code_sha256": report["code_sha256"], "keysets": keysets}
    return report, keysets_doc


def main() -> int:
    """Run the extraction from its real CLI."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--only", action="append", default=[], help="restrict to these source IDs (for synthetic checks)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    csv.field_size_limit(64 << 20)
    if args.only:
        for source in list(SPECS):
            if source not in args.only:
                del SPECS[source]
    report, keysets = build(args.source_root.resolve(), args.inventory.resolve(), include_hai=not args.only or HAI_SOURCE in args.only)
    write_json(args.output / "report.json", report)
    write_json(args.output / "keysets.json", keysets)
    sys.stdout.write(json.dumps({s: v["artifact_status"] for s, v in report["sources"].items()}) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
