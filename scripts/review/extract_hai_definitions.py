"""Record the publisher's HAI measure definitions from every data dictionary and test them against the hospital tables.

Every distinct PDF inside the ``main-hai-pdc`` archives (one nesting level) is read once. For each edition the HAI
passages are cut out by anchor text, page headers are removed and identical passages are grouped across editions.
The hospital infection tables are then read to check that the published SIR equals observed over predicted cases
and to pool observed and predicted counts per measurement window. Publisher documentation is public; the output
holds dictionary text, counts and sums, never facility rows.

Usage (the interpreter needs pypdf)::

    BUNDLED_PYTHON -m scripts.review.extract_hai_definitions --source-root SOURCE_ROOT \
        --inventory data/schema_review/2026_09_30/inputs/capture_inventory_effective.json --output definitions.json
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from pypdf import PdfReader

from scripts.review.extract_dictionary_passages import EDITION, pdf_members
from scripts.review.inspect_hai_archives import SOURCE, Review, column_roles, load_table, measure_family, read_table, review_capture, row_period, sha256
from scripts.review.review_remaining import digest, safe_path, write_json

log = logging.getLogger("extract_hai_definitions")
PAGE_HEADER = re.compile(r"Downloadable Database Dictionary \w+ \d{4} Page \d+ of \d+")
TABLES = {"hospital_table": "Hospital", "state_table": "State", "national_table": "National"}
TABLE_END = re.compile(r" (?:Table (?:\((?:Back to )?File Summary\) )?[A-Z]|Appendix [A-Z] )")
SECTIONS: dict[str, tuple[tuple[str, ...], str, int]] = {
    "description": ((r"Healthcare-Associated Infections \(HAI\) Measures Description/ ?Background",), r"Refreshed (?:quarterly|annually)\.", 4000),
    "measure_directory": ((r"Measure ID Measure Name HAI-1 ",), r"(?:HVBP Measures Directory|File Name Measure|\.csv Measure ID)", 2500),
    "numerator_denominator": ((r"HAI Definition Numerator",), r"(?:Appendix [A-Z] |CCN ASC CCN)", 400),
    "facility_identifier": ((r"Facility ID \(CCN for non ASC facilities\)",), r"(?:Appendix [A-Z] |ZIP Code )", 700),
}
BROKEN_REFERENCE = "Error! Not a valid filename."
FOOTNOTE_START = re.compile(r"(?<![\d.])1 The number of cases/patients is too few to report\.")
FOOTNOTE_CODES = ("3", "4", "5", "8", "11", "12", "13", "19", "28", "29")
FOOTNOTE_LAST = 40
FOOTNOTE_END = re.compile(r"Maryland data fo+t?notes|Appendix [A-Z] [\u2013-]|\b\w+ 20\d\d Release\b")
FOOTNOTE_PAGE_HEADER = re.compile(r"(?:Public Reporting|Hospital Compare) Footnote Values # Text Definition")
DIRECTORY_ENTRY = re.compile(r"(HAI-\d) (.+?) \(alternate Measure ID: (HAI_\d_SIR)\)")
LAYOUT_TOKEN = re.compile(r"(Char\(\d+\)|Num\(\d+\)|Date\(0\))\s+(.+?)(?=\s+(?:Char\(\d+\)|Num\(\d+\)|Date\(0\))|$)")
COMPONENTS = {
    "SIR": "SIR",
    "NUMERATOR": "observed",
    "ELIGCASES": "predicted",
    "CILOWER": "ci_lower",
    "CIUPPER": "ci_upper",
    "DOPC": "exposure",
    "DOPCDAYS": "exposure",
}
NUMBER = re.compile(r"^-?\d+(?:\.\d+)?$")


def clean(text: str) -> str:
    """Remove running page headers and normalize typographic quotes and spacing."""
    text = PAGE_HEADER.sub(" ", " ".join(text.split())).replace("’", "'").replace("“", '"').replace("”", '"')
    text = re.sub("[\ue000-\uf8ff]", "•", text)  # private-use bullet glyphs become the ordinary bullet
    return " ".join(text.split())


def cut(text: str, starts: tuple[str, ...], end: str, cap: int) -> dict[str, Any]:
    """The passage from the first start anchor outside the table of contents up to its end anchor.

    The description keeps its closing reporting-cycle sentence; every other passage stops before the end anchor.
    """
    hits = [m for pattern in starts for m in re.finditer(pattern, text) if "....." not in text[m.start() : m.start() + 200]]
    if not hits:
        return {"status": "section_absent"}
    start = hits[0].start()
    stop = re.search(end, text[start + 10 : start + cap + 10])
    if stop is None:
        return {"status": "capped", "text": text[start : start + cap].strip(), "anchor_matches": len(hits)}
    close = stop.end() if end.startswith("Refreshed") else stop.start()
    return {"status": "found", "text": text[start : start + 10 + close].strip(), "anchor_matches": len(hits)}


def cut_table(text: str, label: str, cap: int = 3000) -> dict[str, Any]:
    """One table layout, joined across page breaks: repeated continuation headers of the same table are removed."""
    first = re.search(rf"HAI \({label}\) Description", text)
    if first is None:
        return {"status": "section_absent"}
    window = text[first.start() : first.start() + cap]
    marker = "Data Type Column Name - CSV"
    head_end = window.find(marker)
    if head_end < 0:
        return {"status": "capped", "text": window}
    head_end += len(marker)
    continuation = re.compile(rf"Table (?:\((?:Back to )?File Summary\) )?HAI \({label}\) Description .{{0,200}}? {marker}")
    body = continuation.sub(" ", window[head_end:])
    stop = TABLE_END.search(body)
    passage = window[:head_end] + (body if stop is None else body[: stop.start()])
    return {"status": "capped" if stop is None else "found", "text": " ".join(passage.split()), "columns": layout_columns(passage[head_end:])}


def layout_columns(body: str) -> list[str]:
    """Data type and column name pairs of a table layout body."""
    body = re.sub(r"\bDate (?=(?:Measure )?(?:Start|End) Date\b)", "Date(0) ", body)
    columns = []
    for kind, name in LAYOUT_TOKEN.findall(" ".join(body.split())):
        # The last column of a layout absorbs the heading that follows it; date columns end at their own name.
        date_name = re.match(r"(?:Measure )?(?:Start|End) Date(?= |$)", name)
        name = date_name.group(0) if date_name else name
        columns.append(f"{kind} {name.strip()}")
    return columns


def footnotes(text: str) -> dict[str, Any]:
    """Definitions of the footnote codes that occur in the HAI hospital tables, from the footnote appendix."""
    starts = [m.start() for m in FOOTNOTE_START.finditer(text)]
    if not starts:
        return {"status": "section_absent"}
    body = text[starts[-1] :]
    end = FOOTNOTE_END.search(body)
    body = " ".join(FOOTNOTE_PAGE_HEADER.sub(" ", body if end is None else body[: end.start()]).split())
    positions: dict[int, int] = {1: 0}
    cursor = 0
    for number in range(2, FOOTNOTE_LAST + 1):
        match = re.search(rf"(?<![\d.(]){number} (?=[A-Z])", body[cursor:])
        if match is None:
            break
        cursor += match.start()
        positions[number] = cursor
    ordered = sorted(positions.items())
    items = {str(n): body[p : ordered[i + 1][1] if i + 1 < len(ordered) else p + 1500].strip() for i, (n, p) in enumerate(ordered)}
    return {"status": "found", "codes_parsed": len(items), "items": {code: items[code] for code in FOOTNOTE_CODES if code in items}}


def edition_sections(pages: list[str]) -> dict[str, Any]:
    """Every configured section of one edition, plus the parsed layouts and directory."""
    text = clean(" ".join(pages))
    result: dict[str, Any] = {name: cut(text, starts, end, cap) for name, (starts, end, cap) in SECTIONS.items()}
    result.update({name: cut_table(text, label) for name, label in TABLES.items()})
    if result["measure_directory"]["status"] != "section_absent":
        result["measure_directory"]["entries"] = [list(entry) for entry in DIRECTORY_ENTRY.findall(result["measure_directory"]["text"])]
    result["footnotes"] = footnotes(text)
    result["broken_reference_markers"] = text.count(BROKEN_REFERENCE)
    return result


def edition_key(label: str | None) -> tuple[int, int]:
    """Sort editions by publication month."""
    if not label:
        return (9999, 99)
    parsed = datetime.strptime(label, "%B %Y")
    return (parsed.year, parsed.month)


def group_variants(editions: list[dict[str, Any]], pick: Any) -> dict[str, Any]:
    """Group editions by identical value of one extracted field."""
    variants: dict[str, dict[str, Any]] = {}
    absent: list[str] = []
    for edition in editions:
        value = pick(edition)
        if value is None:
            absent.append(edition["edition"])
            continue
        key = sha256(json.dumps(value, sort_keys=True).encode())
        variants.setdefault(key, {"value": value, "editions": []})["editions"].append(edition["edition"])
    ordered = sorted(variants.values(), key=lambda v: edition_key(v["editions"][0]))
    # Extraction can split words ("bed si ze"); content variants ignore all whitespace and case.
    content = {re.sub(r"\s+", "", json.dumps(v["value"], sort_keys=True)).lower() for v in ordered}
    return {"variant_count": len(ordered), "content_variant_count": len(content), "absent_in": absent, "variants": ordered}


def component(measure: str) -> tuple[str, str | None]:
    """Measure family and the role of its component, for the three observed ID spellings."""
    family = measure_family(measure)
    rest = measure.replace("-", "_")[len(family) :].replace("_", "").upper()
    return family, COMPONENTS.get(rest)


def pooled_ratios(review: Review) -> dict[str, Any]:
    """Per hospital table and window: SIR consistency with observed/predicted and the pooled observed/predicted ratio."""
    results: dict[str, Any] = {}
    for table_sha, table in sorted(review.tables.items()):
        if table.get("class") != "hai_hospital":
            continue
        _, header, rows = read_table(load_table(review.table_locators[table_sha]))
        roles = column_roles(header)
        values: dict[tuple[str, str], dict[str, dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
        for row in rows:
            if len(row) != len(header):
                continue
            family, role = component(row[roles["measure_id"]].strip())
            score = row[roles["score"]].strip().replace(",", "")
            if role is not None and NUMBER.match(score):
                values[(row_period(row, roles), family)][row[roles["facility"]].strip()][role] = float(score)
        for (period, family), facilities in sorted(values.items()):
            both = [f for f in facilities.values() if "observed" in f and "predicted" in f]
            scored = [f for f in both if "SIR" in f and f["predicted"] > 0]
            consistent = sum(abs(f["SIR"] - f["observed"] / f["predicted"]) <= 0.002 + 0.001 * f["SIR"] for f in scored)
            observed, predicted = sum(f["observed"] for f in both), sum(f["predicted"] for f in both)
            key = f"{period}|{family}|{table_sha[:12]}"
            results[key] = {
                "releases": sorted(review.table_releases[table_sha]),
                "facilities_with_observed_and_predicted": len(both),
                "facilities_with_numeric_sir": sum("SIR" in f for f in facilities.values()),
                "sir_checked": len(scored),
                "sir_equals_observed_over_predicted": consistent,
                "observed_sum": round(observed, 3),
                "predicted_sum": round(predicted, 3),
                "pooled_ratio": round(observed / predicted, 4) if predicted else None,
            }
    return results


def build_report(root: Path, inventory_path: Path) -> dict[str, Any]:
    """Read every dictionary and hospital table of the source and assemble the deterministic report."""
    candidates = sorted((c for c in json.loads(inventory_path.read_text())["candidates"] if c["source_id"] == SOURCE), key=lambda c: c["snapshot_id"])
    documents: dict[str, dict[str, Any]] = {}
    locations: dict[str, set[str]] = defaultdict(set)
    skipped: list[dict[str, str]] = []
    review = Review()
    for index, candidate in enumerate(candidates, start=1):
        receipt_path = safe_path(root, candidate["receipt"])
        if not receipt_path.is_file() or digest(receipt_path) != candidate["receipt_sha256"]:
            skipped.append({"snapshot_id": candidate["snapshot_id"], "reason": "receipt_drift"})
            continue
        status = review_capture(root, candidate, review)["review_status"]
        if status != "archive_reviewed":
            skipped.append({"snapshot_id": candidate["snapshot_id"], "reason": status})
            continue
        for artifact in (a for a in json.loads(receipt_path.read_text())["artifacts"] if a["role"] == "data"):
            for location, data in pdf_members((receipt_path.parent / artifact["storage_path"]).read_bytes(), artifact["stored_file_name"]):
                member_sha = sha256(data)
                locations[member_sha].add(location)
                if member_sha not in documents:
                    pages = [page.extract_text() or "" for page in PdfReader(__import__("io").BytesIO(data)).pages]
                    edition = next((m.group(1) for page in pages[:3] if (m := EDITION.search(" ".join(page.split())))), None)
                    documents[member_sha] = {"edition": edition, "pages": len(pages), "sections": edition_sections(pages)}
        log.info("capture %d of %d read", index, len(candidates))
    editions = sorted(
        ({"sha256": s, **d, "locations": sorted(locations[s])} for s, d in documents.items()), key=lambda d: (edition_key(d["edition"]), d["sha256"])
    )
    labels = [d["edition"] for d in editions]

    def section_text(name: str) -> Any:
        return lambda e: e["sections"][name].get("text")

    grouped = {name: group_variants(editions, section_text(name)) for name in [*SECTIONS, *TABLES]}
    for name in TABLES:
        grouped[f"{name}_columns"] = group_variants(editions, lambda e, name=name: e["sections"][name].get("columns"))
    grouped["directory_entries"] = group_variants(editions, lambda e: e["sections"]["measure_directory"].get("entries"))
    grouped["footnotes"] = {
        code: group_variants(editions, lambda e, code=code: e["sections"]["footnotes"].get("items", {}).get(code)) for code in FOOTNOTE_CODES
    }
    observed_ids = Counter(component(measure)[0] for t in review.tables.values() if t.get("class") == "hai_hospital" for measure in t.get("measures", {}))
    directory_ids = sorted({entry[2][:5] for e in editions for entry in e["sections"]["measure_directory"].get("entries", [])})
    return {
        "version": 1,
        "source_id": SOURCE,
        "inventory_sha256": digest(inventory_path),
        "code_sha256": digest(Path(__file__)),
        "distinct_dictionaries": len(editions),
        "editions": labels,
        "edition_documents": [
            {
                "edition": e["edition"],
                "sha256": e["sha256"],
                "pages": e["pages"],
                "locations": e["locations"],
                "section_status": {name: e["sections"][name]["status"] for name in [*SECTIONS, *TABLES, "footnotes"]},
                "broken_reference_markers": e["sections"]["broken_reference_markers"],
            }
            for e in editions
        ],
        "sections": grouped,
        "measure_families": {"dictionary": directory_ids, "hospital_tables": sorted(observed_ids)},
        "sir_checks": pooled_ratios(review),
        "skipped": skipped,
        "model_eligible": False,
        "limits": [
            "Offline local bytes only; no S3 or publisher requests.",
            "Passages are cut by anchor text; a section whose anchor is absent is recorded as absent, not empty.",
            "PDF text extraction can split words; variants that differ only by such noise stay separate and are counted.",
            "Pooled ratios cover facilities that publish both observed and predicted counts; they are not national SIRs.",
            "No hold is cleared; target construction, leakage and eligibility are outside this check.",
        ],
    }


def main() -> int:
    """Run the extraction from its real CLI."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    csv.field_size_limit(16 << 20)
    report = build_report(args.source_root.resolve(), args.inventory.resolve())
    write_json(args.output, report)
    summary = {name: report["sections"][name]["variant_count"] for name in [*SECTIONS, *TABLES]}
    sys.stdout.write(json.dumps({"distinct_dictionaries": report["distinct_dictionaries"], "section_variants": summary}) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
