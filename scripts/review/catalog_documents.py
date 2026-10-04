"""Catalog every document of the report and reference sources so each can be summarized by what it covers.

For each distinct PDF, DOCX or workbook (archives opened one level), record pages, a title line, the years and
topic keywords it mentions, how table-like its text is and a short first-page excerpt with e-mail addresses and
telephone numbers masked. The full extracted text of each document is written beside the report for reading;
these are public publications. Workbooks get sheet names and dimensions only.

Usage (the interpreter needs openpyxl and pypdf)::

    PYTHONPATH=.review_dependencies BUNDLED_PYTHON -m scripts.review.catalog_documents --source-root SOURCE_ROOT \
        --inventory data/schema_review/2026_09_30/inputs/capture_inventory_effective.json --output documents_run1
"""

from __future__ import annotations

import argparse
import io
import json
import re
import sys
import zipfile
from collections import Counter
from pathlib import Path, PurePosixPath
from typing import Any

import openpyxl
from pypdf import PdfReader
from pypdf.errors import PdfReadError

from scripts.review.inspect_hai_archives import is_resource_fork, sha256
from scripts.review.review_remaining import digest, safe_path, write_json

SOURCES = ("TX", "VT", "IROQ", "MISS", "MA", "NSI", "CHIA", "MARY", "NY", "PSI13", "PSI90", "HRSA", "CCW-ALGORITHMS", "CMS-MUP-DICTIONARIES", "MMD")
KEYWORDS = {
    "hospital": r"\bhospitals?\b",
    "nurse_staffing": r"\bnurs\w*\b|\bstaffing\b|\bRNs?\b",
    "turnover_vacancy": r"\bturnover\b|\bvacanc\w*\b|\bretention\b",
    "infection": r"\binfection\w*\b|\bCLABSI\b|\bCAUTI\b|\bMRSA\b|\bSSI\b|\bC\.? ?diff",
    "budget_finance": r"\bbudget\w*\b|\brevenue\b|\bexpense\w*\b",
    "patient_safety_indicator": r"\bPSI[- ]?\d+\b|\bpatient safety indicator",
    "variable_definition": r"\bvariable\b|\bfield name\b|\bdata dictionary\b|\bcolumn\b",
    "condition_algorithm": r"\bICD-?10\b|\bICD-?9\b|\balgorithm\w*\b",
}
YEAR = re.compile(r"(?<!\d)((?:19[89]|20[0-3])\d)(?!\d)")
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
PHONE = re.compile(r"\(?\b\d{3}\)?[-. ]\d{3}[-. ]\d{4}\b")
EXCERPT = 900


def mask(text: str) -> str:
    """Mask e-mail addresses and telephone numbers."""
    return PHONE.sub("[phone]", EMAIL.sub("[email]", text))


def describe_text(pages: list[str]) -> dict[str, Any]:
    """Coverage hints for one document's text."""
    text = "\n".join(pages)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    numeric_lines = sum(len(re.findall(r"\d[\d,.%]*", line)) >= 4 for line in lines)
    first = next((line for line in (pages[0].splitlines() if pages else []) if len(line.strip()) > 3), "")
    return {
        "pages": len(pages),
        "pages_without_text": sum(not page.strip() for page in pages),
        "characters": len(text),
        "title_line": mask(first.strip())[:160],
        "years_mentioned": dict(Counter(YEAR.findall(text)).most_common(12)),
        "keyword_counts": {name: len(re.findall(pattern, text, flags=re.IGNORECASE)) for name, pattern in KEYWORDS.items()},
        "table_like_line_share": round(numeric_lines / len(lines), 3) if lines else 0.0,
        "first_page_excerpt": mask(" ".join((pages[0] if pages else "").split()))[:EXCERPT],
    }


def read_document(name: str, data: bytes) -> tuple[dict[str, Any], str | None]:
    """Description and full text of one document; workbooks get structure only."""
    suffix = PurePosixPath(name).suffix.lower()
    try:
        if data.startswith(b"%PDF-"):
            pages = [page.extract_text() or "" for page in PdfReader(io.BytesIO(data)).pages]
            return {"kind": "pdf", **describe_text(pages)}, "\n\f\n".join(pages)
        if data.startswith(b"PK") and suffix == ".docx":
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                xml = archive.read("word/document.xml").decode("utf-8", errors="replace")
            paragraphs = [re.sub(r"<[^>]+>", "", p) for p in re.findall(r"<w:p[ >].*?</w:p>", xml, flags=re.S)]
            text = "\n".join(p for p in paragraphs if p.strip())
            return {"kind": "docx", **describe_text([text])}, text
        if data.startswith(b"PK") and suffix in {".xlsx", ".xlsm"}:
            book = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
            try:
                sheets = {sheet.title[:60]: sheet.calculate_dimension() for sheet in book.worksheets}
            finally:
                book.close()
            return {"kind": "workbook", "sheets": sheets}, None
    except (PdfReadError, zipfile.BadZipFile, KeyError, ValueError, OSError) as error:
        return {"kind": "unreadable", "error_type": type(error).__name__}, None
    return {"kind": "other", "suffix": suffix}, None


def walk(name: str, data: bytes) -> list[tuple[str, bytes]]:
    """The file itself, or the members of an archive (one level)."""
    if not (data.startswith(b"PK") and name.lower().endswith(".zip")):
        return [(name, data)]
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        return [
            (f"{name}!{i.filename}", archive.read(i))
            for i in sorted(archive.infolist(), key=lambda i: i.filename)
            if not i.is_dir() and not is_resource_fork(i.filename)
        ]


def main() -> int:
    """Run the catalog from its real CLI."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.source_root.resolve()
    documents: dict[str, dict[str, Any]] = {}
    statuses: Counter[str] = Counter()
    for candidate in sorted(json.loads(args.inventory.read_text())["candidates"], key=lambda c: (c["source_id"], c["snapshot_id"])):
        if candidate["source_id"] not in SOURCES or candidate["status"] != "candidate_not_authorized_for_execution":
            continue
        receipt = safe_path(root, candidate["receipt"])
        if not receipt.is_file() or digest(receipt) != candidate["receipt_sha256"]:
            statuses["receipt_drift"] += 1
            continue
        for artifact in json.loads(receipt.read_text())["artifacts"]:
            name = artifact["stored_file_name"]
            if candidate["source_id"] == "MMD" and not name.lower().endswith(".pdf"):
                continue
            if PurePosixPath(name).suffix.lower() not in {".pdf", ".docx", ".xlsx", ".xlsm", ".zip"}:
                continue
            path = receipt.parent / artifact["storage_path"]
            if not path.is_file():
                statuses["artifact_not_local"] += 1
                continue
            if digest(path) != artifact["sha256"]:
                statuses["artifact_mismatch"] += 1
                continue
            for member, data in walk(name, path.read_bytes()):
                member_sha = sha256(data)
                if member_sha in documents:
                    documents[member_sha]["locations"].append(f"{candidate['source_id']}:{member}")
                    continue
                description, text = read_document(member, data)
                documents[member_sha] = {
                    "source_id": candidate["source_id"],
                    "name": member,
                    "bytes": len(data),
                    **description,
                    "locations": [f"{candidate['source_id']}:{member}"],
                }
                statuses[description["kind"]] += 1
                if text is not None:
                    text_path = args.output / "text" / f"{member_sha[:16]}.txt"
                    text_path.parent.mkdir(parents=True, exist_ok=True)
                    text_path.write_text(text)
    for document in documents.values():
        document["locations"] = sorted(set(document["locations"]))
    by_source: dict[str, list[str]] = {}
    for member_sha, document in sorted(documents.items(), key=lambda item: (item[1]["source_id"], item[1]["name"])):
        by_source.setdefault(document["source_id"], []).append(member_sha)
    write_json(
        args.output / "catalog.json",
        {
            "version": 1,
            "code_sha256": digest(Path(__file__)),
            "inventory_sha256": digest(args.inventory),
            "statuses": dict(sorted(statuses.items())),
            "documents": dict(sorted(documents.items())),
            "by_source": by_source,
            "model_eligible": False,
        },
    )
    sys.stdout.write(json.dumps({"documents": len(documents), "statuses": dict(sorted(statuses.items()))}) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
