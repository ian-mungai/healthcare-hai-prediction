"""Record the publisher's statements about the 2020 COVID-19 reporting exception from every HAI archive dictionary.

Every distinct PDF inside the source's archives (one nesting level) is read once. Each passage that matches a pattern
is kept with the document checksum, page number and bounded surrounding text. Publisher documentation is public.

Usage (the interpreter needs pypdf)::

    BUNDLED_PYTHON -m scripts.review.extract_dictionary_passages --source-root SOURCE_ROOT \
        --inventory data/schema_review/2026_09_30/inputs/capture_inventory_effective.json --output passages.json
"""

from __future__ import annotations

import argparse
import io
import json
import re
import sys
import zipfile
from collections import defaultdict
from pathlib import Path, PurePosixPath
from typing import Any

from pypdf import PdfReader

from scripts.review.inspect_hai_archives import SOURCE, is_resource_fork, sha256
from scripts.review.review_remaining import digest, safe_path, write_json

PATTERNS = {
    "not_using_jan_jun_2020": r"January 1, 2020\s*(?:through|–|-)\s*June 30, 2020",
    "q1_q2_2020_not_reported": r"1st and 2nd quarters of 2020 are not being reported",
    "not_updated_display_october_2020": r"will not be updated\s+in \w+ 20\d\d and will continue to display the same data",
}
WINDOW = 700
EDITION = re.compile(r"Dictionary\s+((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+20\d\d)")


def pdf_members(data: bytes, label: str, depth: int = 0) -> list[tuple[str, bytes]]:
    """Every PDF member of an archive, descending one nesting level."""
    found: list[tuple[str, bytes]] = []
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        for info in sorted((i for i in archive.infolist() if not i.is_dir()), key=lambda i: i.filename):
            suffix = PurePosixPath(info.filename).suffix.lower()
            if is_resource_fork(info.filename):
                continue
            if suffix == ".zip" and depth == 0:
                found.extend(pdf_members(archive.read(info), f"{label}!{info.filename}", depth + 1))
            elif suffix == ".pdf":
                found.append((f"{label}!{info.filename}", archive.read(info)))
    return found


def passages(data: bytes) -> dict[str, Any]:
    """Edition label, page count and every pattern match with its page and surrounding text."""
    pages = [" ".join((page.extract_text() or "").split()) for page in PdfReader(io.BytesIO(data)).pages]
    edition = next((m.group(1) for page in pages[:3] if (m := EDITION.search(page))), None)
    matches = []
    for number, text in enumerate(pages, start=1):
        for name, pattern in PATTERNS.items():
            for match in re.finditer(pattern, text):
                start, end = max(0, match.start() - WINDOW), min(len(text), match.end() + WINDOW)
                matches.append({"pattern": name, "page": number, "passage": text[start:end]})
    return {"edition": edition, "pages": len(pages), "matches": matches}


def main() -> int:
    """Run the extraction from its real CLI."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.source_root.resolve()
    documents: dict[str, dict[str, Any]] = {}
    locations: dict[str, set[str]] = defaultdict(set)
    skipped: list[dict[str, str]] = []
    for candidate in sorted((c for c in json.loads(args.inventory.read_text())["candidates"] if c["source_id"] == SOURCE), key=lambda c: c["snapshot_id"]):
        receipt_path = safe_path(root, candidate["receipt"])
        if not receipt_path.is_file() or digest(receipt_path) != candidate["receipt_sha256"]:
            skipped.append({"snapshot_id": candidate["snapshot_id"], "reason": "receipt_drift"})
            continue
        for artifact in json.loads(receipt_path.read_text())["artifacts"]:
            path = receipt_path.parent / artifact["storage_path"]
            if not path.is_file() or digest(path) != artifact["sha256"]:
                skipped.append({"snapshot_id": candidate["snapshot_id"], "reason": "artifact_not_local_or_changed"})
                continue
            for location, data in pdf_members(path.read_bytes(), artifact["stored_file_name"]):
                member_sha = sha256(data)
                locations[member_sha].add(location)
                if member_sha not in documents:
                    documents[member_sha] = passages(data)
    by_passage: dict[str, list[str]] = defaultdict(list)
    for member_sha, document in documents.items():
        for match in document["matches"]:
            by_passage[match["pattern"]].append(document["edition"] or member_sha)
    matching = {name: sorted(set(editions)) for name, editions in sorted(by_passage.items())}
    report = {
        "source_id": SOURCE,
        "inventory_sha256": digest(args.inventory),
        "code_sha256": digest(Path(__file__)),
        "patterns": PATTERNS,
        "distinct_pdfs": len(documents),
        "documents_matching": matching,
        "documents": {sha: {**doc, "locations": sorted(locations[sha])} for sha, doc in sorted(documents.items())},
        "skipped": skipped,
    }
    write_json(args.output, report)
    sys.stdout.write(json.dumps({"distinct_pdfs": len(documents), "documents_matching": {k: len(v) for k, v in matching.items()}}) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
