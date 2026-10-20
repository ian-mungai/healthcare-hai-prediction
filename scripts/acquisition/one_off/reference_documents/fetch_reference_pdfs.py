"""Fetch the reference-document PDFs named in a history-route candidates file and accept only a real PDF.

AHRQ answers a path it does not have with HTTP 200 and an HTML page, so each file must arrive as ``application/pdf`` and start with ``%PDF-``.
The run writes the originals, a transport record per attempt and one ``receipt.json`` with every file's SHA-256 into a new local folder.
Storage in the project bucket is a separate step through ``history_routes`` (see ``docs/data_collection.md``, Publisher Data Dictionaries).

    .venv/bin/python -m scripts.acquisition.one_off.reference_documents.fetch_reference_pdfs \
        --candidates config/acquisition/ahrq_psi_change_log_candidates.json --output data/reference_checks/<date>/ahrq_psi_change_logs
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
from pathlib import Path
from typing import Any

from scripts.acquisition.one_off.reference_documents.fetch_support import FetchError, run_fetch
from scripts.acquisition.transport import CaptureError


def pdf_specs(candidates: Path) -> list[dict[str, Any]]:
    """Return one fetch spec per candidate; every candidate must be a reference-role PDF."""
    specs = []
    for item in json.loads(candidates.read_text(encoding="utf-8"))["candidates"]:
        if item.get("format") != "pdf" or item.get("role") != "reference":
            raise FetchError(f"Candidate {item.get('url')} is not a reference-role PDF")
        file_name = urllib.parse.unquote(urllib.parse.urlsplit(item["url"]).path.rsplit("/", 1)[-1])
        specs.append(
            {
                "url": item["url"],
                "file_name": file_name,
                "format": "pdf",
                "media_types": {"application/pdf"},
                "magic": b"%PDF-",
                "expected_sha256": item.get("expected_sha256"),
            }
        )
    return specs


def main(argv: list[str] | None = None) -> int:
    """Fetch the candidates into a new folder, print the outcome as JSON and return 0 only when every file was accepted."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="A new empty folder; a folder with a receipt is rechecked offline.")
    args = parser.parse_args(argv)
    try:
        receipt, requested = run_fetch("reference_pdfs", pdf_specs(args.candidates), args.output)
    except (FetchError, CaptureError, OSError) as error:
        parser.error(str(error))
    summary = {
        "receipt": str(args.output / "receipt.json"),
        "requests_made": requested,
        "all_accepted": receipt["all_accepted"],
        "files": [{key: entry[key] for key in ("file_name", "status", "reason", "media_type", "bytes", "sha256")} for entry in receipt["files"]],
    }
    sys.stdout.write(json.dumps(summary, indent=2) + "\n")
    return 0 if receipt["all_accepted"] else 1


if __name__ == "__main__":
    sys.exit(main())
