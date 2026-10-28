"""Fetch the four Census county-change pages (1970, 1980, 1990, 2000) into a local folder with a receipt.

The pages are the Census Bureau's "Changes to Counties or County Equivalent Entities" technical documentation. Storage in the project bucket is a separate
step: the pages go through ``history_routes`` as reference documents of the registered ``ADJ`` source (the transport refuses HTML as a data role). The warehouse
does not read the pages. ``scripts/lakehouse/county_change_seed.py`` turns the fetched pages into the reviewed dbt seed of their change events, with the one
reader in ``scripts/acquisition/county_change_pages.py``.

    .venv/bin/python -m scripts.acquisition.one_off.county_changes.county_changes fetch \\
        --candidates config/acquisition/census_county_change_candidates.json --output <new folder>

``fetch`` writes the originals, a transport record per attempt and one ``receipt.json`` with every page's SHA-256 and the SHA-256 of its change list (the page
bytes differ on every request, so the candidates pin the change list). A folder with a receipt is only rechecked, offline: the recheck runs the current page
check on every accepted page with the requested pin and refuses a page the check refuses or whose change list differs from the receipt. Each file is read once:
its raw hash and the page check come from the same bytes. ``history_routes`` applies the same page check (``page_problem``) before it stores a pinned page and
again for a completed one.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition.county_change_pages import FILE_PATTERN, PAGE_YEARS, bytes_problem
from scripts.acquisition.transport import CaptureError, Limits, download

LIMITS = Limits(attempts=2, timeout_seconds=30, max_seconds=180, max_bytes=2 * 1024**2)
RECEIPT_NAME = "receipt.json"
MIN_BYTES = 50 * 1024


class FetchError(ValueError):
    """A run that cannot start, a page that fails a check or a receipt that no longer matches its files."""


def media_base(value: str | None) -> str:
    """Return the media type without parameters, in lower case."""
    return (value or "").split(";")[0].strip().lower()


def page_specs(candidates: Path) -> list[dict[str, Any]]:
    """Return one fetch spec per candidate; each must be a reference-role HTML page named county-changes.<year>.html."""
    specs = []
    for item in json.loads(candidates.read_text(encoding="utf-8"))["candidates"]:
        file_name = item["url"].rsplit("/", 1)[-1]
        match = FILE_PATTERN.fullmatch(file_name)
        if item.get("format") != "html" or item.get("role") != "reference" or match is None or not item["url"].startswith("https://www.census.gov/"):
            raise FetchError(f"Candidate {item.get('url')} is not a Census county-change page")
        specs.append(
            {"url": item["url"], "file_name": file_name, "page": int(match.group(1)), "change_list_sha256": item["evidence"].get("change_list_sha256")}
        )
    if sorted(spec["page"] for spec in specs) != list(PAGE_YEARS):
        raise FetchError("The candidates must name the 1970, 1980, 1990 and 2000 pages once each")
    return specs


def fetch_one(spec: dict[str, Any], captures: Path, opener: Any = None) -> dict[str, Any]:
    """Download one page and accept it only when transport, media type, size, decade heading and any pinned hash agree; return its receipt entry."""
    result = download(spec["url"], captures / spec["file_name"], spec["file_name"], "html", "reference", LIMITS, opener=opener)
    reason = result.failure
    if reason is None and media_base(result.media_type) != "text/html":
        reason = "unexpected_media_type"
    if reason is None and result.byte_count < MIN_BYTES:
        reason = "below_size_band"
    if reason is None and (result.resolved_url or spec["url"]) != spec["url"]:
        reason = "redirected"
    digest = None
    if reason is None:
        raw = result.path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != result.sha256:
            reason = "file_changed_after_download"
        else:
            reason, digest = bytes_problem(raw, spec["page"], spec.get("change_list_sha256"))
    return {
        "url": spec["url"],
        "file_name": spec["file_name"],
        "status": "accepted" if reason is None else "rejected",
        "reason": reason,
        "http_status": result.http_status,
        "media_type": result.media_type,
        "bytes": result.byte_count,
        "sha256": result.sha256,
        "change_list_sha256": digest,
        "retrieved_at_utc": result.retrieved_at_utc,
        "attempts": result.attempt,
        "path": result.path.relative_to(captures.parent).as_posix(),
    }


def recheck(receipt_path: Path, specs: list[dict[str, Any]]) -> dict[str, Any]:
    """Return a stored receipt after confirming it names the requested pages and every accepted page still has its recorded hash, passes the current page check
    with the requested pin and has the change list the receipt recorded."""
    receipt: dict[str, Any] = json.loads(receipt_path.read_text(encoding="utf-8"))
    if [entry["url"] for entry in receipt["files"]] != [spec["url"] for spec in specs]:
        raise FetchError(f"{receipt_path} does not match the requested pages; use a new output folder")
    for entry, spec in zip(receipt["files"], specs, strict=True):
        if entry["status"] != "accepted":
            continue
        raw = (receipt_path.parent / entry["path"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != entry["sha256"]:
            raise FetchError(f"{entry['file_name']} differs from its receipt")
        reason, digest = bytes_problem(raw, spec["page"], spec["change_list_sha256"])
        if reason is not None:
            raise FetchError(f"{entry['file_name']} is refused by the current page check ({reason}); use a new output folder")
        if digest != entry["change_list_sha256"]:
            raise FetchError(f"The change list of {entry['file_name']} differs from the change list its receipt recorded; use a new output folder")
    return receipt


def run_fetch(specs: list[dict[str, Any]], output: Path, opener: Any = None) -> tuple[dict[str, Any], bool]:
    """Fetch every page into ``output`` and return the receipt and whether requests were made; a folder with a receipt is only rechecked."""
    receipt_path = output / RECEIPT_NAME
    if receipt_path.exists():
        return recheck(receipt_path, specs), False
    if output.exists() and any(output.iterdir()):
        raise FetchError(f"{output} holds files but no receipt; use a new empty output folder")
    names = [spec["file_name"] for spec in specs]
    if len(set(names)) != len(names):
        raise FetchError("File names must be unique within one run")
    output.mkdir(parents=True, exist_ok=True)
    entries = [fetch_one(spec, output / "captures", opener) for spec in specs]
    receipt = {
        "receipt_version": 1,
        "kind": "census_county_change_pages",
        "created_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "files": entries,
        "all_accepted": all(entry["status"] == "accepted" for entry in entries),
        "terms": "Census Bureau page content; the Census data-use notice was not read, terms not verified",
    }
    with receipt_path.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return receipt, True


def main(argv: list[str] | None = None, opener: Any = None) -> int:
    """Run ``fetch`` and print the outcome as JSON; return 0 only when every page was accepted."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    fetch = commands.add_parser("fetch", help="fetch the four pages into a new folder")
    fetch.add_argument("--candidates", type=Path, required=True)
    fetch.add_argument("--output", type=Path, required=True, help="A new empty folder; a folder with a receipt is rechecked offline.")
    args = parser.parse_args(argv)
    try:
        receipt, requested = run_fetch(page_specs(args.candidates), args.output, opener)
    except (FetchError, CaptureError, OSError, KeyError) as error:
        sys.stderr.write(f"county changes: {error}\n")
        return 2
    summary = {
        "receipt": str(args.output / RECEIPT_NAME),
        "requests_made": requested,
        "all_accepted": receipt["all_accepted"],
        "files": [{key: entry[key] for key in ("file_name", "status", "reason", "bytes", "sha256", "change_list_sha256")} for entry in receipt["files"]],
    }
    sys.stdout.write(json.dumps(summary, indent=2) + "\n")
    return 0 if receipt["all_accepted"] else 1


if __name__ == "__main__":
    sys.exit(main())
