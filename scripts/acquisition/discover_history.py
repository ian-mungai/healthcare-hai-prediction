"""Preserve publisher indexes and register only links actually listed in them."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from dataclasses import asdict
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit

from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import canonical_hash, read_json
from scripts.acquisition.transport import Limits, download


class Links(HTMLParser):
    """Collect publisher anchor URLs and text from an HTML index."""

    def __init__(self) -> None:
        super().__init__()
        self.links: list[dict[str, str]] = []
        self.current: dict[str, str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Start collecting an anchor's URL and text from publisher HTML."""
        if tag == "a":
            self.current = {"href": dict(attrs).get("href") or "", "text": ""}

    def handle_data(self, data: str) -> None:
        """Append text to the active publisher anchor."""
        if self.current is not None:
            self.current["text"] += data

    def handle_endtag(self, tag: str) -> None:
        """Complete and retain the current publisher anchor."""
        if tag == "a" and self.current is not None:
            self.links.append(self.current)
            self.current = None


def index(url: str, root: Path) -> tuple[list[dict], dict]:
    """Return publisher-index links and their preserved retrieval evidence."""
    directory = root / "indexes" / hashlib.sha256(url.encode()).hexdigest()
    receipt = directory / "index.json"
    if receipt.exists():
        saved = read_json(receipt)
        return saved.get("links", []), saved
    result = download(url, directory, "index.html", "html", "methodology", Limits(max_bytes=8 * 1024**2, attempts=1))
    saved = {"url": url, "response": asdict(result), "links": []}
    if result.failure is None:
        parser = Links()
        parser.feed(result.path.read_text(encoding="utf-8", errors="replace"))
        saved["links"] = [
            {"url": urljoin(url, item["href"]), "label": " ".join(item["text"].split())}
            for item in parser.links
            if item["href"] and not item["href"].startswith(("?", "#"))
        ]
    saved = json.loads(json.dumps(saved, default=str))
    write_once(receipt, encoded_json(saved))
    logging.getLogger(__name__).info(
        "Publisher index inspected",
        extra={"url_sha256": hashlib.sha256(url.encode()).hexdigest(), "status": result.failure or "listed", "link_count": len(saved["links"])},
    )
    return saved["links"], saved


def candidate(source: str, item: dict, evidence: dict, limit: int = 64 * 1024**2) -> dict:
    """Return a bounded file candidate linked to its preserved publisher-index evidence."""
    suffix = Path(urlsplit(item["url"]).path).suffix[1:].lower()
    return {
        "source_id": source,
        "url": item["url"],
        "format": "txt" if suffix == "dat" else suffix,
        "role": "data",
        "label": item["label"],
        "max_bytes": limit,
        "evidence": {"publisher_index": evidence["url"], "index_sha256": canonical_hash(evidence)},
    }


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", type=Path, required=True)
    args = parser.parse_args()
    root = args.state_root
    candidates: list[dict] = []
    for url, pattern in [
        ("https://www2.census.gov/programs-surveys/sahie/datasets/time-series/estimates-acs/", r"sahie-\d{4}-csv\.zip"),
        ("https://www2.census.gov/programs-surveys/sahie/datasets/time-series/estimates-cps/", r"sahie-(?:2005\.txt|200[67]\.csv)"),
    ]:
        links, evidence = index(url, root)
        candidates.extend(candidate("SAHIE", item, evidence) for item in links if re.fullmatch(pattern, Path(urlsplit(item["url"]).path).name))
    url = "https://www2.census.gov/programs-surveys/saipe/datasets/"
    years, _ = index(url, root)
    for year in years:
        if not re.fullmatch(r"\d{4}/", year["url"].removeprefix(url)):
            continue
        subdirs, _ = index(year["url"], root)
        for subdir in subdirs:
            if not re.search(r"/\d{4}-state-and-county/$", subdir["url"]):
                continue
            links, evidence = index(subdir["url"], root)
            candidates.extend(
                candidate("SAIPE", item, evidence, 8 * 1024**2) for item in links if re.fullmatch(r"est\d{2}all\.txt", Path(urlsplit(item["url"]).path).name)
            )
    links, evidence = index("https://www.bls.gov/lau/tables.htm", root)
    candidates.extend(candidate("BLS", item, evidence, 8 * 1024**2) for item in links if re.search(r"/laucnty\d{2}\.xlsx$", item["url"]))
    unique = {item["url"]: item for item in candidates}
    write_once(root / "candidates.json", encoded_json({"candidates": list(unique.values())}))
    sys.stdout.write(str(json.dumps({"registered_candidates": len(unique)})) + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
