"""The reader the publisher job gives the listing fetch: the links of an index page, through the reviewed transport.

``links_reader`` downloads an index page with the reviewed download (approved hosts only, public addresses, one attempt, a byte bound) into a scratch folder
and returns its links with absolute addresses and one-line labels. A page that failed to download is an error, which the fetch reports as an unreadable
page and never as an empty page. No file that a page links is requested.
"""

from __future__ import annotations

import hashlib
import tempfile
import urllib.parse
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.acquisition import listing_fetch, transport
from scripts.acquisition.discover_history import Links

MAX_PAGE_BYTES = 8 * 1024**2
PAGE_NAME = "index.html"


def links_reader(scratch: Path, download: Callable[..., Any] = transport.download) -> listing_fetch.LinksOf:
    """The ``links_of`` of the listing fetch. Each read keeps its page in a new folder under ``scratch``, so a repeat never meets an earlier download."""

    def links_of(url: str) -> list[dict[str, str]]:
        scratch.mkdir(parents=True, exist_ok=True)
        folder = Path(tempfile.mkdtemp(prefix=hashlib.sha256(url.encode()).hexdigest()[:16] + "_", dir=scratch))
        result = download(url, folder, PAGE_NAME, "html", "methodology", transport.Limits(max_bytes=MAX_PAGE_BYTES, attempts=1))
        if result.failure is not None:
            raise OSError("The index page did not download.")
        parser = Links()
        parser.feed(Path(result.path).read_text(encoding="utf-8", errors="replace"))
        return [
            {"url": urllib.parse.urljoin(url, item["href"]), "label": " ".join(item["text"].split())}
            for item in parser.links
            if item["href"] and not item["href"].startswith(("?", "#"))
        ]

    return links_of
