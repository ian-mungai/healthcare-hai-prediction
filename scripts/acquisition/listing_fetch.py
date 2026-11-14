"""The listing fetch of the monthly hold report: the files each publisher's index pages show now, as the facts the report names.

A reviewed list of sources (``read_sources``) names, for each source, the index pages to read, the file name pattern of its files, their role and collector
mode and their reviewed download paths. The fetch reads each approved index page and keeps the links whose file name matches the pattern. It asks for no
listed file: a discovered file is never requested, so nothing here follows a redirect or reads a header. It decides nothing: ``plan_rebuild`` holds every
file the locked queue does not have and names what would keep it out of a queue.

The fetch makes no request of its own. It is given a way to read an index page's links, so the job gives it the reviewed transport, which enforces the
approved hosts, public addresses and the byte and time bounds. Here a further rule applies before any request: an index page on a host outside the
approved list is never asked for. A page that cannot be read, or a source that lists too many files, is reported as unreached and never stops the run.
A link counts only when its address is the canonical address (no query, fragment or step out of its folder) and lies under one of the source's reviewed
download paths; any other link of a matching name is named in ``outside`` and never listed.
"""

from __future__ import annotations

import re
import urllib.parse
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from scripts.acquisition import plan_rebuild
from scripts.acquisition.transport import CaptureError

MAX_FILES_PER_SOURCE = 500
SOURCE_KEYS = frozenset({"source_id", "index_urls", "file_pattern", "role", "mode", "path_prefixes"})
MAX_PREFIXES = 20
ENCODED_DOT_OR_SLASH = re.compile(r"%(?:2e|2f|5c)", re.IGNORECASE)
SOURCE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
LinksOf = Callable[[str], Iterable[Mapping[str, str]]]


@dataclass(frozen=True)
class ListingSource:
    """One reviewed source of the listing: where its files are shown and what they are."""

    source_id: str
    index_urls: tuple[str, ...]
    file_pattern: re.Pattern[str]
    role: str
    mode: str
    path_prefixes: tuple[str, ...]


@dataclass(frozen=True)
class Listing:
    """The entries ``plan_rebuild.read_entries`` reads, the index pages that gave nothing and the links outside the reviewed paths, each with a fixed reason."""

    entries: list[dict[str, Any]]
    unreached: list[dict[str, str]]
    outside: list[dict[str, str]]


def index_url_problem(url: object) -> bool:
    if not isinstance(url, str):
        return True
    parts = urllib.parse.urlsplit(url)
    try:
        port = parts.port
    except ValueError:
        return True
    return parts.scheme != "https" or not parts.hostname or parts.username is not None or port is not None or bool(parts.fragment)


def plain_path(path: str) -> bool:
    """Whether a path names a place with no step that can leave it: no empty, current or parent segment, no encoded dot or slash and no back slash."""
    segments = path.split("/")[1:]
    return (
        path.startswith("/")
        and "\\" not in path
        and not ENCODED_DOT_OR_SLASH.search(path)
        and all(part not in {".", ".."} for part in segments)
        and all(part for part in segments[:-1])
    )


def prefix_problem(prefix: object) -> bool:
    """Whether a reviewed download path is unusable: it must be a plain directory path that starts and ends with a slash."""
    return not (isinstance(prefix, str) and prefix.startswith("/") and prefix.endswith("/") and plain_path(prefix) and not re.search(r"[?#\s]", prefix))


def address_problem(url: object) -> str | None:
    """Why a file address is not the canonical address the listing builds, or None.

    The canonical form is a plain HTTPS address with a host, no user, port, query or fragment and no step in its path that can leave its folder, and it is
    unchanged by ``normal_url``. The downloader fetches exactly this text."""
    if not isinstance(url, str) or not url or url != normal_url(url):
        return "it is not the canonical address with no query, fragment or padding"
    parts = urllib.parse.urlsplit(url)
    try:
        port = parts.port
    except ValueError:
        return "its port is not valid"
    if parts.scheme != "https" or not parts.hostname or parts.username is not None or port is not None:
        return "it is not a plain HTTPS address with a host, no user and no port"
    if not plain_path(parts.path):
        return "its path has a step that can leave its folder"
    return None


def path_allowed(url: str, prefixes: Iterable[str]) -> bool:
    """Whether the address lies under one of the reviewed download paths of its source."""
    path = urllib.parse.urlsplit(url).path
    return any(path.startswith(prefix) for prefix in prefixes)


def read_sources(document: object) -> list[ListingSource]:
    """Read the reviewed sources document, which holds exactly one list ``sources`` of closed rows. A row that is open or malformed stops the read."""
    if not isinstance(document, Mapping) or set(document) != {"sources"} or not isinstance(document["sources"], list):
        raise CaptureError("The listing sources must hold exactly one list of sources.")
    found: list[ListingSource] = []
    for row in document["sources"]:
        if not isinstance(row, Mapping) or set(row) != SOURCE_KEYS:
            raise CaptureError("A listing source must hold exactly its source id, index URLs, file pattern, role, mode and download paths.")
        urls, pattern = row["index_urls"], row["file_pattern"]
        if not isinstance(row["source_id"], str) or not SOURCE_ID.fullmatch(row["source_id"]):
            raise CaptureError("A listing source needs a source id of letters, digits, dots, dashes and underscores.")
        if not isinstance(urls, list) or not urls or any(index_url_problem(url) for url in urls):
            raise CaptureError("A listing source needs one or more HTTPS index URLs with no user, port or fragment.")
        if not isinstance(pattern, str) or not pattern:
            raise CaptureError("A listing source needs a file name pattern.")
        try:
            compiled = re.compile(pattern)
        except re.error:
            raise CaptureError("A listing source's file name pattern is not a pattern.") from None
        if row["role"] not in plan_rebuild.ROLES or not isinstance(row["mode"], str) or not row["mode"]:
            raise CaptureError("A listing source needs a known file role and a collector mode.")
        prefixes = row["path_prefixes"]
        if (
            not isinstance(prefixes, list)
            or not 0 < len(prefixes) <= MAX_PREFIXES
            or len(set(map(str, prefixes))) != len(prefixes)
            or any(map(prefix_problem, prefixes))
        ):
            raise CaptureError("A listing source needs its download paths: one or more plain directory paths that start and end with a slash.")
        found.append(ListingSource(row["source_id"], tuple(urls), compiled, row["role"], row["mode"], tuple(prefixes)))
    if len({item.source_id for item in found}) != len(found):
        raise CaptureError("A listing source id appears twice.")
    return found


def approved(url: str, hosts: frozenset[str]) -> bool:
    parts = urllib.parse.urlsplit(url)
    return parts.scheme == "https" and parts.hostname in hosts


def normal_url(url: str) -> str:
    """The file's address with no fragment and no query, which the file name pattern and the queue's known URLs are compared with."""
    parts = urllib.parse.urlsplit(url.strip())
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def build_listing(sources: Iterable[ListingSource], hosts: frozenset[str], links_of: LinksOf) -> Listing:
    """Read each source's index pages and return the listing of its files. The result does not depend on the order of the sources."""
    entries: dict[tuple[str, str], dict[str, Any]] = {}
    unreached: list[dict[str, str]] = []
    outside: list[dict[str, str]] = []
    for item in sorted(sources, key=lambda found: found.source_id):
        for index_url in item.index_urls:
            if not approved(index_url, hosts):
                unreached.append({"source_id": item.source_id, "url": index_url, "reason": "index_host_not_approved"})
                continue
            try:
                links = list(links_of(index_url))
            except (OSError, CaptureError, ValueError):
                unreached.append({"source_id": item.source_id, "url": index_url, "reason": "index_unreadable"})
                continue
            named = {normal_url(link["url"]) for link in links if item.file_pattern.fullmatch(urllib.parse.urlsplit(link["url"]).path.rsplit("/", 1)[-1])}
            allowed = {url for url in named if address_problem(url) is None and path_allowed(url, item.path_prefixes)}
            outside.extend({"source_id": item.source_id, "url": url, "reason": "outside_reviewed_paths"} for url in sorted(named - allowed))
            if len(allowed) > MAX_FILES_PER_SOURCE:
                unreached.append({"source_id": item.source_id, "url": index_url, "reason": "too_many_files"})
                continue
            for url in sorted(allowed):
                entries[(url, item.source_id)] = {"source_id": item.source_id, "url": url, "role": item.role, "mode": item.mode}
    return Listing(
        [entries[name] for name in sorted(entries)],
        sorted(unreached, key=lambda row: (row["source_id"], row["url"])),
        sorted(outside, key=lambda row: (row["source_id"], row["url"])),
    )
