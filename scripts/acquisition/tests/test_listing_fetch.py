"""The listing fetch: the files a publisher's index pages show now, as the facts the hold report names. It never asks for a listed file."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from scripts.acquisition import listing_fetch
from scripts.acquisition.transport import CaptureError
from tests.support import check

HOSTS = frozenset({"data.example.gov"})


def source(**changes: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "source_id": "SRC",
        "index_urls": ["https://data.example.gov/files/"],
        "file_pattern": r"measures_\d{4}\.csv",
        "role": "data",
        "mode": "file",
        "path_prefixes": ["/files/"],
    }
    return row | changes


class Site:
    """A fake publisher: index pages by URL. It records every page asked for."""

    def __init__(self, pages: Mapping[str, list[dict[str, str]] | Exception]) -> None:
        self.pages = pages
        self.asked: list[str] = []

    def links_of(self, url: str) -> list[dict[str, str]]:
        self.asked.append(url)
        page = self.pages[url]
        if isinstance(page, Exception):
            raise page
        return page


def page(*names: str, host: str = "data.example.gov") -> list[dict[str, str]]:
    return [{"url": f"https://{host}/files/{name}", "label": name} for name in names]


def run(rows: list[dict[str, Any]], site: Site, hosts: frozenset[str] = HOSTS) -> listing_fetch.Listing:
    return listing_fetch.build_listing(listing_fetch.read_sources({"sources": rows}), hosts, site.links_of)


def test_files_that_match_a_sources_pattern_become_entries_in_url_order() -> None:
    site = Site({"https://data.example.gov/files/": page("measures_2026.csv", "measures_2025.csv", "readme.html", "measures_2026.csv.sig")})
    found = run([source()], site)
    urls = [entry["url"] for entry in found.entries]
    check(
        urls == ["https://data.example.gov/files/measures_2025.csv", "https://data.example.gov/files/measures_2026.csv"], f"matching files in URL order: {urls}"
    )
    check(
        found.entries[0] == {"source_id": "SRC", "url": urls[0], "role": "data", "mode": "file"},
        f"the entry names its source, role and collector: {found.entries[0]}",
    )
    check(found.unreached == [] and found.outside == [], f"nothing was unreachable or outside: {found}")


def test_a_link_with_a_fragment_a_query_or_a_relative_form_is_one_normal_url() -> None:
    links = [
        {"url": "https://data.example.gov/files/measures_2026.csv#top", "label": "a"},
        {"url": "https://data.example.gov/files/measures_2026.csv", "label": "b"},
        {"url": "https://data.example.gov/files/measures_2025.csv?download=1", "label": "c"},
    ]
    found = run([source()], Site({"https://data.example.gov/files/": links}))
    check(
        [entry["url"] for entry in found.entries] == ["https://data.example.gov/files/measures_2025.csv", "https://data.example.gov/files/measures_2026.csv"],
        "urls",
    )


def test_an_index_on_a_host_that_is_not_approved_is_never_fetched_and_is_reported() -> None:
    site = Site({"https://data.example.gov/files/": page("measures_2026.csv")})
    found = run([source(index_urls=["https://other.example.org/files/", "https://data.example.gov/files/"])], site)
    check(site.asked == ["https://data.example.gov/files/"], f"the unapproved page was fetched: {site.asked}")
    check(
        found.unreached == [{"source_id": "SRC", "url": "https://other.example.org/files/", "reason": "index_host_not_approved"}],
        f"reported: {found.unreached}",
    )
    check(len(found.entries) == 1, "the approved index still gives its files")


def test_a_link_to_a_host_that_is_not_approved_is_listed_so_the_report_names_it() -> None:
    found = run([source()], Site({"https://data.example.gov/files/": page("measures_2026.csv", host="cdn.example.org")}))
    check(found.entries[0]["url"].startswith("https://cdn.example.org/"), f"listed: {found.entries}")


@pytest.mark.parametrize("failure", [OSError("down"), CaptureError("refused"), ValueError("bad page"), TimeoutError()])
def test_an_index_that_cannot_be_read_is_reported_and_the_other_sources_still_run(failure: Exception) -> None:
    site = Site({"https://data.example.gov/files/": failure, "https://data.example.gov/other/": page("measures_2026.csv")})
    found = run([source(), source(source_id="TWO", index_urls=["https://data.example.gov/other/"])], site)
    check(found.unreached == [{"source_id": "SRC", "url": "https://data.example.gov/files/", "reason": "index_unreadable"}], f"reported: {found.unreached}")
    check([entry["source_id"] for entry in found.entries] == ["TWO"], "the other source gave its file")


def test_a_source_that_lists_too_many_files_is_skipped_whole_and_reported() -> None:
    names = [f"measures_{2000 + index}.csv" for index in range(listing_fetch.MAX_FILES_PER_SOURCE + 1)]
    found = run([source(file_pattern=r"measures_\d{4}\.csv")], Site({"https://data.example.gov/files/": page(*names)}))
    check(found.entries == [], "no partial list of a source that lists too many")
    check(found.unreached == [{"source_id": "SRC", "url": "https://data.example.gov/files/", "reason": "too_many_files"}], f"reported: {found.unreached}")


@pytest.mark.parametrize(
    "change",
    [
        {"index_urls": []},
        {"index_urls": ["http://data.example.gov/files/"]},
        {"index_urls": ["https://user" + chr(64) + "data.example.gov/files/"]},
        {"index_urls": ["https://data.example.gov:8443/files/"]},
        {"file_pattern": "("},
        {"file_pattern": ""},
        {"role": "slides"},
        {"mode": ""},
        {"source_id": ""},
        {"classification": "non_personal"},
        {"extra": "x"},
        {"path_prefixes": []},
        {"path_prefixes": "/files/"},
        {"path_prefixes": ["files/"]},
        {"path_prefixes": ["/files"]},
        {"path_prefixes": ["/files/../other/"]},
        {"path_prefixes": ["/files//"]},
        {"path_prefixes": ["/%2e%2e/"]},
        {"path_prefixes": ["/files/?x=1/"]},
        {"path_prefixes": ["/files/", "/files/"]},
        {"path_prefixes": ["/files/", 3]},
    ],
)
def test_a_source_row_that_is_open_or_malformed_is_refused(change: dict[str, Any]) -> None:
    with pytest.raises(CaptureError):
        listing_fetch.read_sources({"sources": [source(**change)]})


def test_the_sources_document_must_hold_exactly_a_list_of_sources_with_unique_ids() -> None:
    for document in ({}, {"sources": "x"}, {"sources": [], "extra": 1}, {"sources": [source(), source()]}):
        with pytest.raises(CaptureError):
            listing_fetch.read_sources(document)
    check(listing_fetch.read_sources({"sources": []}) == [], "no source is a valid, empty list")


def test_the_same_pages_give_the_same_entries_whatever_the_order_of_the_sources() -> None:
    pages = {"https://data.example.gov/files/": page("measures_2026.csv"), "https://data.example.gov/other/": page("measures_2026.csv")}
    rows = [source(), source(source_id="TWO", index_urls=["https://data.example.gov/other/"])]
    check(run(rows, Site(pages)).entries == run(list(reversed(rows)), Site(pages)).entries, "the entries do not depend on the order of the sources")


def test_a_link_outside_the_reviewed_download_paths_of_a_source_is_named_and_never_listed() -> None:
    links = [
        {"url": "https://data.example.gov/readmissions/measures_2026.csv", "label": "x"},
        {"url": "https://data.example.gov/files/measures_2026.csv", "label": "y"},
    ]
    found = run([source()], Site({"https://data.example.gov/files/": links}))
    check([entry["url"] for entry in found.entries] == ["https://data.example.gov/files/measures_2026.csv"], f"{found.entries}")
    check(
        found.outside == [{"source_id": "SRC", "url": "https://data.example.gov/readmissions/measures_2026.csv", "reason": "outside_reviewed_paths"}],
        f"{found.outside}",
    )


def test_the_canonical_form_of_a_candidate_address_is_checked_by_one_function() -> None:
    good = "https://data.example.gov/files/measures_2026.csv"
    check(listing_fetch.address_problem(good) is None, "a plain address is canonical")
    for bad in (
        good + "?dataset=other",
        good + "#part",
        " " + good,
        good.upper(),
        "http://data.example.gov/files/measures_2026.csv",
        "https://user" + chr(64) + "data.example.gov/files/measures_2026.csv",
        "https://data.example.gov:8443/files/measures_2026.csv",
        "https://data.example.gov/files/../x/measures_2026.csv",
        "https://data.example.gov/files/./measures_2026.csv",
        "https://data.example.gov/files//measures_2026.csv",
        "https://data.example.gov/files/%2e%2e/measures_2026.csv",
        "https://data.example.gov/files/%2Fmeasures_2026.csv",
        "https://data.example.gov/files\\measures_2026.csv",
        "",
    ):
        check(listing_fetch.address_problem(bad) is not None, f"{bad!r} passed as canonical")
