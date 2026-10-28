"""Repeatable offline verification of the Census county-change fetch tool, its reader and the refusal of stored pages (fake HTTPS boundary)."""

from __future__ import annotations

import argparse
import contextlib
import copy
import hashlib
import io
import json
import sys
import tempfile
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.acquisition import bls_api_contract, history_routes
from scripts.acquisition import county_change_pages as grammar
from scripts.acquisition.one_off.county_changes import county_changes as tool

PAGE_URL = "https://www.census.gov/programs-surveys/geography/technical-documentation/county-changes.{year}.html"
LIMIT_PAD = 70_000
DELETED = "<h4><strong>Deleted Counties or County Equivalent Entities</strong></h4>"
NAMES = "<h4><b>Name and/or Code Changes or Corrections for Counties or County Equivalent Entities</b></h4>"
BOUNDARY = "<h4><strong>Substantial County or County Equivalent Entity Boundary Changes </strong></h4>"
SUMMARY = "<h4><b>Changes to County Equivalent Entities in Alaska</b></h4>"
AK_DELETED = "<h4><b>Deleted county equivalent entities in Alaska</b></h4>"
SECTIONS: dict[int, list[tuple[str, list[str]]]] = {
    1970: [
        (SUMMARY, ["<li>The county equivalent entities in Alaska changed from Census Divisions in 1970 to Boroughs and Census Areas in 1980.</li>"]),
        (
            SUMMARY,
            [
                "<li>New entities created in Alaska are: Dillingham Census Area (02-070), North Slope Borough (02-185), "
                "and Valdez-Cordova Census Area (02-261).</li>"
            ],
        ),
        (AK_DELETED, ["<li>Barrow Division (02-040)</li>"]),
        (DELETED, ["<li>Washabaugh County, South Dakota (46-131):<br />", "Combined into Jackson County (46-071) effective January 1, 1979.</li>"]),
    ],
    1980: [
        (
            DELETED,
            [
                "<li>Kobuk Census Area (02-140):<br />",
                "All taken plus part of the North Slope Borough (02-185) to create the new Northwest Arctic Borough (02-188) effective June 2, 1986.</li>",
            ],
        ),
        (NAMES, ["<li>DeKalb County, Alabama (01-049):<br />", "Name corrected from De Kalb County.</li>"]),
    ],
    1990: [
        (
            DELETED,
            [
                "<li>Skagway-Yakutat-Angoon Census Area, Alaska (02-231):<br />",
                "Split to create the Skagway-Hoonah-Angoon Census Area (02-232) and Yakutat City and Borough (02-282) effective September 22, 1992.</li>",
            ],
        ),
        (
            NAMES,
            [
                "<li>Dade County, Florida (12-025):<br />",
                "Renamed as Miami-Dade County (12-086) effective July 22, 1997.</li>",
                "<li>Miami-Dade County, Florida (12-086):<br />",
                "Renamed from Dade County (12-025) effective July 22, 1997.</li>",
            ],
        ),
        (
            BOUNDARY,
            [
                "<li>Test &amp; Sons County, Virginia (51-999):<br />",
                "Part annexed to Test (independent) city (51-998) effective July 1, 1994.</li>",
            ],
        ),
    ],
    2000: [
        (
            DELETED,
            [
                "<li>Prince of Wales-Outer Ketchikan Census Area, Alaska (02-201):<br />",
                "Part annexed by Ketchikan Gateway Borough (02-130) and part included in new Wrangell City and Borough (02-275), remainder renamed "
                "Prince of Wales-Hyder Census Area (02-198) effective May 19, 2008.</li>",
            ],
        ),
        (NAMES, ["<li>LaSalle County, Illinois (17-099); Name corrected from La Salle County (removing space).</li>"]),
    ],
}
Routes = dict[str, tuple[int, str, bytes]]


class Response(io.BytesIO):
    """A response with the status, headers and URL the transport reads."""

    def __init__(self, url: str, status: int, media: str, body: bytes) -> None:
        super().__init__(body)
        self.status = status
        self.headers = {"Content-Type": media, "Content-Length": str(len(body))}
        self.url = url

    def geturl(self) -> str:
        return self.url


class Web:
    """The only network boundary: serves each URL from the synthetic pages and records every request."""

    def __init__(self, routes: Routes, moved: dict[str, str] | None = None) -> None:
        self.routes = routes
        self.moved = moved or {}
        self.requested: list[str] = []

    def open(self, request: Any, timeout: float) -> Response:
        self.requested.append(request.full_url)
        status, media, body = self.routes[request.full_url]
        return Response(self.moved.get(request.full_url, request.full_url), status, media, body)


class Offline(Web):
    """Any request is a failure: an offline recheck must not use the network."""

    def open(self, request: Any, timeout: float) -> Response:
        raise AssertionError("network used")


def pages_heading(year: int) -> str:
    """Return the decade heading element as the real pages carry it."""
    return f'<h3 class="uscb-h3 ">Changes to Counties or County Equivalent Entities: {year}s</h3>'


def page_html(year: int, sections: list[tuple[str, list[str]]], pad: int = LIMIT_PAD) -> bytes:
    """Return a synthetic page in the real layout: navigation, the decade heading, the sections, the closing tag, a footer list and filler."""
    lines = [
        "<!DOCTYPE html>",
        '<ul><li onclick="x();">1980</li></ul>',
        pages_heading(year),
        '<section style="overflow: none;">',
    ]
    for heading, items in sections:
        lines += [heading, "<ul>", *items, "</ul>"]
    lines += ["</section>", "<h4>Related pages</h4>", "<ul><li>Footer link (99-999): not a change</li></ul>", "<!-- " + "x" * pad + " -->"]
    return "\n".join(lines).encode()


def routes_for(pages: dict[int, bytes]) -> Routes:
    """Serve each page at its URL as text/html."""
    return {PAGE_URL.format(year=year): (200, "text/html; charset=UTF-8", body) for year, body in pages.items()}


def base_pages(change: Callable[[dict[int, list[tuple[str, list[str]]]]], None] | None = None) -> dict[int, bytes]:
    """Return the four synthetic pages, after an optional change to their sections."""
    sections = copy.deepcopy(SECTIONS)
    if change is not None:
        change(sections)
    return {year: page_html(year, items) for year, items in sections.items()}


def candidates_file(path: Path, pins: dict[int, str | None] | None = None, years: tuple[int, ...] = grammar.PAGE_YEARS) -> Path:
    """Write a history-route candidates file for the synthetic pages."""
    items = [
        {
            "evidence": {"decision": "synthetic", "listing": "synthetic", "change_list_sha256": (pins or {}).get(year)},
            "expected_sha256": None,
            "format": "html",
            "label": f"synthetic {year}",
            "max_bytes": 2097152,
            "release_date": None,
            "role": "reference",
            "source_id": "ADJ",
            "url": PAGE_URL.format(year=year),
        }
        for year in years
    ]
    path.write_text(json.dumps({"candidates": items}))
    return path


def run(argv: list[str], opener: Any = None) -> dict[str, Any]:
    """Call the tool's real entry point and return its exit code, parsed JSON output and error text."""
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = tool.main(argv, opener)
    return {"exit": code, "json": json.loads(out.getvalue()) if out.getvalue().strip() else None, "stderr": err.getvalue()}


def require(condition: bool, message: str) -> None:
    """Fail the scenario with a message."""
    if not condition:
        raise AssertionError(message)


@contextlib.contextmanager
def read_race(name: str, replacement: bytes, before_read: int) -> Iterator[list[int]]:
    """Count the read-mode opens of each file called ``name``; from read ``before_read`` on, replace the file by ``replacement`` just before it opens."""
    original = Path.open
    reads = [0]

    def opener(self: Path, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        if self.name == name and "r" in mode and "+" not in mode:
            reads[0] += 1
            if reads[0] >= before_read:
                self.write_bytes(replacement)
        return original(self, mode, *args, **kwargs)

    with patch.object(Path, "open", opener):
        yield reads


class Recorder:
    """Run each scenario, record whether it behaved as expected and keep going after a failure."""

    def __init__(self) -> None:
        self.scenarios: list[dict[str, Any]] = []

    def scenario(self, name: str, action: Callable[[], None]) -> None:
        try:
            action()
            self.scenarios.append({"name": name, "passed": True, "observed": "as expected"})
        except (AssertionError, KeyError, ValueError, OSError, TypeError) as failure:
            self.scenarios.append({"name": name, "passed": False, "observed": f"{type(failure).__name__}: {str(failure)[:300]}"})


def fetch(
    root: Path, name: str, pages: dict[int, bytes], pins: dict[int, str | None] | None = None, moved: dict[str, str] | None = None
) -> tuple[Path, Web, dict[str, Any]]:
    """Fetch the synthetic pages into a new folder and return the folder, the web boundary and the tool's result."""
    candidates = candidates_file(root / f"{name}.candidates.json", pins)
    web = Web(routes_for(pages), moved)
    output = root / name
    return output, web, run(["fetch", "--candidates", str(candidates), "--output", str(output)], web)


def fetch_scenarios(recorder: Recorder, root: Path) -> None:
    """Pages that fail a check are rejected with a reason; a good set is accepted, recorded with hashes and only rechecked on rerun."""
    good = base_pages()

    def accepts_four() -> None:
        folder, web, result = fetch(root, "good", good)
        require(result["exit"] == 0 and result["json"]["requests_made"] and len(web.requested) == 4, f"{result}")
        files = result["json"]["files"]
        require(all(item["status"] == "accepted" and len(item["sha256"]) == 64 and len(item["change_list_sha256"]) == 64 for item in files), f"{files}")
        require((folder / "receipt.json").is_file(), "no receipt")

    recorder.scenario("fetch_accepts_four_pages_with_hashes", accepts_four)

    def rerun_offline() -> None:
        candidates = root / "good.candidates.json"
        result = run(["fetch", "--candidates", str(candidates), "--output", str(root / "good")], Offline({}))
        require(result["exit"] == 0 and result["json"]["requests_made"] is False, f"{result}")

    recorder.scenario("fetch_rerun_offline_no_requests", rerun_offline)

    def rejected(name: str, routes: Routes, reason: str, moved: dict[str, str] | None = None) -> Callable[[], None]:
        def action() -> None:
            candidates = candidates_file(root / f"{name}.candidates.json")
            result = run(["fetch", "--candidates", str(candidates), "--output", str(root / name)], Web(routes, moved))
            reasons = [item["reason"] for item in result["json"]["files"] if item["status"] == "rejected"]
            require(result["exit"] == 1 and reason in reasons, f"{reasons}")

        return action

    url_1990 = PAGE_URL.format(year=1990)
    error_page = (200, "text/html", b"<html><body>" + b"not found " * 8000 + b"</body></html>")
    recorder.scenario("fetch_error_page_with_http_200", rejected("error_page", routes_for(good) | {url_1990: error_page}, "decade_heading_or_layout_differs"))
    recorder.scenario(
        "fetch_wrong_media_type", rejected("media", routes_for(good) | {url_1990: (200, "application/json", good[1990])}, "unexpected_media_type")
    )
    recorder.scenario("fetch_empty_body", rejected("empty", routes_for(good) | {url_1990: (200, "text/html", b"")}, "empty_response"))
    small = page_html(1990, SECTIONS[1990], pad=10)
    recorder.scenario("fetch_below_size_band", rejected("small", routes_for(good) | {url_1990: (200, "text/html", small)}, "below_size_band"))
    recorder.scenario("fetch_http_404", rejected("missing", routes_for(good) | {url_1990: (404, "text/html", b"missing")}, "http_404"))
    other = "https://example.org/county-changes.1990.html"
    recorder.scenario("fetch_redirect_to_another_host", rejected("moved_host", routes_for(good), "CaptureError", {url_1990: other}))
    elsewhere = "https://www.census.gov/programs-surveys/geography/other.html"
    recorder.scenario("fetch_redirect_to_another_page", rejected("moved_page", routes_for(good), "redirected", {url_1990: elsewhere}))

    def pinned_differs() -> None:
        folder, _, result = fetch(root, "pin", good)
        pins = {int(item["file_name"].split(".")[1]): item["change_list_sha256"] for item in result["json"]["files"]}
        pins[1990] = "0" * 64
        candidates = candidates_file(root / "pinned.candidates.json", pins)
        again = run(["fetch", "--candidates", str(candidates), "--output", str(root / "pinned")], Web(routes_for(good)))
        reasons = [item["reason"] for item in again["json"]["files"] if item["status"] == "rejected"]
        require(again["exit"] == 1 and reasons == ["change_list_differs_from_pin"], f"{reasons}")

    recorder.scenario("fetch_change_list_differs_from_pin", pinned_differs)

    def pins_of(result: dict[str, Any]) -> dict[int, str | None]:
        return {int(item["file_name"].split(".")[1]): item["change_list_sha256"] for item in result["json"]["files"]}

    def recheck(name: str, pins: dict[int, str | None]) -> dict[str, Any]:
        candidates = candidates_file(root / f"{name}.recheck.candidates.json", pins)
        return run(["fetch", "--candidates", str(candidates), "--output", str(root / name)], Offline({}))

    def pinned_recheck_passes() -> None:
        _, _, first = fetch(root, "recheck_same", good)
        pins = pins_of(first)
        _, _, pinned = fetch(root, "recheck_pinned", good, pins)
        require(pinned["exit"] == 0, f"{pinned}")
        again = recheck("recheck_pinned", pins)
        require(again["exit"] == 0 and again["json"]["requests_made"] is False, f"{again}")

    recorder.scenario("fetch_recheck_passes_with_the_pins_of_the_receipt", pinned_recheck_passes)

    def recheck_changed_pin() -> None:
        pins = pins_of(fetch(root, "recheck_other", good)[2])
        pinned = fetch(root, "recheck_pinned_other", good, pins)[2]
        require(pinned["exit"] == 0, f"{pinned}")
        again = recheck("recheck_pinned_other", pins | {1990: "0" * 64})
        require(again["exit"] == 2 and "1990" in again["stderr"] and "pin" in again["stderr"], f"{again}")

    recorder.scenario("fetch_recheck_refuses_a_changed_requested_pin", recheck_changed_pin)

    def recheck_added_pin() -> None:
        first = fetch(root, "recheck_unpinned", good)[2]
        again = recheck("recheck_unpinned", pins_of(first) | {1980: "0" * 64})
        require(again["exit"] == 2 and "1980" in again["stderr"] and "pin" in again["stderr"], f"{again}")
        same = recheck("recheck_unpinned", pins_of(first))
        require(same["exit"] == 0 and same["json"]["requests_made"] is False, f"{same}")

    recorder.scenario("fetch_recheck_refuses_a_pin_added_after_an_unpinned_fetch", recheck_added_pin)

    def edit_receipt(folder: Path, file_name: str, change: Callable[[dict[str, Any]], None]) -> None:
        receipt = json.loads((folder / "receipt.json").read_text())
        change(next(entry for entry in receipt["files"] if entry["file_name"] == file_name))
        (folder / "receipt.json").write_text(json.dumps(receipt))

    def recheck_stored_page_outside_the_grammar() -> None:
        folder = fetch(root, "recheck_grammar", good)[0]
        page = next(path for path in (folder / "captures").rglob("county-changes.1990.html") if path.is_file())
        body = page.read_bytes().replace(b"<br />", b"<br-note></br-note>", 1)
        page.write_bytes(body)
        edit_receipt(folder, "county-changes.1990.html", lambda entry: entry.update(sha256=hashlib.sha256(body).hexdigest(), bytes=len(body)))
        again = recheck("recheck_grammar", {})
        require(again["exit"] == 2 and "county-changes.1990.html" in again["stderr"] and "decade_heading_or_layout_differs" in again["stderr"], f"{again}")

    recorder.scenario("fetch_recheck_refuses_a_stored_page_the_current_grammar_refuses", recheck_stored_page_outside_the_grammar)

    def recheck_recorded_change_list_differs() -> None:
        folder = fetch(root, "recheck_digest", good)[0]
        edit_receipt(folder, "county-changes.1980.html", lambda entry: entry.update(change_list_sha256="1" * 64))
        again = recheck("recheck_digest", {})
        require(again["exit"] == 2 and "county-changes.1980.html" in again["stderr"] and "change list" in again["stderr"], f"{again}")

    recorder.scenario("fetch_recheck_refuses_a_recorded_change_list_the_grammar_no_longer_gives", recheck_recorded_change_list_differs)

    revised = good[1990].replace(b"effective July 1, 1994", b"effective July 2, 1994")

    def rejects_a_page_replaced_after_the_download() -> None:
        candidates = candidates_file(root / "replaced.candidates.json")
        with read_race("county-changes.1990.html", good[1990], before_read=99) as counted:
            run(["fetch", "--candidates", str(candidates), "--output", str(root / "replaced_count")], Web(routes_for(good)))
        with read_race("county-changes.1990.html", revised, before_read=counted[0]) as reads:
            result = run(["fetch", "--candidates", str(candidates), "--output", str(root / "replaced")], Web(routes_for(good)))
        files = {item["file_name"]: item for item in result["json"]["files"]}
        require(reads[0] == counted[0], f"{reads[0]} reads, the unchanged run made {counted[0]}")
        require(files["county-changes.1990.html"]["reason"] == "file_changed_after_download" and result["exit"] == 1, f"{files['county-changes.1990.html']}")

    recorder.scenario("fetch_rejects_a_page_replaced_between_the_download_and_the_page_check", rejects_a_page_replaced_after_the_download)

    def recheck_reads_each_page_once() -> None:
        folder = fetch(root, "recheck_once", good)[0]
        candidates = candidates_file(root / "recheck_once.recheck.candidates.json", {})
        with read_race("county-changes.1990.html", revised, before_read=2) as reads:
            again = run(["fetch", "--candidates", str(candidates), "--output", str(folder)], Offline({}))
        require(reads[0] == 1 and again["exit"] == 0, f"{reads[0]} reads, {again}")

    recorder.scenario("fetch_recheck_reads_each_page_once_for_the_hash_and_the_page_check", recheck_reads_each_page_once)

    def page_bytes_differ_pin_holds() -> None:
        _, _, first = fetch(root, "vary_a", good)
        noisy = {year: body.replace(b"<!DOCTYPE html>", b"<!DOCTYPE html><!-- request-specific id 4711 -->") for year, body in good.items()}
        pins = {int(item["file_name"].split(".")[1]): item["change_list_sha256"] for item in first["json"]["files"]}
        candidates = candidates_file(root / "vary_b.candidates.json", pins)
        second = run(["fetch", "--candidates", str(candidates), "--output", str(root / "vary_b")], Web(routes_for(noisy)))
        require(second["exit"] == 0 and first["json"]["files"][0]["sha256"] != second["json"]["files"][0]["sha256"], f"{second}")

    recorder.scenario("fetch_page_bytes_vary_but_change_list_pin_holds", page_bytes_differ_pin_holds)

    def tampered() -> None:
        target = next(path for path in (root / "good" / "captures").rglob("county-changes.1990.html") if path.is_file())
        target.write_bytes(target.read_bytes() + b"\n")
        result = run(["fetch", "--candidates", str(root / "good.candidates.json"), "--output", str(root / "good")], Offline({}))
        require(result["exit"] == 2 and "differs from its receipt" in result["stderr"], f"{result}")

    recorder.scenario("fetch_tampered_capture_fails_recheck", tampered)

    def dirty() -> None:
        folder = root / "dirty"
        folder.mkdir()
        (folder / "stray.txt").write_text("x")
        result = run(["fetch", "--candidates", str(root / "good.candidates.json"), "--output", str(folder)], Web(routes_for(good)))
        require(result["exit"] == 2 and "no receipt" in result["stderr"], f"{result}")

    recorder.scenario("fetch_dirty_folder_refused", dirty)

    def duplicate_names() -> None:
        path = candidates_file(root / "dup.candidates.json", years=(1970, 1970, 1980, 1990))
        result = run(["fetch", "--candidates", str(path), "--output", str(root / "dup")], Web(routes_for(good)))
        require(result["exit"] == 2 and "once each" in result["stderr"], f"{result}")

    recorder.scenario("fetch_duplicate_or_missing_page_refused", duplicate_names)


def grammar_scenarios(recorder: Recorder) -> None:
    """The reader's refusals, its decoding and its determinism."""

    def deterministic() -> None:
        page = base_pages()[1970].decode()
        events = grammar.parse_page(page, 1970)
        keys = [(row["page"], row["county_fips"], row["event"], row["row_text"]) for row in events]
        require(events == grammar.parse_page(page, 1970), "two parses differ")
        require(len(keys) == len(set(keys)), "an event appears twice")

    recorder.scenario("parse_deterministic_and_unique", deterministic)

    def refused(name: str, change: Callable[[dict[int, list[tuple[str, list[str]]]]], None], year: int, fragment: str) -> None:
        def action() -> None:
            sections = copy.deepcopy(SECTIONS)
            change(sections)
            try:
                grammar.parse_page(page_html(year, sections[year]).decode(), year)
            except grammar.ParseError as error:
                require(fragment in str(error), f"{error}")
                return
            raise AssertionError("the parser accepted the page")

        recorder.scenario(f"parse_{name}", action)

    refused(
        "unknown_section", lambda s: s[1990].__setitem__(0, (DELETED.replace("Deleted Counties", "Retired Counties"), s[1990][0][1])), 1990, "unknown section"
    )
    refused("item_without_code", lambda s: s[1980][1][1].append("<li>No code here</li>"), 1980, "item without a county code")
    refused("state_differs_from_code", lambda s: s[1980][0][1].__setitem__(0, "<li>Kobuk Census Area, Texas (02-140):<br />"), 1980, "differs from the code")
    refused(
        "impossible_date",
        lambda s: s[1970][3][1].__setitem__(1, "Combined into Jackson County (46-071) effective February 30, 1979.</li>"),
        1970,
        "impossible date",
    )
    refused(
        "retirement_with_two_dates",
        lambda s: s[1970][3][1].__setitem__(1, "Combined into Jackson County (46-071) effective January 1, 1979 and July 1, 1980.</li>"),
        1970,
        "2 dates",
    )
    refused(
        "retirement_without_successor",
        lambda s: s[1970][3][1].__setitem__(1, "Combined into Jackson County effective January 1, 1979.</li>"),
        1970,
        "no successor",
    )
    refused("unrecognised_name_change", lambda s: s[1980][1][1].__setitem__(1, "Changed in some way.</li>"), 1980, "unrecognised name or code change")
    refused(
        "unrecognised_summary_item", lambda s: s[1970][0][1].__setitem__(0, "<li>Something else happened in Alaska.</li>"), 1970, "unrecognised summary item"
    )

    def another_decade() -> None:
        try:
            grammar.parse_page(page_html(1970, SECTIONS[1970]).decode(), 1980)
        except grammar.ParseError as error:
            require("decade heading" in str(error), f"{error}")
            return
        raise AssertionError("a 1970 page was parsed as the 1980 page")

    recorder.scenario("parse_page_of_another_decade", another_decade)

    def entities_and_tags() -> None:
        rows = grammar.parse_page(base_pages()[1990].decode(), 1990)
        row = next(item for item in rows if item["county_fips"] == "51999")
        require(row["row_text"].startswith("Test & Sons County, Virginia (51-999): Part annexed"), row["row_text"])
        require(row["county_name"] == "Test & Sons County, Virginia" and "  " not in row["row_text"], "tags or spaces left")

    recorder.scenario("parse_entities_tags_and_white_space", entities_and_tags)

    def decoded_entities() -> None:
        html = base_pages()[1990].decode().replace("Test &amp; Sons County", "O&#39;Neil &quot;Test&quot; &amp; Sons&nbsp;County")
        row = next(item for item in grammar.parse_page(html, 1990) if item["county_fips"] == "51999")
        require(row["county_name"] == 'O\'Neil "Test" & Sons County, Virginia', row["county_name"])

    recorder.scenario("parse_decodes_the_supported_entities", decoded_entities)

    def layout_independent() -> None:
        html = base_pages()[1990].decode()
        same_line = html.replace("effective July 22, 1997.</li>\n<li>Miami-Dade", "effective July 22, 1997.</li><li>Miami-Dade")
        joined = same_line.replace("\n", "").replace("<br />", "<br />\n")
        crlf = html.replace("\n", "\r\n")
        require(same_line != html and joined != same_line and crlf != html, "the variants did not change the page")
        for variant in (same_line, joined, crlf):
            require(grammar.parse_page(variant, 1990) == grammar.parse_page(html, 1990), "layout changed the events")
            require(grammar.change_list_digest(variant, 1990) == grammar.change_list_digest(html, 1990), "layout changed the change-list digest")

    recorder.scenario("parse_items_on_one_line_and_other_layouts_equal_the_plain_layout", layout_independent)

    def refuses_markup(name: str, edit: Callable[[str], str], fragment: str, year: int = 1980) -> None:
        def action() -> None:
            html = edit(base_pages()[year].decode())
            try:
                grammar.parse_page(html, year)
            except grammar.ParseError as error:
                require(fragment in str(error), f"{error}")
                return
            raise AssertionError("the parser accepted the markup")

        recorder.scenario(f"parse_refuses_{name}", action)

    refuses_markup("a_comment_in_the_change_list", lambda h: h.replace("</h4>", "</h4><!-- a note -->", 1), "markup '<!-- a note -->'")
    refuses_markup("a_script_in_the_change_list", lambda h: h.replace("</h4>", "</h4><script>var a = '<li>';</script>", 1), "markup '<script>'")
    refuses_markup("a_style_in_the_change_list", lambda h: h.replace("</h4>", "</h4><style>li { color: red }</style>", 1), "markup '<style>'")
    refuses_markup("a_self_closing_item", lambda h: h.replace(DELETED + "\n<ul>", DELETED + "\n<ul><li/>"), "markup '<li/>'")
    refuses_markup("an_attribute_value_with_a_tag_character", lambda h: h.replace("<li>DeKalb", '<li title="a>b">DeKalb'), "is not in the change-list grammar")
    refuses_markup("a_tag_character_in_the_text", lambda h: h.replace("from De Kalb", "from De < Kalb"), "is not in the change-list grammar")
    refuses_markup("an_unsupported_named_entity", lambda h: h.replace("from De Kalb", "from D&eacute; Kalb"), "reference '&eacute;'")
    refuses_markup("an_unsupported_numeric_entity", lambda h: h.replace("from De Kalb", "from D&#233; Kalb"), "reference '&#233;'")
    refuses_markup("an_ampersand_before_a_letter", lambda h: h.replace("from De Kalb", "from De&Kalb"), "reference '&Kalb")
    refuses_markup("an_entity_without_its_semicolon", lambda h: h.replace("from De Kalb", "from De&nbsp Kalb"), "reference '&nbsp")
    refuses_markup("an_entity_that_re_forms_a_reference", lambda h: h.replace("from De Kalb", "from De&amp;nbsp; Kalb"), "re-forms a reference")
    refuses_markup("a_tag_that_starts_with_an_allowed_name", lambda h: h.replace("<br />", "<br-note></br-note>", 1), "markup '<br-note>'")
    refuses_markup("an_end_tag_with_a_space", lambda h: h.replace("1986.</li>", "1986.</li >"), "markup '</li >'")
    refuses_markup("an_attribute_on_an_item", lambda h: h.replace("<li>DeKalb", '<li class="x">DeKalb'), "markup '<li class=\"x\">'")
    refuses_markup(
        "an_attribute_on_a_section_heading", lambda h: h.replace("<h4><strong>Deleted", '<h4 class="x"><strong>Deleted'), "markup '<h4 class=\"x\">'"
    )
    refuses_markup("a_character_outside_the_allowed_set", lambda h: h.replace("from De Kalb", "from De\u00a0Kalb"), "a character outside the allowed set")
    refuses_markup("a_second_decade_heading", lambda h: h.replace("<h4>Related pages</h4>", pages_heading(1980)), "decade heading occurs 2 times")
    refuses_markup("text_between_items", lambda h: h.replace("<ul>\n<li>DeKalb", "<ul>text\n<li>DeKalb"), "text 'text'")
    refuses_markup(
        "a_second_colon_in_the_alaska_created_list", lambda h: h.replace("are: Dillingham", "are: Fairbanks (02-090): Dillingham"), "another colon", 1970
    )

    def unbalanced_frame_tag() -> None:
        html = base_pages()[1980].decode()
        open_div = html.replace('<section style="overflow: none;">', '<section style="overflow: none;"><div>', 1)
        stray_close = html.replace("</ul>", "</ul></p></div>", 1)
        require(open_div != html and stray_close != html, "the variants did not change the page")
        for variant in (open_div, stray_close):
            require(grammar.parse_page(variant, 1980) == grammar.parse_page(html, 1980), "a frame tag changed the events")
            require(grammar.change_list_digest(variant, 1980) == grammar.change_list_digest(html, 1980), "a frame tag changed the digest")

    recorder.scenario("parse_frame_tags_need_not_match_and_change_nothing", unbalanced_frame_tag)

    def heading_without_bold() -> None:
        html = base_pages()[1980].decode()
        plain = html.replace(DELETED, "<h4>Deleted Counties or County Equivalent Entities</h4>")
        require(plain != html, "the variant did not change the page")
        require(grammar.parse_page(plain, 1980) == grammar.parse_page(html, 1980), "the bold wrapper changed the events")
        require(grammar.change_list_digest(plain, 1980) == grammar.change_list_digest(html, 1980), "the bold wrapper changed the digest")

    recorder.scenario("parse_heading_bold_wrapper_is_optional_and_changes_nothing", heading_without_bold)

    def in_change_list(old: str, new: str) -> Callable[[str], str]:
        def edit(html: str) -> str:
            head, tail = html.split("</h3>", 1)
            return head + "</h3>" + tail.replace(old, new, 1)

        return edit

    refuses_markup("an_unclosed_heading", in_change_list("</strong></h4>", "</strong>"), "is not allowed here")
    refuses_markup("an_unclosed_item", in_change_list("</li>", ""), "is not allowed here")
    refuses_markup("an_unclosed_list", in_change_list("</ul>", ""), "is not allowed here")
    refuses_markup("an_unclosed_bold_wrapper", in_change_list("</strong></h4>", "</h4>"), "is not allowed here")

    def section_is_self_sufficient() -> None:
        html = base_pages()[1990].decode()
        section = grammar.page_section(html, 1990)
        require(section.startswith("<h3") and "</section>" not in section and "Footer link" not in section, f"{section[:80]!r}")
        events, digest = grammar.analyze_section(section, 1990)
        require(events == grammar.parse_page(html, 1990) and digest == grammar.change_list_digest(html, 1990), "the section gives other events than the page")
        for name, damaged, fragment in (
            ("text before the heading", " " + section, "does not start with the decade heading"),
            ("another decade", section.replace("1990s", "1980s"), "names 1980s"),
            ("no heading", section.split("</h3>", 1)[1], "occurs 0 times"),
            ("a closing section tag", section + "</section>", "markup '</section>'"),
        ):
            try:
                grammar.analyze_section(damaged, 1990)
            except grammar.ParseError as error:
                require(fragment in str(error), f"{name}: {error}")
            else:
                raise AssertionError(f"the section with {name} was accepted")

    recorder.scenario("parse_section_is_self_sufficient_and_refuses_what_is_not_a_section", section_is_self_sufficient)

    def page_check_reads_the_file_once() -> None:
        revised = base_pages()[1990].replace(b"effective July 1, 1994", b"effective July 2, 1994")
        with tempfile.TemporaryDirectory(prefix="county_changes_read_once_") as scratch:
            path = Path(scratch) / "county-changes.1990.html"
            path.write_bytes(base_pages()[1990])
            digest = grammar.change_list_digest(base_pages()[1990].decode(), 1990)
            with read_race(path.name, revised, before_read=2) as reads:
                outcome = grammar.page_problem(path, 1990, digest)
        require(reads[0] == 1 and outcome == (None, digest), f"{reads[0]} reads, {outcome}")

    recorder.scenario("page_check_reads_the_file_once_and_uses_what_it_read", page_check_reads_the_file_once)

    def outside_is_not_read() -> None:
        html = base_pages()[1980].decode()
        before = "<!-- open <script>if (a<b && c>d) {} </script><ul><li-note onclick"
        noisy = html.replace("<ul><li onclick", before, 1) + "<style>a > b {}</style><!-- never closed"
        require(noisy != html and grammar.parse_page(noisy, 1980) == grammar.parse_page(html, 1980), "markup outside the change list changed the events")

    recorder.scenario("parse_reads_nothing_outside_the_change_list", outside_is_not_read)

    def glued_date() -> None:
        html = base_pages()[1980].decode().replace("effective June 2, 1986", "effective PreJune 2, 1986")
        row = next(item for item in grammar.parse_page(html, 1980) if item["county_fips"] == "02140")
        require(row["effective_date"] == "" and "PreJune 2, 1986" in row["row_text"], f"{row}")

    recorder.scenario("parse_reads_no_date_glued_to_a_letter", glued_date)

    def donor_excluded() -> None:
        rows = grammar.parse_page(base_pages()[1980].decode(), 1980)
        kobuk = next(item for item in rows if item["county_fips"] == "02140" and item["event"] == "deleted")
        require(kobuk["successor_fips"] == "02188", kobuk["successor_fips"])

    recorder.scenario("parse_donor_county_is_not_a_successor", donor_excluded)

    def footer_not_read() -> None:
        codes = {row["county_fips"] for year, body in base_pages().items() for row in grammar.parse_page(body.decode(), year)}
        require("99999" not in codes, "footer item read")

    recorder.scenario("parse_footer_and_navigation_not_read", footer_not_read)


class Uploads:
    """Stands in for the S3 upload and counts how often a capture reaches it."""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, *_args: Any, **_kwargs: Any) -> dict[str, str]:
        self.calls += 1
        return {"reconciliation_path": "reconciliation.json"}


class HistoryJob:
    """One county-change page job of the history route with its capture and upload replaced by stand-ins; ``run`` may be called again on the same state."""

    def __init__(self, root: Path, name: str, evidence: dict[str, Any]) -> None:
        self.directory = root / f"history_{name}"
        self.snapshot = self.directory / "jobs" / name / "captures" / "ADJ" / "snapshot"
        self.completed = self.directory / "jobs" / name / "completed.json"
        self.uploads = Uploads()
        self.job = {
            "job_id": name,
            "plan": {"source_id": "ADJ", "expected_format": "html"},
            "candidate": {"url": PAGE_URL.format(year=1990), "evidence": evidence},
        }

    def store_capture(self, body: bytes) -> Path:
        """Write the captured page and its receipt, as the capture step does, and return the receipt path."""
        (self.snapshot / "files").mkdir(parents=True, exist_ok=True)
        (self.snapshot / "files" / "county-changes.1990.html").write_bytes(body)
        receipt = {
            "snapshot_status": "acquired_unvalidated",
            "artifacts": [{"storage_path": "files/county-changes.1990.html"}],
            "quality_profile": {"checks_failed": []},
        }
        (self.snapshot / "receipt.json").write_text(json.dumps(receipt))
        return self.snapshot / "receipt.json"

    def run(self, body: bytes) -> dict[str, Any]:
        """Run the job; a fresh capture, if the job makes one, holds ``body``."""
        with (
            patch.object(history_routes, "capture", lambda *_args, **_kwargs: self.store_capture(body)),
            patch.object(history_routes, "validate_receipt", lambda *_args, **_kwargs: None),
            patch.object(history_routes, "upload_snapshot", self.uploads),
            patch.object(history_routes, "AwsCli", lambda _settings: None),
        ):
            return history_routes.run_job(self.job, {}, None, self.directory, {}, {})


def history_run(root: Path, name: str, body: bytes, evidence: dict[str, Any], captured_before: bool) -> tuple[dict[str, Any], Uploads, Path]:
    """Run the history route's job for one county-change page over a capture that is made now or found from an earlier run; return the event."""
    job = HistoryJob(root, name, evidence)
    if captured_before:
        job.store_capture(body)
    return job.run(body), job.uploads, job.completed


def fingerprint_scenarios(recorder: Recorder) -> None:
    """The page contract the history route executes sits in a top-level acquisition module, so the collector code fingerprints cover it."""

    def covered() -> None:
        hashes = bls_api_contract.code_hashes()
        path = Path(grammar.__file__).resolve().relative_to(bls_api_contract.REPO_ROOT).as_posix()
        require(path in hashes and path.count("/") == 2, f"{path} is not a top-level acquisition module in the fingerprint: {sorted(hashes)[:3]}")
        require(history_routes.page_problem is grammar.page_problem and tool.bytes_problem is grammar.bytes_problem, "a copy of the page contract is in use")
        require(not [name for name in vars(tool) if name in ("PageReader", "section_event", "parse_item")], "parser code remains in the one-off module")

    recorder.scenario("page_contract_is_in_a_fingerprinted_top_level_module", covered)


def candidate_scenarios(recorder: Recorder) -> None:
    """The committed candidates describe the route the pages take: reference documents of the existing source and the reviewed seed."""

    def decision_names_the_route() -> None:
        path = bls_api_contract.REPO_ROOT / "config/acquisition/census_county_change_candidates.json"
        decisions = {item["evidence"]["decision"] for item in json.loads(path.read_text(encoding="utf-8"))["candidates"]}
        require(len(decisions) == 1, f"{len(decisions)} different decisions")
        text = decisions.pop()
        require("reference documents" in text and "census_county_change_events" in text, text)
        stale = [phrase for phrase in ("bronze table", "staging model", "registered source") if phrase in text]
        require(not stale, f"the decision still says {stale}")

    recorder.scenario("candidates_name_the_reference_document_route_and_the_seed", decision_names_the_route)


def history_scenarios(recorder: Recorder, root: Path) -> None:
    """The history route that stores the pages in the bucket refuses a page whose change list differs from the candidate's pin."""
    page = base_pages()[1990]
    digest = grammar.change_list_digest(page.decode(), 1990)

    def stores_pinned_page() -> None:
        event, uploads, completed = history_run(root, "pinned", page, {"change_list_sha256": digest}, captured_before=False)
        require(event["status"] == "stored_unvalidated" and uploads.calls == 1 and completed.is_file(), f"{event}")

    recorder.scenario("history_route_stores_a_page_with_the_pinned_change_list", stores_pinned_page)

    def refuses(name: str, body: bytes, evidence: dict[str, Any], captured_before: bool, reason: str) -> Callable[[], None]:
        def action() -> None:
            event, uploads, completed = history_run(root, name, body, evidence, captured_before)
            require(event["status"] == "blocked_download" and event["reasons"] == [reason], f"{event}")
            require(uploads.calls == 0 and not completed.exists(), "a refused page was stored")

        return action

    wrong = {"change_list_sha256": "0" * 64}
    recorder.scenario(
        "history_route_refuses_a_new_capture_whose_change_list_differs_from_the_pin",
        refuses("wrong_fresh", page, wrong, False, "change_list_differs_from_pin"),
    )
    recorder.scenario(
        "history_route_refuses_a_reused_capture_whose_change_list_differs_from_the_pin",
        refuses("wrong_reused", page, wrong, True, "change_list_differs_from_pin"),
    )
    error_page = b"<html><body>" + b"not found " * 8000 + b"</body></html>"
    recorder.scenario(
        "history_route_refuses_a_page_without_the_decade_heading",
        refuses("no_heading", error_page, {"change_list_sha256": digest}, False, "decade_heading_or_layout_differs"),
    )

    def leaves_unpinned_candidates() -> None:
        event, uploads, _ = history_run(root, "no_evidence", error_page, {"source": "synthetic"}, captured_before=False)
        require(event["status"] == "stored_unvalidated" and uploads.calls == 1, f"{event}")

    recorder.scenario("history_route_leaves_a_candidate_without_change_list_evidence_alone", leaves_unpinned_candidates)

    def completed(name: str, evidence: dict[str, Any]) -> HistoryJob:
        job = HistoryJob(root, name, evidence)
        stored = job.run(page)
        require(stored["status"] == "stored_unvalidated" and job.completed.is_file() and job.uploads.calls == 1, f"{stored}")
        return job

    def completed_page_still_passes() -> None:
        job = completed("done_passes", {"change_list_sha256": digest})
        again = job.run(page)
        require(again["status"] == "already_completed" and job.uploads.calls == 1, f"{again}")

    recorder.scenario("history_route_reports_a_completed_page_that_still_passes_as_completed", completed_page_still_passes)

    def refuses_completed(name: str, stored: Callable[[HistoryJob], object], reason: str) -> Callable[[], None]:
        def action() -> None:
            job = completed(name, {"change_list_sha256": digest})
            record = job.completed.read_bytes()
            stored(job)
            event = job.run(page)
            require(event["status"] == "blocked_download" and event["reasons"] == [reason], f"{event}")
            require(job.uploads.calls == 1 and job.completed.read_bytes() == record, "a refused completed page was uploaded again or its record changed")

        return action

    def stored_file(job: HistoryJob) -> Path:
        return job.snapshot / "files" / "county-changes.1990.html"

    outside = page.replace(b"<br />", b"<br-note></br-note>", 1)
    revised = page.replace(b"effective July 1, 1994", b"effective July 2, 1994")
    recorder.scenario(
        "history_route_refuses_a_completed_page_the_current_grammar_refuses",
        refuses_completed("done_grammar", lambda job: stored_file(job).write_bytes(outside), "decade_heading_or_layout_differs"),
    )
    recorder.scenario(
        "history_route_refuses_a_completed_page_whose_capture_no_longer_has_the_pinned_change_list",
        refuses_completed("done_pin", lambda job: stored_file(job).write_bytes(revised), "change_list_differs_from_pin"),
    )
    recorder.scenario(
        "history_route_refuses_a_completed_page_whose_capture_is_gone",
        refuses_completed("done_gone", lambda job: stored_file(job).unlink(), "stored_page_capture_missing"),
    )

    def completed_unpinned_candidate_is_left_alone() -> None:
        job = completed("done_unpinned", {"source": "synthetic"})
        stored_file(job).write_bytes(outside)
        again = job.run(page)
        require(again["status"] == "already_completed" and job.uploads.calls == 1, f"{again}")

    recorder.scenario("history_route_leaves_a_completed_candidate_without_change_list_evidence_alone", completed_unpinned_candidate_is_left_alone)


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    recorder = Recorder()
    with tempfile.TemporaryDirectory(prefix="county_changes_e2e_") as directory:
        root = Path(directory)
        fetch_scenarios(recorder, root)
        grammar_scenarios(recorder)
        history_scenarios(recorder, root)
        fingerprint_scenarios(recorder)
        candidate_scenarios(recorder)
    passed = all(item["passed"] for item in recorder.scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "feature": "Census county-change pages: bounded fetch with hashes and a change-list pin, closed-grammar reader, refusal of stored pages",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_county_changes_e2e --output {args.output}",
        "boundary": (
            "Synthetic pages served by a fake opener through the real downloader; the history route runs its real job code with the receipt validator, "
            "the capture and the S3 upload replaced by stand-ins; no live census.gov, S3 or AWS."
        ),
        "cleanup": "Temporary folder removed on exit.",
        "scenarios": recorder.scenarios,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(report, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(
        json.dumps(
            {
                "status": report["status"],
                "passed": sum(item["passed"] for item in recorder.scenarios),
                "total": len(recorder.scenarios),
                "artifact": str(args.output),
            }
        )
        + "\n"
    )
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
