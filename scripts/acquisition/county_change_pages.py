"""The change-list grammar of the Census county-change pages and the one reader that extracts their events.

``page_problem`` refuses every page whose change list falls outside the closed grammar written in ``docs/acquisition_design.md`` (section "County-change page
grammar"); acquisition applies it before a page is stored and whenever a stored page is used again. A page's section is its decade heading and the change list
after it; the section is the part of a page that the repository commits, and ``analyze_section`` gives its events and its digest from one text. The events
become the reviewed dbt seed through ``scripts/lakehouse/county_change_seed.py``. The warehouse reads that seed and never the pages.
This file sits at the top level of ``scripts/acquisition`` so the collector code fingerprints cover it.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from datetime import date
from pathlib import Path

PAGE_YEARS = (1970, 1980, 1990, 2000)
FILE_PATTERN = re.compile(rf"county-changes\.({'|'.join(map(str, PAGE_YEARS))})\.html")
ATTRIBUTE_VALUE = '[^"<>&\\x00-\\x1f\\x7f]*'
HEADING_PATTERN = f'<h3(?: style="{ATTRIBUTE_VALUE}")?(?: class="{ATTRIBUTE_VALUE}")?>Changes to Counties or County Equivalent Entities: ([0-9]{{4}})s</h3>'
HEADING_TAG = re.compile(HEADING_PATTERN)
SECTION_END = "</section>"
LAYOUT_REFUSAL = "decade_heading_or_layout_differs"
PIN_REFUSAL = "change_list_differs_from_pin"
RETIREMENTS = ("deleted", "renamed_to")
EVENT_FIELDS = ("state", "county_fips", "county_name", "event", "effective_date", "successor_fips", "page", "row_text")
STATES = {
    "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas", "06": "California", "08": "Colorado", "09": "Connecticut", "10": "Delaware",
    "11": "District of Columbia", "12": "Florida", "13": "Georgia", "15": "Hawaii", "16": "Idaho", "17": "Illinois", "18": "Indiana", "19": "Iowa",
    "20": "Kansas", "21": "Kentucky", "22": "Louisiana", "23": "Maine", "24": "Maryland", "25": "Massachusetts", "26": "Michigan", "27": "Minnesota",
    "28": "Mississippi", "29": "Missouri", "30": "Montana", "31": "Nebraska", "32": "Nevada", "33": "New Hampshire", "34": "New Jersey", "35": "New Mexico",
    "36": "New York", "37": "North Carolina", "38": "North Dakota", "39": "Ohio", "40": "Oklahoma", "41": "Oregon", "42": "Pennsylvania", "44": "Rhode Island",
    "45": "South Carolina", "46": "South Dakota", "47": "Tennessee", "48": "Texas", "49": "Utah", "50": "Vermont", "51": "Virginia", "53": "Washington",
    "54": "West Virginia", "55": "Wisconsin", "56": "Wyoming", "72": "Puerto Rico",
}  # fmt: skip
MONTHS = {
    name: number
    for number, name in enumerate(("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"), 1)
}
DATE_PATTERN = r"\b(?:" + "|".join(MONTHS) + r") [0-9]{1,2}, [12][0-9]{3}\b"
DATE = re.compile(DATE_PATTERN, re.ASCII)
CODE_PATTERN = r"\([0-9]{2}-[0-9]{3}\)"
CODE = re.compile(CODE_PATTERN)
NARRATIVE_ENTITY_PATTERN = r"[A-Z][A-Za-z\- ]+? \([0-9]{2}-[0-9]{3}\)"
NARRATIVE_ENTITY = re.compile(NARRATIVE_ENTITY_PATTERN)
NARRATIVE_SKIPPED = "The county equivalent entities in Alaska changed from Census Divisions in 1970"
NARRATIVE_CREATED = "New entities created in Alaska are:"
TO_CREATE = " to create "
SECTION_PREFIXES = (
    ("new counties or county equivalent entities", "created"),
    ("deleted counties or county equivalent entities", "deleted"),
    ("deleted county equivalent entities", "deleted"),
    ("name and/or code changes or corrections", "name_or_code_change"),
    ("substantial county or county equivalent", "boundary_change"),
    ("changes to county equivalent entities in alaska", "narrative"),
)
NAME_CHANGES = (("Renamed as", "renamed_to"), ("Renamed from", "renamed_from"), ("Name corrected", "name_corrected"))
ENTITIES = {"&amp;": "&", "&quot;": '"', "&apos;": "'", "&#39;": "'", "&nbsp;": " "}
TOKEN = re.compile(r"<[^<>]*>|&[^<>&;]*;?|[^<>&]+")
TEXT_OUTSIDE = re.compile(r"[^\t\n\r\x20-\x25\x27-\x3b\x3d\x3f-\x7e\u00c0-\u024f]")
HEADING_OUTSIDE = re.compile(r"[^\t\n\r\x20-\x25\x27-\x3b\x3d\x3f-\x7e]")
ENTITY_LIKE = re.compile(r"&[#A-Za-z]")
BLANK = re.compile(r"[ \t\r\n]*")
TAGS = {
    "h4": "<h4>", "/h4": "</h4>", "b": "<b>", "/b": "</b>", "strong": "<strong>", "/strong": "</strong>", "ul": "<ul>", "/ul": "</ul>",
    "li": "<li>", "/li": "</li>", "br": "<br />", "/div": "</div>", "/p": "</p>",
}  # fmt: skip
ATTRIBUTE_TAGS = {
    "div": re.compile(f'<div(?: style="{ATTRIBUTE_VALUE}")?(?: class="{ATTRIBUTE_VALUE}")?>'),
    "p": re.compile(f'<p(?: class="{ATTRIBUTE_VALUE}")?(?: style="{ATTRIBUTE_VALUE}")?>'),
    "section": re.compile(f'<section(?: style="{ATTRIBUTE_VALUE}")?>'),
}
FRAME = frozenset({"div", "/div", "section", "p", "/p"})
WRAPPERS = ("b", "strong")


class ParseError(ValueError):
    """A page differs from the grammar; the message names the item text or the markup."""


def tag_name(token: str) -> str:
    """Return the grammar's name for a complete tag, or refuse markup that is not one of the allowed tags."""
    for name, text in TAGS.items():
        if token == text:
            return name
    for name, pattern in ATTRIBUTE_TAGS.items():
        if pattern.fullmatch(token):
            return name
    raise ParseError(f"markup {token[:40]!r} is not in the change-list grammar")


def collapse(parts: Sequence[str], heading: bool = False) -> str:
    text = " ".join("".join(parts).split())
    if ENTITY_LIKE.search(text):
        raise ParseError(f"a decoded entity re-forms a reference in {'the section heading' if heading else 'the item'}: {text[:60]}")
    return text


def blank(text: str, where: str) -> None:
    if BLANK.fullmatch(text) is None:
        raise ParseError(f"text {text.strip()[:40]!r} {where}")


def decade_heading(text: str, page: int) -> re.Match[str]:
    """Return the decade heading element of a page or section; it must occur exactly once and name the decade of the page."""
    headings = list(HEADING_TAG.finditer(text))
    if len(headings) != 1:
        raise ParseError(f"page {page}: the decade heading occurs {len(headings)} times")
    if headings[0].group(1) != f"{page // 10 * 10}":
        raise ParseError(f"page {page}: the decade heading names {headings[0].group(1)}s")
    return headings[0]


def page_section(html: str, page: int) -> str:
    """Return the section of a page: the decade heading element and the text after it up to the first closing section tag."""
    heading = decade_heading(html, page)
    end = html.find(SECTION_END, heading.end())
    if end < 0:
        raise ParseError(f"page {page}: the closing section tag is missing")
    return html[heading.start() : end]


def read_sections(region: str) -> list[tuple[str, list[str]]]:
    """Read the change list with the fixed state machine of the grammar: frame tags, then per section a heading, a list and its items."""
    sections: list[tuple[str, list[str]]] = []
    parts: list[str] = []
    state, wrapper, position = "frame", "", 0
    while position < len(region):
        token = TOKEN.match(region, position)
        if token is None:
            raise ParseError(f"{region[position : position + 40]!r} is not in the change-list grammar")
        position = token.end()
        text = token.group(0)
        if text[0] == "<":
            name = tag_name(text)
            if state == "frame" and name in FRAME:
                continue
            if state == "frame" and name == "h4":
                state, parts, wrapper = "h4", [], ""
            elif state == "h4" and name in WRAPPERS and not wrapper and not "".join(parts).strip():
                wrapper, parts = name, []
            elif state == "h4" and wrapper and name == f"/{wrapper}":
                state = "h4_closed"
            elif state in ("h4", "h4_closed") and name == "/h4" and (wrapper == "" or state == "h4_closed"):
                sections.append((collapse(parts, True), []))
                state = "after_h4"
            elif state == "after_h4" and name == "ul":
                state = "list"
            elif state == "list" and name == "li":
                state, parts = "item", []
            elif state == "list" and name == "/ul":
                state = "frame"
            elif state == "item" and name == "br":
                parts.append(" ")
            elif state == "item" and name == "/li":
                sections[-1][1].append(collapse(parts))
                state = "list"
            else:
                raise ParseError(f"{text} is not allowed here (inside {state})")
            continue
        if text[0] == "&":
            if text not in ENTITIES:
                raise ParseError(f"reference {text[:20]!r} is not one of the supported entities")
            value = ENTITIES[text]
        else:
            if TEXT_OUTSIDE.search(text) or (state.startswith("h4") and HEADING_OUTSIDE.search(text)):
                raise ParseError(f"a character outside the allowed set in {text[:40]!r}")
            value = text
        if state in ("h4", "item"):
            parts.append(value)
        else:
            blank(text, f"in {state}")
    if state != "frame":
        raise ParseError(f"the change list ends inside {state}")
    return sections


def section_event(heading: str) -> str | None:
    """Return the event kind a section heading gives its items; ``narrative`` marks the 1970 Alaska summary list."""
    text = " ".join(heading.lower().split())
    for prefix, kind in SECTION_PREFIXES:
        if text.startswith(prefix):
            return kind
    return None


def read_section(section: str, page: int) -> list[tuple[str, list[str]]]:
    """Read a section: it starts with the decade heading, which names the decade of the page, and the rest is the change list."""
    heading = decade_heading(section, page)
    if heading.start() != 0:
        raise ParseError(f"page {page}: the section does not start with the decade heading")
    return read_sections(section[heading.end() :])


def digest_of(sections: Sequence[tuple[str, list[str]]]) -> str:
    """Return the SHA-256 of the section headings and list items: the part of a page that carries the changes.

    The Census page bytes differ on every request (element identifiers and script file hashes are regenerated), so the raw hash cannot be pinned.
    """
    return hashlib.sha256(json.dumps(sections, ensure_ascii=True, sort_keys=True).encode("ascii")).hexdigest()


def iso_dates(text: str) -> list[str]:
    """Return each distinct full date in the text as an ISO date, ascending."""
    found = set()
    for match in DATE.finditer(text):
        month, day, year = match.group(0).split(" ")
        try:
            found.add(date(int(year), MONTHS[month], int(day.rstrip(","))).isoformat())
        except ValueError as error:
            raise ParseError(f"impossible date in: {text}") from error
    return sorted(found)


def successor_codes(detail: str, own: str) -> list[str]:
    """Codes named after the last 'to create' (the entities the area went to) or, without it, in the whole detail; own code excluded, text order, distinct."""
    scope = detail.rsplit(TO_CREATE, 1)[-1] if TO_CREATE in detail else detail
    codes = [code[1:3] + code[4:7] for code in CODE.findall(scope)]
    return [code for index, code in enumerate(codes) if code != own and code not in codes[:index]]


def entity_event(kind: str, detail: str, text: str) -> str:
    """Resolve the name-and-code section's items by their wording; any other wording stops the parse."""
    if kind != "name_or_code_change":
        return kind
    for prefix, event in NAME_CHANGES:
        if detail.startswith(prefix):
            return event
    raise ParseError(f"unrecognised name or code change: {text}")


def parse_item(text: str, kind: str, page: int) -> dict[str, str]:
    """Return one event row from a list item that starts with the entity and its (SS-CCC) code."""
    first = CODE.search(text)
    if first is None:
        raise ParseError(f"item without a county code: {text}")
    state_code, fips = first.group(0)[1:3], first.group(0)[1:3] + first.group(0)[4:7]
    if state_code not in STATES:
        raise ParseError(f"unknown state code {state_code}: {text}")
    name = text[: first.start()].strip().rstrip(",").strip()
    if not name:
        raise ParseError(f"item without a name: {text}")
    state = STATES[state_code]
    tail = name.rsplit(",", 1)
    if len(tail) == 2 and tail[1].strip() in STATES.values() and tail[1].strip() != state:
        raise ParseError(f"state {tail[1].strip()} differs from the code {state_code}: {text}")
    rest = text[first.end() :]
    separator = re.search(r"[:;]", rest)
    detail = rest[separator.end() :].strip() if separator else ""
    event = entity_event(kind, detail, text)
    dates = iso_dates(detail)
    successors = successor_codes(detail, fips) if event in RETIREMENTS else []
    if event in RETIREMENTS and len(dates) > 1:
        raise ParseError(f"retirement with {len(dates)} dates: {text}")
    if event in RETIREMENTS and detail and not successors:
        raise ParseError(f"retirement with a detail and no successor: {text}")
    return {
        "state": state,
        "county_fips": fips,
        "county_name": name,
        "event": event,
        "effective_date": ";".join(dates),
        "successor_fips": ";".join(successors),
        "page": str(page),
        "row_text": text,
    }


def narrative_events(text: str, page: int) -> list[dict[str, str]]:
    """Return the events of one Alaska narrative item: the summary, or one created event per entity of the 'New entities created' list."""
    if text.startswith(NARRATIVE_SKIPPED):
        return [dict.fromkeys(EVENT_FIELDS, "") | {"event": "summary", "page": str(page), "row_text": text}]
    if not text.startswith(NARRATIVE_CREATED):
        raise ParseError(f"unrecognised summary item: {text}")
    if text.count(":") != 1 or re.search("[0-9]", CODE.sub("", text)):
        raise ParseError(f"created list with another colon or a digit outside the codes: {text}")
    events = []
    for match in NARRATIVE_ENTITY.finditer(text.split(":", 1)[1]):
        name, code = match.group(0)[:-9], match.group(0)[-8:]
        if code[1:3] not in STATES:
            raise ParseError(f"unknown state code {code[1:3]}: {text}")
        events.append(
            dict.fromkeys(EVENT_FIELDS, "")
            | {"state": STATES[code[1:3]], "county_fips": code[1:3] + code[4:7], "county_name": name, "event": "created", "page": str(page), "row_text": text}
        )
    return events


def events_of(sections: Sequence[tuple[str, list[str]]], page: int) -> list[dict[str, str]]:
    """Return the page's change events in page order; a change list outside the grammar stops the parse."""
    if not sections:
        raise ParseError(f"page {page}: the sections are missing")
    events: list[dict[str, str]] = []
    for heading, items in sections:
        kind = section_event(heading)
        if kind is None:
            raise ParseError(f"page {page}: unknown section {heading!r}")
        for text in items:
            events += narrative_events(text, page) if kind == "narrative" else [parse_item(text, kind, page)]
    if not any(row["event"] != "summary" for row in events):
        raise ParseError(f"page {page}: no change event")
    keys = [(row["page"], row["county_fips"], row["event"], row["row_text"]) for row in events]
    if len(set(keys)) != len(keys):
        raise ParseError(f"page {page}: an event appears twice")
    return events


def analyze_section(section: str, page: int) -> tuple[list[dict[str, str]], str]:
    """Return the events and the change-list digest of a section, both from one reading of its text; a section outside the grammar stops the parse."""
    sections = read_section(section, page)
    return events_of(sections, page), digest_of(sections)


def parse_page(html: str, page: int) -> list[dict[str, str]]:
    """Return the page's change events in page order; a page outside the grammar stops the parse."""
    return analyze_section(page_section(html, page), page)[0]


def change_list_digest(html: str, page: int) -> str:
    """Return the change-list digest of a page."""
    return digest_of(read_section(page_section(html, page), page))


def section_problem(section: str, page: int, pin: str | None) -> tuple[str | None, str | None]:
    """Return the reason a section is refused and its digest: an unreadable layout, or a change list that differs from the pin."""
    try:
        digest = analyze_section(section, page)[1]
    except ParseError:
        return LAYOUT_REFUSAL, None
    return (PIN_REFUSAL if pin not in (None, digest) else None), digest


def decode_text(raw: bytes) -> str:
    """Return the text of a UTF-8 file with every line ending read as LF; bytes that are not UTF-8 raise ``UnicodeDecodeError``."""
    return raw.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")


def html_problem(html: str, page: int, pin: str | None) -> tuple[str | None, str | None]:
    """Return the reason a page's text is refused and its change-list digest, from the one text given."""
    try:
        section = page_section(html, page)
    except ParseError:
        return LAYOUT_REFUSAL, None
    return section_problem(section, page, pin)


def bytes_problem(raw: bytes, page: int, pin: str | None) -> tuple[str | None, str | None]:
    """Return the reason a page's bytes are refused and its change-list digest, from the one buffer given."""
    try:
        html = decode_text(raw)
    except UnicodeDecodeError:
        return LAYOUT_REFUSAL, None
    return html_problem(html, page, pin)


def page_problem(path: Path, page: int, pin: str | None) -> tuple[str | None, str | None]:
    """Return the reason a stored page is refused and its change-list digest; the file is read once."""
    return bytes_problem(path.read_bytes(), page, pin)
