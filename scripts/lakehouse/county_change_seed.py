"""Write and check the seed of the Census county-change events, built from the four committed, pinned sections by the one Python reader.

Run from the repository root:

    .venv/bin/python -m scripts.lakehouse.county_change_seed --check                         # fail when the sections, the seed or the golden file drift
    .venv/bin/python -m scripts.lakehouse.county_change_seed --write                         # write dbt/seeds/census_county_change_events.csv from the sections
    .venv/bin/python -m scripts.lakehouse.county_change_seed --print-golden                  # print the golden structure of the sections for review
    .venv/bin/python -m scripts.lakehouse.county_change_seed --extract --pages <folder>      # write the four sections from the fetched pages

A section is the decade heading and the change list of one Census page, the only part the events come from. The four sections are committed in
``config/acquisition/census_county_changes``; each must give the change-list digest pinned in ``config/acquisition/census_county_change_candidates.json``.
``scripts/acquisition/county_change_pages.py`` is the only reader. Its events become the seed, one row per event with the page, the change-list hash of the page
and the fields silver needs; silver reads the seed through ``ref`` and never the pages. ``--check`` needs no pages and no network: it verifies the sections
against the pins, regenerates the events and requires the seed to equal them byte for byte, and compares the regenerated events with the reviewed golden file
(``config/acquisition/census_county_change_golden.json``: the count per page and event type and every retirement row). ``--extract`` is the only mode that
reads pages: ``<folder>`` holds the four fetched pages (the fetch tool's output folder); nothing is written unless all four pass the page check and their pins.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
from collections import Counter
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any

from scripts.acquisition.county_change_pages import LAYOUT_REFUSAL, PAGE_YEARS, PIN_REFUSAL, RETIREMENTS, ParseError, analyze_section, decode_text, page_section
from scripts.acquisition.one_off.county_changes.county_changes import FetchError, page_specs

REPO_ROOT = Path(__file__).resolve().parents[2]
SEED = REPO_ROOT / "dbt/seeds/census_county_change_events.csv"
CANDIDATES = REPO_ROOT / "config/acquisition/census_county_change_candidates.json"
GOLDEN = REPO_ROOT / "config/acquisition/census_county_change_golden.json"
SECTIONS = REPO_ROOT / "config/acquisition/census_county_changes"
SECTION_NAME = "county-changes.{year}.section.html"
SEED_COLUMNS = (
    "page",
    "event_number",
    "change_list_sha256",
    "state",
    "county_fips",
    "county_name",
    "event_type",
    "effective_date",
    "successor_fips",
    "row_text",
)
GOLDEN_FIELDS = ("state", "county_fips", "event_type", "effective_date", "successor_fips", "page")
EVENT_TYPES = ("created", "deleted", "renamed_to", "renamed_from", "name_corrected", "boundary_change", "summary")
CODE = re.compile(r"[0-9]{5}")
Rows = list[dict[str, str]]


class SeedError(ValueError):
    """The pages, the pins or the events cannot give the seed; the generator writes nothing."""


def pins(candidates: Path = CANDIDATES) -> dict[int, str]:
    """Return the change-list pin of each page from the candidates file; a page without a pin stops the run."""
    found = {}
    for spec in page_specs(candidates):
        if spec["change_list_sha256"] is None:
            raise SeedError(f"{candidates.name}: the candidate of the {spec['page']} page has no change-list pin")
        found[spec["page"]] = spec["change_list_sha256"]
    return found


def find_pages(folder: Path) -> dict[int, Path]:
    """Return the one copy of each page below a folder; a missing or repeated page stops the run."""
    found = {}
    for year in PAGE_YEARS:
        matches = [path for path in sorted(folder.rglob(f"county-changes.{year}.html")) if path.is_file()]
        if len(matches) != 1:
            raise SeedError(f"{folder} holds {len(matches)} copies of county-changes.{year}.html; give a folder with exactly one")
        found[year] = matches[0]
    return found


def shown(path: Path) -> str:
    """Return a path relative to the repository when it is inside it."""
    return str(path.relative_to(REPO_ROOT)) if path.is_relative_to(REPO_ROOT) else str(path)


def section_names() -> dict[int, str]:
    """Return the file name of each page's section."""
    return {year: SECTION_NAME.format(year=year) for year in PAGE_YEARS}


def load_sections(folder: Path = SECTIONS) -> dict[int, str]:
    """Return the text of each committed section, each file read once; a missing, extra or empty file stops the run."""
    names = section_names()
    if not folder.is_dir():
        raise SeedError(f"{shown(folder)} is missing; run --extract")
    extra = sorted(item.name for item in folder.iterdir() if item.name not in names.values())
    if extra:
        raise SeedError(f"{shown(folder)} holds files that are not sections: {extra}")
    found = {}
    for year, name in names.items():
        path = folder / name
        if not path.is_file():
            raise SeedError(f"{shown(path)} is missing; run --extract")
        found[year] = decode_text(path.read_bytes())
        if not found[year].strip():
            raise SeedError(f"{shown(path)} is empty")
    return found


def events_from_sections(sections: dict[int, str], pinned: dict[int, str]) -> Rows:
    """Return the seed rows of the four sections in page order; a section refused by the page check or its pin stops the run."""
    rows: Rows = []
    for year in PAGE_YEARS:
        try:
            events, digest = analyze_section(sections[year], year)
        except ParseError as error:
            raise SeedError(f"the section of the {year} page is refused by the page check ({LAYOUT_REFUSAL}): {error}") from error
        if digest != pinned[year]:
            raise SeedError(f"the section of the {year} page is refused by the page check ({PIN_REFUSAL}): its digest is {digest}, the pin {pinned[year]}")
        for number, event in enumerate(events, start=1):
            dates = event["effective_date"].split(";") if event["effective_date"] else []
            rows.append(
                {
                    "page": str(year),
                    "event_number": str(number),
                    "change_list_sha256": digest,
                    "state": event["state"],
                    "county_fips": event["county_fips"],
                    "county_name": event["county_name"],
                    "event_type": event["event"],
                    "effective_date": dates[0] if len(dates) == 1 else "",
                    "successor_fips": event["successor_fips"],
                    "row_text": event["row_text"],
                }
            )
    return rows


def extract_sections(pages: dict[int, Path], pinned: dict[int, str]) -> dict[int, str]:
    """Return the section of each fetched page, each file read once; a page refused by the page check or its pin stops the run."""
    sections = {}
    for year in PAGE_YEARS:
        try:
            sections[year] = page_section(decode_text(pages[year].read_bytes()), year)
        except (ParseError, UnicodeDecodeError) as error:
            raise SeedError(f"{shown(pages[year])} is refused by the page check ({LAYOUT_REFUSAL}): {error}") from error
    events_from_sections(sections, pinned)
    return sections


def render(rows: Sequence[dict[str, str]]) -> str:
    """Return the seed text: Unix line endings and a fixed column order."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=SEED_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def golden_from(rows: Sequence[dict[str, str]]) -> dict[str, Any]:
    """Return the golden structure of the rows: the count per page and event type and every retirement row."""
    counts = {f"{page}:{event}": n for (page, event), n in sorted(Counter((row["page"], row["event_type"]) for row in rows).items())}
    return {"counts": counts, "retirements": [[row[column] for column in GOLDEN_FIELDS] for row in rows if row["event_type"] in RETIREMENTS]}


def golden_problems(rows: Sequence[dict[str, str]], golden: dict[str, Any], label: str = "seed") -> list[str]:
    """Return the differences between the rows and the reviewed golden file: counts per page and event type, and every retirement row."""
    found = golden_from(rows)
    problems = [
        f"count {key}: {label} {found['counts'].get(key, 0)}, golden {golden['counts'].get(key, 0)}"
        for key in sorted(set(found["counts"]) | set(golden["counts"]))
        if found["counts"].get(key) != golden["counts"].get(key)
    ]
    seed_rows, golden_rows = found["retirements"], golden["retirements"]
    if len(seed_rows) == len(golden_rows):
        problems += [f"retirement row {i + 1}: {label} {a}, golden {b}" for i, (a, b) in enumerate(zip(seed_rows, golden_rows, strict=True)) if a != b]
    else:
        problems.append(f"retirement rows: {label} {len(seed_rows)}, golden {len(golden_rows)}")
        problems += [f"retirement row missing from the {label}: {row}" for row in golden_rows if row not in seed_rows]
        problems += [f"retirement row not in the golden file: {row}" for row in seed_rows if row not in golden_rows]
    return problems


def row_problems(row: dict[str, str]) -> list[str]:
    """Return what is wrong with one seed row on its own."""
    where = f"page {row['page']} event {row['event_number']}"
    problems = []
    if row["event_type"] not in EVENT_TYPES:
        problems.append(f"{where}: event type {row['event_type']!r} is not one of {list(EVENT_TYPES)}")
    if row["event_type"] == "summary":
        if row["county_fips"] or row["state"]:
            problems.append(f"{where}: the summary row names a state or a county code")
    elif CODE.fullmatch(row["county_fips"]) is None or not row["state"] or not row["county_name"]:
        problems.append(
            f"{where}: county code {row['county_fips']!r}, state {row['state']!r} and name {row['county_name']!r} must all be present and the code five digits"
        )
    if row["effective_date"]:
        try:
            date.fromisoformat(row["effective_date"])
        except ValueError:
            problems.append(f"{where}: effective date {row['effective_date']!r} is not an ISO date")
    if row["successor_fips"] and any(CODE.fullmatch(code) is None for code in row["successor_fips"].split(";")):
        problems.append(f"{where}: successor codes {row['successor_fips']!r} are not five-digit codes joined by semicolons")
    return problems


def shape_problems(rows: Sequence[dict[str, str]], pinned: dict[int, str]) -> list[str]:
    """Return the problems of the seed as a whole: the pages present, one change-list hash per page equal to its pin, event numbers and row order."""
    problems = []
    present = [int(row["page"]) if row["page"].isdigit() else -1 for row in rows]
    if present != sorted(present):
        problems.append("rows are not in page order")
    if sorted(set(present)) != list(PAGE_YEARS):
        problems.append(f"pages in the seed {sorted(set(present))}, expected {list(PAGE_YEARS)}")
    for year in PAGE_YEARS:
        page_rows = [row for row in rows if row["page"] == str(year)]
        hashes = {row["change_list_sha256"] for row in page_rows}
        if page_rows and hashes != {pinned[year]}:
            problems.append(f"page {year}: change-list hashes {sorted(hashes)} differ from the pin {pinned[year]}")
        if [row["event_number"] for row in page_rows] != [str(n) for n in range(1, len(page_rows) + 1)]:
            problems.append(f"page {year}: event numbers are not 1 to {len(page_rows)} in file order")
    for row in rows:
        problems += row_problems(row)
    return problems


def read_seed_text(path: Path) -> str:
    """Return the text of a seed file, read once; a missing file stops the run."""
    if not path.is_file():
        raise SeedError(f"{path.name} is missing; run --write")
    return path.read_text()


def parse_seed(text: str, name: str) -> Rows:
    """Return the rows of a seed text; another header or a row with more fields than the header stops the run."""
    reader = csv.DictReader(io.StringIO(text, newline=""))
    header = tuple(reader.fieldnames or ())
    records = list(reader)
    if header != SEED_COLUMNS:
        raise SeedError(f"{name}: columns {list(header)} differ from {list(SEED_COLUMNS)}")
    if any(None in record for record in records):
        raise SeedError(f"{name}: a row has more fields than the header")
    return [{column: value or "" for column, value in record.items()} for record in records]


def read_seed(path: Path) -> Rows:
    """Return the rows of a seed file."""
    return parse_seed(read_seed_text(path), path.name)


def first_difference(seed_text: str, fresh_text: str) -> str:
    """Return the first line at which the seed and the regenerated text differ."""
    seed_lines, fresh_lines = seed_text.splitlines(), fresh_text.splitlines()
    for number, (a, b) in enumerate(zip(seed_lines, fresh_lines, strict=False), start=1):
        if a != b:
            return f"line {number}: seed {a[:120]!r}, regenerated {b[:120]!r}"
    return f"the seed has {len(seed_lines)} lines, the regeneration {len(fresh_lines)}"


def check(seed: Path = SEED, golden: Path = GOLDEN, candidates: Path = CANDIDATES, sections: Path = SECTIONS) -> tuple[list[str], str, int]:
    """Return the problems of the seed, how the regeneration went and the number of events; an empty problem list is a pass."""
    try:
        seed_text = read_seed_text(seed)
        rows = parse_seed(seed_text, seed.name)
        pinned = pins(candidates)
        golden_data = json.loads(golden.read_text(encoding="utf-8"))
    except (SeedError, FetchError, OSError, KeyError, json.JSONDecodeError) as error:
        return [str(error)], "not run", 0
    problems = shape_problems(rows, pinned) + golden_problems(rows, golden_data)
    try:
        fresh_rows = events_from_sections(load_sections(sections), pinned)
    except (SeedError, OSError) as error:
        return [*problems, str(error)], f"failed in {shown(sections)}", len(rows)
    problems += golden_problems(fresh_rows, golden_data, "regenerated events")
    fresh = render(fresh_rows)
    if fresh != seed_text:
        return (
            [*problems, f"{seed.name} differs from a fresh regeneration from {shown(sections)}: {first_difference(seed_text, fresh)}"],
            f"differs in {shown(sections)}",
            len(rows),
        )
    return problems, f"equal to the sections in {shown(sections)}", len(rows)


def write(seed: Path = SEED, golden: Path = GOLDEN, candidates: Path = CANDIDATES, sections: Path = SECTIONS) -> int:
    """Write the seed from the sections and return the event count; a refused section, a missing pin or events the golden file refuses write nothing."""
    rows = events_from_sections(load_sections(sections), pins(candidates))
    problems = golden_problems(rows, json.loads(golden.read_text(encoding="utf-8")), "regenerated events")
    if problems:
        raise SeedError(f"the events differ from {golden.name}; nothing written: {'; '.join(problems[:5])}")
    seed.parent.mkdir(parents=True, exist_ok=True)
    seed.write_bytes(render(rows).encode("utf-8"))
    return len(rows)


def extract(pages: Path, candidates: Path = CANDIDATES, sections: Path = SECTIONS) -> list[Path]:
    """Write the four sections from the fetched pages and return their paths; a page refused by the page check or its pin writes nothing."""
    found = extract_sections(find_pages(pages), pins(candidates))
    sections.mkdir(parents=True, exist_ok=True)
    written = []
    for year, name in section_names().items():
        (sections / name).write_bytes(found[year].encode("utf-8"))
        written.append(sections / name)
    return written


def main(argv: list[str] | None = None) -> int:
    """Write, check or print the golden structure of the seed, or extract the sections from fetched pages, and report the outcome."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true", help="write the seed from the four committed sections")
    mode.add_argument("--check", action="store_true", help="fail when the sections, the pins, the seed or the golden file drift")
    mode.add_argument("--print-golden", action="store_true", help="print the golden structure of the sections for review and write nothing")
    mode.add_argument("--extract", action="store_true", help="write the four sections from the fetched pages given with --pages")
    parser.add_argument("--pages", type=Path, help="a folder holding exactly one copy of each fetched page; only --extract reads it")
    args = parser.parse_args(argv)
    if args.extract != (args.pages is not None):
        parser.error("--extract needs --pages and no other mode takes it")
    try:
        if args.print_golden:
            sys.stdout.write(json.dumps(golden_from(events_from_sections(load_sections(), pins())), indent=1) + "\n")
            return 0
        if args.write:
            sys.stdout.write(f"county change seed: wrote {write()} events to {SEED.relative_to(REPO_ROOT)}\n")
            return 0
        if args.extract:
            sys.stdout.write(f"county change seed: wrote {len(extract(args.pages))} sections to {shown(SECTIONS)}\n")
            return 0
    except (SeedError, FetchError, OSError, KeyError, json.JSONDecodeError) as error:
        sys.stderr.write(f"county change seed: {error}\n")
        return 1
    problems, regeneration, events = check()
    for problem in problems:
        sys.stderr.write(f"county change seed: {problem}\n")
    sys.stdout.write(f"county change seed: {events} events; regeneration {regeneration}; {len(problems)} problems\n")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
