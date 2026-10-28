"""End-to-end run of the county-change seed generator and check: the committed seed and sections pass and each damaged copy fails for its intended cause.

Run from the repository root:

    .venv/bin/python -m scripts.lakehouse.run_county_change_seed_e2e --output <report.json> [--pages <folder holding the four real fetched pages>]

Part 1 copies the committed seed and sections, damages one thing per case and runs the check with the committed golden file and pins; a case passes only
when a problem names its intended cause. Part 2 builds synthetic pages through the fetch E2E's builder, takes their sections, pins them, writes the seed, checks
it, extracts the sections again from the pages and damages the sections, the pages, the pins and the golden file; it also races a file replacement against each
read. ``--pages`` adds the four real pages: they must give the committed sections byte for byte. No network, container or AWS.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import shutil
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition import county_change_pages as grammar
from scripts.acquisition import run_county_changes_e2e as pages_e2e
from scripts.lakehouse import county_change_seed as seed

SYNTHETIC_GOLDEN: dict[str, Any] = {
    "counts": {
        "1970:created": 3,
        "1970:deleted": 2,
        "1970:summary": 1,
        "1980:deleted": 1,
        "1980:name_corrected": 1,
        "1990:boundary_change": 1,
        "1990:deleted": 1,
        "1990:renamed_from": 1,
        "1990:renamed_to": 1,
        "2000:deleted": 1,
        "2000:name_corrected": 1,
    },
    "retirements": [
        ["Alaska", "02040", "deleted", "", "", "1970"],
        ["South Dakota", "46131", "deleted", "1979-01-01", "46071", "1970"],
        ["Alaska", "02140", "deleted", "1986-06-02", "02188", "1980"],
        ["Alaska", "02231", "deleted", "1992-09-22", "02232;02282", "1990"],
        ["Florida", "12025", "renamed_to", "1997-07-22", "12086", "1990"],
        ["Alaska", "02201", "deleted", "2008-05-19", "02130;02275;02198", "2000"],
    ],
}
REAL_EVENTS = 143
REAL_RETIREMENT_ROWS = 22


def renumbered(rows: seed.Rows) -> seed.Rows:
    """Return the rows with event numbers 1 to n per page again."""
    counters: dict[str, int] = {}
    out = []
    for row in rows:
        counters[row["page"]] = counters.get(row["page"], 0) + 1
        out.append({**row, "event_number": str(counters[row["page"]])})
    return out


def first_retirement(golden: dict[str, Any], code: str) -> int:
    """Return the index of the first retirement row of a county code in the golden file."""
    return next(index for index, row in enumerate(golden["retirements"]) if row[1] == code)


def first(rows: seed.Rows, **match: str) -> int:
    """Return the index of the first row with the given field values."""
    return next(index for index, row in enumerate(rows) if all(row[name] == value for name, value in match.items()))


class Suite:
    """Runs each case in a scratch folder and records whether it behaved as expected; a failing case does not stop the others."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.recorder = pages_e2e.Recorder()
        self.committed = seed.read_seed(seed.SEED)
        self.golden = json.loads(seed.GOLDEN.read_text(encoding="utf-8"))
        self.candidates = json.loads(seed.CANDIDATES.read_text(encoding="utf-8"))

    def refuses(
        self,
        name: str,
        rows: seed.Rows | str,
        fragment: str,
        golden: dict[str, Any] | None = None,
        candidates: dict[str, Any] | None = None,
        sections: Path | None = None,
    ) -> None:
        """Part 1: the check on a damaged copy of the committed seed must fail with a problem that names the intended cause."""

        def action() -> None:
            folder = self.root / name
            folder.mkdir()
            path = folder / "seed.csv"
            path.write_text(rows if isinstance(rows, str) else seed.render(rows))
            (folder / "golden.json").write_text(json.dumps(golden or self.golden))
            (folder / "candidates.json").write_text(json.dumps(candidates or self.candidates))
            problems, _, _ = seed.check(path, folder / "golden.json", folder / "candidates.json", sections or seed.SECTIONS)
            pages_e2e.require(any(fragment in problem for problem in problems), f"no problem names {fragment!r}: {problems[:3]}")

        self.recorder.scenario(name, action)

    def committed_sections(self, name: str) -> Path:
        """Return a scratch copy of the committed sections folder."""
        folder = self.root / f"{name}_sections"
        shutil.copytree(seed.SECTIONS, folder)
        return folder

    def damaged_sections(self, name: str, damage: Callable[[Path], object], fragment: str) -> None:
        """Part 1: a damaged copy of the committed sections must fail the check with a problem that names the intended cause."""
        folder = self.committed_sections(name)
        damage(folder)
        self.refuses(name, self.committed, fragment, sections=folder)

    def part_one(self) -> None:
        """The committed seed and sections pass and each damaged copy fails for its intended cause."""
        rows = self.committed

        def committed_passes() -> None:
            problems, regeneration, events = seed.check()
            pages_e2e.require(not problems and events == REAL_EVENTS and regeneration.startswith("equal"), f"{problems[:3]} {regeneration} {events}")

        self.recorder.scenario("seed_committed_passes_pins_golden_shapes_and_regeneration", committed_passes)

        def missing() -> None:
            problems, _, _ = seed.check(self.root / "absent.csv")
            pages_e2e.require(any("is missing" in problem for problem in problems), f"{problems}")

        self.recorder.scenario("seed_missing", missing)
        self.refuses("seed_header_changed", seed.render(rows).replace("row_text", "text", 1), "columns")
        self.refuses("seed_page_missing", [row for row in rows if row["page"] != "1980"], "pages in the seed")
        self.refuses("seed_rows_out_of_page_order", rows[::-1], "page order")
        index = first(rows, page="1980", event_type="boundary_change")
        self.refuses("seed_count_drift", renumbered(rows[:index] + rows[index + 1 :]), "count 1980:boundary_change: seed 29, golden 30")
        self.refuses(
            "seed_event_row_added",
            renumbered(rows[: index + 1] + rows[index : index + 1] + rows[index + 1 :]),
            "count 1980:boundary_change: seed 31, golden 30",
        )
        kobuk = first(rows, county_fips="02140", event_type="deleted")
        self.refuses("seed_retirement_row_changed", [{**row, "successor_fips": "02185"} if i == kobuk else row for i, row in enumerate(rows)], "retirement row")
        self.refuses("seed_retirement_row_removed", renumbered(rows[:kobuk] + rows[kobuk + 1 :]), "retirement row missing from the seed")
        self.refuses("seed_pin_differs", [{**row, "change_list_sha256": "0" * 64} if row["page"] == "1990" else row for row in rows], "differ from the pin")
        self.refuses(
            "seed_two_hashes_for_a_page", [{**row, "change_list_sha256": "0" * 64} if i == 0 else row for i, row in enumerate(rows)], "differ from the pin"
        )
        self.refuses("seed_event_number_gap", [{**row, "event_number": "99"} if i == 3 else row for i, row in enumerate(rows)], "event numbers are not 1 to")
        self.refuses("seed_bad_county_code", [{**row, "county_fips": "2230"} if i == 1 else row for i, row in enumerate(rows)], "county code '2230'")
        self.refuses("seed_bad_date", [{**row, "effective_date": "1979-02-30"} if i == kobuk else row for i, row in enumerate(rows)], "not an ISO date")
        self.refuses("seed_unknown_event_type", [{**row, "event_type": "merged"} if i == 1 else row for i, row in enumerate(rows)], "event type 'merged'")
        self.refuses("seed_bad_successor_code", [{**row, "successor_fips": "2188"} if i == kobuk else row for i, row in enumerate(rows)], "successor codes")
        yellowstone = first(rows, county_fips="30113", event_type="deleted")
        self.refuses(
            "seed_name_edited",
            [{**row, "county_name": "Fabricated County, Montana"} if i == yellowstone else row for i, row in enumerate(rows)],
            "differs from a fresh regeneration",
        )
        changed_golden = copy.deepcopy(self.golden)
        changed_golden["counts"]["1990:deleted"] = 4
        self.refuses("golden_count_changed", rows, "count 1990:deleted: seed 3, golden 4", golden=changed_golden)
        unpinned = copy.deepcopy(self.candidates)
        unpinned["candidates"][0]["evidence"]["change_list_sha256"] = None
        self.refuses("candidate_without_a_pin", rows, "has no change-list pin", candidates=unpinned)
        edited_golden = copy.deepcopy(self.golden)
        edited_golden["retirements"][first_retirement(self.golden, "02140")][4] = "02185"
        self.refuses(
            "seed_and_golden_edited_together",
            [{**row, "successor_fips": "02185"} if i == kobuk else row for i, row in enumerate(rows)],
            "differs from a fresh regeneration",
            golden=edited_golden,
        )
        self.refuses("seed_and_golden_edited_together_golden_differs_from_events", rows, "regenerated events", golden=edited_golden)
        self.part_one_sections()

    def part_one_sections(self) -> None:
        """Damaged copies of the committed sections."""
        page_1980 = "county-changes.1980.section.html"
        self.damaged_sections("section_missing", lambda folder: (folder / "county-changes.1990.section.html").unlink(), "is missing")
        self.damaged_sections("section_empty", lambda folder: (folder / "county-changes.2000.section.html").write_text(""), "is empty")
        self.damaged_sections("section_extra_file", lambda folder: (folder / "notes.txt").write_text("x"), "files that are not sections")
        self.damaged_sections(
            "section_for_another_decade", lambda folder: shutil.copy(folder / page_1980, folder / "county-changes.1970.section.html"), "names 1980s"
        )

        def edit_date(folder: Path) -> None:
            path = folder / page_1980
            text = path.read_text(encoding="utf-8")
            pages_e2e.require("effective June 2, 1986" in text, "the Kobuk date is not in the 1980 section")
            path.write_text(text.replace("effective June 2, 1986", "effective June 3, 1986"), encoding="utf-8")

        self.damaged_sections("section_edited_without_a_pin_change", edit_date, "change_list_differs_from_pin")

        def edited_with_the_seed_and_the_golden_file() -> None:
            folder = self.committed_sections("section_seed_and_golden_edited")
            edit_date(folder)
            edited_rows = [
                {**row, "effective_date": "1986-06-03", "row_text": row["row_text"].replace("June 2, 1986", "June 3, 1986")}
                if row["page"] == "1980" and "June 2, 1986" in row["row_text"]
                else row
                for row in self.committed
            ]
            edited_golden = copy.deepcopy(self.golden)
            for row in edited_golden["retirements"]:
                if row[1] == "02140":
                    row[3] = "1986-06-03"
            path = self.root / "section_seed_and_golden_edited.csv"
            path.write_text(seed.render(edited_rows))
            (self.root / "section_seed_and_golden_edited.golden.json").write_text(json.dumps(edited_golden))
            problems, regeneration, _ = seed.check(path, self.root / "section_seed_and_golden_edited.golden.json", seed.CANDIDATES, folder)
            pages_e2e.require(any("change_list_differs_from_pin" in problem for problem in problems) and regeneration.startswith("failed"), f"{problems}")

        self.recorder.scenario("section_seed_and_golden_edited_together_without_a_pin_change", edited_with_the_seed_and_the_golden_file)

        def pin_changed_golden_not() -> None:
            folder = self.committed_sections("section_and_pin_changed")
            edit_date(folder)
            edited = (folder / page_1980).read_text(encoding="utf-8")
            candidates = copy.deepcopy(self.candidates)
            for item in candidates["candidates"]:
                if item["url"].endswith("county-changes.1980.html"):
                    item["evidence"]["change_list_sha256"] = grammar.analyze_section(edited, 1980)[1]
            candidates_path = self.root / "section_and_pin_changed.candidates.json"
            candidates_path.write_text(json.dumps(candidates))
            path = self.root / "section_and_pin_changed.csv"
            path.write_text(seed.render(seed.events_from_sections(seed.load_sections(folder), seed.pins(candidates_path))))
            problems, regeneration, _ = seed.check(path, seed.GOLDEN, candidates_path, folder)
            pages_e2e.require(regeneration.startswith("equal"), f"{regeneration}")
            pages_e2e.require(any("retirement row" in problem and "golden" in problem for problem in problems), f"the golden file did not object: {problems}")

        self.recorder.scenario("section_and_pin_changed_the_golden_file_still_objects", pin_changed_golden_not)

        def crlf_lines() -> None:
            folder = self.committed_sections("section_crlf")
            for path in folder.iterdir():
                path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
            problems, regeneration, _ = seed.check(sections=folder)
            pages_e2e.require(not problems and regeneration.startswith("equal"), f"{problems} {regeneration}")

        self.recorder.scenario("section_with_crlf_lines_gives_the_same_events", crlf_lines)

    def synthetic(self, name: str, change: Callable[[dict[int, bytes]], dict[int, bytes]] | None = None) -> tuple[Path, Path, Path, Path, Path, Path]:
        """Write synthetic pages, their sections, the candidates file (pins of the unchanged pages), the golden file and a seed path; return the paths."""
        folder = self.root / name
        pages = folder / "pages"
        sections = folder / "sections"
        sections.mkdir(parents=True)
        good = pages_e2e.base_pages()
        for year, body in (change(dict(good)) if change else good).items():
            (pages / "captures" / f"attempt_{year}").mkdir(parents=True, exist_ok=True)
            (pages / "captures" / f"attempt_{year}" / f"county-changes.{year}.html").write_bytes(body)
        for year, body in good.items():
            (sections / seed.section_names()[year]).write_bytes(grammar.page_section(body.decode(), year).encode())
        pins: dict[int, str | None] = {year: grammar.change_list_digest(body.decode(), year) for year, body in good.items()}
        candidates = pages_e2e.candidates_file(folder / "candidates.json", pins)
        (folder / "golden.json").write_text(json.dumps(SYNTHETIC_GOLDEN))
        return pages, sections, candidates, folder / "golden.json", folder / "seed.csv", folder

    def part_two(self) -> None:
        """Synthetic pages: write and check from the sections, extract the sections again and damage the pages, the pins and the golden file."""

        def written_and_checked() -> None:
            _, sections, candidates, golden, path, _ = self.synthetic("written")
            count = seed.write(path, golden, candidates, sections)
            problems, regeneration, events = seed.check(path, golden, candidates, sections)
            pages_e2e.require(count == 14 and events == 14 and not problems and regeneration.startswith("equal"), f"{count} {problems} {regeneration}")

        self.recorder.scenario("write_then_check_passes_from_the_sections", written_and_checked)

        def idempotent() -> None:
            _, sections, candidates, golden, path, _ = self.synthetic("idempotent")
            seed.write(path, golden, candidates, sections)
            before = path.read_bytes()
            seed.write(path, golden, candidates, sections)
            pages_e2e.require(path.read_bytes() == before and before.endswith(b"\n") and b"\r" not in before, "a second write changed the seed")

        self.recorder.scenario("write_is_idempotent", idempotent)

        def refuses_golden_disagreement() -> None:
            _, sections, candidates, golden, path, _ = self.synthetic("golden_refuses")
            changed = copy.deepcopy(SYNTHETIC_GOLDEN)
            changed["retirements"][2][4] = "02188;02185"
            golden.write_text(json.dumps(changed))
            try:
                seed.write(path, golden, candidates, sections)
            except seed.SeedError as error:
                pages_e2e.require("nothing written" in str(error) and "retirement row 3" in str(error) and not path.exists(), f"{error}")
                return
            raise AssertionError("write accepted events the golden file refuses")

        self.recorder.scenario("write_refuses_events_the_golden_refuses", refuses_golden_disagreement)

        def extracts_the_sections() -> None:
            pages, sections, candidates, _, _, folder = self.synthetic("extracts")
            out = folder / "extracted"
            written = seed.extract(pages, candidates, out)
            pages_e2e.require(len(written) == 4 and all(path.read_bytes() == (sections / path.name).read_bytes() for path in written), "extracted bytes differ")
            before = [path.read_bytes() for path in written]
            seed.extract(pages, candidates, out)
            pages_e2e.require(before == [path.read_bytes() for path in written], "a second extract changed a section")

        self.recorder.scenario("extract_writes_the_sections_of_the_pages_and_is_idempotent", extracts_the_sections)

        def refused_pages(name: str, change: Callable[[dict[int, bytes]], dict[int, bytes]], fragment: str) -> None:
            def action() -> None:
                _, _, candidates, _, _, folder = self.synthetic(name)
                bad_pages = folder / "bad_pages"
                for year, body in change(pages_e2e.base_pages()).items():
                    (bad_pages / f"v{year}").mkdir(parents=True, exist_ok=True)
                    (bad_pages / f"v{year}" / f"county-changes.{year}.html").write_bytes(body)
                out = folder / "extracted"
                try:
                    seed.extract(bad_pages, candidates, out)
                except seed.SeedError as error:
                    pages_e2e.require(fragment in str(error) and not out.exists(), f"{error}")
                    return
                raise AssertionError("extract accepted the pages")

            self.recorder.scenario(name, action)

        refused_pages(
            "extract_refuses_a_page_changed_after_review_and_writes_nothing",
            lambda pages: {**pages, 1990: pages[1990].replace(b"effective July 1, 1994", b"effective July 2, 1994")},
            "change_list_differs_from_pin",
        )
        refused_pages(
            "extract_refuses_a_page_outside_the_grammar_and_writes_nothing",
            lambda pages: {**pages, 1980: pages[1980].replace(b"<br />", b"<br-note></br-note>", 1)},
            "decade_heading_or_layout_differs",
        )

        def folder_problem(name: str, damage: Callable[[Path], None], fragment: str) -> None:
            def action() -> None:
                pages, _, candidates, _, _, folder = self.synthetic(name)
                damage(pages)
                out = folder / "extracted"
                try:
                    seed.extract(pages, candidates, out)
                except seed.SeedError as error:
                    pages_e2e.require(fragment in str(error) and not out.exists(), f"{error}")
                    return
                raise AssertionError("extract accepted the pages folder")

            self.recorder.scenario(name, action)

        folder_problem(
            "pages_folder_missing_a_page", lambda pages: next(pages.rglob("county-changes.1980.html")).unlink(), "0 copies of county-changes.1980.html"
        )

        def twice(pages: Path) -> None:
            original = next(path for path in pages.rglob("county-changes.1990.html") if path.is_file())
            (pages / "again").mkdir()
            shutil.copy(original, pages / "again" / original.name)

        folder_problem("pages_folder_with_a_page_twice", twice, "2 copies of county-changes.1990.html")
        self.read_once()

    def read_once(self) -> None:
        """A file replaced between two reads must not give events other than the checked text: every file is read once."""
        revised = pages_e2e.base_pages()[1990].replace(b"effective July 1, 1994", b"effective July 2, 1994")

        def sections_read_once() -> None:
            _, sections, candidates, _, _, _ = self.synthetic("race_sections")
            section_revised = grammar.page_section(revised.decode(), 1990).encode()
            with pages_e2e.read_race(seed.section_names()[1990], section_revised, before_read=2) as reads:
                rows = seed.events_from_sections(seed.load_sections(sections), seed.pins(candidates))
            pages_e2e.require(reads[0] == 1, f"the 1990 section was read {reads[0]} times")
            pages_e2e.require(not any("July 2, 1994" in row["row_text"] for row in rows), "events came from the replacement")

        self.recorder.scenario("sections_are_read_once_so_a_replacement_cannot_change_the_events", sections_read_once)

        def pages_read_once() -> None:
            pages, _, candidates, _, _, folder = self.synthetic("race_pages")
            with pages_e2e.read_race("county-changes.1990.html", revised, before_read=2) as reads:
                seed.extract(pages, candidates, folder / "extracted")
            pages_e2e.require(reads[0] == 1, f"the 1990 page was read {reads[0]} times")
            text = (folder / "extracted" / seed.section_names()[1990]).read_text(encoding="utf-8")
            pages_e2e.require("July 2, 1994" not in text and "July 1, 1994" in text, "the section came from the replacement")

        self.recorder.scenario("pages_are_read_once_by_the_extraction", pages_read_once)

        def seed_read_once() -> None:
            _, sections, candidates, golden, path, _ = self.synthetic("race_seed")
            seed.write(path, golden, candidates, sections)
            damaged = path.read_bytes().replace(b"Test & Sons", b"Fabricated", 1)
            with pages_e2e.read_race(path.name, damaged, before_read=2) as reads:
                problems, regeneration, _ = seed.check(path, golden, candidates, sections)
            pages_e2e.require(reads[0] == 1 and not problems and regeneration.startswith("equal"), f"{reads[0]} reads, {problems} {regeneration}")

        self.recorder.scenario("seed_is_read_once_by_the_check", seed_read_once)

    def real_pages(self, pages: Path) -> None:
        """The four real pages give the committed sections byte for byte, with 143 events and 22 retirement rows."""

        def extracts() -> None:
            out = self.root / "real_sections"
            written = seed.extract(pages, seed.CANDIDATES, out)
            pages_e2e.require(len(written) == 4, f"{written}")
            for path in written:
                pages_e2e.require(path.read_bytes() == (seed.SECTIONS / path.name).read_bytes(), f"{path.name} differs from the committed section")
            problems, regeneration, events = seed.check(sections=out)
            retirements = len(seed.golden_from(seed.read_seed(seed.SEED))["retirements"])
            pages_e2e.require(not problems and regeneration.startswith("equal") and events == REAL_EVENTS, f"{problems} {regeneration} {events}")
            pages_e2e.require(retirements == REAL_RETIREMENT_ROWS, f"{retirements} retirement rows")

        self.recorder.scenario("real_pages_give_the_committed_sections_byte_for_byte", extracts)


def main(argv: list[str] | None = None) -> int:
    """Run both parts, write the report and return 0 only when every case behaved as expected."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, required=True, help="The JSON report to write; an existing file is replaced.")
    parser.add_argument("--pages", type=Path, help="A folder holding the four real fetched pages.")
    args = parser.parse_args(argv)
    started = datetime.now(UTC)
    with tempfile.TemporaryDirectory(prefix="county_change_seed_e2e_") as scratch:
        suite = Suite(Path(scratch))
        suite.part_one()
        suite.part_two()
        if args.pages is not None:
            suite.real_pages(args.pages)
    scenarios = suite.recorder.scenarios
    status = "passed" if all(item["passed"] for item in scenarios) else "failed"
    report = {
        "run": {
            "feature": "county-change seed: pinned sections, regeneration, golden and shape checks, the writer and the extraction",
            "started_utc": started.isoformat().replace("+00:00", "Z"),
            "python": sys.version.split()[0],
            "seed_sha256": hashlib.sha256(seed.SEED.read_bytes()).hexdigest(),
            "golden_sha256": hashlib.sha256(seed.GOLDEN.read_bytes()).hexdigest(),
            "candidates_sha256": hashlib.sha256(seed.CANDIDATES.read_bytes()).hexdigest(),
            "sections_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(seed.SECTIONS.iterdir())},
            "reader_sha256": hashlib.sha256(Path(grammar.__file__).read_bytes()).hexdigest(),
            "seed_module_sha256": hashlib.sha256(Path(seed.__file__).read_bytes()).hexdigest(),
            "real_pages_checked": args.pages is not None,
            "reproduce": ".venv/bin/python -m scripts.lakehouse.run_county_change_seed_e2e --output <report.json> [--pages <folder of the four fetched pages>]",
            "limits": "Damaged copies and synthetic pages in a scratch folder; real pages only with --pages; no network, container, AWS or dbt",
            "cleanup": "The scratch folder is removed on exit.",
        },
        "status": status,
        "scenarios": scenarios,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    sys.stdout.write(
        json.dumps({"status": status, "passed": sum(item["passed"] for item in scenarios), "total": len(scenarios), "report": str(args.output)}) + "\n"
    )
    return 0 if status == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
