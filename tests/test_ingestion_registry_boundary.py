"""The readiness report does not move with what a file says, and no supplied file can interrupt it.

Each case runs the real release command on a copy of the committed inputs. The output must equal the output on the committed files, byte for byte, once the
``shape:`` lines are removed, and the exit code and every problem must be the same. A malformed, looping or dangling supplied file is a named shape refusal
that prints no input value and no traceback.
"""

from __future__ import annotations

import io
import json
import os
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any

import pytest

from scripts.ingestion import registry
from tests.registry_perfect_support import EVIDENCE, FILES, IAM, MANUAL, RETENTION, add_evidence, add_manual, add_r4, bind, digest
from tests.registry_support import edit_json, fresh_copy, replace, scope_report
from tests.support import check

COLLECTIONS = "config/ingestion/collections.json"
CONTRACTS = "config/ingestion/layout_contracts.json"
GOLDEN = "config/ingestion/unit_golden.json"
LOCK = "config/ingestion/registry.lock.json"
SENTINEL = "SENTINEL_INPUT_VALUE_7f3a"


def run_release(root: Path, folder: Path) -> tuple[int, str, str]:
    """The real release command on a tree: its exit code, its output and its output without the shape lines."""
    report_file = folder / "scope_report.json"
    report_file.write_bytes(registry.scope.report_bytes(scope_report()))
    output = io.StringIO()
    with redirect_stdout(output), redirect_stderr(output):
        code = registry.main(["check", "--release", "--root", str(root), "--scope-report", str(report_file)])
    text = output.getvalue()
    return code, text, "".join(line for line in text.splitlines(keepends=True) if not line.strip().startswith("shape:"))


@pytest.fixture(scope="module")
def committed(tmp_path_factory: pytest.TempPathFactory) -> tuple[int, str]:
    """The exit code and the output without shape lines of the release command on the committed files."""
    folder = tmp_path_factory.mktemp("committed")
    code, _, kept = run_release(fresh_copy(folder / "tree"), folder)
    check(code == 1 and "READINESS" in kept, f"The committed files do not fail the release check: {code}")
    return code, kept


def rebind(root: Path, relative: str) -> None:
    """Write the lock's digest of one bound file again, for a file the lock command cannot read."""
    body = (root / relative).read_bytes()
    edit_json(root, LOCK, lambda lock: next(row for row in lock["files"] if row["path"] == relative).update(sha256=digest(body)))


def each_source(document: Any) -> list[dict[str, Any]]:
    return [source for collection in document["collections"] for source in collection["sources"]]


def one_decision(root: Path) -> None:
    edit_json(root, COLLECTIONS, lambda d: d["decisions"]["D3"].update(value="B", recorded="2026-11-27"))


def every_state(root: Path) -> None:
    """The seven pending decision and state fields, all filled."""

    def change(document: Any) -> None:
        document["decisions"]["D3"].update(value="B", recorded="2026-11-27")
        document["decisions"]["Q4"]["interim"].update(accountable="a person", due="a date")
        for source in each_source(document):
            if source["admission"] == "pending_owner":
                source["admission"] = "unit_records"
        for plan in document["plans"]:
            plan["lifecycle"] = "retired"

    edit_json(root, COLLECTIONS, change)


def states_reopened(root: Path) -> None:
    """The other direction: recorded decisions turned back to pending."""

    def change(document: Any) -> None:
        for name in ("D1", "D2", "Q4"):
            document["decisions"][name].update(value="pending_owner", recorded=None)
        for source in each_source(document):
            if source["admission"] == "unit_records":
                source["admission"] = "pending_owner"

    edit_json(root, COLLECTIONS, change)


def closed_source_matches(root: Path) -> None:
    """Match fields for every closed source that has no machine test, none of which covers a queued unit."""

    def change(document: Any) -> None:
        for source in each_source(document):
            if source["acquisition"] == "closed_partial":
                source["match"] = {"url_regex": "no-queued-unit-has-this-text"}

    edit_json(root, COLLECTIONS, change)


def closed_sources_relabelled(root: Path) -> None:
    """The closed sources said to be on no schedule, so no closed scope is left in the file."""

    def change(document: Any) -> None:
        for source in each_source(document):
            if source["acquisition"] == "closed_partial":
                source.update(acquisition="not_scheduled", scope="whole_source")

    edit_json(root, COLLECTIONS, change)


def another_layout(root: Path) -> None:
    """A second valid layout contract for a mapped table."""

    def change(document: Any) -> None:
        document["tables"]["cms_hai_hospital"] = json.loads(json.dumps(document["tables"]["hcai_finance_csv"]))

    edit_json(root, CONTRACTS, change)


def gap_routes_relabelled(root: Path) -> None:
    """The authored route of every gap changed."""
    edit_json(root, COLLECTIONS, lambda d: [gap.update(mode="unit_records") for gap in d["gaps"].values()])


AUTHORED_CASES: dict[str, Callable[[Path], None]] = {
    "one recorded decision filled": one_decision,
    "all decision and state fields filled": every_state,
    "recorded decisions turned back to pending": states_reopened,
    "match fields for the closed sources": closed_source_matches,
    "closed sources relabelled": closed_sources_relabelled,
    "a second layout contract": another_layout,
    "gap routes relabelled": gap_routes_relabelled,
}


@pytest.mark.parametrize("name", list(AUTHORED_CASES))
def test_an_authored_value_changes_no_output_outside_the_shape_lines(tmp_path: Path, committed: tuple[int, str], name: str) -> None:
    """The release output of a tree with other authored values, bound by the lock command, is the committed output once the shape lines are removed."""
    root = fresh_copy(tmp_path / "tree")
    AUTHORED_CASES[name](root)
    bind(root)
    code, _, kept = run_release(root, tmp_path)
    check((code, kept) == committed, f"{name} changed the report:\n{diff(committed[1], kept)}")


def diff(first: str, second: str) -> str:
    before, after = set(first.splitlines()), set(second.splitlines())
    return "\n".join([f"- {line[:200]}" for line in sorted(before - after)][:8] + [f"+ {line[:200]}" for line in sorted(after - before)][:8])


def test_a_golden_list_with_no_manual_unit_removes_no_readiness_item(tmp_path: Path) -> None:
    """The manual units come from the queue: a golden list that calls none of them manual, with no outcome, still leaves every readiness item and problem."""
    baseline = registry.check(fresh_copy(tmp_path / "committed"), release=True, scope_report=scope_report(), skip=frozenset({"I6"}))
    root = fresh_copy(tmp_path / "tree")
    edit_json(root, GOLDEN, lambda d: [row.update({"class": "file"}) for row in d["units"] if row["class"] == "manual"])
    edit_json(root, MANUAL, lambda d: d.update(outcomes=[]))
    registry.write_lock(root, scope_report())
    result = registry.check(root, release=True, scope_report=scope_report(), skip=frozenset({"I6"}))
    check(any(problem.predicate == "I3" for problem in result.problems), "The relabelled golden list was not named by I3")
    check(result.readiness == baseline.readiness, f"The readiness items moved:\n{diff(chr(10).join(baseline.readiness), chr(10).join(result.readiness))}")
    check(list(map(str, result.release_problems)) == list(map(str, baseline.release_problems)), "The readiness problems moved with the golden list")


def supply_table_value(value: Any) -> Callable[[Path], None]:
    """An evidence entry whose table is not a string."""

    def change(root: Path) -> None:
        add_evidence(root, "cms_hai_hospital")
        edit_json(root, EVIDENCE, lambda document: document["tables"][0].update(table=value))

    return change


def loop(root: Path, relative: str) -> None:
    """Replace a file with a link that points at itself."""
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    os.symlink(path.name, path)


def dangling(root: Path, relative: str) -> None:
    """Replace a file with a link to a file that does not exist."""
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    os.symlink("no_such_file", path)


def record_link(link: Callable[[Path, str], None], table: str, part: str) -> Callable[[Path], None]:
    def change(root: Path) -> None:
        add_evidence(root, table)
        link(root, f"{FILES}/{table}_{part}.json")

    return change


def manual_link(link: Callable[[Path, str], None]) -> Callable[[Path], None]:
    def change(root: Path) -> None:
        add_manual(root)
        link(root, f"{FILES}/manual_00.json")

    return change


def r4_link(link: Callable[[Path, str], None], relative: str) -> Callable[[Path], None]:
    def change(root: Path) -> None:
        add_r4(root)
        link(root, relative)

    return change


def six_record_kinds(link: Callable[[Path, str], None], word: str) -> dict[str, Callable[[Path], None]]:
    """The same broken link in the place of each of the six record kinds."""
    return {
        f"{word} completion record": record_link(link, "cms_hai_hospital", "record"),
        f"{word} inventory record": record_link(link, "illinois_report_card_csv", "record"),
        f"{word} live check record": record_link(link, "cms_hai_hospital", "live"),
        f"{word} manual outcome record": manual_link(link),
        f"{word} retention input": r4_link(link, RETENTION),
        f"{word} IAM plan evidence": r4_link(link, IAM),
    }


MALFORMED_CASES: dict[str, Callable[[Path], None]] = {
    "an evidence table that is an array": supply_table_value([SENTINEL]),
    "an evidence table that is an object": supply_table_value({SENTINEL: 1}),
    **six_record_kinds(loop, "a looping"),
    **six_record_kinds(dangling, "a dangling"),
}


@pytest.mark.parametrize("name", list(MALFORMED_CASES))
def test_a_malformed_or_broken_supplied_file_is_a_named_refusal_in_a_whole_report(tmp_path: Path, committed: tuple[int, str], name: str) -> None:
    """No traceback: the command prints the whole report, with a shape line that says refused, and the rest of the output is unchanged."""
    root = fresh_copy(tmp_path / "tree")
    MALFORMED_CASES[name](root)
    bind(root)
    code, text, kept = run_release(root, tmp_path)
    check((code, kept) == committed, f"{name} changed the report:\n{diff(committed[1], kept)}")
    shapes = [line for line in text.splitlines() if line.strip().startswith("shape:")]
    check(any("refused" in line for line in shapes), f"{name} was not reported as a shape refusal: {shapes}")
    check(SENTINEL not in text and str(root) not in text and "no_such_file" not in text, f"{name} printed an input value or a path of the tree")


ENVELOPES: dict[str, bytes] = {
    "a wrong kind and a boolean schema version": json.dumps(
        {"kind": SENTINEL, "schema_version": True, "candidate_map_sha256": "1" * 64, "tables": []}
    ).encode(),
    "an unknown property": json.dumps({"kind": "input_evidence", "schema_version": 1, "candidate_map_sha256": "1" * 64, "tables": [], SENTINEL: 1}).encode(),
    "a missing candidate digest": json.dumps({"kind": "input_evidence", "schema_version": 1, "tables": []}).encode(),
    "a top level array": json.dumps([SENTINEL]).encode(),
    "tables that is an object": json.dumps({"kind": "input_evidence", "schema_version": 1, "candidate_map_sha256": "1" * 64, "tables": {SENTINEL: 1}}).encode(),
    "bytes that are not UTF-8": b'{"kind": "\xff\xfe' + SENTINEL.encode() + b'"}',
    "text that is not JSON": SENTINEL.encode(),
    "nesting too deep to parse": b"[" * 100_000,
}


@pytest.mark.parametrize("name", list(ENVELOPES))
def test_a_malformed_evidence_envelope_is_a_shape_refusal_and_readiness_is_still_computed(tmp_path: Path, committed: tuple[int, str], name: str) -> None:
    """The evidence file is read through the shape boundary only: a bad envelope is one shape line, with no integrity problem and no decoder text."""
    root = fresh_copy(tmp_path / "tree")
    replace(root, EVIDENCE, ENVELOPES[name])
    rebind(root, EVIDENCE)
    code, text, kept = run_release(root, tmp_path)
    check((code, kept) == committed, f"{name} changed the report:\n{diff(committed[1], kept)}")
    shapes = [line for line in text.splitlines() if line.strip().startswith("shape:") and "evidence file" in line]
    check(len(shapes) == 1 and "refused" in shapes[0], f"{name} was not one shape refusal of the evidence file: {shapes}")
    check(not any(word in text for word in (SENTINEL, "codec", "0xff", "Expecting value", "recursion")), f"{name} printed input or decoder text")


def test_the_lock_command_binds_an_evidence_file_it_cannot_parse(tmp_path: Path) -> None:
    """The lock binds the evidence bytes and reads nothing in them, so a malformed file does not stop the lock command."""
    root = fresh_copy(tmp_path / "tree")
    replace(root, EVIDENCE, b"\xff")
    registry.write_lock(root, scope_report())
    check(not registry.lock_problems(root), "The lock does not bind the evidence bytes it was written from")
