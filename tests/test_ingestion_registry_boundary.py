"""The readiness report does not move with what a file says, and no supplied file can interrupt it.

Each case runs the real release command on a copy of the committed inputs. The output must equal the output on the committed files, byte for byte, once the
``shape:`` lines are removed, and the exit code and every problem must be the same. A malformed, looping or dangling supplied file is a named shape refusal
that prints no input value and no traceback.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import socket
import urllib.request
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any

import pytest

from scripts.ingestion import registry
from scripts.ingestion import registry_records as records
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


LIVE_SCHEMA = "config/ingestion/schemas/live_check.schema.json"


def supplied_evidence(root: Path, schema: bytes) -> registry.Result:
    """A perfect evidence entry under a target tree whose live check schema is ``schema``, bound by the lock command, then the release check."""
    replace(root, LIVE_SCHEMA, schema)
    add_evidence(root, "cms_hai_hospital")
    bind(root)
    return registry.check(root, release=True, scope_report=scope_report(), skip=frozenset({"I6"}))


def test_the_shape_checks_use_the_schemas_bound_under_the_root(tmp_path: Path) -> None:
    """The target tree binds a live check schema that accepts nothing, so its live check record gets no shape acceptance."""
    result = supplied_evidence(fresh_copy(tmp_path / "tree"), b"false\n")
    live = [note for note in result.notes if "cms_hai_hospital: live check" in note]
    check(len(live) == 1 and "refused" in live[0], f"The bound schema was ignored: {result.notes}")


UNUSABLE_SCHEMAS = {
    "a pattern that overflows the regex compiler": b'{"pattern": "x{99999999999}"}\n',
    "not a schema": b'{"type": 5}\n',
    "an address that is no address": b'{"$ref": "http://["}\n',
    "not JSON": b"{\n",
    "a reference to nothing": b'{"$ref": "#/$defs/absent"}\n',
    "a reference to itself": b'{"$ref": "#"}\n',
}


@pytest.mark.parametrize("name", list(UNUSABLE_SCHEMAS))
def test_a_bound_schema_that_cannot_be_used_is_a_named_refusal_in_a_whole_report(tmp_path: Path, name: str) -> None:
    """No traceback: the shape check refuses and, where the file is no schema at all, I2 names it. Readiness stays complete."""
    result = supplied_evidence(fresh_copy(tmp_path / "tree"), UNUSABLE_SCHEMAS[name])
    named = any(problem.predicate == "I2" and problem.subject == LIVE_SCHEMA for problem in result.problems)
    refused = any("live check" in note and "refused" in note for note in result.notes)
    check(named or refused, f"{name} was not refused by name: {result.problems[:3]} {result.notes}")
    check(not any("live check" in note and "shape accepted" in note for note in result.notes), f"{name} was accepted: {result.notes}")
    check(len(result.readiness) >= 9, f"{name} removed readiness items: {result.readiness}")


DEEP_PATTERN = "(" * 750 + "x" + ")" * 750


def deep_feed(root: Path) -> None:
    edit_json(root, COLLECTIONS, lambda d: d["feeds"][0].update(name_regex=DEEP_PATTERN))


def deep_match(root: Path) -> None:
    edit_json(root, COLLECTIONS, lambda d: next(s for s in each_source(d) if s["acquisition"] == "closed_partial").update(match={"url_regex": DEEP_PATTERN}))


def huge_repeat(root: Path) -> None:
    edit_json(root, COLLECTIONS, lambda d: d["feeds"][0].update(name_regex="x{99999999999}"))


@pytest.mark.parametrize("change", [deep_feed, deep_match, huge_repeat])
def test_a_pattern_that_overflows_the_regex_compiler_is_a_named_i2_refusal(tmp_path: Path, change: Callable[[Path], None]) -> None:
    root = fresh_copy(tmp_path / "tree")
    change(root)
    bind(root)
    code, text, _ = run_release(root, tmp_path)
    check(code == 1 and "pattern is not a regular expression" in text and "I2 " in text, f"No named refusal: {code} {text[:600]}")
    check("Traceback" not in text and "fail (102 problems)" in text, f"The report is not whole: {text[-300:]}")


SUPPLIED_SCOPE = {
    "a number": b"5",
    "a null": b"null",
    "an array": json.dumps([SENTINEL]).encode(),
    "text that is not JSON": SENTINEL.encode(),
    "nesting too deep to parse": b"[" * 100_000,
}


@pytest.mark.parametrize("command", ["check", "lock"])
@pytest.mark.parametrize("name", list(SUPPLIED_SCOPE))
def test_a_supplied_scope_report_that_is_no_json_object_is_a_named_i2_refusal(tmp_path: Path, name: str, command: str) -> None:
    root = fresh_copy(tmp_path / "tree")
    report_file = tmp_path / "scope.json"
    report_file.write_bytes(SUPPLIED_SCOPE[name])
    before = (root / LOCK).read_bytes()
    output = io.StringIO()
    with redirect_stdout(output), redirect_stderr(output):
        code = registry.main([command, "--root", str(root), "--scope-report", str(report_file)])
    text = output.getvalue()
    check(code == 1 and "I2 scope report: is not a JSON object" in text, f"{command} with {name}: {code} {text[:400]}")
    check(SENTINEL not in text and (root / LOCK).read_bytes() == before, f"{command} printed the input or wrote the lock")


def run_kept(root: Path, folder: Path, committed: tuple[int, str]) -> str:
    """The release output of a broken tree, after checking that it holds every open item and readiness problem of the committed output and no traceback."""
    _, text, _ = run_release(root, folder)
    lines = set(text.splitlines())
    lost = [line for line in committed[1].splitlines()[:-1] if line not in lines]
    check(not lost, f"The readiness report lost lines: {lost[:3]}\n{text[:800]}")
    check("Traceback" not in text, f"A traceback was printed: {text[-600:]}")
    return text


def drop_outcome(root: Path) -> None:
    edit_json(root, MANUAL, lambda d: d["outcomes"].pop(0))


def edit_unbound(relative: str) -> Callable[[Path], None]:
    """Change a bound file's bytes and leave the lock as it is."""
    return lambda root: replace(root, relative, (root / relative).read_bytes() + b" \n")


def nul_plan_path(root: Path) -> None:
    """A classified plan whose path no file system call accepts, listed by the lock."""
    edit_json(root, COLLECTIONS, lambda d: d["plans"].append({"path": "config/\u0000.json", "lifecycle": "retired"}))
    rows = [{"path": name, "sha256": "0" * 64} for name in ("config/\u0000.json", "config/\u0000.lock.json")]
    edit_json(root, LOCK, lambda lock: lock["files"].extend(rows))
    rebind(root, COLLECTIONS)


def unencodable_decision(root: Path) -> None:
    """A decision record the canonical form refuses, under a schema that lets it through."""
    replace(root, COLLECTIONS_SCHEMA, b"true\n")
    edit_json(root, COLLECTIONS, lambda d: d["decisions"]["D1"].update(meaning=float("nan")))
    rebind(root, COLLECTIONS_SCHEMA)
    rebind(root, COLLECTIONS)


COLLECTIONS_SCHEMA = "config/ingestion/schemas/collections.schema.json"
LOCK_SCHEMA = "config/ingestion/schemas/registry_lock.schema.json"
LEAVING = "config/lakehouse/bronze_leaving_scope.json"
SOURCES = "config/acquisition/source_registry.json"
BESIDE_READINESS: dict[str, tuple[Callable[[Path], None], str]] = {
    "an outcome removed without relocking": (drop_outcome, f"I1 {MANUAL}: differs from the lock"),
    "collections changed without relocking": (edit_unbound(COLLECTIONS), f"I1 {COLLECTIONS}: differs from the lock"),
    "the leaving record unreadable": (lambda root: replace(root, LEAVING, b"{"), f"I1 {LEAVING}: differs from the lock"),
    "the source registry changed": (edit_unbound(SOURCES), f"I1 {SOURCES}: differs from the lock"),
    "an authored file deleted": (lambda root: (root / CONTRACTS).unlink(), f"I1 {CONTRACTS}: is bound by the lock but cannot be read"),
    "a lock with an unknown property": (lambda root: edit_json(root, LOCK, lambda d: d.update(surprise=1)), f"I2 {LOCK}:"),
    "a lock with unusable pins": (lambda root: edit_json(root, LOCK, lambda d: d.update(pins=None)), f"I2 {LOCK}:"),
    "the lock schema deleted": (lambda root: (root / LOCK_SCHEMA).unlink(), f"I1 {LOCK_SCHEMA}:"),
    "a plan path no file call accepts": (nul_plan_path, "is bound by the lock but cannot be read"),
    "a record no canonical form accepts": (unencodable_decision, "I2 inputs:"),
}


@pytest.mark.parametrize("name", list(BESIDE_READINESS))
def test_a_failure_beside_the_queue_the_map_and_the_scope_removes_no_readiness(tmp_path: Path, committed: tuple[int, str], name: str) -> None:
    """The integrity problem is named AND every open item and readiness problem is still printed."""
    change, named = BESIDE_READINESS[name]
    root = fresh_copy(tmp_path / "tree")
    change(root)
    text = run_kept(root, tmp_path, committed)
    check(named in text, f"{name} was not named ({named}): {text[:800]}")


@pytest.mark.parametrize("report", ["no_such_file.json", "."])
def test_a_supplied_scope_report_that_cannot_be_read_is_named_in_a_whole_report(tmp_path: Path, committed: tuple[int, str], report: str) -> None:
    root = fresh_copy(tmp_path / "tree")
    output = io.StringIO()
    with redirect_stdout(output), redirect_stderr(output):
        code = registry.main(["check", "--release", "--root", str(root), "--scope-report", str(tmp_path / report)])
    text = output.getvalue()
    check(code == 1 and "I2 scope report: cannot be read" in text and "fail (102 problems)" in text, f"No whole report: {code} {text[-600:]}")
    check(all(line in text.splitlines() for line in committed[1].splitlines()[:-1] if not line.startswith("shape:")), "The report lost readiness lines")
    before = (root / LOCK).read_bytes()
    with redirect_stdout(output), redirect_stderr(output):
        code = registry.main(["lock", "--root", str(root), "--scope-report", str(tmp_path / report)])
    check(code == 1 and (root / LOCK).read_bytes() == before, f"The lock command took an unreadable report: {code}")


@pytest.mark.parametrize("relative", ["config/acquisition/baseline_run/capture_specs.json", "config/acquisition/collection_layout.json"])
def test_unlocked_gap_inputs_leave_every_item_open_and_say_that_the_gaps_are_not_computed(tmp_path: Path, committed: tuple[int, str], relative: str) -> None:
    """The capture specifications and the layout only feed the gap list and the marked outputs. Their failure removes no item and no readiness problem."""
    root = fresh_copy(tmp_path / "tree")
    replace(root, relative, b"{")
    _, text, _ = run_release(root, tmp_path)
    lines = text.splitlines()
    wanted = [line for line in committed[1].splitlines()[:-1] if not line.startswith(("  open: R1 usable", "  open: I4"))]
    check(not [line for line in wanted if line not in lines], f"The report lost lines: {text[:600]}")
    check(sum(line.startswith("  open: ") for line in lines) == sum(line.startswith("  open: ") for line in wanted) + 2, f"An item was lost: {text}")
    check("gaps not computed" in text and "I4 outputs the check cannot decide offline: not counted" in text, f"The unknown facts are not said: {text}")


@pytest.mark.parametrize("relative", ["config/acquisition/baseline_run/queue.json", "config/lakehouse/bronze_tables.json"])
def test_an_unlocked_queue_or_table_map_computes_no_readiness(tmp_path: Path, relative: str) -> None:
    """Readiness is never computed from a queue or a map the lock does not bind: a shorter queue would print fewer open units."""
    root = fresh_copy(tmp_path / "tree")
    edit_unbound(relative)(root)
    code, text, _ = run_release(root, tmp_path)
    check(code == 1 and f"I1 {relative}: differs from the lock" in text and "(not computed)" in text, f"No named stop: {code} {text[:800]}")


def test_a_tree_whose_scope_cannot_be_derived_still_names_its_lock_problems(tmp_path: Path) -> None:
    root = fresh_copy(tmp_path / "tree")
    shutil.rmtree(root / "dbt")
    drop_outcome(root)
    output = io.StringIO()
    with redirect_stdout(output), redirect_stderr(output):
        code = registry.main(["check", "--root", str(root)])
    check(code == 1 and f"I1 {MANUAL}: differs from the lock" in output.getvalue(), f"The lock problem was lost: {code} {output.getvalue()[:600]}")


SCHEMA_NAMES = sorted(path.name.removesuffix(".schema.json") for path in (registry.ROOT / "config/ingestion/schemas").glob("*.schema.json"))


@pytest.mark.parametrize("name", SCHEMA_NAMES)
def test_a_missing_required_schema_is_an_i1_problem_that_the_lock_command_refuses_to_bind(tmp_path: Path, committed: tuple[int, str], name: str) -> None:
    root = fresh_copy(tmp_path / "tree")
    relative = f"config/ingestion/schemas/{name}.schema.json"
    (root / relative).unlink()
    before = (root / LOCK).read_bytes()
    output = io.StringIO()
    with redirect_stdout(output), redirect_stderr(output):
        code = registry.main(["lock", "--root", str(root)])
    check(code == 2 and relative in output.getvalue() and (root / LOCK).read_bytes() == before, f"The lock command bound a tree without {name}: {code}")
    edit_json(root, LOCK, lambda lock: lock.update(files=[row for row in lock["files"] if row["path"] != relative]))
    text = run_kept(root, tmp_path, committed)
    check(any(line.startswith(f"I1 {relative}:") for line in text.splitlines()), f"The missing {name} schema was not named by I1: {text[:800]}")


def edited(change: Callable[[Any], object]) -> Callable[[Any], Any]:
    """A draft changed in place."""

    def apply(draft: Any) -> Any:
        change(draft)
        return draft

    return apply


def becomes(value: Any) -> Callable[[Any], Any]:
    """A draft replaced whole."""
    return lambda _: value


def first_source(change: dict[str, Any]) -> Callable[[Any], Any]:
    return edited(lambda d: d["collections"][0]["sources"][0].update(change))


def contract(change: dict[str, Any]) -> Callable[[Any], Any]:
    return edited(lambda d: d["tables"]["hcai_finance_csv"].update(change))


UNREADABLE_DRAFTS: dict[str, tuple[str, Callable[[Any], Any]]] = {
    "collections is null": ("collections", becomes(None)),
    "collections is an array": ("collections", becomes([SENTINEL])),
    "collections is empty": ("collections", becomes({})),
    "a decision is null": ("collections", edited(lambda d: d["decisions"].update(D1=None))),
    "a decision is absent": ("collections", edited(lambda d: d["decisions"].pop("D2"))),
    "the interim record is null": ("collections", edited(lambda d: d["decisions"]["Q4"].update(interim=None))),
    "the feeds are an object": ("collections", edited(lambda d: d.update(feeds={}))),
    "a feed pattern is a number": ("collections", edited(lambda d: d["feeds"][0].update(name_regex=5))),
    "a gap step is a number": ("collections", edited(lambda d: next(iter(d["gaps"].values())).update(step=5))),
    "a source match is null": ("collections", first_source({"match": None})),
    "a source state is a list": ("collections", first_source({"acquisition": [SENTINEL]})),
    "a plan path is a number": ("collections", edited(lambda d: d["plans"][0].update(path=5))),
    "manual outcomes is null": ("manual_outcomes", becomes(None)),
    "an outcome has no unit": ("manual_outcomes", edited(lambda d: d["outcomes"].append({}))),
    "the outcomes are a string": ("manual_outcomes", becomes({"outcomes": ""})),
    "layout contracts is null": ("layout_contracts", becomes(None)),
    "contract tables is an array": ("layout_contracts", becomes({"tables": []})),
    "contract columns are numbers": ("layout_contracts", contract({"accepted_layouts": [{"id": "a", "columns_sha256": "b", "columns": [1, "a"]}]})),
    "consumer columns are objects": ("layout_contracts", contract({"consumer_columns": [{}]})),
    "the golden list is null": ("unit_golden", becomes(None)),
    "a golden row is empty": ("unit_golden", edited(lambda d: d["units"].append({}))),
    "the golden counts are empty": ("unit_golden", edited(lambda d: d.update(counts={}))),
}


def break_draft(root: Path, name: str) -> str:
    """Bind a schema that accepts anything and the changed draft, by their digests alone, and return the draft's path."""
    kind, change = UNREADABLE_DRAFTS[name]
    schema, draft = f"config/ingestion/schemas/{kind}.schema.json", f"config/ingestion/{kind}.json"
    changed = change(json.loads((root / draft).read_bytes()))
    replace(root, schema, b"true\n")
    replace(root, draft, json.dumps(changed).encode())
    rebind(root, schema)
    rebind(root, draft)
    return draft


@pytest.mark.parametrize("name", list(UNREADABLE_DRAFTS))
def test_a_draft_that_lacks_what_its_reader_needs_is_refused_by_name_whatever_its_schema_allows(tmp_path: Path, committed: tuple[int, str], name: str) -> None:
    root = fresh_copy(tmp_path / "tree")
    draft = break_draft(root, name)
    text = run_kept(root, tmp_path, committed)
    check(f" {draft}: lacks the structure its reader needs" in text, f"{name} was not refused by name: {text[:900]}")
    check(SENTINEL not in text, f"{name} printed an input value")


@pytest.mark.parametrize("name", [name for name, (kind, _) in UNREADABLE_DRAFTS.items() if kind == "collections"])
def test_the_lock_command_refuses_by_name_a_collections_draft_it_cannot_read(tmp_path: Path, name: str) -> None:
    root = fresh_copy(tmp_path / "tree")
    draft = break_draft(root, name)
    before = (root / LOCK).read_bytes()
    output = io.StringIO()
    with redirect_stdout(output), redirect_stderr(output):
        code = registry.main(["lock", "--root", str(root)])
    text = output.getvalue()
    check(code == 2 and f"{draft} lacks the structure its reader needs" in text, f"{name}: no named refusal: {code} {text[:600]}")
    check((root / LOCK).read_bytes() == before and SENTINEL not in text, f"{name} wrote the lock or printed an input value")


@pytest.mark.parametrize(
    "change",
    [
        lambda d: d["pins"]["gaps"][0].pop("table"),
        lambda d: d["pins"].update(gaps=None),
        lambda d: d.pop("parents"),
        lambda d: d["files"].append({"sha256": "0" * 64}),
    ],
)
def test_a_lock_that_lacks_what_its_reader_needs_is_refused_by_name_whatever_its_schema_allows(
    tmp_path: Path, committed: tuple[int, str], change: Callable[[Any], object]
) -> None:
    root = fresh_copy(tmp_path / "tree")
    replace(root, LOCK_SCHEMA, b"true\n")
    rebind(root, LOCK_SCHEMA)
    edit_json(root, LOCK, change)
    text = run_kept(root, tmp_path, committed)
    check(f"I2 {LOCK}: lacks the structure its reader needs" in text, f"The lock was not refused by name: {text[:900]}")


def test_the_report_prints_text_no_encoding_accepts_as_escapes() -> None:
    result = registry.Result(problems=[registry.Problem("I3", "\ud800", "is a unit key")])
    check("\\ud800" in registry.render(result, False).encode().decode(), "A lone surrogate stops the report")


EXTERNAL_REFERENCES = {
    "an external address": {"$ref": "https://registry.invalid/live.schema.json"},
    "an external address nothing reaches": {"$defs": {"unused": {"$ref": "https://registry.invalid/live.schema.json"}}, "type": "object"},
    "an external address inside a property": {"properties": {"absent": {"items": {"$ref": "https://registry.invalid/live.schema.json"}}}},
    "an external dynamic reference": {"$dynamicRef": "https://registry.invalid/live.schema.json#node"},
    "a file address": {"$ref": "file:///etc/hosts"},
    "a schema the tree does not bind": {"$ref": "other.schema.json"},
    "an absent local definition": {"$ref": "#/$defs/absent"},
    "a relative address under an external identifier": {"$id": "https://registry.invalid/a.json", "$ref": "b.json"},
    "the metaschema": {"$ref": "https://json-schema.org/draft/2020-12/schema"},
}


@pytest.fixture
def retrievals(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every attempt to open an address or a connection, refused before any input or output."""
    seen: list[str] = []

    def refuse(target: Any, *_: Any, **__: Any) -> Any:
        seen.append(str(getattr(target, "full_url", target)))
        raise OSError("this test allows no network access")

    monkeypatch.setattr(urllib.request, "urlopen", refuse)
    monkeypatch.setattr(socket.socket, "connect", refuse)
    return seen


@pytest.mark.parametrize("name", list(EXTERNAL_REFERENCES))
def test_a_schema_reference_that_no_bound_schema_resolves_is_a_named_refusal_with_no_retrieval(tmp_path: Path, retrievals: list[str], name: str) -> None:
    result = supplied_evidence(fresh_copy(tmp_path / "tree"), json.dumps(EXTERNAL_REFERENCES[name]).encode())
    named = [str(problem) for problem in result.problems if problem.predicate == "I2" and problem.subject == LIVE_SCHEMA]
    check(len(named) == 1 and "reference" in named[0], f"{name} was not named: {result.problems[:3]}")
    check(not retrievals, f"{name} tried to retrieve: {retrievals}")
    live = [note for note in result.notes if "cms_hai_hospital: live check" in note]
    check(len(live) == 1 and "refused" in live[0], f"{name} was accepted: {result.notes}")
    check(len(result.readiness) >= 9, f"{name} removed readiness items: {result.readiness}")


def test_evaluating_an_unresolved_reference_retrieves_nothing(tmp_path: Path, retrievals: list[str]) -> None:
    """The validator itself holds no retriever, so a reference the walk of a schema does not reach is a refusal too."""
    schemas = records.Schemas(registry.ROOT)
    found = schemas.errors_of({"$ref": "https://registry.invalid/live.schema.json"}, {})
    check(found and "cannot be evaluated" in found[0] and not retrievals, f"The reference was retrieved or accepted: {found} {retrievals}")


@pytest.mark.parametrize(
    "schema",
    [
        {"$defs": {"record": {"type": "object"}}, "$ref": "#/$defs/record"},
        {"$ref": "iam_plan_evidence.schema.json"},
        {"$id": "https://registry.invalid/live.json", "$defs": {"record": {"$id": "record.json", "type": "object"}}, "$ref": "record.json"},
    ],
)
def test_local_definitions_and_bound_schemas_still_resolve(tmp_path: Path, retrievals: list[str], schema: dict[str, Any]) -> None:
    result = supplied_evidence(fresh_copy(tmp_path / "tree"), json.dumps(schema).encode())
    check(not [problem for problem in result.problems if problem.subject == LIVE_SCHEMA], f"A resolvable reference was refused: {result.problems[:3]}")
    live = [note for note in result.notes if "cms_hai_hospital: live check" in note]
    check(len(live) == 1 and "cannot be evaluated" not in live[0] and not retrievals, f"The reference did not resolve: {live} {retrievals}")


def test_a_local_definition_of_the_evidence_schema_resolves_for_one_entry(tmp_path: Path) -> None:
    root = fresh_copy(tmp_path / "tree")
    relative = "config/ingestion/schemas/input_evidence.schema.json"

    def change(schema: Any) -> None:
        schema["$defs"] = {"entry": schema["properties"]["tables"]["items"]}
        schema["properties"]["tables"]["items"] = {"$ref": "#/$defs/entry"}

    edit_json(root, relative, change)
    add_evidence(root, "cms_hai_hospital")
    bind(root)
    result = registry.check(root, release=True, scope_report=scope_report(), skip=frozenset({"I6"}))
    live = [note for note in result.notes if "cms_hai_hospital: live check" in note]
    check(len(live) == 1 and "shape accepted" in live[0], f"The local definition was not resolved: {result.notes}")
