"""The registry integrity check, cases 1 to 4 of design v4 section 5.1: the lock, byte binding, pinned gaps and seats, and the collector closures.

Each case edits a copy of the committed inputs and runs the real check on it. Nothing here reads S3 or the network, and the repository is not written.
"""

from __future__ import annotations

import codecs
import copy
import hashlib
import io
import json
import shutil
from collections.abc import Callable, Mapping
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any

import pytest

from scripts.ingestion import registry
from scripts.lakehouse import bronze, pins
from tests.registry_support import append_text, edit_json, fresh_copy, replace, scope_report
from tests.support import check

LOCK = "config/ingestion/registry.lock.json"
QUEUE = "config/acquisition/baseline_run/queue.json"
COLLECTIONS = "config/ingestion/collections.json"
NO_CLOSURES = frozenset({"I6"})


def named(problems: list[registry.Problem], predicate: str) -> list[str]:
    """The subjects the check names for one predicate."""
    return sorted(problem.subject for problem in problems if problem.predicate == predicate)


def command(arguments: list[str]) -> tuple[int, str]:
    """The real command in this process: its exit code and everything it printed."""
    output = io.StringIO()
    with redirect_stdout(output), redirect_stderr(output):
        code = registry.main(arguments)
    return code, output.getvalue()


def test_check_with_no_lock_fails_by_name(tmp_path: Path) -> None:
    """Case 1: a tree with no lock fails, names the lock and says it is missing; the command exits 1."""
    root = fresh_copy(tmp_path / "tree")
    (root / LOCK).unlink()
    result = registry.check(root, scope_report=scope_report())
    check(not result.ok, "A tree with no lock passed the check")
    check(named(result.problems, "lock") == [LOCK], f"The missing lock was not named: {result.problems}")
    output = io.StringIO()
    with redirect_stdout(output):
        code = registry.main(["check", "--root", str(root)])
    check(code == 1 and LOCK in output.getvalue() and "missing" in output.getvalue(), f"The command did not fail by name: {code} {output.getvalue()!r}")


def test_one_changed_byte_in_a_bound_file_names_that_file(tmp_path: Path) -> None:
    """Case 2: one byte of the queue changes and I1 names the queue; an authored file changes and I1 names it; a bound file the lock lacks is named."""
    root = fresh_copy(tmp_path / "tree")
    check(not registry.lock_problems(root), "The fresh lock does not match its own files")
    body = bytearray((root / QUEUE).read_bytes())
    body[10] ^= 1
    replace(root, QUEUE, bytes(body))
    check(named(registry.lock_problems(root), "I1") == [QUEUE], "A changed byte of queue.json was not named")
    result = registry.check(root, scope_report=scope_report())
    check(not result.ok and named(result.problems, "I1") == [QUEUE], f"The check did not fail on the changed queue: {result.problems}")
    root = fresh_copy(tmp_path / "second")
    append_text(root, COLLECTIONS, " ")
    check(named(registry.lock_problems(root), "I1") == [COLLECTIONS], "A changed authored file was not named")
    root = fresh_copy(tmp_path / "third")
    edit_json(root, LOCK, lambda lock: lock.update(files=[row for row in lock["files"] if row["path"] != QUEUE]))
    check(named(registry.lock_problems(root), "I1") == [QUEUE], "A bound file missing from the lock was not named")
    root = fresh_copy(tmp_path / "fourth")
    (root / "config/ingestion/manual_outcomes.json").unlink()
    check(named(registry.lock_problems(root), "I1") == ["config/ingestion/manual_outcomes.json"], "A deleted bound file was not named")


def test_the_committed_files_pass_with_no_byte_changed() -> None:
    """The check passes on a fresh lock of the committed inputs and writes nothing."""
    result = registry.check(registry.ROOT, scope_report=scope_report())
    check(result.ok, f"The committed files fail the integrity check: {result.problems}")


def test_a_removed_pinned_gap_names_the_unexpected_gap(tmp_path: Path) -> None:
    """Case 3a: one pinned gap removed makes that table an unexpected gap; I7 names it and only it.

    A table with a declared feed also fails I5 when its pin goes, because the feed no longer has a pinned gap to excuse it.
    """
    root = fresh_copy(tmp_path / "tree")
    edit_json(root, LOCK, lambda lock: lock["pins"]["gaps"].remove(next(gap for gap in lock["pins"]["gaps"] if gap["table"] == "hcai_finance_csv")))
    result = registry.check(root, scope_report=scope_report(), skip=NO_CLOSURES)
    check(named(result.problems, "I7") == ["hcai_finance_csv"], f"I7 did not name the removed pin: {result.problems}")
    check({problem.predicate for problem in result.problems} == {"I7"}, f"Other predicates failed on a pin edit: {result.problems}")
    root = fresh_copy(tmp_path / "fed")
    edit_json(root, LOCK, lambda lock: lock["pins"]["gaps"].remove(next(gap for gap in lock["pins"]["gaps"] if gap["table"] == "hrsa_mua_detail")))
    result = registry.check(root, scope_report=scope_report(), skip=NO_CLOSURES)
    check(named(result.problems, "I7") == ["hrsa_mua_detail"] and named(result.problems, "I5") == ["hrsa_mua_detail"], f"{result.problems}")


def test_a_pinned_gap_that_is_closed_fails_until_the_pin_changes(tmp_path: Path) -> None:
    """Case 3b: a pin for a table that has a selected output (a closed gap) fails, so the pin cannot go stale."""
    root = fresh_copy(tmp_path / "tree")
    edit_json(root, LOCK, lambda lock: lock["pins"]["gaps"].append({"table": "cms_hai_hospital", "kind": "data_table", "step": "S9"}))
    result = registry.check(root, scope_report=scope_report(), skip=NO_CLOSURES)
    check(named(result.problems, "I7") == ["cms_hai_hospital"], f"A closed gap was not named: {result.problems}")


def test_a_pin_for_another_map_is_refused(tmp_path: Path) -> None:
    """Case 3c: the pinned gaps are keyed to the map digest; a pin for another map digest fails and says so."""
    root = fresh_copy(tmp_path / "tree")
    edit_json(root, LOCK, lambda lock: lock["pins"].update(map_sha256="0" * 64))
    result = registry.check(root, scope_report=scope_report(), skip=NO_CLOSURES)
    check(any(problem.predicate == "I7" and "map" in problem.message for problem in result.problems), f"A pin for another map passed: {result.problems}")


def repeat_pin(position: int, **change: str) -> Callable[[Any], None]:
    """A second pin of the first pinned table, in front of the real one or after every pin."""
    return lambda lock: lock["pins"]["gaps"].insert(position, {**lock["pins"]["gaps"][0], **change})


@pytest.mark.parametrize("position", [0, 99])
@pytest.mark.parametrize("step", ["S99", None])
def test_a_repeated_gap_pin_is_refused_and_the_step_of_every_pin_is_checked(tmp_path: Path, position: int, step: str | None) -> None:
    """A repeated table hides no pin: I7 refuses the repeat wherever it stands and I8 reads the label of every pin."""
    root = fresh_copy(tmp_path / "tree")
    table = json.loads((root / LOCK).read_bytes())["pins"]["gaps"][0]["table"]
    edit_json(root, LOCK, repeat_pin(position, **({"step": step} if step else {})))
    result = registry.check(root, scope_report=scope_report(), skip=NO_CLOSURES)
    repeats = [problem for problem in result.problems if problem.predicate == "I7" and "pinned more than once" in problem.message]
    check([problem.subject for problem in repeats] == [table], f"The repeated pin was not refused: {result.problems}")
    labels = [str(problem) for problem in result.problems if problem.predicate == "I8"]
    check(bool(labels) == bool(step) and all("'S99'" in text for text in labels), f"The labels were not each checked: {result.problems}")


def test_the_labels_of_pins_for_another_map_are_still_checked(tmp_path: Path) -> None:
    root = fresh_copy(tmp_path / "tree")
    edit_json(root, LOCK, lambda lock: (lock["pins"].update(map_sha256="0" * 64), lock["pins"]["gaps"][0].update(step="S99")))
    result = registry.check(root, scope_report=scope_report(), skip=NO_CLOSURES)
    check(named(result.problems, "I8") == [json.loads((root / LOCK).read_bytes())["pins"]["gaps"][0]["table"]], f"The label was skipped: {result.problems}")


@pytest.mark.parametrize(
    ("relative", "first"),
    [(LOCK, '"step": "S99", '), (COLLECTIONS, '"step": "S99", ')],
)
def test_a_json_object_that_repeats_a_key_is_refused_by_name(tmp_path: Path, relative: str, first: str) -> None:
    """The parser keeps the last of two keys, so a repeated key would hide an invalid migration step."""
    root = fresh_copy(tmp_path / "tree")
    text = (root / relative).read_text()
    replace(root, relative, text.replace('"step": ', first + '"step": ', 1).encode())
    if relative != LOCK:
        edit_json_digest(root, relative)
    code, output = command(["check", "--root", str(root)])
    check(code == 1 and relative in output and "Duplicate JSON key: step" in output, f"The repeated key was accepted: {code} {output[:600]}")
    if relative != LOCK:
        code, output = command(["lock", "--root", str(root)])
        check(code == 2 and "Duplicate JSON key: step" in output, f"The lock command took a repeated key: {code} {output[:600]}")


def edit_json_digest(root: Path, relative: str) -> None:
    """Write the lock's digest of one bound file again."""
    digest = hashlib.sha256((root / relative).read_bytes()).hexdigest()
    edit_json(root, LOCK, lambda lock: next(row for row in lock["files"] if row["path"] == relative).update(sha256=digest))


def test_the_pinned_census_seat_gap_is_needed_and_removing_it_names_the_mode(tmp_path: Path) -> None:
    """Case 3d: the census_acs_tables units miss the stored-mirror plan modes on this base; removing the pinned seat gap makes I11 name the mode."""
    result = registry.check(registry.ROOT, scope_report=scope_report(), skip=NO_CLOSURES)
    check(result.seat_gaps == {"census_acs_tables": ["PLAN_MODES"]}, f"The computed seat gap changed: {result.seat_gaps}")
    root = fresh_copy(tmp_path / "tree")
    edit_json(root, LOCK, lambda lock: lock["pins"]["seat_gaps"].pop("census_acs_tables"))
    result = registry.check(root, scope_report=scope_report(), skip=NO_CLOSURES)
    check(named(result.problems, "I11") == ["census_acs_tables"], f"I11 did not name the mode: {result.problems}")


def test_an_edited_closure_file_names_its_entry_point(tmp_path: Path) -> None:
    """Case 4a: an edit to a file only one entry point can run (the workbook module is in the workbook closure alone) fails I6 for that entry point alone."""
    root = fresh_copy(tmp_path / "tree")
    append_text(root, "scripts/acquisition/store_hud_xlsx.py", "# edited\n")
    result = registry.check(root, scope_report=scope_report())
    check(named(result.problems, "I6") == ["hud_api_workbook"], f"I6 did not name only the edited entry point: {result.problems}")


def test_an_edit_to_a_shared_closure_file_names_every_entry_point_that_runs_it(tmp_path: Path) -> None:
    """Case 4b: an edit to a file in every closure fails every entry point (13 of 13 on this base)."""
    root = fresh_copy(tmp_path / "tree")
    append_text(root, "scripts/acquisition/code_closure.py", "# edited\n")
    result = registry.check(root, scope_report=scope_report())
    check(len(named(result.problems, "I6")) == len(registry.ENTRY_POINTS), f"A shared file did not fail every entry point: {named(result.problems, 'I6')}")


def version_hashes(version: Mapping[str, Any]) -> dict[str, str]:
    """The path to SHA-256 map of one committed version, in either of the two forms the lists use."""
    listed = version["code_sha256"]
    return dict(listed) if isinstance(listed, dict) else {row["file"]: row["sha256"] for row in listed}


def test_removing_only_the_workbook_entry_from_the_hud_list_fails_only_the_workbook(tmp_path: Path) -> None:
    """Case 4c: the HUD list serves two entry points; the position of the workbook closure is derived, never written here."""
    root = fresh_copy(tmp_path / "tree")
    path = root / "config/acquisition/hud_api_code_versions.json"
    from scripts.acquisition.code_closure import closure_hashes

    workbook = closure_hashes("scripts.acquisition.store_hud_xlsx", root)
    document = json.loads(path.read_bytes())
    listed = [version for version in document["versions"] if version_hashes(version) != workbook]
    check(len(listed) == len(document["versions"]) - 1, "The workbook closure was not exactly one listed version")
    document["versions"] = listed
    replace(root, "config/acquisition/hud_api_code_versions.json", (json.dumps(document, indent=2) + "\n").encode())
    result = registry.check(root, scope_report=scope_report())
    check(named(result.problems, "I6") == ["hud_api_workbook"], f"Only the workbook entry point should fail: {result.problems}")


def test_the_entry_table_covers_exactly_the_version_lists_on_disk(tmp_path: Path) -> None:
    """Case 4d: a list file no entry point checks, and an entry point whose list is missing, are both named."""
    root = fresh_copy(tmp_path / "tree")
    replace(root, "config/acquisition/zz_new_code_versions.json", b'{"versions": []}\n')
    result = registry.check(root, scope_report=scope_report())
    check(named(result.problems, "I6") == ["zz_new"], f"An unchecked version list was not named: {result.problems}")
    root = fresh_copy(tmp_path / "second")
    shutil.copyfile(root / "config/acquisition/wonder_export_code_versions.json", root / "config/acquisition/wonder_export_code_versions.json.bak")
    (root / "config/acquisition/wonder_export_code_versions.json").unlink()
    result = registry.check(root, scope_report=scope_report())
    check("wonder_export" in named(result.problems, "I6"), f"A missing version list was not named: {result.problems}")


@pytest.mark.parametrize("seat", ["MODES (manual_content)", "SEED_ONLY_MODES"])
def test_a_seed_only_mode_that_one_of_its_seats_does_not_name_is_a_seat_gap(seat: str) -> None:
    """I11 on the two seed-only modes: the manual handler and the seed-only list are their seats, and each one that drops a mode is named on its own."""
    seats = registry.seat_membership()
    seats[seat] = seats[seat] - {"hospital_archive"}
    result = registry.check(registry.ROOT, scope_report=scope_report(), seats=seats, skip=NO_CLOSURES)
    check(named(result.problems, "I11") == ["hospital_archive"], f"I11 did not name the seed-only mode: {result.problems}")
    check(result.seat_gaps["hospital_archive"] == [seat], f"The gap did not name the dropped seat alone: {result.seat_gaps}")


def test_the_entry_table_agrees_with_the_existing_closure_cases() -> None:
    """The table repeats no entry the existing test file lists differently: its ten entries are in the registry's table with the same list and module."""
    from scripts.acquisition.tests.test_committed_code_versions import ENTRIES

    table = {entry.name: (entry.stem, entry.module) for entry in registry.ENTRY_POINTS}
    check(all(table.get(name) == value for name, value in ENTRIES.items()), "An entry of the existing closure test is missing from the registry table")
    check(len(registry.ENTRY_POINTS) == 13 and len({entry.stem for entry in registry.ENTRY_POINTS}) == 12, "The table is not 13 entry points over 12 lists")


def test_the_committed_lock_is_exactly_what_the_lock_command_writes() -> None:
    """The lock is generated: building it again from the committed files gives the committed bytes, so a second run changes nothing."""
    built = registry.build_lock(registry.ROOT, scope_report())
    check(registry.encode(built) == (registry.ROOT / LOCK).read_bytes(), "The committed lock differs from a freshly generated one")
    check(registry.encode(registry.build_lock(registry.ROOT, scope_report())) == registry.encode(built), "Two builds of the lock differ")


def test_the_committed_golden_list_is_exactly_what_the_golden_command_writes(tmp_path: Path) -> None:
    """The golden list is generated from the queue and specifications: the command writes the committed bytes, and a changed unit changes them."""
    golden = "config/ingestion/unit_golden.json"
    root = fresh_copy(tmp_path / "tree")
    (root / golden).unlink()
    output = io.StringIO()
    with redirect_stdout(output):
        code = registry.main(["golden", "--root", str(root)])
    check(code == 0 and (root / golden).read_bytes() == (registry.ROOT / golden).read_bytes(), f"The golden command did not write the committed bytes: {code}")
    edit_json(root, QUEUE, lambda queue: queue["queue"].pop())
    check(registry.encode(registry.build_golden(root)) != (registry.ROOT / golden).read_bytes(), "A removed queue unit left the golden list unchanged")


def test_a_tree_with_no_dbt_project_cannot_run_the_check_and_exits_2(tmp_path: Path) -> None:
    """The check parses the tree's own dbt project; a tree without one is an environment stop (exit 2)."""
    root = fresh_copy(tmp_path / "tree")
    shutil.rmtree(root / "dbt")
    errors = io.StringIO()
    with redirect_stderr(errors):
        code = registry.main(["check", "--root", str(root)])
    check(code == 2 and "cannot run" in errors.getvalue(), f"A tree with no dbt project did not stop with exit 2: {code} {errors.getvalue()!r}")


def test_a_supplied_scope_report_that_differs_is_refused_by_both_commands_and_removes_no_readiness(tmp_path: Path) -> None:
    """The scope comes from the tree's dbt project. A supplied report with a table moved out of scope (same map digest) is an I1 problem: the lock command
    writes nothing and the release report keeps the table's R1 item."""
    root = fresh_copy(tmp_path / "tree")
    reduced = copy.deepcopy(scope_report())
    row = next(row for row in reduced["in_scope"] if row["table"] == "cms_hai_hospital")
    reduced["in_scope"].remove(row)
    reduced["leaving"].append(row)
    report_file = tmp_path / "reduced_scope.json"
    report_file.write_bytes(registry.scope.report_bytes(reduced))
    before = (root / LOCK).read_bytes()
    code, text = command(["lock", "--root", str(root), "--scope-report", str(report_file)])
    check(code == 1 and "I1 scope report" in text, f"The lock command took a reduced scope report: {code} {text}")
    check((root / LOCK).read_bytes() == before, "The refused lock command wrote the lock")
    code, text = command(["check", "--release", "--root", str(root), "--scope-report", str(report_file)])
    check(code == 1 and "I1 scope report" in text, f"The check took a reduced scope report: {code} {text[-600:]}")
    check("R1 cms_hai_hospital:" in text and "fail (102 problems)" in text, f"The reduced report removed readiness: {text[-600:]}")


def drop_row(root: Path, name: str) -> None:
    edit_json(root, LOCK, lambda lock: lock.update(files=[row for row in lock["files"] if row["path"] != name]))


def delete_bound(name: str) -> Callable[[Path], None]:
    def change(root: Path) -> None:
        (root / name).unlink()

    return change


def delete_and_unlist(name: str) -> Callable[[Path], None]:
    def change(root: Path) -> None:
        (root / name).unlink()
        drop_row(root, name)

    return change


def parent(**change: str) -> Callable[[Path], None]:
    return lambda root: edit_json(root, LOCK, lambda lock: lock["parents"]["source_registry"].update(change))


def second_row(digest: str | None) -> Callable[[Path], None]:
    """A second row for the queue, in front of the real one, with a conflicting digest or (None) the same one."""

    def change(root: Path) -> None:
        edit_json(root, LOCK, lambda lock: lock["files"].insert(0, {"path": QUEUE, "sha256": digest or registry.lock_files(lock)[QUEUE]}))

    return change


ACTIVE_PLAN = "config/acquisition/census_acs_tables_plan.json"
ACTIVE_LOCK = "config/acquisition/census_acs_tables_plan.lock.json"
CLASSIFIED_LOCK = "config/acquisition/hcai_util_2018_2025_plan.lock.json"
BROKEN_BINDINGS: dict[str, tuple[Callable[[Path], None], str]] = {
    "the active Census plan deleted": (delete_bound(ACTIVE_PLAN), ACTIVE_PLAN),
    "the active Census plan lock deleted and unlisted": (delete_and_unlist(ACTIVE_LOCK), ACTIVE_LOCK),
    "a manual handler plan deleted and unlisted": (delete_and_unlist("config/acquisition/wonder_export_plan.json"), "wonder_export_plan.json"),
    "a classified plan lock deleted and unlisted": (delete_and_unlist(CLASSIFIED_LOCK), CLASSIFIED_LOCK),
    "a false source registry parent digest": (parent(sha256="0" * 64), "source registry parent"),
    "a source registry parent with another path": (parent(path=QUEUE), "source registry parent"),
    "a second queue row with a conflicting digest": (second_row("0" * 64), QUEUE),
    "a second queue row with the same digest": (second_row(None), QUEUE),
}


@pytest.mark.parametrize("name", list(BROKEN_BINDINGS))
def test_a_broken_lock_binding_is_an_i1_problem_that_names_it(tmp_path: Path, name: str) -> None:
    change, subject = BROKEN_BINDINGS[name]
    root = fresh_copy(tmp_path / "tree")
    change(root)
    result = registry.check(root, scope_report=scope_report(), skip=NO_CLOSURES)
    found = [problem for problem in result.problems if problem.predicate == "I1"]
    check(not result.ok and any(subject in str(problem) for problem in found), f"{name} was not named by I1: {result.problems}")


@pytest.mark.parametrize("name", [ACTIVE_PLAN, ACTIVE_LOCK, CLASSIFIED_LOCK])
def test_the_lock_command_refuses_a_tree_that_lacks_a_bound_plan_file_and_names_it(tmp_path: Path, name: str) -> None:
    root = fresh_copy(tmp_path / "tree")
    (root / name).unlink()
    before = (root / LOCK).read_bytes()
    code, text = command(["lock", "--root", str(root)])
    check(code == 2 and name in text, f"The lock command did not refuse the missing {name}: {code} {text}")
    check((root / LOCK).read_bytes() == before, "The refused lock command wrote the lock")


def classify_a_missing_plan(root: Path) -> None:
    edit_json(root, COLLECTIONS, lambda d: d["plans"].append({"path": "config/acquisition/none.json", "lifecycle": "retired"}))


def test_a_classified_plan_that_is_not_on_disk_stops_the_lock_command_by_name(tmp_path: Path) -> None:
    root = fresh_copy(tmp_path / "tree")
    classify_a_missing_plan(root)
    code, text = command(["lock", "--root", str(root)])
    check(code == 2 and "config/acquisition/none" in text, f"A classified plan that is not on disk was locked: {code} {text}")


def drop_an_outcome(root: Path) -> None:
    edit_json(root, "config/ingestion/manual_outcomes.json", lambda d: d["outcomes"].pop(0))


def add_a_field(root: Path) -> None:
    edit_json(root, COLLECTIONS, lambda d: d["collections"][0].update(surprise=1))


@pytest.mark.parametrize("draft", [drop_an_outcome, add_a_field])
def test_an_incomplete_authored_draft_keeps_every_readiness_item(tmp_path: Path, draft: Callable[[Path], None]) -> None:
    """Readiness comes from the queue, the map and the scope, so an authored file that fails I2 removes no item and no readiness problem."""
    baseline = registry.check(fresh_copy(tmp_path / "committed"), release=True, scope_report=scope_report(), skip=NO_CLOSURES)
    root = fresh_copy(tmp_path / "tree")
    draft(root)
    registry.write_lock(root, scope_report())
    result = registry.check(root, release=True, scope_report=scope_report(), skip=NO_CLOSURES)
    check(any(problem.predicate == "I2" for problem in result.problems), f"The draft was not named by I2: {result.problems}")
    check(result.readiness == baseline.readiness and len(result.readiness) >= 9, f"The readiness items moved: {result.readiness}")
    check(list(map(str, result.release_problems)) == list(map(str, baseline.release_problems)), "The readiness problems moved with an incomplete draft")
    check(all(problem in result.problems for problem in result.release_problems), "check --release dropped readiness problems")


LAYOUT = "config/acquisition/collection_layout.json"
SPECS = "config/acquisition/baseline_run/capture_specs.json"
QUEUE_LOCK = "config/acquisition/baseline_run/queue.lock.json"
TABLE_MAP = "config/lakehouse/bronze_tables.json"


def rewrite(root: Path, relative: str, old: str, new: str) -> None:
    """Change the first place a file holds some text."""
    text = (root / relative).read_text()
    check(old in text, f"{relative} does not hold {old!r}")
    replace(root, relative, text.replace(old, new, 1).encode())


def second_dictionary_key(root: Path) -> list[str]:
    """A second dictionary table that another root already requires: the parser would keep it and drop the real dictionary from the scope."""
    real = '"table": "cms_hospital_cost_reports_dictionaries",\n        "file_pattern"'
    rewrite(root, TABLE_MAP, real, real.replace('"file_pattern"', '"table": "cms_hospitals_dictionaries",\n        "file_pattern"'))
    return [TABLE_MAP]


def byte_order_mark(root: Path) -> list[str]:
    """A UTF-8 byte-order mark before the table map: the silver pin reader and the AWS bronze wrapper cannot read such a file."""
    replace(root, TABLE_MAP, codecs.BOM_UTF8 + (root / TABLE_MAP).read_bytes())
    return [TABLE_MAP]


def source_routed_twice(root: Path) -> list[str]:
    edit_json(root, LAYOUT, lambda layout: layout["groups"][0]["source_ids"].append(layout["groups"][0]["source_ids"][0]))
    return [LAYOUT]


def queue_lock_count(root: Path) -> list[str]:
    edit_json(root, QUEUE_LOCK, lambda lock: lock.update(entries=lock["entries"] + 1))
    return [QUEUE_LOCK]


def spec_field(change: Callable[[dict[str, Any]], object], held: str) -> Callable[[Path], list[str]]:
    """Change the first capture specification that holds a non-empty field."""

    def edit(root: Path) -> list[str]:
        edit_json(root, SPECS, lambda document: change(next(spec for spec in document["specs"].values() if spec.get(held))))
        return [SPECS]

    return edit


REFUSED_INPUTS: dict[str, tuple[Callable[[Path], list[str]], str]] = {
    "a table map that repeats a dictionary table key": (second_dictionary_key, "Duplicate JSON key: table"),
    "a table map with a byte-order mark": (byte_order_mark, "Unexpected UTF-8 BOM"),
    "a source the layout routes twice": (source_routed_twice, "A source cannot route to multiple collections"),
    "a queue lock with another entry count": (queue_lock_count, "Queue entries differ from its lock"),
    "stored file names that are one string": (
        spec_field(lambda spec: spec.update(stored_data_files=spec["stored_data_files"][0]), "stored_data_files"),
        f"{SPECS} lacks the structure its reader needs",
    ),
    "an archive with an empty member list": (
        spec_field(lambda spec: spec.update(archive_members=[]), "archive_members"),
        f"{SPECS} lacks the structure its reader needs",
    ),
    "an archive member whose chain is one string": (
        spec_field(lambda spec: spec["archive_members"][0].update(member_chain="a.csv"), "archive_members"),
        f"{SPECS} lacks the structure its reader needs",
    ),
}


def rebind_by_hand(root: Path, changed: list[str]) -> None:
    """Write the lock rows of some files as a lock that accepted them would hold them."""

    def change(lock: Any) -> None:
        rows = {row["path"]: row["sha256"] for row in lock["files"]}
        rows.update({name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in changed})
        lock["files"] = [{"path": name, "sha256": digest} for name, digest in sorted(rows.items())]

    edit_json(root, LOCK, change)


def refused_by_both_commands(root: Path, changed: list[str], reason: str) -> str:
    """The lock command writes nothing and names the reason; the check of a lock that binds the bytes anyway fails by the same reason."""
    before = (root / LOCK).read_bytes()
    code, output = command(["lock", "--root", str(root)])
    check(code == 2 and reason in output and (root / LOCK).read_bytes() == before, f"The lock command took the input: {code} {output[:600]}")
    rebind_by_hand(root, changed)
    code, output = command(["check", "--release", "--root", str(root)])
    check(code != 0 and reason in output, f"The check did not refuse the bound input: {code} {output[:900]}")
    return output


@pytest.mark.parametrize("name", list(REFUSED_INPUTS))
def test_an_input_the_runtime_readers_refuse_is_refused_after_a_relock(tmp_path: Path, name: str) -> None:
    """A new lock does not make a file pass that the pipeline's own readers cannot read, and no count is made from it."""
    change, reason = REFUSED_INPUTS[name]
    root = fresh_copy(tmp_path / "tree")
    output = refused_by_both_commands(root, change(root), reason)
    check("check --release: pass" not in output and "in dataset-ID collections" not in output, f"A count was made from a refused input: {output[:900]}")


@pytest.mark.parametrize("reader", [bronze.load_table_map, pins.mapped_tables])
def test_the_table_map_loader_and_the_pin_reader_both_refuse_a_byte_order_mark(tmp_path: Path, reader: Callable[[Path], object]) -> None:
    root = fresh_copy(tmp_path / "tree")
    byte_order_mark(root)
    try:
        reader(root / TABLE_MAP)
    except ValueError as error:
        check("Unexpected UTF-8 BOM" in str(error), f"Another refusal: {error}")
    else:
        check(False, "The table map with a byte-order mark was read")


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (b'{"tables": [], "tables": []}', "Duplicate JSON key: tables"),
        (b'{"tables": [], "unused": NaN}', "Invalid JSON number: NaN"),
        (b"[]", "must contain a JSON object"),
    ],
)
def test_the_table_map_loader_stays_strict(tmp_path: Path, body: bytes, reason: str) -> None:
    (tmp_path / "bronze_tables.json").write_bytes(body)
    try:
        bronze.load_table_map(tmp_path / "bronze_tables.json")
    except ValueError as error:
        check(reason in str(error), f"Another refusal: {error}")
    else:
        check(False, f"The table map was read: {body!r}")


def test_a_bound_schema_that_repeats_a_key_is_unusable(tmp_path: Path) -> None:
    """The parser keeps the last of two keywords, so a second one would open a schema that its first one closes."""
    root = fresh_copy(tmp_path / "tree")
    schema = "config/ingestion/schemas/collections.schema.json"
    rewrite(root, schema, '\n  "additionalProperties": false,', '\n  "additionalProperties": false,\n  "additionalProperties": true,')
    edit_json(root, COLLECTIONS, lambda document: document.update(surprise=1))
    registry.write_lock(root, scope_report())
    result = registry.check(root, scope_report=scope_report(), skip=NO_CLOSURES)
    check(
        any(problem.predicate == "I2" and problem.subject == schema and "not a usable" in problem.message for problem in result.problems), f"{result.problems}"
    )


@pytest.mark.parametrize("way", ["an absolute path", "a path through the parent folder", "a link out of the tree"])
def test_a_classified_plan_outside_the_tree_is_refused_by_both_commands(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, way: str) -> None:
    """The lock names the files of the checked tree, so a classified plan and its lock are regular files inside it."""
    root = fresh_copy(tmp_path / "tree")
    outside = tmp_path / "outside"
    outside.mkdir()
    for name in ("plan.json", "plan.lock.json"):
        (outside / name).write_text("{}\n")
    paths = {"an absolute path": str(outside / "plan.json"), "a path through the parent folder": "../outside/plan.json"}
    plan = paths.get(way, "config/acquisition/linked_plan.json")
    if way not in paths:
        (root / plan).symlink_to(outside / "plan.json")
        (root / registry.lock_of(plan)).symlink_to(outside / "plan.lock.json")
    edit_json(root, COLLECTIONS, lambda document: document["plans"].append({"path": plan, "lifecycle": "retired"}))
    refused_by_both_commands(root, [COLLECTIONS, plan, registry.lock_of(plan)], "is no readable file inside the tree")
    opened: list[str] = []
    read_bytes = Path.read_bytes

    def tracked(path: Path) -> bytes:
        if path.resolve().parent == outside.resolve():
            opened.append(path.name)
        return read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", tracked)
    for name in ("lock", "check"):
        command([name, "--root", str(root)])
        check(not opened, f"The {name} command read a file outside the tree before it refused it: {opened}")
