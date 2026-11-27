"""The registry integrity check, cases 1 to 4 of design v4 section 5.1: the lock, byte binding, pinned gaps and seats, and the collector closures.

Each case edits a copy of the committed inputs and runs the real check on it. Nothing here reads S3 or the network, and the repository is not written.
"""

from __future__ import annotations

import io
import json
import shutil
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from scripts.ingestion import registry
from tests.registry_support import append_text, edit_json, fresh_copy, replace, scope_report
from tests.support import check

LOCK = "config/ingestion/registry.lock.json"
QUEUE = "config/acquisition/baseline_run/queue.json"
COLLECTIONS = "config/ingestion/collections.json"
NO_CLOSURES = frozenset({"I6"})


def named(problems: list[registry.Problem], predicate: str) -> list[str]:
    """The subjects the check names for one predicate."""
    return sorted(problem.subject for problem in problems if problem.predicate == predicate)


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


def test_removing_only_the_workbook_entry_from_the_hud_list_fails_only_the_workbook(tmp_path: Path) -> None:
    """Case 4c: the HUD list serves two entry points; the position of the workbook closure is derived, never written here."""
    root = fresh_copy(tmp_path / "tree")
    path = root / "config/acquisition/hud_api_code_versions.json"
    from scripts.acquisition.code_closure import closure_hashes

    workbook = closure_hashes("scripts.acquisition.store_hud_xlsx", root)
    document = json.loads(path.read_bytes())
    listed = [version for version in document["versions"] if registry.version_hashes(version) != workbook]
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


def test_a_seed_only_mode_that_the_manual_handler_does_not_name_is_a_seat_gap() -> None:
    """I11 on the two seed-only modes: the manual handler and the seed-only list are their seats, so a mode one of them drops is named."""
    seats = registry.seat_membership()
    seats["MODES (manual_content)"] = seats["MODES (manual_content)"] - {"hospital_archive"}
    result = registry.check(registry.ROOT, scope_report=scope_report(), seats=seats, skip=NO_CLOSURES)
    check(named(result.problems, "I11") == ["hospital_archive"], f"I11 did not name the seed-only mode: {result.problems}")


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
    """With no supplied scope report the check parses the tree's own dbt project; a tree without one is an environment stop (exit 2)."""
    root = fresh_copy(tmp_path / "tree")
    errors = io.StringIO()
    with redirect_stderr(errors):
        code = registry.main(["check", "--root", str(root)])
    check(code == 2 and "cannot run" in errors.getvalue(), f"A tree with no dbt project did not stop with exit 2: {code} {errors.getvalue()!r}")
