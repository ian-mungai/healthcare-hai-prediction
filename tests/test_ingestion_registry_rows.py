"""The integrity rows the ten first cases do not reach.

I2 (authored files), I3 (unit keys), I8 (migration labels), I9 (closed sources) and I12 (layout contracts). Rows that read files edit a copy of the committed
inputs and write its lock again, so the edit reaches the predicate and not I1. Rows that read parsed values use synthetic trees.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from scripts.ingestion import registry
from tests.registry_support import edit_json, fresh_copy, scope_report
from tests.support import check

COLLECTIONS = "config/ingestion/collections.json"
OUTCOMES = "config/ingestion/manual_outcomes.json"
GOLDEN = "config/ingestion/unit_golden.json"
CONTRACTS = "config/ingestion/layout_contracts.json"
NO_CLOSURES = frozenset({"I6"})


def subjects(problems: list[registry.Problem], predicate: str) -> list[str]:
    return sorted({problem.subject for problem in problems if problem.predicate == predicate})


def checked(root: Path) -> list[registry.Problem]:
    """Write the copy's lock again (so the edit is bound) and run the check without the closure walk."""
    registry.write_lock(root, scope_report())
    return registry.check(root, scope_report=scope_report(), skip=NO_CLOSURES).problems


def test_an_added_removed_or_mistyped_field_in_an_authored_file_fails_i2(tmp_path: Path) -> None:
    """I2: each of the three edits fails and names the file."""
    root = fresh_copy(tmp_path / "added")
    edit_json(root, COLLECTIONS, lambda document: document["collections"][0].update(surprise=1))
    check(subjects(checked(root), "I2") == [COLLECTIONS], "An added field passed the schema")
    root = fresh_copy(tmp_path / "removed")
    edit_json(root, OUTCOMES, lambda document: document["outcomes"][0].pop("basis"))
    check(subjects(checked(root), "I2") == [OUTCOMES], "A removed field passed the schema")
    root = fresh_copy(tmp_path / "mistyped")
    edit_json(root, COLLECTIONS, lambda document: document["collections"][0]["sources"][0].update(acquisition="running"))
    check(subjects(checked(root), "I2") == [COLLECTIONS], "A mistyped state passed the schema")


def test_authored_files_that_disagree_with_the_inputs_they_classify_fail_i2(tmp_path: Path) -> None:
    """I2: each way an authored file can disagree with the inputs it classifies fails and names the subject.

    A decision reference that is not the registry's, a map collection or layout source with no record, an outcome set that is not the manual units,
    a feed or gap for an unmapped table and a plan that is not on disk.
    """
    committed = json.loads((registry.ROOT / COLLECTIONS).read_bytes())
    first = committed["collections"][0]
    shared = next(collection for collection in committed["collections"] if len(collection["sources"]) > 1)
    cases: dict[str, Any] = {
        "decision_ref": (
            COLLECTIONS,
            lambda d: next(s for c in d["collections"] for s in c["sources"] if s["source_id"] == "IL")["decision_ref"].update(sha256="0" * 64),
            "IL",
        ),
        "collection_missing": (COLLECTIONS, lambda d: d["collections"].pop(0), first["collection"]),
        "source_missing": (
            COLLECTIONS,
            lambda d: next(c for c in d["collections"] if c["collection"] == shared["collection"])["sources"].pop(),
            shared["sources"][-1]["source_id"],
        ),
        "outcome_unit": (OUTCOMES, lambda d: d["outcomes"].pop(), "outcome"),
        "feed_table": (COLLECTIONS, lambda d: d["feeds"].append({"table": "zz_none", "source_id": "ACS", "name_regex": "x"}), "zz_none"),
        "gap_table": (COLLECTIONS, lambda d: d["gaps"].update(zz_none={"class": "x", "step": "S9", "mode": "unit_records"}), "zz_none"),
        "plan_path": (COLLECTIONS, lambda d: d["plans"].append({"path": "config/acquisition/none.json", "lifecycle": "retired"}), "none.json"),
    }
    for label, (path, change, word) in cases.items():
        root = fresh_copy(tmp_path / label)
        edit_json(root, path, change)
        found = [problem for problem in checked(root) if problem.predicate == "I2"]
        check(found and any(word in str(problem) for problem in found), f"{label}: the disagreement was not named ({word}): {found}")


def test_a_collection_that_left_the_bronze_load_still_needs_its_record(tmp_path: Path) -> None:
    """I2: the four collections with no mapped table keep their records because the leaving record names them; dropping one fails and names it."""
    root = fresh_copy(tmp_path / "tree")
    edit_json(root, COLLECTIONS, lambda d: d.update(collections=[c for c in d["collections"] if c["collection"] != "nsi/retention_and_staffing"]))
    check(any("nsi/retention_and_staffing" in str(problem) for problem in checked(root)), "A leaving collection lost its record unnoticed")


def test_a_lock_that_records_other_decisions_than_the_authored_file_fails_i2(tmp_path: Path) -> None:
    root = fresh_copy(tmp_path / "tree")
    edit_json(root, "config/ingestion/registry.lock.json", lambda lock: lock["decisions"]["D1"].update(record_sha256="0" * 64))
    result = registry.check(root, scope_report=scope_report(), skip=NO_CLOSURES)
    check(subjects(result.problems, "I2") == ["D1"], f"A changed recorded decision passed: {result.problems}")


def unit(snapshot: str, url: str = "https://example.org/a.csv", route: str = "publisher_url", mode: str = "file") -> tuple[dict[str, Any], dict[str, Any]]:
    return (
        {"snapshot_id": snapshot, "source_id": "SYN", "mode": mode, "route": route, "pages": 1, "url": url, "flags": [], "receipt": "r"},
        {"mode": mode, "expected_format": "csv", "stored_data_files": ["a.csv"]},
    )


def unit_tree(units: list[tuple[dict[str, Any], dict[str, Any]]], golden: list[dict[str, Any]]) -> registry.Tree:
    counts = {"units": len(units), "file": len(units), "plan_mode": 0, "manual": 0, "receipt_bearing": len(units)}
    return registry.Tree(
        root=registry.ROOT,
        units=[item[0] for item in units],
        specs={item[0]["snapshot_id"]: item[1] for item in units},
        routes={},
        tables=[],
        collections={},
        golden={"counts": counts, "units": golden},
        manual={},
        contracts={},
        lock={},
        source_registry={},
    )


def golden_for(units: list[tuple[dict[str, Any], dict[str, Any]]]) -> list[dict[str, Any]]:
    return [{"unit": registry.unit_key(*item)[0], "kind": "unit_key", "class": "file", "receipt": True} for item in units]


def test_unit_keys_that_repeat_or_differ_from_the_golden_list_fail_i3() -> None:
    """I3: a repeated key, a key the golden list lacks, a key only the golden list has, a changed class and changed counts each fail."""
    first, second = unit("SYN__1", "https://example.org/a.csv"), unit("SYN__2", "https://example.org/b.csv")
    good = [first, second]
    check(not registry.unit_problems(unit_tree(good, golden_for(good))), "A matching golden list failed")
    twin = unit("SYN__3", "https://example.org/a.csv")
    repeated = registry.unit_problems(unit_tree([first, twin], golden_for([first])))
    check(subjects(repeated, "I3") != [] and any("two units" in problem.message for problem in repeated), f"A repeated key passed: {repeated}")
    dropped = registry.unit_problems(unit_tree(good, golden_for([first])))
    check(any("lacks" in problem.message for problem in dropped), f"A key the golden list lacks passed: {dropped}")
    extra = registry.unit_problems(unit_tree([first], golden_for(good)))
    check(any("no unit has it" in problem.message for problem in extra), f"A golden-only key passed: {extra}")
    changed = golden_for(good)
    changed[0]["class"] = "manual"
    check(any("changed class" in problem.message for problem in registry.unit_problems(unit_tree(good, changed))), "A changed class passed")
    built = unit_tree(good, golden_for(good))
    built.golden["counts"]["receipt_bearing"] = 1
    check(subjects(registry.unit_problems(built), "I3") == ["receipt_bearing"], "A changed receipt count passed")


def test_the_committed_golden_list_names_the_class_counts_of_the_design() -> None:
    """The golden list holds 2,050 units in classes 666 + 1,290 + 94 with 1,867 receipt-bearing, and the queue gives the same."""
    golden = json.loads((registry.ROOT / GOLDEN).read_bytes())
    check(golden["counts"] == {"units": 2050, "file": 666, "plan_mode": 1290, "manual": 94, "receipt_bearing": 1867}, f"{golden['counts']}")
    check(len({row["unit"] for row in golden["units"]}) == 2050, "The golden list repeats a key")


def test_a_migration_label_that_is_not_a_design_step_fails_i8(tmp_path: Path) -> None:
    root = fresh_copy(tmp_path / "tree")
    edit_json(root, "config/ingestion/registry.lock.json", lambda lock: lock["pins"]["gaps"][0].update(step="S99"))
    result = registry.check(root, scope_report=scope_report(), skip=NO_CLOSURES)
    check(
        subjects(result.problems, "I8") == [json.loads((root / "config/ingestion/registry.lock.json").read_bytes())["pins"]["gaps"][0]["table"]],
        f"{result.problems}",
    )


def source_tree(sources: list[dict[str, Any]], units: list[tuple[dict[str, Any], dict[str, Any]]]) -> registry.Tree:
    built = unit_tree(units, [])
    built.collections = {"collections": [{"collection": "pub/coll", "sources": sources}]}
    return built


def source(state: str, scope_kind: str = "whole_source", **extra: Any) -> dict[str, Any]:
    return {"source_id": "SYN", "acquisition": state, "admission": "none", "scope": scope_kind, "decision_ref": None, **extra}


def automatic(**kwargs: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    return unit("SYN__1", **kwargs)


def manual() -> tuple[dict[str, Any], dict[str, Any]]:
    return unit("SYN__2", route="manual_browser_download")


def test_a_unit_inside_a_closed_scope_fails_i9() -> None:
    """I9: a whole-source closure, a hold, a source never scheduled or a manual-only source refuses an automatic unit; a manual unit is allowed under each."""
    for state in ("held_access", "not_scheduled", "excluded", "manual"):
        check(subjects(registry.source_problems(source_tree([source(state)], [automatic()])), "I9") == ["SYN"], f"{state} accepted an automatic unit")
        check(not registry.source_problems(source_tree([source(state)], [manual()])), f"{state} refused a manual unit")
    closed = source("closed_partial", "whole_source")
    check(subjects(registry.source_problems(source_tree([closed], [automatic()])), "I9") == ["SYN"], "A whole-source closure accepted a unit")
    check(not registry.source_problems(source_tree([source("scheduled")], [automatic()])), "A scheduled source refused a unit")


def test_a_payload_or_vintage_closure_refuses_only_units_inside_its_scope() -> None:
    inside = source("closed_partial", "vintage", match={"url_regex": r"/2016/"})
    check(
        subjects(registry.source_problems(source_tree([inside], [automatic(url="https://example.org/2016/a.csv")])), "I9") == ["SYN"],
        "A unit inside the vintage passed",
    )
    check(not registry.source_problems(source_tree([inside], [automatic(url="https://example.org/2018/a.csv")])), "A unit outside the vintage failed")
    untestable = source("closed_partial", "payload")
    check(not registry.source_problems(source_tree([untestable], [automatic()])), "An untestable scope failed instead of being listed for review")
    check(registry_review(source_tree([untestable], [automatic()])) == ["SYN"], "An untestable scope was not listed for manual review")


def registry_review(built: registry.Tree) -> list[str]:
    from scripts.ingestion import registry_readiness

    return registry_readiness.manual_review_sources(built)


def contract(table: str = "t_one", **change: Any) -> registry.Tree:
    columns = ["a", "b"]
    entry: dict[str, Any] = {
        "consumer_columns": ["a"],
        "accepted_layouts": [{"id": "one", "columns": columns, "columns_sha256": registry.sha256_bytes("\n".join(sorted(columns)).encode()), "note": "n"}],
    }
    entry.update(change)
    built = unit_tree([], [])
    built.tables = [{"table": "t_one", "collection": "pub/coll"}]
    built.contracts = {"tables": {table: entry}}
    return built


def test_a_malformed_layout_contract_fails_i12() -> None:
    """I12: a good contract passes; a wrong column digest, a table not in the map, a repeated layout and a consumer column listed twice each fail."""
    check(not registry.contract_problems(contract()), "A good contract failed")
    bad_sha = copy.deepcopy(contract())
    bad_sha.contracts["tables"]["t_one"]["accepted_layouts"][0]["columns_sha256"] = "0" * 64
    check(subjects(registry.contract_problems(bad_sha), "I12") == ["t_one"], "A wrong column digest passed")
    check(subjects(registry.contract_problems(contract(table="zz_none")), "I12") == ["zz_none"], "A contract for an unmapped table passed")
    twice = contract()
    twice.contracts["tables"]["t_one"]["accepted_layouts"].append(copy.deepcopy(twice.contracts["tables"]["t_one"]["accepted_layouts"][0]))
    check(subjects(registry.contract_problems(twice), "I12") == ["t_one"], "A repeated layout passed")
    check(subjects(registry.contract_problems(contract(consumer_columns=["a", "a"])), "I12") == ["t_one"], "A repeated consumer column passed")
    check(not registry.contract_problems(contract(consumer_columns="any")), "A table that declares any consumer column failed")


def test_a_contract_whose_column_list_is_not_a_list_fails_i12(tmp_path: Path) -> None:
    """I12 by file: the schema failure of the contracts file is named under I12, not I2."""
    root = fresh_copy(tmp_path / "tree")
    edit_json(root, CONTRACTS, lambda document: document["tables"]["hcai_finance_csv"].update(consumer_columns="oshpd_id"))
    check(subjects(checked(root), "I12") == [CONTRACTS], "A column list that is not a list passed")


STEPS = ("S0", "S1", "S2", "S3", "S4", "S5", "S6a", "S6b", "S7", "S8", "S8a", "S9", "S10", "S11", "S12", "S13a", "S13b", "S14")


def test_only_the_steps_of_section_8_are_migration_labels() -> None:
    """The label set is exactly the steps of design v4 section 8, alone or as a comma separated list; S6, S13 and near misses are refused."""
    for step in STEPS:
        check(registry.STEP_LIST.fullmatch(step), f"{step} is a step of section 8 and was refused")
    check(registry.STEP_LIST.fullmatch("S9, S10") and registry.STEP_LIST.fullmatch("S6a, S6b, S13a"), "A list of steps was refused")
    for label in ("S6", "S13", "S15", "S99", "S", "", "s6a", "S6a,S6b", "S9 , S10", "S9, S6", "S9, ", "S06", "S8b", "S13c"):
        check(not registry.STEP_LIST.fullmatch(label), f"{label!r} is not a step of section 8 and was accepted")


def test_a_pin_labelled_s6_or_s13_fails_i8_and_the_lock_command_refuses_the_authored_label(tmp_path: Path) -> None:
    """By command: a pinned label S6 or S13 fails I8 and names the table; an authored gap label S6 or S13 stops the lock command."""
    for label in ("S6", "S13"):
        root = fresh_copy(tmp_path / f"pin_{label}")
        edit_json(root, "config/ingestion/registry.lock.json", lambda lock, label=label: lock["pins"]["gaps"][0].update(step=label))
        table = json.loads((root / "config/ingestion/registry.lock.json").read_bytes())["pins"]["gaps"][0]["table"]
        result = registry.check(root, scope_report=scope_report(), skip=NO_CLOSURES)
        check(subjects(result.problems, "I8") == [table], f"A pin labelled {label} did not fail I8: {result.problems}")
        root = fresh_copy(tmp_path / f"authored_{label}")
        edit_json(root, COLLECTIONS, lambda document, label=label: document["gaps"]["hrsa_mua_detail"].update(step=label))
        try:
            registry.write_lock(root, scope_report())
        except registry.RegistryFileError as error:
            check("hrsa_mua_detail" in str(error), f"The refusal does not name the table: {error}")
        else:
            raise AssertionError(f"The lock command accepted the authored label {label}")


def test_a_closed_scope_pattern_that_is_not_a_pattern_is_refused_not_trusted(tmp_path: Path) -> None:
    """For I9: a closure whose match pattern does not compile is an I2 refusal naming the source; an empty match is still listed for manual review."""
    root = fresh_copy(tmp_path / "tree")
    edit_json(root, COLLECTIONS, lambda document: source_of(document, "IL").update(match={"url_regex": "("}))
    found = [problem for problem in checked(root) if problem.predicate == "I2" and problem.subject == "IL"]
    check(len(found) == 1 and "regular expression" in found[0].message, f"A pattern that does not compile was trusted: {found}")
    empty = source("closed_partial", "vintage", match={})
    check(registry_review(source_tree([empty], [automatic()])) == ["SYN"], "An empty match was not listed for manual review")


def source_of(document: Any, source_id: str) -> dict[str, Any]:
    return next(source for collection in document["collections"] for source in collection["sources"] if source["source_id"] == source_id)
