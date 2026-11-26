"""The scope report's logic on hand-built manifests: counts against independent constants, duplicate rows in equality and the exit codes.

No dbt runs here (a fake manifest stands in for the parse), so every case takes milliseconds. test_ingestion_scope.py holds the cases that parse real
and synthetic dbt projects. Every expected number below is computed by hand from the graph written beside it, never from the code under test.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from scripts.ingestion import scope
from scripts.process import LaunchesNotStopped, ProcessGroupSurvived, SessionListingFailed
from scripts.quality import dbt_manifest_labels
from tests.support import check

OPERATIONAL = "stg_bronze__file_copies"


def entry(table: str, collection: str, dictionary: str | None = None) -> dict[str, Any]:
    """One valid table map entry; a dictionary table is a ``csv_rows`` entry made with ``dictionary_of``."""
    row: dict[str, Any] = {"table": table, "group": "g", "collection": collection, "format": "csv", "dataset_ids": [table], "encodings": ["utf-8"]}
    if dictionary:
        row["dictionary"] = {"table": dictionary, "file_pattern": "dictionary"}
    return row


def dictionary_of(table: str, collection: str) -> dict[str, Any]:
    """A table map entry that can serve as a dictionary."""
    return {**entry(table, collection), "format": "csv_rows"}


def table_map(*entries: dict[str, Any]) -> dict[str, Any]:
    """A table map document."""
    return {"version": 1, "namespace": "bronze", "classification": "Public", "tables": list(entries)}


def manifest(readers: dict[str, list[str]], sources: list[str], other_nodes: dict[str, list[str]] | None = None) -> dict[str, Any]:
    """A manifest with bronze sources, model nodes reading them and (in ``other_nodes``) test nodes that read them and must not count."""
    source_ids = {name: f"source.p.bronze.{name}" for name in [*scope.LOADER_TABLES, *sources]}
    nodes: dict[str, Any] = {}
    for model, tables in readers.items():
        nodes[f"model.p.{model}"] = {"resource_type": "model", "name": model, "depends_on": {"nodes": [source_ids[table] for table in tables]}}
    for name, tables in (other_nodes or {}).items():
        nodes[f"test.p.{name}"] = {"resource_type": "test", "name": name, "depends_on": {"nodes": [source_ids[table] for table in tables]}}
    return {
        "metadata": {"dbt_version": "9.9.9"},
        "nodes": nodes,
        "sources": {unique_id: {"source_name": "bronze", "name": name} for name, unique_id in source_ids.items()},
    }


# Graph 1, by hand. Roots t_a, t_b (both documented by d_a) and t_c (documented by d_c); leaving t_x and t_y.
# Collections: x/a holds t_a, t_b, d_a; x/b holds t_c; x/d holds d_c; x/e holds t_x and t_y.
GRAPH_1_MAP = table_map(
    entry("t_a", "x/a", "d_a"),
    entry("t_b", "x/a", "d_a"),
    entry("t_c", "x/b", "d_c"),
    dictionary_of("d_a", "x/a"),
    dictionary_of("d_c", "x/d"),
    entry("t_x", "x/e"),
    entry("t_y", "x/e"),
)
GRAPH_1_COPIES = {"t_a": "fam1", "t_b": "fam1", "t_c": "fam2"}
GRAPH_1_COUNTS = {
    "mapped": 7,  # t_a t_b t_c d_a d_c t_x t_y
    "in_scope_data": 3,  # t_a t_b t_c
    "dictionary": 2,  # d_a (once, though two roots need it) and d_c
    "default_scope": 5,  # 3 roots + 2 dictionaries
    "leaving": 2,  # t_x t_y
    "collections_before": 4,  # x/a x/b x/d x/e
    "collections_retained": 3,  # x/a x/b x/d
    "collections_leaving": 1,  # x/e
    "silver_declared_sources": 7,  # 3 roots + 4 loader tables
    "silver_pins": 4,  # 3 roots + file_metadata
    "silver_pins_if_dictionaries_declared": 6,  # 3 roots + 2 dictionaries + file_metadata
    "glue_mirror_bronze_names": 9,  # 5 scope tables + 4 loader tables
}
GRAPH_1_READERS = {"stg_a": ["t_a"], "stg_b": ["t_b"], "stg_c": ["t_c"], OPERATIONAL: ["t_a", "t_b", "t_c", "t_x"]}
GRAPH_1_SOURCES = ["t_a", "t_b", "t_c", "t_x"]

# Graph 2 adds a root t_z in a new collection x/f with a new dictionary d_z, and one more leaving table.
GRAPH_2_MAP = table_map(*GRAPH_1_MAP["tables"], entry("t_z", "x/f", "d_z"), dictionary_of("d_z", "x/f"), entry("t_w", "x/g"))
GRAPH_2_COPIES = {**GRAPH_1_COPIES, "t_z": "fam3"}
GRAPH_2_COUNTS = {
    "mapped": 10,  # graph 1's 7 + t_z d_z t_w
    "in_scope_data": 4,
    "dictionary": 3,
    "default_scope": 7,
    "leaving": 3,  # t_x t_y t_w
    "collections_before": 6,  # x/a x/b x/d x/e x/f x/g
    "collections_retained": 4,  # x/a x/b x/d x/f
    "collections_leaving": 2,  # x/e x/g
    "silver_declared_sources": 8,  # 4 roots + 4 loader tables
    "silver_pins": 5,  # 4 roots + file_metadata
    "silver_pins_if_dictionaries_declared": 8,  # 4 roots + 3 dictionaries + file_metadata
    "glue_mirror_bronze_names": 11,  # 7 scope tables + 4 loader tables
}


def graph_1() -> tuple[dict[str, Any], list[str]]:
    """The scope report of graph 1; a test node that reads leaving t_y must not make it a root."""
    return scope.derive_scope(manifest(GRAPH_1_READERS, [*GRAPH_1_SOURCES, "t_y"], {"unique_t_y": ["t_y"]}), GRAPH_1_MAP, GRAPH_1_COPIES)


def graph_2() -> tuple[dict[str, Any], list[str]]:
    """The scope report of graph 2."""
    readers = {**GRAPH_1_READERS, "stg_z": ["t_z"], OPERATIONAL: [*GRAPH_1_READERS[OPERATIONAL], "t_z"]}
    return scope.derive_scope(manifest(readers, [*GRAPH_1_SOURCES, "t_y", "t_z", "t_w"]), GRAPH_2_MAP, GRAPH_2_COPIES)


@pytest.mark.parametrize(("graph", "counts"), [(graph_1, GRAPH_1_COUNTS), (graph_2, GRAPH_2_COUNTS)], ids=["graph_1", "graph_2"])
def test_every_count_of_a_graph_equals_its_hand_computed_value(graph: Any, counts: dict[str, int]) -> None:
    """All twelve counts, whole dictionary at once, against numbers computed by hand: a changed or removed formula fails here."""
    report, problems = graph()
    check(problems == [], f"unexpected problems: {problems}")
    check(report["counts"] == counts, f"counts differ: {report['counts']} against {counts}")
    check(report["silver_source_declaration"]["pins"]["decided_roots_only"] == counts["silver_pins"], "the declaration's pins disagree with the counts")
    check(report["silver_source_declaration"]["glue_mirror_names"]["map_union_declared"] == counts["glue_mirror_bronze_names"], "mirror names")
    check(report["silver_source_declaration"]["declared_sources"] == counts["silver_declared_sources"], "declared sources")


def test_graph_1_lists_match_the_hand_derived_scope() -> None:
    """Roots, dictionaries and the dictionary relationship of graph 1, by name; a test node reading t_y does not count."""
    report, _ = graph_1()
    check([row["table"] for row in report["in_scope"]] == ["t_a", "t_b", "t_c"], f"roots: {report['in_scope']}")
    check([(row["table"], row["required_by"]) for row in report["dictionary"]] == [("d_a", ["t_a", "t_b"]), ("d_c", ["t_c"])], f"{report['dictionary']}")
    check([row["table"] for row in report["leaving"]] == ["t_x", "t_y"], f"leaving: {report['leaving']}")
    check([row["source_family"] for row in report["in_scope"]] == ["fam1", "fam1", "fam2"], "source families")
    check(all(row["operational_readers"] == [OPERATIONAL] for row in report["in_scope"]), "operational readers")


@pytest.mark.parametrize(
    ("roots", "dictionaries", "extra", "expected"),
    [
        (3, 2, 0, {"mapped": 5, "declared": 7, "pins": 4, "mirror_names": 9}),
        (3, 2, 1, {"mapped": 5, "declared": 8, "pins": 4, "mirror_names": 10}),
        (3, 2, 2, {"mapped": 5, "declared": 9, "pins": 4, "mirror_names": 11}),
        (74, 11, 3, {"mapped": 85, "declared": 81, "pins": 75, "mirror_names": 92}),
        (0, 0, 0, {"mapped": 0, "declared": 4, "pins": 1, "mirror_names": 4}),
        (74, 11, 0, {"mapped": 85, "declared": 78, "pins": 75, "mirror_names": 89}),
        (74, 11, 1, {"mapped": 85, "declared": 79, "pins": 75, "mirror_names": 90}),
        (75, 11, 0, {"mapped": 86, "declared": 79, "pins": 76, "mirror_names": 90}),
        (75, 11, 1, {"mapped": 86, "declared": 80, "pins": 76, "mirror_names": 91}),
        (73, 11, 0, {"mapped": 84, "declared": 77, "pins": 74, "mirror_names": 88}),
        (73, 11, 1, {"mapped": 84, "declared": 78, "pins": 74, "mirror_names": 89}),
    ],
    ids=["3_2", "3_2_index", "3_2_two_extra", "today_three_extra", "empty", "today", "today_index", "d3_a1", "d3_a1_index", "d3_a2", "d3_a2_index"],
)
def test_projected_counts_equal_independent_constants(roots: int, dictionaries: int, extra: int, expected: dict[str, int]) -> None:
    """The whole returned dictionary, for small hand cases and for the (mapped, declared, pins, mirror names) tuples with and without the index."""
    got = scope.projected_counts(roots, dictionaries, extra_loader_sources=extra)
    check(got == expected, f"{got} against {expected}")


@pytest.mark.parametrize("graph", [graph_1, graph_2], ids=["graph_1", "graph_2"])
def test_the_report_carries_the_interim_and_index_delivered_counts(graph: Any) -> None:
    """Both index states, each as a full (mapped, declared, pins, mirror names) set, agree with the report's own counts for the interim."""
    report, _ = graph()
    counts = report["counts"]
    interim = {"mapped": counts["default_scope"], "declared": counts["silver_declared_sources"], "pins": counts["silver_pins"]}
    interim["mirror_names"] = counts["glue_mirror_bronze_names"]
    delivered = {**interim, "declared": interim["declared"] + 1, "mirror_names": interim["mirror_names"] + 1}
    check(report["counts_by_q4_state"] == {"interim": interim, "index_delivered": delivered}, f"{report['counts_by_q4_state']}")


def test_a_second_graph_moves_the_counts_by_hand_computed_amounts() -> None:
    """Graph 2 minus graph 1: one root, one dictionary, two scope tables, one declared source, one pin, two mirror names, one more collection retained."""
    first, _ = graph_1()
    second, _ = graph_2()
    moved = {key: second["counts"][key] - first["counts"][key] for key in first["counts"]}
    check(
        moved
        == {
            "mapped": 3,
            "in_scope_data": 1,
            "dictionary": 1,
            "default_scope": 2,
            "leaving": 1,
            "collections_before": 2,
            "collections_retained": 1,
            "collections_leaving": 1,
            "silver_declared_sources": 1,
            "silver_pins": 1,
            "silver_pins_if_dictionaries_declared": 2,
            "glue_mirror_bronze_names": 2,
        },
        str(moved),
    )


# The consumer rules on a fake manifest (the real dbt parse of the same cases is in test_ingestion_scope.py).
def graph_1_with(readers: dict[str, list[str]], copies: dict[str, str]) -> tuple[dict[str, Any], list[str]]:
    """Graph 1's map and sources with other readers or copy_tables."""
    return scope.derive_scope(manifest(readers, [*GRAPH_1_SOURCES, "t_y"]), GRAPH_1_MAP, copies)


def test_a_root_missing_from_copy_tables_is_still_a_root_and_is_named() -> None:
    """A model reads t_c but copy_tables lacks it: t_c is a root (the readers decide) and the missing entry is a named problem."""
    report, problems = graph_1_with(GRAPH_1_READERS, {"t_a": "fam1", "t_b": "fam1"})
    check([row["table"] for row in report["in_scope"]] == ["t_a", "t_b", "t_c"], f"roots: {report['in_scope']}")
    check(problems == ["t_c has an analytical reader but copy_tables does not name it"], f"{problems}")


def test_a_copy_table_nobody_reads_is_no_root_and_is_named() -> None:
    """stg_b stops reading t_b but copy_tables still names it: t_b leaves the roots, and the stale entry is the named problem."""
    readers = {**GRAPH_1_READERS, "stg_b": []}
    report, problems = graph_1_with(readers, GRAPH_1_COPIES)
    check([row["table"] for row in report["in_scope"]] == ["t_a", "t_c"], f"roots: {report['in_scope']}")
    check(len(problems) == 1 and "t_b" in problems[0] and "copy_tables" in problems[0], f"{problems}")


def test_a_table_only_the_operational_model_reads_is_no_root_and_is_named() -> None:
    """The operational model reads t_x and nothing else does: t_x stays out of the roots; naming it in copy_tables is the named circle."""
    report, problems = graph_1_with(GRAPH_1_READERS, {**GRAPH_1_COPIES, "t_x": "fam9"})
    check("t_x" not in {row["table"] for row in report["in_scope"]}, "t_x became a root")
    check(len(problems) == 1 and "t_x" in problems[0] and OPERATIONAL in problems[0], f"{problems}")


def test_a_model_reading_a_source_outside_the_map_is_named_with_the_model() -> None:
    """A model reads a bronze source that is neither mapped nor a loader table."""
    readers = {**GRAPH_1_READERS, "stg_ghost": ["t_ghost"]}
    _, problems = scope.derive_scope(manifest(readers, [*GRAPH_1_SOURCES, "t_ghost"]), GRAPH_1_MAP, GRAPH_1_COPIES)
    check(problems == ["model stg_ghost reads bronze source t_ghost: it is not in the table map and not a loader table"], f"{problems}")


def test_a_loader_table_read_by_a_model_is_neither_a_root_nor_a_problem() -> None:
    """A model that reads file_metadata (a loader table) adds no root and no problem."""
    readers = {**GRAPH_1_READERS, "stg_meta": ["file_metadata"]}
    report, problems = graph_1_with(readers, GRAPH_1_COPIES)
    check(problems == [] and [row["table"] for row in report["in_scope"]] == ["t_a", "t_b", "t_c"], f"{problems} {report['in_scope']}")


def test_a_dictionary_that_is_itself_read_is_a_root_and_no_dictionary() -> None:
    """d_a is read by a model, so it is a root; it then no longer counts as a dictionary, and t_a and t_b still need it."""
    readers = {**GRAPH_1_READERS, "stg_d": ["d_a"], OPERATIONAL: [*GRAPH_1_READERS[OPERATIONAL], "d_a"]}
    report, problems = scope.derive_scope(manifest(readers, [*GRAPH_1_SOURCES, "t_y", "d_a"]), GRAPH_1_MAP, {**GRAPH_1_COPIES, "d_a": "fam1"})
    check(problems == [], f"{problems}")
    check([row["table"] for row in report["dictionary"]] == ["d_c"], f"dictionaries: {report['dictionary']}")
    check(report["counts"]["in_scope_data"] == 4 and report["counts"]["dictionary"] == 1 and report["counts"]["default_scope"] == 5, f"{report['counts']}")


# Duplicate rows in equality.
def compact(value: Any) -> str:
    """Compact JSON text, the form the equality messages use."""
    return json.dumps(value, separators=(",", ":"))


def with_row_before(expected: dict[str, Any], group: str, change: dict[str, Any]) -> dict[str, Any]:
    """A copy of ``expected`` with a changed copy of its first row of ``group`` inserted before the original."""
    document = copy.deepcopy(expected)
    document[group].insert(0, {**document[group][0], **change})
    return document


def with_row_after(expected: dict[str, Any], group: str, change: dict[str, Any]) -> dict[str, Any]:
    """The same, with the changed copy appended after the original."""
    document = copy.deepcopy(expected)
    document[group].append({**document[group][0], **change})
    return document


@pytest.mark.parametrize(
    ("group", "change"),
    [("in_scope", {"analytical_readers": ["different_reader"]}), ("dictionary", {"required_by": ["nobody"]}), ("leaving", {"reason": "other"})],
)
def test_a_table_defined_twice_with_conflicting_fields_is_refused_in_either_order(group: str, change: dict[str, Any]) -> None:
    """The same table twice in the expected document: a named problem whichever row comes first, with both definitions, never a silent choice."""
    report, _ = graph_1()
    table = report[group][0]["table"]
    before = scope.equality_problems(report, with_row_before(report, group, change))
    after = scope.equality_problems(report, with_row_after(report, group, change))
    check(before == after, f"the answer depends on the row order: {before} against {after}")
    check(len(before) == 1 and table in before[0] and "more than once" in before[0], f"the duplicate is not named: {before}")
    key = next(iter(change))
    check(compact(change[key]) in before[0] and compact(report[group][0][key]) in before[0], f"both definitions must appear: {before[0]}")


def test_a_duplicate_on_the_report_side_is_refused_too() -> None:
    """The check is symmetric: a report document that repeats a table with conflicting definitions is named."""
    report, _ = graph_1()
    bad = with_row_before(report, "in_scope", {"source_family": "other"})
    found = scope.equality_problems(bad, report)
    check(len(found) == 1 and "t_a" in found[0] and "report" in found[0] and "more than once" in found[0], f"{found}")


def test_a_byte_identical_duplicate_is_tolerated_and_an_annotation_only_difference_is_not() -> None:
    """A repeated row that is byte for byte the same adds no problem; one that differs only outside the projection is still not byte identical."""
    report, _ = graph_1()
    same = copy.deepcopy(report)
    same["in_scope"].append(copy.deepcopy(same["in_scope"][0]))
    check(scope.equality_problems(report, same) == [], "a byte-identical duplicate must not fail")
    annotated = with_row_after(report, "in_scope", {"notes": ["only an annotation differs"]})
    found = scope.equality_problems(report, annotated)
    check(len(found) == 1 and "t_a" in found[0] and "outside the projection" in found[0], f"{found}")


def test_a_conflicting_table_is_not_also_compared_field_by_field() -> None:
    """The conflict is the one problem for that table: no extra, order-dependent field line follows it."""
    report, _ = graph_1()
    found = scope.equality_problems(report, with_row_before(report, "in_scope", {"analytical_readers": ["different_reader"], "source_family": "z"}))
    check(len(found) == 1, f"{found}")


def test_a_row_without_a_table_name_is_named_not_a_crash() -> None:
    """An entry with no ``table`` key is a named problem."""
    report, _ = graph_1()
    broken = copy.deepcopy(report)
    del broken["leaving"][0]["table"]
    found = scope.equality_problems(report, broken)
    check(any("leaving" in line and "no table" in line for line in found), f"{found}")


def test_the_table_map_may_not_repeat_a_table() -> None:
    """A table map document that repeats a name is an input error naming the table, not a silent last-one-wins."""
    duplicated = table_map(entry("t_a", "x/a"), entry("t_a", "x/b"))
    with pytest.raises(scope.ScopeError, match="t_a"):
        scope.derive_scope(manifest({"stg_a": ["t_a"]}, ["t_a"]), duplicated, {"t_a": "f"})


# Every projected field, written out here and not read from the module, so that dropping a field from the projection fails a case.
PROJECTED = {
    "in_scope": (
        "group",
        "collection",
        "format",
        "dictionary_table",
        "roles",
        "analytical_readers",
        "operational_readers",
        "source_family",
        "silver_source_declared",
    ),
    "dictionary": ("group", "collection", "format", "required_by", "silver_source_declared", "silver_pin"),
    "leaving": ("group", "collection", "format", "reason"),
}
ANNOTATIONS = ("queue_units", "collection_without_queue_unit", "queued_output_selected", "notes", "anything_else")
COUNT_NAMES = tuple(GRAPH_1_COUNTS)
DECLARATION_FACTS = ("declared_sources", "analytical_roots", "loader_tables")
PINS_FACTS = ("decided_roots_only", "if_all_scope_tables_declared", "extra_if_all_declared", "current_yml_declared_sources")


@pytest.mark.parametrize(("group", "field"), [(group, field) for group, fields in PROJECTED.items() for field in fields])
def test_a_change_to_any_projected_field_is_named(group: str, field: str) -> None:
    """Changing one projected field of one row gives exactly one problem that names the table and the field."""
    report, _ = graph_1()
    changed = copy.deepcopy(report)
    changed[group][0][field] = (
        "changed" if not isinstance(changed[group][0][field], (list, bool)) else (["changed"] if isinstance(changed[group][0][field], list) else None)
    )
    found = scope.equality_problems(report, changed)
    check(len(found) == 1 and report[group][0]["table"] in found[0] and field in found[0], f"{group}.{field}: {found}")


@pytest.mark.parametrize("group", list(PROJECTED))
def test_a_table_name_change_is_a_report_only_and_an_expected_only_table(group: str) -> None:
    """Renaming a row's table gives one problem for each side, so the table key itself is compared."""
    report, _ = graph_1()
    changed = copy.deepcopy(report)
    changed[group][0]["table"] = "renamed_table"
    found = scope.equality_problems(report, changed)
    check(len(found) == 2 and any("report only" in line for line in found) and any("expected document only" in line for line in found), f"{found}")


@pytest.mark.parametrize("group", list(PROJECTED))
@pytest.mark.parametrize("annotation", ANNOTATIONS)
def test_an_annotation_never_changes_equality(group: str, annotation: str) -> None:
    """A field outside the projection may differ or be absent without a problem."""
    report, _ = graph_1()
    changed = copy.deepcopy(report)
    changed[group][0][annotation] = "any value"
    check(scope.equality_problems(report, changed) == [], f"{group}.{annotation} must not matter")


@pytest.mark.parametrize("count", COUNT_NAMES)
def test_a_change_to_any_count_is_named(count: str) -> None:
    """Each of the twelve counts is compared; a count outside the twelve is not."""
    report, _ = graph_1()
    changed = copy.deepcopy(report)
    changed["counts"][count] += 1
    found = scope.equality_problems(report, changed)
    check(len(found) == 1 and count in found[0], f"{count}: {found}")
    changed["counts"][count] -= 1
    changed["counts"]["leaving_with_queue_units"] = 236
    check(scope.equality_problems(report, changed) == [], "a count outside the twelve must not matter")


@pytest.mark.parametrize("fact", DECLARATION_FACTS)
def test_a_change_to_a_declaration_fact_is_named(fact: str) -> None:
    """The silver declaration facts are compared."""
    report, _ = graph_1()
    changed = copy.deepcopy(report)
    changed["silver_source_declaration"][fact] = ["other"] if fact == "loader_tables" else 99
    found = scope.equality_problems(report, changed)
    check(len(found) == 1 and fact in found[0], f"{fact}: {found}")


@pytest.mark.parametrize("fact", PINS_FACTS)
def test_a_change_to_a_pin_fact_is_named(fact: str) -> None:
    """The pin facts of the declaration are compared."""
    report, _ = graph_1()
    changed = copy.deepcopy(report)
    changed["silver_source_declaration"]["pins"][fact] = ["other"] if fact == "extra_if_all_declared" else 99
    found = scope.equality_problems(report, changed)
    check(len(found) == 1 and fact in found[0], f"{fact}: {found}")


def test_a_change_to_the_mirror_name_count_of_the_declaration_is_named() -> None:
    """The mirror-name count inside the declaration is compared."""
    report, _ = graph_1()
    changed = copy.deepcopy(report)
    changed["silver_source_declaration"]["glue_mirror_names"]["map_union_declared"] = 99
    found = scope.equality_problems(report, changed)
    check(len(found) == 1 and "map_union_declared" in found[0], f"{found}")
    changed["silver_source_declaration"]["decision"] = "text"
    changed["silver_source_declaration"]["glue_mirror_names"] = {"map_union_declared": 9, "rule": "text"}
    check(scope.equality_problems(report, changed) == [], "descriptive text must not matter")


def test_row_order_does_not_matter() -> None:
    """The same rows in another order are equal."""
    report, _ = graph_1()
    changed = copy.deepcopy(report)
    for group in PROJECTED:
        changed[group].reverse()
    check(scope.equality_problems(report, changed) == [], "order must not matter")


# Exit codes, with the parse replaced by a fake manifest so that no dbt runs.
def project_folder(root: Path, copy_tables: str = "    t_a: fam\n") -> Path:
    """A folder holding only a dbt_project.yml with the given ``copy_tables`` lines (the fake parse reads nothing else)."""
    project = root / "project"
    project.mkdir()
    (project / "dbt_project.yml").write_text(f"name: synth\nversion: '1.0'\nconfig-version: 2\nprofile: synth\nvars:\n  copy_tables:\n{copy_tables}")
    return project


def run_main(tmp_path: Path, *extra: str) -> int:
    """Run the command line on the files under ``tmp_path`` with the packages and the parse out of the way."""
    return scope.main(
        ["--project", str(tmp_path / "project"), "--table-map", str(tmp_path / "map.json"), "--no-packages", "--out", str(tmp_path / "out.json"), *extra]
    )


def fake_parse(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the parse with one that returns a one-root manifest at once."""
    monkeypatch.setattr(dbt_manifest_labels, "parse_manifest", lambda *args, **kwargs: manifest({"stg_a": ["t_a"], OPERATIONAL: ["t_a"]}, ["t_a"]))


def good_files(tmp_path: Path) -> None:
    """A valid project folder and table map for the one-root manifest."""
    project_folder(tmp_path)
    (tmp_path / "map.json").write_text(json.dumps(table_map(entry("t_a", "x/a"))))


def test_a_clean_run_exits_0_and_writes_the_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """Exit 0, the report file exists and holds the one root; the output line carries the parse seconds."""
    fake_parse(monkeypatch)
    good_files(tmp_path)
    check(run_main(tmp_path) == 0, "a clean run must exit 0")
    check([row["table"] for row in json.loads((tmp_path / "out.json").read_text())["in_scope"]] == ["t_a"], "report content")
    check("dbt parse" in capsys.readouterr().out, "parse seconds on the output line")


def test_a_scope_problem_exits_1_names_the_table_and_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """copy_tables names a table that no model reads: exit 1 (never 2), the name is printed, no report."""
    fake_parse(monkeypatch)
    project_folder(tmp_path, "    t_a: fam\n    t_b: fam\n")
    (tmp_path / "map.json").write_text(json.dumps(table_map(entry("t_a", "x/a"), entry("t_b", "x/a"))))
    check(run_main(tmp_path) == 1, "a scope problem must exit 1")
    check("t_b" in capsys.readouterr().out, "the table is not named")
    check(not (tmp_path / "out.json").exists(), "a failing run wrote a report")


def test_malformed_table_map_json_exits_2_naming_the_file_and_keeps_an_old_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A map file of ``{"tables": [}``: exit 2, the file named, the report from an earlier run untouched."""
    fake_parse(monkeypatch)
    good_files(tmp_path)
    (tmp_path / "map.json").write_text('{"tables": [}')
    (tmp_path / "out.json").write_text("older report\n")
    check(run_main(tmp_path) == 2, "malformed map JSON must exit 2")
    check("map.json" in capsys.readouterr().err, "the file is not named")
    check((tmp_path / "out.json").read_text() == "older report\n", "the older report changed")


def test_malformed_project_yaml_exits_2_naming_the_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """A dbt_project.yml holding ``vars: [``: exit 2 with the file named."""
    fake_parse(monkeypatch)
    good_files(tmp_path)
    (tmp_path / "project" / "dbt_project.yml").write_text("vars: [\n")
    check(run_main(tmp_path) == 2, "malformed project YAML must exit 2")
    check("dbt_project.yml" in capsys.readouterr().err, "the file is not named")


@pytest.mark.parametrize(
    ("name", "text"),
    [
        ("missing table key", json.dumps({"tables": [{"group": "g"}]})),
        ("repeated table", json.dumps(table_map(entry("t_a", "x/a"), entry("t_a", "x/b")))),
        ("tables not a list", json.dumps({"tables": 5})),
        ("document not an object", "[1]"),
        ("unreadable bytes", None),
    ],
)
def test_an_invalid_table_map_exits_2_naming_the_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], name: str, text: str | None
) -> None:
    """Every way the map can be invalid, valid JSON or not, is exit 2 with the file named and no traceback."""
    fake_parse(monkeypatch)
    good_files(tmp_path)
    if text is None:
        (tmp_path / "map.json").write_bytes(b"\xff\xfe\x00bad")
    else:
        (tmp_path / "map.json").write_text(text)
    check(run_main(tmp_path) == 2, f"{name}: exit code")
    check("map.json" in capsys.readouterr().err, f"{name}: the file is not named")


def test_a_missing_input_exits_2(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing table map is exit 2."""
    fake_parse(monkeypatch)
    project_folder(tmp_path)
    check(run_main(tmp_path) == 2, "a missing map must exit 2")


def test_a_parser_timeout_exits_2_naming_the_deadline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """A parse that raises the subprocess timeout (as the real runner does at its deadline) is exit 2 with the project and the deadline named."""
    good_files(tmp_path)

    def expire(*args: Any, **kwargs: Any) -> None:
        raise scope.TimeoutExpired("dbt", scope.PARSE_SECONDS)

    monkeypatch.setattr(dbt_manifest_labels, "parse_manifest", expire)
    check(run_main(tmp_path) == 2, "a parse timeout must exit 2")
    err = capsys.readouterr().err
    check(str(scope.PARSE_SECONDS) in err and "dbt parse" in err, f"{err}")
    check(not (tmp_path / "out.json").exists(), "a timed-out run wrote a report")


@pytest.mark.parametrize(
    "failure",
    [ProcessGroupSurvived("dbt", 1.0, 4242), LaunchesNotStopped("a stop was not confirmed", {4242}), SessionListingFailed("the session could not be listed")],
    ids=["group_survived", "launches_not_stopped", "session_listing_failed"],
)
def test_a_process_cleanup_failure_is_not_turned_into_exit_2(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: BaseException) -> None:
    """A parse whose process group survived KILL, or whose stop or listing failed, propagates as itself, so the process module's own exit handling applies."""
    good_files(tmp_path)

    def fail(*args: Any, **kwargs: Any) -> None:
        raise failure

    monkeypatch.setattr(dbt_manifest_labels, "parse_manifest", fail)
    with pytest.raises(type(failure)):
        run_main(tmp_path)


def test_an_invalid_manifest_file_exits_2(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """A parse whose manifest.json is not JSON is a failed parse: exit 2 and the manifest named."""
    good_files(tmp_path)

    def bad_manifest(*args: Any, **kwargs: Any) -> None:
        raise json.JSONDecodeError("Expecting value", "x", 0)

    monkeypatch.setattr(dbt_manifest_labels, "parse_manifest", bad_manifest)
    check(run_main(tmp_path) == 2, "an invalid manifest must exit 2")
    check("manifest" in capsys.readouterr().err, "the manifest is not named")


def test_an_unwritable_report_path_exits_2_and_leaves_no_partial_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The report cannot be written (its folder is missing): exit 2 naming the path; nothing is left behind."""
    fake_parse(monkeypatch)
    good_files(tmp_path)
    out = tmp_path / "no_such_folder" / "out.json"
    code = scope.main(["--project", str(tmp_path / "project"), "--table-map", str(tmp_path / "map.json"), "--no-packages", "--out", str(out)])
    check(code == 2, f"exit {code}")
    check("no_such_folder" in capsys.readouterr().err, "the path is not named")
    check(sorted(path.name for path in tmp_path.iterdir()) == ["map.json", "project"], "files were left behind")


def test_the_report_is_written_whole_or_not_at_all(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A write that fails midway leaves the earlier report as it was: the new bytes go to a temporary file first and replace it atomically."""
    fake_parse(monkeypatch)
    good_files(tmp_path)
    out = tmp_path / "out.json"
    out.write_text("older report\n")

    def broken_replace(source: Any, destination: Any) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(scope.os, "replace", broken_replace)
    check(run_main(tmp_path) == 2, "a failed replace must exit 2")
    check(out.read_text() == "older report\n", "the older report changed")
    check(sorted(path.name for path in tmp_path.iterdir()) == ["map.json", "out.json", "project"], "a temporary file was left behind")
