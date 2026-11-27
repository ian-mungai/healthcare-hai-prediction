"""The registry route predicates over synthetic units (design v4 section 5.1, case 5): I4, I5 and I10.

I4: an output selects at most one table. I5: a declared feed selects exactly its table. I10: a document output may select none.
The synthetic trees use the real ``bronze.select``. One case reads the committed files.
"""

from __future__ import annotations

from typing import Any

from scripts.ingestion import registry
from tests.registry_support import scope_report
from tests.support import check

COLLECTION = "pub/coll"


def table(name: str, pattern: str, **extra: Any) -> dict[str, Any]:
    """A table map entry of the synthetic collection that selects by file name."""
    return {"table": name, "group": "g", "collection": COLLECTION, "format": "csv", "member_pattern": pattern, "roles": ["data"], **extra}


def tree(tables: list[dict[str, Any]], outputs: list[tuple[str, str]], feeds: list[dict[str, str]] | None = None, archive: bool = False) -> registry.Tree:
    """A tree with one unit per ``(role, name)`` output, the tables and the declared feeds. Only the parts the route predicates read are filled in."""
    units: list[dict[str, Any]] = []
    specs: dict[str, dict[str, Any]] = {}
    for number, (role, name) in enumerate(outputs):
        snapshot = f"SYN__{number}"
        units.append({"snapshot_id": snapshot, "source_id": "SYN", "mode": "file", "route": "publisher_url"})
        specs[snapshot] = {"stored_data_files": [name], "receipt_template": {"role": role}}
        if archive:
            specs[snapshot]["archive_members"] = [{"dataset_id": "syn", "member_chain": [name], "role": role}]
    collections: dict[str, Any] = {"feeds": feeds or [], "gaps": {}}
    return registry.Tree(
        root=registry.ROOT,
        units=units,
        specs=specs,
        routes={"SYN": COLLECTION},
        tables=tables,
        collections=collections,
        golden={},
        manual={},
        contracts={},
        lock={},
        source_registry={},
    )


def run(built: registry.Tree, pinned: frozenset[str] = frozenset()) -> tuple[list[registry.Problem], dict[str, Any]]:
    """The route problems and facts of a synthetic tree."""
    outputs = registry.unit_outputs(built)
    return registry.route_problems(built, outputs, registry.selections(built, outputs), pinned)


def named(problems: list[registry.Problem], predicate: str) -> list[str]:
    return sorted(problem.subject for problem in problems if problem.predicate == predicate)


def test_an_output_two_selectors_match_fails_i4() -> None:
    """Case 5a: a synthetic output matched by two tables fails I4 and names the output and both tables."""
    built = tree([table("t_one", r"^a\.csv$"), table("t_two", r"\.csv$")], [("data", "a.csv")])
    problems, _ = run(built)
    check(named(problems, "I4") == ["SYN__0"], f"I4 did not name the output: {problems}")
    check("t_one" in problems[0].message and "t_two" in problems[0].message and "a.csv" in problems[0].message, f"I4 did not name both tables: {problems}")


def test_an_output_one_selector_matches_passes() -> None:
    built = tree([table("t_one", r"^a\.csv$"), table("t_two", r"^b\.csv$")], [("data", "a.csv"), ("data", "b.csv")])
    problems, _ = run(built)
    check(not problems, f"A clean tree failed: {problems}")


def test_a_document_output_that_selects_nothing_passes_and_is_counted() -> None:
    """Case 5b: a document output (role methodology) with no declared feed may select zero tables, and the check counts it."""
    built = tree([table("t_one", r"^a\.csv$")], [("methodology", "doc.pdf"), ("data", "a.csv")])
    problems, facts = run(built)
    check(not problems, f"A document output with no selection failed: {problems}")
    check(facts["document_outputs_unselected"] == 1, f"The document output was not counted: {facts}")


def test_the_same_output_declared_as_a_feed_fails_i5() -> None:
    """Case 5c: the same document output declared as feeding a table selects zero, so I5 names the table."""
    feed = {"table": "t_one", "source_id": "SYN", "name_regex": r"^doc\.pdf$"}
    built = tree([table("t_one", r"^a\.csv$")], [("methodology", "doc.pdf"), ("data", "a.csv")], [feed])
    problems, _ = run(built)
    check(named(problems, "I5") == ["t_one"], f"I5 did not name the table: {problems}")
    problems, _ = run(built, pinned=frozenset({"t_one"}))
    check(not problems, f"A pinned gap still failed I5: {problems}")


def test_a_feed_that_selects_another_table_fails_i5() -> None:
    feed = {"table": "t_one", "source_id": "SYN", "name_regex": r"^b\.csv$"}
    built = tree([table("t_one", r"^a\.csv$"), table("t_two", r"^b\.csv$")], [("data", "b.csv")], [feed])
    problems, _ = run(built)
    check(named(problems, "I5") == ["t_one"] and "t_two" in problems[0].message, f"A feed that selects another table passed: {problems}")


def test_a_feed_that_matches_no_output_fails_i5() -> None:
    feed = {"table": "t_one", "source_id": "SYN", "name_regex": r"^missing\.csv$"}
    built = tree([table("t_one", r"^a\.csv$")], [("data", "a.csv")], [feed])
    problems, _ = run(built)
    check(named(problems, "I5") == ["t_one"] and "no output" in problems[0].message, f"A feed with no output passed: {problems}")


def test_a_good_feed_passes() -> None:
    feed = {"table": "t_one", "source_id": "SYN", "name_regex": r"^a\.csv$"}
    problems, _ = run(tree([table("t_one", r"^a\.csv$")], [("data", "a.csv")], [feed]))
    check(not problems, f"A good feed failed: {problems}")


def test_outputs_the_check_cannot_decide_offline_are_marked_not_passed() -> None:
    """Archive members and the outputs of a collection with a dataset-ID table are counted as marked, never as decided."""
    plain = tree([table("t_one", r"^a\.csv$")], [("data", "a.csv")])
    _, facts = run(plain)
    check(facts["marked_outputs"] == {"archive_members": 0, "dataset_id_collections": 0}, f"A plain output was marked: {facts}")
    members = tree([table("t_one", r"^a\.csv$")], [("data", "a.csv")], archive=True)
    _, facts = run(members)
    check(facts["marked_outputs"]["archive_members"] == 1, f"An archive member was not marked: {facts}")
    by_id = tree([table("t_one", r"^a\.csv$", dataset_ids=["syn"])], [("data", "a.csv")])
    _, facts = run(by_id)
    check(facts["marked_outputs"]["dataset_id_collections"] == 1, f"An output of a dataset-ID collection was not marked: {facts}")


def test_the_committed_files_pass_the_route_predicates_and_report_their_marked_outputs() -> None:
    result = registry.check(registry.ROOT, scope_report=scope_report(), skip=frozenset({"I6"}))
    check(
        not [problem for problem in result.problems if problem.predicate in {"I4", "I5", "I10"}],
        f"The committed files fail the route predicates: {result.problems}",
    )
    check(
        result.facts["marked_outputs"]["archive_members"] > 0 and result.facts["marked_outputs"]["dataset_id_collections"] > 0,
        f"No marked outputs: {result.facts}",
    )
    check(result.facts["document_outputs_unselected"] > 0, f"No document output was counted: {result.facts}")
