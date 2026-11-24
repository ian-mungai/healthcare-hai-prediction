"""The bronze load scope changed; the registry placements and the source verification log still derive from the whole approved bronze map."""

from __future__ import annotations

from scripts.acquisition import registry_additions, source_metadata, source_verification_log
from scripts.acquisition.source_registry import BRONZE_LEAVING_PATH, BRONZE_TABLES_PATH, historical_bronze_tables, load_registry, read_json
from tests.support import check

LEFT_SCOPE_SOURCES = ("CCW-ALGORITHMS", "HRSA", "NSI", "PSI13", "PSI90")


def test_historical_tables_are_the_loaded_tables_and_the_tables_that_left_the_scope() -> None:
    loaded = [table["table"] for table in read_json(BRONZE_TABLES_PATH)["tables"]]
    departed = [table["table"] for table in read_json(BRONZE_LEAVING_PATH)["tables"]]
    names = [table["table"] for table in historical_bronze_tables()]
    check(names == [*loaded, *departed], "the historical map is the loaded tables, then the tables that left the scope")
    check(len(names) == len(set(names)) and not set(loaded) & set(departed), "no table is both loaded and left the scope")


def test_each_departed_table_keeps_the_selectors_that_registry_evidence_reads() -> None:
    for table in read_json(BRONZE_LEAVING_PATH)["tables"]:
        check({"table", "collection", "group"} <= set(table), f"{table['table']}: the table, its collection and its group stay recorded")
        check("dataset_ids" in table or "member_pattern" in table, f"{table['table']}: a dataset or member selector stays recorded")


def test_registry_placements_re_derive_from_the_historical_map() -> None:
    placed, unplaced = source_metadata.validate_placements(read_json(registry_additions.ADDITIONS_PATH), load_registry())
    check(placed > 0 and unplaced > 0, "the stored placements and the unplaced evidence re-derive from the committed inputs")


def test_registry_additions_load_with_the_load_scope_switched() -> None:
    registry = load_registry()
    check(len(registry_additions.load_registry_additions(registry)) == 5, "the five added controls validate, so the placement evidence re-derives")


def test_sources_whose_tables_left_the_scope_keep_their_log_rows() -> None:
    inputs = source_verification_log.load_inputs()
    logged = set(source_verification_log.logged_source_ids(inputs))
    check(set(LEFT_SCOPE_SOURCES) <= logged, "every source named in the log keeps its row")
    departed = {table["collection"] for table in read_json(BRONZE_LEAVING_PATH)["tables"]}
    check(departed <= set(source_verification_log.bronze_groups(inputs["bronze"])), "the collections that left the scope still count as bronze collections")
    committed = read_json(source_verification_log.LOG_PATH)
    check(source_verification_log.check_log(committed, inputs) == [], "the committed log passes its check with the load scope switched")
