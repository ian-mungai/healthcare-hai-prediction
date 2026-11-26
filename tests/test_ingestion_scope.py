"""The scope report derives the default bronze scope from a fresh dbt parse.

Synthetic dbt projects cover the logic one case at a time. The real project and mutated copies of it cover the numbers and the independent removal,
addition and undefined-source cases. Nothing here reads S3 or the network, and the repository is not written.
"""

from __future__ import annotations

import copy
import json
import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from orchestration import ecs_lineage
from scripts.acquisition import source_registry
from scripts.ingestion import scope
from scripts.lakehouse import bronze, glue_mirror, pins
from scripts.process import run_command
from scripts.quality import dbt_manifest_labels
from tests.support import check
from tests.test_ingestion_scope_logic import use_a_repository_elsewhere

ROOT = Path(__file__).resolve().parents[1]
TABLE_MAP = ROOT / "config/lakehouse/bronze_tables.json"
SOURCES = ROOT / "dbt/models/silver/staging/_bronze__sources.yml"
LEAVING_RECORD = ROOT / "config/lakehouse/bronze_leaving_scope.json"
# The table names of the map before the bronze scope trim, frozen from Git history. No file the trim wrote is an input to it. It sits under config/,
# where the typos check keeps the publishers' spellings.
PRETRIM_NAMES = ROOT / "config/quality/ingestion_scope_pretrim_tables.json"
SILVER_READERS = ROOT / "config/lakehouse/bronze_silver_readers.json"
# The silver source declaration of the trimmed map in the shape of the approved scope draft, written by hand from the silver reader list, the sources file
# and the dictionaries of the table map. The code under test writes no part of it.
EXPECTED_DECLARATION = ROOT / "tests/fixtures/ingestion_scope_expected_declaration.json"
POLICIES = ROOT / "config/acquisition/projection_policies.json"
# The mapped tables that no consumer derives, by the owner decision that keeps each group loaded (the bronze scope trim). The report lists them as leaving the
# consumer scope; config/lakehouse/bronze_leaving_scope.json does not list them, because bronze still loads them.
# The nine document-only state sources stay until their bronze layout profile is decided.
DOCUMENT_STATE_TABLES = {
    "cdph_documents_pdf",
    "iroquois_workforce_pdf",
    "maryland_nurse_support_documents_pdf",
    "maryland_nurse_support_documents_sheets",
    "maryland_nurse_support_docx",
    "maryland_nurse_support_pdf",
    "maryland_nurse_support_sheet_rows",
    "massachusetts_workforce_documents_pdf",
    "massachusetts_workforce_pdf",
    "massachusetts_workforce_sheet_rows",
    "mississippi_workforce_pdf",
    "new_york_infections_pdf",
    "patientcarelink_workforce_pdf",
    "texas_nurse_staffing_pdf",
    "vermont_hospital_reports_pdf",
}
# The state workbook sheet tables stay with those state sources.
WORKBOOK_SHEET_TABLES = {"hcai_finance_sheet_rows", "hcai_util_workbook_sheet_rows"}
# The derivative policy check still reads these from the table map: three tables a policy names and the dictionary of two of them.
POLICY_CHECK_TABLES = {"acs_api_detailed", "acs_api_profile", "acs_documents_json", "wonder_exports_text_lines"}
HELD_TABLES = DOCUMENT_STATE_TABLES | WORKBOOK_SHEET_TABLES | POLICY_CHECK_TABLES
OPERATIONAL = "stg_bronze__file_copies"
READ = "union all select 1 from {{ source('bronze', 'TABLE') }}\n"
PROFILES = "synth:\n  target: lint\n  outputs:\n    lint:\n      type: duckdb\n      path: ':memory:'\n      threads: 1\n"


def map_entry(table: str, dictionary: str | None = None) -> dict[str, Any]:
    """One valid table map entry for a synthetic collection; ``dictionary`` names the table that documents it."""
    entry: dict[str, Any] = {"table": table, "group": "synthetic", "collection": "pub/coll", "format": "csv", "dataset_ids": [table], "encodings": ["utf-8"]}
    if dictionary:
        entry["dictionary"] = {"table": dictionary, "file_pattern": "dictionary"}
    return entry


def map_document(*entries: dict[str, Any]) -> dict[str, Any]:
    """A table map document around entries; dictionary tables are ``csv_rows`` entries."""
    return {"version": 1, "namespace": "bronze", "classification": "Public", "tables": list(entries)}


def dictionary_entry(table: str) -> dict[str, Any]:
    """A table map entry that can serve as a dictionary."""
    return {**map_entry(table), "format": "csv_rows"}


def synthetic_files(models: dict[str, list[str]], declared: list[str], copy_tables: dict[str, str]) -> dict[str, str]:
    """A synthetic dbt project: one model per name reading the listed bronze tables, the operational copy model and the sources file."""
    sources = "version: 2\nsources:\n  - name: bronze\n    schema: bronze\n    tables:\n"
    sources += "".join(f"      - name: {name}\n" for name in [*scope.LOADER_TABLES, *declared])
    copies = "".join(f"    {name}: {family}\n" for name, family in copy_tables.items()) or "    {}\n"
    project = f"name: synth\nversion: '1.0'\nconfig-version: 2\nprofile: synth\nmodel-paths: [models]\nvars:\n  copy_tables:\n{copies}"
    files = {"dbt_project.yml": project, "profiles.yml": PROFILES, "models/_sources.yml": sources}
    for model, tables in models.items():
        files[f"models/{model}.sql"] = "select 1 as id\n" + "".join(READ.replace("TABLE", table) for table in tables)
    return files


def write_project(files: dict[str, str], work: Path) -> Path:
    """Write the synthetic project files under ``work/project`` and return that folder."""
    project = work / "project"
    for relative, text in files.items():
        path = project / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return project


def derive(files: dict[str, str], table_map: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Parse a synthetic project with the pinned dbt and derive its scope."""
    with tempfile.TemporaryDirectory(prefix="hai_scope_test_") as scratch:
        work = Path(scratch)
        project = write_project(files, work)
        manifest = dbt_manifest_labels.parse_manifest(project, project, work)
        return scope.derive_scope(manifest, table_map, scope.read_copy_tables(project))


def base_project() -> tuple[dict[str, list[str]], list[str], dict[str, str], dict[str, Any]]:
    """Two roots (one with a dictionary), the dictionary table and one leaving table, with the operational model reading both roots."""
    models = {"stg_a": ["t_a"], "stg_b": ["t_b"], OPERATIONAL: ["t_a", "t_b"]}
    declared = ["t_a", "t_b"]
    copies = {"t_a": "alpha", "t_b": "beta"}
    table_map = map_document(map_entry("t_a", "t_dict"), map_entry("t_b"), dictionary_entry("t_dict"), map_entry("t_gone"))
    return models, declared, copies, table_map


@pytest.fixture(scope="module")
def real_scope() -> Iterator[tuple[dict[str, Any], list[str], float]]:
    """The scope of the repository's own project, parsed once for the cases that read it."""
    yield scope.scope_of_project(ROOT / "dbt", TABLE_MAP, ROOT)


def mutated_real(change: Any) -> tuple[dict[str, Any], list[str]]:
    """The scope of a scratch copy of the real dbt project after ``change(project_dir)`` edited it."""
    with tempfile.TemporaryDirectory(prefix="hai_scope_real_") as scratch:
        project = Path(scratch) / "dbt"
        shutil.copytree(ROOT / "dbt", project)
        change(project)
        report, problems, _ = scope.scope_of_project(project, TABLE_MAP, ROOT)
        return report, problems


# 1. Removal case.
def test_a_table_no_model_reads_drops_out_of_the_roots_even_when_copy_tables_names_it() -> None:
    """The reader goes away, so the table is no root. The stale copy_tables entry is a named problem, not a root."""
    models, declared, copies, table_map = base_project()
    models["stg_b"] = []
    report, problems = derive(synthetic_files(models, declared, copies), table_map)
    check([entry["table"] for entry in report["in_scope"]] == ["t_a"], f"t_b must leave the roots: {report['in_scope']}")
    check(any("t_b" in line and "copy_tables" in line for line in problems), f"the stale copy_tables entry t_b is not named: {problems}")


def test_removal_in_the_real_project_names_the_stale_copy_table() -> None:
    """The real project with one staging model repointed: acs_b16005 loses its only analytical reader."""

    def repoint(project: Path) -> None:
        (project / "models/silver/staging/stg_acs_b16005.sql").write_text("{{ canonical_rows('acs_b19013') }}\n")

    report, problems = mutated_real(repoint)
    check("acs_b16005" not in {entry["table"] for entry in report["in_scope"]}, "acs_b16005 is still a root after its reader moved")
    check(any("acs_b16005" in line and "copy_tables" in line for line in problems), f"no problem names acs_b16005: {problems}")


# 2. Addition case.
def test_a_table_a_model_starts_reading_becomes_a_root() -> None:
    """A new reader of a mapped table makes it a root; with copy_tables updated the report is clean."""
    models, declared, copies, table_map = base_project()
    models["stg_gone"] = ["t_gone"]
    declared.append("t_gone")
    models[OPERATIONAL] = ["t_a", "t_b", "t_gone"]
    report, problems = derive(synthetic_files(models, declared, {**copies, "t_gone": "gamma"}), table_map)
    check([entry["table"] for entry in report["in_scope"]] == ["t_a", "t_b", "t_gone"], f"t_gone must become a root: {report['in_scope']}")
    check(problems == [], f"unexpected problems: {problems}")
    check(report["leaving"] == [], f"nothing should leave: {report['leaving']}")


def test_addition_in_the_real_project_names_the_missing_copy_table() -> None:
    """A model that reads a held table of the real map makes it a root, and the missing copy_tables entry is named."""

    def add_reader(project: Path) -> None:
        sources = project / "models/silver/staging/_bronze__sources.yml"
        sources.write_text(sources.read_text() + "      - name: cdph_documents_pdf\n")
        (project / "models/silver/staging/stg_probe_reader.sql").write_text("{{ canonical_rows('cdph_documents_pdf') }}\n")

    report, problems = mutated_real(add_reader)
    check("cdph_documents_pdf" in {entry["table"] for entry in report["in_scope"]}, "cdph_documents_pdf is not a root after a model reads it")
    check(any("cdph_documents_pdf" in line and "copy_tables" in line for line in problems), f"no problem names cdph_documents_pdf: {problems}")


# 3. Circle case.
def test_a_copy_tables_entry_with_no_analytical_reader_fails_by_name() -> None:
    """copy_tables names t_gone, whose only reader is the operational copy model: the loop is a named problem."""
    models, declared, copies, table_map = base_project()
    models[OPERATIONAL] = ["t_a", "t_b", "t_gone"]
    declared.append("t_gone")
    report, problems = derive(synthetic_files(models, declared, {**copies, "t_gone": "gamma"}), table_map)
    check(any("t_gone" in line and "copy_tables" in line for line in problems), f"the circle on t_gone is not named: {problems}")
    check("t_gone" not in {entry["table"] for entry in report["in_scope"]}, "a table only the operational model reads became a root")


def deferred_project(deferred: list[str], copy_tables: dict[str, str], table_map: dict[str, Any], accounted: bool = True) -> tuple[dict[str, Any], list[str]]:
    """The base project plus the declared source t_gone, which only the operational model reads (or, with ``accounted`` false, no model), with the given
    deferral record and copy_tables entries."""
    models, declared, copies, _ = base_project()
    models[OPERATIONAL] = ["t_a", "t_b", *(["t_gone"] if accounted else [])]
    files = synthetic_files(models, [*declared, "t_gone"], {**copies, **copy_tables})
    files["dbt_project.yml"] += "  deferred_models:\n    - reason: held\n      models: [" + ", ".join(deferred) + "]\n"
    with tempfile.TemporaryDirectory(prefix="hai_scope_test_") as scratch:
        work = Path(scratch)
        project = write_project(files, work)
        manifest = dbt_manifest_labels.parse_manifest(project, project, work)
        return scope.derive_scope(manifest, table_map, scope.read_copy_tables(project), scope.read_deferred_models(project))


def test_a_copied_table_whose_staging_model_is_deferred_stays_a_root() -> None:
    """The deferral record names stg_t_gone, so the copy_tables entry stands without a staging reader: a root, marked as deferred, and no problem."""
    report, problems = deferred_project(["stg_t_gone"], {"t_gone": "gamma"}, base_project()[3])
    rows = {entry["table"]: entry for entry in report["in_scope"]}
    check(problems == [], f"unexpected problems: {problems}")
    check(sorted(rows) == ["t_a", "t_b", "t_gone"], f"{sorted(rows)}")
    check(rows["t_gone"]["analytical_readers"] == [] and rows["t_gone"]["deferred_staging_model"] == "stg_t_gone", f"{rows['t_gone']}")
    check(rows["t_a"]["deferred_staging_model"] is None, "a table with a reader is marked deferred")
    check(report["counts"]["silver_pins"] == 4, f"the deferred table is declared to silver and pinned: {report['counts']['silver_pins']}")


def test_a_deferred_copy_table_that_the_copy_accounting_model_does_not_read_is_no_root() -> None:
    """t_gone stays mapped, declared to dbt and named by copy_tables with its staging model deferred, but no model reads it: the declaration alone is no
    reader, so the table is no root and the copy_tables entry is a named problem."""
    report, problems = deferred_project(["stg_t_gone"], {"t_gone": "gamma"}, base_project()[3], accounted=False)
    check("t_gone" not in {entry["table"] for entry in report["in_scope"]}, "a table that no model reads stayed a root")
    check("t_gone" in {entry["table"] for entry in report["leaving"]}, "t_gone did not leave")
    check(any("t_gone" in line and "copy_tables" in line for line in problems), f"the copy_tables entry t_gone is not named: {problems}")


@pytest.mark.parametrize("deferred", [["int_t_gone"], ["stg_other"], ["t_gone"]])
def test_a_deferral_of_any_other_model_keeps_no_table(deferred: list[str]) -> None:
    """Only the table's own staging model keeps it: another deferred name leaves the stale copy_tables entry a named problem."""
    report, problems = deferred_project(deferred, {"t_gone": "gamma"}, base_project()[3])
    check("t_gone" not in {entry["table"] for entry in report["in_scope"]}, "t_gone became a root")
    check(any("t_gone" in line and "copy_tables" in line for line in problems), f"{problems}")


def test_a_deferred_staging_model_without_a_copy_tables_entry_keeps_no_table() -> None:
    """The deferral record alone adds nothing: copy_tables is what tracks the stored copies."""
    report, problems = deferred_project(["stg_t_gone"], {}, base_project()[3])
    check(problems == [], f"unexpected problems: {problems}")
    check("t_gone" in {entry["table"] for entry in report["leaving"]}, "t_gone did not leave")


def test_a_deferred_copy_table_outside_the_map_is_named() -> None:
    """A deferral cannot keep a table the map lacks: it is named and is no root."""
    table_map = base_project()[3]
    table_map["tables"] = [entry for entry in table_map["tables"] if entry["table"] != "t_gone"]
    report, problems = deferred_project(["stg_t_gone"], {"t_gone": "gamma"}, table_map)
    check("t_gone" not in {entry["table"] for entry in report["in_scope"]}, "t_gone became a root")
    check(any("t_gone" in line and "not in the table map" in line for line in problems), f"{problems}")


# 4. Fake undefined table.
def test_a_model_reading_a_source_outside_the_map_fails_by_name() -> None:
    """The source is declared to dbt but is no mapped table and no loader table: the report names the model and the table."""
    models, declared, copies, table_map = base_project()
    models["stg_ghost"] = ["t_ghost"]
    _, problems = derive(synthetic_files(models, [*declared, "t_ghost"], copies), table_map)
    check(any("stg_ghost" in line and "t_ghost" in line for line in problems), f"the undefined source is not named: {problems}")


def test_undefined_source_in_the_real_project_fails_by_name() -> None:
    """A model of the real project reads a bronze source that the real map lacks."""

    def add_ghost(project: Path) -> None:
        sources = project / "models/silver/staging/_bronze__sources.yml"
        sources.write_text(sources.read_text() + "      - name: zz_not_mapped\n")
        (project / "models/silver/staging/stg_probe_ghost.sql").write_text("select * from {{ source('bronze', 'zz_not_mapped') }}\n")

    _, problems = mutated_real(add_ghost)
    check(any("stg_probe_ghost" in line and "zz_not_mapped" in line for line in problems), f"the undefined source is not named: {problems}")


# 5. Operational exclusion.
def test_the_operational_model_alone_does_not_make_a_root() -> None:
    """t_only is read by stg_bronze__file_copies and by nothing else: it is no root and the operational model is not an analytical reader."""
    models, declared, copies, table_map = base_project()
    models[OPERATIONAL] = ["t_a", "t_b", "t_gone"]
    report, _ = derive(synthetic_files(models, [*declared, "t_gone"], copies), table_map)
    names = {entry["table"] for entry in report["in_scope"]}
    check(names == {"t_a", "t_b"}, f"only the analytically read tables are roots: {names}")
    check(all(entry["operational_readers"] == [OPERATIONAL] for entry in report["in_scope"]), "each root must list the operational reader apart")
    check(all(OPERATIONAL not in entry["analytical_readers"] for entry in report["in_scope"]), "the operational model counted as an analytical reader")


# 6. Real project.
def test_real_project_has_74_roots_11_dictionaries_and_21_outside_the_consumer_scope(real_scope: tuple[dict[str, Any], list[str], float]) -> None:
    """The counts of the trimmed map: 74 data roots, 11 dictionaries, 85 in default scope, 21 outside it of 106 mapped; no problem."""
    report, problems, _ = real_scope
    check(problems == [], f"the real project has problems: {problems}")
    counts = report["counts"]
    got = (counts["mapped"], counts["in_scope_data"], counts["dictionary"], counts["default_scope"], counts["leaving"])
    check(got == (106, 74, 11, 85, 21), f"counts: {got}")
    check((len(report["in_scope"]), len(report["dictionary"]), len(report["leaving"])) == (74, 11, 21), "the lists disagree with the counts")


def trim_names(table_map_path: Path, leaving_path: Path, pretrim_path: Path) -> tuple[set[str], set[str], set[str]]:
    """The loaded names, the departure names and the frozen names of the map before the trim. A name listed twice fails before a set can hide it: the
    registry helper refuses it in the loaded map and the departure record, and the frozen list is checked here."""
    source_registry.historical_bronze_tables(table_map_path, leaving_path)
    frozen = json.loads(pretrim_path.read_text())["tables"]
    repeated = sorted({name for name in frozen if frozen.count(name) > 1})
    check(not repeated, f"the frozen list names a table twice: {repeated}")
    mapped = {entry["table"] for entry in bronze.load_table_map(table_map_path)["tables"]}
    recorded = {entry["table"] for entry in json.loads(leaving_path.read_text())["tables"]}
    return mapped, recorded, set(frozen)


@pytest.mark.parametrize(
    ("repeated_in", "expected"), [("departures", source_registry.RegistryError), ("frozen", AssertionError)], ids=["departure_row", "frozen_name"]
)
def test_a_trim_name_listed_twice_fails_before_the_names_are_compared(repeated_in: str, expected: type[Exception], tmp_path: Path) -> None:
    """A second cms_pos_clia row in the departure record, or in the frozen list, leaves both sets as they were, so it must fail on the list itself."""
    leaving = json.loads(LEAVING_RECORD.read_text())
    frozen = json.loads(PRETRIM_NAMES.read_text())
    if repeated_in == "departures":
        leaving["tables"].append(next(row for row in leaving["tables"] if row["table"] == "cms_pos_clia"))
    else:
        frozen["tables"].append("cms_pos_clia")
    (tmp_path / "leaving.json").write_text(json.dumps(leaving))
    (tmp_path / "frozen.json").write_text(json.dumps(frozen))
    with pytest.raises(expected, match="twice"):
        trim_names(TABLE_MAP, tmp_path / "leaving.json", tmp_path / "frozen.json")


def test_the_report_agrees_with_the_bronze_scope_trim(real_scope: tuple[dict[str, Any], list[str], float]) -> None:
    """The derived scope and the named held tables are the loaded map. The loaded names and the departure names together are the frozen names of the map
    before the trim, name for name, and what is left of those names is the leaving record."""
    report, _, _ = real_scope
    roots = {entry["table"] for entry in report["in_scope"]}
    derived = roots | {entry["table"] for entry in report["dictionary"]}
    outside = {entry["table"] for entry in report["leaving"]}
    mapped, recorded, approved = trim_names(TABLE_MAP, LEAVING_RECORD, PRETRIM_NAMES)
    silver_reads = set(json.loads(SILVER_READERS.read_text())["tables"])
    check(roots | set(scope.LOADER_TABLES) == silver_reads, f"roots differ from what silver reads: {(roots | set(scope.LOADER_TABLES)) ^ silver_reads}")
    check(outside == HELD_TABLES, f"mapped tables outside the consumer scope: unexpected {outside - HELD_TABLES}, missing {HELD_TABLES - outside}")
    check(derived == mapped - HELD_TABLES, f"the derived scope differs from the loaded map less the held tables: {derived ^ (mapped - HELD_TABLES)}")
    check(not mapped & recorded, f"a table is both loaded and recorded as left: {mapped & recorded}")
    check(mapped | recorded == approved, f"loaded plus departures is not the map before the trim: {(mapped | recorded) ^ approved}")
    left = approved - derived - HELD_TABLES
    check(left == recorded, f"the leaving set differs from the record: only derived {left - recorded}, only recorded {recorded - left}")
    check((len(approved), len(recorded), len(approved - derived)) == (331, 225, 246), f"{len(approved)} {len(recorded)} {len(approved - derived)}")


def test_the_policy_check_tables_follow_from_the_derivative_policies(real_scope: tuple[dict[str, Any], list[str], float]) -> None:
    """The named policy tables are exactly the policy-named tables no consumer derives, with their dictionaries."""
    report, _, _ = real_scope
    derived = {entry["table"] for group in ("in_scope", "dictionary") for entry in report[group]}
    mapped = {entry["table"]: entry for entry in bronze.load_table_map(TABLE_MAP)["tables"]}
    named = set()
    for policy in json.loads(POLICIES.read_text())["entries"]:
        named |= {policy["consumers"]["bronze_table"], *policy["consumers"]["dropped_bronze_tables"]}
    held = named - derived
    held |= {mapped[table]["dictionary"]["table"] for table in held if "dictionary" in mapped[table]} - derived
    check(held == POLICY_CHECK_TABLES, f"{sorted(held)}")


def test_only_the_deferred_finance_table_is_a_root_without_an_analytical_reader(real_scope: tuple[dict[str, Any], list[str], float]) -> None:
    """hcai_finance_csv stays in scope because its staging model is deferred; every other root has a model that reads it."""
    report, _, _ = real_scope
    unread = {entry["table"]: entry["deferred_staging_model"] for entry in report["in_scope"] if not entry["analytical_readers"]}
    check(unread == {"hcai_finance_csv": "stg_hcai_finance_csv"}, f"{unread}")
    check(sum(entry["deferred_staging_model"] is not None for entry in report["in_scope"]) == 1, "another root carries a deferral")
    check(report["counts"]["collections_leaving"] == 9, f"collections leaving: {report['counts']['collections_leaving']}")
    got = (report["silver_source_declaration"]["analytical_roots"], report.get("data_roots_with_an_analytical_reader"))
    check(got == (74, 73), f"the declared roots and the data roots with an analytical reader: {got}")


def test_a_dictionary_is_the_table_named_by_an_in_scope_table(real_scope: tuple[dict[str, Any], list[str], float]) -> None:
    """Each dictionary of the report is named by at least one root, and no root's dictionary is missing."""
    report, _, _ = real_scope
    table_map = {entry["table"]: entry for entry in bronze.load_table_map(TABLE_MAP)["tables"]}
    wanted = {table_map[entry["table"]]["dictionary"]["table"] for entry in report["in_scope"] if "dictionary" in table_map[entry["table"]]}
    check(wanted == {entry["table"] for entry in report["dictionary"]}, f"dictionary tables differ: {wanted ^ {e['table'] for e in report['dictionary']}}")


# 7. Derived sets.
def test_pins_lineage_and_mirror_names_derive_from_the_scope(real_scope: tuple[dict[str, Any], list[str], float], tmp_path: Path) -> None:
    """Pins come from the real pins function, the silver lineage from the real lineage function and the mirror names from the real mirror function."""
    report, _, _ = real_scope
    roots = {entry["table"] for entry in report["in_scope"]}
    scope_tables = roots | {entry["table"] for entry in report["dictionary"]}
    declared = roots | set(scope.LOADER_TABLES)
    pinned = pins.pinned_tables(scope_tables, declared)
    check(len(pinned) == 75 == report["counts"]["silver_pins"], f"pins: {len(pinned)} against {report['counts']['silver_pins']}")
    reduced = copy.deepcopy(bronze.load_table_map(TABLE_MAP))
    reduced["tables"] = [entry for entry in reduced["tables"] if entry["table"] in scope_tables]
    root = tmp_path / "root"
    for relative in (ecs_lineage.HOST_FILE, ecs_lineage.JOBS_FILE, SOURCES.relative_to(ROOT), *ecs_lineage.CONTRACT_FILES.values()):
        (root / relative).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, root / relative)
    (root / ecs_lineage.BRONZE_TABLES_FILE).parent.mkdir(parents=True, exist_ok=True)
    (root / ecs_lineage.BRONZE_TABLES_FILE).write_text(json.dumps(reduced))
    lineage = ecs_lineage.declared_lineage("silver", root=root)["inputs"]
    check(sorted(item["name"].rsplit(".", 1)[-1] for item in lineage) == pinned, "the silver lineage inputs differ from the pins")
    reduced_path = tmp_path / "reduced_map.json"
    reduced_path.write_text(json.dumps(reduced))
    patched = pytest.MonkeyPatch()
    patched.setattr(glue_mirror, "BRONZE_TABLES", reduced_path)
    try:
        names = glue_mirror.layer_tables("bronze")
    finally:
        patched.undo()
    check(
        len(names) == 89 == report["counts"]["glue_mirror_bronze_names"], f"mirror names: {len(names)} against {report['counts']['glue_mirror_bronze_names']}"
    )
    check(report["counts"]["silver_declared_sources"] == 78 == report["silver_source_declaration"]["pins"]["current_yml_declared_sources"], "declared sources")
    check(report["counts"]["silver_pins_if_dictionaries_declared"] == 86, "pins with the dictionaries declared")


# 8. Equality.
def expected_from(report: dict[str, Any]) -> dict[str, Any]:
    """A draft-shaped document: the report's projected table rows and counts with acquisition annotations the equality rule must ignore, and the silver
    declaration from the hand-written expected file, text included."""
    document = copy.deepcopy(report)
    del document["data_roots_with_an_analytical_reader"]
    for group in ("in_scope", "dictionary", "leaving"):
        for entry in document[group]:
            entry.update(queue_units=300, queued_output_selected=False, collection_without_queue_unit=False, notes=["annotation"])
    document["counts"].update(default_scope_without_queued_output=22, leaving_with_queue_units=236)
    document["silver_source_declaration"] = json.loads(EXPECTED_DECLARATION.read_text())
    document["gap_records"] = ["not compared"]
    return document


def test_the_declaration_of_the_trimmed_map_equals_the_approved_draft_shape(real_scope: tuple[dict[str, Any], list[str], float]) -> None:
    """A declaration with the keys of the approved draft, and no key the draft lacks, compares equal. There analytical_roots counts every data root."""
    report, _, _ = real_scope
    expected = expected_from(report)
    check(scope.equality_problems(report, expected) == [], f"{scope.equality_problems(report, expected)}")
    expected["silver_source_declaration"]["analytical_roots"] = 73
    found = scope.equality_problems(report, expected)
    check(len(found) == 1 and "analytical_roots" in found[0], f"{found}")


def test_equality_ignores_acquisition_annotations_and_names_the_first_difference(real_scope: tuple[dict[str, Any], list[str], float]) -> None:
    """The report equals a draft-shaped document in every consumer-scope key. A changed reader set, dictionary relationship or count is named."""
    report, _, _ = real_scope
    expected = expected_from(report)
    check(scope.equality_problems(report, expected) == [], "annotations and text must not change equality")
    first = expected["in_scope"][0]["table"]
    expected["in_scope"][0]["analytical_readers"] = ["somebody_else"]
    expected["dictionary"][0]["required_by"] = ["nobody"]
    expected["counts"]["silver_pins"] = 74
    expected["leaving"].reverse()
    found = scope.equality_problems(report, expected)
    check(any(first in line and "analytical_readers" in line for line in found), f"reader change not named: {found}")
    check(any(expected["dictionary"][0]["table"] in line and "required_by" in line for line in found), f"dictionary change not named: {found}")
    check(any("silver_pins" in line for line in found), f"count change not named: {found}")
    check(len(found) == 3, f"only the three real differences count, order does not: {found}")


def test_equality_names_a_missing_and_an_extra_table(real_scope: tuple[dict[str, Any], list[str], float]) -> None:
    """A table present on one side only is named, so a dropped or added root cannot pass as a count change."""
    report, _, _ = real_scope
    expected = expected_from(report)
    dropped = expected["in_scope"].pop()["table"]
    found = scope.equality_problems(report, expected)
    check(any(dropped in line for line in found), f"the table missing from the expected side is not named: {found}")


# 9. Counts derive, never freeze.
def test_counts_move_by_the_formula_when_the_reader_graph_changes() -> None:
    """One more root with a new dictionary moves mapped by 2, declared by 1, pins by 1 and mirror names by 2; an index adds one of each declared and mirror."""
    models, declared, copies, table_map = base_project()
    before, _ = derive(synthetic_files(models, declared, copies), table_map)
    models["stg_c"] = ["t_c"]
    models[OPERATIONAL] = ["t_a", "t_b", "t_c"]
    bigger = map_document(*table_map["tables"], map_entry("t_c", "t_dict_c"), dictionary_entry("t_dict_c"))
    after, problems = derive(synthetic_files(models, [*declared, "t_c"], {**copies, "t_c": "gamma"}), bigger)
    check(problems == [], f"unexpected problems: {problems}")
    keys = ("in_scope_data", "dictionary", "default_scope", "silver_declared_sources", "silver_pins", "glue_mirror_bronze_names")
    moved = {key: after["counts"][key] - before["counts"][key] for key in keys}
    check(
        moved == {"in_scope_data": 1, "dictionary": 1, "default_scope": 2, "silver_declared_sources": 1, "silver_pins": 1, "glue_mirror_bronze_names": 2},
        str(moved),
    )
    q4 = scope.projected_counts(roots=3, dictionaries=2, extra_loader_sources=1)
    plain = scope.projected_counts(roots=3, dictionaries=2)
    check((q4["declared"] - plain["declared"], q4["mirror_names"] - plain["mirror_names"], q4["pins"] - plain["pins"]) == (1, 1, 0), f"{q4} {plain}")
    delivered = after["counts_by_q4_state"]["index_delivered"]
    check(delivered["declared"] == after["counts"]["silver_declared_sources"] + 1, "the index-delivered state declares one more source")


# 10. Idempotence and the command line.
@pytest.fixture
def artifact_folder() -> Iterator[Path]:
    """A new folder under the repository's ignored data/ folder, where the command admits its files; removed afterwards."""
    with tempfile.TemporaryDirectory(prefix="hai_scope_out_", dir=ROOT / "data") as folder:
        yield Path(folder)


def test_two_runs_give_identical_bytes_and_the_timing_stays_outside(artifact_folder: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """The report file is byte identical across runs; the parse seconds are in the sidecar and the output line, never in the report."""
    tmp_path = artifact_folder
    first, second = tmp_path / "first.json", tmp_path / "second.json"
    timing = tmp_path / "timing.json"
    before = run_command("git", ["status", "--porcelain"], cwd=ROOT, check=True).stdout
    check(scope.main(["--out", str(first), "--timing-out", str(timing)]) == 0, "first run failed")
    check(scope.main(["--out", str(second)]) == 0, "second run failed")
    check(first.read_bytes() == second.read_bytes(), "two runs differ")
    check("dbt parse" in capsys.readouterr().out, "the run does not print its parse seconds")
    check(0 < json.loads(timing.read_text())["dbt_parse_seconds"] < scope.PARSE_SECONDS, "the sidecar holds no parse seconds")
    check("seconds" not in first.read_text(), "the report holds timing")
    check(run_command("git", ["status", "--porcelain"], cwd=ROOT, check=True).stdout == before, "the run changed the repository")


def test_a_failing_run_exits_1_names_the_problem_and_writes_no_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A synthetic project with an undefined source: exit 1, the table is named, and no report file exists."""
    use_a_repository_elsewhere(monkeypatch, tmp_path)
    models, declared, copies, table_map = base_project()
    models["stg_ghost"] = ["t_ghost"]
    project = write_project(synthetic_files(models, [*declared, "t_ghost"], copies), tmp_path)
    map_path = tmp_path / "map.json"
    map_path.write_text(json.dumps(table_map))
    out = tmp_path / "report.json"
    code = scope.main(["--project", str(project), "--table-map", str(map_path), "--no-packages", "--out", str(out)])
    check(code == 1, f"exit {code}")
    check("t_ghost" in capsys.readouterr().out, "the undefined table is not named")
    check(not out.exists(), "a failing run wrote a report")


def test_a_real_parse_past_its_deadline_exits_2(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """The real pinned dbt is started with a deadline that cannot be met: the runner stops it, and the command exits 2 naming the deadline."""
    use_a_repository_elsewhere(monkeypatch, tmp_path)
    models, declared, copies, table_map = base_project()
    project = write_project(synthetic_files(models, declared, copies), tmp_path)
    map_path = tmp_path / "map.json"
    map_path.write_text(json.dumps(table_map))
    monkeypatch.setattr(dbt_manifest_labels, "PARSE_SECONDS", 0.05)
    out = tmp_path / "report.json"
    code = scope.main(["--project", str(project), "--table-map", str(map_path), "--no-packages", "--out", str(out)])
    check(code == 2, f"exit {code}")
    check("dbt parse" in capsys.readouterr().err, "the failed parse is not named")
    check(not out.exists(), "a timed-out run wrote a report")


SOURCES_FILE_FAULTS = {
    "syntax": "version: 2\nsources: [\n",
    "sources_not_a_list": "version: 2\nsources: 5\n",
    "source_without_name": "version: 2\nsources:\n  - schema: bronze\n    tables:\n      - name: t_a\n",
    "tables_not_a_list": "version: 2\nsources:\n  - name: bronze\n    schema: bronze\n    tables: 5\n",
    "table_without_name": "version: 2\nsources:\n  - name: bronze\n    schema: bronze\n    tables:\n      - description: no name\n",
    "table_names_repeated": "version: 2\nsources:\n  - name: bronze\n    schema: bronze\n    tables:\n      - name: t_a\n      - name: t_a\n",
    "declares_nothing": "version: 2\nsources: []\n",
    "empty_file": "",
}


@pytest.mark.parametrize("fault", sorted(SOURCES_FILE_FAULTS))
def test_a_malformed_sources_file_exits_2_naming_it_and_writes_no_report(
    fault: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The sources file is read by dbt only. A fault in it fails the real parse, so the command exits 2 with the failed parse and the file named."""
    use_a_repository_elsewhere(monkeypatch, tmp_path)
    models, declared, copies, table_map = base_project()
    files = synthetic_files(models, declared, copies)
    files["models/_sources.yml"] = SOURCES_FILE_FAULTS[fault]
    project = write_project(files, tmp_path)
    map_path = tmp_path / "map.json"
    map_path.write_text(json.dumps(table_map))
    out = tmp_path / "report.json"
    code = scope.main(["--project", str(project), "--table-map", str(map_path), "--no-packages", "--out", str(out)])
    err = capsys.readouterr().err
    check(code == 2, f"{fault}: exit {code}: {err[:300]}")
    check("dbt parse" in err and ("_sources.yml" in err or "models/stg_bronze__file_copies.sql" in err), f"{fault}: no file is named: {err[:400]}")
    if fault in ("sources_not_a_list", "tables_not_a_list", "syntax"):
        check("_sources.yml" in err, f"{fault}: the sources file is not named: {err[:400]}")
    check(not out.exists(), "a report was written")


def test_relative_paths_are_read_from_the_callers_folder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """The real parse runs in its own scratch folder, so a relative project path must be made absolute first. The other paths may be relative too."""
    use_a_repository_elsewhere(monkeypatch, tmp_path)
    models, declared, copies, table_map = base_project()
    write_project(synthetic_files(models, declared, copies), tmp_path)
    (tmp_path / "map.json").write_text(json.dumps(table_map))
    monkeypatch.chdir(tmp_path)
    code = scope.main(["--project", "project", "--table-map", "map.json", "--no-packages", "--out", "report.json"])
    check(code == 0, f"exit {code}: {capsys.readouterr().err[:400]}")
    check([row["table"] for row in json.loads((tmp_path / "report.json").read_text())["in_scope"]] == ["t_a", "t_b"], "report content")
