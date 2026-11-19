"""E2E for the declared consumer columns of the bronze tables that silver reads (config/lakehouse/bronze_consumer_columns.json).

Run from the repository root: ``.venv/bin/python -m scripts.lakehouse.run_bronze_consumer_columns_e2e``. No Docker, catalog or AWS service is used. Four groups:

- the engine on small compiled models written as the compile writes them: quoted names, literals and comments, flag inputs, output aliases, lambda parameters,
  pass-through chains, ambiguous reads, set operations, models the parser refuses and the union of several readers of one table;
- the declaration of the real project: the committed file equals what the compiled models give, every staged and every deferred table is declared, the tables
  read through any column are named with their reasons, and the three state tables keep the lists that their projections read;
- scratch plants in a copy of the real dbt project, compiled once with the pinned dbt: a quoted read, a flag input, a literal and a comment, a second reader
  of one table, a read of a staging view that selects every column, a pass-through chain, an ambiguous read, a new staging model without a declaration and a
  new ACS column;
- the load check and the live pre-check on tables that stand for a wide table, a layout that varies by release, an empty table, a dictionary table, a table
  with no file in the run and a table of another layout: the check passes when it should and stops by table and column name when a column is missing.

The report in ``data/e2e/bronze_consumer_columns/`` holds outcomes only.
"""

from __future__ import annotations

import contextlib
import io
import json
import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import bronze_consumer_columns as columns
from scripts.lakehouse import bronze_consumer_declare as declare
from scripts.lakehouse import bronze_consumer_lineage as lineage
from scripts.lakehouse import bronze_consumer_precheck as precheck

ROOT = Path(__file__).resolve().parents[2]
MACRO = "dbt/macros/state_columns.sql"
SOURCES = "dbt/models/silver/staging/_bronze__sources.yml"
PROJECT = "dbt/dbt_project.yml"
SEED = "dbt/seeds/acs_variable_map.csv"
UTIL = "hcai_util_pages_csv"
NONRESPONSE = "hcai_util_nonresponse_csv"
ILLINOIS = "illinois_report_card_csv"
ANCHOR = "    nullif(trim(bronze.name_friendly), '') as name_friendly,\n"
FLAG_ANCHOR = "{%- endfor %}\n{%- endmacro %}"
QUOTED_PLANT = (
    '    bronze."review_quoted_column" as review_quoted_column,\n'
    "    'bronze.review_literal_only' as review_literal_only,\n"
    "    -- bronze.review_comment_only\n"
)
SOURCE_PLANT = "      - name: review_added\n        description: A table added by a plant.\n        meta:\n          classification: Public\n"
LINEAGE = {"_object_key", "_member_sha256", "_row_number"}


STAGING_VIEW = (
    'with bronze as (select * from "lakehouse"."bronze"."@table"), files as (select canonical_object_key from "memory"."main"."stg_bronze__files")\n'
    "select bronze.*, files.canonical_object_key from bronze inner join files on bronze._object_key = files.canonical_object_key"
)


def stg(table: str) -> str:
    """The compiled form of a staging view that selects every column of its bronze table (canonical_rows)."""
    return STAGING_VIEW.replace("@table", table)


def compiled_folder(root: Path, models: dict[str, str], tests: dict[str, str] | None = None, outside: dict[str, str] | None = None) -> Path:
    """A folder laid out like the compile output: silver models, gold models and tests."""
    for name, sql in models.items():
        (root / "hai/models/silver").mkdir(parents=True, exist_ok=True)
        (root / f"hai/models/silver/{name}.sql").write_text(sql)
    for name, sql in (outside or {}).items():
        (root / "hai/models/gold").mkdir(parents=True, exist_ok=True)
        (root / f"hai/models/gold/{name}.sql").write_text(sql)
    for name, sql in (tests or {}).items():
        (root / "hai/tests").mkdir(parents=True, exist_ok=True)
        (root / f"hai/tests/{name}.sql").write_text(sql)
    return root


def needs(models: dict[str, str], tests: dict[str, str] | None = None, outside: dict[str, str] | None = None) -> dict[str, Any]:
    """Table to its read columns (with the models) or its ``any`` reason, for models written as the compile writes them."""
    with tempfile.TemporaryDirectory(prefix="bronze_lineage_") as folder:
        found, _ = lineage.bronze_needs(lineage.read_models(compiled_folder(Path(folder), models, tests, outside)))
    return {table: value if isinstance(value, str) else {name: sorted(readers) for name, readers in value.items()} for table, value in found.items()}


def sql(text: str) -> str:
    """A model text where @t1 and @t2 stand for the compiled references of two staging views."""
    return text.replace("@t1", '"memory"."main"."stg_t1"').replace("@t2", '"memory"."main"."stg_t2"')


def names_of(found: dict[str, Any], table: str) -> set[str]:
    """The column names of one table's entry, or an empty set when it is read through any column."""
    value = found.get(table)
    return set(value) if isinstance(value, dict) else set()


def engine_cases() -> dict[str, bool]:
    """The extraction cases: each plants one construct in compiled SQL and states what it reads."""
    out: dict[str, bool] = {}
    base = {"stg_t1": stg("t1")}
    found = needs({**base, "int_a": sql("select a, b as renamed from @t1")})
    out["engine_a_named_read_of_a_staging_view_is_a_read_of_the_bronze_column"] = {"a", "b"} <= names_of(found, "t1") and "renamed" not in names_of(found, "t1")
    found = needs({**base, "int_a": sql('select "Quoted Col" from @t1')})
    out["engine_a_quoted_identifier_is_a_read"] = "quoted col" in names_of(found, "t1")
    found = needs({**base, "int_a": sql("select 'bronze.literal_only' as x, 'stg_t1.other' as y, a from @t1 -- bronze.comment_only")})
    out["engine_a_literal_or_a_comment_is_never_a_read"] = names_of(found, "t1") >= {"a"} and not names_of(found, "t1") & {
        "literal_only",
        "comment_only",
        "other",
        "x",
        "y",
    }
    flag_view = (
        'with bronze_rows as (select * from "lakehouse"."bronze"."t1"), bronze as (select *, review_flag_input as review_flag_output from bronze_rows)\n'
        'select bronze.a, bronze.review_flag_output from bronze inner join "memory"."main"."stg_bronze__files" as files on bronze._object_key = files.k'
    )
    found = needs({"stg_t1": flag_view, "int_a": sql("select a from @t1")})
    out["engine_a_flag_input_is_a_read_and_the_flag_output_is_not"] = (
        "review_flag_input" in names_of(found, "t1") and "review_flag_output" not in names_of(found, "t1") and "a" in names_of(found, "t1")
    )
    found = needs({**base, "int_a": sql("select a from @t1"), "int_b": sql("select a, b from @t1")})
    out["engine_two_readers_of_one_table_give_the_union_with_each_reader_kept"] = (
        names_of(found, "t1") >= {"a", "b"} and found["t1"]["a"] == ["int_a", "int_b"] and found["t1"]["b"] == ["int_b"]
    )
    found = needs({**base, "int_a": sql("with s as (select * from @t1) select a from s")})
    out["engine_a_pass_through_cte_reads_what_its_reader_names"] = "a" in names_of(found, "t1")
    found = needs({**base, "int_a": sql("select a from (select * from @t1) as s")})
    out["engine_a_pass_through_derived_table_reads_what_its_reader_names"] = "a" in names_of(found, "t1")
    found = needs({**base, "int_mid": sql("select * from @t1"), "int_a": 'select z from "memory"."main"."int_mid"'})
    out["engine_a_pass_through_model_reads_what_the_next_model_names"] = "z" in names_of(found, "t1")
    found = needs({**base, "int_leaf": sql("select * from @t1")})
    out["engine_a_model_with_no_reader_that_selects_every_column_is_declared_any_with_its_name"] = isinstance(found["t1"], str) and "int_leaf" in found["t1"]
    found = needs({**base}, outside={"fct_out": sql("select * from @t1")})
    out["engine_a_gold_model_that_selects_every_column_is_declared_any"] = isinstance(found["t1"], str) and "fct_out" in found["t1"]
    found = needs({**base, "int_a": sql('select s.a, q from @t1 as s join "memory"."main"."stg_t2" as u on s.a = u.a')})
    out["engine_an_unqualified_name_in_a_join_is_declared_any_not_guessed"] = isinstance(found["t1"], str) and "unqualified column q" in found["t1"]
    found = needs({**base, "int_a": sql('select s.a, u.b from @t1 as s join "memory"."main"."stg_t2" as u on s.a = u.a')})
    out["engine_a_qualified_name_in_a_join_is_read_from_its_own_table"] = names_of(found, "t1") >= {"a"} and "b" not in names_of(found, "t1")
    found = needs({**base, "int_a": sql("select list_filter(values_list, item -> item = bound_value) as kept from @t1")})
    out["engine_a_lambda_parameter_is_not_a_column"] = "item" not in names_of(found, "t1") and names_of(found, "t1") >= {"values_list", "bound_value"}
    found = needs({**base, "int_a": sql("with s as (select a from @t1) select a as shown from s where shown is not null order by shown")})
    out["engine_an_output_alias_no_input_column_can_share_is_never_a_read"] = "shown" not in names_of(found, "t1") and "a" in names_of(found, "t1")
    found = needs({**base, "int_a": sql("select a as shown from @t1 order by shown")})
    out["engine_an_output_alias_in_order_by_is_never_a_read"] = "shown" not in names_of(found, "t1") and "a" in names_of(found, "t1")
    found = needs({**base, "int_a": sql("select a from @t1 union all select b from @t1")})
    out["engine_a_set_operation_with_named_columns_reads_each_name"] = names_of(found, "t1") >= {"a", "b"}
    found = needs({**base, "int_a": sql("select * from @t1 union all select * from @t1")})
    out["engine_a_set_operation_that_passes_columns_through_is_declared_any"] = isinstance(found["t1"], str)
    found = needs({**base, "int_a": sql('select a from "memory"."main"."stg_t0" as o where exists (select 1 from @t1 as s where s.k = o.k and s.flag = 1)')})
    out["engine_a_subquery_reads_its_own_relation_by_name"] = names_of(found, "t1") >= {"k", "flag"}
    found = needs({**base, "int_a": sql('select s.a from @t1 as s join "memory"."main"."stg_t2" as u using (k)')})
    out["engine_a_using_join_column_is_read_from_the_tables_that_hold_it"] = "k" in names_of(found, "t1")
    found = needs({**base, "int_a": sql("select a from @t1")}, tests={"assert_t1": sql("select m from @t1 where n = 1")})
    out["engine_a_test_is_a_reader_and_is_named"] = found["t1"]["m"] == ["assert_t1"] and found["t1"]["a"] == ["int_a"]
    found = needs({**base, "int_a": sql("select a from @t1"), "int_broken": sql("from @t1")})
    out["engine_a_model_the_parser_refuses_makes_the_relations_it_names_any"] = isinstance(found["t1"], str) and "int_broken" in found["t1"]
    found = needs({**base, "int_a": sql("select a from @t1"), "int_broken": 'from "memory"."main"."other_table" select b'})
    out["engine_a_model_the_parser_refuses_that_names_no_bronze_relation_changes_nothing"] = names_of(found, "t1") >= {"a"}
    found = needs({**base, "int_a": sql("select t.payload.field as x from @t1 as t")})
    out["engine_a_qualified_struct_access_reads_the_top_level_column"] = "payload" in names_of(found, "t1") and "t" not in names_of(found, "t1")
    found = needs({**base, "int_a": sql("select payload.field as x from @t1")})
    out["engine_an_unqualified_struct_access_reads_the_top_level_column"] = "payload" in names_of(found, "t1") and "field" not in names_of(found, "t1")
    found = needs({**base, "int_a": sql("select a as b from @t1 where b = 1")})
    out["engine_an_alias_that_could_also_be_an_input_column_makes_the_table_any"] = isinstance(found["t1"], str) and "int_a" in found["t1"]
    found = needs({**base, "int_a": sql("with q as (select * exclude(hidden) from @t1) select a from q")})
    out["engine_a_star_exclusion_reads_the_excluded_column"] = names_of(found, "t1") >= {"a", "hidden"}
    found = needs({**base, "int_a": sql("select q.renamed from (select * from @t1) q(renamed)")})
    out["engine_a_positional_column_alias_never_invents_a_bronze_column"] = "renamed" not in names_of(found, "t1") and isinstance(found["t1"], str)
    found = needs({**base, "int_a": sql("select a as x, x + 1 as y from @t1")})
    out["engine_an_alias_used_later_in_the_select_list_could_be_an_input_column_so_the_table_is_any"] = isinstance(found["t1"], str)
    out.update(unwalkable_cases())
    return out


RECURSIVE_MODEL = (
    'with recursive r as (select _row_number from "lakehouse"."bronze"."@table" union all select _row_number + 1 from r where _row_number < 0) '
    "select _row_number from r"
)


def recursive_model(table: str) -> str:
    """A model whose query the analysis cannot walk: a recursive common table expression over a bronze table."""
    return RECURSIVE_MODEL.replace("@table", table)


def manifest_for(model: str, depends_on: list[str], sources: list[str]) -> dict[str, Any]:
    """A minimal dbt manifest: one silver model with its dependencies and the bronze sources."""
    nodes = {f"model.hai.{model}": {"resource_type": "model", "name": model, "path": f"silver/intermediate/{model}.sql", "depends_on": {"nodes": depends_on}}}
    nodes.update(
        {
            f"model.hai.stg_{table}": {
                "resource_type": "model",
                "name": f"stg_{table}",
                "path": f"x/stg_{table}.sql",
                "depends_on": {"nodes": [f"source.hai.bronze.{table}"]},
            }
            for table in sources
        }
    )
    return {"nodes": nodes, "sources": {f"source.hai.bronze.{table}": {"source_name": "bronze", "name": table} for table in sources}}


def unwalkable_cases() -> dict[str, bool]:
    """A model the analysis cannot walk takes its inputs from the manifest; when they cannot be established the declaration fails."""
    out: dict[str, bool] = {}
    with tempfile.TemporaryDirectory(prefix="bronze_unwalkable_") as folder:
        target = Path(folder) / "target"
        compiled_folder(target / "compiled", {"int_rec": recursive_model("acs_api_profile")})
        manifest = target / "manifest.json"
        manifest.write_text(json.dumps(manifest_for("int_rec", ["source.hai.bronze.acs_api_profile"], ["acs_api_profile"])))
        entries, unparsed = declare.derive(target / "compiled", ROOT)
        out["engine_a_model_the_analysis_cannot_walk_makes_its_manifest_source_any_with_the_model"] = "int_rec" in unparsed and "int_rec" in str(
            entries.get("acs_api_profile", {}).get("any")
        )
        manifest.write_text(json.dumps(manifest_for("int_rec", ["model.hai.stg_cms_hai_hospital"], ["cms_hai_hospital"])))
        entries, _ = declare.derive(target / "compiled", ROOT)
        out["engine_a_model_the_analysis_cannot_walk_makes_the_bronze_table_of_its_staging_dependency_any"] = "int_rec" in str(
            entries.get("cms_hai_hospital", {}).get("any")
        )
        manifest.write_text(json.dumps({"nodes": {}, "sources": {}}))
        try:
            declare.derive(target / "compiled", ROOT)
            refused = False
        except columns.ConsumerColumnsError as error:
            refused = "int_rec" in str(error)
        out["engine_a_model_the_analysis_cannot_walk_with_no_manifest_entry_fails_the_declaration_naming_it"] = refused
    return out


def scratch_plants(root: Path) -> None:
    """Edit a scratch copy of the dbt project: one plant for each case that reads the real compile."""
    shutil.copytree(ROOT / "dbt", root / "dbt", ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"))

    def replace(path: str, old: str, new: str) -> None:
        text = (root / path).read_text()
        if old not in text:
            raise AssertionError(f"{path} no longer has the anchor this plant edits: {old.strip()[:60]}")
        (root / path).write_text(text.replace(old, new, 1))

    replace(MACRO, ANCHOR, ANCHOR + QUOTED_PLANT)
    replace(MACRO, FLAG_ANCHOR, "{%- endfor %}\n,\nreview_flag_input as review_flag_output\n{%- endmacro %}")
    models = root / "dbt/models/silver"
    (models / "staging/stg_review_second.sql").write_text("{{ canonical_projection('illinois_report_card_csv', 'bronze._object_key') }}\n")
    (models / "staging/stg_review_third.sql").write_text("{{ canonical_projection('illinois_report_card_csv', 'bronze.review_third_only') }}\n")
    (models / "intermediate/int_review_unlisted.sql").write_text("select review_unlisted_column from {{ ref('stg_cms_hai_hospital') }}\n")
    (models / "intermediate/int_review_chain.sql").write_text("with s as (select * from {{ ref('stg_cms_hai_national') }}) select review_chain_column from s\n")
    (models / "intermediate/int_review_leaf.sql").write_text("select * from {{ ref('stg_cms_hai_state') }}\n")
    (models / "intermediate/int_review_ambiguous.sql").write_text(
        "select review_ambiguous from {{ ref('stg_places') }} as a inner join {{ ref('stg_rucc') }} as b on a._object_key = b._object_key\n"
    )
    (models / "staging/stg_review_added.sql").write_text("{{ canonical_rows('review_added') }}\n")
    replace(SOURCES, "      - name: stored_copies\n", SOURCE_PLANT + "      - name: stored_copies\n")
    replace(PROJECT, "    hud_zip_county_sheet_rows: geography\n", "    hud_zip_county_sheet_rows: geography\n    review_added: geography\n")
    with (root / SEED).open("a") as handle:
        handle.write("B99999_001E,acs_b16005,2099,review_acs_column,Estimate!!Review,mapped,a plant\n")
        handle.write("B99999_002E,acs_b16005,2099,review_held_column,Estimate!!Review,held,a plant\n")


TARGET_MACRO = (
    "{% macro review_target() %}{% if target.name == 'lint' %}select 1 as v{% else %}"
    "select review_target_only as v from {{ ref('stg_cms_hai_hospital') }}{% endif %}{% endmacro %}\n"
)


def cli_plants(root: Path) -> None:
    """A scratch copy for the real command: a recursive reader of a new bronze source, a struct read and a macro that branches on the target."""
    shutil.copytree(ROOT / "dbt", root / "dbt", ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"))
    (root / columns.CONTRACT).parent.mkdir(parents=True)
    shutil.copyfile(ROOT / columns.CONTRACT, root / columns.CONTRACT)
    shutil.copyfile(ROOT / declare.TARGET_BRANCHES, root / declare.TARGET_BRANCHES)
    sources = root / SOURCES
    sources.write_text(
        sources.read_text().replace(
            "      - name: stored_copies\n", "      - name: acs_api_profile\n        meta:\n          classification: Public\n      - name: stored_copies\n", 1
        )
    )
    models = root / "dbt/models/silver/intermediate"
    (models / "int_review_recursive.sql").write_text(
        "with recursive r as (select _row_number from {{ source('bronze', 'acs_api_profile') }} "
        "union all select _row_number + 1 from r where _row_number < 0) select _row_number from r\n"
    )
    (models / "int_review_struct.sql").write_text("select t.review_payload.field as v from {{ ref('stg_cms_hai_hospital') }} as t\n")
    (root / "dbt/macros/review_target.sql").write_text(TARGET_MACRO)
    (models / "int_review_target.sql").write_text("{{ review_target() }}\n")


def cli_cases() -> dict[str, bool]:
    """The real command on the scratch copy: it must never exit 0 with a planted read undeclared."""
    out: dict[str, bool] = {}
    with tempfile.TemporaryDirectory(prefix="bronze_columns_cli_") as folder:
        scratch = Path(folder)
        cli_plants(scratch)
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = declare.run(scratch)
    text = stdout.getvalue() + stderr.getvalue()
    out["cli_a_recursive_reader_of_a_new_bronze_table_fails_the_command_naming_the_table_as_any"] = code == 1 and any(
        "acs_api_profile" in line and "any column" in line for line in text.splitlines()
    )
    out["cli_a_struct_read_through_a_staging_view_fails_the_command_naming_the_column"] = any(
        "cms_hai_hospital" in line and "review_payload" in line for line in text.splitlines()
    )
    out["cli_a_macro_that_branches_on_the_target_fails_the_command_naming_file_and_line"] = code == 1 and "dbt/macros/review_target.sql:1" in text
    return out


def guard_cases() -> dict[str, bool]:
    """The target and env_var guard on a small tree: an unreviewed line fails by file and line, a reviewed one passes, a stale entry fails."""
    out: dict[str, bool] = {}
    line = "{% if target.name == 'lakehouse' %}select 1{% else %}select 2{% endif %}"
    with tempfile.TemporaryDirectory(prefix="bronze_guard_") as folder:
        root = Path(folder)
        (root / "dbt/macros").mkdir(parents=True)
        (root / "dbt/models/silver").mkdir(parents=True)
        (root / "config/lakehouse").mkdir(parents=True)
        (root / "dbt/macros/m.sql").write_text(f"{{% macro m() %}}\n    {line}\n{{% endmacro %}}\n")
        (root / "dbt/models/silver/x.sql").write_text("select {{ env_var('X', '1') }} as v\n")
        found = declare.target_branches_in(root, {})
        out["guard_an_unreviewed_target_or_env_var_line_fails_by_file_and_line"] = (
            len(found) == 2
            and found[0].startswith("dbt/models/silver/x.sql:1:")
            and found[1].startswith("dbt/macros/m.sql:2:")
            and "DYNAMIC_READERS" in found[0]
        )
        reviewed = {("dbt/macros/m.sql", line): "a reason", ("dbt/models/silver/x.sql", "select {{ env_var('X', '1') }} as v"): "a reason"}
        out["guard_a_reviewed_line_passes"] = declare.target_branches_in(root, reviewed) == []
        stale = {**reviewed, ("dbt/macros/gone.sql", "{{ target.name }}"): "a reason"}
        out["guard_a_reviewed_entry_that_matches_no_line_fails_as_stale"] = len(declare.target_branches_in(root, stale)) == 1
    return out


def real_cases(base: dict[str, dict[str, Any]], contract: dict[str, columns.Declaration], project: dict[str, Any]) -> dict[str, bool]:
    """The committed declaration against the models of the real project."""
    out: dict[str, bool] = {}
    out["real_the_committed_file_equals_what_the_compiled_models_give"] = declare.differences(base, contract) == []
    out["real_every_staged_and_every_deferred_table_is_declared"] = declare.completeness(contract, project) == []
    copies = set(project["vars"]["copy_tables"])
    out["real_every_copied_table_is_declared"] = copies <= set(contract)
    out["real_every_target_and_env_var_line_of_the_models_and_macros_is_reviewed"] = declare.target_branches(ROOT) == []
    out["real_the_deferred_finance_table_is_declared_as_deferred_by_name"] = "hcai_finance_csv" in contract and bool(contract["hcai_finance_csv"].deferred)
    anys = {table for table, declaration in contract.items() if declaration.columns is None}
    out["real_the_tables_read_through_any_column_are_named_with_a_reason"] = anys == {"cms_geographic_variation_csv", "svi"} and all(
        contract[t].reason for t in anys
    )
    out["real_every_declared_list_has_the_lineage_columns_it_is_read_with"] = all(
        set(declaration.columns or LINEAGE) >= LINEAGE for table, declaration in contract.items() if table in copies
    )
    out["real_the_three_state_tables_keep_the_lists_their_projections_read"] = (
        len(contract[UTIL].columns or {}) == 240 and len(contract[NONRESPONSE].columns or {}) == 240 and len(contract[ILLINOIS].columns or {}) == 17
    )
    out["real_an_acs_table_declares_its_mapped_columns_and_geo_id_from_the_seed"] = "geo_id" in (contract["acs_b16005"].columns or {}) and "b16005_001e" in (
        contract["acs_b16005"].columns or {}
    )
    out["real_a_declaration_keeps_which_models_read_each_column"] = (contract[ILLINOIS].columns or {}).get("county_name") == ("stg_illinois_report_card_csv",)
    return out


def plant_cases(planted: dict[str, dict[str, Any]], failures: list[str], contract: dict[str, columns.Declaration], project: dict[str, Any]) -> dict[str, bool]:
    """The scratch plants: what the declaration derived from the planted project says."""
    out: dict[str, bool] = {}
    illinois = planted[ILLINOIS]["columns"]
    out["plant_a_quoted_read_is_declared"] = "review_quoted_column" in illinois
    out["plant_a_literal_and_a_comment_are_not_declared"] = not {"review_literal_only", "review_comment_only"} & set(illinois)
    out["plant_a_flag_input_is_declared_and_its_output_is_not"] = (
        "review_flag_input" in planted[UTIL]["columns"] and "review_flag_output" not in planted[UTIL]["columns"]
    )
    out["plant_a_second_reader_adds_a_union_and_keeps_both_readers"] = (
        "review_third_only" in illinois
        and {"stg_illinois_report_card_csv", "stg_review_second"} <= set(illinois["_object_key"])
        and illinois["review_third_only"] == ["stg_review_third"]
    )
    out["plant_the_check_names_the_new_read_by_table_and_column"] = any(
        ILLINOIS in line and "review_quoted_column" in line for line in declare.differences(planted, contract)
    )
    out["plant_a_read_of_a_staging_view_that_selects_every_column_is_declared"] = planted["cms_hai_hospital"]["columns"].get("review_unlisted_column") == [
        "int_review_unlisted"
    ]
    out["plant_the_check_names_the_unlisted_read_by_table_and_column"] = any(
        "cms_hai_hospital" in line and "review_unlisted_column" in line for line in declare.differences(planted, contract)
    )
    out["plant_a_pass_through_chain_is_declared"] = "review_chain_column" in planted["cms_hai_national"]["columns"]
    out["plant_a_leaf_that_selects_every_column_makes_the_table_any_with_the_model"] = "int_review_leaf" in str(planted["cms_hai_state"].get("any"))
    out["plant_an_ambiguous_read_makes_both_tables_any"] = "any" in planted["places"] and "any" in planted["rucc"]
    out["plant_a_new_acs_column_is_declared_and_a_held_one_is_not"] = (
        "review_acs_column" in planted["acs_b16005"]["columns"] and "review_held_column" not in planted["acs_b16005"]["columns"]
    )
    added = [line for line in declare.differences(planted, contract) if "review_added" in line]
    out["plant_a_staging_model_added_without_a_declaration_fails_the_check_by_table"] = bool(added)
    gaps = declare.completeness(
        contract, {**project, "vars": {**project["vars"], "copy_tables": {**project["vars"]["copy_tables"], "review_added": "geography"}}}
    )
    out["plant_a_staged_table_with_no_declaration_fails_the_completeness_check_by_table"] = any("review_added" in line for line in gaps)
    gaps = declare.completeness({**contract, "hcai_finance_csv": columns.Declaration({}, "", "")}, project)
    out["plant_a_deferred_table_without_its_note_fails_the_completeness_check"] = any("hcai_finance_csv" in line for line in gaps)
    out["plant_the_sixteen_models_the_parser_refuses_are_listed_not_hidden"] = len(failures) == 16 and all(name.startswith("fct_") for name in failures)
    return out


def load_cases(contract: dict[str, columns.Declaration]) -> dict[str, bool]:
    """The load check on stand-ins for each kind of table, and the live pre-check."""
    out: dict[str, bool] = {}
    wanted = list(contract[ILLINOIS].columns or {})
    wide = [*wanted, *(f"extra_{index}" for index in range(300))]
    check = columns.check_loaded(ILLINOIS, True, wide, 5, contract)
    out["load_a_wide_table_with_extra_columns_passes"] = check.status == "ok" and not check.missing
    release_a = [name for name in wanted if name != "county_name"] + ["county"]
    release_b = [*wanted, "another_release_column"]
    union = sorted({*release_a, *release_b})
    out["load_a_table_whose_layout_varies_by_release_passes_when_the_union_holds_every_column"] = (
        columns.check_loaded(ILLINOIS, True, union, 9, contract).status == "ok"
    )
    out["load_a_table_that_lacks_a_column_in_every_release_is_refused_by_name"] = columns.check_loaded(
        ILLINOIS, True, release_a, 9, contract
    ).missing.keys() == {"county_name"}
    out["load_an_empty_table_passes_whatever_its_columns"] = columns.check_loaded(UTIL, True, ["_object_key"], 0, contract).status == "empty"
    out["load_a_dictionary_table_is_undeclared_and_passes"] = columns.check_loaded("hai_dictionary_columns", True, ["a"], 5, contract).status == "undeclared"
    out["load_a_table_with_no_file_in_the_run_is_skipped_not_refused"] = columns.check_loaded(UTIL, False, ["_object_key"], 0, contract).status == "skipped"
    out["load_a_table_with_no_file_in_the_run_is_skipped_even_with_rows_left_from_another_run"] = (
        columns.check_loaded(UTIL, False, ["_object_key"], 6995, contract).status == "skipped"
    )
    out["load_a_table_read_through_any_column_passes_and_says_so"] = columns.check_loaded("svi", True, ["_object_key"], 5, contract).status == "any"
    out["load_a_table_of_another_layout_is_refused_naming_each_column_and_its_reader"] = "fac_name" in columns.check_loaded(
        UTIL, True, ["_object_key", "u_id"], 6995, contract
    ).missing and "stg_hcai_util_pages_csv" in columns.refusal(
        UTIL, columns.check_loaded(UTIL, True, ["_object_key", "u_id"], 6995, contract).missing, ["_object_key", "u_id"]
    )
    out["load_the_refusal_names_the_table_the_column_and_the_present_columns"] = all(
        part in columns.refusal(ILLINOIS, {"county_name": ("stg_x", "int_y")}, ["u_id"])
        for part in (ILLINOIS, "county_name", "stg_x, int_y", "u_id", "missing")
    )
    out["load_column_names_compare_without_case"] = columns.check_loaded(ILLINOIS, True, [name.upper() for name in wanted], 3, contract).status == "ok"
    export = [
        {"Name": ILLINOIS, "Columns": [*wanted, "extra"], "Partitions": None},
        {"Name": UTIL, "Columns": ["_object_key", "u_id"], "Partitions": []},
        {"Name": "svi", "Columns": ["a"], "Partitions": None},
        {"Name": "hai_dictionary_columns", "Columns": ["a"], "Partitions": None},
    ]
    with tempfile.TemporaryDirectory(prefix="bronze_precheck_") as folder:
        path = Path(folder) / "glue.json"
        path.write_text(json.dumps(export))
        slim = precheck.report(contract, precheck.read_export(path))
        path.write_text(
            json.dumps(
                {
                    "TableList": [
                        {"Name": ILLINOIS, "StorageDescriptor": {"Columns": [{"Name": name} for name in wanted[:-1]]}, "PartitionKeys": [{"Name": wanted[-1]}]}
                    ]
                }
            )
        )
        full = precheck.report(contract, precheck.read_export(path))
    out["precheck_reports_the_missing_columns_of_a_table_by_name"] = (
        "fac_name" in slim["tables"][UTIL]["missing"] and slim["tables"][ILLINOIS]["status"] == "ok"
    )
    out["precheck_counts_absent_any_and_undeclared_tables_without_failing_them"] = (
        slim["tables"]["svi"]["status"] == "any"
        and slim["tables"]["acs_dp02"]["status"] == "absent"
        and slim["loaded_not_declared"] == ["hai_dictionary_columns"]
        and slim["counts"]["missing"] == 1
    )
    out["precheck_reads_the_full_get_tables_shape_and_counts_partition_columns"] = full["tables"][ILLINOIS]["status"] == "ok"
    rows = precheck.report(contract, {UTIL: ["_object_key"]}, {UTIL: 0})
    out["precheck_passes_a_table_a_row_count_file_gives_as_empty"] = rows["tables"][UTIL]["status"] == "empty"
    return out


def cases() -> dict[str, bool]:
    """Run every case and return its outcome."""
    contract = columns.load(ROOT / columns.CONTRACT)
    project = declare.read_project(ROOT)
    results = engine_cases()
    with tempfile.TemporaryDirectory(prefix="bronze_columns_") as folder:
        base, _ = declare.derive(declare.compile_project(ROOT / "dbt", Path(folder) / "target"), ROOT)
    results.update(real_cases(base, contract, project))
    with tempfile.TemporaryDirectory(prefix="bronze_columns_case_") as folder:
        scratch = Path(folder)
        scratch_plants(scratch)
        compiled = declare.compile_project(scratch / "dbt", scratch / "target", packages=ROOT / "data/analytics/dbt/dbt_packages")
        planted, failures = declare.derive(compiled, scratch)
    results.update(plant_cases(planted, sorted(failures), contract, project))
    results.update(guard_cases())
    results.update(cli_cases())
    results.update(load_cases(contract))
    return results


def main() -> int:
    """Run the cases, write the report and return 1 when any case fails."""
    started = datetime.now(UTC)
    outcome = cases()
    folder = ROOT / "data" / "e2e" / "bronze_consumer_columns"
    folder.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {"run_at_utc": started.strftime("%Y%m%dT%H%M%SZ"), "cases": outcome, "passed": all(outcome.values())}
    (folder / f"report_{report['run_at_utc']}.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    for name, passed in outcome.items():
        sys.stdout.write(f"{'ok' if passed else 'FAIL':<5} {name}\n")
    sys.stdout.write(f"{sum(outcome.values())}/{len(outcome)} cases passed\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
