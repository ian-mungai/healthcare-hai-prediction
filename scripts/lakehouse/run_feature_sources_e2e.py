"""End-to-end run of the feature-phase source check: the committed files pass and each planted defect fails for its own reason.

Run from the repository root:

    .venv/bin/python -m scripts.lakehouse.run_feature_sources_e2e [--output-directory DIR] [--only CASE_NAME_PART ...]

Every text case copies the feature folders, the gold and silver model names, the seed names, the project file, the selector file and the gold build
selections to a scratch folder, plants one defect and runs ``feature_sources.check`` on the copy. The defects cover a silver or bronze read, a retired
seed read, a reference the text pass cannot resolve (a Jinja variable, an expression, a silver relation named in SQL text), each missing tag, selector or
exclusion and each way a bridge filter model can drift (another seed, an added predicate, fewer columns, a comment, a missing file, a file for no retired
seed, another model in the folder, a filter model that nothing reads, a reader that names a missing filter model). The resolved cases build the restricted
database once (``feature_columns.build_shell``: the gold tables of the contract, the feature seeds and the live seeds, nothing else), then copy the dbt
project and that database for each case, plant one defect and run ``feature_sources.resolved_check``, which builds the feature phase in two stages. They
read the silver rating input in every spelling the text pass missed, a silver read in a test, a bronze table, a retired seed, a bridge seed, a file and
another database. They also read the raw ``bridge_measure_control`` in every way that rounds 1 to 4 of the review hid from the earlier guards (a ``ref``, a
``count(*)`` over ``query_table``, a macro, a singular test, an alias equal to the bridge name, strings and comments, a read beside a filtered upstream
model): each must fail as a stage-2 build or bind error because stage 2 holds no raw bridge. Reads of a filter model in the forms that failed for a wrong
reason before (an aliased model, dashes in a string, the bridge name as alias, an aggregate, a test) and a read of a gold fact through a table function or in
upper case must pass. The allow-list cases (``allowlist_cases``) plant the ways to run SQL outside the compiled query of a node: a ``run_query`` that
rewrites a filter table or creates, reads and drops the raw bridge, a ``sql_header``, a changed run-hook macro, a ``statement`` block, ``adapter.execute``, an
unknown config key, a macro that reaches ``run_query`` through another macro, Jinja in a YAML test argument and the like; each must fail with its own message
before anything builds, and the committed files must pass with no message. The artifact (``report.json`` in the output folder, default
``data/e2e/feature_sources/<UTC time>/``) lists each case with the message that matched. The run needs no Docker; the resolved cases start dbt from
``.tools/sqlfluff`` (about 30 seconds each).
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import feature_columns as shell
from scripts.lakehouse import feature_shards
from scripts.lakehouse import feature_sources as sources

SQL_TREES = ("dbt/models/features", "dbt/macros/features", "dbt/tests/features", "dbt/models/gold", "dbt/models/silver")
COPIED_FILES = ("dbt/dbt_project.yml", "dbt/selectors.yml", *sources.GOLD_BUILD_FILES)
ENABLED_MODEL = "dbt/models/features/int_spine_hai_outcomes.sql"
CARE_COMPARE_MODEL = "dbt/models/features/int_spine_care_compare_measures.sql"
RATING_REF = "from {{ ref('fct_hgi_release') }}"
FILTER_FOLDER = "dbt/models/features/bridge"
GEOGRAPHY_FILTER = f"{FILTER_FOLDER}/feature_bridge_geography_measures.sql"
BYPASS_SET = "{% set rating_input = 'int_hgi_hospital_releases' %}\n"
COST_REPORT_MODEL = "dbt/models/features/int_cost_report_measures.sql"
HAI_OUTCOME_MODEL = "dbt/models/features/int_hai_outcome_values.sql"
HOSPITAL_TEST = "dbt/tests/features/assert_hospital_control_identity.sql"
ENABLED_TEST = "dbt/tests/features/assert_spine_as_of_window_start.sql"
MACRO = "dbt/macros/features/cmi_years.sql"
PROJECT = "dbt/dbt_project.yml"
SELECTORS = "dbt/selectors.yml"
Damage = Callable[[Path], None]


def replace(name: str, old: str, new: str) -> Damage:
    """A defect that replaces one text in one file; it must change the file."""

    def change(root: Path) -> None:
        path = root / name
        text = path.read_text()
        if old not in text:
            raise RuntimeError(f"{name} does not hold the text this case changes: {old!r}")
        path.write_text(text.replace(old, new, 1))

    return change


def append(name: str, text: str) -> Damage:
    """A defect that adds text at the end of a file."""

    def change(root: Path) -> None:
        (root / name).write_text((root / name).read_text() + text)

    return change


def create(name: str, text: str) -> Damage:
    """A defect that adds a new file."""

    def change(root: Path) -> None:
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(text)

    return change


def prepend(name: str, text: str) -> Damage:
    """A defect that adds text at the start of a file."""

    def change(root: Path) -> None:
        (root / name).write_text(text + (root / name).read_text())

    return change


def remove(name: str) -> Damage:
    """A defect that deletes a file."""
    return lambda root: (root / name).unlink()


def add_seed(name: str) -> Damage:
    """A defect that brings an empty seed file back under ``dbt/seeds``."""

    def change(root: Path) -> None:
        (root / name).touch()

    return change


def both(first: Damage, second: Damage) -> Damage:
    """A defect made of two changes."""

    def change(root: Path) -> None:
        first(root)
        second(root)

    return change


def copy_scratch(scratch: Path) -> None:
    """Copy what the check reads: the SQL trees, empty files for the seed names and the configuration files."""
    for tree in SQL_TREES:
        shutil.copytree(sources.REPO_ROOT / tree, scratch / tree, ignore=shutil.ignore_patterns("*.yml"))
    for seed in (sources.REPO_ROOT / "dbt/seeds").rglob("*.csv"):
        target = scratch / seed.relative_to(sources.REPO_ROOT)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.touch()
    for name in COPIED_FILES:
        (scratch / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(sources.REPO_ROOT / name, scratch / name)


def cases() -> list[tuple[str, Damage, str | None]]:
    """The defects with the message each must produce; ``None`` means the committed state with a harmless change must still pass."""
    return [
        ("committed_files", lambda root: None, None),
        ("silver_int_ref_in_model", append(ENABLED_MODEL, "-- {{ ref('int_hai_hospital_windows') }}\n"), "int_hai_hospital_windows') is a silver model"),
        ("silver_stg_ref_in_model", append(ENABLED_MODEL, "{{ ref('stg_acs_dp02') }}\n"), "stg_acs_dp02') is a silver model"),
        (
            "silver_ref_with_two_arguments",
            append(ENABLED_MODEL, '{{ ref("hai", "int_pos_hospital_snapshots") }}\n'),
            "int_pos_hospital_snapshots') is a silver model",
        ),
        ("silver_ref_in_macro", replace(MACRO, "ref('fct_cmi_observation')", "ref('int_cmi_hospital_rows')"), "int_cmi_hospital_rows') is a silver model"),
        ("silver_ref_in_test", append(ENABLED_TEST, "{{ ref('int_hospital_enrollment_rows') }}\n"), "int_hospital_enrollment_rows') is a silver model"),
        ("source_call", append(ENABLED_MODEL, "{{ source('bronze', 'cms_hai_hospital') }}\n"), "reads bronze"),
        (
            "retired_seed_ref_in_model",
            replace(COST_REPORT_MODEL, "ref('cost_report_measures_recipe')", "ref('cost_report_measures')"),
            "ref('cost_report_measures') is a retired registry-map seed",
        ),
        (
            "retired_seed_ref_in_test",
            replace(HOSPITAL_TEST, "ref('impact_measures_recipe')", "ref('impact_measures')"),
            "ref('impact_measures') is a retired registry-map seed",
        ),
        ("retired_seed_ref_in_macro", append(MACRO, "{{ ref('mup_measures') }}\n"), "ref('mup_measures') is a retired registry-map seed"),
        (
            "retired_seed_back_in_seed_folder",
            both(
                replace(COST_REPORT_MODEL, "ref('cost_report_measures_recipe')", "ref('cost_report_measures')"), add_seed("dbt/seeds/cost_report_measures.csv")
            ),
            "ref('cost_report_measures') is a retired registry-map seed",
        ),
        ("unknown_name_ref", append(ENABLED_MODEL, "{{ ref('not_a_model') }}\n"), "ref('not_a_model') names no gold model, live seed or feature model"),
        (
            "jinja_variable_ref_bypass",
            both(prepend(CARE_COMPARE_MODEL, BYPASS_SET), replace(CARE_COMPARE_MODEL, RATING_REF, "from {{ ref(rating_input) }}")),
            "'ref(rating_input) }}' is a ref() or source() call whose argument is not a quoted literal",
        ),
        (
            "jinja_expression_ref",
            replace(CARE_COMPARE_MODEL, RATING_REF, "from {{ ref('int_hgi_' ~ 'hospital_releases') }}"),
            "is a ref() or source() call whose argument is not a quoted literal",
        ),
        (
            "jinja_variable_source",
            append(ENABLED_MODEL, "{{ source(bronze_schema, 'cms_hai_hospital') }}\n"),
            "source(bronze_schema, 'cms_hai_hospital') }}\" is a ref() or source() call whose argument is not a quoted literal",
        ),
        (
            "ref_with_keyword_argument",
            append(ENABLED_MODEL, "{{ ref('int_spine_linkage', v=1) }}\n"),
            "ref('int_spine_linkage', v=1) }}\" is a ref() or source() call whose argument is not a quoted literal",
        ),
        (
            "ref_in_macro_with_variable",
            append(MACRO, "{% macro read_input(name) %}{{ ref(name) }}{% endmacro %}\n"),
            "cmi_years.sql:",
        ),
        (
            "silver_relation_named_in_sql",
            replace(CARE_COMPARE_MODEL, RATING_REF, "from main.int_hgi_hospital_releases"),
            "int_hgi_hospital_releases is a silver model or retired seed named in SQL text",
        ),
        (
            "silver_relation_double_quoted_in_sql",
            replace(CARE_COMPARE_MODEL, RATING_REF, 'from "main"."int_hgi_hospital_releases"'),
            "int_hgi_hospital_releases is a silver model or retired seed named in SQL text",
        ),
        (
            "retired_seed_named_in_sql",
            replace(COST_REPORT_MODEL, "{{ ref('cost_report_measures_recipe') }}", "cost_report_measures"),
            "cost_report_measures is a silver model or retired seed named in SQL text",
        ),
        (
            "silver_relation_named_in_test",
            append(ENABLED_TEST, "select * from int_hospital_enrollment_rows\n"),
            "int_hospital_enrollment_rows is a silver model or retired seed named in SQL text",
        ),
        (
            "filter_model_for_another_seed",
            replace(GEOGRAPHY_FILTER, "'geography_measures'", "'registry_measure_sources'"),
            "feature_bridge_geography_measures.sql does not equal the bridge filter template",
        ),
        (
            "filter_model_with_an_added_predicate",
            replace(GEOGRAPHY_FILTER, "where source_seed = 'geography_measures'", "where source_seed = 'geography_measures' or true"),
            "feature_bridge_geography_measures.sql does not equal the bridge filter template",
        ),
        (
            "filter_model_with_fewer_columns",
            replace(GEOGRAPHY_FILTER, "select *", "select measure_control"),
            "feature_bridge_geography_measures.sql does not equal the bridge filter template",
        ),
        (
            "filter_model_with_a_comment",
            append(GEOGRAPHY_FILTER, "-- filtered\n"),
            "feature_bridge_geography_measures.sql does not equal the bridge filter template",
        ),
        ("filter_model_missing", remove(GEOGRAPHY_FILTER), "retired seed geography_measures has no filter model"),
        (
            "filter_model_for_no_retired_seed",
            create(f"{FILTER_FOLDER}/feature_bridge_not_a_seed.sql", sources.bridge_filter_text("not_a_seed")),
            "feature_bridge_not_a_seed.sql names no retired seed",
        ),
        ("other_model_in_the_filter_folder", create(f"{FILTER_FOLDER}/int_other.sql", "select 1 as one\n"), "int_other.sql is no bridge filter model"),
        (
            "filter_model_without_a_reader",
            replace(
                "dbt/tests/features/assert_registry_controls_identity.sql",
                "{{ ref('feature_bridge_cost_report_measures') }}",
                "{{ ref('feature_bridge_mup_measures') }}",
            ),
            "feature_bridge_cost_report_measures is read by no feature model or test",
        ),
        (
            "reader_names_a_missing_filter_model",
            replace(HAI_OUTCOME_MODEL, "ref('feature_bridge_hai_outcome_measures')", "ref('feature_bridge_not_a_seed')"),
            "ref('feature_bridge_not_a_seed') names no gold model, live seed or feature model",
        ),
        ("bridge_seed_folder_missing", lambda root: shutil.rmtree(root / "dbt/seeds/gold"), "no bridge seed found"),
        (
            "seed_tag_removed",
            replace(
                PROJECT,
                "seeds:\n  hai:\n    # The formula recipes are configuration of the feature phase, not gold: the gold selections exclude this tag.\n"
                "    features:\n      +tags: ['features']\n",
                "seeds:\n",
            ),
            "seeds.hai.features has no +tags",
        ),
        ("no_feature_model", lambda root: shutil.rmtree(root / "dbt/models/features"), "no feature model found"),
        (
            "model_tag_removed",
            replace(PROJECT, "      +materialized: view\n      +tags: ['features']\n", "      +materialized: view\n"),
            "models.hai.features has no +tags",
        ),
        (
            "test_tag_removed",
            replace(PROJECT, "data_tests:\n  hai:\n    features:\n      +tags: ['features']\n", "data_tests:\n"),
            "data_tests.hai.features has no +tags",
        ),
        (
            "view_materialization_removed",
            replace(PROJECT, "features:\n      +materialized: view", "features:\n      +materialized: table"),
            "is not +materialized: view",
        ),
        (
            "extra_on_run_end_hook",
            replace(PROJECT, '  - "{{ hai_finalize_build_marker(results) }}"\n', '  - "{{ hai_finalize_build_marker(results) }}"\n  - "select 1"\n'),
            "on-run-end is ['{{ hai_finalize_build_marker(results) }}', 'select 1'], not the pinned",
        ),
        (
            "extra_on_run_start_hook",
            replace(PROJECT, '  - "{{ hai_drop_build_marker() }}"\n', '  - "{{ hai_drop_build_marker() }}"\n  - "select 1"\n'),
            "on-run-start is ['{{ hai_drop_build_marker() }}', 'select 1'], not the pinned",
        ),
        (
            "dispatch_setting_added",
            append(PROJECT, "dispatch:\n  - macro_namespace: dbt\n    search_order: ['hai', 'dbt']\n"),
            "the key dispatch is outside the keys of the feature build",
        ),
        (
            "query_comment_macro_added",
            append(PROJECT, 'query-comment:\n  comment: "{{ hai_comment() }}"\n'),
            "the key query-comment is outside the keys of the feature build",
        ),
        ("default_selector_removed", remove(SELECTORS), "no default selector leaves tag 'features' out"),
        ("default_selector_loses_exclusion", replace(SELECTORS, "value: features", "value: other"), "no default selector leaves tag 'features' out"),
        (
            "exclusion_removed_from_broker_template",
            broker_without_exclusion,
            "config/orchestration/broker_templates.json:1: the gold build selection does not exclude",
        ),
        (
            "exclusion_removed_from_pipeline_build",
            replace("scripts/lakehouse/pipeline_build.py", '"tag:features"', '"tag:other"'),
            "pipeline_build.py:1: the gold build selection does not exclude",
        ),
        (
            "exclusion_removed_from_gold_e2e",
            replace("scripts/lakehouse/run_gold_e2e.py", " --exclude tag:features", ""),
            "run_gold_e2e.py:1: the gold build selection does not exclude",
        ),
        (
            "default_selector_includes_the_tag",
            replace(
                SELECTORS, "        - exclude:\n            - method: tag\n              value: features", "        - method: tag\n          value: features"
            ),
            "no default selector leaves tag 'features' out",
        ),
        (
            "exclusion_only_in_a_comment_of_pipeline_build",
            replace(
                "scripts/lakehouse/pipeline_build.py", '*(["--exclude", "tag:features"] if args.layer == "gold" else []),', '# ["--exclude", "tag:features"]'
            ),
            "pipeline_build.py:1: the gold build selection does not exclude",
        ),
        ("exclusion_apart_from_its_option_in_a_broker_template", broker_exclusion_moved, "broker_templates.json:1: the gold build selection does not exclude"),
    ]


def broker_without_exclusion(root: Path) -> None:
    """Drop the exclusion from every mode of the gold_build template."""
    path = root / "config/orchestration/broker_templates.json"
    document = json.loads(path.read_text())
    for mode in document["templates"]["gold_build"]["modes"].values():
        args = mode.get("launcher", {}).get("args") or []
        if "--exclude" in args:
            del args[args.index("--exclude") : args.index("--exclude") + 2]
    path.write_text(json.dumps(document, indent=2) + "\n")


def broker_exclusion_moved(root: Path) -> None:
    """Keep ``--exclude`` and ``tag:features`` in every mode of the gold_build template, but not next to each other."""
    path = root / "config/orchestration/broker_templates.json"
    document = json.loads(path.read_text())
    for mode in document["templates"]["gold_build"]["modes"].values():
        args = mode.get("launcher", {}).get("args") or []
        if "--exclude" in args:
            args[args.index("--exclude") : args.index("--exclude") + 2] = ["--exclude", "tag:other", "--select", "tag:features"]
    path.write_text(json.dumps(document, indent=2) + "\n")


def pass_cases() -> list[tuple[str, Damage, str | None]]:
    """Changes that are no defect and must still pass: silver names in comments and strings."""
    return [
        (
            "silver_name_in_comments_passes",
            append(ENABLED_MODEL, "-- ref('int_hai_hospital_windows') in a SQL comment\n{# ref('stg_acs_dp02') #}\n"),
            None,
        ),
        (
            "silver_name_in_string_and_comment_passes",
            append(ENABLED_TEST, "-- int_hospital_enrollment_rows\nselect 'int_hospital_enrollment_rows' as note\n"),
            None,
        ),
    ]


BUILD_FAILS = "model.hai.int_spine_care_compare_measures did not build on a database that holds only what the feature phase may read"
TEST_FAILS = "the compiled query of test.hai.assert_spine_as_of_window_start does not bind on a database that holds only what the feature phase may read"
RATING_QUERY = "int_hgi_hospital_releases"
PROBE_MODEL = "dbt/models/features/int_probe.sql"
PROBE_FAILS = "model.hai.int_probe did not build on a database that holds only what the feature phase may read"


def probe(query: str, config: str = "") -> Damage:
    """A defect that adds a feature model with no contract that does nothing but run ``query``, so the build can fail only on what the query reads."""
    return create(PROBE_MODEL, "{{ config(contract={'enforced': false}" + config + ") }}\n" + query + "\n")


def contracted_probe(name: str, query: str) -> Damage:
    """A defect that adds a feature model with the folder's enforced contract and one ``measure_control`` column, so only what its query reads can fail it."""
    return both(
        create(f"dbt/models/features/{name}.sql", query + "\n"),
        create(
            f"dbt/models/features/_{name}.yml",
            f"version: 2\nmodels:\n  - name: {name}\n    columns:\n      - name: measure_control\n        data_type: varchar\n",
        ),
    )


def rating_from(source: str) -> Damage:
    """A defect that reads the Hospital General Information releases from ``source`` instead of the gold fact."""
    return replace(CARE_COMPARE_MODEL, RATING_REF, f"from {source}")


def resolved_cases() -> list[tuple[str, Damage, str | None]]:
    """Defects the resolved pass must catch by building and binding the feature phase, with the message each must produce; ``None`` must pass.

    The silver read of the rating input is spelled in every way a text rule missed: a literal name, case and quotes, ``query_table`` with a plain, quoted,
    variable-built and macro-built argument, an adapter relation, a catalog name and a name joined at run time. A test, a retired seed, a bridge seed, a
    file and another database are read in the same way.
    """
    review_macro = "dbt/macros/review_rating.sql"
    return [
        ("committed_files_resolved", lambda root: None, None),
        (
            "jinja_variable_ref_bypass_resolved",
            both(prepend(CARE_COMPARE_MODEL, BYPASS_SET), replace(CARE_COMPARE_MODEL, RATING_REF, "from {{ ref(rating_input) }}")),
            "depends on model.hai.int_hgi_hospital_releases, a silver model",
        ),
        ("silver_relation_literal_resolved", rating_from("main.int_hgi_hospital_releases"), BUILD_FAILS),
        ("silver_relation_quoted_uppercase_resolved", rating_from('"MAIN"."INT_HGI_HOSPITAL_RELEASES"'), BUILD_FAILS),
        ("silver_query_table_resolved", rating_from(f"query_table('{RATING_QUERY}')"), BUILD_FAILS),
        ("silver_query_table_qualified_quoted_resolved", rating_from('query_table(\'"main"."int_hgi_hospital_releases"\')'), BUILD_FAILS),
        ("silver_query_table_uppercase_resolved", rating_from("query_table('INT_HGI_HOSPITAL_RELEASES')"), BUILD_FAILS),
        ("silver_query_table_joined_name_resolved", rating_from("query_table('int_hgi_' || 'hospital_releases')"), BUILD_FAILS),
        ("silver_query_table_var_resolved", rating_from('query_table(\'{{ var("rating_table", "int_hgi_hospital_releases") }}\')'), BUILD_FAILS),
        (
            "silver_query_table_macro_resolved",
            both(create(review_macro, f"{{% macro review_rating() %}}query_table('{RATING_QUERY}'){{% endmacro %}}\n"), rating_from("{{ review_rating() }}")),
            BUILD_FAILS,
        ),
        (
            "silver_adapter_relation_uppercase_resolved",
            rating_from("{{ adapter.Relation.create(schema='MAIN', identifier='INT_HGI_HOSPITAL_RELEASES') }}"),
            BUILD_FAILS,
        ),
        (
            "silver_adapter_relation_var_resolved",
            rating_from("{{ adapter.Relation.create(schema='main', identifier=var('rating_table', 'int_hgi_hospital_releases')) }}"),
            BUILD_FAILS,
        ),
        ("silver_catalog_qualified_resolved", rating_from("lakehouse.silver.int_hgi_hospital_releases"), BUILD_FAILS),
        ("bronze_table_by_name_resolved", probe("select * from query_table('cms_hai_hospital')"), PROBE_FAILS),
        ("silver_query_function_resolved", rating_from("query('select * from int_hgi_hospital_releases')"), BUILD_FAILS),
        (
            "silver_read_in_a_test_resolved",
            append(ENABLED_TEST, "\nunion all\nselect ccn, 0, null, null, null from query_table('int_hgi_hospital_releases')\n"),
            TEST_FAILS,
        ),
        ("retired_seed_by_name_resolved", probe("select * from query_table('impact_measures')"), PROBE_FAILS),
        ("bridge_seed_by_name_resolved", probe("select * from query_table('registry_measure_sources_bridge')"), PROBE_FAILS),
        ("gold_dimension_without_contract_columns_resolved", probe("select * from {{ ref('dim_hospital') }}"), PROBE_FAILS),
        (
            "file_read_resolved",
            both(create("silver_probe.csv", "ccn\n000001\n"), probe("select * from read_csv('silver_probe.csv')")),
            PROBE_FAILS,
        ),
        (
            "other_database_attached_resolved",
            probe("select 1 as one", ", pre_hook=\"attach 'other_probe.duckdb' as other\""),
            PROBE_FAILS,
        ),
        *bypass_cases(),
        *filter_read_cases(),
        *isolation_cases(),
        ("contract_switched_off_resolved", replace(PROJECT, "enforced: true", "enforced: false"), "has no enforced column contract"),
        (
            "run_query_rewrites_a_filter_table_is_seen_in_the_relation_digest_resolved",
            prepend(
                "dbt/models/features/int_cmi_holds.sql",
                "{% if execute %}{% do run_query('create or replace table main.feature_bridge_cost_report_measures as select * from "
                "main.feature_bridge_geography_measures') %}{% endif %}\n",
            ),
            "the stage-2 database holds the relation main.feature_bridge_cost_report_measures with other rows after the run than before",
        ),
        ("gold_fact_by_table_function_passes", rating_from("query_table('fct_hgi_release')"), None),
        ("gold_fact_quoted_uppercase_passes", rating_from('"MAIN"."FCT_HGI_RELEASE"'), None),
    ]


def isolation_cases() -> list[tuple[str, Damage, str | None]]:
    """What round 5 of the review found hidden from the guard: hooks, macros that replace dbt's own, nodes that no step bound and a relation the run added.

    Each case plants one defect in a copy of the dbt project and expects its own message: a hook on a filter model, a folder-level hook, a stage-2 pre-hook that
    recreates ``bridge_measure_control`` from a filter table (the hook is reported and so is the relation), a model that adds a relation through ``run_query``,
    a global ``ref`` override (reported as an override and as a filter query that differs from the template), the other overrides (``source``, a dbt macro
    name, an adapter implementation, a materialization), a generic test on a seed that reads the raw bridge (tagged or not), and a tagged snapshot, analysis,
    source and unit test.
    """
    ref_override = (
        "{% macro ref(model_name) %}{% if model_name == 'bridge_measure_control' %}{{ builtins.ref(model_name) }} where true union all select * from "
        "{{ builtins.ref(model_name) }}{% else %}{{ builtins.ref(model_name) }}{% endif %}{% endmacro %}\n"
    )
    raw_test = "{% test reads_raw_bridge(model, column_name) %}select * from {{ ref('bridge_measure_control') }} limit 0{% endtest %}\n"
    test_macro = "dbt/macros/features/raw_bridge_generic_test.sql"
    seed_column = "      - name: measure_control\n        description: The registry measure control the rule belongs to.\n"
    seed_test = "        data_tests:\n          - reads_raw_bridge" + "{config}\n"
    seeds_yaml = "dbt/seeds/features/_features__seeds.yml"
    recreate = probe(
        "select cast(count(*) as varchar) as measure_control from query_table('bridge_measure_control')",
        ', pre_hook="create table bridge_measure_control as select * from feature_bridge_geography_measures"',
    )
    run_query = create(
        PROBE_MODEL,
        "{{ config(contract={'enforced': false}) }}\n"
        "{% if execute %}{% do run_query('create or replace table leftover_relation as select 1 as one') %}{% endif %}\n"
        "select 'x' as measure_control\n",
    )
    return [
        ("stage_two_pre_hook_recreates_the_bridge_resolved", recreate, "the stage-2 database holds the relation main.bridge_measure_control"),
        ("relation_added_by_run_query_resolved", run_query, "the stage-2 database holds the relation main.leftover_relation"),
        ("ref_override_resolved", create("dbt/macros/ref_override.sql", ref_override), "macro.hai.ref overrides the dbt built-in ref()"),
        (
            "ref_override_changes_the_filter_query_resolved",
            create("dbt/macros/ref_override.sql", ref_override),
            "the compiled query of model.hai.feature_bridge_geography_measures is",
        ),
        (
            "source_override_resolved",
            create(
                "dbt/macros/source_override.sql", "{% macro source(source_name, table_name) %}{{ builtins.source(source_name, table_name) }}{% endmacro %}\n"
            ),
            "macro.hai.source overrides the dbt built-in source()",
        ),
        (
            "dbt_macro_name_override_resolved",
            create("dbt/macros/schema_override.sql", "{% macro generate_schema_name(custom_schema_name, node) -%}{{ target.schema }}{%- endmacro %}\n"),
            "macro.hai.generate_schema_name has the name of a dbt or dbt-duckdb macro",
        ),
        (
            "adapter_implementation_override_resolved",
            create(
                "dbt/macros/adapter_override.sql",
                "{% macro duckdb__create_table_as(temporary, relation, compiled_code, language='sql') -%}\n"
                "create table {{ relation }} as ({{ compiled_code }})\n{%- endmacro %}\n",
            ),
            "macro.hai.duckdb__create_table_as has the name of a dbt or dbt-duckdb macro",
        ),
        (
            "materialization_macro_resolved",
            create("dbt/macros/materialization_probe.sql", "{% materialization probe_mat, default %}{{ return({'relations': []}) }}{% endmaterialization %}\n"),
            "macro.hai.materialization_probe_mat_default defines a dbt materialization",
        ),
        (
            "tagged_seed_generic_test_reading_the_raw_bridge_resolved",
            both(
                create(test_macro, raw_test),
                replace(seeds_yaml, seed_column, seed_column + seed_test.format(config=":\n              config:\n                tags: ['features']")),
            ),
            "the compiled query of test.hai.reads_raw_bridge_",
        ),
        (
            "untagged_seed_generic_test_reading_the_raw_bridge_resolved",
            both(create(test_macro, raw_test), replace(seeds_yaml, seed_column, seed_column + seed_test.format(config=""))),
            "the compiled query of test.hai.reads_raw_bridge_",
        ),
        (
            "generic_test_in_a_yaml_file_named_like_the_filter_folder_resolved",
            both(
                both(
                    create(test_macro, raw_test),
                    create("dbt/models/features/int_probe_bridge.sql", "select 'x' as measure_control\n"),
                ),
                create(
                    "dbt/models/features/bridge_review.yml",
                    "version: 2\nmodels:\n  - name: int_probe_bridge\n    columns:\n      - name: measure_control\n        data_type: varchar\n"
                    "        data_tests:\n          - reads_raw_bridge\n",
                ),
            ),
            "the compiled query of test.hai.reads_raw_bridge_int_probe_bridge_measure_control",
        ),
        (
            "tagged_snapshot_resolved",
            create(
                "dbt/snapshots/probe_snapshot.sql",
                "{% snapshot probe_snapshot %}\n"
                "{{ config(target_schema='main', unique_key='measure_control', strategy='check', check_cols='all', tags=['features']) }}\n"
                "select * from {{ ref('bridge_measure_control') }}\n{% endsnapshot %}\n",
            ),
            "snapshot.hai.probe_snapshot has the resource type 'snapshot'",
        ),
        (
            "tagged_analysis_resolved",
            create("dbt/analyses/probe_analysis.sql", "{{ config(tags=['features']) }}\nselect * from {{ ref('bridge_measure_control') }}\n"),
            "analysis.hai.probe_analysis has the resource type 'analysis'",
        ),
        (
            "tagged_source_resolved",
            create(
                "dbt/models/features/_probe_source.yml",
                "version: 2\nsources:\n  - name: probe_bronze\n    tags: ['features']\n    tables:\n      - name: probe_table\n",
            ),
            "source.hai.probe_bronze.probe_table has the resource type 'source'",
        ),
        (
            "unit_test_resolved",
            both(
                create(
                    "dbt/models/features/int_probe_unit.sql",
                    "{{ config(contract={'enforced': false}) }}\nselect 'x' as measure_control\n",
                ),
                create(
                    "dbt/models/features/_probe_unit.yml",
                    "version: 2\nunit_tests:\n  - name: probe_unit\n    model: int_probe_unit\n    given: []\n"
                    "    expect:\n      rows:\n        - {measure_control: x}\n",
                ),
            ),
            "unit_test.hai.int_probe_unit.probe_unit has the resource type 'unit_test'",
        ),
    ]


def allowlist_cases() -> list[tuple[str, Damage, str | None]]:
    """What round 6 of the review found hidden from the guard, and the other ways to run SQL outside the compiled query of a node.

    Each case plants one defect and expects the allow-list message of its own. A ``run_query`` in a contracted model rewrites a filter table (also seen in the
    digest of the relation) or creates, reads and drops the raw bridge (no trace afterwards). A ``sql_header`` sits on a tagged model outside the feature folder
    or at folder level. The body of a run-hook macro or of a helper it reaches changes. Then come a ``statement`` call block, ``adapter.execute``, an unknown
    config key, ``persist_docs``, a ``run_query`` in a macro reached through another macro, a dbt macro outside the list, Jinja in a YAML test argument, a
    project macro that replaces a dbt context function, SQL text in a seed column type and a column data type, an ``execute`` branch, a ``ref`` fed by
    ``var()``, a filter outside the list and a ``run_query`` in the shell model that builds the raw bridge.
    """
    holds = "dbt/models/features/int_cmi_holds.sql"
    hooks = "dbt/macros/build_marker_hooks.sql"
    seeds_yaml = "dbt/seeds/features/_features__seeds.yml"
    models_yaml = "dbt/models/features/_features__models.yml"
    widen = "create or replace table main.feature_bridge_cost_report_measures as select * from main.feature_bridge_geography_measures"
    rewrite = prepend(holds, "{% if execute %}{% do run_query('" + widen + "') %}{% endif %}\n")
    transient = prepend(
        holds,
        "{% if execute %}{% do run_query('create table main.bridge_measure_control as select * from main.feature_bridge_cost_report_measures "
        "union all select * from main.feature_bridge_geography_measures') %}{% set seen = run_query('select count(*) from main.bridge_measure_control') %}"
        "{% do run_query('drop table main.bridge_measure_control') %}{% endif %}\n",
    )
    header = create(
        "dbt/models/outside/int_probe_header.sql",
        "{{ config(tags=['features'], sql_header=\"" + widen + ';") }}\nselect 1 as one\n',
    )
    features_block = "      +materialized: view\n      +tags: ['features']\n"
    yaml_test = (
        "version: 2\nmodels:\n  - name: int_probe_yaml\n    columns:\n      - name: measure_control\n        data_type: varchar\n"
        "        data_tests:\n          - accepted_values:\n"
        "              values: [\"{{ run_query('create or replace table main.leftover_y as select 1') }}\"]\n"
    )
    return [
        (
            "run_query_rewrites_a_filter_table_in_a_contracted_model_resolved",
            rewrite,
            "model.hai.int_cmi_holds calls run_query()",
        ),
        ("run_query_creates_reads_and_drops_the_raw_bridge_resolved", transient, "model.hai.int_cmi_holds calls run_query()"),
        ("sql_header_on_a_tagged_model_outside_the_folder_resolved", header, "model.hai.int_probe_header has the config key sql_header"),
        (
            "sql_header_on_a_tagged_model_outside_the_folder_is_seen_in_the_call_resolved",
            header,
            "model.hai.int_probe_header calls config() with the key sql_header",
        ),
        (
            "folder_level_sql_header_resolved",
            replace(PROJECT, features_block, features_block + "      +sql_header: 'select 1;'\n"),
            "model.hai.int_hospital_spine has the config key sql_header",
        ),
        (
            "persist_docs_at_folder_level_resolved",
            replace(PROJECT, features_block, features_block + "      +persist_docs:\n        relation: true\n"),
            "model.hai.int_hospital_spine has the config key persist_docs",
        ),
        (
            "hook_macro_body_changed_resolved",
            replace(hooks, "{%- do hai_build_provenance() -%}", "{%- do hai_build_provenance() -%}\n        {%- do run_query('" + widen + "') -%}"),
            "macro.hai.hai_drop_build_marker is reached by a run hook; its code changed",
        ),
        (
            "hook_helper_macro_changed_resolved",
            replace(hooks, "{{ return(['compile', 'debug', 'ls', 'parse']) }}", "{{ return(['compile', 'debug', 'ls', 'parse', 'run']) }}"),
            "macro.hai.hai_marker_untouched_commands is reached by a run hook; its code changed",
        ),
        (
            "hook_macro_added_to_the_hook_closure_resolved",
            both(
                create("dbt/macros/hai_extra_step.sql", "{% macro hai_extra_step() %}{{ return(1) }}{% endmacro %}\n"),
                replace(hooks, "{%- do hai_build_provenance() -%}", "{%- do hai_build_provenance() -%}{%- do hai_extra_step() -%}"),
            ),
            "macro.hai.hai_extra_step is reached by a run hook but is not pinned",
        ),
        (
            "statement_call_block_resolved",
            contracted_probe(
                "int_probe_statement",
                "{% call statement('leftover', fetch_result=False) %}create or replace table main.leftover_s as select 1{% endcall %}\n"
                "select 'x' as measure_control",
            ),
            "model.hai.int_probe_statement uses the Jinja construct CallBlock",
        ),
        (
            "adapter_execute_resolved",
            contracted_probe(
                "int_probe_adapter", "{% do adapter.execute('create or replace table main.leftover_a as select 1') %}\nselect 'x' as measure_control"
            ),
            "model.hai.int_probe_adapter reads adapter.execute",
        ),
        (
            "unknown_config_key_resolved",
            contracted_probe("int_probe_key", "{{ config(my_key='x') }}\nselect 'x' as measure_control"),
            "model.hai.int_probe_key calls config() with the key my_key",
        ),
        (
            "run_query_in_a_macro_reached_through_another_macro_resolved",
            both(
                both(
                    create("dbt/macros/features/probe_outer.sql", "{% macro probe_outer() %}{{ probe_inner() }}{% endmacro %}\n"),
                    create(
                        "dbt/macros/probe_inner.sql",
                        "{% macro probe_inner() %}{% do run_query('create or replace table main.leftover_m as select 1') %}'x'{% endmacro %}\n",
                    ),
                ),
                contracted_probe("int_probe_chain", "select {{ probe_outer() }} as measure_control"),
            ),
            "macro.hai.probe_inner calls run_query()",
        ),
        (
            "dbt_macro_outside_the_list_resolved",
            contracted_probe("int_probe_package", "select cast({{ current_timestamp() }} as varchar) as measure_control"),
            "macro.dbt.current_timestamp is a dbt or package macro that the feature phase may not reach",
        ),
        (
            "run_query_in_a_yaml_test_argument_resolved",
            both(contracted_probe("int_probe_yaml", "select 'x' as measure_control"), create("dbt/models/features/_int_probe_yaml.yml", yaml_test)),
            "that calls run_query()",
        ),
        (
            "macro_named_like_a_context_function_resolved",
            create("dbt/macros/local_md5.sql", "{% macro local_md5(value) %}{{ return(value) }}{% endmacro %}\n"),
            "macro.hai.local_md5 has the name of the dbt context function local_md5()",
        ),
        (
            "seed_column_type_with_sql_resolved",
            replace(seeds_yaml, "        measure_control: varchar\n", '        measure_control: "varchar); drop table x; select (1"\n'),
            "has the config key column_types",
        ),
        (
            "column_data_type_with_sql_resolved",
            replace(models_yaml, "data_type: varchar", 'data_type: "varchar) ; select (1"'),
            "that is no plain type text",
        ),
        (
            "filter_outside_the_list_resolved",
            contracted_probe("int_probe_upper", "select '{{ 'x' | upper }}' as measure_control"),
            "model.hai.int_probe_upper uses the filter upper",
        ),
        (
            "execute_branch_resolved",
            contracted_probe("int_probe_execute", "{% if execute %}select 'x' as measure_control{% else %}select 'y' as measure_control{% endif %}"),
            "model.hai.int_probe_execute uses the name execute",
        ),
        (
            "ref_argument_from_var_resolved",
            contracted_probe("int_probe_var", "select measure_control from {{ ref(var('rating_table', 'feature_bridge_geography_measures')) }}"),
            "model.hai.int_probe_var calls var()",
        ),
        (
            "run_query_in_the_shell_model_resolved",
            prepend("dbt/models/gold/dimensions/bridge_measure_control.sql", "{% do run_query('select 1') %}\n"),
            "model.hai.bridge_measure_control calls run_query()",
        ),
    ]


def hook_cases() -> list[tuple[str, Damage, str | None]]:
    """Hooks that the admission check rejects (they were found after the build until round 7).

    They are a post-hook on a filter model, a folder-level post-hook, a pre-hook on a probe model that would recreate ``bridge_measure_control``, a post-hook on
    every seed and a pre-hook on the gold model that builds the raw bridge.
    """
    hook = "insert into {{ this }} select * from {{ ref('bridge_measure_control') }}"
    recreate = probe(
        "select cast(count(*) as varchar) as measure_control from query_table('bridge_measure_control')",
        ', pre_hook="create table bridge_measure_control as select * from feature_bridge_geography_measures"',
    )
    return [
        (
            "filter_model_post_hook_resolved",
            replace(GEOGRAPHY_FILTER, "{{ config(materialized='table') }}", "{{ config(materialized='table', post_hook=\"" + hook + '") }}'),
            "model.hai.feature_bridge_geography_measures has the config key post-hook",
        ),
        (
            "folder_level_post_hook_resolved",
            replace(
                PROJECT,
                "      +materialized: view\n      +tags: ['features']\n",
                "      +materialized: view\n      +tags: ['features']\n      +post-hook: 'select 1'\n",
            ),
            "model.hai.int_hospital_spine has the config key post-hook",
        ),
        ("stage_two_pre_hook_resolved", recreate, "model.hai.int_probe has the config key pre-hook"),
        (
            "seed_post_hook_resolved",
            replace(PROJECT, "seeds:\n  hai:\n", "seeds:\n  hai:\n    +post-hook: 'select 1'\n"),
            "has the config key post-hook",
        ),
        (
            "gold_model_pre_hook_resolved",
            prepend("dbt/models/gold/dimensions/bridge_measure_control.sql", "{{ config(pre_hook='select 1') }}\n"),
            "model.hai.bridge_measure_control has the config key pre-hook",
        ),
    ]


def input_cases() -> list[tuple[str, Damage, str | None]]:
    """Round 7: a model or test reads a filter table of another source, which the list of pinned inputs does not allow.

    The registry window key gets a count of the cost-report filter (the reviewer's probe), through ``ref()`` (admission fails), by relation name and through
    ``query_table`` (admission cannot see it; the bind on the pinned relations fails). The same for a singular test. A new model has no entry, an entry has no
    node, and a generic test may read only the node it tests.
    """
    registry = "dbt/models/features/int_registry_measure_windows.sql"
    key = "windows.window_end as registry_window_key"
    foreign = "feature_bridge_cost_report_measures"
    test = "dbt/tests/features/assert_hospital_control_identity.sql"
    guard = "where exists (select 1 from {{ ref('int_spine_hospital_measures') }})"
    inputs = "dbt/models/features/_feature_inputs.json"
    listed = "which dbt/models/features/_feature_inputs.json does not list for it"
    bind_model = "the compiled query of model.hai.int_registry_measure_windows does not bind on a database that holds only what the feature phase may read"
    bind_test = "the compiled query of test.hai.assert_hospital_control_identity does not bind on a database that holds only what the feature phase may read"

    def counted(source: str) -> str:
        return "windows.window_end || ':' || (select count(*) from <source>)::varchar as registry_window_key".replace("<source>", source)

    def also_counts(source: str) -> str:
        return guard + " and (select count(*) from <source>) >= 0".replace("<source>", source)

    generic = both(
        create(
            "dbt/macros/features/reads_other_model_generic_test.sql",
            "{% test reads_other_model(model, column_name) %}select * from {{ model }} "
            "where (select count(*) from {{ ref('fct_cmi_observation') }}) < 0{% endtest %}\n",
        ),
        replace(
            "dbt/seeds/features/_features__seeds.yml",
            "      - name: measure_control\n        description: The registry measure control the rule belongs to.\n",
            "      - name: measure_control\n        description: The registry measure control the rule belongs to.\n"
            "        data_tests:\n          - reads_other_model\n",
        ),
    )
    return [
        (
            "registry_window_key_reads_another_sources_filter_through_ref_resolved",
            replace(registry, key, counted("{{ ref('" + foreign + "') }}")),
            f"model.hai.int_registry_measure_windows reads {foreign}, {listed}",
        ),
        (
            "model_reads_another_sources_filter_by_name_resolved",
            replace(registry, key, counted(f"main.{foreign}")),
            bind_model,
        ),
        (
            "model_reads_another_sources_filter_by_query_table_resolved",
            replace(registry, key, counted(f"query_table('{foreign}')")),
            bind_model,
        ),
        (
            "test_reads_another_sources_filter_through_ref_resolved",
            replace(test, guard, also_counts("{{ ref('" + foreign + "') }}")),
            f"test.hai.assert_hospital_control_identity reads {foreign}, {listed}",
        ),
        (
            "test_reads_another_sources_filter_by_name_resolved",
            replace(test, guard, also_counts("main." + foreign)),
            bind_test,
        ),
        (
            "model_without_a_pinned_entry_resolved",
            contracted_probe("int_probe_unpinned", "select 'x' as measure_control"),
            "model.hai.int_probe_unpinned has no entry in dbt/models/features/_feature_inputs.json",
        ),
        (
            "pinned_entry_without_a_node_resolved",
            replace(
                inputs, '  "int_cmi_holds": ["fct_cmi_observation"],', '  "int_cmi_holds": ["fct_cmi_observation"],\n  "int_gone": ["fct_cmi_observation"],'
            ),
            "pins model int_gone, which is not a selected feature model",
        ),
        ("generic_test_reads_another_model_resolved", generic, "reads fct_cmi_observation, " + listed),
        (
            "pinned_inputs_removed_for_a_model_resolved",
            replace(inputs, '"int_cmi_holds": ["fct_cmi_observation"]', '"int_cmi_holds": []'),
            "int_cmi_holds reads fct_cmi_observation",
        ),
    ]


def stage_two_fails(name: str) -> str:
    """The message of a feature model that does not build in stage 2, where the raw bridge does not exist."""
    return f"model.hai.{name} did not build on a database that holds only what the feature phase may read"


def stage_two_test_fails(name: str) -> str:
    """The message of a feature test whose compiled query does not bind in stage 2."""
    return f"the compiled query of test.hai.{name} does not bind on a database that holds only what the feature phase may read"


def filled(text: str) -> str:
    """The query text with ``<raw>`` read as a hidden read of the raw bridge and ``<filter>`` as a read of the registry filter model."""
    return text.replace("<raw>", "query_table('BRIDGE_MEASURE_CONTROL')").replace("<filter>", "{{ ref('feature_bridge_registry_measure_sources') }}")


def raw_probe(name: str, query: str) -> Damage:
    """A contracted feature model ``name`` that runs ``query`` with its markers filled."""
    return contracted_probe(name, filled(query))


def bypass_cases() -> list[tuple[str, Damage, str | None]]:
    """Every way a read of the raw ``bridge_measure_control`` was hidden from the guards of rounds 1 to 4; each must fail as a stage-2 build or bind error.

    The raw bridge exists only in stage 1, so none of these needs the filter models to exist: the read fails whatever its spelling (a ``ref``, ``query_table``
    in any case or built at run time, a ``var()``, a macro, an adapter relation, an aggregate that the planner rewrites to a column-data scan, an alias equal
    to the bridge name, strings and comments, a filtered read, a read beside a filtered upstream model, in a model or in a singular test).
    """
    probe_macro = "dbt/macros/features/raw_bridge_probe.sql"
    test_path = "dbt/tests/features/assert_probe_raw_bridge.sql"
    fails, test_fails = stage_two_fails("int_probe_raw"), stage_two_test_fails("assert_probe_raw_bridge")
    count = "select cast(count(*) as varchar) as measure_control from "
    upstream = "select windows.measure_control from {{ ref('int_registry_measure_windows') }} as windows\ncross join <raw> as bridge"
    return [
        (
            "raw_bridge_ref_in_a_reader_resolved",
            replace(HAI_OUTCOME_MODEL, "{{ ref('feature_bridge_hai_outcome_measures') }} as bridge", "{{ ref('bridge_measure_control') }} as bridge"),
            stage_two_fails(Path(HAI_OUTCOME_MODEL).stem),
        ),
        (
            "raw_bridge_ref_in_a_test_resolved",
            create(test_path, "select measure_control from {{ ref('bridge_measure_control') }} limit 0\n"),
            test_fails,
        ),
        ("raw_bridge_count_star_over_query_table_resolved", raw_probe("int_probe_raw", count + "<raw>"), fails),
        ("raw_bridge_count_star_over_ref_resolved", raw_probe("int_probe_raw", count + "{{ ref('bridge_measure_control') }}"), fails),
        (
            "raw_bridge_count_star_in_a_macro_resolved",
            both(
                create(probe_macro, "{% macro raw_bridge_count() %}" + filled(count + "<raw>") + "{% endmacro %}\n"),
                contracted_probe("int_probe_raw", "{{ raw_bridge_count() }}"),
            ),
            fails,
        ),
        ("raw_bridge_count_star_in_a_test_resolved", create(test_path, filled(count + "<raw>") + "\n"), test_fails),
        (
            "raw_bridge_ref_in_a_macro_resolved",
            both(
                create(probe_macro, "{% macro raw_bridge() %}{{ ref('bridge_measure_control') }}{% endmacro %}\n"),
                contracted_probe("int_probe_raw", "select measure_control from {{ raw_bridge() }}"),
            ),
            fails,
        ),
        (
            "raw_bridge_with_a_source_seed_filter_resolved",
            raw_probe("int_probe_raw", "select measure_control from <raw> where source_seed = 'registry_measure_sources'"),
            fails,
        ),
        (
            "raw_bridge_joined_name_resolved",
            contracted_probe("int_probe_raw", "select measure_control from query_table('bridge_' || 'measure_control')"),
            fails,
        ),
        (
            "raw_bridge_name_from_var_resolved",
            contracted_probe("int_probe_raw", 'select measure_control from query_table(\'{{ var("bridge_table", "bridge_measure_control") }}\')'),
            fails,
        ),
        (
            "raw_bridge_adapter_relation_resolved",
            contracted_probe("int_probe_raw", "select measure_control from {{ adapter.Relation.create(schema='MAIN', identifier='BRIDGE_MEASURE_CONTROL') }}"),
            fails,
        ),
        ("raw_bridge_qualified_name_resolved", contracted_probe("int_probe_raw", 'select measure_control from "main"."BRIDGE_MEASURE_CONTROL"'), fails),
        (
            "raw_bridge_alias_equal_to_the_name_resolved",
            raw_probe("int_probe_raw", "select measure_control from <raw> as bridge_measure_control where source_seed = 'registry_measure_sources'"),
            fails,
        ),
        ("raw_bridge_alias_ending_in_the_name_resolved", raw_probe("int_probe_raw", "select measure_control from <raw> as old_bridge_measure_control"), fails),
        (
            "raw_bridge_name_in_a_string_resolved",
            raw_probe("int_probe_raw", "select measure_control from <raw> where measure_control <> 'bridge_measure_control'"),
            fails,
        ),
        (
            "raw_bridge_dashes_in_a_string_resolved",
            raw_probe("int_probe_raw", "select '--' || measure_control as measure_control from <raw> where source_seed = 'registry_measure_sources'"),
            fails,
        ),
        (
            "raw_bridge_name_in_a_comment_resolved",
            raw_probe("int_probe_raw", "-- reads bridge_measure_control filtered by source_seed\nselect measure_control from <raw>"),
            fails,
        ),
        ("raw_bridge_in_a_subquery_resolved", raw_probe("int_probe_raw", "with hidden as (select * from <raw>) select measure_control from hidden"), fails),
        ("raw_bridge_beside_a_filtered_upstream_model_resolved", raw_probe("int_probe_raw", upstream), fails),
        ("raw_bridge_in_a_test_beside_a_filtered_upstream_model_resolved", create(test_path, filled(upstream) + "\n"), test_fails),
        (
            "raw_bridge_beside_a_filter_model_read_resolved",
            raw_probe("int_probe_raw", "select measure_control from <filter>\nunion all\nselect measure_control from <raw>"),
            fails,
        ),
    ]


def filter_read_cases() -> list[tuple[str, Damage, str | None]]:
    """Reads of a filter model in every form that failed a rule of rounds 2 to 4 for a wrong reason; each must pass."""
    test_path = "dbt/tests/features/assert_probe_filter.sql"
    return [
        ("filter_model_read_passes", raw_probe("int_probe_filter", "select measure_control from <filter>"), None),
        (
            "filter_model_with_an_alias_read_by_a_model_passes",
            both(
                raw_probe("int_probe_filter", "{{ config(alias='review_actual_relation') }}\nselect measure_control from <filter>"),
                contracted_probe("int_probe_filter_reader", "select measure_control from {{ ref('int_probe_filter') }}"),
            ),
            None,
        ),
        (
            "filter_model_read_with_dashes_in_a_string_passes",
            raw_probe("int_probe_filter", "select '--' || measure_control as measure_control from <filter> where source_seed <> '--'"),
            None,
        ),
        (
            "filter_model_read_with_the_bridge_name_as_alias_passes",
            raw_probe("int_probe_filter", "select measure_control from <filter> as bridge_measure_control"),
            None,
        ),
        ("filter_model_count_star_passes", raw_probe("int_probe_filter", "select cast(count(*) as varchar) as measure_control from <filter>"), None),
        ("filter_model_read_in_a_test_passes", create(test_path, filled("select measure_control from <filter> where measure_control is null\n")), None),
    ]


DIRECT_READ_MESSAGES = frozenset(
    {
        "the compiled query of model.hai.int_registry_measure_windows does not bind on a database that holds only what the feature phase may read",
        "the compiled query of test.hai.assert_hospital_control_identity does not bind on a database that holds only what the feature phase may read",
    }
)


def admission_cases() -> list[tuple[str, Damage, str | None]]:
    """The cases of the whole resolved check: the committed project builds, and every other case is stopped before anything builds.

    The two direct-read cases (a relation name or ``query_table`` that admission cannot see) are the exception: they are admitted, built and then fail to bind.
    """
    return [("committed_files_admitted", lambda root: None, None), *allowlist_cases(), *hook_cases(), *input_cases()]


ADMISSION_STARTS = frozenset({"dbt parse", "dbt ls tag:features", "feature_allowlist"})


def run_resolved(damage: Damage, base: Path, admitted: bool = True) -> tuple[list[str], list[str]]:
    """Plant the defect in a copy of the dbt project and return the problems and the labels of the process starts.

    ``admitted`` runs the whole resolved check (admission, then the build when admission finds nothing). Otherwise only the build and bind layer
    (``built_check``) runs, to show that the second layer catches what it was built for without help from admission.
    """
    with tempfile.TemporaryDirectory(prefix="feature_sources_resolved_") as folder:
        scratch = Path(folder)
        shutil.copytree(sources.REPO_ROOT / "dbt", scratch / "dbt", ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"))
        damage(scratch)
        shell.STARTED_STEPS.clear()
        problems, _ = (sources.resolved_check if admitted else sources.built_check)(scratch, base)
        return problems, list(shell.STARTED_STEPS)


def case_names() -> list[str]:
    """List every case in execution order without building a database or writing files."""
    return (
        [name for name, _, _ in [*admission_cases(), *resolved_cases()]]
        + ["all_feature_views_have_checksums", "unreadable_relation_fails_after", "unreadable_relation_fails_before"]
        + [name for name, _, _ in [*cases(), *pass_cases()]]
    )


def run(output: Path, only: list[str] | None = None, shard: str = "") -> dict[str, Any]:
    """Run the selected cases on fresh scratch copies and persist exact shard membership."""
    inventory = case_names()
    selected = feature_shards.select(inventory, shard, only)
    before = feature_shards.snapshot("sources")
    results: list[dict[str, Any]] = []
    needs_base = {name for name, _, _ in [*admission_cases(), *resolved_cases()]} | {"all_feature_views_have_checksums"}
    if needs_base.intersection(selected):
        with tempfile.TemporaryDirectory(prefix="feature_sources_base_") as base_folder:
            base = shell.build_shell(sources.REPO_ROOT, Path(base_folder))
            results.extend(resolved_results(base, selected))
            results.extend(checksum_results(base, selected))
    else:
        results.extend(checksum_results(Path(), selected))
    for name, damage, expected in [*cases(), *pass_cases()]:
        if name not in selected:
            continue
        with feature_shards.timed_case("sources", name) as started:
            with tempfile.TemporaryDirectory(prefix="feature_sources_") as folder:
                scratch = Path(folder)
                copy_scratch(scratch)
                damage(scratch)
                problems, counts = sources.check(scratch)
            if expected is None:
                results.append({"case": name, "expected": "no problem", "problems": problems[:3], "counts": counts, "ok": not problems})
            else:
                matched = [problem for problem in problems if expected in problem]
                results.append({"case": name, "expected": expected, "matched": matched[:2], "problems": len(problems), "ok": bool(matched)})
            results[-1]["seconds"] = round(time.monotonic() - started, 3)
    report: dict[str, Any] = {
        "written_utc": datetime.now(UTC).isoformat(),
        "mode": "iteration" if only else "full",
        "note": "not commit evidence" if only else "complete assigned shard",
        "committed_counts": results[0].get("counts"),
        "cases": results,
        "status": "pass" if all(result["ok"] for result in results) else "fail",
    }
    return feature_shards.finish(output, "sources", shard, inventory, selected, before, report)


# One synthetic hospital-window in the spine, so that the views over it have rows to checksum.
SPINE_ROW = (
    "insert into main.int_hospital_spine (ccn, window_year, window_start, state_code, zip_code, county_fips, county_source, spine_key,"
    "is_primary_population,is_sensitivity_population,is_connecticut,is_maryland,has_pos_snapshot,is_cmi_held) values "
    "('010001',2022,date '2022-01-01','AL','35004','01001','pos','010001:2022',true,true,false,false,true,false)"
)
INSERT_PROGRAM = "import duckdb,sys;c=duckdb.connect(sys.argv[1]);c.execute(sys.argv[2]);c.close()"
CHECKSUM = re.compile(r"\d+ rows, md5 ([0-9a-f]{32})?")


def checksum_results(base: Path, only: list[str] | None = None) -> list[dict[str, Any]]:
    """Round 7, finding 3: the committed feature models, views included, have a row count and md5 after the build; an unreadable relation is a problem."""
    results: list[dict[str, Any]] = []
    wanted = "all_feature_views_have_checksums"
    if not only or wanted in only:
        with feature_shards.timed_case("sources", wanted) as started:
            with tempfile.TemporaryDirectory(prefix="feature_sources_checksums_") as folder:
                run = shell.run_features(sources.REPO_ROOT, Path(folder), base, contract=True)
                shell.step(
                    sources.REPO_ROOT / ".tools/sqlfluff/bin/python",
                    ["-c", INSERT_PROGRAM, str(run.database), SPINE_ROW],
                    sources.REPO_ROOT,
                    shell.tool_environment(sources.REPO_ROOT),
                    "insert one synthetic hospital-window",
                )
                after = shell.read_relations(run.database)
            built = {name: digest for name, digest in after.items() if name not in run.relations}
            folder_models = sources.REPO_ROOT / "dbt/models/features"
            tables = sum("materialized='table'" in path.read_text() for path in folder_models.glob("*.sql"))
            real = {name for name, digest in built.items() if CHECKSUM.fullmatch(digest)}
            with_rows = sorted(name for name, digest in built.items() if (found := CHECKSUM.fullmatch(digest)) and found.group(1))
            results.append(
                {
                    "case": wanted,
                    "expected": f"{len(built)} built relations, {len(built) - tables} views and {tables} tables, each readable (the md5 is empty without rows)",
                    "unreadable": sorted(set(built) - real),
                    "with_rows_and_an_md5": with_rows,
                    "ok": len(built) == 31 and tables == 4 and real == set(built) and bool(with_rows),
                }
            )
            results[-1]["seconds"] = round(time.monotonic() - started, 3)
    wanted = "unreadable_relation_fails"
    if not only or any(name.startswith(wanted + "_") for name in only):
        message = "which cannot be read to compare its rows"
        for side, before, after_state in (
            ("after", {"main.x": "1 rows, md5 a"}, {"main.x": shell.UNREADABLE + "boom"}),
            ("before", {"main.x": shell.UNREADABLE + "boom"}, {"main.x": "1 rows, md5 a"}),
        ):
            if only and f"{wanted}_{side}" not in only:
                continue
            with feature_shards.timed_case("sources", f"{wanted}_{side}") as started:
                problems = sources.relation_problems({}, before, after_state, {})
                results.append(
                    {"case": f"{wanted}_{side}", "expected": message, "matched": problems[:1], "ok": any(message in problem for problem in problems)}
                )
                results[-1]["seconds"] = round(time.monotonic() - started, 3)
    return results


def judge(name: str, expected: str | None, problems: list[str], starts: list[str], rejected: bool) -> dict[str, Any]:
    """The result of one resolved case. A case with ``rejected`` must also show that the rejected code never ran: it started only the admission's steps."""
    ran = sorted(set(starts) - ADMISSION_STARTS)
    if expected is None:
        return {"case": name, "expected": "no problem", "problems": problems[:3], "process_starts": sorted(set(starts)), "ok": not problems}
    matched = [problem for problem in problems if expected in problem]
    result: dict[str, Any] = {"case": name, "expected": expected, "matched": matched[:2], "problems": len(problems), "ok": bool(matched)}
    if rejected:
        result.update({"process_starts": sorted(set(starts)), "started_beyond_admission": ran})
        result["ok"] = bool(matched) and not ran
    return result


def resolved_results(base: Path, only: list[str] | None = None) -> list[dict[str, Any]]:
    """Run each resolved case (or only those whose name contains one of ``only``) on a copy of the restricted database ``base``; one result each.

    The admission cases (``admission_cases``) run the whole resolved check and must be stopped by admission: the process starts show no dbt run, compile or seed
    and no worker. The first of them is the committed project, which must pass and build. The build cases run ``built_check`` alone.
    """
    results: list[dict[str, Any]] = []
    for name, damage, expected in admission_cases():
        if only and name not in only:
            continue
        with feature_shards.timed_case("sources", name) as started:
            problems, starts = run_resolved(damage, base)
            built = {"dbt run tag:features", "dbt compile tag:features"} <= set(starts)
            result = judge(name, expected, problems, starts, rejected=expected is not None and expected not in DIRECT_READ_MESSAGES)
            if expected is None:
                result["ok"] = result["ok"] and built
            result["seconds"] = round(time.monotonic() - started, 3)
        results.append(result)
    for name, damage, expected in resolved_cases():
        if only and name not in only:
            continue
        with feature_shards.timed_case("sources", name) as started:
            problems, starts = run_resolved(damage, base, admitted=False)
            results.append(judge(name, expected, problems, starts, rejected=False))
            results[-1]["seconds"] = round(time.monotonic() - started, 3)
    return results


def main() -> int:
    """Run the cases, write the artifact and fail when a case does not behave as expected."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output-directory", type=Path, default=None)
    parser.add_argument("--only", action="append", help="run only the cases whose name contains this text (repeatable); the default runs all of them")
    parser.add_argument("--shard", default="", help="run every Nth case as K/N, with 1 <= K <= N")
    parser.add_argument("--list-cases", action="store_true", help="print the selected case names as JSON without running them")
    args = parser.parse_args()
    try:
        selected = feature_shards.select(case_names(), args.shard, args.only)
    except ValueError as error:
        parser.error(str(error))
    if args.list_cases:
        sys.stdout.write(json.dumps(selected) + "\n")
        return 0
    if args.output_directory is None:
        label = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + ("_shard" + args.shard.replace("/", "of") if args.shard else "")
        args.output_directory = sources.REPO_ROOT / "data/e2e/feature_sources" / label
    report = run(args.output_directory, args.only, args.shard)
    failed = [result["case"] for result in report["cases"] if not result["ok"]]
    sys.stdout.write(f"feature sources e2e: {report['status']}; artifact {args.output_directory / 'report.json'}\n")
    if failed:
        sys.stderr.write(f"feature sources e2e: failed cases {failed}\n")
    return int(report["status"] != "pass")


if __name__ == "__main__":
    raise SystemExit(main())
