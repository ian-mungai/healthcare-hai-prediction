"""Synthetic dbt projects and property blocks for the Public label guard cases of run_model_metadata_e2e.

Each manifest fixture hides one Confidential label where raw YAML cannot show its effect (SQL config, templated values, versions, test metadata and the
resource sections that the project walk once omitted). Each section fixture holds the same label in a raw property block.
"""

from __future__ import annotations

from textwrap import dedent

LABEL = "classification: Confidential"
PROJECT_FILE = "name: hai\nversion: '1.0'\nconfig-version: 2\nprofile: hai\nmodel-paths: [models]\nseed-paths: [seeds]\n"
FIXTURE_PROJECT = {
    "dbt_project.yml": PROJECT_FILE,
    "profiles.yml": "hai:\n  target: lint\n  outputs:\n    lint:\n      type: duckdb\n      path: ':memory:'\n      threads: 1\n",
    "models/base.sql": "select 1 as id, 'a' as value\n",
}
UNIT_TEST_BODY = "    model: base\n    given: []\n    expect:\n      rows:\n        - {id: 1, value: a}\n"
FUTURE_UNIT_TEST = f"version: 2\nunit_tests:\n  - name: future_ut\n    config:\n      meta:\n        {LABEL}\n{UNIT_TEST_BODY}"
SEMANTIC = dedent(
    """\
    semantic_models:
      - name: sem
        model: ref('base')
        defaults:
          agg_time_dimension: created_day
        entities:
          - name: id
            type: primary
        dimensions:
          - name: created_day
            type: time
            type_params:
              time_granularity: day
        measures:
          - name: rows
            agg: count
            expr: id
    """
)
METRIC = "metrics:\n  - name: row_count\n    label: Rows\n    type: simple\n    type_params:\n      measure: rows\n"
SPINE = {
    "models__spine_dot_sql": "select cast('2020-01-01' as date) as date_day\n",
    "models__spine_dot_yml": (
        "version: 2\nmodels:\n  - name: spine\n    time_spine:\n      standard_granularity_column: date_day\n"
        "    columns:\n      - name: date_day\n        granularity: day\n"
    ),
}
CONFIG_META = f"    config:\n      meta:\n        {LABEL}\n"


def yaml(text: str) -> str:
    """Dedent an indented YAML block and end it with one newline."""
    return dedent(text).strip("\n") + "\n"


def project(**files: str) -> dict[str, str]:
    """The base fixture project plus files; a keyword name maps double underscores to folders and ``_dot_`` to a dot."""
    return {**FIXTURE_PROJECT, **{name.replace("__", "/").replace("_dot_", "."): text for name, text in files.items()}}


def manifest_fixtures() -> list[tuple[str, dict[str, str], str]]:
    """Name, files and the manifest entry the check must name (empty when the project is clean)."""
    versions = {"models__v_v1_dot_sql": "select 1 as id\n", "models__v_v2_dot_sql": "select 1 as id\n"}
    base = "version: 2\nmodels:\n  - name: base\n"
    return [
        ("manifest_clean_control", project(models__base_dot_yml=f"{base}    meta:\n      owner: lakehouse\n"), ""),
        ("manifest_sql_config_meta", project(models__base_dot_sql="{{ config(meta={'classification': 'Confidential'}) }}\nselect 1 as id\n"), "model.hai.base"),
        ("manifest_templated_value", project(models__base_dot_yml=f"{base}    meta:\n      classification: \"{{{{ 'Confidential' }}}}\"\n"), "model.hai.base"),
        (
            "manifest_version_config_meta",
            project(
                **versions,
                models__v_dot_yml=yaml(
                    f"""
                    version: 2
                    models:
                      - name: v
                        latest_version: 2
                        versions:
                          - v: 1
                            config:
                              meta:
                                {LABEL}
                          - v: 2
                    """
                ),
            ),
            "model.hai.v.v1",
        ),
        (
            "manifest_version_column_meta",
            project(
                **versions,
                models__v_dot_yml=yaml(
                    f"""
                    version: 2
                    models:
                      - name: v
                        latest_version: 2
                        columns:
                          - name: id
                        versions:
                          - v: 1
                            columns:
                              - include: all
                              - name: id
                                meta:
                                  {LABEL}
                          - v: 2
                    """
                ),
            ),
            "model.hai.v.v1",
        ),
        (
            "manifest_test_meta",
            project(
                models__base_dot_yml=yaml(
                    f"""
                    version: 2
                    models:
                      - name: base
                        columns:
                          - name: id
                            data_tests:
                              - not_null:
                                  config:
                                    meta:
                                      {LABEL}
                    """
                )
            ),
            "test.hai.not_null_base_id",
        ),
        ("manifest_column_meta", project(models__base_dot_yml=f"{base}    columns:\n      - name: id\n        meta:\n          {LABEL}\n"), "model.hai.base"),
        (
            "manifest_seed_config_meta",
            project(seeds__ref_dot_csv="a\n1\n", seeds__ref_dot_yml=f"version: 2\nseeds:\n  - name: ref\n{CONFIG_META}"),
            "seed.hai.ref",
        ),
        (
            "manifest_source_column_meta",
            project(
                models__src_dot_yml=yaml(
                    f"""
                    version: 2
                    sources:
                      - name: raw
                        tables:
                          - name: t
                            columns:
                              - name: c
                                meta:
                                  {LABEL}
                    """
                )
            ),
            "source.hai.raw.t",
        ),
        (
            "manifest_unit_test_meta",
            project(models__base_dot_yml=f"{base}unit_tests:\n  - name: ut\n{CONFIG_META}{UNIT_TEST_BODY}"),
            "unit_test.hai.base.ut",
        ),
        (
            "manifest_project_unit_test_meta",
            {
                **project(models__base_dot_yml=f"{base}unit_tests:\n  - name: ut\n{UNIT_TEST_BODY}"),
                "dbt_project.yml": f"{PROJECT_FILE}unit_tests:\n  hai:\n    +meta:\n      {LABEL}\n",
            },
            "unit_test.hai.base.ut",
        ),
        (
            "manifest_exposure_meta",
            project(
                models__exp_dot_yml=yaml(
                    f"""
                    version: 2
                    exposures:
                      - name: dash
                        type: dashboard
                        owner:
                          email: a@example.invalid
                        depends_on: [ref('base')]
                        meta:
                          {LABEL}
                    """
                )
            ),
            "exposure.hai.dash",
        ),
        ("manifest_semantic_model_meta", project(**SPINE, models__sem_dot_yml=f"version: 2\n{SEMANTIC}{CONFIG_META}"), "semantic_model.hai.sem"),
        ("manifest_metric_meta", project(**SPINE, models__sem_dot_yml=f"version: 2\n{SEMANTIC}{METRIC}{CONFIG_META}"), "metric.hai.row_count"),
        (
            "manifest_saved_query_meta",
            project(
                **SPINE,
                models__sem_dot_yml=(
                    f"version: 2\n{SEMANTIC}{METRIC}saved_queries:\n  - name: sq\n    query_params:\n      metrics: [row_count]\n{CONFIG_META}"
                ),
            ),
            "saved_query.hai.sq",
        ),
    ]


def future_entry_projects() -> tuple[dict[str, str], dict[str, str]]:
    """A project whose only Confidential entry is a named future entry, and the same project with a research model labeled Confidential as well."""
    labeled = "{{ config(meta={'classification': 'Confidential', 'confidential_columns': ['id']}) }}\nselect 1 as id\n"
    future = {**FIXTURE_PROJECT, "models/future_entry.sql": labeled}
    return future, {**future, "models/base.sql": "{{ config(meta={'classification': 'Confidential'}) }}\nselect 1 as id\n"}


SECTIONS = {
    "unit_tests": f"version: 2\nunit_tests:\n  - name: ut\n{CONFIG_META}{UNIT_TEST_BODY}",
    "metrics": f"version: 2\nmetrics:\n  - name: m\n{CONFIG_META}",
    "semantic_models": f"version: 2\nsemantic_models:\n  - name: sem\n    meta:\n      {LABEL}\n",
    "saved_queries": f"version: 2\nsaved_queries:\n  - name: sq\n{CONFIG_META}",
    "exposures": f"version: 2\nexposures:\n  - name: dash\n    meta:\n      {LABEL}\n",
    "version_config": f"version: 2\nmodels:\n  - name: v\n    versions:\n      - v: 1\n        config:\n          meta:\n            {LABEL}\n",
    "version_column": (
        f"version: 2\nmodels:\n  - name: v\n    versions:\n      - v: 1\n        columns:\n          - name: id\n            meta:\n              {LABEL}\n"
    ),
    "test": yaml(
        f"""
        version: 2
        models:
          - name: base
            columns:
              - name: id
                data_tests:
                  - not_null:
                      config:
                        meta:
                          {LABEL}
        """
    ),
}
PROJECT_KEYS = ("unit_tests", "exposures", "metrics", "semantic-models", "saved-queries", "seeds", "snapshots", "sources", "data_tests")


def project_sections() -> dict[str, str]:
    """A dbt_project.yml per resource section, each with a Confidential +meta."""
    return {key: f"name: hai\n{key}:\n  hai:\n    +meta:\n      {LABEL}\n" for key in PROJECT_KEYS}
