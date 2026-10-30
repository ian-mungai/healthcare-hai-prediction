"""Exercise the gold expression boundary with synthetic SQL and render every delivered gold model.

Run from the repository root with ``.venv/bin/python -m scripts.lakehouse.run_gold_sql_e2e --out plans/wp/<record>.json``.
The local DuckDB process parses text in an empty in-memory database; it reads no source data and starts no containers.
The separate gold fixture runner remains the integrated build verification.
"""

from __future__ import annotations

import argparse
import ast
import copy
import hashlib
import importlib.abc
import importlib.machinery
import json
import re
import sys
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from string import Template
from types import ModuleType, SimpleNamespace
from typing import Any

from jinja2 import Environment, StrictUndefined, select_autoescape

from scripts.lakehouse import gold_checks as installed_checks
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
gold_checks: Any = installed_checks
A43 = {
    "dim_hospital": ("length", "ccn", 6),
    "dim_county": ("length", "county_fips", 5),
    "dim_state": ("regexp_matches", "state_code", "^[A-Z]{2}$"),
    "dim_zip": ("regexp_matches", "zip_code", "^[0-9]{5}$"),
    "dim_census_tract": ("regexp_matches", "census_tract_geoid", "^[0-9]{11}$"),
    "dim_owner_organization": ("regexp_matches", "associate_id", "^[0-9]{10}$"),
}


@dataclass(frozen=True)
class Probe:
    name: str
    model: str
    sql: str
    failures: tuple[str, ...]
    cause: str = ""
    remove: str = ""
    real_pins: bool = False


def parse(sql: str) -> str:
    """Parse synthetic text with the installed DuckDB CLI; preserve its serializer errors as test inputs."""
    query = "select json_serialize_sql('" + sql.replace("'", "''") + "') as parsed"
    result = run_command("duckdb", [":memory:", "-init", "/dev/null", "-json", "-c", query], timeout=60, check=True)
    value = json.loads(result.stdout)[0]["parsed"]
    return json.dumps(value) if isinstance(value, dict) else str(value)


def render_models() -> dict[str, str]:
    """Render delivered SQL with stand-in main-schema refs; this is a static acceptance check, not dbt compilation."""
    env = Environment(undefined=StrictUndefined, autoescape=select_autoescape(default_for_string=False))
    macro = "".join((ROOT / f"dbt/macros/{name}.sql").read_text().split("{% test ", 1)[0] for name in ("gold_facts", "gold_identity"))
    output: dict[str, str] = {}
    for path in sorted((ROOT / "dbt/models/gold").rglob("*.sql")):
        template = env.from_string(macro + path.read_text())
        output[path.stem] = template.render(ref=lambda name: f'main."{name}"', config=lambda **_kwargs: "")
    return output


def probes(rendered: dict[str, str]) -> list[Probe]:
    """Define the executed review counterexamples and permission controls before the checker implementation."""
    fact = rendered["fct_hai_hospital_window"]
    source = "main.int_hospital_enrollment_rows"
    f18 = """with q as (select s.entity_id as real_entity_id, s.measure_id as entity_id,
        s.measure_id, s.window_start, s.window_end, s.measure_name, s.score, s.footnote,
        s.compared_to_national, s.member_sha256, s.object_key, s.release_date,
        s.release_file_count, s.window_key from main.int_hai_hospital_windows s)
        select s.real_entity_id as entity_id, s.measure_id, s.window_start, s.window_end,
        s.measure_name, s.score, s.footnote, s.compared_to_national, s.member_sha256,
        s.object_key, s.release_date, s.release_file_count, s.window_key,
        coalesce(d0.hospital_sk, md5('<unknown>')) as hospital_sk,
        coalesce(d1.release_sk, md5('<unknown>')) as release_sk
        from q s left join main.dim_hospital d0 on md5(cast(s.entity_id as varchar)) = d0.hospital_sk
        left join main.dim_release d1 on md5(cast(s.member_sha256 as varchar)) = d1.release_sk"""
    f19 = Template("""with q as (select ccn as real_ccn, cast(incorporation_date as varchar) as ccn from $source),
        q2 as (select real_ccn as ccn, ccn as first_seen from q)
        select md5(ccn) as hospital_sk, ccn, first_seen from q2""").substitute(source=source)
    expr = ("expression_functions",)
    cases = [
        Probe("F18_935_wrong_fact_hash_preserved_output", "fct_hai_hospital_window", f18, expr),
        Probe("F19_936_date_cast_under_temporary_ccn", "dim_hospital", f19, expr, "cast"),
        Probe(
            "F1_bogus_key_case",
            "fct_hai_hospital_window",
            "with q as (select case when value > 0 then value else 0 end as bogus_sk from main.int_hai_hospital_windows) select bogus_sk as value from q",
            ("expression_case",),
        ),
        Probe(
            "F1_renamed_arithmetic",
            "dim_hospital",
            "with q as (select value as v from main.int_hai_hospital_windows) select v + 1 as first_seen from q",
            ("expression_arithmetic",),
            "arithmetic",
        ),
        Probe(
            "F1_unrelated_fiscal_cte",
            "dim_date",
            "with fiscal_year_unrelated as (select case when value > 0 then value else 0 end as bogus from main.int_hai_hospital_windows) "
            "select bogus from fiscal_year_unrelated",
            ("expression_case",),
            "case",
        ),
        Probe(
            "F2_string_aggregate",
            "dim_enrollment",
            Template("select string_agg(enrollment_id, ',') as enrollment_id from $source").substitute(source=source),
            expr,
            "aggregate",
        ),
        Probe(
            "F2_window_aggregate",
            "dim_enrollment",
            Template("select min(enrollment_id) over () as enrollment_id from $source").substitute(source=source),
            expr,
            "window",
        ),
        Probe("F2_power_value", "fct_hai_hospital_window", "select power(value, 1) as value from main.int_hai_hospital_windows", expr),
        Probe(
            "F2_empty_allowlist",
            "dim_hospital",
            Template("select md5(case when ccn is null then '<unknown>' else cast(ccn as varchar) end) as hospital_sk, ccn from $source").substitute(
                source=source
            ),
            expr,
            "",
            "all",
        ),
        Probe(
            "F6_literal_exists",
            "fct_hai_hospital_window",
            Template("$fact where exists (select 1 from main.stg_bronze__file_copies b where b.member_sha256 = silver.member_sha256)").substitute(fact=fact),
            ("lineage",),
            "stg_bronze__file_copies",
        ),
        Probe(
            "F9_938_between_hidden_read",
            "fct_hai_hospital_window",
            Template("$fact where power(1, 1) between 0 and (select count(*) + 1 from main.stg_bronze__file_copies)").substitute(fact=fact),
            ("lineage", "expression_arithmetic", "expression_functions"),
        ),
        Probe("F9_table_function", "dim_date", "select * from generate_series(1, cast(power(3, 1) as bigint))", ("lineage", "expression_functions"), "power"),
        Probe(
            "F10_direct_wrong_hospital_key",
            "dim_hospital",
            Template("select md5(enrollment_id) as hospital_sk, ccn from $source").substitute(source=source),
            expr,
            "contracted",
        ),
        Probe(
            "F10_nonkey_guard_case",
            "fct_hai_hospital_window",
            "select md5(case when entity_id is null then '<unknown>' else cast(entity_id as varchar) end) as entity_id from main.int_hai_hospital_windows",
            ("expression_case",),
        ),
        Probe(
            "F10_guard_requires_varchar",
            "dim_hospital",
            Template("select md5(case when ccn is null then '<unknown>' else cast(ccn as varchar) end) as hospital_sk, ccn from $source").substitute(
                source=source
            ),
            expr,
            "",
            "varchar_cast",
        ),
        Probe(
            "F11_nonkey_integer_cast",
            "dim_hospital",
            "select md5(ccn) as hospital_sk, cast(ccn as integer) as changed from main.int_cmi_hospital_rows",
            expr,
            "cast",
        ),
        Probe(
            "F11_year_aggregate",
            "dim_hospital",
            "select md5('<unknown>') as hospital_sk, min(fiscal_year) as first_seen from main.int_cost_reports",
            expr,
            "aggregate",
        ),
        Probe(
            "F11_date_shift",
            "dim_date",
            "select md5(cast(window_start as varchar)) as date_sk, date_add(window_start, interval 10 year) as calendar_date from "
            "main.int_hai_hospital_windows",
            expr,
        ),
        Probe(
            "F11_direct_date_attribute_cast",
            "dim_hospital",
            Template("select md5(ccn) as hospital_sk, ccn, cast(incorporation_date as varchar) as first_seen from $source").substitute(source=source),
            expr,
            "cast",
        ),
        Probe(
            "F13_fact_predicate_functions",
            "fct_hai_hospital_window",
            fact + " where length(silver.entity_id) >= 0 and regexp_matches(silver.entity_id, '^.*$')",
            expr,
        ),
        Probe("F13_fact_like", "fct_hai_hospital_window", fact + " where silver.entity_id like '%'", expr),
        Probe(
            "F16_wrong_key_cte",
            "dim_hospital",
            Template("with q as (select enrollment_id as ccn, ccn as real_ccn from $source) select md5(ccn) as hospital_sk, real_ccn as ccn from q").substitute(
                source=source
            ),
            expr,
            "lineage",
        ),
        Probe(
            "F16_hoisted_cast_capability",
            "dim_hospital",
            Template("with q as (select cast(ccn as varchar) as ccn from $source) select md5(ccn) as hospital_sk, ccn from q").substitute(source=source),
            expr,
            "allowlist",
            "varchar_cast",
        ),
        Probe(
            "F16_where_hash_tautology",
            "fct_hai_hospital_window",
            fact + " where md5(cast(silver.entity_id as varchar)) is null or md5(cast(silver.entity_id as varchar)) is not null",
            expr,
        ),
        Probe(
            "936_normalized_key_leaks_attribute",
            "dim_hospital",
            Template("with q as (select cast(ccn as varchar) as ccn from $source) select md5(ccn) as hospital_sk, ccn, ccn as first_seen from q").substitute(
                source=source
            ),
            expr,
            "role",
        ),
        Probe(
            "937_order_cast_leaks_output",
            "dim_drg",
            "with q as (select cast(data_year as varchar) as label_order from main.int_mup_drg_discharges) select md5('<unknown>') as "
            "drg_sk, label_order as code_label from q",
            expr,
            "role",
        ),
        Probe(
            "938_recursive_cte",
            "fct_hai_hospital_window",
            "with recursive q as (select * from main.int_hai_hospital_windows union all select * from q where false) select * from q",
            expr,
            "not supported",
        ),
        Probe("935_fact_projection_changed", "fct_hai_hospital_window", fact.replace("silver.*", "silver.* exclude(score)"), expr),
        Probe(
            "positive_hoisted_key_cast",
            "dim_hospital",
            Template("with q as (select cast(ccn as varchar) as ccn from $source) select md5(ccn) as hospital_sk, ccn from q").substitute(source=source),
            (),
        ),
        Probe(
            "positive_typed_date_alias",
            "dim_hospital",
            "with q as (select entity_id as ccn, window_start as dt_for_bounds from main.int_hai_hospital_windows) select md5(ccn) as hospital_sk, ccn, "
            "min(dt_for_bounds) as first_seen from q group by ccn",
            (),
        ),
        Probe(
            "positive_independent_fiscal_case",
            "dim_date",
            "select md5(cast(calendar_date as varchar)) as date_sk, calendar_date, case when month(calendar_date) >= 10 then year(calendar_date) + 1 else "
            "year(calendar_date) end as federal_fiscal_year from main.dim_date",
            (),
            "",
            "key_case",
        ),
    ]
    for model, (function, column, argument) in A43.items():
        source_name, source_column = {
            "dim_hospital": ("int_hospital_enrollment_rows", "ccn"),
            "dim_county": ("int_acs_county_values", "county_fips"),
            "dim_state": ("int_cost_reports", "state_code"),
            "dim_zip": ("int_hsa_zip_cases", "zip_code"),
            "dim_census_tract": ("int_ruca_codes", "geography_id"),
            "dim_owner_organization": ("int_hospital_owner_rows", "associate_id"),
        }[model]
        predicate = f"length({column}) = {argument}" if function == "length" else f"regexp_matches({column}, '{argument}')"
        sql = Template("with q as (select $source_column as $column from main.$source) select md5($column) as $key, $column from q").substitute(
            column=column, key=gold_checks.dimension_key(model), source=source_name, source_column=source_column
        )
        cases += [
            Probe(f"A43_positive_{model}", model, sql + " where " + predicate, ()),
            Probe(f"A43_changed_{model}", model, sql + " where " + predicate.replace(str(argument), str(argument) + "1"), expr, function),
        ]
    label_choice = """with published as (select 'synthetic_fact' as fact, cast(measure_id as varchar) as measure_key,
        measure_name, release_date, window_end from main.int_hai_hospital_windows),
        names as (select distinct on (fact, measure_key) fact, measure_key, measure_name from published
        order by fact, measure_key, release_date desc nulls last, window_end desc nulls last, measure_name)
        select md5(concat_ws('|', ids.fact, ids.measure_key, ids.value_column)) as measure_sk,
        ids.fact, ids.measure_key, ids.value_column, names.measure_name as measure_label
        from main.synthetic_identities ids left join names
        on ids.fact = names.fact and ids.measure_key = names.measure_key"""
    cases += [
        Probe("937_positive_A48_normalized_label_key", "dim_measure", label_choice, ()),
        Probe(
            "937_A48_wrong_distinct_role",
            "dim_measure",
            label_choice.replace("distinct on (fact, measure_key)", "distinct on (fact, measure_name)"),
            expr,
            "no approved final role",
        ),
        Probe(
            "937_A48_key_leaks_label",
            "dim_measure",
            label_choice.replace("names.measure_name as measure_label", "names.measure_key as measure_label"),
            expr,
            "role",
        ),
    ]
    return cases


def round2_probes(rendered: dict[str, str]) -> list[Probe]:
    """Round 1 reviewer counterexamples and controls, recorded before the root fixes."""
    expr = ("expression_functions",)
    cases: list[Probe] = []
    hospital = "with q as (select entity_id as ccn, min(window_start) as first_seen from main.int_hai_hospital_windows group by entity_id) "
    release = "with q as (select member_sha256, try_cast(publication_release as date) as release_date from main.stg_bronze__file_copies) "
    normalized = "with q as (select cast(ccn as varchar) as ccn from main.int_hospital_enrollment_rows) "
    for name, value, suffix in [
        ("good_min", "first_seen", ""),
        ("direct_wrong_min_role", "first_seen", ""),
        ("any_value_wrong_min_role", "any_value(first_seen)", " group by ccn"),
        ("nested_max_min", "max(first_seen)", " group by ccn"),
        ("scalar_wrong_min_role", "(select q.first_seen)", ""),
    ]:
        role = "first_seen" if name == "good_min" else "last_seen"
        cases.append(
            Probe(
                "R2_a36_" + name,
                "dim_hospital",
                Template("$prefix select md5(ccn) as hospital_sk, ccn, $value as $role from q $suffix").substitute(
                    prefix=hospital, value=value, role=role, suffix=suffix
                ),
                () if name == "good_min" else expr,
                "" if name == "good_min" else "date_bound:min",
            )
        )
    for name, value, suffix in [
        ("good", "release_date", ""),
        ("direct_wrong_role", "release_date", ""),
        ("any_value_wrong_role", "any_value(release_date)", " group by member_sha256"),
        ("scalar_wrong_role", "(select q.release_date)", ""),
    ]:
        role = "release_date" if name == "good" else "period_start"
        cases.append(
            Probe(
                "R2_a36_release_" + name,
                "dim_release",
                Template("$prefix select md5(member_sha256) as release_sk, member_sha256, $value as $role from q $suffix").substitute(
                    prefix=release, value=value, role=role, suffix=suffix
                ),
                () if name == "good" else expr,
                "" if name == "good" else "release_date",
            )
        )
    cases.append(
        Probe(
            "R2_a36_release_count_distinct",
            "dim_release",
            "select md5(member_sha256) as release_sk, member_sha256, count(distinct object_key) as copy_count "
            "from main.stg_bronze__file_copies group by member_sha256",
            expr,
            "aggregate",
        )
    )
    for name, value in [("direct", "ccn"), ("scalar", "(select q.ccn)")]:
        cases.append(
            Probe(
                "R2_normalization_" + name + "_wrong_role",
                "dim_hospital",
                Template("$prefix select md5(ccn) as hospital_sk, ccn, $value as first_seen from q").substitute(prefix=normalized, value=value),
                expr,
                "forbidden",
            )
        )
    cases.append(
        Probe(
            "R2_normalization_exists_control",
            "dim_hospital",
            Template("$prefix select md5(ccn) as hospital_sk, ccn from q where exists (select 1 where q.ccn is not null)").substitute(prefix=normalized),
            (),
        )
    )
    for name, sql, failures in [
        ("good_key", "select md5(ccn) as hospital_sk, ccn from main.int_hospital_enrollment_rows where length(ccn) = 6", ()),
        ("direct_bad_key", "select md5(ccn) as hospital_sk, ccn from main.int_hospital_enrollment_rows where length(enrollment_id) = 6", expr),
        (
            "aliased_bad_key",
            "with q as (select ccn as real_ccn, enrollment_id as ccn from main.int_hospital_enrollment_rows), "
            "filtered as (select real_ccn as ccn from q where length(ccn) = 6) select md5(ccn) as hospital_sk, ccn from filtered",
            expr,
        ),
        ("having", "select md5(ccn) as hospital_sk, ccn from main.int_hospital_enrollment_rows group by ccn having length(ccn) = 6", expr),
    ]:
        cases.append(Probe("R2_a43_" + name, "dim_hospital", sql, failures, "length" if failures else ""))
    for model in ("dim_drg", "dim_rurality_code", "dim_measure"):
        sql = rendered[model]
        cases.append(Probe("R2_a48_original_" + model, model, sql, ()))
        label = "measure_name" if model == "dim_measure" else "code_label"
        vintage = "release_date" if model == "dim_measure" else "label_order"
        mutations = {
            "earliest": sql.replace(f"{vintage} desc nulls last", f"{vintage} asc nulls last"),
            "wrong_tie": sql.replace(f"{label} asc", f"{label} desc"),
        }
        if model == "dim_measure":
            mutations["missing_order"] = sql.replace(
                "order by fact asc, measure_key asc, release_date desc nulls last, window_end desc nulls last, measure_name asc", ""
            )
        else:
            mutations["extra_distinct"] = sql.replace("distinct on (code_system, code)", "distinct on (code_system, code, code_label)")
            mutations["unrelated_cast_consumption"] = Template("$sql order by (select label_order from observed limit 1) desc nulls last").substitute(
                sql=sql.replace("label_order desc nulls last, ", "")
            )
        for name, changed in mutations.items():
            if changed == sql:
                raise ValueError(f"Round 2 mutation did not change {name}/{model}")
            cases.append(Probe(f"R2_a48_{name}_{model}", model, changed, expr, "A48"))
    for scope, control_name, cause in [
        ("value_space", "R2_a43_good_key", "length"),
        ("normalization_inputs", "positive_hoisted_key_cast", "cast"),
        ("source_date_models", "R2_a36_good_min", "aggregate"),
    ]:
        control = next(case for case in [*cases, *probes(rendered)] if case.name == control_name)
        cases.append(replace(control, name="R2_contract_control_" + scope))
        cases.append(replace(control, name="R2_contract_removed_" + scope, failures=expr, cause=cause, remove="scope:" + scope))
    return cases


def round2_siblings(rendered: dict[str, str]) -> list[Probe]:
    """Exercise adjacent grammar forms at the same expression boundary."""
    cases: list[Probe] = []
    expr = ("expression_functions",)
    prefix = "with q as (select entity_id as ccn, min(window_start) as first_seen from main.int_hai_hospital_windows group by entity_id)"
    for name, value, suffix, cause in [
        ("nested_scalar", "(select (select q.first_seen))", "", "date_bound:min"),
        ("unqualified_scalar", "(select first_seen)", "", "date_bound:min"),
        ("scalar_cte", "(with nested as (select q.first_seen as x) select x from nested)", "", "date_bound:min"),
        ("uncorrelated_scalar", "(select first_seen from q limit 1)", "", "date_bound:min"),
        ("nested_cte", "v.x", ", lateral (with nested as (select q.first_seen as x) select x from nested) v", "date_bound:min"),
        ("lateral", "v.x", ", lateral (select q.first_seen as x) v", "date_bound:min"),
        ("implicit_lateral", "v.x", ", (select q.first_seen as x) v", "date_bound:min"),
        ("window", "min(first_seen) over ()", "", "window"),
        ("list", "list(first_seen)", " group by ccn", "aggregate"),
        ("first", "first(first_seen)", " group by ccn", "aggregate"),
        ("sum", "sum(first_seen)", " group by ccn", "aggregate"),
        ("nested_any_value", "any_value(first_seen)", " group by ccn", "date_bound:min"),
    ]:
        sql = Template("$prefix select md5(ccn) as hospital_sk, ccn, $value as last_seen from q $suffix").substitute(prefix=prefix, value=value, suffix=suffix)
        cases.append(Probe("R2_sibling_" + name, "dim_hospital", sql, expr, cause))
        if cause == "date_bound:min":
            cases.append(Probe("R2_control_" + name, "dim_hospital", sql.replace("as last_seen", "as first_seen"), ()))
    for kind, predicate in [
        ("exists", "exists (select 1 where q.first_seen is not null)"),
        ("in", "q.first_seen in (select q.first_seen)"),
    ]:
        sql = Template("$prefix select md5(ccn) as hospital_sk, ccn, first_seen from q where $predicate").substitute(prefix=prefix, predicate=predicate)
        cases.append(Probe("R2_control_" + kind, "dim_hospital", sql, ()))
        sql = (
            sql.replace("select 1 where", "select max(q.first_seen) as last_seen where")
            if kind == "exists"
            else sql.replace("select q.first_seen)", "select max(q.first_seen))")
        )
        # EXISTS/IN may use date predicates; a forbidden aggregate must still be visited.
        sql = sql.replace("max(q.first_seen)", "sum(q.first_seen)")
        cases.append(Probe("R2_sibling_" + kind + "_aggregate", "dim_hospital", sql, expr, "aggregate"))
    cases += [
        Probe(
            "R2_sibling_union_role",
            "dim_hospital",
            Template(
                "$prefix select md5(ccn) as hospital_sk, ccn, first_seen as last_seen from q union all select md5('<unknown>'), '<unknown>', null"
            ).substitute(prefix=prefix),
            expr,
            "date_bound:min",
        ),
        Probe(
            "R2_sibling_unresolved_correlation",
            "dim_hospital",
            Template("$prefix select md5(ccn) as hospital_sk, ccn, (select missing.first_seen) as first_seen from q").substitute(prefix=prefix),
            expr,
            "unresolved binding",
        ),
        Probe(
            "R2_sibling_key_matcher_role",
            "dim_release",
            "with q as (select min(object_key) as member_sha256 from main.stg_bronze__file_copies) "
            "select md5(member_sha256) as release_sk, member_sha256 from q",
            expr,
            "object_key",
        ),
    ]
    for model, (function, column, argument) in A43.items():
        source, source_column = {
            "dim_hospital": ("int_hospital_enrollment_rows", "ccn"),
            "dim_county": ("int_acs_county_values", "county_fips"),
            "dim_state": ("int_cost_reports", "state_code"),
            "dim_zip": ("int_hsa_zip_cases", "zip_code"),
            "dim_census_tract": ("int_ruca_codes", "geography_id"),
            "dim_owner_organization": ("int_hospital_owner_rows", "associate_id"),
        }[model]
        predicate = f"length({column}) = {argument}" if function == "length" else f"regexp_matches({column}, '{argument}')"
        sql = Template(
            "with q as (select $source_column as real_key, unrelated_value as $column from main.$source), "
            "nested as (select * from q), filtered as (select real_key as $column from nested where $predicate) "
            "select md5($column) as $key, $column from filtered"
        ).substitute(source_column=source_column, column=column, source=source, predicate=predicate, key=gold_checks.dimension_key(model))
        cases.append(Probe("R2_A43_alias_" + model, model, sql, expr, "A43"))
        cases.append(Probe("R2_A43_nested_control_" + model, model, sql.replace("unrelated_value", source_column), ()))
        union = sql.replace("nested as (select * from q)", "nested as (select * from q union all select * from q)")
        cases.append(Probe("R2_A43_union_" + model, model, union, expr, "A43"))
        cases.append(Probe("R2_A43_union_control_" + model, model, union.replace("unrelated_value", source_column), ()))
    for model in ("dim_drg", "dim_rurality_code", "dim_measure"):
        sql = rendered[model]
        keys = "fact, measure_key" if model == "dim_measure" else "code_system, code"
        vintage = "release_date" if model == "dim_measure" else "label_order"
        label = "measure_name" if model == "dim_measure" else "code_label"
        mutations = {
            "missing_distinct": sql.replace(f"distinct on ({keys})", ""),
            "plain_distinct": sql.replace(f"distinct on ({keys})", "distinct"),
            "nulls_first": sql.replace(f"{vintage} desc nulls last", f"{vintage} desc nulls first"),
            "missing_vintage": sql.replace(f"{vintage} desc nulls last, ", ""),
            "missing_tie": sql.replace(f", {label} asc", ""),
        }
        if model == "dim_measure":
            mutations["wrong_second_vintage"] = sql.replace("window_end desc nulls last", "window_end asc nulls last")
            mutations["aliased_vintage"] = sql.replace("        release_date,", "        window_start as release_date,")
            mutations["aliased_label"] = sql.replace("        measure_name,", "        window_start as measure_name,")
        else:
            mutations["aliased_vintage"] = (
                sql.replace("cast(data_year as varchar)", "cast(drg_cd as varchar)")
                if model == "dim_drg"
                else sql.replace("cast(vintage as varchar)", "cast(rucc_code as varchar)")
            )
        for name, changed in mutations.items():
            if changed == sql:
                raise ValueError(f"Sibling mutation did not change {name}/{model}")
            cases.append(Probe(f"R2_A48_{name}_{model}", model, changed, expr, "A48"))
    return cases


def round3_review_probes() -> list[Probe]:
    """Retain every F5 to F7 reviewer SQL, including the confounded negative controls."""
    return [
        Probe("R3_review_C2_direct", "dim_hospital", "select md5(ccn) as hospital_sk,ccn from main.int_hospital_enrollment_rows where length(ccn)=6;", (), ""),
        Probe(
            "R3_review_P2_self_join",
            "dim_hospital",
            "select distinct md5(b.ccn) as hospital_sk,b.ccn from main.int_hospital_enrollment_rows a cross join main.int_h"
            "ospital_enrollment_rows b where length(a.ccn)=6;",
            ("expression_functions",),
            "A43",
        ),
        Probe(
            "R3_review_C2_self_join",
            "dim_hospital",
            "select distinct md5(b.ccn) as hospital_sk,b.ccn from main.int_hospital_enrollment_rows a cross join main.int_h"
            "ospital_enrollment_rows b where length(b.ccn)=6;",
            (),
            "",
        ),
        Probe(
            "R3_review_P2_correlated_exists",
            "dim_hospital",
            "select md5(b.ccn) as hospital_sk,b.ccn from main.int_hospital_enrollment_rows b where exists (select 1 from ma"
            "in.int_hospital_enrollment_rows a where length(a.ccn)=6);",
            ("expression_functions",),
            "A43",
        ),
        Probe(
            "R3_review_P2_renamed",
            "dim_hospital",
            "with q(real_ccn,ccn) as (select ccn,enrollment_id from main.int_hospital_enrollment_rows) select md5(real_ccn)"
            " as hospital_sk,real_ccn as ccn from q where length(ccn)=6;",
            ("expression_functions",),
            "A43",
        ),
        Probe(
            "R3_review_P2_using",
            "dim_hospital",
            "with a as (select ccn,enrollment_id from main.int_hospital_enrollment_rows), b as (select ccn as real_ccn,enro"
            "llment_id from main.int_hospital_enrollment_rows) select md5(b.real_ccn) as hospital_sk,b.real_ccn as ccn from"
            " a join b using(enrollment_id) where length(a.ccn)=6;",
            ("expression_functions",),
            "A43",
        ),
        Probe(
            "R3_review_C2_using",
            "dim_hospital",
            "with a as (select ccn,enrollment_id from main.int_hospital_enrollment_rows), b as (select ccn,enrollment_id fr"
            "om main.int_hospital_enrollment_rows) select md5(a.ccn) as hospital_sk,a.ccn from a join b using(ccn) where le"
            "ngth(a.ccn)=6;",
            (),
            "",
        ),
        Probe(
            "R3_review_P2_lateral",
            "dim_hospital",
            "select md5(b.ccn) as hospital_sk,b.ccn from main.int_hospital_enrollment_rows b,lateral (select a.ccn from mai"
            "n.int_hospital_enrollment_rows a where length(a.ccn)=6) x;",
            ("expression_functions",),
            "A43",
        ),
        Probe(
            "R3_review_C3_selection",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,cast(d"
            "rg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_discharges),"
            "picked as (select distinct on (code_system,code) code_system,code,code_label from observed order by code_syste"
            "m asc,code asc,label_order desc nulls last,code_label asc) select md5(concat_ws('|',code_system,code)) as drg_"
            "sk,code_system,code,code_label from picked;",
            (),
            "",
        ),
        Probe(
            "R3_review_P3_renamed_order",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,cast(d"
            "rg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_discharges),"
            "picked as (select distinct on (code_system,code) code_system,code,code_label from observed order by code_syste"
            "m asc,code asc,label_order asc nulls last,code_label asc) select md5(concat_ws('|',code_system,code)) as drg_s"
            "k,code_system,code,code_label from picked;",
            ("expression_functions",),
            "A48",
        ),
        Probe(
            "R3_review_P3_union_scopes",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,cast(d"
            "rg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_discharges),"
            "a as (select distinct on (code_system,code) code_system,code,code_label from observed where label_order = '202"
            "0' order by code_system asc,code asc,label_order desc nulls last,code_label asc),b as (select distinct on (cod"
            "e_system,code) code_system,code,code_label from observed where label_order = '2025' order by code_system asc,c"
            "ode asc,label_order desc nulls last,code_label asc),picked as (select * from a union all select * from b) sele"
            "ct md5(concat_ws('|',code_system,code)) as drg_sk,code_system,code,code_label from picked;",
            ("expression_functions",),
            "A48",
        ),
        Probe(
            "R3_review_P3_wrong_null_guard",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,cast(d"
            "rg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_discharges),"
            "picked as (select a.code_system,a.code,a.code_label from observed a cross join observed b where b.code_label i"
            "s null) select md5(concat_ws('|',code_system,code)) as drg_sk,code_system,code,code_label from picked;",
            ("expression_functions",),
            "A48",
        ),
        Probe(
            "R3_review_C3_null_guard",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,cast(d"
            "rg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_discharges),"
            "picked as (select a.code_system,a.code,a.code_label from observed a cross join observed b where a.code_label i"
            "s null) select md5(concat_ws('|',code_system,code)) as drg_sk,code_system,code,code_label from picked;",
            ("expression_functions",),
            "normalization cast has no approved final role",
        ),
        Probe(
            "R3_review_P3_wrong_null_guard_star",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,cast(d"
            "rg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_discharges),"
            "picked as (select a.* exclude(label_order) from observed a cross join observed b where b.code_label is null) s"
            "elect md5(concat_ws('|',code_system,code)) as drg_sk,code_system,code,code_label from picked;",
            ("expression_functions",),
            "A48",
        ),
        Probe(
            "R3_review_P3_case_label",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,cast(d"
            "rg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_discharges),"
            "picked as (select distinct on (code_system,code) code_system,code,case when code_label is null then null else "
            "code_label end as code_label from observed order by code_system asc,code asc,label_order desc nulls last,code_"
            "label asc) select md5(concat_ws('|',code_system,code)) as drg_sk,code_system,code,code_label from picked;",
            ("expression_case",),
            "case",
        ),
        Probe(
            "R3_review_P3_filtered_label_aggregate",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,cast(d"
            "rg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_discharges),"
            "picked as (select code_system,code,any_value(code_label) filter(where label_order='2020') as code_label from o"
            "bserved group by code_system,code) select md5(concat_ws('|',code_system,code)) as drg_sk,code_system,code,code"
            "_label from picked;",
            ("expression_functions",),
            "A48",
        ),
        Probe(
            "R3_review_P3_wrong_null_guard_clean",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,cast(d"
            "rg_desc as varchar) as code_label,data_year as label_order from main.int_mup_drg_discharges),picked as (select"
            " a.code_system,a.code,a.code_label from observed a cross join observed b where b.code_label is null) select md"
            "5(concat_ws('|',code_system,code)) as drg_sk,code_system,code,code_label from picked;",
            ("expression_functions",),
            "A48",
        ),
        Probe(
            "R3_review_C3_null_guard_clean",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,cast(d"
            "rg_desc as varchar) as code_label,data_year as label_order from main.int_mup_drg_discharges),picked as (select"
            " a.code_system,a.code,a.code_label from observed a cross join observed b where a.code_label is null) select md"
            "5(concat_ws('|',code_system,code)) as drg_sk,code_system,code,code_label from picked;",
            (),
            "",
        ),
        Probe(
            "R3_review_P3_wrong_null_guard_star_clean",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,cast(d"
            "rg_desc as varchar) as code_label,data_year as label_order from main.int_mup_drg_discharges),picked as (select"
            " a.* exclude(label_order) from observed a cross join observed b where b.code_label is null) select md5(concat_"
            "ws('|',code_system,code)) as drg_sk,code_system,code,code_label from picked;",
            ("expression_functions",),
            "A48",
        ),
        Probe(
            "R3_review_P2_using_clean",
            "dim_hospital",
            "with a as (select ccn,enrollment_id from main.int_hospital_enrollment_rows), b as (select ccn,enrollment_id fr"
            "om main.int_hospital_enrollment_rows) select md5(b.ccn) as hospital_sk,b.ccn from a join b using(enrollment_id"
            ") where length(a.ccn)=6;",
            ("expression_functions",),
            "A43",
        ),
        Probe(
            "R3_review_P2_renamed_clean",
            "dim_hospital",
            "with q(real_ccn,ccn) as (select ccn,enrollment_id from main.int_hospital_enrollment_rows), r as (select real_c"
            "cn as ccn from q where length(ccn)=6) select md5(ccn) as hospital_sk,ccn from r;",
            ("expression_functions",),
            "A43",
        ),
    ]


def round3_siblings(rendered: dict[str, str]) -> list[Probe]:
    """Challenge occurrence and selection boundaries independently of the reviewer SQL."""
    cases: list[Probe] = []
    expr = ("expression_functions",)
    hospital = "with q as (select ccn,enrollment_id from main.int_hospital_enrollment_rows) "
    for name, body, accepted in [
        ("three_cte_reads", "select md5(c.ccn) as hospital_sk,c.ccn from q a cross join q b cross join q c where length(b.ccn)=6", False),
        ("three_cte_reads_control", "select md5(c.ccn) as hospital_sk,c.ccn from q a cross join q b cross join q c where length(c.ccn)=6", True),
        ("in_scope", "select md5(b.ccn) as hospital_sk,b.ccn from q b where b.enrollment_id in (select a.enrollment_id from q a where length(a.ccn)=6)", False),
        (
            "in_scope_control",
            "select md5(b.ccn) as hospital_sk,b.ccn from q b where length(b.ccn)=6 and b.enrollment_id in (select a.enrollment_id from q a)",
            True,
        ),
        ("correlated_scope", "select md5(b.ccn) as hospital_sk,b.ccn from q b where exists (select b.ccn where length(b.ccn)=6)", False),
        ("exists_control", "select md5(b.ccn) as hospital_sk,b.ccn from q b where length(b.ccn)=6 and exists (select 1 from q a where a.ccn=b.ccn)", True),
        ("lateral_bound", "select md5(b.ccn) as hospital_sk,b.ccn from q b,lateral (select b.ccn where length(b.ccn)=6) x", False),
        ("lateral_output_control", "select md5(x.ccn) as hospital_sk,x.ccn from q b,lateral (select a.ccn from q a where length(a.ccn)=6) x", True),
        ("cte_star", "select md5(b.ccn) as hospital_sk,b.* from q a cross join q b where length(a.ccn)=6", False),
        ("cte_star_control", "select md5(b.ccn) as hospital_sk,b.* from q a cross join q b where length(b.ccn)=6", True),
        ("physical_star", "select md5(b.ccn) as hospital_sk,b.ccn from (select * from q) a cross join (select * from q) b where length(a.ccn)=6", False),
        ("using_qualified_wrong", "select md5(b.ccn) as hospital_sk,b.ccn from q a join q b using(ccn) where length(a.ccn)=6", False),
        ("using_unqualified_control", "select md5(ccn) as hospital_sk,ccn from q a join q b using(ccn) where length(ccn)=6", True),
        ("alias_columns_wrong", "select md5(b.ccn) as hospital_sk,b.ccn from q a cross join (select ccn from q) b(ccn) where length(a.ccn)=6", False),
        ("unused_filtered_cte", ", filtered as (select ccn from q where length(ccn)=6) select md5(ccn) as hospital_sk,ccn from q", False),
    ]:
        sql = hospital.rstrip() + body if body.startswith(",") else hospital + body
        cases.append(Probe("R3_A43_" + name, "dim_hospital", sql, () if accepted else expr, "" if accepted else "A43"))
    for operation in ("intersect", "except"):
        sql = Template(
            "$hospital select md5(b.ccn) as hospital_sk,b.ccn from q b where exists "
            "(select a.ccn from q a where length(a.ccn)=6 $operation select c.ccn from q c)"
        ).substitute(hospital=hospital, operation=operation)
        cases.append(Probe("R3_A43_" + operation + "_nested", "dim_hospital", sql, expr, "A43"))
    for aliases, accepted in (("enrollment_id,ccn", False), ("ccn,enrollment_id", True)):
        sql = Template("$hospital select md5(ccn) as hospital_sk,ccn from q a($aliases) where length(ccn)=6").substitute(hospital=hospital, aliases=aliases)
        cases.append(Probe("R3_A43_reference_alias_" + str(accepted), "dim_hospital", sql, () if accepted else expr, "" if accepted else "A43"))
    observed = (
        "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,"
        "cast(drg_desc as varchar) as code_label,data_year as label_order from main.int_mup_drg_discharges)"
    )
    final = " select md5(concat_ws('|',code_system,code)) as drg_sk,code_system,code,code_label from picked"
    for aliases, accepted in (("code_system,code_label,code,label_order", False), ("code_system,code,code_label,label_order", True)):
        sql = Template("$observed, picked as (select a.code_system,a.code,a.code_label from observed a($aliases) where a.code_label is null)$final").substitute(
            observed=observed, aliases=aliases, final=final
        )
        cases.append(Probe("R3_A48_reference_alias_" + str(accepted), "dim_drg", sql, () if accepted else expr, "" if accepted else "forbidden"))
    sql = Template(
        "$observed, picked as (select a.* exclude(label_order) rename(code as code_label,code_label as code) from observed a where a.code_label is null)$final"
    ).substitute(observed=observed, final=final)
    cases.append(Probe("R3_A48_star_renamed_roles", "dim_drg", sql, expr, "forbidden"))
    for name, projection, predicate, accepted in [
        ("three_cte_reads", "a.code_system,a.code,a.code_label", "c.code_label is null", False),
        ("three_cte_reads_control", "a.code_system,a.code,a.code_label", "a.code_label is null", True),
        ("star", "a.* exclude(label_order)", "b.code_label is null", False),
        ("star_control", "a.* exclude(label_order)", "a.code_label is null", True),
        ("exists", "a.code_system,a.code,a.code_label", "exists(select 1 from observed z where z.code_label is null)", False),
        ("in", "a.code_system,a.code,a.code_label", "a.code in (select z.code from observed z where z.code_label is null)", False),
        ("or_guard", "a.code_system,a.code,a.code_label", "a.code_label is null or b.code_label is null", False),
        ("not_null", "a.code_system,a.code,a.code_label", "a.code_label is not null", False),
    ]:
        sql = Template(
            "$observed, picked as (select $projection from observed a cross join observed b cross join observed c where $predicate)$final"
        ).substitute(observed=observed, projection=projection, predicate=predicate, final=final)
        cases.append(Probe("R3_A48_null_" + name, "dim_drg", sql, () if accepted else expr, "" if accepted else "A48"))
    for alias in ("a", "b"):
        sql = Template(
            "$observed, renamed as (select a.code_system,a.code,a.code_label as renamed_label from observed a cross join observed b where "
            "$alias.code_label is null), picked as (select code_system,code,renamed_label as code_label from renamed)$final"
        ).substitute(observed=observed, alias=alias, final=final)
        cases.append(Probe("R3_A48_null_renamed_" + alias, "dim_drg", sql, () if alias == "a" else expr, "" if alias == "a" else "A48"))
    selection = "select distinct on (code_system,code) code_system,code,code_label,label_order from observed"
    order = " order by code_system asc,code asc,label_order desc nulls last,code_label asc"
    chosen = observed + ", a as (" + selection + " where label_order=2020" + order + "), b as (" + selection + " where label_order=2025" + order + ")"
    for operation in ("union all", "union", "intersect", "except"):
        sql = Template("$chosen, picked as (select code_system,code,code_label from a $operation select code_system,code,code_label from b)$final").substitute(
            chosen=chosen, operation=operation, final=final
        )
        accepted = operation in {"intersect", "except"}
        cases.append(Probe("R3_A48_set_" + operation.replace(" ", "_"), "dim_drg", sql, () if accepted else expr, "" if accepted else "A48"))
        if not accepted:
            combined = Template("$chosen, combined as (select * from a $operation select * from b), picked as (").substitute(chosen=chosen, operation=operation)
            sql = combined + selection.replace("from observed", "from combined") + order + ")" + final
            cases.append(Probe("R3_A48_reselect_" + operation.replace(" ", "_"), "dim_drg", sql, ()))
        else:
            sql = Template(
                "$chosen, picked as (select code_system,code,code_label from observed $operation select code_system,code,code_label from a)$final"
            ).substitute(chosen=chosen, operation=operation, final=final)
            cases.append(Probe("R3_A48_right_certificate_" + operation, "dim_drg", sql, expr, "A48"))
    for name, keys, vintage, accepted in [
        ("cross_join_key", "b.code_system,b.code,a.code_label", "a.label_order", False),
        ("cross_join_vintage", "a.code_system,a.code,a.code_label", "b.label_order", False),
        ("cross_join_control", "a.code_system,a.code,a.code_label", "a.label_order", True),
    ]:
        sql = observed + ", picked as (select distinct on (code_system,code) " + keys
        sql += " from observed a cross join observed b order by code_system asc,code asc," + vintage + " desc nulls last,code_label asc)" + final
        cases.append(Probe("R3_A48_" + name, "dim_drg", sql, () if accepted else expr, "" if accepted else "A48"))
    for name, suffix in [
        (
            "window_partition",
            ", picked as (select code_system,code,first_value(code_label) over(partition by code_system,code "
            "order by label_order desc) as code_label from observed)",
        ),
        (
            "recursive",
            ", recursive_rows as (select * from observed union all select * from recursive_rows where false), "
            "picked as (select code_system,code,code_label from recursive_rows)",
        ),
    ]:
        sql = observed + suffix + final
        if name == "recursive":
            sql = sql.replace("with observed", "with recursive observed", 1)
        cases.append(Probe("R3_A48_" + name, "dim_drg", sql, expr, "window" if name == "window_partition" else "not supported"))
    for model in ("dim_drg", "dim_rurality_code"):
        sql = rendered[model]
        cases.append(Probe("R3_A48_labeled_unlabeled_unknown_" + model, model, sql, ()))
        source = "single" if model == "dim_rurality_code" else "observed"
        for name, before, after in [
            ("missing_anti_key", f"labeled.code = {source}.code", "true"),
            ("wrong_anti_alias", f"labeled.code = {source}.code", "labeled.code = labeled.code"),
            ("partial_anti_rows", f"labeled.code = {source}.code", f"labeled.code = {source}.code and labeled.code = '068'"),
            ("anti_limit", "select 1\n            from labeled", "select 1\n            from (select * from labeled limit 1) labeled"),
        ]:
            changed = sql.replace(before, after)
            if changed == sql:
                raise ValueError("Null-domain mutation did not change " + name)
            cases.append(Probe("R3_A48_" + name + "_" + model, model, changed, expr, "A48"))
    cases.append(Probe("R3_A48_measure_join_unknown", "dim_measure", rendered["dim_measure"], ()))
    for key in ("fact", "measure_key"):
        sql = rendered["dim_measure"].replace("identities." + key + " = names." + key, "names." + key + " = names." + key)
        cases.append(Probe("R3_A48_measure_join_wrong_" + key, "dim_measure", sql, expr, "A48"))
    sql = rendered["dim_drg"]
    restricted = sql.replace("unlabeled as (", "subset as (select * from labeled where code = '068'), unlabeled as (")
    restricted = restricted.replace("from labeled\n            where", "from subset as labeled\n            where")
    cases.append(Probe("R3_A48_anti_filtered_cte", "dim_drg", restricted, expr, "A48"))
    cases.append(
        Probe("R3_A48_anti_filtered_cte_control", "dim_drg", restricted.replace("select * from labeled\n    union", "select * from subset\n    union"), ())
    )
    alias_sql = Template(
        "$chosen, renamed(system_name,code_name,label_name) as (select code_system,code,code_label from a), picked as ("
        "select system_name as code_system,code_name as code,label_name as code_label from renamed)$final"
    ).substitute(chosen=chosen, final=final)
    cases.append(Probe("R3_A48_renamed_certificate_control", "dim_drg", alias_sql, ()))
    multi = observed.replace(
        "from main.int_mup_drg_discharges)",
        "from main.int_mup_drg_discharges union all select 'synthetic_second_system',cast(drg_cd as varchar),cast(drg_desc as varchar),data_year "
        "from main.int_mup_drg_discharges)",
    )
    for accepted in (False, True):
        sql = multi + ", selected as (" + selection + order + "), picked as ("
        sql += "select a.code_system,b.code,b.code_label from selected a join selected b on a.code=b.code"
        sql += " and a.code_system=b.code_system" if accepted else ""
        sql += ")" + final
        cases.append(Probe("R3_A48_literal_union_join_" + str(accepted), "dim_drg", sql, () if accepted else expr, "" if accepted else "A48"))
    return cases


def round4_review_probes() -> list[Probe]:
    """Exact F8/F9 reviewer queries and their passing controls, frozen before the fixes."""
    return [
        Probe(
            "R4_review_hash_a.ccn",
            "dim_hospital",
            "select md5(a.ccn) as hospital_sk,b.ccn from main.int_hospital_enrollment_rows a cross join main.int_hospital_enrollment_rows b",
            ("expression_functions",),
            "hashes a column",
        ),
        Probe(
            "R4_review_hash_b.ccn",
            "dim_hospital",
            "select md5(b.ccn) as hospital_sk,b.ccn from main.int_hospital_enrollment_rows a cross join main.int_hospital_enrollment_rows b",
            (),
            "",
        ),
        Probe(
            "R4_review_hash_repeated_renamed",
            "dim_hospital",
            "with x as (select ccn from main.int_hospital_enrollment_rows),y(v) as (select ccn from x) select md5(a.v)"
            " as hospital_sk,b.v as ccn from y a cross join y b",
            ("expression_functions",),
            "not the contracted expression",
        ),
        Probe(
            "R4_review_hash_using",
            "dim_hospital",
            "select md5(a.ccn) as hospital_sk,b.ccn from main.int_hospital_enrollment_rows a join main.int_hospital_enrollment_rows b using(enrollment_id)",
            ("expression_functions",),
            "hashes a column",
        ),
        Probe(
            "R4_review_hash_lateral",
            "dim_hospital",
            "select md5(x.ccn) as hospital_sk,b.ccn from main.int_hospital_enrollment_rows b,lateral (select a.ccn from main.int_hospital_enrollment_rows a) x",
            ("expression_functions",),
            "hashes a column",
        ),
        Probe(
            "R4_review_domain_except_selected",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,c"
            "ast(drg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_di"
            "scharges),selected as (select distinct on (code_system,code) code_system,code,code_label from observed wh"
            "ere code_label is not null order by code_system,code,label_order desc nulls last,code_label),subset as (s"
            "elect * from selected except select * from selected where code='068'),picked as (select * from selected u"
            "nion all select o.code_system,o.code,o.code_label from observed o where o.code_label is null and not exis"
            "ts (select 1 from subset s where s.code_system=o.code_system and s.code=o.code)) select md5(concat_ws('|'"
            ",code_system,code)) as drg_sk,code_system,code,code_label from picked",
            ("expression_functions",),
            "A48",
        ),
        Probe(
            "R4_review_domain_except_subset",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,c"
            "ast(drg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_di"
            "scharges),selected as (select distinct on (code_system,code) code_system,code,code_label from observed wh"
            "ere code_label is not null order by code_system,code,label_order desc nulls last,code_label),subset as (s"
            "elect * from selected except select * from selected where code='068'),picked as (select * from subset uni"
            "on all select o.code_system,o.code,o.code_label from observed o where o.code_label is null and not exists"
            " (select 1 from subset s where s.code_system=o.code_system and s.code=o.code)) select md5(concat_ws('|',c"
            "ode_system,code)) as drg_sk,code_system,code,code_label from picked",
            (),
            "",
        ),
        Probe(
            "R4_review_domain_intersect_selected",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,c"
            "ast(drg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_di"
            "scharges),selected as (select distinct on (code_system,code) code_system,code,code_label from observed wh"
            "ere code_label is not null order by code_system,code,label_order desc nulls last,code_label),subset as (s"
            "elect * from selected intersect select * from selected where code<>'068'),picked as (select * from select"
            "ed union all select o.code_system,o.code,o.code_label from observed o where o.code_label is null and not "
            "exists (select 1 from subset s where s.code_system=o.code_system and s.code=o.code)) select md5(concat_ws"
            "('|',code_system,code)) as drg_sk,code_system,code,code_label from picked",
            ("expression_functions",),
            "A48",
        ),
        Probe(
            "R4_review_domain_intersect_subset",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,c"
            "ast(drg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_di"
            "scharges),selected as (select distinct on (code_system,code) code_system,code,code_label from observed wh"
            "ere code_label is not null order by code_system,code,label_order desc nulls last,code_label),subset as (s"
            "elect * from selected intersect select * from selected where code<>'068'),picked as (select * from subset"
            " union all select o.code_system,o.code,o.code_label from observed o where o.code_label is null and not ex"
            "ists (select 1 from subset s where s.code_system=o.code_system and s.code=o.code)) select md5(concat_ws('"
            "|',code_system,code)) as drg_sk,code_system,code,code_label from picked",
            (),
            "",
        ),
        Probe(
            "R4_review_domain_except_all_selected",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,c"
            "ast(drg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_di"
            "scharges),selected as (select distinct on (code_system,code) code_system,code,code_label from observed wh"
            "ere code_label is not null order by code_system,code,label_order desc nulls last,code_label),subset as (s"
            "elect * from selected except all select * from selected where code='068'),picked as (select * from select"
            "ed union all select o.code_system,o.code,o.code_label from observed o where o.code_label is null and not "
            "exists (select 1 from subset s where s.code_system=o.code_system and s.code=o.code)) select md5(concat_ws"
            "('|',code_system,code)) as drg_sk,code_system,code,code_label from picked",
            ("expression_functions",),
            "A48",
        ),
        Probe(
            "R4_review_domain_except_all_subset",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,c"
            "ast(drg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_di"
            "scharges),selected as (select distinct on (code_system,code) code_system,code,code_label from observed wh"
            "ere code_label is not null order by code_system,code,label_order desc nulls last,code_label),subset as (s"
            "elect * from selected except all select * from selected where code='068'),picked as (select * from subset"
            " union all select o.code_system,o.code,o.code_label from observed o where o.code_label is null and not ex"
            "ists (select 1 from subset s where s.code_system=o.code_system and s.code=o.code)) select md5(concat_ws('"
            "|',code_system,code)) as drg_sk,code_system,code,code_label from picked",
            (),
            "",
        ),
        Probe(
            "R4_review_domain_intersect_all_selected",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,c"
            "ast(drg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_di"
            "scharges),selected as (select distinct on (code_system,code) code_system,code,code_label from observed wh"
            "ere code_label is not null order by code_system,code,label_order desc nulls last,code_label),subset as (s"
            "elect * from selected intersect all select * from selected where code<>'068'),picked as (select * from se"
            "lected union all select o.code_system,o.code,o.code_label from observed o where o.code_label is null and "
            "not exists (select 1 from subset s where s.code_system=o.code_system and s.code=o.code)) select md5(conca"
            "t_ws('|',code_system,code)) as drg_sk,code_system,code,code_label from picked",
            ("expression_functions",),
            "A48",
        ),
        Probe(
            "R4_review_domain_intersect_all_subset",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,c"
            "ast(drg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_di"
            "scharges),selected as (select distinct on (code_system,code) code_system,code,code_label from observed wh"
            "ere code_label is not null order by code_system,code,label_order desc nulls last,code_label),subset as (s"
            "elect * from selected intersect all select * from selected where code<>'068'),picked as (select * from su"
            "bset union all select o.code_system,o.code,o.code_label from observed o where o.code_label is null and no"
            "t exists (select 1 from subset s where s.code_system=o.code_system and s.code=o.code)) select md5(concat_"
            "ws('|',code_system,code)) as drg_sk,code_system,code,code_label from picked",
            (),
            "",
        ),
        Probe(
            "R4_review_hash_valid_keys_a",
            "dim_hospital",
            "select md5(a.ccn) as hospital_sk,b.ccn from main.int_hospital_enrollment_rows a cross join main.int_hospi"
            "tal_enrollment_rows b where a.ccn='123456' and b.ccn='654321'",
            ("expression_functions",),
            "hashes a column",
        ),
        Probe(
            "R4_review_hash_valid_keys_b",
            "dim_hospital",
            "select md5(b.ccn) as hospital_sk,b.ccn from main.int_hospital_enrollment_rows a cross join main.int_hospi"
            "tal_enrollment_rows b where a.ccn='123456' and b.ccn='654321'",
            (),
            "",
        ),
        Probe(
            "R4_review_hash_renamed_clean",
            "dim_hospital",
            "with q(x) as (select ccn from main.int_hospital_enrollment_rows),r as (select x as ccn from q) select md5"
            "(a.ccn) as hospital_sk,b.ccn from r a cross join r b",
            ("expression_functions",),
            "hashes a column",
        ),
        Probe(
            "R4_review_hash_group",
            "dim_hospital",
            "select md5(a.ccn) as hospital_sk,b.ccn from main.int_hospital_enrollment_rows a cross join main.int_hospi"
            "tal_enrollment_rows b group by a.ccn,b.ccn",
            ("expression_functions",),
            "hashes a column",
        ),
        Probe(
            "R4_review_hash_star",
            "dim_hospital",
            "with q as (select ccn from main.int_hospital_enrollment_rows) select md5(a.ccn) as hospital_sk,b.* from q a cross join q b",
            ("expression_functions",),
            "hashes a column",
        ),
        Probe(
            "R4_review_hash_distinct",
            "dim_hospital",
            "select distinct md5(a.ccn) as hospital_sk,b.ccn from main.int_hospital_enrollment_rows a cross join main.int_hospital_enrollment_rows b",
            ("expression_functions",),
            "hashes a column",
        ),
        Probe(
            "R4_review_hash_wrong_column_control",
            "dim_hospital",
            "select md5(a.ccn) as hospital_sk,b.enrollment_id as ccn from main.int_hospital_enrollment_rows a cross join main.int_hospital_enrollment_rows b",
            ("expression_functions",),
            "hashes a column",
        ),
        Probe(
            "R4_review_domain_full_original",
            "dim_drg",
            "-- One row per (diagnosis-related group code system, code) as published, plus the unknown member. code_sy"
            "stem names the silver model and column;\n-- code_label is the paired label column where silver has one, el"
            "se null. A code with two different labels keeps the label of its latest data\n-- year (amendment A48); a c"
            "ode with a label and a null label keeps the label [824] [750].\n\nwith\n\nobserved as (\n    select\n        'i"
            "nt_mup_drg_discharges.drg_cd' as code_system,\n        cast(drg_cd as varchar) as code,\n        cast(drg_d"
            'esc as varchar) as code_label,\n        cast(data_year as varchar) as label_order\n    from main."int_mup_d'
            'rg_discharges"\n    where drg_cd is not null\n),\n\n-- A code published with more than one label keeps the la'
            "bel of its latest vintage (then the first label in order); every\n-- published label stays on the fact row"
            "s (amendment A48).\nlabeled as (\n    select distinct on (code_system, code)\n        code_system,\n        c"
            "ode,\n        code_label\n    from observed\n    where code_label is not null\n    order by code_system asc, "
            "code asc, label_order desc nulls last, code_label asc\n),\n\nunlabeled as (\n    select distinct\n        obse"
            "rved.code_system,\n        observed.code,\n        observed.code_label\n    from observed\n    where\n        "
            "observed.code_label is null\n        and not exists (\n            select 1\n            from labeled\n      "
            "      where\n                labeled.code_system = observed.code_system\n                and labeled.code ="
            " observed.code\n        )\n),\n\nranked as (\n    select * from labeled\n    union all\n    select * from unlabe"
            "led\n),\n\nfinal as (\n    select\n        md5(concat_ws('|', code_system, code)) as drg_sk,\n        code_syst"
            "em,\n        code,\n        code_label\n    from ranked\n    union all\n    select\n        md5('<unknown>') as"
            " drg_sk,\n        '<unknown>' as code_system,\n        '<unknown>' as code,\n        cast(null as varchar) a"
            "s code_label\n)\n\nselect * from final",
            (),
            "",
        ),
        Probe(
            "R4_review_domain_full_except",
            "dim_drg",
            "-- One row per (diagnosis-related group code system, code) as published, plus the unknown member. code_sy"
            "stem names the silver model and column;\n-- code_label is the paired label column where silver has one, el"
            "se null. A code with two different labels keeps the label of its latest data\n-- year (amendment A48); a c"
            "ode with a label and a null label keeps the label [824] [750].\n\nwith\n\nobserved as (\n    select\n        'i"
            "nt_mup_drg_discharges.drg_cd' as code_system,\n        cast(drg_cd as varchar) as code,\n        cast(drg_d"
            'esc as varchar) as code_label,\n        cast(data_year as varchar) as label_order\n    from main."int_mup_d'
            'rg_discharges"\n    where drg_cd is not null\n),\n\n-- A code published with more than one label keeps the la'
            "bel of its latest vintage (then the first label in order); every\n-- published label stays on the fact row"
            "s (amendment A48).\nlabeled as (\n    select distinct on (code_system, code)\n        code_system,\n        c"
            "ode,\n        code_label\n    from observed\n    where code_label is not null\n    order by code_system asc, "
            "code asc, label_order desc nulls last, code_label asc\n),\n\nsubset as (select * from labeled except select "
            "* from labeled where code='068'), unlabeled as (\n    select distinct\n        observed.code_system,\n      "
            "  observed.code,\n        observed.code_label\n    from observed\n    where\n        observed.code_label is n"
            "ull\n        and not exists (\n            select 1\n            from subset as labeled\n            where\n  "
            "              labeled.code_system = observed.code_system\n                and labeled.code = observed.code"
            "\n        )\n),\n\nranked as (\n    select * from labeled\n    union all\n    select * from unlabeled\n),\n\nfinal "
            "as (\n    select\n        md5(concat_ws('|', code_system, code)) as drg_sk,\n        code_system,\n        co"
            "de,\n        code_label\n    from ranked\n    union all\n    select\n        md5('<unknown>') as drg_sk,\n     "
            "   '<unknown>' as code_system,\n        '<unknown>' as code,\n        cast(null as varchar) as code_label\n)"
            "\n\nselect * from final",
            ("expression_functions",),
            "A48",
        ),
        Probe(
            "R4_review_domain_full_intersect",
            "dim_drg",
            "-- One row per (diagnosis-related group code system, code) as published, plus the unknown member. code_sy"
            "stem names the silver model and column;\n-- code_label is the paired label column where silver has one, el"
            "se null. A code with two different labels keeps the label of its latest data\n-- year (amendment A48); a c"
            "ode with a label and a null label keeps the label [824] [750].\n\nwith\n\nobserved as (\n    select\n        'i"
            "nt_mup_drg_discharges.drg_cd' as code_system,\n        cast(drg_cd as varchar) as code,\n        cast(drg_d"
            'esc as varchar) as code_label,\n        cast(data_year as varchar) as label_order\n    from main."int_mup_d'
            'rg_discharges"\n    where drg_cd is not null\n),\n\n-- A code published with more than one label keeps the la'
            "bel of its latest vintage (then the first label in order); every\n-- published label stays on the fact row"
            "s (amendment A48).\nlabeled as (\n    select distinct on (code_system, code)\n        code_system,\n        c"
            "ode,\n        code_label\n    from observed\n    where code_label is not null\n    order by code_system asc, "
            "code asc, label_order desc nulls last, code_label asc\n),\n\nsubset as (select * from labeled intersect sele"
            "ct * from labeled where code<>'068'), unlabeled as (\n    select distinct\n        observed.code_system,\n  "
            "      observed.code,\n        observed.code_label\n    from observed\n    where\n        observed.code_label "
            "is null\n        and not exists (\n            select 1\n            from subset as labeled\n            wher"
            "e\n                labeled.code_system = observed.code_system\n                and labeled.code = observed."
            "code\n        )\n),\n\nranked as (\n    select * from labeled\n    union all\n    select * from unlabeled\n),\n\nfi"
            "nal as (\n    select\n        md5(concat_ws('|', code_system, code)) as drg_sk,\n        code_system,\n      "
            "  code,\n        code_label\n    from ranked\n    union all\n    select\n        md5('<unknown>') as drg_sk,\n "
            "       '<unknown>' as code_system,\n        '<unknown>' as code,\n        cast(null as varchar) as code_lab"
            "el\n)\n\nselect * from final",
            ("expression_functions",),
            "A48",
        ),
        Probe(
            "R4_review_domain_complete_exclusion_control",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system,cast(drg_cd as varchar) as code,c"
            "ast(drg_desc as varchar) as code_label,cast(data_year as varchar) as label_order from main.int_mup_drg_di"
            "scharges),selected as (select distinct on (code_system,code) code_system,code,code_label from observed wh"
            "ere code_label is not null order by code_system,code,label_order desc nulls last,code_label),subset as (s"
            "elect * from selected except select * from selected where code='068'),picked as (select * from selected u"
            "nion all select o.code_system,o.code,o.code_label from observed o where o.code_label is null and not exis"
            "ts (select 1 from selected s where s.code_system=o.code_system and s.code=o.code)) select md5(concat_ws('"
            "|',code_system,code)) as drg_sk,code_system,code,code_label from picked",
            (),
            "",
        ),
    ]


def round5_probes() -> list[Probe]:
    """GX round 5 (amendment A90): a sampled FROM reference restricts a selection, so it may not keep its domain; a null natural
    key beside the unknown-member hash is accepted only where the contract exempts it (dim_measure key parts, dim_date)."""
    originals = {probe.name.removeprefix("R4_review_"): probe.sql for probe in round4_review_probes()}
    bad_domain, good_domain = originals["domain_except_selected"], originals["domain_except_subset"]
    old = "select * from selected except select * from selected where code='068'"
    result: list[Probe] = []
    samples = {
        "tablesample_cte_ref": "select * from selected tablesample 0 percent (bernoulli)",
        "tablesample_subquery_ref": "select * from (select * from selected) tablesample 0 percent (bernoulli)",
        "tablesample_aliased_ref": "select s.* from selected as s tablesample 0 percent (bernoulli)",
    }
    for name, restriction in samples.items():
        for control, sql in ((False, bad_domain), (True, good_domain)):
            result.append(
                Probe(
                    f"R5_{name}_{'control' if control else 'reject'}",
                    "dim_drg",
                    sql.replace(old, restriction),
                    () if control else ("expression_functions",),
                    "" if control else "A48",
                )
            )
    full = originals["domain_full_original"]
    sampled = full.replace("unlabeled as (", "subset as (select * from labeled tablesample 0 percent (bernoulli)),\n\nunlabeled as (", 1).replace(
        "select 1\n            from labeled\n", "select 1\n            from subset as labeled\n", 1
    )
    result.append(Probe("R5_full_tablesample_reject", "dim_drg", sampled, ("expression_functions",), "A48"))
    result.append(
        Probe(
            "R5_unknown_arm_null_ccn_reject",
            "dim_hospital",
            "select md5(ccn) as hospital_sk,ccn from main.int_hospital_enrollment_rows "
            "union all select md5('<unknown>') as hospital_sk,cast(null as varchar) as ccn",
            ("expression_functions",),
        )
    )
    result.append(
        Probe(
            "R5_unknown_arm_sentinel_control",
            "dim_hospital",
            "select md5(ccn) as hospital_sk,ccn from main.int_hospital_enrollment_rows union all select md5('<unknown>') as hospital_sk,'<unknown>' as ccn",
            (),
        )
    )
    return result


def round4_siblings() -> list[Probe]:
    """Exercise domain restrictions and ordered value identities beyond the exact reviewer forms."""
    originals = {probe.name.removeprefix("R4_review_"): probe.sql for probe in round4_review_probes()}
    result: list[Probe] = []
    bad_domain = originals["domain_except_selected"]
    good_domain = originals["domain_except_subset"]
    restrictions = {
        "where": "select * from selected where code <> '068'",
        "limit": "select * from selected limit 0",
        "offset": "select * from selected offset 1",
        "nested_except": "select * from (select * from selected except select * from selected where code='068') r",
        "nested_intersect": "select * from (select * from selected intersect select * from selected where code<>'068') r",
        "except_then_intersect": "(select * from selected except select * from selected where code='068') intersect select * from selected",
        "intersect_then_except": "(select * from selected intersect select * from selected) except select * from selected where code='068'",
        "set_limit": "(select * from selected except select * from selected where code='068') limit 0",
        "set_offset": "(select * from selected intersect select * from selected) offset 1",
        "group": "select code_system,code,code_label from selected group by code_system,code,code_label having code <> '068'",
        "distinct": "select distinct * from selected where code <> '068'",
    }
    old = "select * from selected except select * from selected where code='068'"
    for name, restriction in restrictions.items():
        for control, sql in ((False, bad_domain), (True, good_domain)):
            result.append(
                Probe(
                    f"R4_domain_{name}_{'control' if control else 'reject'}",
                    "dim_drg",
                    sql.replace(old, restriction),
                    () if control else ("expression_functions",),
                    "" if control else "A48",
                )
            )
    hospital = "main.int_hospital_enrollment_rows"
    pairs = {
        "inner_on": (f"from {hospital} a join {hospital} b on a.ccn=b.ccn", True),
        "inner_using_key": (f"from {hospital} a join {hospital} b using(ccn)", True),
        "left_on": (f"from {hospital} a left join {hospital} b on a.ccn=b.ccn", False),
        "right_on": (f"from {hospital} a right join {hospital} b on a.ccn=b.ccn", False),
        "full_on": (f"from {hospital} a full join {hospital} b on a.ccn=b.ccn", False),
        "transitive_on": (f"from {hospital} a join {hospital} b on a.ccn=b.ccn join {hospital} c on b.ccn=c.ccn", True),
    }
    for name, (tail, equal) in pairs.items():
        natural = "c.ccn" if name == "transitive_on" else "b.ccn"
        result.append(
            Probe(
                f"R4_hash_{name}",
                "dim_hospital",
                f"select md5(a.ccn) as hospital_sk,{natural} {tail}",
                () if equal else ("expression_functions",),
                "" if equal else "hashes a column",
            )
        )
    cross = f"from {hospital} a cross join {hospital} b"
    wrong = f"select md5(a.ccn) as hospital_sk,b.ccn {cross}"
    matched = f"select md5(b.ccn) as hospital_sk,b.ccn {cross}"
    swapped = f"select md5(b.ccn) as hospital_sk,a.ccn {cross}"
    for operator in ("union", "union all", "except", "intersect", "except all", "intersect all"):
        for control in (False, True):
            first, second = (matched, matched) if control else (wrong, swapped)
            result.append(
                Probe(
                    f"R4_hash_arms_{operator.replace(' ', '_')}_{control}",
                    "dim_hospital",
                    Template("with q as ($first $operator $second) select * from q").substitute(first=first, operator=operator, second=second),
                    () if control else ("expression_functions",),
                    "" if control else "hashes a column",
                )
            )
    q = "with q as (select 'system' as code_system,cast(chow_type_code as varchar) as code from main.int_change_of_ownership_rows) "
    for name, operands, natural, control in (
        ("first_operand", "a.code_system,b.code", "b.code_system,b.code", False),
        ("second_operand", "b.code_system,a.code", "b.code_system,b.code", False),
        ("both_operands", "a.code_system,a.code", "b.code_system,b.code", False),
        ("same_operands", "b.code_system,b.code", "b.code_system,b.code", True),
        ("mixed_matching", "a.code_system,b.code", "a.code_system,b.code", True),
        ("wrong_order", "b.code,b.code_system", "b.code_system,b.code", False),
    ):
        sql = Template("$prefix select md5(concat_ws('|',$operands)) as change_type_sk,$natural from q a cross join q b").substitute(
            prefix=q, operands=operands, natural=natural
        )
        result.append(
            Probe(
                f"R4_concat_{name}",
                "dim_change_type",
                sql,
                () if control else ("expression_functions",),
                "" if control else "not the contracted expression" if name == "wrong_order" else "hashes a column",
            )
        )
    for name in ("hash_renamed_clean", "hash_lateral", "hash_group", "hash_star", "hash_distinct"):
        sql = originals[name].replace("md5(a.ccn)", "md5(b.ccn)").replace("md5(x.ccn)", "md5(b.ccn)")
        result.append(Probe("R4_control_" + name, "dim_hospital", sql, ()))
    for control in (False, True):
        checked = "a" if control else "b"
        sql = Template(
            "select md5(case when $checked.ccn is null then '<unknown>' else cast(a.ccn as varchar) end) as hospital_sk,a.ccn "
            "from main.int_hospital_enrollment_rows a cross join main.int_hospital_enrollment_rows b"
        ).substitute(checked=checked)
        result.append(
            Probe(
                f"R4_guard_occurrence_{control}",
                "dim_hospital",
                sql,
                () if control else ("expression_functions",),
                "" if control else "not the contracted expression",
            )
        )
        sql = Template(
            "select md5(cast($hashed.ccn as varchar)) as hospital_sk,b.ccn "
            "from main.int_hospital_enrollment_rows a cross join main.int_hospital_enrollment_rows b"
        ).substitute(hashed="b" if control else "a")
        result.append(
            Probe(f"R4_cast_occurrence_{control}", "dim_hospital", sql, () if control else ("expression_functions",), "" if control else "hashes a column")
        )
    for control in (False, True):
        first = matched if control else wrong
        second = matched if control else swapped
        sql = Template(
            "with a as ($first), b as ($second), q as (select hospital_sk as k,ccn as n from a union all "
            "select hospital_sk as k,ccn as n from b) select k as hospital_sk,n as ccn from q"
        ).substitute(first=first, second=second)
        result.append(
            Probe(f"R4_renamed_union_{control}", "dim_hospital", sql, () if control else ("expression_functions",), "" if control else "hashes a column")
        )
    for control in (False, True):
        # Source-free literal arms exercise branch correlation without physical lineage or original FROM paths.
        first = "with q as (select '123456' as ccn) select md5(ccn) as hospital_sk,ccn from q"
        second = "with q as (select '654321' as ccn) select md5(ccn) as hospital_sk,ccn from q"
        left_natural = "ccn" if control else "'654321'"
        right_natural = "ccn" if control else "'123456'"
        sql = Template(
            "with a as ($first),b as ($second),q as (select hospital_sk as k,$left_natural as n from a union all "
            "select hospital_sk as k,$right_natural as n from b) select k as hospital_sk,n as ccn from q"
        ).substitute(first=first, second=second, left_natural=left_natural, right_natural=right_natural)
        result.append(
            Probe(
                f"R4_literal_renamed_union_{control}", "dim_hospital", sql, () if control else ("expression_functions",), "" if control else "hashes a column"
            )
        )
    return result


G4_MODELS = ("bridge_county_planning_region", "bridge_measure_control", "dim_county", "dim_measure", "dim_rurality_code", "dim_state")


EXPR = ("expression_functions",)
CONTROL_SEED = "main.registry_measure_sources_bridge"
STATE_SQL = Template(
    "with fips as (select distinct state as state_code, $expression as $alias from $source "
    "where state is not null and county_fips is not null) select md5(state_code) as state_sk, state_code, $out from fips"
)
TRACT_SQL = Template(
    "with counted as (select county_fips, county_fips_2023, member_sha256, $count as tract_count from $source$group) "
    "select county_fips as old_county_fips, county_fips_2023 as planning_region_fips, $out, member_sha256 from counted"
)
TRACT_GROUP = " group by county_fips, county_fips_2023, member_sha256"
RURALITY_BOUNDS_SQL = Template(
    "with observed as (select 'int_rucc_county_codes.rucc_code' as code_system, cast(rucc_code as varchar) as code, "
    "cast(vintage as varchar) as label_order from main.int_rucc_county_codes), "
    "editions as (select code_system, code, $start as edition_start, $end as edition_end from observed$group) "
    "select md5(concat_ws('|', code_system, code, edition_start)) as rurality_code_sk, code_system, code, edition_start, edition_end$extra from editions"
)
MEASURE_BOUNDS_SQL = Template(
    "with names_published as (select 'fct_hai_hospital_window' as fact, cast(measure_id as varchar) as measure_key, "
    "'score' as value_column, release_date, window_end from main.int_hai_hospital_windows), "
    "editions as (select fact, measure_key, value_column, $start as edition_start, $end as edition_end from names_published$group) "
    "select md5(concat_ws('|', fact, measure_key, value_column, edition_start)) as measure_sk, "
    "concat_ws('|', fact, measure_key, value_column, edition_start) as measure_identity, "
    "fact, measure_key, value_column, edition_start, edition_end from editions"
)
COUNTY_LISTS_SQL = Template(
    "with lists as (select 'int_county_adjacency_edges' as list_name, vintage, cast(county_fips as varchar) as county_fips "
    "from main.int_county_adjacency_edges where vintage is not null union "
    "select 'int_rucc_county_codes' as list_name, vintage, cast(county_fips as varchar) as county_fips from main.int_rucc_county_codes "
    "where vintage is not null), "
    "vintages as (select distinct vintage from lists), "
    "latest_vintage as (select max(vintage) as vintage from lists where list_name = 'int_county_adjacency_edges'), "
    "latest as (select lists.county_fips from lists inner join latest_vintage on lists.vintage = latest_vintage.vintage "
    "where lists.list_name = 'int_county_adjacency_edges'), "
    "last_listed as (select county_fips, max(vintage) as last_listed_vintage from lists group by county_fips), "
    "first_unlisted as (select last_listed.county_fips, min(vintages.vintage) as first_unlisted_vintage from last_listed "
    "inner join vintages on last_listed.last_listed_vintage < vintages.vintage "
    "where last_listed.county_fips not in (select latest.county_fips from latest) group by last_listed.county_fips) "
    "select md5(last_listed.county_fips) as county_sk, last_listed.county_fips, $membership as $alias, "
    "last_listed.last_listed_vintage, first_unlisted.first_unlisted_vintage from last_listed "
    "$join latest on $condition "
    "left join first_unlisted on last_listed.county_fips = first_unlisted.county_fips"
)
COUNTY_BOUNDS_SQL = Template(
    "select md5(county_fips) as county_sk, county_fips, $first as first_unlisted_vintage, $last as $last_alias from $source group by county_fips"
)
RURALITY_KEY_SQL = Template(
    "with observed as (select 'int_rucc_county_codes.rucc_code' as code_system, cast(rucc_code as varchar) as code, "
    "cast(vintage as varchar) as label_order from main.int_rucc_county_codes), "
    "editions as (select code_system, code, min(label_order) as edition_start, max(label_order) as edition_end from observed group by code_system, code) "
    "select md5(concat_ws('|', $parts)) as rurality_code_sk, code_system, code, edition_start, edition_end from editions"
)
MEASURE_UNION_SQL = Template(
    "with names_published as (select 'fct_hai_hospital_window' as fact, cast(measure_id as varchar) as measure_key, 'score' as value_column, release_date "
    "from main.int_hai_hospital_windows), "
    "base as (select md5(concat_ws('|', fact, measure_key, value_column)) as measure_sk, "
    "concat_ws('|', fact, measure_key, value_column) as measure_identity, fact, measure_key, value_column from names_published), "
    "editions as (select fact, measure_key, value_column, cast(min(release_date) as varchar) as edition_start, "
    "cast(max(release_date) as varchar) as edition_end from names_published group by fact, measure_key, value_column), "
    "ordinary as (select measure_sk, measure_identity, fact, measure_key, value_column, $ordinary_start as edition_start, "
    "cast(null as varchar) as edition_end from base), "
    "split_rows as (select md5(concat_ws('|', base.fact, base.measure_key, base.value_column$split_part)) as measure_sk, "
    "concat_ws('|', base.fact, base.measure_key, base.value_column$identity_part) as measure_identity, "
    "base.fact, base.measure_key, base.value_column, editions.edition_start, editions.edition_end from base "
    "inner join editions on base.fact = editions.fact and base.measure_key = editions.measure_key and base.value_column = editions.value_column) "
    "select * from ordinary union all select * from split_rows union all "
    "select md5('<unknown>') as measure_sk, '<unknown>' as measure_identity, cast(null as varchar) as fact, cast(null as varchar) as measure_key, "
    "cast(null as varchar) as value_column, cast(null as varchar) as edition_start, cast(null as varchar) as edition_end"
)
BRIDGE_KEYS_SQL = Template(
    "with seed_rows as (select fact, measure_key, value_column, vintage_start, vintage_end, measure_control from $seed), "
    "editions as (select fact, measure_key, value_column, edition_start, edition_end from main.dim_measure where edition_start is not null) "
    "select md5(concat_ws('|', seed_rows.fact, seed_rows.measure_key, seed_rows.value_column$fourth)) as $key, "
    "md5(seed_rows.measure_control) as registry_control_sk, seed_rows.measure_control, seed_rows.vintage_start$extra "
    "from seed_rows left join editions on seed_rows.fact = editions.fact and seed_rows.measure_key = editions.measure_key "
    "and seed_rows.value_column = editions.value_column "
    "and (seed_rows.vintage_start is null or seed_rows.vintage_start = '' or $first) "
    "and (seed_rows.vintage_end is null or seed_rows.vintage_end = '' or $second)$tail"
)
EDITION_START_CMP = "left(editions.edition_end, 4) >= seed_rows.vintage_start"
EDITION_END_CMP = "left(editions.edition_start, 4) <= seed_rows.vintage_end"
LABEL_EDITION_SQL = Template(
    "with observed as (select '$system' as code_system, cast($code_column as varchar) as code, cast($label_column as varchar) as code_label, "
    "cast($order_column as varchar) as label_order from main.$source), "
    "editions as (select code_system, code, code_label, $start as edition_start, $end as edition_end from observed$where$group) "
    "select md5(concat_ws('|', code_system, code$edition)) as $key, code_system, code, code_label, edition_start, edition_end from editions"
)
NAMED_EDITION_SQL = Template(
    "with names_published as (select 'fct_hai_hospital_window' as fact, cast(measure_id as varchar) as measure_key, measure_name, release_date "
    "from main.int_hai_hospital_windows), "
    "editions as (select fact, measure_key, measure_name as edition_label, $start as edition_start, $end as edition_end "
    "from names_published$where$group), "
    "keyed as (select fact, measure_key, edition_label as measure_label, 'score' as value_column, edition_start, edition_end from editions) "
    "select md5(concat_ws('|', fact, measure_key, value_column, edition_start)) as measure_sk, "
    "concat_ws('|', fact, measure_key, value_column, edition_start) as measure_identity, fact, measure_key, value_column, measure_label, "
    "edition_start, edition_end from keyed"
)
COUNTY_CASTS_SQL = Template(
    "with lists as (select 'int_county_adjacency_edges' as list_name, vintage, cast($column as varchar) as county_fips from main.$source "
    "where vintage is not null), "
    "latest_vintage as (select max(vintage) as vintage from lists where list_name = 'int_county_adjacency_edges'), "
    "latest as (select lists.county_fips from lists inner join latest_vintage on lists.vintage = latest_vintage.vintage "
    "where lists.list_name = 'int_county_adjacency_edges'), "
    "valid as (select cast(county_fips as varchar) as county_fips from main.int_rucc_county_codes) "
    "select md5(valid.county_fips) as county_sk, valid.county_fips, $membership as $alias$extra "
    "from valid left join latest on valid.county_fips = latest.county_fips"
)
COUNTY_ROWS_SQL = Template(
    "with lists as (select a.county_fips, $vintage as vintage from $source) "
    "select md5(county_fips) as county_sk, county_fips, max(vintage) as last_listed_vintage from lists group by county_fips"
)
RURALITY_ROWS_SQL = Template(
    "with observed as (select 'int_rucc_county_codes.rucc_code' as code_system, cast(r.rucc_code as varchar) as code, "
    "cast(r.description as varchar) as code_label, $date as label_order from $source), "
    "editions as (select code_system, code, code_label, min(label_order) as edition_start, max(label_order) as edition_end "
    "from observed where code_label is not null and label_order is not null group by code_system, code, code_label) "
    "select md5(concat_ws('|', code_system, code, edition_start)) as rurality_code_sk, code_system, code, code_label, edition_start, edition_end from editions"
)
KEY_ROWS_SQL = Template("with q as (select $columns from $source) select md5(concat_ws('|', $parts)) as $key, $parts from q")
COUNTY_FILTER_SQL = Template(
    "with counted as (select county_fips, county_fips_2023, member_sha256, count(*) as tract_count from $source where vintage = '2020' and $filter "
    "group by county_fips, county_fips_2023, member_sha256) "
    "select county_fips as old_county_fips, county_fips_2023 as planning_region_fips, tract_count, member_sha256$extra from counted"
)


def changed(sql: str, before: str, after: str) -> str:
    """The text with ``before`` replaced; a replacement that changes nothing raises, so a changed template cannot silently void a probe."""
    result = sql.replace(before, after)
    if result == sql:
        raise ValueError("Probe mutation did not change the text: " + before)
    return result


def state_sql(expression: str = "left(county_fips, 2)", alias: str = "state_fips", out: str = "state_fips", source: str = "main.int_rucc_county_codes") -> str:
    return STATE_SQL.substitute(expression=expression, alias=alias, out=out, source=source)


def tract_sql(count: str = "count(*)", source: str = "main.int_ruca_codes", group: str = TRACT_GROUP, out: str = "tract_count") -> str:
    return TRACT_SQL.substitute(count=count, source=source, group=group, out=out)


def rurality_bounds_sql(start: str = "min(label_order)", end: str = "max(label_order)", group: str = " group by code_system, code", extra: str = "") -> str:
    return RURALITY_BOUNDS_SQL.substitute(start=start, end=end, group=group, extra=extra)


def measure_bounds_sql(
    start: str = "cast(min(release_date) as varchar)", end: str = "cast(max(release_date) as varchar)", group: str = " group by fact, measure_key, value_column"
) -> str:
    return MEASURE_BOUNDS_SQL.substitute(start=start, end=end, group=group)


def county_lists_sql(
    membership: str = "latest.county_fips is not null",
    alias: str = "in_latest_county_list",
    join: str = "left join",
    condition: str = "last_listed.county_fips = latest.county_fips",
) -> str:
    return COUNTY_LISTS_SQL.substitute(membership=membership, alias=alias, join=join, condition=condition)


def county_bounds_sql(
    first: str = "min(vintage)", last: str = "max(vintage)", last_alias: str = "last_listed_vintage", source: str = "main.int_rucc_county_codes"
) -> str:
    return COUNTY_BOUNDS_SQL.substitute(first=first, last=last, last_alias=last_alias, source=source)


def bridge_keys_sql(
    fourth: str = ", editions.edition_start",
    key: str = "measure_sk",
    extra: str = "",
    first: str = EDITION_START_CMP,
    second: str = EDITION_END_CMP,
    seed: str = CONTROL_SEED,
    tail: str = "",
) -> str:
    return BRIDGE_KEYS_SQL.substitute(seed=seed, fourth=fourth, key=key, extra=extra, first=first, second=second, tail=tail)


def label_edition_sql(
    model: str = "dim_rurality_code",
    start: str = "min(label_order)",
    end: str = "max(label_order)",
    where: str = " where code_label is not null and label_order is not null",
    group: str = " group by code_system, code, code_label",
) -> str:
    drg = model == "dim_drg"
    return LABEL_EDITION_SQL.substitute(
        system="int_mup_drg_discharges.drg_cd" if drg else "int_rucc_county_codes.rucc_code",
        code_column="drg_cd" if drg else "rucc_code",
        label_column="drg_desc" if drg else "description",
        order_column="data_year" if drg else "vintage",
        source="int_mup_drg_discharges" if drg else "int_rucc_county_codes",
        start=start,
        end=end,
        where=where,
        group=group,
        edition="" if drg else ", edition_start",
        key="drg_sk" if drg else "rurality_code_sk",
    )


def named_edition_sql(
    start: str = "cast(min(release_date) as varchar)",
    end: str = "cast(max(release_date) as varchar)",
    where: str = " where release_date is not null",
    group: str = " group by fact, measure_key, measure_name",
) -> str:
    return NAMED_EDITION_SQL.substitute(start=start, end=end, where=where, group=group)


def county_filter_sql(condition: str = "county_fips like '09%'", source: str = "main.int_ruca_codes", extra: str = "") -> str:
    return COUNTY_FILTER_SQL.substitute(source=source, filter=condition, extra=extra)


def state_prefix_probes() -> list[Probe]:
    """``left(county_fips, 2)``: admitted in dim_state over the RUCC county code, into ``state_fips`` only."""
    cases: list[Probe] = []

    def state(name: str, expression: str = "left(county_fips, 2)", alias: str = "state_fips", out: str = "state_fips", **kwargs: Any) -> None:
        sql = state_sql(expression, alias, out)
        cases.append(Probe("G4_state_prefix_" + name, kwargs.pop("model", "dim_state"), kwargs.pop("sql", sql), **kwargs))

    state("admitted", failures=())
    state("entry_removed", failures=EXPR, cause="left", remove="state_prefix")
    state("wrong_column", "left(state, 2)", failures=EXPR, cause="contracted expression")
    state("wrong_width", "left(county_fips, 3)", failures=EXPR, cause="contracted expression")
    state("wrong_function", "right(county_fips, 2)", failures=EXPR, cause="right")
    state("wrong_substring", "substr(county_fips, 1, 2)", failures=EXPR, cause="substr")
    state("other_alias", alias="state_prefix", out="state_prefix", failures=EXPR, cause="left")
    state("reaches_other_output", out="state_fips, state_fips as state_name", failures=EXPR, cause="forbidden final role")
    state(
        "in_dim_county",
        model="dim_county",
        sql="with fips as (select distinct county_fips, left(county_fips, 2) as state_fips from main.int_rucc_county_codes) "
        "select md5(county_fips) as county_sk, county_fips, state_fips from fips",
        failures=EXPR,
        cause="left",
    )
    return cases


def tract_count_probes() -> list[Probe]:
    """``count(*)`` of the RUCA tract rows: admitted in bridge_county_planning_region per old county, planning region and file."""
    cases: list[Probe] = []

    def tract(name: str, **kwargs: Any) -> None:
        sql = tract_sql(
            kwargs.pop("count", "count(*)"), kwargs.pop("source", "main.int_ruca_codes"), kwargs.pop("group", TRACT_GROUP), kwargs.pop("out", "tract_count")
        )
        cases.append(Probe("G4_tract_count_" + name, kwargs.pop("model", "bridge_county_planning_region"), sql, **kwargs))

    tract("admitted", failures=())
    tract("entry_removed", failures=EXPR, cause="aggregate count_star", remove="tract_count")
    tract("in_bridge_zip_county", model="bridge_zip_county", failures=EXPR, cause="aggregate count_star")
    tract("in_dim_hospital", model="dim_hospital", failures=EXPR, cause="aggregate count_star")
    tract("other_relation", source="main.int_hospital_enrollment_rows", failures=EXPR, cause="aggregate count_star")
    tract("ungrouped", group="", failures=EXPR, cause="aggregate count_star")
    tract("without_planning_region", group=" group by county_fips, member_sha256", failures=EXPR, cause="aggregate count_star")
    tract("without_old_county", group=" group by county_fips_2023, member_sha256", failures=EXPR, cause="aggregate count_star")
    tract("column_count", count="count(county_fips)", failures=EXPR, cause="aggregate count")
    tract("sum", count="sum(county_fips)", failures=EXPR, cause="aggregate sum")
    tract("other_output", out="tract_count as weight", failures=EXPR, cause="forbidden final role")
    tract("arithmetic", count="count(*) + 1", failures=("expression_arithmetic",), cause="arithmetic")
    return cases


def rurality_bound_probes() -> list[Probe]:
    """``min`` and ``max`` of a label group's vintage in dim_rurality_code, and the same bounds in dim_drg, which has no edition."""
    cases: list[Probe] = []

    def rurality(name: str, **kwargs: Any) -> None:
        sql = rurality_bounds_sql(
            kwargs.pop("start", "min(label_order)"),
            kwargs.pop("end", "max(label_order)"),
            kwargs.pop("group", " group by code_system, code"),
            kwargs.pop("extra", ""),
        )
        cases.append(Probe("G4_label_bounds_rurality_" + name, "dim_rurality_code", sql, **kwargs))

    rurality("admitted", failures=())
    rurality("entry_removed", failures=EXPR, cause="aggregate min", remove="label_bounds")
    rurality("wrong_column", start="min(code)", failures=EXPR, cause="aggregate min")
    rurality("ungrouped", group="", failures=EXPR, cause="aggregate min")
    rurality("swapped", start="max(label_order)", end="min(label_order)", failures=EXPR, cause="forbidden final role")
    rurality("other_output", extra=", edition_start as unit", failures=EXPR, cause="forbidden final role")
    rurality("expression_operand", start="min(label_order || 'x')", failures=EXPR, cause="function ||")
    cases.append(
        Probe(
            "G4_label_bounds_in_dim_drg",
            "dim_drg",
            "with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system, cast(drg_cd as varchar) as code, data_year "
            "from main.int_mup_drg_discharges) select md5(concat_ws('|', code_system, code)) as drg_sk, code_system, code, "
            "min(data_year) as edition_start from observed group by code_system, code",
            EXPR,
            "aggregate min",
        )
    )
    return cases


def measure_bound_probes() -> list[Probe]:
    """``min`` and ``max`` of the release dates of one measure identity in dim_measure."""
    cases: list[Probe] = []

    def measure(name: str, **kwargs: Any) -> None:
        sql = measure_bounds_sql(
            kwargs.pop("start", "cast(min(release_date) as varchar)"),
            kwargs.pop("end", "cast(max(release_date) as varchar)"),
            kwargs.pop("group", " group by fact, measure_key, value_column"),
        )
        cases.append(Probe("G4_label_bounds_measure_" + name, "dim_measure", sql, **kwargs))

    measure("admitted", failures=())
    measure("entry_removed", failures=EXPR, cause="aggregate min", remove="label_bounds")
    measure("wrong_column", start="cast(min(window_end) as varchar)", end="cast(max(window_end) as varchar)", failures=EXPR, cause="aggregate min")
    measure("ungrouped", group="", failures=EXPR, cause="aggregate min")
    measure("swapped", start="cast(max(release_date) as varchar)", end="cast(min(release_date) as varchar)", failures=EXPR, cause="forbidden final role")
    measure("two_operands", start="cast(min(release_date, window_end) as varchar)", failures=EXPR, cause="aggregate min")
    measure("column_cast", start="cast(release_date as varchar)", end="cast(release_date as varchar)", group="", failures=EXPR, cause="cast to VARCHAR")
    return cases


def county_list_probes() -> list[Probe]:
    """The county-list bounds and the membership test of dim_county, over the delivered chain of list, latest vintage and latest list."""
    cases: list[Probe] = []

    def county(name: str, **kwargs: Any) -> None:
        sql = county_lists_sql(kwargs.pop("membership", "latest.county_fips is not null"), kwargs.pop("alias", "in_latest_county_list"))
        cases.append(Probe("G4_county_" + name, kwargs.pop("model", "dim_county"), kwargs.pop("sql", sql), **kwargs))

    def bounds(name: str, **kwargs: Any) -> None:
        sql = county_bounds_sql(kwargs.pop("first", "min(vintage)"), kwargs.pop("last", "max(vintage)"), kwargs.pop("last_alias", "last_listed_vintage"))
        county(name, sql=sql, **kwargs)

    county("lists_admitted", failures=())
    county("lists_entry_removed", failures=EXPR, cause="aggregate max", remove="county_list_bounds")
    county("membership_other_alias", alias="is_listed", failures=EXPR, cause="boolean expression")
    county("membership_wrong_column", membership="latest.neighbor_fips is not null", failures=EXPR, cause="boolean expression")
    county("membership_is_null", membership="latest.county_fips is null", failures=EXPR, cause="boolean expression")
    county("membership_comparison", membership="latest.county_fips = '09001'", failures=EXPR, cause="boolean expression")
    bounds("bounds_minimal_admitted", failures=())
    bounds("bounds_swapped", first="max(vintage)", last="min(vintage)", failures=EXPR, cause="forbidden final role")
    bounds("bounds_wrong_column", first="min(county_fips)", failures=EXPR, cause="aggregate min")
    bounds("bounds_other_output", last_alias="county_name", failures=EXPR, cause="forbidden final role")
    bounds("bounds_cast", first="cast(min(vintage) as varchar)", failures=EXPR, cause="cast to VARCHAR")
    county(
        "bounds_in_dim_zip",
        sql="select md5(zip_code) as zip_sk, zip_code, max(vintage) as last_listed_vintage from main.int_hsa_zip_cases group by zip_code",
        model="dim_zip",
        failures=EXPR,
        cause="aggregate max",
    )
    county(
        "membership_in_dim_zip",
        sql="select md5(zip_code) as zip_sk, zip_code, zip_code is not null as in_latest_county_list from main.int_hsa_zip_cases",
        model="dim_zip",
        failures=EXPR,
        cause="boolean expression",
    )
    return cases


def g4_entry_probes() -> list[Probe]:
    """The four gold contract allowlist entries that name a model (G4): each capability admits its one expression in its model and columns.

    A control carries the delivered shape of the expression. A refusal changes exactly one thing: the entry, the model, the source column, the
    width, the output column, the grouping or the operator around the result. The synthetic SQL is only parsed; no relation is bound.
    """
    return state_prefix_probes() + tract_count_probes() + rurality_bound_probes() + measure_bound_probes() + county_list_probes()


def rurality_key_probes() -> list[Probe]:
    """The edition part of dim_rurality_code's key: the last component, a bounded ``min`` of the vintage or a typed null."""
    cases: list[Probe] = []

    def rurality(name: str, parts: str = "code_system, code, edition_start", **kwargs: Any) -> None:
        cases.append(Probe("P2_key_" + name, kwargs.pop("model", "dim_rurality_code"), kwargs.pop("sql", RURALITY_KEY_SQL.substitute(parts=parts)), **kwargs))

    rurality("rurality_edition", failures=())
    rurality("edition_end", "code_system, code, edition_end", failures=EXPR, cause="contracted expression")
    rurality("edition_not_last", "code_system, edition_start, code", failures=EXPR, cause="contracted expression")
    rurality("extra_part", "code_system, code, edition_start, edition_end", failures=EXPR, cause="contracted expression")
    rurality("coalesced_edition", "code_system, code, coalesce(edition_start, '')", failures=EXPR, cause="contracted expression")
    rurality("edition_plain_column", sql=rurality_bounds_sql("label_order", "label_order", ""), failures=EXPR, cause="contracted expression")
    rurality("edition_start_is_max", sql=rurality_bounds_sql("max(label_order)", "max(label_order)"), failures=EXPR, cause="forbidden final role")
    rurality(
        "bound_as_code",
        sql="with observed as (select 'int_rucc_county_codes.rucc_code' as code_system, cast(vintage as varchar) as label_order "
        "from main.int_rucc_county_codes), "
        "editions as (select code_system, min(label_order) as code from observed group by code_system) "
        "select md5(concat_ws('|', code_system, code)) as rurality_code_sk, code_system, code from editions",
        failures=EXPR,
        cause="forbidden final role",
    )
    rurality(
        "edition_in_dim_drg",
        model="dim_drg",
        sql="with observed as (select 'int_mup_drg_discharges.drg_cd' as code_system, cast(drg_cd as varchar) as code, "
        "cast(data_year as varchar) as label_order from main.int_mup_drg_discharges) "
        "select md5(concat_ws('|', code_system, code, label_order)) as drg_sk, code_system, code, label_order as edition_start from observed",
        failures=EXPR,
        cause="contracted expression",
    )
    return cases


def measure_union_probes() -> list[Probe]:
    """The ordinary and split rows of dim_measure merged in one union: a key and an identity text pad or carry the edition."""
    cases: list[Probe] = []

    def union(
        name: str, ordinary_start: str = "cast(null as varchar)", split_part: str = ", editions.edition_start", identity_part: str | None = None, **kwargs: Any
    ) -> None:
        sql = MEASURE_UNION_SQL.substitute(
            ordinary_start=ordinary_start, split_part=split_part, identity_part=split_part if identity_part is None else identity_part
        )
        cases.append(Probe("P2_key_measure_" + name, "dim_measure", sql, **kwargs))

    union("union_edition", failures=())
    union("split_rows_omit_edition", split_part="", failures=EXPR, cause="omits the edition")
    union("split_rows_identity_omits_edition", identity_part="", failures=EXPR, cause="omits the edition")
    union("split_rows_edition_end", split_part=", editions.edition_end", failures=EXPR, cause="contracted expression")
    union("ordinary_arm_has_edition", ordinary_start="cast('2020' as varchar)", failures=EXPR, cause="omits the edition")
    return cases


def bridge_edition_probes() -> list[Probe]:
    """The control bridge: its measure key adds the edition start and its join compares the edition years with the seed range."""
    cases: list[Probe] = []
    open_join = {"first": "seed_rows.vintage_start is not null", "second": "seed_rows.vintage_end is not null"}

    def bridge(name: str, **kwargs: Any) -> None:
        sql = bridge_keys_sql(
            kwargs.pop("fourth", ", editions.edition_start"),
            kwargs.pop("key", "measure_sk"),
            kwargs.pop("extra", ""),
            kwargs.pop("first", EDITION_START_CMP),
            kwargs.pop("second", EDITION_END_CMP),
        )
        cases.append(Probe(name, kwargs.pop("model", "bridge_measure_control"), sql, **kwargs))

    bridge("P2_key_bridge_edition", failures=(), **open_join)
    bridge("P2_key_bridge_other_column", fourth=", editions.edition_end", failures=EXPR, cause="contracted expression", **open_join)
    bridge("P2_key_bridge_other_model", model="bridge_zip_county", failures=EXPR, cause="md5 is allowed only", **open_join)
    bridge("P2_left_edition_join", failures=())
    bridge("P2_left_entry_removed", failures=EXPR, cause="function left is not approved", remove="edition_year")
    bridge("P2_left_other_column", first="left(editions.measure_key, 4) >= seed_rows.vintage_start", failures=EXPR, cause="function left is not approved")
    bridge("P2_left_wrong_width", first="left(editions.edition_end, 5) >= seed_rows.vintage_start", failures=EXPR, cause="function left is not approved")
    bridge("P2_left_reversed", first="left(editions.edition_end, 4) <= seed_rows.vintage_start", failures=EXPR, cause="function left is not approved")
    bridge("P2_left_swapped_bounds", second="left(editions.edition_end, 4) <= seed_rows.vintage_end", failures=EXPR, cause="function left is not approved")
    bridge(
        "P2_left_other_seed_column", first="left(editions.edition_end, 4) >= seed_rows.measure_control", failures=EXPR, cause="function left is not approved"
    )
    bridge("P2_left_other_function", first="right(editions.edition_end, 4) >= seed_rows.vintage_start", failures=EXPR, cause="function right is not approved")
    bridge("P2_left_in_output", extra=", left(editions.edition_end, 4) as edition_year", failures=EXPR, cause="function left is not approved")
    cases.append(
        Probe(
            "P2_left_in_where",
            "bridge_measure_control",
            "select md5(concat_ws('|', fact, measure_key, value_column)) as measure_sk, md5(measure_control) as registry_control_sk, measure_control "
            "from main.dim_measure where left(edition_end, 4) >= vintage_start",
            EXPR,
            "function left is not approved",
        )
    )
    cases.append(
        Probe(
            "P2_left_in_dim_hospital",
            "dim_hospital",
            "select md5(a.ccn) as hospital_sk, a.ccn from main.int_cost_reports a "
            "inner join main.dim_measure editions on left(editions.edition_end, 4) >= a.ccn",
            EXPR,
            "function left is not approved",
        )
    )
    return cases


def rurality_selection_probes() -> list[Probe]:
    """The label selection with editions in dim_rurality_code and dim_drg: one row per published label, grouped by the keys and the label."""
    cases: list[Probe] = []

    def edition(name: str, **kwargs: Any) -> None:
        model = kwargs.pop("model", "dim_rurality_code")
        sql = label_edition_sql(
            model,
            kwargs.pop("start", "min(label_order)"),
            "max(label_order)",
            kwargs.pop("where", " where code_label is not null and label_order is not null"),
            kwargs.pop("group", " group by code_system, code, code_label"),
        )
        cases.append(Probe("P2_a48_edition_" + name, model, sql, **kwargs))

    edition("rurality", failures=())
    edition("in_dim_drg", model="dim_drg", failures=EXPR, cause="A48")
    edition("null_constant", start="cast(null as varchar)", failures=EXPR, cause="A48")
    edition("wrong_aggregate", start="max(label_order)", failures=EXPR, cause="A48")
    edition("unguarded", where=" where code_label is not null", failures=EXPR, cause="A48")
    edition("order_guard_only", where=" where label_order is not null", failures=())
    edition("group_without_label", group=" group by code_system, code", failures=EXPR, cause="A48")
    edition("group_without_key", group=" group by code_system, code_label", failures=EXPR, cause="A48")
    edition("group_with_extra", group=" group by code_system, code, code_label, label_order", failures=EXPR, cause="A48")
    return cases


def measure_selection_probes() -> list[Probe]:
    """The label selection with editions in dim_measure: one row per published name of a measure, bounded by the first and last release date."""
    cases: list[Probe] = []

    def measure_edition(name: str, **kwargs: Any) -> None:
        sql = named_edition_sql(
            kwargs.pop("start", "cast(min(release_date) as varchar)"),
            where=kwargs.pop("where", " where release_date is not null"),
            group=kwargs.pop("group", " group by fact, measure_key, measure_name"),
        )
        cases.append(Probe("P2_a48_edition_measure_" + name, "dim_measure", sql, **kwargs))

    measure_edition("admitted", failures=())
    measure_edition("null_constant", start="cast(null as varchar)", failures=EXPR, cause="A48")
    measure_edition("unguarded", where="", failures=EXPR, cause="A48")
    measure_edition("group_without_label", group=" group by fact, measure_key", failures=EXPR, cause="A48")
    measure_edition("group_without_key", group=" group by fact, measure_name", failures=EXPR, cause="A48")
    return cases


def delivered_edition_probes(rendered: dict[str, str]) -> list[Probe]:
    """The delivered edition models with one part of their selection or key taken out: each must reach the label-selection or the key refusal."""
    cases: list[Probe] = []
    for name, model, before, after in (
        ("rurality_null_constant", "dim_rurality_code", "min(observed.label_order) as edition_start", "cast(null as varchar) as edition_start"),
        ("rurality_unguarded", "dim_rurality_code", " and observed.label_order is not null", ""),
        (
            "measure_null_constant",
            "dim_measure",
            "cast(min(names_published.release_date) as varchar) as edition_start",
            "cast(null as varchar) as edition_start",
        ),
        ("measure_unguarded", "dim_measure", " and names_published.release_date is not null", ""),
    ):
        cases.append(Probe("P2_a48_delivered_" + name, model, changed(rendered[model], before, after), EXPR, "A48"))
    cases.append(
        Probe(
            "P2_key_delivered_rurality_omits_edition",
            "dim_rurality_code",
            changed(rendered["dim_rurality_code"], "combined.code, combined.edition_start)) as rurality_code_sk", "combined.code)) as rurality_code_sk"),
            EXPR,
            "omits the edition",
        )
    )
    return cases


def county_cast_probes() -> list[Probe]:
    """Casts of the county lists in dim_county: the adjacency counties and neighbors are county code inputs; no other column or relation is."""
    cases: list[Probe] = []
    adjacency = "int_county_adjacency_edges"

    def county(name: str, column: str = "county_fips", source: str = adjacency, **kwargs: Any) -> None:
        sql = COUNTY_CASTS_SQL.substitute(
            column=column,
            source=source,
            membership=kwargs.pop("membership", "latest.county_fips is not null"),
            alias=kwargs.pop("alias", "in_latest_county_list"),
            extra=kwargs.pop("extra", ""),
        )
        cases.append(Probe("P2_county_" + name, kwargs.pop("model", "dim_county"), kwargs.pop("sql", sql), **kwargs))

    county("membership_of_normalized_code", failures=())
    county("normalized_to_other_output", membership="latest.county_fips", alias="county_name", failures=EXPR, cause="normalized source reaches forbidden")
    county("normalized_code_also_to_other_output", extra=", latest.county_fips as county_name", failures=EXPR, cause="normalized source reaches forbidden")
    county("adjacency_cast", failures=())
    county("adjacency_neighbor_cast", column="neighbor_fips", failures=())
    county("adjacency_cast_vintage", column="vintage", failures=EXPR, cause="cast to VARCHAR")
    county("adjacency_cast_other_column", column="shared_border_length_m", failures=EXPR, cause="cast to VARCHAR")
    county("adjacency_cast_other_relation", column="neighbor_fips", source="int_hsa_zip_cases", failures=EXPR, cause="cast to VARCHAR")
    county(
        "adjacency_cast_in_dim_zip",
        model="dim_zip",
        sql="with lists as (select cast(county_fips as varchar) as zip_code from main.int_county_adjacency_edges) "
        "select md5(zip_code) as zip_sk, zip_code from lists",
        failures=EXPR,
        cause="cast to VARCHAR",
    )
    return cases


def county_filter_probes() -> list[Probe]:
    """``county_fips like '09%'`` over the RUCA rows: a positive conjunct of the WHERE clause of bridge_county_planning_region."""
    cases: list[Probe] = []

    def like(name: str, condition: str = "county_fips like '09%'", source: str = "main.int_ruca_codes", **kwargs: Any) -> None:
        sql = kwargs.pop("sql", county_filter_sql(condition, source, kwargs.pop("extra", "")))
        cases.append(Probe("P2_like_" + name, kwargs.pop("model", "bridge_county_planning_region"), sql, **kwargs))

    like("admitted", failures=())
    like("entry_removed", failures=EXPR, cause="function ~~ is not approved", remove="county_prefix")
    like("other_model", model="bridge_zip_county", failures=EXPR, cause="function ~~ is not approved")
    like("other_column", condition="county_fips_2023 like '09%'", failures=EXPR, cause="function ~~ is not approved")
    like("other_pattern", condition="county_fips like '0%'", failures=EXPR, cause="function ~~ is not approved")
    like("wildcard_inside", condition="county_fips like '0_%'", failures=EXPR, cause="function ~~ is not approved")
    like("other_relation", source="main.int_pos_hospital_snapshots", failures=EXPR, cause="function ~~ is not approved")
    like("not_like", condition="county_fips not like '09%'", failures=EXPR, cause="function !~~ is not approved")
    like("ilike", condition="county_fips ilike '09%'", failures=EXPR, cause="function ~~* is not approved")
    like("escape", condition="county_fips like '09%' escape '!'", failures=EXPR, cause="function like_escape is not approved")
    like("in_output", extra=", county_fips like '09%' as in_connecticut", failures=EXPR, cause="function ~~ is not approved")
    like(
        "in_having",
        sql="select county_fips as old_county_fips, county_fips_2023 as planning_region_fips, count(*) as tract_count, member_sha256 from main.int_ruca_codes "
        "where vintage = '2020' group by county_fips, county_fips_2023, member_sha256 having county_fips like '09%'",
        failures=EXPR,
        cause="function ~~ is not approved",
    )
    like(
        "in_dim_hospital",
        model="dim_hospital",
        sql="select md5(ccn) as hospital_sk, ccn from main.int_cost_reports where ccn like '09%'",
        failures=EXPR,
        cause="function ~~ is not approved",
    )
    return cases


def edition_probes(rendered: dict[str, str]) -> list[Probe]:
    """The edition-split gold models: the edition part of a key, the label selection with editions and three narrow forms.

    Each control carries the delivered shape; a refusal changes one thing (the model, the column, the width, the pairing, the aggregate, a filter or the
    grouping). The mutations of delivered text raise when they no longer change it, so a model change cannot silently void a probe.
    """
    return (
        rurality_key_probes()
        + measure_union_probes()
        + bridge_edition_probes()
        + rurality_selection_probes()
        + measure_selection_probes()
        + delivered_edition_probes(rendered)
        + county_cast_probes()
        + county_filter_probes()
    )


def filtered_bound_probes(rendered: dict[str, str]) -> list[Probe]:
    """A bound is granted only to a plain aggregate; a filter, ``distinct`` or ``order by`` leaves it a refused aggregate."""
    cases: list[Probe] = []
    null_rurality, null_measure = "filter (where label_order is null)", "filter (where release_date is null)"
    for name, start, end in (
        ("filter_false", "min(label_order) filter (where false)", "max(label_order) filter (where false)"),
        ("filter_null_column", f"min(label_order) {null_rurality}", f"max(label_order) {null_rurality}"),
        ("distinct", "min(distinct label_order)", "max(distinct label_order)"),
        ("order_by", "min(label_order order by label_order)", "max(label_order)"),
    ):
        sql = label_edition_sql(start=start, end=end)
        cases.append(Probe("P3_h1_rurality_" + name, "dim_rurality_code", sql, EXPR, "" if name == "filter_false" else "aggregate m"))
    for name, start, end in (
        ("filter_false", "cast(min(release_date) filter (where false) as varchar)", "cast(max(release_date) filter (where false) as varchar)"),
        ("filter_null_column", f"cast(min(release_date) {null_measure} as varchar)", f"cast(max(release_date) {null_measure} as varchar)"),
    ):
        cases.append(
            Probe("P3_h1_measure_" + name, "dim_measure", named_edition_sql(start=start, end=end), EXPR, "" if name == "filter_false" else "aggregate m")
        )
    for model, operand in (("dim_rurality_code", "observed.label_order"), ("dim_measure", "names_published.release_date")):
        for name, modifier in (("filter_false", "filter (where false)"), ("filter_null_column", f"filter (where {operand} is null)")):
            text = changed(changed(rendered[model], f"min({operand})", f"min({operand}) {modifier}"), f"max({operand})", f"max({operand}) {modifier}")
            cases.append(Probe(f"P3_h1_delivered_{model.removeprefix('dim_')}_{name}", model, text, EXPR, "" if name == "filter_false" else "aggregate m"))
    bounds = county_bounds_sql(first="min(vintage) filter (where vintage is null)")
    cases.append(Probe("P3_h1_county_bounds_filter", "dim_county", bounds, EXPR, "aggregate min"))
    return cases


def bound_input_probes() -> list[Probe]:
    """The label bounds read the date of the relation that carries the label, from the occurrence that carries it."""
    published = (
        "names_published as (select 'fct_hai_hospital_window' as fact, cast(measure_id as varchar) as measure_key, measure_name, release_date "
        "from main.int_hai_hospital_windows), "
    )
    other_date = (
        "names_published as (select 'fct_hai_hospital_window' as fact, cast(w.measure_id as varchar) as measure_key, w.measure_name, c.release_date "
        "from main.int_hai_hospital_windows w cross join main.int_cost_reports c), "
    )
    rucc = "main.int_rucc_county_codes r"
    sources = (
        (
            "rurality_bound_unrelated_date",
            "dim_rurality_code",
            RURALITY_ROWS_SQL.substitute(date="h.release_date", source=rucc + " cross join main.int_hai_hospital_windows h"),
        ),
        (
            "rurality_bound_other_relation",
            "dim_rurality_code",
            RURALITY_ROWS_SQL.substitute(date="b.vintage", source=rucc + " cross join main.int_ruca_codes b"),
        ),
        (
            "rurality_bound_label_from_other_row",
            "dim_rurality_code",
            RURALITY_ROWS_SQL.substitute(date="cast(o.vintage as varchar)", source=rucc + " cross join main.int_rucc_county_codes o"),
        ),
        ("measure_bound_other_relation", "dim_measure", changed(named_edition_sql(), published, other_date)),
        ("county_bounds_other_relation", "dim_county", county_bounds_sql(source="main.int_ruca_codes")),
        (
            "county_bounds_other_row",
            "dim_county",
            COUNTY_ROWS_SQL.substitute(source="main.int_county_adjacency_edges a cross join main.int_rucc_county_codes b", vintage="b.vintage"),
        ),
    )
    causes = {"rurality_bound_label_from_other_row": "A48", "county_bounds_other_relation": "aggregate max", "county_bounds_other_row": "aggregate max"}
    cases = [Probe("P3_m1_" + name, model, sql, EXPR, causes.get(name, "aggregate min")) for name, model, sql in sources]
    return [
        Probe("P3_m1_county_bounds_same_row", "dim_county", COUNTY_ROWS_SQL.substitute(source="main.int_county_adjacency_edges a", vintage="a.vintage"), ())
    ] + cases


def key_input_probes() -> list[Probe]:
    """The state prefix reads the RUCC county code, and only the control bridge hashes the edition start of dim_measure."""
    rurality_key = KEY_ROWS_SQL.substitute(
        columns="'int_rucc_county_codes.rucc_code' as code_system, cast(r.rucc_code as varchar) as code, m.edition_start",
        source="main.int_rucc_county_codes r cross join main.dim_measure m",
        parts="code_system, code, edition_start",
        key="rurality_code_sk",
    )
    measure_key = (
        "with q as (select fact, measure_key, value_column, edition_start from main.dim_measure) "
        "select md5(concat_ws('|', fact, measure_key, value_column, edition_start)) as measure_sk, "
        "concat_ws('|', fact, measure_key, value_column, edition_start) as measure_identity, fact, measure_key, value_column, edition_start from q"
    )
    renamed = (
        "with q as (select state_code, ccn as county_fips from main.int_cost_reports) "
        "select md5(state_code) as state_sk, state_code, left(county_fips, 2) as state_fips from q"
    )
    return [
        Probe("P3_m1_state_prefix_renamed_ccn", "dim_state", renamed, EXPR, "contracted expression"),
        Probe("P3_m1_state_prefix_other_relation", "dim_state", state_sql(source="main.int_pos_hospital_snapshots"), EXPR, "contracted expression"),
        Probe("P3_m1_rurality_key_measure_edition", "dim_rurality_code", rurality_key, EXPR, "contracted expression"),
        Probe("P3_m1_measure_key_passthrough", "dim_measure", measure_key, EXPR, "contracted expression"),
    ]


def latest_list_probes() -> list[Probe]:
    """The membership test reads the county code of the latest adjacency-list selection and is left joined on the county code."""
    base = county_lists_sql()
    latest = (
        "latest as (select lists.county_fips from lists inner join latest_vintage on lists.vintage = latest_vintage.vintage "
        "where lists.list_name = 'int_county_adjacency_edges'), "
    )
    latest_vintage = "latest_vintage as (select max(vintage) as vintage from lists where list_name = 'int_county_adjacency_edges'), "
    sources = (
        ("membership_last_listed", changed(base, "latest.county_fips is not null", "last_listed.county_fips is not null"), "contracted expression"),
        (
            "membership_unrelated_relation",
            "select md5(county_fips) as county_sk, county_fips, county_fips is not null as in_latest_county_list from main.int_ruca_codes",
            "contracted expression",
        ),
        (
            "latest_without_vintage_join",
            changed(base, latest, "latest as (select lists.county_fips from lists where lists.list_name = 'int_county_adjacency_edges'), "),
            "contracted expression",
        ),
        (
            "latest_other_list",
            changed(base, "where lists.list_name = 'int_county_adjacency_edges'), ", "where lists.list_name = 'int_rucc_county_codes'), "),
            "contracted expression",
        ),
        (
            "latest_vintage_unfiltered",
            changed(base, latest_vintage, "latest_vintage as (select max(vintage) as vintage from lists), "),
            "contracted expression",
        ),
        (
            "latest_vintage_other_list",
            changed(base, latest_vintage, latest_vintage.replace("'int_county_adjacency_edges'", "'int_rucc_county_codes'")),
            "contracted expression",
        ),
        (
            "latest_union_extra_rows",
            changed(base, latest, latest.replace("), ", " union select county_fips from lists), ")),
            "contracted expression",
        ),
        ("membership_inner_join", county_lists_sql(join="inner join"), "left join"),
        ("membership_not_joined_on_county", county_lists_sql(condition="latest.county_fips = '09001'"), "left join"),
    )
    return [Probe("P3_m2_" + name, "dim_county", sql, EXPR, cause) for name, sql, cause in sources]


def tract_grain_probes() -> list[Probe]:
    """The tract count is a plain ``count(*)``, grouped by exactly the old county, planning region and file, and only a select item."""
    having = changed(
        tract_sql(), "group by county_fips, county_fips_2023, member_sha256", "group by county_fips, county_fips_2023, member_sha256 having count(*) > 1"
    )
    sources = (
        ("filter_false", tract_sql(count="count(*) filter (where false)")),
        ("extra_group", tract_sql(group=TRACT_GROUP + ", geography_id")),
        ("rollup", tract_sql(group=" group by rollup (county_fips, county_fips_2023, member_sha256)")),
        ("having", having),
    )
    return [Probe("P3_m3_tract_count_" + name, "bridge_county_planning_region", sql, EXPR, "aggregate count_star") for name, sql in sources]


def seed_side_probes() -> list[Probe]:
    """The seed side of the edition-year comparison is the physical seed range of the left source, nothing renamed or borrowed."""
    projected = "value_column, vintage_start, vintage_end, measure_control from"
    other_row = bridge_keys_sql(
        first="seed_rows.vintage_start is not null",
        second="seed_rows.vintage_end is not null",
        tail=" inner join main.impact_measures_bridge other_seed on other_seed.fact = seed_rows.fact "
        "and (other_seed.vintage_start is null or left(editions.edition_end, 4) >= other_seed.vintage_start)",
    )
    sources = (
        ("seed_renamed_control", changed(bridge_keys_sql(), projected, "value_column, measure_control as vintage_start, vintage_end, measure_control from")),
        ("seed_renamed_source", changed(bridge_keys_sql(), projected, "value_column, source_seed as vintage_start, vintage_end, measure_control from")),
        ("seed_other_relation", bridge_keys_sql(seed="main.int_cost_reports")),
        ("seed_other_row", other_row),
    )
    return [Probe("P3_m4_" + name, "bridge_measure_control", sql, EXPR, "function left is not approved") for name, sql in sources]


def predicate_context_probes() -> list[Probe]:
    """The county like and the year comparison keep their clause and their enclosing operators."""
    sources = (
        ("like_nested_having", county_filter_sql("exists (select 1 from main.int_ruca_codes r group by r.county_fips having r.county_fips like '09%')")),
        ("like_nested_where", county_filter_sql("exists (select 1 from main.int_ruca_codes r where r.county_fips like '09%')")),
        ("like_negated", county_filter_sql("not (county_fips like '09%')")),
        ("like_is_false", county_filter_sql("(county_fips like '09%') is false")),
        ("like_in_or", county_filter_sql("(county_fips like '09%' or geography_type = 'tract')")),
    )
    cases = [
        Probe("P3_m5_" + name, "bridge_county_planning_region", sql, EXPR, "" if name == "like_is_false" else "function ~~ is not approved")
        for name, sql in sources
    ]
    base = bridge_keys_sql()
    guarded = "and (seed_rows.vintage_start is null or seed_rows.vintage_start = '' or " + EDITION_START_CMP + ")"
    years = (
        ("year_nested_having", bridge_keys_sql(first=f"exists (select 1 having {EDITION_START_CMP})")),
        ("year_negated", bridge_keys_sql(first=f"not ({EDITION_START_CMP})")),
        ("year_outside_or", changed(base, guarded, "and " + EDITION_START_CMP)),
    )
    return cases + [Probe("P3_m5_" + name, "bridge_measure_control", sql, EXPR, "function left is not approved") for name, sql in years]


def near_delivered_probes(rendered: dict[str, str]) -> list[Probe]:
    """Near-delivered expressions that leave the approved bound: a filtered aggregate, a borrowed column, row or relation, a negated or nested predicate."""
    return (
        filtered_bound_probes(rendered)
        + key_input_probes()
        + bound_input_probes()
        + latest_list_probes()
        + tract_grain_probes()
        + seed_side_probes()
        + predicate_context_probes()
    )


PIN_CAUSE = "pinned to the delivered"
STAND_IN_LATEST = (
    "latest as (select lists.county_fips from lists inner join latest_vintage on lists.vintage = latest_vintage.vintage "
    "where lists.list_name = 'int_county_adjacency_edges'), "
)
STAND_IN_LATEST_VINTAGE = "latest_vintage as (select max(vintage) as vintage from lists where list_name = 'int_county_adjacency_edges'), "
ADJACENCY_LITERAL, RUCC_LITERAL = "'int_county_adjacency_edges' as list_name", "'int_rucc_county_codes' as list_name"
NULL_BOUNDS = ("filter (where edition_start is null)", "filter (where edition_end is null)")
FALSE_BOUNDS = ("filter (where false)", "filter (where false)")
PLAIN_BOUNDS = ("", "")
RESTRICTED_LATEST = Template("latest as (select county_fips from actual_latest$tail), ")
RESTRICTED_DELIVERED_LATEST = Template("latest as (select county_fips from actual_latest$tail),\nlast_listed as (")
STAND_IN_RURALITY_ANY_VALUE = Template(
    "), filtered_editions as (select code_system, code, code_label, any_value(edition_start) $start as edition_start, "
    "any_value(edition_end) $end as edition_end from editions group by code_system, code, code_label) select md5("
)
STAND_IN_MEASURE_ANY_VALUE = Template(
    "filtered_editions as (select fact, measure_key, edition_label, any_value(edition_start) $start as edition_start, "
    "any_value(edition_end) $end as edition_end from editions group by fact, measure_key, edition_label), "
    "keyed as (select fact, measure_key, edition_label as measure_label, 'score' as value_column, edition_start, edition_end from filtered_editions) "
)
DELIVERED_RURALITY_ANY_VALUE = Template(
    "editions as (select code_system, code, code_label, any_value(edition_start) $start as edition_start, "
    "any_value(edition_end) $end as edition_end from raw_editions group by code_system, code, code_label),\ncombined as ("
)
DELIVERED_MEASURE_ANY_VALUE = Template(
    "editions as (select fact, measure_key, edition_label, any_value(edition_start) $start as edition_start, "
    "any_value(edition_end) $end as edition_end from raw_editions group by fact, measure_key, edition_label),\nbase as ("
)


def changed_once(sql: str, before: str, after: str) -> str:
    """The text with its one occurrence of ``before`` replaced; a missing or repeated match raises, so a probe changes exactly the element it names."""
    if sql.count(before) != 1:
        raise ValueError(f"Probe mutation must match exactly once ({sql.count(before)} found): {before}")
    return sql.replace(before, after)


def swapped(sql: str, first: str, second: str) -> str:
    """The text with every ``first`` and every ``second`` exchanged."""
    return changed(changed(changed(sql, first, "\0"), second, first), "\0", second)


def stand_in_county(name: str) -> str:
    """The delivered chain of dim_county's list, latest vintage and latest list as a small query, with one change that alters its meaning."""
    base = county_lists_sql()
    narrowed = "from (select * from lists where $rows) narrowed where"

    def restricted(tail: str) -> str:
        return changed_once(base, STAND_IN_LATEST, STAND_IN_LATEST.replace("latest as", "actual_latest as") + RESTRICTED_LATEST.substitute(tail=tail))

    def vintage_over(rows: str) -> str:
        return changed_once(base, STAND_IN_LATEST_VINTAGE, STAND_IN_LATEST_VINTAGE.replace("from lists where", Template(narrowed).substitute(rows=rows)))

    return {
        "after_filter_false": lambda: restricted(" where false"),
        "after_filter_county": lambda: restricted(" where county_fips <> '09110'"),
        "after_limit_zero": lambda: restricted(" limit 0"),
        "after_except_self": lambda: restricted(" except select county_fips from actual_latest"),
        "after_intersect_empty": lambda: restricted(" intersect select county_fips from actual_latest where false"),
        "max_over_old_vintage": lambda: vintage_over("vintage = '2013'"),
        "max_over_empty": lambda: vintage_over("false"),
        "extra_join_false": lambda: county_lists_sql(condition="last_listed.county_fips = latest.county_fips and false"),
        "projects_vintage": lambda: changed_once(
            base, STAND_IN_LATEST, STAND_IN_LATEST.replace("select lists.county_fips", "select lists.vintage as county_fips")
        ),
        "swapped_list_literals": lambda: swapped(base, "select " + ADJACENCY_LITERAL, "select " + RUCC_LITERAL),
        "positive": lambda: base,
    }[name]()


def delivered_county(text: str, name: str) -> str:
    """The delivered dim_county with the same change made to its own latest-list chain."""
    latest = "latest as (\n    select lists.county_fips\n    from lists\n    inner join latest_vintage"
    vintage = "select max(vintage) as vintage\n    from lists\n    where list_name = 'int_county_adjacency_edges'"
    renamed = changed_once(text, latest, latest.replace("latest as", "actual_latest as"))

    def restricted(tail: str) -> str:
        return changed_once(renamed, "last_listed as (", RESTRICTED_DELIVERED_LATEST.substitute(tail=tail))

    return {
        "after_filter_false": lambda: restricted(" where false"),
        "after_filter_county": lambda: restricted(" where county_fips <> '09110'"),
        "after_limit_zero": lambda: restricted(" limit 0"),
        "after_except_self": lambda: restricted(" except select county_fips from actual_latest"),
        "after_intersect_empty": lambda: restricted(" intersect select county_fips from actual_latest where false"),
        "max_over_old_vintage": lambda: changed_once(
            text, vintage, vintage.replace("from lists", "from (select * from lists where vintage = '2013') as narrowed")
        ),
        "max_over_empty": lambda: changed_once(text, vintage, vintage.replace("from lists", "from (select * from lists where false) as narrowed")),
        "extra_join_false": lambda: changed_once(text, "on valid.county_fips = latest.county_fips", "on valid.county_fips = latest.county_fips and false"),
        "projects_vintage": lambda: changed_once(text, latest, latest.replace("select lists.county_fips", "select lists.vintage as county_fips")),
        "swapped_list_literals": lambda: swapped(text, ADJACENCY_LITERAL, RUCC_LITERAL),
    }[name]()


def stand_in_any_value(model: str, bounds: tuple[str, str]) -> str:
    """The positive edition selection with a later grouped CTE that re-reads each bound through ``any_value``."""
    start, end = bounds
    if model == "dim_rurality_code":
        text = changed_once(label_edition_sql(), "edition_start, edition_end from editions", "edition_start, edition_end from filtered_editions")
        return changed_once(text, ") select md5(", STAND_IN_RURALITY_ANY_VALUE.substitute(start=start, end=end))
    keyed = "keyed as (select fact, measure_key, edition_label as measure_label, 'score' as value_column, edition_start, edition_end from editions) "
    return changed_once(named_edition_sql(), keyed, STAND_IN_MEASURE_ANY_VALUE.substitute(start=start, end=end))


def delivered_any_value(text: str, model: str, bounds: tuple[str, str]) -> str:
    """The delivered model whose edition CTE is re-read by a later grouped CTE through ``any_value``."""
    start, end = bounds
    if model == "dim_rurality_code":
        opened, closing, later = "editions as (\n    select\n        observed.code_system,", "combined as (", DELIVERED_RURALITY_ANY_VALUE
    else:
        opened, closing, later = "editions as (\n    select\n        names_published.fact,", "base as (", DELIVERED_MEASURE_ANY_VALUE
    return changed_once(changed_once(text, opened, "raw_" + opened), closing, later.substitute(start=start, end=end))


def borrowed_row_probes(rendered: dict[str, str]) -> list[Probe]:
    """The seed range or the edition bounds of the control bridge come from another row of their own table."""
    seed_rows = "seed_rows as (select fact, measure_key, value_column, vintage_start, vintage_end, measure_control from main.registry_measure_sources_bridge)"
    borrowed_seed = (
        "seed_rows as (select a.fact, a.measure_key, a.value_column, b.vintage_start, a.vintage_end, a.measure_control "
        "from main.registry_measure_sources_bridge a cross join main.registry_measure_sources_bridge b)"
    )
    editions = "editions as (select fact, measure_key, value_column, edition_start, edition_end from main.dim_measure where edition_start is not null)"
    borrowed_edition = (
        "editions as (select a.fact, a.measure_key, a.value_column, b.edition_start, b.edition_end "
        "from main.dim_measure a cross join main.dim_measure b where b.edition_start is not null)"
    )
    text = rendered["bridge_measure_control"]
    seed_pairs = "from seed_rows as seed_rows"
    editions_opened = (
        "    select\n        fact,\n        measure_key,\n        value_column,\n        edition_start,\n        edition_end\n"
        '    from main."dim_measure"\n    where edition_start is not null'
    )
    borrowed_rows = (
        "borrowed as (select a.fact, a.measure_key, a.value_column, b.vintage_start, a.vintage_end, a.measure_control, a.source_seed, a.source_row "
        "from seed_rows a cross join seed_rows b),\n"
    )
    other_edition = (
        "    select a.fact, a.measure_key, a.value_column, b.edition_start, b.edition_end\n"
        '    from main."dim_measure" a cross join main."dim_measure" b\n    where b.edition_start is not null'
    )
    delivered_seed = changed_once(changed_once(text, "keyed as (", borrowed_rows + "keyed as ("), seed_pairs, "from borrowed as seed_rows")
    cases = (
        ("P4_m4_seed_bounds_other_row", changed_once(bridge_keys_sql(), seed_rows, borrowed_seed)),
        ("P4_m4_edition_bounds_other_row", changed_once(bridge_keys_sql(), editions, borrowed_edition)),
        ("P4_d_bridge_seed_bounds_other_row", delivered_seed),
        ("P4_d_bridge_edition_bounds_other_row", changed_once(text, editions_opened, other_edition)),
    )
    return [Probe(name, "bridge_measure_control", sql, EXPR, PIN_CAUSE, real_pins=True) for name, sql in cases]


def pinned_query_reproducers(rendered: dict[str, str]) -> list[Probe]:
    """Changes that keep the evidence of a pinned capability and alter its meaning, each as a small query and as a change of the delivered text."""
    cases: list[Probe] = []
    for label, bounds in (("null", NULL_BOUNDS), ("false", FALSE_BOUNDS), ("plain", PLAIN_BOUNDS)):
        for model, short in (("dim_rurality_code", "rurality"), ("dim_measure", "measure")):
            cases.append(Probe(f"P4_h1r2_{short}_any_value_{label}", model, stand_in_any_value(model, bounds), EXPR, PIN_CAUSE, real_pins=True))
            if label != "plain":
                cases.append(
                    Probe(f"P4_d_{short}_any_value_{label}", model, delivered_any_value(rendered[model], model, bounds), EXPR, PIN_CAUSE, real_pins=True)
                )
    names = ("after_filter_false", "after_filter_county", "after_limit_zero", "after_except_self", "after_intersect_empty", "max_over_old_vintage")
    for name in (*names, "max_over_empty", "extra_join_false", "projects_vintage", "swapped_list_literals"):
        group = "m2b" if name in {"projects_vintage", "swapped_list_literals"} else "m2a"
        cases.append(Probe(f"P4_{group}_{name}", "dim_county", stand_in_county(name), EXPR, PIN_CAUSE, real_pins=True))
        cases.append(Probe(f"P4_d_county_{name}", "dim_county", delivered_county(rendered["dim_county"], name), EXPR, PIN_CAUSE, real_pins=True))
    cases.append(Probe("P4_m2a_latest_positive", "dim_county", stand_in_county("positive"), EXPR, PIN_CAUSE, real_pins=True))
    return cases + borrowed_row_probes(rendered)


MUTATIONS: dict[str, tuple[tuple[str, str, str], ...]] = {
    "dim_rurality_code": (
        ("modifier", "min(observed.label_order) as edition_start", "min(distinct observed.label_order) as edition_start"),
        ("filter", " and observed.label_order is not null", ""),
        ("join_type", "inner join split_keys", "left join split_keys"),
        ("grouping", "group by observed.code_system, observed.code, observed.code_label", "group by observed.code_system, observed.code"),
        ("source_table", 'from main."int_rucc_county_codes"', 'from main."int_ruca_codes"'),
        ("literal", "where identity_rule = 'split'", "where identity_rule = 'merged'"),
    ),
    "dim_measure": (
        ("modifier", "cast(min(names_published.release_date) as varchar)", "cast(min(distinct names_published.release_date) as varchar)"),
        ("filter", " and names_published.release_date is not null", ""),
        ("join_type", "inner join editions", "left join editions"),
        (
            "grouping",
            "group by names_published.fact, names_published.measure_key, names_published.measure_name",
            "group by names_published.fact, names_published.measure_key",
        ),
        (
            "source_table",
            'from main."int_hai_state_windows"\n    where measure_id is not null and measure_name is not null',
            'from main."int_hai_national_windows"\n    where measure_id is not null and measure_name is not null',
        ),
        ("literal", "identity_rules.identity_rule = 'split' and", "identity_rules.identity_rule = 'merged' and"),
    ),
    "dim_county": (
        ("modifier", "select max(vintage) as vintage", "select max(distinct vintage) as vintage"),
        ("filter", "where vintage is not null and neighbor_fips is not null", "where neighbor_fips is not null"),
        ("join_type", "inner join latest_vintage", "left join latest_vintage"),
        ("grouping", "group by county_fips\n)", "group by county_fips, vintage\n)"),
        ("source_table", 'from main."int_acs_county_values"\n    where vintage is not null', 'from main."int_ruca_codes"\n    where vintage is not null'),
        ("literal", "'int_acs_county_values' as list_name", "'int_acs_county_vals' as list_name"),
    ),
    "bridge_measure_control": (
        ("select_modifier", "select\n    measure_sk,", "select distinct\n    measure_sk,"),
        ("filter", "    where edition_start is not null", ""),
        ("join_type", "left join editions as editions", "inner join editions as editions"),
        ("source_table", 'from main."registry_measure_sources_bridge"', 'from main."impact_measures_bridge"'),
        ("literal", "left(editions.edition_end, 4) >= seed_rows.vintage_start", "left(editions.edition_end, 5) >= seed_rows.vintage_start"),
        ("operator", "left(editions.edition_end, 4) >= seed_rows.vintage_start", "left(editions.edition_end, 4) > seed_rows.vintage_start"),
    ),
}
UNRELATED_EDITS = {
    "dim_rurality_code": ("select * from final", "select final.*, 'x' as note from final"),
    "dim_measure": ("select * from final", "select final.*, 'x' as note from final"),
    "dim_county": ("select * from final", "select final.*, 'x' as note from final"),
    "bridge_measure_control": ("    source_row\nfrom keyed", "    source_row,\n    source_seed as seed_copy\nfrom keyed"),
}


def mutation_probes(
    rendered: dict[str, str],
    mutations: dict[str, tuple[tuple[str, str, str], ...]] = MUTATIONS,
    unrelated: dict[str, tuple[str, str]] = UNRELATED_EDITS,
    prefix: str = "P4",
) -> list[Probe]:
    """One element of each pinned model changed at a time (modifier, filter, join type, grouping, source table, literal, operator) and an unrelated edit."""
    cases = [
        Probe(f"{prefix}_mut_{model.removeprefix('dim_')}_{element}", model, changed_once(rendered[model], before, after), EXPR, PIN_CAUSE, real_pins=True)
        for model, edits in mutations.items()
        for element, before, after in edits
    ]
    for model, (before, after) in unrelated.items():
        cases.append(Probe(f"{prefix}_pin_unrelated_edit_{model}", model, changed_once(rendered[model], before, after), EXPR, PIN_CAUSE, real_pins=True))
    return cases


def layout_only(text: str) -> str:
    """The same query with other indentation, keyword case, a comment and unquoted relation names: its parse does not change."""
    return "/* layout only */\n" + re.sub(r'main\."(\w+)"', r"main.\1", re.sub(r"\bselect\b", "SELECT", text.replace("\n    ", "\n      ")))


def layout_pin_probes(rendered: dict[str, str], models: Iterable[str], prefix: str) -> list[Probe]:
    """What a pin accepts: the delivered query with other layout, and with the build catalog written on its relations (the parse alone counts)."""
    cases: list[Probe] = []
    for model in models:
        cases.append(Probe(f"{prefix}_pin_text_only_{model}", model, layout_only(rendered[model]), (), real_pins=True))
        cases.append(Probe(f"{prefix}_pin_catalog_{model}", model, rendered[model].replace('main."', '"staging".main."'), (), real_pins=True))
    return cases


def missing_pin_probes(rendered: dict[str, str], causes: dict[str, str], prefix: str) -> list[Probe]:
    """A pinned capability whose pin entry is missing is not granted: the delivered query is refused for the cause its model's capability would have lifted."""
    return [
        Probe(f"{prefix}_pin_missing_{model}", model, rendered[model], EXPR, cause, remove="pin:" + model, real_pins=True) for model, cause in causes.items()
    ]


def pin_application_probes(rendered: dict[str, str]) -> list[Probe]:
    """What the pin accepts (the parse alone counts) and what it does not carry over (another model, a CTE read as a build relation, a missing pin)."""
    cases = layout_pin_probes(rendered, MUTATIONS, "P4")
    cases.append(
        Probe(
            "P4_pin_cte_made_physical",
            "dim_county",
            changed_once(rendered["dim_county"], "select max(vintage) as vintage\n    from lists", 'select max(vintage) as vintage\n    from main."lists"'),
            EXPR,
            PIN_CAUSE,
            real_pins=True,
        )
    )
    cases.append(Probe("P4_pin_other_model_county_as_measure", "dim_measure", rendered["dim_county"], EXPR, PIN_CAUSE, real_pins=True))
    cases.append(Probe("P4_pin_other_model_rurality_as_drg", "dim_drg", rendered["dim_rurality_code"], EXPR, "aggregate min", real_pins=True))
    causes = {
        "dim_rurality_code": "aggregate min",
        "dim_measure": "aggregate min",
        "dim_county": "aggregate max",
        "bridge_measure_control": "function left is not approved",
    }
    return cases + missing_pin_probes(rendered, causes, "P4")


def own_pin_any_value_probes() -> list[Probe]:
    """Without the pin, ``any_value`` keeps a certified bound only as a plain call (each probe runs with the fingerprint of its own query as the pin)."""
    cases = []
    for label, bounds, failures, cause in (
        ("filter_null", NULL_BOUNDS, EXPR, "aggregate any_value"),
        ("filter_false", FALSE_BOUNDS, EXPR, "aggregate any_value"),
        ("plain", PLAIN_BOUNDS, (), ""),
    ):
        for model, short in (("dim_rurality_code", "rurality"), ("dim_measure", "measure")):
            cases.append(Probe(f"P4_any_value_{label}_own_pin_{short}", model, stand_in_any_value(model, bounds), failures, cause))
    return cases


def pinned_model_probes(rendered: dict[str, str]) -> list[Probe]:
    """The reproducers, the one-element mutations of the pinned models and the pin's own rules."""
    return pinned_query_reproducers(rendered) + mutation_probes(rendered) + pin_application_probes(rendered) + own_pin_any_value_probes()


MUTATIONS_STATE_PLANNING: dict[str, tuple[tuple[str, str, str], ...]] = {
    "dim_state": (
        ("modifier", "select distinct\n        state as state_code,", "select\n        state as state_code,"),
        ("filter", "where state is not null and county_fips is not null", "where state is not null"),
        ("join_type", "left join fips", "inner join fips"),
        (
            "source_table",
            'from main."int_rucc_county_codes"\n    where state is not null and county_fips',
            'from main."int_ruca_codes"\n    where state is not null and county_fips',
        ),
        ("literal", "left(county_fips, 2)", "left(county_fips, 3)"),
        ("operator", "'^[A-Z]{2}$'", "'^[A-Z]{3}$'"),
    ),
    "bridge_county_planning_region": (
        ("modifier", "count(*) as tract_count", "count(distinct county_fips) as tract_count"),
        ("filter", " and geography_type = 'tract'", ""),
        ("grouping", "group by county_fips, county_fips_2023, member_sha256", "group by county_fips, county_fips_2023"),
        ("source_table", 'from main."int_ruca_codes"', 'from main."int_rucc_county_codes"'),
        ("literal", "county_fips like '09%'", "county_fips like '36%'"),
        ("operator", "vintage = '2020'", "vintage <> '2020'"),
    ),
}
UNRELATED_EDITS_STATE_PLANNING = {
    "dim_state": ("select * from final", "select final.*, 'x' as note from final"),
    "bridge_county_planning_region": ("    member_sha256\nfrom counted", "    member_sha256,\n    'x' as note\nfrom counted"),
}


def second_pin_probes(rendered: dict[str, str]) -> list[Probe]:
    """The pins of dim_state and bridge_county_planning_region: one element changed at a time, an unrelated edit, layout-only text and a missing pin."""
    causes = {"dim_state": "function left is not approved", "bridge_county_planning_region": "aggregate count_star"}
    return (
        mutation_probes(rendered, MUTATIONS_STATE_PLANNING, UNRELATED_EDITS_STATE_PLANNING, "P5")
        + layout_pin_probes(rendered, MUTATIONS_STATE_PLANNING, "P5")
        + missing_pin_probes(rendered, causes, "P5")
    )


QUALIFIED_CAUSE = "is qualified"


def spoofed_calls(rendered: dict[str, str]) -> list[tuple[str, str, str]]:
    """Qualified spellings of the approved calls, each as a small query and as a change of the delivered text: the model's name, the model and the SQL."""
    state, planning, dim_county = rendered["dim_state"], rendered["bridge_county_planning_region"], rendered["dim_county"]
    left = ('spoof."left"(county_fips, 2)', "main.left(county_fips, 2)", 'spoof_db.spoof."left"(county_fips, 2)', 'system.main."left"(county_fips, 2)')
    counts = ("spoof.count_star()", "system.main.count_star()", "main.count_star()")
    likes = ("spoof.\"~~\"(county_fips, '09%')", "spoof_db.spoof.\"~~\"(county_fips, '09%')", "main.\"~~\"(county_fips, '09%')")
    cases: list[tuple[str, str, str]] = []
    for index, call in enumerate(left):
        cases.append((f"state_left_{index}", "dim_state", state_sql(call)))
        cases.append((f"d_state_left_{index}", "dim_state", changed_once(state, "left(county_fips, 2)", call)))
    for index, call in enumerate(counts):
        cases.append((f"tract_count_{index}", "bridge_county_planning_region", tract_sql(call)))
        cases.append((f"d_tract_count_{index}", "bridge_county_planning_region", changed_once(planning, "count(*)", call)))
    for index, call in enumerate(likes):
        cases.append((f"county_like_{index}", "bridge_county_planning_region", county_filter_sql(call)))
        cases.append((f"d_county_like_{index}", "bridge_county_planning_region", changed_once(planning, "county_fips like '09%'", call)))
    cases.append(("aggregate_max", "dim_county", changed_once(dim_county, "select max(vintage) as vintage", "select main.max(vintage) as vintage")))
    bound = ("min(vintages.vintage) as first_unlisted_vintage", "main.min(vintages.vintage) as first_unlisted_vintage")
    cases.append(("aggregate_min", "dim_county", changed_once(dim_county, *bound)))
    return cases


def qualified_function_probes(rendered: dict[str, str]) -> list[Probe]:
    """A function written with a schema or catalog never receives the permission of the built-in it is named after, with or without a pin."""
    cases = spoofed_calls(rendered)
    cases.append(("md5_key", "dim_state", changed_once(state_sql(), "md5(state_code)", "spoof.md5(state_code)")))
    cases.append(("other_function", "dim_state", state_sql("main.upper(county_fips)")))
    cases.append(("window", "dim_state", state_sql("main.row_number() over ()")))
    refused = [Probe(f"P5_q_{name}", model, sql, EXPR, QUALIFIED_CAUSE) for name, model, sql in cases]
    refused += [Probe(f"P5_q_{name}_real_pins", model, sql, EXPR, QUALIFIED_CAUSE, real_pins=True) for name, model, sql in cases]
    controls = (
        ("control_state", "dim_state", state_sql()),
        ("control_tract_count", "bridge_county_planning_region", tract_sql()),
        ("control_county_like", "bridge_county_planning_region", county_filter_sql()),
        ("control_d_state", "dim_state", rendered["dim_state"]),
        ("control_d_planning", "bridge_county_planning_region", rendered["bridge_county_planning_region"]),
    )
    return refused + [Probe(f"P5_q_{name}", model, sql, ()) for name, model, sql in controls]


class SelectedSourceLoader(importlib.abc.InspectLoader):
    """Import selected local definitions in memory without creating or executing a runner file."""

    def __init__(self, source: str) -> None:
        self.source = source

    def get_source(self, fullname: str) -> str:
        if not fullname:
            raise ImportError("the in-memory loader needs a module name")
        return self.source


def isolated_source(source: str, names: set[str], bindings: dict[str, Any], mutation: str = "") -> dict[str, Any]:
    """Execute only selected trusted local AST definitions; runner imports and stages are excluded."""
    tree = ast.parse(source)
    body: list[ast.stmt] = [ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)]
    for original in tree.body:
        if isinstance(original, (ast.FunctionDef, ast.ClassDef)) and original.name in names:
            node = copy.deepcopy(original)
            if isinstance(node, ast.FunctionDef):
                if mutation == "required_test" and node.name == "release_test_problems":
                    node.body = ast.parse("return []").body
                elif mutation == "status" and node.name == "accepted":
                    node.body = ast.parse("return status in BUILD_OK or status == 'warn'").body
                elif mutation == "rebuild" and node.name == "rebuild_identical":
                    node.body = [item for item in node.body if not isinstance(item, ast.If)]
                elif mutation == "setup" and node.name == "real_setup":
                    registration = node.body.pop(2)
                    node.body.append(registration)
                elif mutation == "coverage" and node.name == "coverage_problems":
                    node.body[-1] = ast.parse("return [], detail").body[0]
                elif mutation == "allowlist" and node.name == "capabilities":
                    node.body = ast.parse("return frozenset(CAPABILITIES[x] for x in allowlist if x in CAPABILITIES)").body
                elif mutation == "bridge" and node.name == "bridge_table":
                    node.body = ast.parse("return Table(name, 'bridge', str(entry['grain']), ())").body
            body.append(node)
        elif isinstance(original, (ast.Assign, ast.AnnAssign)):
            targets = original.targets if isinstance(original, ast.Assign) else [original.target]
            if any(isinstance(target, ast.Name) and target.id in names for target in targets):
                body.append(copy.deepcopy(original))
    selected = ast.unparse(ast.fix_missing_locations(ast.Module(body=body, type_ignores=[])))
    module = ModuleType("_gx_static_helpers")
    module.__dict__.update({key: value for key, value in bindings.items() if key != "__name__"})
    sys.modules[module.__name__] = module
    loader = SelectedSourceLoader(selected)
    # The loader must serve exactly the selected definitions before they run (amendment A65: replaces the temporary allowlist entry).
    if loader.get_source(module.__name__) != selected:
        raise RuntimeError("the in-memory loader does not serve the selected source")
    loader.exec_module(module)
    return module.__dict__


def runner_helpers(source: str, mutation: str = "") -> dict[str, Any]:
    names = {
        "BUILD_OK",
        "GOLD_MODELS",
        "RELEASE_COPY_TEST",
        "RELEASE_COPY_NAMES",
        "RELEASE_MODEL",
        "EXPRESSION_CHECKS",
        "LEAF_EXPRESSION_CLASSES",
        "CHECK_NAMES",
        "Results",
        "accepted",
        "not_passing",
        "disabled_problems",
        "release_test_problems",
        "gold_node_problems",
        "rebuild_problems",
        "rebuild_identical",
        "real_setup",
        "incomplete_analysis",
        "run_checks",
        "keyed_facts",
        "bridge_ready",
    }
    return isolated_source(source, names, {"__name__": __name__, "dataclass": dataclass, "gold_checks": gold_checks, "json": json}, mutation)


def helper_cases(contract: Any, runner_source: str, checker_source: str) -> list[dict[str, Any]]:
    """Retain the executed metadata, coverage, status, rebuild and setup counterexamples with sensitive mutants."""
    records: list[dict[str, Any]] = []

    def record(name: str, verdict: bool, mutant_verdict: bool, evidence: Any) -> None:
        records.append({"name": name, "passed": verdict and not mutant_verdict, "mutant_detected": not mutant_verdict, "evidence": evidence})

    bridge = next(table for table in contract.bridges if table.name == "bridge_measure_control")
    bridge_contract = replace(contract, facts=(), tables=(bridge,))
    good = {bridge.name: [(column.name, column.type) for column in bridge.columns]}
    bad = {bridge.name: [("measure_sk", "VARCHAR")]}
    weakened = replace(bridge_contract, tables=(replace(bridge, columns=(gold_checks.Column("measure_sk", "measure_sk", "VARCHAR"),)),))
    result = gold_checks.structure_problems(bridge_contract, bad)
    record(
        "F3_control_bridge_schema",
        not any(gold_checks.structure_problems(bridge_contract, good).values()) and bool(result["column_map"]),
        bool(gold_checks.structure_problems(weakened, bad)["column_map"]),
        result,
    )
    mutant_bridge = isolated_source(checker_source, {"bridge_table"}, {**gold_checks.__dict__}, "bridge")

    def missing_schema(function: Any) -> bool:
        try:
            function("bridge_synthetic", {"grain": "synthetic_id"})
        except gold_checks.ContractError:
            return True
        return False

    record(
        "F3_unknown_bridge_missing_schema", missing_schema(gold_checks.bridge_table), missing_schema(mutant_bridge["bridge_table"]), "ContractError required"
    )
    weak_coverage = isolated_source(checker_source, {"coverage_problems"}, gold_checks.__dict__, "coverage")["coverage_problems"]
    ordinary = next(fact for fact in contract.facts if not fact.is_hold)
    holds = tuple(fact for fact in contract.facts if fact.is_hold)
    for name, selected, counts, allowed in (
        ("F4_equal_empty_ordinary", (ordinary,), {ordinary.name: 0}, {}),
        ("F4_all_empty_named_holds", holds, {fact.name: 0 for fact in holds}, {fact.name: "synthetic exception" for fact in holds}),
    ):
        tested = replace(contract, facts=selected, tables=())
        problems, detail = gold_checks.coverage_problems(tested, counts, counts, allowed)
        record(name, bool(problems), bool(weak_coverage(tested, counts, counts, allowed)[0]), {"problems": problems, "rows": detail})
    positive, _detail = gold_checks.coverage_problems(replace(contract, facts=(ordinary,), tables=()), {ordinary.name: 1}, {ordinary.name: 1}, {})
    records.append({"name": "positive_nonempty_coverage", "passed": not positive, "problems": positive})

    helpers = runner_helpers(runner_source)
    model_id = "model.synthetic.dim_release"
    model = {"resource_type": "model", "name": "dim_release", "fqn": ["synthetic", "gold", "dim_release"]}
    test_name = helpers["RELEASE_COPY_TEST"]
    test_id = "test.synthetic." + test_name
    test = {"resource_type": "test", "name": test_name, "attached_node": model_id, "config": {"severity": "error"}}
    manifest = {"nodes": {model_id: model, test_id: test}}
    statuses = {"dim_release": "success", test_name: "pass"}
    for suffix in ("", "_"):
        active = copy.deepcopy(manifest)
        active["nodes"].pop(test_id)
        active["nodes"][test_id + suffix] = {**test, "name": test_name + suffix}
        found = helpers["gold_node_problems"](active, {"dim_release": "success", test_name + suffix: "pass"}, True)
        records.append({"name": "positive_release_copy_pass" + ("_trailing" if suffix else "_named"), "passed": not found, "problems": found})
    required_mutant = runner_helpers(runner_source, "required_test")
    for label, changed in (
        ("deleted", {"nodes": {model_id: model}}),
        ("disabled", {"nodes": {model_id: model}, "disabled": {test_id: [test]}}),
        ("severity_warn", {"nodes": {model_id: model, test_id: {**test, "config": {"severity": "warn"}}}}),
        ("wrong_attachment", {"nodes": {model_id: model, test_id: {**test, "attached_node": "model.synthetic.dim_hospital"}}}),
        ("disabled_config", {"nodes": {model_id: model, test_id: {**test, "config": {"severity": "error", "enabled": False}}}}),
    ):
        local_mutant = dict(required_mutant)
        if label == "disabled":
            # The historical gate also skipped disabled declarations.
            local_mutant = isolated_source(runner_source, {"gold_node_problems"}, {**local_mutant, "disabled_problems": lambda _manifest: []})
        found = helpers["gold_node_problems"](changed, statuses, True)
        escaped = local_mutant["gold_node_problems"](changed, statuses, True)
        record("F17_required_test_" + label, bool(found), bool(escaped), {"problems": found, "mutant_problems": escaped})
    for status in ("not run", "fail", "error", "skipped", "warn"):
        found = helpers["gold_node_problems"](manifest, {"dim_release": "success", test_name: status}, True)
        # Simulate the old missing-status guard accepting this required node.
        weak = dict(helpers)
        weak["accepted"] = lambda _name, _status: True
        weak = isolated_source(runner_source, {"gold_node_problems"}, weak)
        record(
            "F17_required_test_status_" + status.replace(" ", "_"),
            bool(found),
            bool(weak["gold_node_problems"](manifest, {"dim_release": "success", test_name: status}, True)),
            found,
        )
    other_warn = {"unrelated_test": "warn"}
    record(
        "F17_other_warning_rejected",
        bool(helpers["not_passing"](other_warn)),
        bool(runner_helpers(runner_source, "status")["not_passing"](other_warn)),
        other_warn,
    )

    for name, code, status_map in (("nonzero", 1, {ordinary.name: "success"}), ("no_statuses", 0, {}), ("failed_status", 0, {ordinary.name: "error"})):
        observations: list[str] = []
        before = [[ordinary.name, "gold", "1", "synthetic-fingerprint"]]
        bindings: dict[str, Any] = {
            **helpers,
            "dbt_run": lambda _case, _select, code=code, status_map=status_map: (code, status_map),
            "gold_prints": lambda _contract, _backend, observations=observations, before=before: observations.append("fingerprints") or before,
        }
        namespace = isolated_source(runner_source, {"rebuild_identical"}, bindings)
        tested = replace(contract, facts=(ordinary,), tables=())
        found = namespace["rebuild_identical"](None, tested, None, before, "synthetic")
        verdict = bool(found) and not observations
        changed_namespace = isolated_source(runner_source, {"rebuild_identical"}, bindings, "rebuild")
        escaped = changed_namespace["rebuild_identical"](None, tested, None, before, "synthetic")
        record("F7_rebuild_" + name, verdict, bool(escaped) and not observations, {"problems": found, "mutant_problems": escaped, "calls": observations})
    for label, after in (
        ("identical", [[ordinary.name, "gold", "1", "synthetic-fingerprint"]]),
        ("changed", [[ordinary.name, "gold", "2", "changed"]]),
        ("absent", []),
    ):
        first = [[ordinary.name, "gold", "1", "synthetic-fingerprint"]]
        problems = gold_checks.rebuild_problems(first, after, [ordinary.name])
        records.append({"name": "F7_fingerprint_" + label, "passed": bool(problems) == (label != "identical"), "problems": problems})

    def setup_probe(namespace: dict[str, Any], stop_at: str, exception: type[BaseException]) -> tuple[bool, list[str]]:
        events: list[str] = []
        run = SimpleNamespace(cleanups=[], enter=events.append)

        def callback(stage: str) -> None:
            events.append(stage)
            if stage == stop_at:
                raise exception("synthetic orderly stop")

        stopped = False
        try:
            namespace["real_setup"](run, lambda: callback("start"), lambda: callback("install"), lambda: events.append("release"))
        except (SystemExit, KeyboardInterrupt):
            stopped = True
        for _name, release in run.cleanups:
            release()
        return stopped and events.count("release") == 1, events

    for stage in ("start", "install"):
        for exception in (SystemExit, KeyboardInterrupt):
            verdict, events = setup_probe(helpers, stage, exception)
            weak_verdict, weak_events = setup_probe(runner_helpers(runner_source, "setup"), stage, exception)
            record("F5_F12_setup_" + stage + "_" + exception.__name__, verdict, weak_verdict, {"events": events, "mutant_events": weak_events})
    unknown = replace(contract, allowlist=(*contract.allowlist, "synthetic unsupported allowlist entry"))

    def allowlist_probe(function: Any) -> bool:
        try:
            function(unknown.allowlist)
        except gold_checks.gold_sql.PolicyError:
            return True
        return False

    weak = isolated_source((ROOT / "scripts/lakehouse/gold_sql.py").read_text(), {"capabilities"}, gold_checks.gold_sql.__dict__, "allowlist")
    record("F2_unknown_allowlist_exception", allowlist_probe(gold_checks.gold_sql.capabilities), allowlist_probe(weak["capabilities"]), "PolicyError required")
    return records


def label_choice_mutation(contract: Any, rendered: dict[str, str], aggregates: frozenset[str]) -> dict[str, Any]:
    """The delivered label query must fail if its scoped normalization consumption is removed."""
    source = (ROOT / "scripts/lakehouse/gold_sql.py").read_text()
    anchor = 'self.use_normalizations(value, column_name(target) or "")'
    if source.count(anchor) != 1:
        raise ValueError("label-choice mutation needs its single scoped consumption call")
    namespace = isolated_source(source.replace(anchor, "None"), {"Walker", "_analyze_model"}, gold_checks.gold_sql.__dict__)
    document = json.loads(parse(rendered["dim_measure"]))
    policy = gold_checks.policy_for(contract, aggregates)
    original = gold_checks.gold_sql.analyze_model("dim_measure", "dimension", document, policy, {})
    changed = namespace["_analyze_model"]("dim_measure", "dimension", document, policy, {})
    escaped = [finding.message for finding in changed.findings]
    return {
        "name": "937_A48_consumption_mutation",
        "passed": not original.findings and any("no approved final role" in message for message in escaped),
        "mutant_detected": bool(escaped),
        "mutant_findings": escaped,
    }


@dataclass(frozen=True)
class SyntheticPath:
    """An in-memory manifest and compiled file stand-in; no build output is read or written."""

    files: dict[str, str]
    name: str = ""

    def __truediv__(self, other: str) -> SyntheticPath:
        return SyntheticPath(self.files, (self.name + "/" + other).lstrip("/"))

    def read_text(self) -> str:
        return self.files[self.name]

    def is_file(self) -> bool:
        return self.name in self.files


def boundary_cases(contract: Any, rendered: dict[str, str], expected: dict[str, str], aggregates: frozenset[str], runner_source: str) -> list[dict[str, Any]]:
    """Call the extracted run_checks propagation boundary with real expressions and a synthetic no-I/O backend."""
    fact = next(item for item in contract.facts if item.name == "fct_hai_hospital_window")
    tested = replace(contract, facts=(fact,), tables=())
    model = fact.name
    node = {
        "name": model,
        "resource_type": "model",
        "fqn": ["synthetic", "gold", model],
        "depends_on": {"nodes": []},
        "package_name": "synthetic",
        "original_file_path": "models/gold/" + model + ".sql",
    }
    manifest = {"nodes": {"model.synthetic." + model: node}}
    metadata = [[model, name, kind] for name, kind in fact.expected_columns]
    metadata += [[fact.silver_model, column.silver_column, column.type] for column in fact.columns]
    backend = SimpleNamespace(plan={}, query=lambda _sql: metadata, run=lambda _statements: [])
    records = []
    cases: list[tuple[str, str | None, str | None, bool]] = [
        ("positive_macro", rendered[model], parse(rendered[model]), False),
        ("missing_sql", None, None, True),
        ("serializer_error", "select from", parse("select from"), True),
        (
            "recursive_cte",
            "with recursive q as (select * from main.int_hai_hospital_windows union all select * from q where false) select * from q",
            None,
            True,
        ),
    ]
    for label, sql, document, wanted in cases:
        if document is None and sql is not None:
            document = parse(sql)
        files = {"manifest.json": json.dumps(manifest)}
        if sql is not None:
            files["compiled/synthetic/" + node["original_file_path"]] = sql
        namespace = runner_helpers(runner_source)
        facade = SimpleNamespace(**gold_checks.__dict__)
        facade.rendered_facts = lambda _contract, _manifest: expected
        facade.declared_relations = lambda _manifest, sql=sql: {model: set(re.findall(r'main\."?(\w+)', sql or ""))}
        namespace["gold_checks"] = facade
        namespace["parse_compiled"] = lambda _target, _manifest, _models, _plan, document=document: ({model: document}, aggregates)
        result = namespace["run_checks"](tested, backend, SyntheticPath(files), None, None, {})
        actual = bool(result.problems["analysis_complete"])
        counted = result.verdicts["analysis_complete"] == (not wanted)
        records.append(
            {
                "name": "A49_run_checks_" + label,
                "passed": actual == wanted and counted and result.report_only == {},
                "analysis_complete": result.problems["analysis_complete"],
                "expression": {name: result.problems[name] for name in namespace["EXPRESSION_CHECKS"]},
                "boundary": "selected run_checks; synthetic query responses; no fixture or runner imports",
            }
        )
    helpers = runner_helpers(runner_source)
    for marker in ("COLUMN_REF", "CONSTANT", "PARAMETER", "DEFAULT", "SUBQUERY"):
        problem = "synthetic " + marker + " was not analyzed (unsupported shape)"
        records.append({"name": "A49_leaf_" + marker, "passed": helpers["incomplete_analysis"](problem) == (marker == "SUBQUERY")})
    for name in helpers["EXPRESSION_CHECKS"]:
        result = helpers["Results"]({name: ["synthetic expression finding"]}, {})
        records.append({"name": "939_static_counted_" + name, "passed": result.verdicts[name] is False and result.report_only == {}})
    return records


def range_cases(contract: Any, aggregates: frozenset[str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Render a synthetic edition join from macro text, then change its fact-date predicate as a negative control."""
    fact = next(item for item in contract.facts if item.name == "fct_hai_hospital_window")
    macro = (ROOT / "dbt/macros/gold_facts.sql").read_text()
    anchor = "    on md5(cast(silver.{{ mapping.natural_key }} as varchar)) = {{ key_name }}_dim.{{ mapping.dimension[4:] }}_sk"
    addition = "\n    and silver.window_start >= {{ key_name }}_dim.vintage_start\n    and silver.window_start < {{ key_name }}_dim.vintage_end"
    if macro.count(anchor) != 1:
        raise ValueError("changed-macro probe needs exactly one delivered join anchor")
    variant = macro.replace(anchor, anchor + addition)
    keys = {key.name: {"dimension": key.dimension, "natural_key": key.natural_key} for key in fact.keys}
    relations = {name: f'main."{name}"' for name in (fact.silver_model, *(key.dimension for key in fact.keys))}
    rendered = gold_checks.gold_sql.render_fact(variant, fact.silver_model, fact.grain, keys, relations)
    original = gold_checks.gold_sql.render_fact(macro, fact.silver_model, fact.grain, keys, relations)
    records = []
    edited = rendered.replace("silver.window_start >=", "silver.window_end >=", 1)
    for label, compiled, expectation, reject in (
        ("positive_range_macro", rendered, rendered, False),
        ("negative_edited_range_output", edited, rendered, True),
        ("negative_old_macro_expectation", rendered, original, True),
    ):
        problems, reads = gold_checks.expression_problems(
            contract, {fact.name: parse(compiled)}, [fact.name], aggregates, {fact.name: set(relations)}, {fact.name: compiled}, {fact.name: expectation}
        )
        failure = bool(problems["expression_functions"])
        passed = failure if reject else not any(problems.values())
        records.append({"name": "935_" + label, "passed": passed, "problems": problems, "reads": reads})
    return records, {
        "macro_sha256": hashlib.sha256(variant.encode()).hexdigest(),
        "macro": variant,
        "rendered": rendered,
        "negative_control": edited,
        "boundary": "synthetic macro text only; no dbt file edited",
    }


def g4_policy_record(contract: Any, aggregates: frozenset[str]) -> dict[str, Any]:
    """The real contract's allowlist is readable, every entry switches on its own capability and an altered entry text is refused."""
    sql = gold_checks.gold_sql
    allowlist = contract.allowlist
    error = ""
    try:
        gold_checks.policy_for(contract, aggregates)
    except sql.PolicyError as raised:
        error = str(raised)
    capabilities = [sql.CAPABILITIES.get(entry) for entry in allowlist]
    altered_refused = False
    try:
        sql.capabilities((allowlist[-1] + " ",))
    except sql.PolicyError:
        altered_refused = True
    return {
        "name": "G4_real_contract_policy_reads",
        "entries": list(allowlist),
        "capabilities": capabilities,
        "error": error,
        "altered_entry_refused": altered_refused,
        "passed": not error and None not in capabilities and len(set(capabilities)) == len(allowlist) and altered_refused,
    }


def policy_mirror_record(contract: Any) -> dict[str, Any]:
    """The contract's expression policy and the checker's built-in fallback policy (``A36_A39_A43_A48``) say the same."""
    mirror = json.loads(json.dumps(gold_checks.gold_sql.A36_A39_A43_A48))
    differing = sorted(key for key in mirror.keys() | contract.expression_policy.keys() if mirror.get(key) != contract.expression_policy.get(key))
    return {"name": "P2_policy_mirror_equal", "differs": differing, "passed": not differing}


def tested_contract(contract: Any, probe: Probe) -> Any:
    """The contract a probe runs against: the allowlist entry or scope entry it names is removed."""
    if probe.remove.startswith("scope:"):
        scopes = copy.deepcopy(contract.expression_policy)
        name = probe.remove.removeprefix("scope:")
        if name == "source_date_models":
            scopes[name] = []
        else:
            scopes[name].pop(probe.model)
        return replace(contract, expression_policy=scopes)
    if probe.remove and not probe.remove.startswith("pin:"):
        allowlist = () if probe.remove == "all" else tuple(entry for entry in contract.allowlist if gold_checks.gold_sql.CAPABILITIES[entry] != probe.remove)
        return replace(contract, allowlist=allowlist)
    return contract


def probe_pins(probe: Probe, document: str) -> dict[tuple[str, str], Any] | None:
    """The pin table a probe runs with: the real one for a probe that tests a pin, the probe's own query fingerprint as its model's pin otherwise."""
    sql = gold_checks.gold_sql
    pins = getattr(sql, "PINS", None)
    if pins is None:
        return None
    if probe.remove.startswith("pin:"):
        return {key: pin for key, pin in pins.items() if key[0] != probe.remove.removeprefix("pin:")}
    statements = json.loads(document).get("statements", [])
    if probe.real_pins or len(statements) != 1:
        return dict(pins)
    own = sql.query_fingerprint(statements[0])
    return {key: pin._replace(fingerprint=own) if key[0] == probe.model else pin for key, pin in pins.items()}


@contextmanager
def patched_pins(pins: dict[tuple[str, str], Any] | None) -> Iterator[None]:
    """Run with another pin table in the checker; the original is back after the block, also after an exception."""
    sql = gold_checks.gold_sql
    if pins is None:
        yield
        return
    original = sql.PINS
    sql.PINS = pins
    try:
        yield
    finally:
        sql.PINS = original


def evaluate_probe(probe: Probe, contract: Any, aggregates: frozenset[str], expected: dict[str, str]) -> dict[str, Any]:
    """Check one probe through the production entry point and return its record."""
    document = parse(probe.sql)
    declared = set(re.findall(r"main\.\"?(\w+)", probe.sql)) - ({"stg_bronze__file_copies"} if not probe.name.startswith(("R2_", "R3_", "R4_")) else set())
    kwargs: dict[str, Any] = {}
    if "compiled" in gold_checks.expression_problems.__annotations__:
        kwargs = {"compiled": {probe.model: probe.sql}, "expected_fact_sql": expected}
    with patched_pins(probe_pins(probe, document)):
        problems, _reads = gold_checks.expression_problems(
            tested_contract(contract, probe), {probe.model: document}, [probe.model], aggregates, {probe.model: declared}, **kwargs
        )
    found = {name for name, items in problems.items() if items}
    intended = set(probe.failures) <= found if probe.failures else not found
    parsed = not json.loads(document).get("error", False)
    if probe.name.startswith(("R2_", "R3_", "R4_")):
        intended &= parsed
    cause = not probe.cause or any(probe.cause in message for name in probe.failures for message in problems[name])
    return {
        "name": probe.name,
        "expected_failures": probe.failures,
        "sql": probe.sql,
        "expected_cause": probe.cause,
        "parsed": parsed,
        "problems": problems,
        "passed": intended and cause,
    }


def pin_records(contract: Any, rendered: dict[str, str], aggregates: frozenset[str], expected: dict[str, str]) -> list[dict[str, Any]]:
    """The pins equal the delivered queries; the refusal names both fingerprints; a removed allowlist entry leaves no pin message."""
    sql = gold_checks.gold_sql

    def fingerprint(text: str) -> str:
        return str(sql.query_fingerprint(json.loads(parse(text))["statements"][0]))

    delivered = {key: fingerprint(rendered[key[0]]) for key in sql.PINS}
    mutated = changed_once(rendered["dim_county"], "select max(vintage) as vintage", "select max(distinct vintage) as vintage")
    named = evaluate_probe(Probe("P4_pin_message_names_both", "dim_county", mutated, EXPR, PIN_CAUSE, real_pins=True), contract, aggregates, expected)
    pin = sql.PINS[("dim_county", "county_list_bounds")]
    shown = " ".join(message for items in named["problems"].values() for message in items)
    named["passed"] = named["passed"] and all(part in shown for part in (pin.fingerprint, fingerprint(mutated), pin.reason))
    removed_probe = Probe(
        "P4_pin_entry_removed_county", "dim_county", rendered["dim_county"], EXPR, "aggregate max", remove="county_list_bounds", real_pins=True
    )
    removed = evaluate_probe(removed_probe, contract, aggregates, expected)
    removed["passed"] = removed["passed"] and PIN_CAUSE not in " ".join(message for items in removed["problems"].values() for message in items)
    equal = {
        "name": "P4_pins_equal_delivered",
        "pins": {f"{model}:{capability}": value for (model, capability), value in delivered.items()},
        "passed": all(delivered[key] == value.fingerprint for key, value in sql.PINS.items())
        and {capability for _model, capability in sql.PINS} == sql.PINNED_CAPABILITIES,
    }
    return [equal, named, removed]


def run(out: Path, proposals: Path | None = None, only_g4: bool = False) -> bool:
    global gold_checks
    if proposals is not None:
        proposals = proposals.resolve()
    checker_path = proposals / "gold_checks.after" if proposals else ROOT / "scripts/lakehouse/gold_checks.py"
    runner_path = proposals / "run_gold_e2e.after" if proposals else ROOT / "scripts/lakehouse/run_gold_e2e.py"
    contract_path = proposals / "gold_contract.after" if proposals else ROOT / "data_contracts/lakehouse/gold_contract.json"
    if proposals:
        # This trusted local proposal imports only the owned analyzer and the Python standard library.
        module = ModuleType("_gx_static_checker")
        module.__file__ = str(checker_path)
        sys.modules[module.__name__] = module
        importlib.machinery.SourceFileLoader(module.__name__, str(checker_path)).exec_module(module)
        gold_checks = module
    inputs = [
        ROOT / path
        for path in (
            "scripts/lakehouse/gold_sql.py",
            "scripts/lakehouse/gold_checks.py",
            "scripts/lakehouse/run_gold_sql_e2e.py",
            "scripts/lakehouse/run_gold_e2e.py",
            "data_contracts/lakehouse/gold_contract.json",
            "data_contracts/lakehouse/silver_columns.json",
            "dbt/macros/gold_facts.sql",
            "dbt/macros/gold_identity.sql",
        )
    ]
    inputs += [checker_path, runner_path, contract_path]
    inputs += sorted((ROOT / "dbt/models/gold").rglob("*.sql"))
    hashes = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in inputs}
    started = datetime.now(UTC).isoformat()
    contract = gold_checks.load_contract(contract_path, ROOT / "data_contracts/lakehouse/silver_columns.json")
    rendered = render_models()
    expected = {}
    if hasattr(gold_checks.gold_sql, "render_fact"):
        names = set(re.findall(r"main\.\"?(\w+)", "\n".join(rendered.values())))
        relations = {name: f'main."{name}"' for name in names}
        macro = (ROOT / "dbt/macros/gold_facts.sql").read_text()
        expected = {
            fact.name: gold_checks.gold_sql.render_fact(
                macro, fact.silver_model, fact.grain, {key.name: {"dimension": key.dimension, "natural_key": key.natural_key} for key in fact.keys}, relations
            )
            for fact in contract.facts
        }
    result = run_command(
        "duckdb",
        [":memory:", "-init", "/dev/null", "-json", "-c", "select distinct function_name from duckdb_functions() where function_type='aggregate'"],
        timeout=60,
        check=True,
    )
    aggregates = frozenset(row["function_name"] for row in json.loads(result.stdout))
    records: list[dict[str, Any]] = [g4_policy_record(contract, aggregates), policy_mirror_record(contract)]
    corpus = (
        []
        if only_g4
        else probes(rendered)
        + round2_probes(rendered)
        + round2_siblings(rendered)
        + round3_review_probes()
        + round3_siblings(rendered)
        + round4_review_probes()
        + round4_siblings()
        + round5_probes()
    )
    original_pins = dict(getattr(gold_checks.gold_sql, "PINS", {}))
    fixes = near_delivered_probes(rendered) + pinned_model_probes(rendered) + second_pin_probes(rendered) + qualified_function_probes(rendered)
    for probe in corpus + g4_entry_probes() + edition_probes(rendered) + fixes:
        records.append(evaluate_probe(probe, contract, aggregates, expected))
    for model, sql in rendered.items():
        if only_g4 and model not in G4_MODELS:
            continue
        kwargs = {"compiled": {model: sql}, "expected_fact_sql": expected} if "compiled" in gold_checks.expression_problems.__annotations__ else {}
        problems, _reads = gold_checks.expression_problems(
            contract, {model: parse(sql)}, [model], aggregates, {model: set(re.findall(r"main\.\"?(\w+)", sql))}, **kwargs
        )
        records.append({"name": "accept_" + model, "problems": problems, "passed": not any(problems.values())})
    if hasattr(gold_checks.gold_sql, "PINS"):
        records += pin_records(contract, rendered, aggregates, expected)
        records.append({"name": "P4_pins_restored", "passed": original_pins == gold_checks.gold_sql.PINS})
    range_artifact: dict[str, Any] = {}
    if not only_g4:
        records += helper_cases(contract, runner_path.read_text(), checker_path.read_text())
        records.append(label_choice_mutation(contract, rendered, aggregates))
        ranged, range_artifact = range_cases(contract, aggregates)
        records += ranged
        records += boundary_cases(contract, rendered, expected, aggregates, runner_path.read_text())
    unchanged = hashes == {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in inputs}
    records.append({"name": "source_unchanged_during_corpus", "passed": unchanged})
    report = {
        "started_at_utc": started,
        "finished_at_utc": datetime.now(UTC).isoformat(),
        "mode": "G4 allowlist entries and their delivered models, serial" if only_g4 else "full static corpus, serial",
        "boundary": "static synthetic parser and Jinja rendering; no dbt build",
        "inputs_sha256": hashes,
        "range_macro": range_artifact,
        "duckdb": run_command("duckdb", ["--version"], timeout=60, check=True).stdout.strip(),
        "cases": records,
        "passed": sum(row["passed"] for row in records),
        "total": len(records),
        "cleanup": "done; synchronous local parser processes exited",
    }
    with out.open("x") as stream:
        stream.write(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(
        json.dumps({"report": str(out), "passed": report["passed"], "total": report["total"], "failed": [row["name"] for row in records if not row["passed"]]})
        + "\n"
    )
    return report["passed"] == report["total"]


def main() -> int:
    global gold_checks
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--proposals", type=Path, help="Read preserved local checker, contract and runner proposals without applying them")
    parser.add_argument("--g4", action="store_true", help="Run only the six model-bound allowlist entries (G4) and the models that use them")
    args = parser.parse_args()
    try:
        return 0 if run(args.out, args.proposals, args.g4) else 1
    finally:
        sys.modules.pop("_gx_static_checker", None)
        sys.modules.pop("_gx_static_helpers", None)
        gold_checks = installed_checks


if __name__ == "__main__":
    raise SystemExit(main())
