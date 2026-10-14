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


def parse(sql: str) -> str:
    """Parse synthetic text with the installed DuckDB CLI; preserve its serializer errors as test inputs."""
    query = "select json_serialize_sql('" + sql.replace("'", "''") + "') as parsed"
    result = run_command("duckdb", [":memory:", "-init", "/dev/null", "-json", "-c", query], timeout=60, check=True)
    value = json.loads(result.stdout)[0]["parsed"]
    return json.dumps(value) if isinstance(value, dict) else str(value)


def render_models() -> dict[str, str]:
    """Render delivered SQL with stand-in main-schema refs; this is a static acceptance check, not dbt compilation."""
    env = Environment(undefined=StrictUndefined, autoescape=select_autoescape(default_for_string=False))
    macro = (ROOT / "dbt/macros/gold_facts.sql").read_text().split("{% test ", 1)[0]
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
        for name, before, after in [
            ("missing_anti_key", "labeled.code = observed.code", "true"),
            ("wrong_anti_alias", "labeled.code = observed.code", "labeled.code = labeled.code"),
            ("partial_anti_rows", "labeled.code = observed.code", "labeled.code = observed.code and labeled.code = '068'"),
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
    """GX round 5 (amendment A90, issue WV3-007): a sampled FROM reference restricts a selection, so it may not keep its domain; a null natural
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
        "WARN_TOLERATED",
        "WARN_NAMES",
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
    test_name = helpers["WARN_TOLERATED"]
    test_id = "test.synthetic." + test_name
    test = {"resource_type": "test", "name": test_name, "attached_node": model_id, "config": {"severity": "warn"}}
    manifest = {"nodes": {model_id: model, test_id: test}}
    statuses = {"dim_release": "success", test_name: "warn"}
    for suffix in ("", "_"):
        active = copy.deepcopy(manifest)
        active["nodes"].pop(test_id)
        active["nodes"][test_id + suffix] = {**test, "name": test_name + suffix}
        found = helpers["gold_node_problems"](active, {"dim_release": "success", test_name + suffix: "warn"}, True)
        records.append({"name": "positive_A42_warn" + ("_trailing" if suffix else "_named"), "passed": not found, "problems": found})
    required_mutant = runner_helpers(runner_source, "required_test")
    for label, changed in (
        ("deleted", {"nodes": {model_id: model}}),
        ("disabled", {"nodes": {model_id: model}, "disabled": {test_id: [test]}}),
        ("severity_error", {"nodes": {model_id: model, test_id: {**test, "config": {"severity": "error"}}}}),
        ("wrong_attachment", {"nodes": {model_id: model, test_id: {**test, "attached_node": "model.synthetic.dim_hospital"}}}),
    ):
        local_mutant = dict(required_mutant)
        if label == "disabled":
            # The historical gate also skipped disabled declarations.
            local_mutant = isolated_source(runner_source, {"gold_node_problems"}, {**local_mutant, "disabled_problems": lambda _manifest: []})
        found = helpers["gold_node_problems"](changed, statuses, True)
        escaped = local_mutant["gold_node_problems"](changed, statuses, True)
        record("F17_required_test_" + label, bool(found), bool(escaped), {"problems": found, "mutant_problems": escaped})
    for status in ("not run", "fail", "error", "skipped"):
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


def run(out: Path, proposals: Path | None = None) -> bool:
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
    records: list[dict[str, Any]] = []
    for probe in (
        probes(rendered)
        + round2_probes(rendered)
        + round2_siblings(rendered)
        + round3_review_probes()
        + round3_siblings(rendered)
        + round4_review_probes()
        + round4_siblings()
        + round5_probes()
    ):
        tested = contract
        if probe.remove.startswith("scope:"):
            scopes = copy.deepcopy(contract.expression_policy)
            name = probe.remove.removeprefix("scope:")
            if name == "source_date_models":
                scopes[name] = []
            else:
                scopes[name].pop(probe.model)
            tested = replace(contract, expression_policy=scopes)
        elif probe.remove:
            allowlist = (
                () if probe.remove == "all" else tuple(entry for entry in contract.allowlist if gold_checks.gold_sql.CAPABILITIES[entry] != probe.remove)
            )
            tested = replace(contract, allowlist=allowlist)
        document = parse(probe.sql)
        declared = set(re.findall(r"main\.\"?(\w+)", probe.sql)) - ({"stg_bronze__file_copies"} if not probe.name.startswith(("R2_", "R3_", "R4_")) else set())
        kwargs: dict[str, Any] = {}
        if "compiled" in gold_checks.expression_problems.__annotations__:
            kwargs = {"compiled": {probe.model: probe.sql}, "expected_fact_sql": expected}
        problems, _reads = gold_checks.expression_problems(tested, {probe.model: document}, [probe.model], aggregates, {probe.model: declared}, **kwargs)
        found = {name for name, items in problems.items() if items}
        intended = set(probe.failures) <= found if probe.failures else not found
        parsed = not json.loads(document).get("error", False)
        if probe.name.startswith(("R2_", "R3_", "R4_")):
            intended &= parsed
        cause = not probe.cause or any(probe.cause in message for name in probe.failures for message in problems[name])
        records.append(
            {
                "name": probe.name,
                "expected_failures": probe.failures,
                "sql": probe.sql,
                "expected_cause": probe.cause,
                "parsed": parsed,
                "problems": problems,
                "passed": intended and cause,
            }
        )
    for model, sql in rendered.items():
        kwargs = {"compiled": {model: sql}, "expected_fact_sql": expected} if "compiled" in gold_checks.expression_problems.__annotations__ else {}
        problems, _reads = gold_checks.expression_problems(
            contract, {model: parse(sql)}, [model], aggregates, {model: set(re.findall(r"main\.\"?(\w+)", sql))}, **kwargs
        )
        records.append({"name": "accept_" + model, "problems": problems, "passed": not any(problems.values())})
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
        "mode": "full static corpus, serial",
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
    args = parser.parse_args()
    try:
        return 0 if run(args.out, args.proposals) else 1
    finally:
        sys.modules.pop("_gx_static_checker", None)
        sys.modules.pop("_gx_static_helpers", None)
        gold_checks = installed_checks


if __name__ == "__main__":
    raise SystemExit(main())
