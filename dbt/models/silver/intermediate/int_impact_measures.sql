-- One row per impact-file release, hospital and registry measure field of source CMS_IPPS whose value is published, by
-- the impact_measures seed: value, code or ratio (field / denominator field; a zero or missing denominator gives null)
-- (failure mode 353). Staging a measure does not make it a predictor.
{{ config(materialized='table') }}

with

measures as (
    select
        measure_control,
        field,
        rule,
        denominator_field,
        review_decision
    from {{ ref('impact_measures') }}
),

hospital_values as (
    select
        member_sha256,
        sheet_name,
        family,
        rule_fiscal_year,
        rule_stage,
        ccn,
        field,
        value_text,
        value_number
    from {{ ref('int_impact_hospital_values') }}
),

measure_values as (
    select
        numerators.member_sha256,
        numerators.sheet_name,
        numerators.family,
        numerators.rule_fiscal_year,
        numerators.rule_stage,
        numerators.ccn,
        measures.measure_control,
        measures.field,
        measures.rule,
        measures.review_decision,
        case when measures.rule = 'code' then numerators.value_text end as value_code,
        case measures.rule
            when 'value' then numerators.value_number
            when 'ratio' then numerators.value_number / nullif(denominators.value_number, 0)
        end as value_number
    from measures
    inner join hospital_values as numerators on measures.field = numerators.field
    left join hospital_values as denominators
        on
            numerators.member_sha256 = denominators.member_sha256
            and numerators.sheet_name = denominators.sheet_name
            and numerators.ccn = denominators.ccn
            and measures.denominator_field = denominators.field
    where
        (measures.rule = 'code' and numerators.value_text is not null)
        or (measures.rule <> 'code' and numerators.value_number is not null)
)

select
    *,
    member_sha256 || ':' || sheet_name || ':' || ccn || ':' || measure_control || ':' || field as measure_key
from measure_values
