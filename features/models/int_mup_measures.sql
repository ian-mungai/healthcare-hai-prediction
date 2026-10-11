-- One row per Medicare inpatient provider summary, data year and registry measure field of sources CMS-MUP-PROVIDER and
-- CMS_MEDICARE_PROVIDER whose value or numerator is published, by the mup_measures seed: value, ratio (a null or zero
-- denominator gives null) or drg_share (published DRG discharges over the provider total; cells under 11 discharges are
-- not published). Held and closed controls give no rows (failure modes 357 and 360 to 363).
{{ config(materialized='table') }}

with

measures as (
    select
        measure_control,
        field,
        rule,
        denominator_field,
        drg_codes,
        review_decision
    from {{ ref('mup_measures') }}
    where rule <> 'hold'
),

providers as (
    select
        member_sha256,
        ccn,
        data_year,
        release_year,
        {%- for column in mup_provider_numeric_columns() %}
        {{ column }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ ref('int_mup_providers') }}
),

provider_values as (
    -- A file holds one row per CCN, so each value keeps its file and CCN.
    unpivot (select * exclude (data_year, release_year) from providers)
    on columns(* exclude (member_sha256, ccn)) into name field value field_value
),

provider_keys as (
    select
        member_sha256,
        ccn,
        data_year,
        release_year
    from providers
),

drg_totals as (
    -- The published discharges of each drg_share's DRG codes, per provider and data year. A data year has one DRG file
    -- (assert_mup_drg_one_file_per_data_year), which can come from an earlier release than its provider file (356).
    select
        measures.measure_control,
        drg_cells.ccn,
        drg_cells.data_year,
        sum(drg_cells.tot_dschrgs) as drg_discharges
    from measures
    inner join {{ ref('int_mup_drg_discharges') }} as drg_cells
        on list_contains(string_split(measures.drg_codes, ' '), drg_cells.drg_cd)
    where measures.rule = 'drg_share'
    group by
        measures.measure_control,
        drg_cells.ccn,
        drg_cells.data_year
),

measure_values as (
    select
        provider_keys.member_sha256,
        provider_keys.ccn,
        provider_keys.data_year,
        provider_keys.release_year,
        measures.measure_control,
        measures.field,
        measures.rule,
        measures.review_decision,
        case measures.rule
            when 'value' then numerators.field_value
            when 'ratio' then numerators.field_value / nullif(denominators.field_value, 0)
            when 'drg_share' then drg_totals.drg_discharges / nullif(denominators.field_value, 0)
        end as value_number
    from measures
    inner join provider_values as numerators on measures.field = numerators.field
    inner join provider_keys
        on
            numerators.member_sha256 = provider_keys.member_sha256
            and numerators.ccn = provider_keys.ccn
    left join provider_values as denominators
        on
            numerators.member_sha256 = denominators.member_sha256
            and numerators.ccn = denominators.ccn
            and measures.denominator_field = denominators.field
    left join drg_totals
        on
            measures.measure_control = drg_totals.measure_control
            and provider_keys.ccn = drg_totals.ccn
            and provider_keys.data_year = drg_totals.data_year
    where measures.rule <> 'drg_share' or drg_totals.drg_discharges is not null
)

select
    *,
    member_sha256 || ':' || ccn || ':' || measure_control || ':' || field as measure_key
from measure_values
