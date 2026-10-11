-- One row per cost report and registry measure control of family S01 whose numerator the report publishes: the value by
-- the seed's rule (value, code, ratio, per_day or ratio_per_day_denominator); a zero or missing denominator gives null,
-- never infinity (failure modes 339 and 340). Staging a measure does not make it a predictor.
{{ config(materialized='table') }}

with

measures as (
    select
        measure_control,
        rule,
        numerator_column,
        denominator_column,
        review_decision
    from {{ ref('cost_report_measures') }}
),

reports as (
    select
        rpt_rec_num,
        ccn,
        fiscal_year,
        reporting_days,
        type_of_control,
        provider_type,
        {%- for column in cost_report_amount_columns() %}
        {{ column }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ ref('int_cost_reports') }}
),

amounts as (
    unpivot (
        select
            rpt_rec_num,
            {%- for column in cost_report_amount_columns() %}
            {{ column }}{% if not loop.last %},{% endif %}
            {%- endfor %}
        from reports
    ) on columns(* exclude (rpt_rec_num)) into name column_name value amount
),

codes as (
    unpivot (
        select
            rpt_rec_num,
            type_of_control,
            provider_type
        from reports
    ) on type_of_control, provider_type into name column_name value code
),

numeric_measures as (
    select
        reports.rpt_rec_num,
        reports.ccn,
        reports.fiscal_year,
        measures.measure_control,
        measures.rule,
        measures.review_decision,
        numerators.amount as numerator,
        denominators.amount as denominator,
        cast(null as varchar) as value_code,
        case measures.rule
            when 'value' then numerators.amount
            when 'ratio' then numerators.amount / nullif(denominators.amount, 0)
            when 'per_day' then numerators.amount / nullif(reports.reporting_days, 0)
            when 'ratio_per_day_denominator'
                then numerators.amount / nullif(denominators.amount / nullif(reports.reporting_days, 0), 0)
        end as value_number
    from measures
    inner join amounts as numerators on measures.numerator_column = numerators.column_name
    inner join reports on numerators.rpt_rec_num = reports.rpt_rec_num
    left join amounts as denominators
        on
            numerators.rpt_rec_num = denominators.rpt_rec_num
            and measures.denominator_column = denominators.column_name
    where measures.rule <> 'code'
),

code_measures as (
    select
        reports.rpt_rec_num,
        reports.ccn,
        reports.fiscal_year,
        measures.measure_control,
        measures.rule,
        measures.review_decision,
        cast(null as double) as numerator,
        cast(null as double) as denominator,
        codes.code as value_code,
        cast(null as double) as value_number
    from measures
    inner join codes on measures.numerator_column = codes.column_name
    inner join reports on codes.rpt_rec_num = reports.rpt_rec_num
    where measures.rule = 'code'
),

combined as (
    select * from numeric_measures
    union all
    select * from code_measures
)

select
    *,
    rpt_rec_num || ':' || measure_control as measure_key
from combined
