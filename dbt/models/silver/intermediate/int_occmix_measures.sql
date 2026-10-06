-- Every S03 control stays visible. C043 has no occupational-mix value because paid hours are not bedside worked hours.
{{ config(materialized='table') }}

with

survey as (
    select * from {{ ref('int_occmix_survey_rows') }}
),

measures as (
    select * from {{ ref('occmix_measures') }}
)

select
    survey.survey_row_key,
    survey.member_sha256,
    survey.sheet_name,
    survey.ccn,
    survey.survey_start_date,
    survey.survey_end_date,
    survey.rule_fiscal_years,
    survey.rule_stages,
    survey.hold_reason,
    measures.measure_control,
    measures.definition,
    measures.review_decision,
    measures.source_status,
    survey.survey_row_key || ':' || measures.measure_control as measure_key,
    case
        when survey.hold_reason is not null then null
        when measures.value_column = 'rnhr' and survey.rnhr >= 0 then survey.rnhr
        when measures.value_column = 'rn_paid_hour_share' then survey.rn_paid_hour_share
        when measures.value_column = 'lpnst_paid_hour_share' then survey.lpnst_paid_hour_share
        when measures.value_column = 'naorat_paid_hour_share' then survey.naorat_paid_hour_share
        when measures.value_column = 'rn_paid_hour_wage' then survey.rn_paid_hour_wage
    end as value_number
from survey
cross join measures
