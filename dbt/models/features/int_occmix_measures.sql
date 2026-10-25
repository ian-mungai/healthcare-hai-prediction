-- Every occupational-mix control of the recipe stays visible. C043 has no occupational-mix value because paid hours are not bedside worked hours.

with

survey as (
    select * from {{ ref('fct_occupational_mix_survey') }}
),

measures as (
    -- One row per occupational-mix control: the output column, definition and source status of the recipe, and the review decision of
    -- the registry control dimension. C043 has no output column.
    select
        recipe.measure_control,
        max(recipe.output_name) as value_column,
        max(recipe.definition) as definition,
        max(recipe.source_status) as source_status,
        any_value(control.review_decision) as review_decision
    from {{ ref('occmix_measures_recipe') }} as recipe
    inner join {{ ref('dim_registry_control') }} as control on recipe.measure_control = control.measure_control
    group by recipe.measure_control
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
    -- Values by the recipe's formula and guard: a held row has no value; a ratio needs a positive denominator and a non-negative numerator.
    case
        when survey.hold_reason is not null then null
        when measures.value_column = 'rnhr' and survey.rnhr >= 0 then survey.rnhr
        when measures.value_column = 'rn_paid_hour_share' and survey.nursehr > 0 and survey.rnhr >= 0 then survey.rnhr / survey.nursehr
        when measures.value_column = 'lpnst_paid_hour_share' and survey.nursehr > 0 and survey.lpnsthr >= 0 then survey.lpnsthr / survey.nursehr
        when measures.value_column = 'naorat_paid_hour_share' and survey.nursehr > 0 and survey.naorathr >= 0 then survey.naorathr / survey.nursehr
        when measures.value_column = 'rn_paid_hour_wage' and survey.rnhr > 0 and survey.rnsal >= 0 then survey.rnsal / survey.rnhr
    end as value_number
from survey
cross join measures
