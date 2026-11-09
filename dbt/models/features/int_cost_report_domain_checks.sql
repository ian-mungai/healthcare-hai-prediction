-- One row per cost-report control that states a domain or a floor in the recipe and per fiscal year: the rows evaluated, the rows whose value was removed
-- because it lay outside the domain and the rows whose value was removed because the denominator was missing or not above the floor. The counts come from
-- the domain_status that int_cost_report_measures writes; assert_cost_report_domain_counts recomputes them from the amounts. The features build copies
-- these rows into its receipt.

with

checked_controls as (
    -- The controls with a domain bound or a denominator floor, with the rule text the owner reviews.
    select
        measure_control,
        max(case when role in ('value', 'numerator') then domain_min end) as domain_min,
        max(case when role in ('value', 'numerator') then domain_max end) as domain_max,
        max(case when role = 'denominator' then denominator_floor end) as denominator_floor,
        string_agg(distinct domain_reason, '; ') as domain_reason
    from {{ ref('cost_report_measures_recipe') }}
    group by measure_control
    having
        max(case when role in ('value', 'numerator') then domain_min end) is not null
        or max(case when role in ('value', 'numerator') then domain_max end) is not null
        or max(case when role = 'denominator' then denominator_floor end) is not null
),

measures as (
    select
        measure_control,
        fiscal_year,
        domain_status
    from {{ ref('int_cost_report_measures') }}
)

select
    measures.measure_control,
    measures.fiscal_year,
    count(*) as rows_evaluated,
    count(*) filter (where measures.domain_status = 'out_of_domain') as rows_out_of_domain,
    count(*) filter (where measures.domain_status = 'denominator_not_positive') as rows_bad_denominator,
    any_value(checked_controls.domain_min) as domain_min,
    any_value(checked_controls.domain_max) as domain_max,
    any_value(checked_controls.denominator_floor) as denominator_floor,
    any_value(checked_controls.domain_reason) as domain_reason,
    measures.measure_control || ':' || coalesce(measures.fiscal_year::varchar, 'none') as check_key
from measures
inner join checked_controls on measures.measure_control = checked_controls.measure_control
group by
    measures.measure_control,
    measures.fiscal_year
