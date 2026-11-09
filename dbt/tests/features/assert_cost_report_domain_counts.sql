-- Fails when int_cost_report_domain_checks differs, for any control and fiscal year that states a domain or a floor, from the counts recomputed here from the
-- numerator, the denominator and the recipe bounds, or from the nulls that int_cost_report_measures produced because of a bound or a floor; and for each row
-- whose domain_status disagrees with its value. The recomputation does not read domain_status, so a status and a count that agree with each other but not
-- with the data fail.
with

bounds as (
    select
        measure_control,
        max(case when role in ('value', 'numerator') then domain_min end) as domain_min,
        max(case when role in ('value', 'numerator') then domain_max end) as domain_max,
        max(case when role = 'denominator' then denominator_floor end) as denominator_floor
    from {{ ref('cost_report_measures_recipe') }}
    group by measure_control
    having
        max(case when role in ('value', 'numerator') then domain_min end) is not null
        or max(case when role in ('value', 'numerator') then domain_max end) is not null
        or max(case when role = 'denominator' then denominator_floor end) is not null
),

recomputed as (
    -- The value by the rule before any bound, from the same inputs the model reads; a bad denominator is null or not above the floor.
    select
        measures.measure_key,
        measures.measure_control,
        measures.fiscal_year,
        measures.value_number,
        measures.domain_status,
        bounds.domain_min,
        bounds.domain_max,
        coalesce(
            bounds.denominator_floor is not null and (measures.denominator is null or measures.denominator <= bounds.denominator_floor),
            false
        ) as is_bad_denominator,
        case measures.rule
            when 'value' then measures.numerator
            when 'ratio' then measures.numerator / nullif(measures.denominator, 0)
            when 'per_day' then measures.numerator / nullif(reports.reporting_days, 0)
            when 'ratio_per_day_denominator' then measures.numerator / nullif(measures.denominator / nullif(reports.reporting_days, 0), 0)
        end as unbounded_value
    from {{ ref('int_cost_report_measures') }} as measures
    inner join bounds on measures.measure_control = bounds.measure_control
    inner join {{ ref('fct_cost_report') }} as reports on measures.rpt_rec_num = reports.rpt_rec_num
),

classified as (
    select
        measure_key,
        measure_control,
        fiscal_year,
        value_number,
        domain_status,
        is_bad_denominator,
        unbounded_value,
        not is_bad_denominator and (unbounded_value < domain_min or unbounded_value > domain_max) as is_out_of_domain
    from recomputed
),

expected as (
    select
        measure_control,
        fiscal_year,
        count(*) as rows_evaluated,
        count(*) filter (where is_out_of_domain) as rows_out_of_domain,
        count(*) filter (where is_bad_denominator) as rows_bad_denominator,
        count(*) filter (where value_number is null and (is_bad_denominator or unbounded_value is not null)) as rows_nulled
    from classified
    group by
        measure_control,
        fiscal_year
),

compared as (
    select
        expected.rows_evaluated as expected_evaluated,
        expected.rows_out_of_domain as expected_out_of_domain,
        expected.rows_bad_denominator as expected_bad_denominator,
        expected.rows_nulled as expected_nulled,
        checks.rows_evaluated as found_evaluated,
        checks.rows_out_of_domain as found_out_of_domain,
        checks.rows_bad_denominator as found_bad_denominator,
        coalesce(expected.measure_control, checks.measure_control) as measure_control,
        coalesce(expected.fiscal_year, checks.fiscal_year) as fiscal_year
    from expected
    full outer join {{ ref('int_cost_report_domain_checks') }} as checks
        on
            expected.measure_control = checks.measure_control
            and expected.fiscal_year is not distinct from checks.fiscal_year
)

select
    'count row differs from the recomputed counts' as failure,
    measure_control || ' ' || coalesce(fiscal_year::varchar, 'no year')
    || ' expected ' || coalesce(expected_evaluated::varchar, 'none') || '/' || coalesce(expected_out_of_domain::varchar, 'none')
    || '/' || coalesce(expected_bad_denominator::varchar, 'none')
    || ' found ' || coalesce(found_evaluated::varchar, 'none') || '/' || coalesce(found_out_of_domain::varchar, 'none')
    || '/' || coalesce(found_bad_denominator::varchar, 'none') as detail
from compared
where
    expected_evaluated is distinct from found_evaluated
    or expected_out_of_domain is distinct from found_out_of_domain
    or expected_bad_denominator is distinct from found_bad_denominator
union all
select
    'count model differs from the nulls the measure model produced' as failure,
    measure_control || ' ' || coalesce(fiscal_year::varchar, 'no year')
    || ' nulled ' || expected_nulled::varchar || ' counted ' || (found_out_of_domain + found_bad_denominator)::varchar as detail
from compared
where expected_nulled <> found_out_of_domain + found_bad_denominator
union all
select
    'domain status disagrees with the value' as failure,
    measure_key || ' ' || domain_status as detail
from classified
where
    domain_status is distinct from (
        case when is_bad_denominator then 'denominator_not_positive' when is_out_of_domain then 'out_of_domain' else 'in_domain' end
    )
    or (domain_status <> 'in_domain' and value_number is not null)
