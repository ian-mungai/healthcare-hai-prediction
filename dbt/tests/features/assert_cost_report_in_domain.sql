-- Fails for each published cost-report value that lies outside the domain its control states in cost_report_measures_recipe (domain_min, domain_max),
-- and for each value published with a denominator that is not above the recipe's floor (denominator_floor). It reads int_cost_report_measures and the
-- cost-report rows of int_spine_hospital_measures, so a value that leaves the measure model is caught again in the spine model that the matrix pivots.

with

bounds as (
    -- One row per control that states a domain or a floor: the domain sits on the value or numerator row, the floor on the denominator row.
    select
        measure_control,
        max(case when role in ('value', 'numerator') then domain_min end) as domain_min,
        max(case when role in ('value', 'numerator') then domain_max end) as domain_max,
        max(case when role = 'denominator' then denominator_floor end) as denominator_floor
    from {{ ref('cost_report_measures_recipe') }}
    group by measure_control
),

published as (
    select
        'int_cost_report_measures' as model_name,
        measure_key as row_key,
        measure_control,
        value_number,
        denominator
    from {{ ref('int_cost_report_measures') }}
    union all
    select
        'int_spine_hospital_measures' as model_name,
        alignment_key as row_key,
        measure_control,
        value_number,
        null::double as denominator
    from {{ ref('int_spine_hospital_measures') }}
    where measure_source = 'cost_report'
)

select
    published.model_name,
    published.row_key,
    published.measure_control,
    case
        when published.value_number < bounds.domain_min then 'value below the domain minimum'
        when published.value_number > bounds.domain_max then 'value above the domain maximum'
        else 'value published with a denominator that is not above the floor'
    end as failure
from published
inner join bounds on published.measure_control = bounds.measure_control
where
    published.value_number < bounds.domain_min
    or published.value_number > bounds.domain_max
    or (published.value_number is not null and published.denominator <= bounds.denominator_floor)
