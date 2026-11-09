-- Fails for each county derived row that is missing or has the wrong value (design 2.11). Density (C191 density_log) is the natural log of the SVI
-- population over the area in square miles and is null when either is not above 0 or missing. The ordinal of the rural-urban continuum code (C198) is the
-- code as a number from 1 to 9 and is null for any other text. The expected values come from the parent rows of the same spine key and literal rules.

with

density as (
    select
        population.spine_key,
        'C191' as measure_control,
        'density_log' as derived_field,
        case when population.value_number > 0 and area.value_number > 0 then ln(population.value_number / area.value_number) end as expected_value
    from {{ ref('int_spine_county_context') }} as population
    inner join {{ ref('int_spine_county_context') }} as area
        on
            population.spine_key = area.spine_key
            and area.measure_source = 'svi'
            and area.measure_control = 'C191'
            and area.field = 'area_sqmi'
    where
        population.measure_source = 'svi'
        and population.measure_control = 'C191'
        and population.field = 'e_totpop'
),

rucc as (
    select
        spine_key,
        measure_control,
        'rucc_code_ordinal' as derived_field,
        case when try_cast(value_text as integer) between 1 and 9 then try_cast(value_text as integer) end as expected_value
    from {{ ref('int_spine_county_measures') }}
    where
        measure_source = 'rucc'
        and measure_control = 'C198'
        and field = 'rucc_code'
),

expected as (
    select * from density
    union all
    select * from rucc
),

published as (
    select
        spine_key,
        measure_control,
        field,
        alignment_key,
        value_number
    from {{ ref('int_spine_county_context') }}
    where field = 'density_log'
    union all
    select
        spine_key,
        measure_control,
        field,
        alignment_key,
        value_number
    from {{ ref('int_spine_county_measures') }}
    where field = 'rucc_code_ordinal'
)

select
    expected.spine_key || ':' || expected.measure_control || ':' || expected.derived_field as row_key,
    case
        when published.alignment_key is null then 'derived row missing'
        else 'derived value differs from the arithmetic of the parent'
    end as failure
from expected
left join published
    on
        expected.spine_key = published.spine_key
        and expected.measure_control = published.measure_control
        and expected.derived_field = published.field
where
    published.alignment_key is null
    or (published.value_number is null) <> (expected.expected_value is null)
    or abs(published.value_number - expected.expected_value) > 0.000000001
