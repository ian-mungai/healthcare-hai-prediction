-- Fails for each county derived row that is missing or has the wrong value (design 2.11). Density (C191 density_log) is the natural log of the SVI
-- population over the area in square miles and is null when either is not above 0 or missing. The ordinal of the rural-urban continuum code (C198) is the
-- code as a number from 1 to 9 and is null for any other text. The expected values come from the parent rows of the same spine key and literal rules.

-- Fails for each county derived row that is missing or has the wrong value or period (design 2.11). Density (C191 density_log) is the natural log of the SVI
-- population over the area in square miles of the same SVI edition: the latest edition that ends before the window start and has both a population and an area
-- (each one value, not missing). It is null when the area or the population is not above 0, and null with no period when no edition has both; the row takes
-- the period of the pair. An edition with a population and no area is skipped, never paired with the area of another edition. The ordinal of the rural-urban
-- continuum code (C198) is the code as a number from 1 to 9 and is null for any other text. The expected values come from the gold SVI and RUCC rows and
-- the spine, not from the parent rows of the model.

with

geos as (
    select
        spine_key,
        window_start,
        county_fips as geo_fips
    from {{ ref('int_hospital_spine') }}
    where county_fips is not null
    union
    select
        spine_key,
        window_start,
        planning_region_fips as geo_fips
    from {{ ref('int_hospital_spine') }}
    where planning_region_fips is not null
),

svi_values as (
    select
        geos.spine_key,
        svi.field,
        make_date(svi.edition::integer, 12, 31) as period_end,
        count(distinct svi.value_number) as value_count,
        count(*) filter (where svi.value_number is null) as missing_count,
        max(svi.value_number) as value_number
    from geos
    inner join {{ ref('fct_svi_county_value') }} as svi
        on
            geos.geo_fips = svi.county_fips
            and geos.window_start > make_date(svi.edition::integer, 12, 31)
    where svi.field in ('e_totpop', 'area_sqmi')
    group by
        geos.spine_key,
        svi.field,
        make_date(svi.edition::integer, 12, 31)
),

svi_pairs as (
    select
        population.spine_key,
        population.period_end,
        population.value_number as population,
        area.value_number as area
    from svi_values as population
    inner join svi_values as area
        on
            population.spine_key = area.spine_key
            and population.period_end = area.period_end
    where
        population.field = 'e_totpop'
        and area.field = 'area_sqmi'
        and population.value_count = 1
        and population.missing_count = 0
        and area.value_count = 1
        and area.missing_count = 0
),

latest_pairs as (
    select
        spine_key,
        period_end,
        population,
        area
    from svi_pairs
    qualify row_number() over (partition by spine_key order by period_end desc) = 1
),

density as (
    select
        spine.spine_key,
        'C191' as measure_control,
        'density_log' as derived_field,
        latest_pairs.period_end as expected_period_end,
        case when latest_pairs.population > 0 and latest_pairs.area > 0 then ln(latest_pairs.population / latest_pairs.area) end as expected_value
    from {{ ref('int_hospital_spine') }} as spine
    left join latest_pairs on spine.spine_key = latest_pairs.spine_key
),

rucc as (
    select
        spine_key,
        measure_control,
        'rucc_code_ordinal' as derived_field,
        period_end as expected_period_end,
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
        period_end,
        value_number
    from {{ ref('int_spine_county_context') }}
    where field = 'density_log'
    union all
    select
        spine_key,
        measure_control,
        field,
        alignment_key,
        period_end,
        value_number
    from {{ ref('int_spine_county_measures') }}
    where field = 'rucc_code_ordinal'
)

select
    expected.spine_key || ':' || expected.measure_control || ':' || expected.derived_field as row_key,
    case
        when published.alignment_key is null then 'derived row missing'
        when (published.period_end is null) <> (expected.expected_period_end is null) or published.period_end <> expected.expected_period_end
            then 'derived period differs from the period of the parent'
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
    or (published.period_end is null) <> (expected.expected_period_end is null)
    or published.period_end <> expected.expected_period_end
    or (published.value_number is null) <> (expected.expected_value is null)
    or abs(published.value_number - expected.expected_value) > 0.000000001
