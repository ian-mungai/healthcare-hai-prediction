-- One row per county, year and WONDER export: deaths, population and the crude rate with its 95% limits and standard
-- error; Suppressed, Unreliable, Missing and Not Available are null with the token kept. Each row keeps its database
-- (bridged race 1999 to 2020, single race 2018 to 2024); overlapping years stay apart. A county and year that a longer
-- export of the same database repeats with identical values is held as repeated_in_wider_export
-- (3,142 rows). The year is year_code (failure modes 457 to 459).
{{ config(materialized='table') }}

with

periods as (
    select
        member_sha256,
        vintage,
        period_start,
        period_end
    from {{ ref('geography_file_periods') }}
    where bronze_table = 'wonder_county_mortality'
),

typed as (
    select
        stg._member_sha256 as member_sha256,
        stg._row_number as source_row_number,
        periods.vintage as wonder_database,
        periods.period_start,
        periods.period_end,
        stg.deaths as deaths_published,
        stg.crude_rate as crude_rate_published,
        {{ county_fips('stg.county_code') }} as county_fips,
        try_cast(trim(stg.year_code) as integer) as data_year,
        {{ strict_number('stg.deaths') }} as deaths,
        {{ strict_number('stg.population') }} as population,
        {{ strict_number('stg.crude_rate') }} as crude_rate,
        {{ strict_number('stg.crude_rate_lower_95_confidence_interval') }} as crude_rate_lower_95,
        {{ strict_number('stg.crude_rate_upper_95_confidence_interval') }} as crude_rate_upper_95,
        {{ strict_number('stg.crude_rate_standard_error') }} as crude_rate_standard_error,
        case when trim(stg.deaths) in {{ wonder_tokens() }} then trim(stg.deaths) end as deaths_token,
        case when trim(stg.crude_rate) in {{ wonder_tokens() }} then trim(stg.crude_rate) end as crude_rate_token
    from {{ ref('stg_wonder_county_mortality') }} as stg
    left join periods on stg._member_sha256 = periods.member_sha256
),

ranked as (
    select
        *,
        row_number() over (
            partition by wonder_database, county_fips, data_year
            order by period_end - period_start desc, member_sha256
        ) as export_rank
    from typed
)

select
    member_sha256,
    source_row_number,
    wonder_database,
    period_start,
    period_end,
    county_fips,
    data_year,
    deaths_published,
    deaths,
    deaths_token,
    population,
    crude_rate_published,
    crude_rate,
    crude_rate_token,
    crude_rate_lower_95,
    crude_rate_upper_95,
    crude_rate_standard_error,
    case when export_rank > 1 then 'repeated_in_wider_export' end as hold_reason,
    {{ county_scope('county_fips') }} as county_scope,
    member_sha256 || ':' || source_row_number as wonder_row_key,
    left(county_fips, 2) = '09' as is_connecticut
from ranked
