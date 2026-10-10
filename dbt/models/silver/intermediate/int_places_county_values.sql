-- One row per county, PLACES release, data year, measure and value type (crude or age-adjusted): the value, its 95%
-- limits, footnote symbol and population. The 2020 release publishes no county code; its rows take the code PLACES
-- itself publishes for the same state and county name in its other releases (county_source). Rows with neither, such as
-- the national rows, are not typed. is_all_states is true only where the release's county rows for that measure, value
-- type and year cover all 50 states and DC. Releases and value types are never mixed (failure modes 449 to 452 and 623
-- to 625).
{{ config(materialized='table') }}

with

periods as (
    select
        member_sha256,
        vintage
    from {{ ref('geography_file_periods') }}
    where bronze_table = 'places'
),

names as (
    -- County codes by state and county name, from the rows that publish both; a pair with two codes is dropped [623].
    select
        trim(stateabbr) as state_abbr,
        trim(locationname) as location_name,
        min(trim(locationid)) as county_fips
    from {{ ref('stg_places') }}
    where regexp_full_match(trim(locationid), '[0-9]{5}')
    group by
        trim(stateabbr),
        trim(locationname)
    having count(distinct trim(locationid)) = 1
),

typed as (
    select
        stg._member_sha256 as member_sha256,
        stg._row_number as source_row_number,
        periods.vintage as edition,
        trim(stg.measureid) as measureid,
        trim(stg.datavaluetypeid) as datavaluetypeid,
        stg.data_value as data_value_published,
        nullif(trim(stg.data_value_footnote_symbol), '') as footnote_symbol,
        try_cast(trim(stg.year) as integer) as data_year,
        coalesce({{ county_fips('stg.locationid') }}, names.county_fips) as county_fips,
        case when regexp_full_match(trim(stg.locationid), '[0-9]{5}') then 'published' else 'places_name_crosswalk' end as county_source,
        {{ strict_number('stg.data_value') }} as data_value,
        {{ strict_number('stg.low_confidence_limit') }} as low_confidence_limit,
        {{ strict_number('stg.high_confidence_limit') }} as high_confidence_limit,
        {{ strict_number("replace(stg.totalpopulation, ',', '')") }} as total_population
    from {{ ref('stg_places') }} as stg
    left join periods on stg._member_sha256 = periods.member_sha256
    left join names
        on
            nullif(trim(stg.locationid), '') is null
            and trim(stg.stateabbr) = names.state_abbr
            and trim(stg.locationname) = names.location_name
    where
        regexp_full_match(trim(stg.locationid), '[0-9]{5}')
        or names.county_fips is not null
),

scoped as (
    select
        *,
        {{ county_scope('county_fips') }} as county_scope
    from typed
)

select
    *,
    member_sha256 || ':' || source_row_number as places_row_key,
    left(county_fips, 2) = '09' as is_connecticut,
    count(distinct case when county_scope = 'state' then left(county_fips, 2) end) over (
        partition by member_sha256, measureid, datavaluetypeid, data_year
    ) = 51 as is_all_states
from scoped
