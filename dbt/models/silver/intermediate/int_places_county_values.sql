-- One row per county, PLACES release, data year, measure and value type (crude or age-adjusted): the value, its 95%
-- limits, footnote symbol and population. Rows without a 5-digit county code (the 2020 release, national rows) are not
-- typed. is_all_states is true only where the release's county rows for that measure, value type and year cover all 50
-- states and DC. Releases and value types are never mixed (failure modes 449 to 452).
{{ config(materialized='table') }}

with

periods as (
    select
        member_sha256,
        vintage
    from {{ ref('geography_file_periods') }}
    where bronze_table = 'places'
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
        {{ county_fips('stg.locationid') }} as county_fips,
        {{ strict_number('stg.data_value') }} as data_value,
        {{ strict_number('stg.low_confidence_limit') }} as low_confidence_limit,
        {{ strict_number('stg.high_confidence_limit') }} as high_confidence_limit,
        {{ strict_number("replace(stg.totalpopulation, ',', '')") }} as total_population
    from {{ ref('stg_places') }} as stg
    left join periods on stg._member_sha256 = periods.member_sha256
    where regexp_full_match(trim(stg.locationid), '[0-9]{5}')
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
