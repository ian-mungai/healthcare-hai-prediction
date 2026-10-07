-- One row per county and Rural-Urban Continuum Code vintage: 2023 from the long CSV (one row per county and attribute)
-- and 2013 from its declared workbook sheet. The code is a text category 1 to 9 ('2.0' in the workbook is '2'); a blank
-- code is null, and a county without a code row keeps its population and description. The vintage is the reference year,
-- not the publication date (failure modes 400, 402, 410 and 411).
{{ config(materialized='table') }}

with

long_rows as (
    select
        _member_sha256 as member_sha256,
        fips,
        state,
        county_name,
        attribute,
        value
    from {{ ref('stg_rucc') }}
),

periods as (
    select
        member_sha256,
        vintage
    from {{ ref('geography_file_periods') }}
    where bronze_table = 'rucc'
),

long_counties as (
    select
        long_rows.member_sha256,
        periods.vintage,
        trim(long_rows.fips) as county_published,
        max(nullif(trim(long_rows.state), '')) as state,
        max(nullif(trim(long_rows.county_name), '')) as county_name,
        max(nullif(trim(long_rows.value), '')) filter (where trim(long_rows.attribute) = 'RUCC_' || periods.vintage) as rucc_published,
        max(nullif(trim(long_rows.value), '')) filter (where trim(long_rows.attribute) like 'Population\_%' escape '\') as population_published,
        max(nullif(trim(long_rows.value), '')) filter (where trim(long_rows.attribute) = 'Description') as description
    from long_rows
    left join periods on long_rows.member_sha256 = periods.member_sha256
    group by
        long_rows.member_sha256,
        periods.vintage,
        trim(long_rows.fips)
),

sheet_rows as (
    {{ geography_sheet_rows('rucc_sheet_rows') }}
),

sheet_counties as (
    select
        member_sha256,
        vintage,
        trim(cells[1]) as county_published,
        nullif(trim(cells[2]), '') as state,
        nullif(trim(cells[3]), '') as county_name,
        nullif(trim(cells[5]), '') as rucc_published,
        nullif(trim(cells[4]), '') as population_published,
        nullif(trim(cells[6]), '') as description
    from sheet_rows
),

counties as (
    select * from long_counties
    union all
    select * from sheet_counties
),

typed as (
    select
        *,
        {{ county_fips('county_published') }} as county_fips,
        {{ category_code('rucc_published') }} as rucc_category
    from counties
)

select
    member_sha256,
    vintage,
    county_fips,
    county_published,
    state,
    county_name,
    rucc_published,
    description,
    {{ strict_number('population_published') }} as population,
    population_published,
    {{ county_scope('county_fips') }} as county_scope,
    member_sha256 || ':' || county_published as rucc_key,
    case when rucc_category in ('1', '2', '3', '4', '5', '6', '7', '8', '9') then rucc_category end as rucc_code,
    left(county_fips, 2) = '09' as is_connecticut
from typed
