-- One row per census tract or ZIP code and Rural-Urban Commuting Area vintage: 2020 tracts and ZIP codes and 2010 ZIP
-- codes from their CSVs, 2010 tracts from the declared workbook sheet. Primary (1 to 10) and secondary codes (the 22
-- published labels such as 10.3) stay text categories; 99 (not coded) and blanks are null with the published value kept.
-- ZIP codes are postal codes, never ZCTAs; a tract keeps its 2020 county and, for 2020, the 2023 county (Connecticut
-- planning regions). RUCA 2020 was published in 2025: the vintage is not a publication date (failure modes 400, 407, 410 and 411).
{{ config(materialized='table') }}

with

periods as (
    select
        member_sha256,
        vintage
    from {{ ref('geography_file_periods') }}
    where bronze_table in ('ruca_tracts_2020', 'ruca_zip_2020', 'ruca_zip_2010')
),

tracts_2020 as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        null::varchar as sheet_name,
        'tract' as geography_type,
        tractfips20 as geography_published,
        countyfips20 as county_published,
        countyfips23 as county_2023_published,
        null::varchar as usps_state,
        null::varchar as zip_type,
        primaryruca as primary_published,
        secondaryruca as secondary_published
    from {{ ref('stg_ruca_tracts_2020') }}
),

zips_2020 as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        null::varchar as sheet_name,
        'zip' as geography_type,
        zipcode as geography_published,
        null::varchar as county_published,
        null::varchar as county_2023_published,
        upper(nullif(trim(state), '')) as usps_state,
        nullif(trim(zipcodetype), '') as zip_type,
        primaryruca as primary_published,
        secondaryruca as secondary_published
    from {{ ref('stg_ruca_zip_2020') }}
),

zips_2010 as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        null::varchar as sheet_name,
        'zip' as geography_type,
        zip_code as geography_published,
        null::varchar as county_published,
        null::varchar as county_2023_published,
        upper(nullif(trim(state), '')) as usps_state,
        nullif(trim(zip_type), '') as zip_type,
        ruca1 as primary_published,
        ruca2 as secondary_published
    from {{ ref('stg_ruca_zip_2010') }}
),

csv_rows as (
    select * from tracts_2020
    union all
    select * from zips_2020
    union all
    select * from zips_2010
),

csv_codes as (
    select
        csv_rows.*,
        periods.vintage
    from csv_rows
    left join periods on csv_rows.member_sha256 = periods.member_sha256
),

sheet_rows as (
    {{ geography_sheet_rows('ruca_sheet_rows') }}
),

sheet_codes as (
    select
        member_sha256,
        source_row_number,
        sheet_name,
        'tract' as geography_type,
        cells[4] as geography_published,
        cells[1] as county_published,
        null::varchar as county_2023_published,
        null::varchar as usps_state,
        null::varchar as zip_type,
        cells[5] as primary_published,
        cells[6] as secondary_published,
        vintage
    from sheet_rows
),

codes as (
    select * from csv_codes
    union all
    select * from sheet_codes
),

typed as (
    select
        member_sha256,
        source_row_number,
        sheet_name,
        vintage,
        geography_type,
        geography_published,
        case
            when geography_type = 'zip' then {{ zip_code('geography_published') }}
            when regexp_full_match(trim(geography_published), '[0-9]{11}') then trim(geography_published)
        end as geography_id,
        {{ county_fips('county_published') }} as county_fips,
        {{ county_fips('county_2023_published') }} as county_fips_2023,
        usps_state,
        zip_type,
        nullif(trim(primary_published), '') as primary_published,
        nullif(trim(secondary_published), '') as secondary_published,
        {{ category_code('primary_published') }} as primary_category,
        {{ category_code('secondary_published') }} as secondary_category
    from codes
)

select
    member_sha256,
    source_row_number,
    sheet_name,
    vintage,
    geography_type,
    geography_id,
    geography_published,
    county_fips,
    county_fips_2023,
    usps_state,
    zip_type,
    primary_published,
    secondary_published,
    member_sha256 || ':' || coalesce(sheet_name, '') || ':' || source_row_number as ruca_row_key,
    case when primary_category in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10') then primary_category end as primary_ruca,
    case when secondary_category in {{ ruca_secondary_codes() }} and secondary_category <> '99' then secondary_category end as secondary_ruca,
    left(county_fips, 2) = '09' as is_connecticut
from typed
