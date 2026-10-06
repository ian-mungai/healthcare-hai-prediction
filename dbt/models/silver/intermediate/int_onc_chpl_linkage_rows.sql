-- One row per hospital, program year, performance period and certified product (CHPL ID) of the ONC Promoting
-- Interoperability linkage. The criterion is true for Y and false for N; blank is null. Dates and the year are typed; the
-- CCN follows the B5a rule. The telephone column is left out (failure modes 380 to 382).
{{ config(materialized='table') }}

with

linkage as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        nullif(trim(facility_id), '') as facility_id,
        {{ published_ccn('facility_id') }} as ccn,
        nullif(trim(facility_name), '') as facility_name,
        nullif(trim(address), '') as address,
        nullif(trim(city_town), '') as city_town,
        upper(nullif(trim(state), '')) as state,
        nullif(trim(zip_code), '') as zip_code,
        nullif(trim(county_parish), '') as county_parish,
        {{ yes_no('meets_criteria_for_promoting_interoperability_of_ehrs') }} as meets_criteria_for_promoting_interoperability_of_ehrs,
        {{ month_day_year('start_date') }} as start_date,
        {{ month_day_year('end_date') }} as end_date,
        {{ year_number('year') }} as program_year,
        nullif(trim(cehrt_id), '') as cehrt_id,
        nullif(trim(chpl_id), '') as chpl_id,
        nullif(trim(product_database_id), '') as product_database_id,
        nullif(trim(developer_name), '') as developer_name,
        nullif(trim(product_name), '') as product_name
    from {{ ref('stg_onc_pi_chpl_linkage_csv') }}
)

select
    *,
    concat_ws(
        '|',
        coalesce(facility_id, ''),
        coalesce(program_year::varchar, ''),
        coalesce(start_date::varchar, ''),
        coalesce(end_date::varchar, ''),
        coalesce(chpl_id, '')
    ) as linkage_key
from linkage
