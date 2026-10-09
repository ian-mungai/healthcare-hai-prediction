-- HUD ZIP-to-county rows whose code is not a county of the states, DC or territories: two-digit island codes and 99999.
-- They are removed from int_hud_zip_county_quarters and kept here so every bronze row is counted
-- (failure modes 401 and 419).
{{ config(materialized='table') }}

with

rows as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        zip,
        geoid,
        state
    from {{ ref('stg_hud_zip_county') }}
),

periods as (
    select
        member_sha256,
        vintage
    from {{ ref('geography_file_periods') }}
    where bronze_table = 'hud_zip_county'
),

typed as (
    select
        rows.member_sha256,
        rows.source_row_number,
        periods.vintage as quarter_label,
        {{ zip_code('rows.zip') }} as zip_code,
        rows.geoid as geoid_published,
        upper(nullif(trim(rows.state), '')) as usps_state,
        {{ county_scope(county_fips('rows.geoid')) }} as county_scope
    from rows
    left join periods on rows.member_sha256 = periods.member_sha256
)

select
    member_sha256,
    source_row_number,
    quarter_label,
    zip_code,
    geoid_published,
    usps_state,
    'not_county_code' as hold_reason,
    member_sha256 || ':' || source_row_number as hud_row_key
from typed
where county_scope = 'not_county'
