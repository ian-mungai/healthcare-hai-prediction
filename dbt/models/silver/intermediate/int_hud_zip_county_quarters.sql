-- One row per HUD ZIP-to-county pair and USPS quarter, with the four address ratios typed. The quarter is the one the
-- capture receipt records. Codes outside the states, DC and territories go to int_hud_zip_county_holds instead (approved
-- Oct 1 2026). Connecticut is kept and flagged. Each ratio type keeps its own denominator; has_residential_addresses is
-- false for a ZIP whose residential ratios are all 0, which gets no residential county (failure modes 400 to 407).
{{ config(materialized='table') }}

with

rows as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        zip,
        geoid,
        state,
        city,
        res_ratio,
        bus_ratio,
        oth_ratio,
        tot_ratio
    from {{ ref('stg_hud_zip_county') }}
),

periods as (
    select
        member_sha256,
        vintage,
        period_start,
        period_end
    from {{ ref('geography_file_periods') }}
    where bronze_table = 'hud_zip_county'
),

typed as (
    select
        rows.member_sha256,
        rows.source_row_number,
        periods.vintage as quarter_label,
        periods.period_start as quarter_start_date,
        periods.period_end as quarter_end_date,
        {{ zip_code('rows.zip') }} as zip_code,
        rows.geoid as geoid_published,
        {{ county_fips('rows.geoid') }} as county_fips,
        upper(nullif(trim(rows.state), '')) as usps_state,
        nullif(trim(rows.city), '') as city,
        {{ strict_number('rows.res_ratio') }} as res_ratio,
        {{ strict_number('rows.bus_ratio') }} as bus_ratio,
        {{ strict_number('rows.oth_ratio') }} as oth_ratio,
        {{ strict_number('rows.tot_ratio') }} as tot_ratio
    from rows
    left join periods on rows.member_sha256 = periods.member_sha256
),

scoped as (
    select
        *,
        {{ county_scope('county_fips') }} as county_scope,
        -- Rounded so the parallel sum's last-digit noise cannot change a rebuild [418].
        round(sum(res_ratio) over (partition by member_sha256, zip_code), 9) as zip_residential_ratio_sum
    from typed
)

select
    member_sha256,
    source_row_number,
    quarter_label,
    quarter_start_date,
    quarter_end_date,
    zip_code,
    county_fips,
    county_scope,
    usps_state,
    city,
    res_ratio,
    bus_ratio,
    oth_ratio,
    tot_ratio,
    zip_residential_ratio_sum,
    member_sha256 || ':' || source_row_number as hud_row_key,
    left(county_fips, 2) = '09' as is_connecticut,
    coalesce(zip_residential_ratio_sum > 0, false) as has_residential_addresses
from scoped
where county_scope <> 'not_county'
