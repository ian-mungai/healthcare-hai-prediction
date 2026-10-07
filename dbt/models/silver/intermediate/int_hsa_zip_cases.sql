-- One row per hospital service area row: Medicare cases, days and charges per provider and beneficiary ZIP code and data
-- year (the job plan's catalog coverage). 2015 suppresses whole rows (ZIP *, no counts); later years show the ZIP and
-- publish suppressed counts as *: both become flags and null counts, never zeros, and suppressed mass is not inferred.
-- The provider ID stays as published (letters and zeros kept); repeated suppressed rows are all kept. Weights and
-- ZIP-to-county allocation are later work (failure modes 413 to 416).
{{ config(materialized='table') }}

with

rows as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        medicare_prov_num,
        zip_cd_of_residence,
        total_cases,
        total_days_of_care,
        total_charges
    from {{ ref('stg_cms_hsa_csv') }}
),

periods as (
    select
        member_sha256,
        vintage,
        period_start,
        period_end
    from {{ ref('geography_file_periods') }}
    where bronze_table = 'cms_hsa_csv'
)

select
    rows.member_sha256,
    rows.source_row_number,
    periods.vintage as data_year,
    periods.period_start,
    periods.period_end,
    nullif(trim(rows.medicare_prov_num), '') as ccn_published,
    rows.zip_cd_of_residence as zip_published,
    {{ zip_code('rows.zip_cd_of_residence') }} as zip_code,
    {{ strict_number('rows.total_cases') }} as total_cases,
    {{ strict_number('rows.total_days_of_care') }} as total_days_of_care,
    {{ strict_number('rows.total_charges') }} as total_charges,
    rows.member_sha256 || ':' || rows.source_row_number as hsa_row_key,
    coalesce(regexp_full_match(upper(trim(rows.medicare_prov_num)), '[0-9A-Z]{6}'), false) as is_ccn_shape_valid,
    coalesce(trim(rows.zip_cd_of_residence) = '*', false) as is_zip_suppressed,
    coalesce(trim(rows.zip_cd_of_residence), '') = '' as is_zip_missing,
    coalesce(trim(rows.total_cases) = '*', false) as is_cases_suppressed,
    coalesce(trim(rows.total_days_of_care) = '*', false) as is_days_suppressed,
    coalesce(trim(rows.total_charges) = '*', false) as is_charges_suppressed
from rows
left join periods on rows.member_sha256 = periods.member_sha256
