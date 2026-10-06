-- One row per Medicare inpatient provider summary (CCN) and file: the data and release years from the file name, the
-- place columns as published and every numeric column typed; a blank (suppressed) value is null (failure modes 355 to 359).
{{ config(materialized='table') }}

with

providers as (
    select
        _member_sha256 as member_sha256,
        _member_path as member_path,
        nullif(trim(rndrng_prvdr_ccn), '') as ccn,
        nullif(trim(rndrng_prvdr_org_name), '') as provider_name,
        upper(nullif(trim(rndrng_prvdr_state_abrvtn), '')) as state_code,
        nullif(trim(rndrng_prvdr_state_fips), '') as state_fips,
        nullif(trim(rndrng_prvdr_zip5), '') as zip5,
        nullif(trim(rndrng_prvdr_ruca), '') as ruca,
        try_cast(regexp_extract(lower(_member_path), 'dy([0-9]{2})', 1) as integer) + 2000 as data_year,
        try_cast(regexp_extract(lower(_member_path), 'ry([0-9]{2})', 1) as integer) + 2000 as release_year,
        {%- for column in mup_provider_numeric_columns() %}
        {{ strict_number(column) }} as {{ column }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ ref('stg_cms_medicare_inpatient_by_provider') }}
)

select
    *,
    member_sha256 || ':' || ccn as provider_key
from providers
