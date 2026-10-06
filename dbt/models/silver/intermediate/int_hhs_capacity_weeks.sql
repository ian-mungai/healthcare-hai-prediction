-- One row per HHS hospital (hospital_pk) and collection week, with every weekly number typed. A count of 1 to 3 is published
-- as -999999: it is null and its column is listed in suppressed_fields. The CCN follows the B5a rule; a hospital without
-- one keeps its hospital_pk. Any other negative value, which a count or occupancy cannot be, is null and its column is
-- listed in negative_fields. is_corrected and the coverage counts are kept; nothing is filtered (failure modes 375 to 379).
-- The geocoded point is left out; the ZIP and county code give the place.
{{ config(materialized='table') }}

with

weeks as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        nullif(trim(hospital_pk), '') as hospital_pk,
        {{ slash_date('collection_week') }} as collection_week,
        upper(nullif(trim(state), '')) as state,
        {{ published_ccn('ccn') }} as ccn,
        upper(nullif(trim(ccn), '')) as ccn_published,
        nullif(trim(hospital_name), '') as hospital_name,
        nullif(trim(address), '') as address,
        nullif(trim(city), '') as city,
        nullif(trim(zip), '') as zip,
        nullif(trim(hospital_subtype), '') as hospital_subtype,
        nullif(trim(fips_code), '') as fips_code,
        {{ true_false('is_metro_micro') }} as is_metro_micro,
        {{ true_false('is_corrected') }} as is_corrected,
        nullif(trim(hhs_ids), '') as hhs_ids,
        {%- for column in hhs_numeric_columns() %}
        {{ hhs_number(column) }} as {{ column }},
        {%- endfor %}
        list_sort(list_distinct([
            {%- for column in hhs_numeric_columns() %}
            case when trim({{ column }}) = '-999999' then '{{ column }}' end{% if not loop.last %},{% endif %}
            {%- endfor %}
        ])) as suppressed_fields,
        list_sort(list_distinct([
            {%- for column in hhs_numeric_columns() %}
            case when trim({{ column }}) <> '-999999' and ({{ strict_number(column) }}) < 0 then '{{ column }}' end{% if not loop.last %},{% endif %}
            {%- endfor %}
        ])) as negative_fields
    from {{ ref('stg_hhs_capacity_csv') }}
)

select
    *,
    hospital_pk || ':' || collection_week::varchar as week_key
from weeks
