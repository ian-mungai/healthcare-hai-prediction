-- One Hospital General Information row per hospital (CCN) and stored file, with the file's release date and the number of
-- files on that date; the overall rating and emergency services are typed, the published text kept (failure modes 328 and
-- 329). Choosing a release for a hospital-year waits for the alignment step.
{{ config(materialized='table') }}

with

files as (
    select
        member_sha256,
        latest_publication_date as release_date
    from {{ ref('stg_bronze__files') }}
    where bronze_table = 'cms_cc_hospital_general_information'
),

dated as (
    select
        member_sha256,
        release_date,
        count(*) over (partition by release_date) as release_file_count
    from files
),

hgi as (
    select
        _member_sha256 as member_sha256,
        _object_key as object_key,
        _row_number as source_row_number,
        is_label_held,
        nullif(trim(coalesce(facility_id, provider_id)), '') as ccn,
        upper(nullif(trim(state), '')) as state_code,
        nullif(trim(coalesce(county_name, county_parish)), '') as county_name,
        case when regexp_full_match(trim(zip_code), '[0-9]{5}(-?[0-9]{4})?') then left(trim(zip_code), 5) end as zip_code,
        nullif(trim(hospital_type), '') as hospital_type,
        nullif(trim(hospital_ownership), '') as hospital_ownership,
        case upper(trim(emergency_services)) when 'YES' then true when 'NO' then false end as has_emergency_services,
        case when trim(hospital_overall_rating) in ('1', '2', '3', '4', '5') then trim(hospital_overall_rating)::integer end as overall_rating,
        nullif(trim(hospital_overall_rating), '') as overall_rating_text,
        nullif(trim(hospital_overall_rating_footnote), '') as overall_rating_footnote
    from {{ ref('stg_cms_cc_hospital_general_information') }}
)

select
    hgi.*,
    dated.release_date,
    dated.release_file_count,
    hgi.ccn || ':' || hgi.member_sha256 as release_key
from hgi
left join dated on hgi.member_sha256 = dated.member_sha256
