-- One row per hospital (CCN) and version of its Hospital General Information description, dated by release dates only
-- (SCD2 source for dim_hospital, silver step 7.3, failure modes 670 to 677). The overall rating is a measure that AL2
-- aligns, so it is not tracked here. Two files on one release date that disagree for a CCN are held for that date [674].
{{ config(materialized='table') }}

with

releases as (
    select
        release_date,
        row_number() over (order by release_date) as release_number
    from (
        select distinct release_date from {{ ref('int_hgi_hospital_releases') }}
        where release_date is not null
    )
),

per_release as (
    select
        hgi.ccn,
        releases.release_date,
        releases.release_number,
        min(hgi.member_sha256) as member_sha256,
        count(distinct row(
            hgi.state_code, hgi.county_name, hgi.zip_code, hgi.hospital_type, hgi.hospital_ownership,
            hgi.has_emergency_services
        )) as distinct_sets,
        any_value(hgi.state_code) as state_code,
        any_value(hgi.county_name) as county_name,
        any_value(hgi.zip_code) as zip_code,
        any_value(hgi.hospital_type) as hospital_type,
        any_value(hgi.hospital_ownership) as hospital_ownership,
        any_value(hgi.has_emergency_services) as has_emergency_services
    from {{ ref('int_hgi_hospital_releases') }} as hgi
    inner join releases on hgi.release_date = releases.release_date
    where hgi.ccn is not null and not hgi.is_label_held
    group by hgi.ccn, releases.release_date, releases.release_number
),

agreeing as (
    select * exclude (distinct_sets)
    from per_release
    where distinct_sets = 1
),

{{ scd2_versions('agreeing', 'releases', ['ccn'],
    ['state_code', 'county_name', 'zip_code', 'hospital_type', 'hospital_ownership', 'has_emergency_services']) }}

select
    *,
    ccn || ':' || valid_from as history_key
from dated_versions
