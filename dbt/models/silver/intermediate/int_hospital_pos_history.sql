-- One row per hospital (CCN) and version of its Provider of Services attributes, dated by snapshot period ends only, so a
-- clean checkout rebuilds the same versions (SCD2 source for dim_hospital, silver step 7.3, failure modes 670 to 677 in
-- plans/silver_processed_zone_20261009/plan.md). Versions come from scd2_versions: a change in a tracked attribute or a
-- snapshot without the CCN starts a new one. Values are as published; fills stay on the spine.
{{ config(materialized='table') }}

with

releases as (
    -- Every snapshot in order, so a CCN missing from one snapshot is a gap [673].
    select
        period_end as release_date,
        row_number() over (order by period_end) as release_number
    from (
        select distinct period_end from {{ ref('int_pos_hospital_snapshots') }}
        where period_end is not null
    )
),

per_release as (
    -- A CCN with disagreeing rows in one snapshot is held for that snapshot, not versioned [674].
    select
        pos.ccn,
        releases.release_date,
        releases.release_number,
        min(pos.member_sha256) as member_sha256,
        count(distinct row(
            pos.state_code, pos.county_fips, pos.ssa_county_code, pos.zip_code, pos.provider_subtype_code,
            pos.control_type_code, pos.termination_code, pos.cbsa_urban_rural_code, pos.bed_count, pos.certified_bed_count
        ))
            as distinct_sets,
        any_value(pos.state_code) as state_code,
        any_value(pos.county_fips) as county_fips,
        any_value(pos.ssa_county_code) as ssa_county_code,
        any_value(pos.zip_code) as zip_code,
        any_value(pos.provider_subtype_code) as provider_subtype_code,
        any_value(pos.control_type_code) as control_type_code,
        any_value(pos.termination_code) as termination_code,
        any_value(pos.cbsa_urban_rural_code) as cbsa_urban_rural_code,
        any_value(pos.bed_count) as bed_count,
        any_value(pos.certified_bed_count) as certified_bed_count
    from {{ ref('int_pos_hospital_snapshots') }} as pos
    inner join releases on pos.period_end = releases.release_date
    where pos.ccn is not null
    group by pos.ccn, releases.release_date, releases.release_number
),

agreeing as (
    select * exclude (distinct_sets)
    from per_release
    where distinct_sets = 1
),

{{ scd2_versions('agreeing', 'releases', ['ccn'],
    ['state_code', 'county_fips', 'ssa_county_code', 'zip_code', 'provider_subtype_code', 'control_type_code',
     'termination_code', 'cbsa_urban_rural_code', 'bed_count', 'certified_bed_count']) }}

select
    *,
    ccn || ':' || valid_from as history_key
from dated_versions
