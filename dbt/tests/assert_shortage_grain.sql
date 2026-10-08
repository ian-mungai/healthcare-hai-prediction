-- Fails for a repeated grain among kept rows: an MMD file with two values for one control, year and county or state; an
-- HPSA file with two rows for one ID, designation date and geography; an MUA file with two rows for one component of one
-- designation [476].
select
    'mmd' as checked_source,
    member_sha256 || ':' || measure_control || ':' || data_year || ':' || coalesce(county_fips, state_fips) as grain
from {{ ref('int_mmd_prevalence') }}
group by member_sha256, measure_control, data_year, geography_level, coalesce(county_fips, state_fips)
having count(*) > 1
union all
select
    'hpsa' as checked_source,
    member_sha256 || ':' || hpsa_id || ':' || coalesce(designation_date_published, '') || ':' || coalesce(geography_id, '') as grain
from {{ ref('int_hpsa_components') }}
where hold_reason is null
group by member_sha256, hpsa_id, designation_date_published, geography_id
having count(*) > 1
union all
select
    'mua' as checked_source,
    member_sha256 || ':' || mua_id || ':' || designation_type_code || ':' || coalesce(designation_date_published, '') || ':'
    || coalesce(component_name, '') as grain
from {{ ref('int_mua_components') }}
where hold_reason is null
group by
    member_sha256,
    mua_id,
    designation_type_code,
    designation_date_published,
    component_type,
    county_code_published,
    county_subdivision_code,
    component_name
having count(*) > 1
