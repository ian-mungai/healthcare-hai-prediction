-- bridge_county_planning_region has one row per old Connecticut county, planning region and RUCA tract file, and its tract_count is the number of tract rows
-- of that pair in the file. The tracts are found by the state prefix of the tract ID, not by the county code the model reads. A tract with no 2023
-- county keeps a null planning region.
with

expected as (
    select
        county_fips as old_county_fips,
        county_fips_2023 as planning_region_fips,
        member_sha256,
        count(geography_id) as tract_count
    from {{ ref('int_ruca_codes') }}
    where vintage = '2020' and geography_type = 'tract' and left(geography_id, 2) = '09'
    group by county_fips, county_fips_2023, member_sha256
)

select
    bridge.tract_count as bridge_count,
    expected.tract_count as expected_count,
    coalesce(bridge.old_county_fips, expected.old_county_fips) as old_county_fips,
    coalesce(bridge.planning_region_fips, expected.planning_region_fips) as planning_region_fips
from {{ ref('bridge_county_planning_region') }} as bridge
full outer join expected
    on
        bridge.old_county_fips is not distinct from expected.old_county_fips
        and bridge.planning_region_fips is not distinct from expected.planning_region_fips
        and bridge.member_sha256 = expected.member_sha256
where bridge.tract_count is distinct from expected.tract_count
