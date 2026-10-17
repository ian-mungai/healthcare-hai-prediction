-- One row per old Connecticut county, planning region and RUCA tract file: the many-to-many between a 2020 tract's old county and the planning region
-- the 2023 county codes give it, with the number of tract rows as the only weight (silver holds no tract population). The tracts are the 2020 tract
-- rows of the old Connecticut counties (county FIPS 09001 to 09015). A tract with no 2023 county keeps a null planning region, so the tract counts of
-- a file add up to its Connecticut tract rows [943].
with

counted as (
    select
        county_fips,
        county_fips_2023,
        member_sha256,
        count(*) as tract_count
    from {{ ref('int_ruca_codes') }}
    where vintage = '2020' and geography_type = 'tract' and county_fips like '09%'
    group by county_fips, county_fips_2023, member_sha256
)

select
    county_fips as old_county_fips,
    county_fips_2023 as planning_region_fips,
    tract_count,
    member_sha256
from counted
