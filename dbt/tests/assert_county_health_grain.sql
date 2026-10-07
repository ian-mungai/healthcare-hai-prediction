-- Fails for a repeated grain: a PLACES release with two values for one county, measure, value type and data year; the
-- geographic variation file with two county rows for one year; a WONDER export with two rows for one county and year
-- [451] [454] [457].
select
    'places' as checked_source,
    member_sha256 || ':' || county_fips || ':' || measureid || ':' || datavaluetypeid || ':' || data_year as grain
from {{ ref('int_places_county_values') }}
group by member_sha256, county_fips, measureid, datavaluetypeid, data_year
having count(*) > 1
union all
select
    'geographic_variation' as checked_source,
    member_sha256 || ':' || county_fips || ':' || data_year || ':' || field as grain
from {{ ref('int_gv_county_values') }}
group by member_sha256, county_fips, data_year, field
having count(*) > 1
union all
select
    'wonder' as checked_source,
    member_sha256 || ':' || county_fips || ':' || data_year as grain
from {{ ref('int_wonder_county_deaths') }}
group by member_sha256, county_fips, data_year
having count(*) > 1
