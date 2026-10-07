-- Fails for a county and year that two exports of one WONDER database publish with different values: only an identical
-- repeat may be held, never a revision [457].
select
    wonder_database,
    county_fips,
    data_year,
    count(distinct coalesce(deaths_published, '') || '|' || coalesce(population::varchar, '') || '|' || coalesce(crude_rate_published, '')) as versions
from {{ ref('int_wonder_county_deaths') }}
group by
    wonder_database,
    county_fips,
    data_year
having
    count(*) > 1
    and count(distinct coalesce(deaths_published, '') || '|' || coalesce(population::varchar, '') || '|' || coalesce(crude_rate_published, '')) > 1
