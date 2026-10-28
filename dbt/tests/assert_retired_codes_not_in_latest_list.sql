with

latest_vintage as (
    select max(vintage) as vintage
    from {{ ref('int_county_adjacency_edges') }}
),

latest_codes as (
    select distinct edges.county_fips
    from {{ ref('int_county_adjacency_edges') }} as edges
    inner join latest_vintage
        on edges.vintage = latest_vintage.vintage
    union
    select distinct edges.neighbor_fips as county_fips
    from {{ ref('int_county_adjacency_edges') }} as edges
    inner join latest_vintage
        on edges.vintage = latest_vintage.vintage
    where edges.neighbor_fips is not null
)

select
    retirements.county_fips,
    retirements.change_page,
    retirements.change_event
from {{ ref('int_county_retirements') }} as retirements
inner join latest_codes
    on retirements.county_fips = latest_codes.county_fips
