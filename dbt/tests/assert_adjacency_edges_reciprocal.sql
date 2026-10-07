-- Fails for an adjacency edge between two different counties whose reverse edge is missing from the same file: Census
-- lists every neighbor pair both ways, so a one-way edge means a misread line or row [408].
with

edges as (
    select
        member_sha256,
        vintage,
        county_fips,
        neighbor_fips
    from {{ ref('int_county_adjacency_edges') }}
    where not is_self_link and not is_isolated
)

select
    edges.member_sha256,
    edges.vintage,
    edges.county_fips,
    edges.neighbor_fips
from edges
left join edges as reverse_edges
    on
        edges.member_sha256 = reverse_edges.member_sha256
        and edges.county_fips = reverse_edges.neighbor_fips
        and edges.neighbor_fips = reverse_edges.county_fips
where reverse_edges.member_sha256 is null
