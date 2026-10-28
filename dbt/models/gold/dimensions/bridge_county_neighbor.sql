-- One row per county, neighbor and vintage as Census publishes it (edge_key), with every silver column unchanged: the shared border is as
-- published, with no weights.
select
    member_sha256,
    source_row_number,
    vintage,
    county_fips,
    county_name,
    county_published,
    neighbor_fips,
    neighbor_name,
    neighbor_published,
    shared_border_length_m,
    length_published,
    county_scope,
    edge_key,
    is_self_link,
    is_isolated,
    is_connecticut
from {{ ref('int_county_adjacency_edges') }}
