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
from {{ ref('bridge_county_neighbor') }}
