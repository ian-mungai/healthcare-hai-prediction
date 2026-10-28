-- One row per ZIP code, county and USPS quarter as HUD publishes it (hud_row_key), with every silver column unchanged: the ratios are as
-- published.
select
    member_sha256,
    source_row_number,
    quarter_label,
    quarter_start_date,
    quarter_end_date,
    zip_code,
    county_fips,
    county_scope,
    usps_state,
    city,
    res_ratio,
    bus_ratio,
    oth_ratio,
    tot_ratio,
    zip_residential_ratio_sum,
    hud_row_key,
    is_connecticut,
    has_residential_addresses
from {{ ref('int_hud_zip_county_quarters') }}
