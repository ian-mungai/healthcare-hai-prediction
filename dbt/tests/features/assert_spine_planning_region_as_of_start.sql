-- Fails for each spine row whose planning_region_fips comes from a HUD quarter that does not end strictly before the window
-- start, that is not the latest such quarter of the spine's ZIP, or whose label, period end or state does not match [631] [698].
select
    spine.ccn,
    spine.window_start,
    spine.planning_region_fips,
    spine.planning_region_source,
    spine.planning_region_source_period_end
from {{ ref('int_hospital_spine') }} as spine
where
    (
        spine.planning_region_fips is null
        and (spine.planning_region_source is not null or spine.planning_region_source_period_end is not null)
    )
    or (
        spine.planning_region_fips is not null
        and (
            spine.planning_region_source is distinct from 'hud_quarter'
            or spine.planning_region_source_period_end is null
            or spine.planning_region_source_period_end >= spine.window_start
            or spine.state_code is distinct from 'CT'
            or not exists (
                select 1
                from {{ ref('bridge_zip_county') }} as quarters
                where
                    quarters.zip_code = spine.zip_code
                    and quarters.usps_state = 'CT'
                    and quarters.county_fips = spine.planning_region_fips
                    and quarters.quarter_end_date = spine.planning_region_source_period_end
            )
            or exists (
                select 1
                from {{ ref('bridge_zip_county') }} as quarters
                where
                    quarters.zip_code = spine.zip_code
                    and quarters.usps_state = 'CT'
                    and quarters.county_fips like '091%'
                    and quarters.quarter_end_date > spine.planning_region_source_period_end
                    and quarters.quarter_end_date < spine.window_start
            )
        )
    )
