-- Fails for each spine row whose county_fips comes from a source that does not end strictly before the window start, whose
-- label or period end does not match the value, or whose source is not the latest eligible one: a snapshot's county, or a HUD
-- quarter of the spine's ZIP and state [616] [698].
select
    spine.ccn,
    spine.window_start,
    spine.county_fips,
    spine.county_source,
    spine.county_source_period_end
from {{ ref('int_hospital_spine') }} as spine
where
    (spine.county_fips is null and (spine.county_source is not null or spine.county_source_period_end is not null))
    or (
        spine.county_fips is not null
        and (spine.county_source is null or spine.county_source_period_end is null or spine.county_source_period_end >= spine.window_start)
    )
    or (spine.county_source = 'pos_own' and spine.county_source_period_end is distinct from spine.pos_period_end)
    or (
        spine.county_source in ('pos_own', 'pos_earlier')
        and not exists (
            select 1
            from {{ ref('fct_pos_snapshot') }} as snapshots
            where
                snapshots.ccn = spine.ccn
                and snapshots.period_end = spine.county_source_period_end
                and snapshots.county_fips = spine.county_fips
        )
    )
    or (
        spine.county_source = 'pos_earlier'
        and (
            spine.county_source_period_end < (spine.window_start - interval 24 month)::date
            or exists (
                select 1
                from {{ ref('fct_pos_snapshot') }} as snapshots
                where
                    snapshots.ccn = spine.ccn
                    and snapshots.period_end > spine.county_source_period_end
                    and snapshots.period_end < spine.window_start
            )
        )
    )
    or (
        spine.county_source = 'hud_quarter'
        and (
            not exists (
                select 1
                from {{ ref('bridge_zip_county') }} as quarters
                where
                    quarters.zip_code = spine.zip_code
                    and quarters.usps_state = spine.state_code
                    and quarters.county_fips = spine.county_fips
                    and quarters.quarter_end_date = spine.county_source_period_end
            )
            or exists (
                select 1
                from {{ ref('bridge_zip_county') }} as quarters
                where
                    quarters.zip_code = spine.zip_code
                    and quarters.usps_state = spine.state_code
                    and quarters.county_fips not like '091%'
                    and quarters.quarter_end_date > spine.county_source_period_end
                    and quarters.quarter_end_date < spine.window_start
            )
        )
    )
    or spine.county_source not in ('pos_own', 'pos_earlier', 'hud_quarter')
