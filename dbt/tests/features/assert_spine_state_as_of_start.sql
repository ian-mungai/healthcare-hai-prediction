-- Fails for each spine row whose state_code comes from a source that does not end strictly before the window start, whose
-- label or period end does not match the value, or whose source is not the latest eligible one: every predictor is as of
-- the window start (project standard, Existing HAI decisions item 1).
select
    spine.ccn,
    spine.window_start,
    spine.state_code,
    spine.state_source,
    spine.state_source_period_end
from {{ ref('int_hospital_spine') }} as spine
where
    -- No value means no source; a value needs a source whose period ends before the start.
    (spine.state_code is null and (spine.state_source is not null or spine.state_source_period_end is not null))
    or (
        spine.state_code is not null
        and (spine.state_source is null or spine.state_source_period_end is null or spine.state_source_period_end >= spine.window_start)
    )
    -- The own snapshot is the spine's snapshot.
    or (spine.state_source = 'pos_own' and spine.state_source_period_end is distinct from spine.pos_period_end)
    -- A snapshot source holds the value at that period end; an earlier snapshot is within 24 months and the latest before the start.
    or (
        spine.state_source in ('pos_own', 'pos_earlier')
        and not exists (
            select 1
            from {{ ref('fct_pos_snapshot') }} as snapshots
            where
                snapshots.ccn = spine.ccn
                and snapshots.period_end = spine.state_source_period_end
                and snapshots.state_code = spine.state_code
        )
    )
    or (
        spine.state_source = 'pos_earlier'
        and (
            spine.state_source_period_end < (spine.window_start - interval 24 month)::date
            or exists (
                select 1
                from {{ ref('fct_pos_snapshot') }} as snapshots
                where
                    snapshots.ccn = spine.ccn
                    and snapshots.period_end > spine.state_source_period_end
                    and snapshots.period_end < spine.window_start
            )
        )
    )
    -- An enrollment row of the CCN holds the value at that period end, and no later row ends before the start.
    or (
        spine.state_source = 'enrollment'
        and (
            not exists (
                select 1
                from {{ ref('fct_enrollment_release') }} as enrollment_rows
                where
                    enrollment_rows.ccn = spine.ccn
                    and enrollment_rows.period_end = spine.state_source_period_end
                    and enrollment_rows.state = spine.state_code
            )
            or exists (
                select 1
                from {{ ref('fct_enrollment_release') }} as enrollment_rows
                where
                    enrollment_rows.ccn = spine.ccn
                    and enrollment_rows.period_end > spine.state_source_period_end
                    and enrollment_rows.period_end < spine.window_start
            )
        )
    )
    -- No other label gives a state.
    or spine.state_source not in ('pos_own', 'pos_earlier', 'enrollment')
