-- Fails for each spine row whose zip_code comes from a source that does not end strictly before the window start, whose
-- label or period end does not match the value, or whose source is not the latest eligible one [308] [615].
select
    spine.ccn,
    spine.window_start,
    spine.zip_code,
    spine.zip_source,
    spine.zip_source_period_end
from {{ ref('int_hospital_spine') }} as spine
where
    (spine.zip_code is null and (spine.zip_source is not null or spine.zip_source_period_end is not null))
    or (
        spine.zip_code is not null
        and (spine.zip_source is null or spine.zip_source_period_end is null or spine.zip_source_period_end >= spine.window_start)
    )
    or (spine.zip_source = 'pos_own' and spine.zip_source_period_end is distinct from spine.pos_period_end)
    or (
        spine.zip_source in ('pos_own', 'pos_earlier')
        and not exists (
            select 1
            from {{ ref('fct_pos_snapshot') }} as snapshots
            where
                snapshots.ccn = spine.ccn
                and snapshots.period_end = spine.zip_source_period_end
                and left(snapshots.zip_code, 5) = spine.zip_code
        )
    )
    or (
        spine.zip_source = 'pos_earlier'
        and (
            spine.zip_source_period_end < (spine.window_start - interval 24 month)::date
            or exists (
                select 1
                from {{ ref('fct_pos_snapshot') }} as snapshots
                where
                    snapshots.ccn = spine.ccn
                    and snapshots.period_end > spine.zip_source_period_end
                    and snapshots.period_end < spine.window_start
            )
        )
    )
    or (
        spine.zip_source = 'enrollment'
        and (
            not exists (
                select 1
                from {{ ref('fct_enrollment_release') }} as enrollment_rows
                where
                    enrollment_rows.ccn = spine.ccn
                    and enrollment_rows.period_end = spine.zip_source_period_end
                    and left(enrollment_rows.zip_code, 5) = spine.zip_code
            )
            or exists (
                select 1
                from {{ ref('fct_enrollment_release') }} as enrollment_rows
                where
                    enrollment_rows.ccn = spine.ccn
                    and enrollment_rows.period_end > spine.zip_source_period_end
                    and enrollment_rows.period_end < spine.window_start
            )
        )
    )
    or spine.zip_source not in ('pos_own', 'pos_earlier', 'enrollment')
