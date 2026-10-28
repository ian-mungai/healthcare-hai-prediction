-- Fails for each spine row whose provider subtype or control type comes from a snapshot that does not end strictly before the
-- window start, whose label or period end does not match the value, or whose snapshot is not the own or the latest earlier one
-- [614]. A classification never depends on a later snapshot.
select
    spine.ccn,
    spine.window_start,
    spine.provider_subtype_code,
    spine.control_type_code,
    spine.classification_source,
    spine.classification_source_period_end
from {{ ref('int_hospital_spine') }} as spine
where
    (
        spine.classification_source is null
        and (
            spine.classification_source_period_end is not null
            or spine.provider_subtype_code is not null
            or spine.control_type_code is not null
        )
    )
    or (
        spine.classification_source is not null
        and (spine.classification_source_period_end is null or spine.classification_source_period_end >= spine.window_start)
    )
    or (spine.classification_source = 'pos_own' and spine.classification_source_period_end is distinct from spine.pos_period_end)
    or (
        spine.classification_source in ('pos_own', 'pos_earlier')
        and not exists (
            select 1
            from {{ ref('fct_pos_snapshot') }} as snapshots
            where
                snapshots.ccn = spine.ccn
                and snapshots.period_end = spine.classification_source_period_end
                and snapshots.provider_subtype_code is not distinct from spine.provider_subtype_code
                and snapshots.control_type_code is not distinct from spine.control_type_code
        )
    )
    or (
        spine.classification_source = 'pos_earlier'
        and (
            spine.classification_source_period_end < (spine.window_start - interval 24 month)::date
            or exists (
                select 1
                from {{ ref('fct_pos_snapshot') }} as snapshots
                where
                    snapshots.ccn = spine.ccn
                    and snapshots.period_end > spine.classification_source_period_end
                    and snapshots.period_end < spine.window_start
            )
        )
    )
    or spine.classification_source not in ('pos_own', 'pos_earlier')
