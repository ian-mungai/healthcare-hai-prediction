-- One row per group D registry control, component measure, hospital and window: the published value of exactly the measure
-- ID the registry names, and a number only when the value is a plain number; tokens stay text. A parent control appears
-- once per component. E038 and E039 read PSI_90 and PSI_13 (registry revision 3); PSI_90_SAFETY and PSI_13_POST_SEPSIS are
-- not aliased (failure modes 508 to 510). A reviewed renamed
-- ID enters under the exact ID only where the hospital and window have no exact-ID row (failure modes 629 and 630).
{{ config(materialized='table') }}

with

seed as (
    select
        measure_control,
        source_model,
        source_measure_id,
        review_decision
    from {{ ref('validation_measures') }}
    where nullif(source_measure_id, '') is not null
),

windows as (
    select
        entity_id,
        measure_id,
        window_start,
        window_end,
        score as value_text,
        footnote as footnote_text,
        release_date,
        member_sha256,
        'int_cc_unplanned_visits_windows' as source_model
    from {{ ref('int_cc_unplanned_visits_windows') }}
    union all
    select
        entity_id,
        measure_id,
        window_start,
        window_end,
        score as value_text,
        footnote as footnote_text,
        release_date,
        member_sha256,
        'int_cc_complications_deaths_windows' as source_model
    from {{ ref('int_cc_complications_deaths_windows') }}
    union all
    select
        entity_id,
        measure_id,
        window_start,
        window_end,
        excess_readmission_ratio as value_text,
        footnote as footnote_text,
        release_date,
        member_sha256,
        'int_cc_hrrp_windows' as source_model
    from {{ ref('int_cc_hrrp_windows') }}
),

aliased as (
    -- The exact ID wins; an alias fills only a hospital and window the exact ID leaves empty [629].
    select
        published.* exclude (measure_id),
        published.measure_id as published_measure_id,
        coalesce(aliases.measure_id, published.measure_id) as measure_id
    from windows as published
    left join {{ ref('measure_id_aliases') }} as aliases
        on
            published.source_model = aliases.source_model
            and published.measure_id = aliases.published_measure_id
    where
        aliases.measure_id is null
        or not exists (
            select 1
            from windows as exact
            where
                exact.source_model = published.source_model
                and exact.measure_id = aliases.measure_id
                and exact.entity_id = published.entity_id
                and exact.window_start = published.window_start
                and exact.window_end = published.window_end
        )
)

select
    seed.measure_control,
    seed.review_decision,
    windows.entity_id,
    windows.measure_id,
    windows.published_measure_id,
    windows.window_start,
    windows.window_end,
    windows.value_text,
    windows.footnote_text,
    windows.release_date,
    windows.member_sha256,
    {{ strict_number('windows.value_text') }} as value_number,
    seed.measure_control || ':' || windows.measure_id || ':' || windows.entity_id || ':' || windows.window_start || ':'
    || windows.window_end as validation_key
from seed
inner join aliased as windows
    on
        seed.source_model = windows.source_model
        and seed.source_measure_id = windows.measure_id
