-- One row per group D registry control, component measure, hospital and window: the published value of exactly the measure
-- ID the registry names, and a number only when the value is a plain number; tokens stay text. A parent control appears
-- once per component. E038 and E039 read PSI_90 and PSI_13 (registry revision 3); PSI_90_SAFETY and PSI_13_POST_SEPSIS are
-- not aliased (failure modes 508 to 510). The gold window facts already carry the reviewed measure ID: a reviewed renamed ID
-- enters under the exact ID only where the hospital and window have no exact-ID row, and the published ID stays in
-- published_measure_id (failure modes 629 and 630, applied in silver by validation_aliased_windows).

with

seed as (
    -- The window controls of the gold control bridge, by fact, measure key and typed value column; the review decision comes
    -- from the registry control dimension.
    select
        bridge.measure_control,
        measure.fact,
        measure.measure_key,
        measure.value_column,
        control.review_decision
    from {{ ref('feature_bridge_validation_measures') }} as bridge
    inner join {{ ref('dim_measure') }} as measure on bridge.measure_sk = measure.measure_sk
    inner join {{ ref('dim_registry_control') }} as control on bridge.registry_control_sk = control.registry_control_sk
    where measure.fact in ('fct_unplanned_visits_window', 'fct_complications_deaths_window', 'fct_hrrp_window')
),

windows as (
    select
        entity_id,
        measure_id,
        published_measure_id,
        window_start,
        window_end,
        score as value_text,
        footnote as footnote_text,
        release_date,
        member_sha256,
        'fct_unplanned_visits_window' as fact,
        'score_number' as value_column
    from {{ ref('fct_unplanned_visits_window') }}
    union all
    select
        entity_id,
        measure_id,
        published_measure_id,
        window_start,
        window_end,
        score as value_text,
        footnote as footnote_text,
        release_date,
        member_sha256,
        'fct_complications_deaths_window' as fact,
        'score_number' as value_column
    from {{ ref('fct_complications_deaths_window') }}
    union all
    select
        entity_id,
        measure_id,
        published_measure_id,
        window_start,
        window_end,
        excess_readmission_ratio as value_text,
        footnote as footnote_text,
        release_date,
        member_sha256,
        'fct_hrrp_window' as fact,
        'excess_readmission_ratio_number' as value_column
    from {{ ref('fct_hrrp_window') }}
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
inner join windows
    on
        seed.fact = windows.fact
        and seed.measure_key = windows.measure_id
        and seed.value_column = windows.value_column
