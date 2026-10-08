-- One row per group D registry control, component measure, hospital and window: the published value of exactly the measure
-- ID the registry names, and a number only when the value is a plain number; tokens stay text. A parent control appears
-- once per component. E038 and E039 name no published ID and get no rows (failure modes 508 to 510).
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
)

select
    seed.measure_control,
    seed.review_decision,
    windows.entity_id,
    windows.measure_id,
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
        seed.source_model = windows.source_model
        and seed.source_measure_id = windows.measure_id
