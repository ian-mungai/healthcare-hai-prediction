-- One row per registry measure control of families S14 to S16, hospital and measurement window: the published value of
-- exactly the measure ID and column the registry names, its footnote, and a number only when the value is a plain number
-- (failure modes 323 to 327).

with

seed as (
    -- The registry controls of families S14 to S16 from the gold control bridge, by fact, measure key and typed value column; the
    -- Hospital General Information overall rating is aligned in int_spine_care_compare_measures instead.
    select
        bridge.measure_control,
        measure.fact,
        measure.measure_key,
        measure.value_column
    from {{ ref('feature_bridge_registry_measure_sources') }} as bridge
    inner join {{ ref('dim_measure') }} as measure on bridge.measure_sk = measure.measure_sk
    where measure.fact <> 'fct_hgi_release'
),

windows as (
    select
        entity_id,
        measure_id,
        window_start,
        window_end,
        score as value_text,
        footnote as footnote_text,
        sample as sample_text,
        release_date,
        member_sha256,
        'fct_timely_effective_care_window' as fact,
        'score_number' as value_column
    from {{ ref('fct_timely_effective_care_window') }}
    union all
    select
        entity_id,
        measure_id,
        window_start,
        window_end,
        score as value_text,
        footnote as footnote_text,
        sample as sample_text,
        release_date,
        member_sha256,
        'fct_maternal_health_window' as fact,
        'score_number' as value_column
    from {{ ref('fct_maternal_health_window') }}
    {%- for value_column in ['hcahps_answer_percent', 'number_of_completed_surveys', 'survey_response_rate_percent'] %}
    union all
    select
        entity_id,
        measure_id,
        window_start,
        window_end,
        {{ value_column }} as value_text,
        {{ value_column }}_footnote as footnote_text,
        null as sample_text,
        release_date,
        member_sha256,
        'fct_hcahps_window' as fact,
        '{{ value_column }}_number' as value_column
    from {{ ref('fct_hcahps_window') }}
    {%- endfor %}
)

select
    seed.measure_control,
    windows.entity_id,
    windows.measure_id,
    windows.window_start,
    windows.window_end,
    windows.value_text,
    windows.footnote_text,
    windows.sample_text,
    windows.release_date,
    windows.member_sha256,
    -- A category such as high, a token such as Not Available or a value with other characters stays text [327].
    case when regexp_full_match(trim(windows.value_text), '-?[0-9]+(\.[0-9]+)?') then trim(windows.value_text)::double end as value_number,
    seed.measure_control || ':' || windows.entity_id || ':' || windows.window_start || ':' || windows.window_end as registry_window_key
from seed
inner join windows
    on
        seed.fact = windows.fact
        and seed.measure_key = windows.measure_id
        and seed.value_column = windows.value_column
