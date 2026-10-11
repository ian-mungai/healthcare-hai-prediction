-- One row per registry measure control of families S14 to S16, hospital and measurement window: the published value of
-- exactly the measure ID and column the registry names, its footnote, and a number only when the value is a plain number
-- (failure modes 323 to 327).
{{ config(materialized='table') }}

with

seed as (
    select
        measure_control,
        source_model,
        source_measure_id,
        value_column
    from {{ ref('registry_measure_sources') }}
    where source_model <> 'int_hgi_hospital_releases'
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
        'int_cc_timely_effective_windows' as source_model,
        'score' as value_column
    from {{ ref('int_cc_timely_effective_windows') }}
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
        'int_cc_maternal_windows' as source_model,
        'score' as value_column
    from {{ ref('int_cc_maternal_windows') }}
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
        'int_cc_hcahps_windows' as source_model,
        '{{ value_column }}' as value_column
    from {{ ref('int_cc_hcahps_windows') }}
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
        seed.source_model = windows.source_model
        and seed.source_measure_id = windows.measure_id
        and seed.value_column = windows.value_column
