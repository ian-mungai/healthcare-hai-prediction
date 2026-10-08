-- One row per spine hospital-window (calendar year) and HAI infection type: the published SIR, its bounds and counts by
-- exact measure ID, tokens kept as text with a null number, the SIR footnote and its codes, and the spine's population
-- flags. A part from a release after the last 2015-baseline review is held and counted, never used. No value is derived
-- (alignment step AL1, failure modes 549 to 558, plans/alignment_20261008/failure_modes_al1.md).
{{ config(materialized='table') }}

with

spine as (
    select
        ccn,
        window_year,
        window_start,
        spine_key,
        is_primary_population,
        is_sensitivity_population,
        is_connecticut,
        is_maryland
    from {{ ref('int_hospital_spine') }}
),

types as (
    {%- for hai_type in hai_outcome_types() %}
    select '{{ hai_type }}' as hai_type
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
),

calendar_parts as (
    -- The spine's calendar-year windows only [549].
    select
        entity_id as ccn,
        year(window_start) as window_year,
        left(measure_id, 5) as hai_type,
        substr(measure_id, 7) as part,
        measure_name,
        score,
        footnote,
        release_date,
        release_date <= {{ hai_baseline_reviewed_through() }} as is_baseline_reviewed
    from {{ ref('int_hai_hospital_windows') }}
    where
        month(window_start) = 1
        and day(window_start) = 1
        and window_end = make_date(year(window_start), 12, 31)
),

pivoted as (
    select
        ccn,
        window_year,
        hai_type,
        {%- for part, column in hai_outcome_parts() %}
        max(score) filter (where part = '{{ part }}' and is_baseline_reviewed) as {{ column }}_text,
        {%- endfor %}
        max(measure_name) filter (where part = 'SIR' and is_baseline_reviewed) as measure_name,
        max(footnote) filter (where part = 'SIR' and is_baseline_reviewed) as sir_footnote,
        max(release_date) filter (where is_baseline_reviewed) as release_date,
        count(*) filter (where is_baseline_reviewed) as published_parts,
        count(*) filter (where not is_baseline_reviewed) as baseline_held_parts
    from calendar_parts
    group by
        ccn,
        window_year,
        hai_type
)

select
    spine.ccn,
    spine.window_year,
    spine.window_start,
    types.hai_type,
    pivoted.measure_name,
    {%- for part, column in hai_outcome_parts() %}
    pivoted.{{ column }}_text,
    {{ strict_number('pivoted.' ~ column ~ '_text') }} as {{ column }},
    {%- endfor %}
    pivoted.sir_footnote,
    {{ hai_footnote_codes('pivoted.sir_footnote') }} as sir_footnote_codes,
    pivoted.release_date,
    coalesce(pivoted.published_parts, 0) as published_parts,
    coalesce(pivoted.baseline_held_parts, 0) as baseline_held_parts,
    {{ strict_number('pivoted.sir_text') }} is not null as has_sir,
    spine.spine_key,
    spine.is_primary_population,
    spine.is_sensitivity_population,
    spine.is_connecticut,
    spine.is_maryland,
    spine.spine_key || ':' || types.hai_type as outcome_key
from spine
cross join types
left join pivoted
    on
        spine.ccn = pivoted.ccn
        and spine.window_year = pivoted.window_year
        and types.hai_type = pivoted.hai_type
