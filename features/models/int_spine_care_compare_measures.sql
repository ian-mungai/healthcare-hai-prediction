-- One row per spine hospital-window and Care Compare process or structure control (families S13 to S16): the latest
-- measure window that ends before the HAI window starts, or for the overall rating (C284) the latest Hospital General
-- Information release dated before the start, with its age in months and an alignment status. Values stay as published;
-- C141's spellings map to one category each. No limit on age and no eligibility choice (alignment step AL2, failure modes
-- 564 to 575, plans/alignment_20261008/failure_modes_al2.md).
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

controls as (
    select
        measure_control,
        family,
        source_model,
        source_measure_id,
        review_decision
    from {{ ref('registry_measure_sources') }}
),

windows as (
    select
        measure_control,
        entity_id as ccn,
        measure_id,
        window_start as period_start,
        window_end as period_end,
        value_text,
        value_number,
        footnote_text,
        sample_text,
        release_date,
        1 as release_file_count,
        false as is_release_conflict
    from {{ ref('int_registry_measure_windows') }}
),

ratings as (
    -- One value per CCN and release date; files that disagree on one date are a conflict, never chosen [572].
    select
        'C284' as measure_control,
        ccn,
        null as measure_id,
        null::date as period_start,
        release_date as period_end,
        max(overall_rating_text) as value_text,
        max(overall_rating)::double as value_number,
        max(overall_rating_footnote) as footnote_text,
        null as sample_text,
        release_date,
        max(release_file_count) as release_file_count,
        count(distinct coalesce(overall_rating_text, '') || '|' || coalesce(overall_rating_footnote, '')) > 1 as is_release_conflict
    from {{ ref('int_hgi_hospital_releases') }}
    where ccn is not null
    group by
        ccn,
        release_date
),

periods as (
    select * from windows
    union all
    select * from ratings
),

candidates as (
    -- Only periods that end before the HAI window starts [564]; the latest one wins [565].
    select
        periods.*,
        spine.spine_key,
        row_number() over (partition by spine.spine_key, periods.measure_control order by periods.period_end desc) as recency
    from spine
    inner join periods
        on
            spine.ccn = periods.ccn
            and spine.window_start > periods.period_end
),

chosen as (
    select * from candidates
    where recency = 1
),

any_period as (
    select distinct
        measure_control,
        ccn
    from periods
),

held_before as (
    -- Windows staging held for the hospital and measure that end before the start [567].
    select distinct
        spine.spine_key,
        controls.measure_control
    from {{ ref('int_cc_window_holds') }} as holds
    inner join controls on holds.measure_id = controls.source_measure_id
    inner join spine
        on
            holds.entity_id = spine.ccn
            and holds.window_end < spine.window_start
)

select
    spine.spine_key,
    spine.ccn,
    spine.window_year,
    spine.window_start,
    controls.measure_control,
    controls.family,
    controls.source_model,
    controls.review_decision,
    chosen.measure_id,
    case when not chosen.is_release_conflict then chosen.value_text end as value_text,
    case when not chosen.is_release_conflict then chosen.value_number end as value_number,
    -- Each published C141 spelling maps to one category; Not Available stays a token [569].
    case
        when controls.measure_control = 'C141' and not chosen.is_release_conflict
            then {{ edv_category('chosen.value_text') }}
    end as value_category,
    case when not chosen.is_release_conflict then chosen.footnote_text end as footnote_text,
    chosen.sample_text,
    chosen.period_start,
    chosen.period_end,
    chosen.release_date,
    chosen.release_file_count,
    -- Whole months from the period end to the window start; no limit [570].
    case when chosen.period_end is not null then datediff('month', chosen.period_end, spine.window_start) end as age_months,
    case
        when chosen.spine_key is not null and not chosen.is_release_conflict then 'aligned'
        when chosen.is_release_conflict then 'held_in_staging'
        when held_before.spine_key is not null then 'held_in_staging'
        when any_period.ccn is not null then 'no_period_before_start'
        else 'not_in_source'
    end as alignment_status,
    spine.is_primary_population,
    spine.is_sensitivity_population,
    spine.is_connecticut,
    spine.is_maryland,
    spine.spine_key || ':' || controls.measure_control as alignment_key
from spine
cross join controls
left join chosen
    on
        spine.spine_key = chosen.spine_key
        and controls.measure_control = chosen.measure_control
left join held_before
    on
        spine.spine_key = held_before.spine_key
        and controls.measure_control = held_before.measure_control
left join any_period
    on
        spine.ccn = any_period.ccn
        and controls.measure_control = any_period.measure_control
