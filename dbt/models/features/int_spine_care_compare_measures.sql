-- One row per spine hospital-window and Care Compare process or structure control (families S13 to S16): the latest
-- measure window that ends before the HAI window starts, or for the overall rating (C284) the latest Hospital General
-- Information release dated before the start, with its age in months and an alignment status. Values stay as published;
-- C141's spellings and C167's texts map to one category each. No limit on age and no eligibility choice (alignment step AL2, failure modes
-- 564 to 575, plans/alignment_20261008/failure_modes_al2.md).

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

control_keys as (
    -- One row per registry control and published measure ID of the Care Compare process and structure families, from the gold control bridge; the review
    -- decision comes from the registry control dimension. The family is left out until the owner defines it. A control that the bridge maps to two IDs
    -- (C154: IMM_3_OP_27_FAC_ADHPCT, then IMM_3) is one measure under two names, so it has two rows here and one in controls.
    select
        bridge.measure_control,
        measure.fact,
        measure.measure_key,
        control.review_decision
    from {{ ref('feature_bridge_registry_measure_sources') }} as bridge
    inner join {{ ref('dim_measure') }} as measure on bridge.measure_sk = measure.measure_sk
    inner join {{ ref('dim_registry_control') }} as control on bridge.registry_control_sk = control.registry_control_sk
),

controls as (
    select distinct
        measure_control,
        fact,
        review_decision
    from control_keys
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
    from {{ ref('fct_hgi_release') }}
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
    -- Only periods that end before the HAI window starts [564]; the latest one wins [565]. Windows with the same end rank by the later start, the later
    -- release, the lower measure ID and the published texts and numbers, so a rebuild picks the same row.
    select
        periods.*,
        spine.spine_key,
        row_number() over (
            partition by spine.spine_key, periods.measure_control
            order by
                periods.period_end desc,
                periods.period_start desc nulls last,
                periods.release_date desc nulls last,
                periods.measure_id asc,
                periods.value_text asc,
                periods.value_number asc,
                periods.footnote_text asc,
                periods.sample_text asc
        ) as recency
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
    from {{ ref('fct_cc_window_hold') }} as holds
    inner join control_keys as controls on holds.measure_id = controls.measure_key
    inner join spine
        on
            holds.entity_id = spine.ccn
            and holds.window_end < spine.window_start
),

base as (
    select
        spine.spine_key,
        spine.ccn,
        spine.window_year,
        spine.window_start,
        controls.measure_control,
        controls.fact,
        -- The field the matrix model pivots on beside the control; the family has one, so every value is field value.
        'value' as field,
        controls.review_decision,
        chosen.measure_id,
        case when not chosen.is_release_conflict then chosen.value_text end as value_text,
        case when not chosen.is_release_conflict then chosen.value_number end as value_number,
        -- Each published C141 spelling and each published C167 (SM_7) text maps to one category; Not Available stays a token [569].
        case
            when controls.measure_control = 'C141' and not chosen.is_release_conflict
                then {{ edv_category('chosen.value_text') }}
            when controls.measure_control = 'C167' and not chosen.is_release_conflict
                then {{ sm7_category('chosen.value_text') }}
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
),

derived_fields as (
    -- The log and ordinal fields of the reviewed lists (dbt/seeds/features/derived_fields.csv, written by the feature schema generator).
    select
        measure_control,
        parent_field,
        field,
        transform
    from {{ ref('derived_fields') }}
    where family = 'care_compare'
),

derived as (
    -- A log is the natural log of the score and null when the score is not above 0. An ordinal maps the emergency department volume category low, medium,
    -- high and very high to 1 to 4; any other value gives null. The row keeps the parent's period, release and alignment status.
    select
        base.spine_key,
        base.ccn,
        base.window_year,
        base.window_start,
        base.measure_control,
        base.fact,
        derived_fields.field,
        base.review_decision,
        base.measure_id,
        null::varchar as value_text,
        case derived_fields.transform
            when 'log' then case when base.value_number > 0 then ln(base.value_number) end
            else case base.value_category when 'low' then 1 when 'medium' then 2 when 'high' then 3 when 'very high' then 4 end
        end::double as value_number,
        null::varchar as value_category,
        base.footnote_text,
        base.sample_text,
        base.period_start,
        base.period_end,
        base.release_date,
        base.release_file_count,
        base.age_months,
        base.alignment_status,
        base.is_primary_population,
        base.is_sensitivity_population,
        base.is_connecticut,
        base.is_maryland,
        base.alignment_key || ':' || derived_fields.field as alignment_key
    from base
    inner join derived_fields
        on
            base.measure_control = derived_fields.measure_control
            and base.field = derived_fields.parent_field
)

select * from base
union all
select * from derived
