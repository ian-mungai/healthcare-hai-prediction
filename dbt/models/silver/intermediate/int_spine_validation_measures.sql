-- One row per spine hospital-window and validation control and measure (HAC and VBP program years, HRRP, unplanned visits
-- and 30-day mortality windows): the published period that matches the HAI window itself, because these outcomes are
-- compared with it, never used to predict it. Kept apart from every predictor table (alignment step AL5, failure modes
-- 597 to 603, plans/alignment_20261008/failure_modes_al5.md).
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
        is_maryland,
        make_date(window_year, 12, 31) as window_end
    from {{ ref('int_hospital_spine') }}
),

controls as (
    select
        measure_control,
        source_model,
        review_decision,
        coalesce(source_measure_id, 'none') as measure
    from {{ ref('validation_measures') }}
),

program_periods as (
    -- The HAI measure period HAC publishes for each fiscal year; VBP years use the same mapping [598].
    -- The dates are published as MM/DD/YYYY text; one period per fiscal year on real data.
    select
        fiscal_year,
        min(try_strptime(hai_measures_start_date, '%m/%d/%Y'))::date as period_start,
        max(try_strptime(hai_measures_end_date, '%m/%d/%Y'))::date as period_end
    from {{ ref('int_hac_program_years') }}
    where
        try_strptime(hai_measures_start_date, '%m/%d/%Y') is not null
        and try_strptime(hai_measures_end_date, '%m/%d/%Y') is not null
    group by fiscal_year
),

periods as (
    select
        windows.measure_control,
        windows.entity_id as ccn,
        windows.measure_id as measure,
        windows.window_start as period_start,
        windows.window_end as period_end,
        windows.value_text,
        windows.value_number,
        windows.footnote_text,
        windows.release_date,
        null::integer as fiscal_year
    from {{ ref('int_validation_measure_windows') }} as windows
    union all
    select
        program_values.measure_control,
        program_values.ccn,
        program_values.field as measure,
        program_periods.period_start,
        program_periods.period_end,
        program_values.value_text,
        program_values.value_number,
        null as footnote_text,
        program_values.release_date,
        program_values.fiscal_year
    from {{ ref('int_validation_program_values') }} as program_values
    inner join program_periods on program_values.fiscal_year = program_periods.fiscal_year
),

candidates as (
    -- The equal calendar-year window first, then the largest overlap, then the latest end [597].
    select
        periods.measure_control,
        periods.ccn,
        periods.measure,
        periods.period_start,
        periods.period_end,
        periods.value_text,
        periods.value_number,
        periods.footnote_text,
        periods.release_date,
        periods.fiscal_year,
        spine.spine_key,
        least(periods.period_end, spine.window_end) - greatest(periods.period_start, spine.window_start) + 1 as overlap_days,
        row_number() over (
            partition by spine.spine_key, periods.measure_control, periods.measure
            order by
                periods.period_start = spine.window_start and periods.period_end = spine.window_end desc,
                least(periods.period_end, spine.window_end) - greatest(periods.period_start, spine.window_start) desc,
                periods.period_end desc
        ) as match_rank
    from spine
    inner join periods
        on
            spine.ccn = periods.ccn
            and spine.window_end >= periods.period_start
            and spine.window_start <= periods.period_end
),

chosen as (
    select * from candidates
    where match_rank = 1
),

any_period as (
    select distinct
        measure_control,
        measure,
        ccn
    from periods
),

held as (
    -- Windows or program years staging held that overlap the HAI window [601].
    select distinct
        spine.spine_key,
        holds.measure_id as measure
    from {{ ref('int_validation_window_holds') }} as holds
    inner join spine
        on
            holds.entity_id = spine.ccn
            and holds.window_start <= spine.window_end
            and holds.window_end >= spine.window_start
    union
    select distinct
        spine.spine_key,
        controls.measure
    from {{ ref('int_validation_program_holds') }} as holds
    inner join program_periods on holds.fiscal_year = program_periods.fiscal_year
    inner join spine
        on
            holds.ccn = spine.ccn
            and program_periods.period_start <= spine.window_end
            and program_periods.period_end >= spine.window_start
    inner join controls on controls.source_model in ('int_hac_program_years', 'int_vbp_program_years')
)

select
    spine.spine_key,
    spine.ccn,
    spine.window_year,
    spine.window_start,
    controls.measure_control,
    controls.measure,
    controls.source_model,
    controls.review_decision,
    chosen.value_text,
    chosen.value_number,
    chosen.footnote_text,
    chosen.period_start,
    chosen.period_end,
    chosen.fiscal_year,
    chosen.release_date,
    chosen.overlap_days,
    spine.is_primary_population,
    spine.is_sensitivity_population,
    spine.is_connecticut,
    spine.is_maryland,
    case
        when chosen.spine_key is not null then 'aligned'
        when held.spine_key is not null then 'held_in_staging'
        when any_period.ccn is not null then 'no_matching_period'
        else 'not_in_source'
    end as alignment_status,
    spine.spine_key || ':' || controls.measure_control || ':' || controls.measure as alignment_key
from spine
cross join controls
left join chosen
    on
        spine.spine_key = chosen.spine_key
        and controls.measure_control = chosen.measure_control
        and controls.measure = chosen.measure
left join held
    on
        spine.spine_key = held.spine_key
        and controls.measure = held.measure
left join any_period
    on
        spine.ccn = any_period.ccn
        and controls.measure_control = any_period.measure_control
        and controls.measure = any_period.measure
