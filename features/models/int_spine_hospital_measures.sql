-- One row per spine hospital-window and hospital finance or operations control (cost reports, IPPS impact files,
-- Medicare inpatient summaries and the occupational-mix survey), by measure_source and field: the latest period that ends before
-- the HAI window starts, its age in months and an alignment status. Several values for one period are held, never chosen
-- (alignment step AL3a, failure modes 576 to 586, plans/alignment_20261008/failure_modes_al3a.md).
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
        'cost_report' as measure_source,
        measure_control,
        'value' as field,
        review_decision
    from {{ ref('cost_report_measures') }}
    union all
    select
        'ipps_impact' as measure_source,
        measure_control,
        field,
        review_decision
    from {{ ref('impact_measures') }}
    union all
    select
        'medicare_inpatient' as measure_source,
        measure_control,
        coalesce(field, 'none') as field,
        min(review_decision) as review_decision
    from {{ ref('mup_measures') }}
    group by
        measure_control,
        coalesce(field, 'none')
    union all
    select
        'occupational_mix' as measure_source,
        measure_control,
        coalesce(value_column, 'none') as field,
        review_decision
    from {{ ref('occmix_measures') }}
),

cost as (
    -- The latest report by period end; reports ending the same day with different values are a conflict, a blank is not [578].
    select
        'cost_report' as measure_source,
        measures.ccn,
        measures.measure_control,
        'value' as field,
        min(reports.period_begin) as period_start,
        reports.period_end,
        null::integer as rule_fiscal_year,
        null as rule_stage,
        max(measures.value_code) as value_code,
        max(measures.value_number) as value_number,
        count(distinct measures.rpt_rec_num) as period_copy_count,
        count(distinct coalesce(measures.value_code, measures.value_number::varchar)) > 1 as is_conflict
    from {{ ref('int_cost_report_measures') }} as measures
    inner join {{ ref('int_cost_reports') }} as reports on measures.rpt_rec_num = reports.rpt_rec_num
    where measures.ccn is not null
    group by
        measures.ccn,
        measures.measure_control,
        reports.period_end
),

impact_ranked as (
    -- Rule fiscal year Y runs Oct 1 Y-1 to Sep 30 Y; within a year the most final stage wins [579].
    select
        ccn,
        measure_control,
        field,
        rule_fiscal_year,
        rule_stage,
        value_code,
        value_number,
        member_sha256,
        dense_rank() over (
            partition by ccn, measure_control, field, rule_fiscal_year
            order by {{ ipps_stage_rank('rule_stage') }}
        ) as stage_rank
    from {{ ref('int_impact_measures') }}
    where ccn is not null
),

impact as (
    -- Files of the chosen stage that disagree are a conflict [580].
    select
        'ipps_impact' as measure_source,
        ccn,
        measure_control,
        field,
        make_date(rule_fiscal_year - 1, 10, 1) as period_start,
        make_date(rule_fiscal_year, 9, 30) as period_end,
        rule_fiscal_year,
        max(rule_stage) as rule_stage,
        max(value_code) as value_code,
        max(value_number) as value_number,
        count(distinct member_sha256) as period_copy_count,
        count(distinct coalesce(value_code, value_number::varchar)) > 1 as is_conflict
    from impact_ranked
    where stage_rank = 1
    group by
        ccn,
        measure_control,
        field,
        rule_fiscal_year
),

inpatient as (
    select
        'medicare_inpatient' as measure_source,
        ccn,
        measure_control,
        coalesce(field, 'none') as field,
        make_date(data_year, 1, 1) as period_start,
        make_date(data_year, 12, 31) as period_end,
        null::integer as rule_fiscal_year,
        null as rule_stage,
        null as value_code,
        max(value_number) as value_number,
        count(distinct member_sha256) as period_copy_count,
        count(distinct value_number) > 1 as is_conflict
    from {{ ref('int_mup_measures') }}
    where ccn is not null
    group by
        ccn,
        measure_control,
        coalesce(field, 'none'),
        data_year
),

occmix_ranked as (
    -- Copies of one survey from the latest rule year and most final stage, the workbook copy first [581].
    select
        measures.ccn,
        measures.measure_control,
        controls.field,
        measures.survey_start_date,
        measures.survey_end_date,
        measures.value_number,
        measures.member_sha256,
        list_max(measures.rule_fiscal_years) as rule_fiscal_year,
        dense_rank() over (
            partition by measures.ccn, measures.measure_control, measures.survey_end_date
            order by list_max(measures.rule_fiscal_years) desc, list_min(list_transform(measures.rule_stages, stage -> {{ ipps_stage_rank('stage') }}))
        ) as copy_rank,
        row_number() over (
            partition by measures.ccn, measures.measure_control, measures.survey_end_date
            order by
                list_max(measures.rule_fiscal_years) desc,
                list_min(list_transform(measures.rule_stages, stage -> {{ ipps_stage_rank('stage') }})),
                measures.sheet_name is null,
                measures.survey_row_key
        ) as copy_order
    from {{ ref('int_occmix_measures') }} as measures
    inner join controls
        on
            measures.measure_control = controls.measure_control
            and controls.measure_source = 'occupational_mix'
    where
        measures.hold_reason is null
        and measures.value_number is not null
),

occmix as (
    select
        'occupational_mix' as measure_source,
        ccn,
        measure_control,
        field,
        survey_start_date as period_start,
        survey_end_date as period_end,
        max(rule_fiscal_year) as rule_fiscal_year,
        null as rule_stage,
        null as value_code,
        max(value_number) filter (where copy_order = 1) as value_number,
        count(distinct member_sha256) as period_copy_count,
        max(value_number) - min(value_number) > 0.0001 as is_conflict
    from occmix_ranked
    where copy_rank = 1
    group by
        ccn,
        measure_control,
        field,
        survey_start_date,
        survey_end_date
),

periods as (
    select * from cost
    union all
    select * from impact
    union all
    select * from inpatient
    union all
    select * from occmix
),

candidates as (
    -- Only periods that end before the HAI window starts [576]; the latest one wins [577].
    select
        periods.*,
        spine.spine_key,
        row_number() over (
            partition by spine.spine_key, periods.measure_source, periods.measure_control, periods.field
            order by periods.period_end desc, periods.period_start desc
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
        measure_source,
        measure_control,
        field,
        ccn
    from periods
),

held_before as (
    -- Occupational-mix survey rows staging held that end before the start [583].
    select distinct
        held.measure_control,
        spine.spine_key,
        'occupational_mix' as measure_source
    from {{ ref('int_occmix_measures') }} as held
    inner join spine
        on
            held.ccn = spine.ccn
            and held.survey_end_date < spine.window_start
    where held.hold_reason is not null
)

select
    spine.spine_key,
    spine.ccn,
    spine.window_year,
    spine.window_start,
    controls.measure_source,
    controls.measure_control,
    controls.field,
    controls.review_decision,
    chosen.period_start,
    chosen.period_end,
    chosen.rule_fiscal_year,
    chosen.rule_stage,
    chosen.period_copy_count,
    spine.is_primary_population,
    spine.is_sensitivity_population,
    spine.is_connecticut,
    spine.is_maryland,
    case when not chosen.is_conflict then chosen.value_code end as value_code,
    case when not chosen.is_conflict then chosen.value_number end as value_number,
    -- Calendar months from the period end to the window start; no limit [585].
    case when chosen.period_end is not null then datediff('month', chosen.period_end, spine.window_start) end as age_months,
    case
        when chosen.spine_key is not null and not chosen.is_conflict then 'aligned'
        when chosen.is_conflict then 'held_in_staging'
        when held_before.spine_key is not null then 'held_in_staging'
        when any_period.ccn is not null then 'no_period_before_start'
        else 'not_in_source'
    end as alignment_status,
    spine.spine_key || ':' || controls.measure_source || ':' || controls.measure_control || ':' || controls.field as alignment_key
from spine
cross join controls
left join chosen
    on
        spine.spine_key = chosen.spine_key
        and controls.measure_source = chosen.measure_source
        and controls.measure_control = chosen.measure_control
        and controls.field = chosen.field
left join held_before
    on
        spine.spine_key = held_before.spine_key
        and controls.measure_source = held_before.measure_source
        and controls.measure_control = held_before.measure_control
left join any_period
    on
        spine.ccn = any_period.ccn
        and controls.measure_source = any_period.measure_source
        and controls.measure_control = any_period.measure_control
        and controls.field = any_period.field
