-- Fails for each hospital log or change row that is missing, has the wrong value or breaks the as-of rule (design 2.10 and 2.11). The expected rows are
-- worked out here from literal control lists and, for the cost report, from the report periods of int_cost_report_measures and fct_cost_report:
-- a log is the natural log of the value and null when the value is not above 0; a change is the value of the latest period that ends before the window start
-- minus that of the period before it, and null when either value is null, when there is no second period or when the two period ends are more than 18
-- months apart. A change row carries the latest period end in period_end and the earlier one in previous_period_end; both end before the start.

with

parents as (
    select
        alignment_key,
        spine_key,
        measure_source,
        measure_control,
        field,
        value_number
    from {{ ref('int_spine_hospital_measures') }}
    where
        field in ('value', 'rnhr')
        and (
            (measure_source = 'cost_report' and measure_control in ('C038', 'C057', 'C028', 'C040', 'C045', 'C054'))
            or (measure_source = 'occupational_mix' and measure_control = 'C030')
        )
),

derived as (
    select
        alignment_key,
        spine_key,
        measure_source,
        measure_control,
        field,
        value_number,
        period_end,
        previous_period_end,
        window_start
    from {{ ref('int_spine_hospital_measures') }}
    where field in ('value_log', 'value_change', 'rnhr_change')
),

wanted as (
    select
        spine_key,
        measure_source,
        measure_control,
        field || '_log' as derived_field,
        case when value_number > 0 then ln(value_number) end as expected_value
    from parents
    where
        field = 'value'
        and measure_control in ('C038', 'C057')
),

reports as (
    -- Periods of the cost report as the spine model groups them: one value per control and period end, null when the reports of one period disagree.
    select
        measures.ccn,
        measures.measure_control,
        reports.period_end,
        case
            when count(distinct coalesce(measures.value_code, measures.value_number::varchar)) > 1 then null
            else max(measures.value_number)
        end as value_number
    from {{ ref('int_cost_report_measures') }} as measures
    inner join {{ ref('fct_cost_report') }} as reports on measures.rpt_rec_num = reports.rpt_rec_num
    where
        measures.ccn is not null
        and measures.measure_control in ('C028', 'C040', 'C045', 'C054')
    group by
        measures.ccn,
        measures.measure_control,
        reports.period_end
),

ranked as (
    select
        spine.spine_key,
        reports.measure_control,
        reports.period_end,
        reports.value_number,
        row_number() over (partition by spine.spine_key, reports.measure_control order by reports.period_end desc) as recency
    from {{ ref('int_hospital_spine') }} as spine
    inner join reports
        on
            spine.ccn = reports.ccn
            and spine.window_start > reports.period_end
),

changes as (
    select
        latest.spine_key,
        'cost_report' as measure_source,
        latest.measure_control,
        'value_change' as derived_field,
        case
            when datediff('month', earlier.period_end, latest.period_end) <= 18 then latest.value_number - earlier.value_number
        end as expected_value
    from ranked as latest
    left join ranked as earlier
        on
            latest.spine_key = earlier.spine_key
            and latest.measure_control = earlier.measure_control
            and earlier.recency = 2
    where latest.recency = 1
),

expected as (
    select * from wanted
    union all
    select * from changes
)

select
    expected.spine_key || ':' || expected.measure_source || ':' || expected.measure_control || ':' || expected.derived_field as row_key,
    case
        when derived.alignment_key is null then 'derived row missing'
        else 'derived value differs from the arithmetic of the parent'
    end as failure
from expected
left join derived
    on
        expected.spine_key = derived.spine_key
        and expected.measure_source = derived.measure_source
        and expected.measure_control = derived.measure_control
        and expected.derived_field = derived.field
where
    derived.alignment_key is null
    or (derived.value_number is null) <> (expected.expected_value is null)
    or abs(derived.value_number - expected.expected_value) > 0.000000001
union all
-- The as-of rule and the gap rule of every change row, including the occupational-mix survey, which the arithmetic above does not recompute.
select
    alignment_key as row_key,
    case
        when period_end >= window_start then 'latest period not before the start'
        when previous_period_end >= window_start then 'previous period not before the start'
        when previous_period_end >= period_end then 'previous period not before the latest'
        when value_number is not null and previous_period_end is null then 'change without a previous period'
        else 'change across a gap of more than 18 months'
    end as failure
from derived
where
    field in ('value_change', 'rnhr_change')
    and (
        period_end >= window_start
        or previous_period_end >= window_start
        or previous_period_end >= period_end
        or (value_number is not null and previous_period_end is null)
        or (value_number is not null and datediff('month', previous_period_end, period_end) > 18)
    )
