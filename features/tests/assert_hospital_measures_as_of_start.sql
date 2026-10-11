-- Fails for each aligned hospital finance or operations row whose period ends on or after the HAI window start, and for
-- each hospital-window without exactly one row per source, control and field [576] [582].
with

expected as (
    select count(*) as controls
    from (
        select measure_control from {{ ref('cost_report_measures') }}
        union all
        select measure_control from {{ ref('impact_measures') }}
        union all
        select distinct measure_control || ':' || coalesce(field, 'none') from {{ ref('mup_measures') }}
        union all
        select measure_control from {{ ref('occmix_measures') }}
    )
)

select
    alignment_key,
    'period not before the start' as failure
from {{ ref('int_spine_hospital_measures') }}
where
    alignment_status = 'aligned'
    and period_end >= window_start
union all
select
    spine_key as alignment_key,
    'control count differs' as failure
from {{ ref('int_spine_hospital_measures') }}
group by spine_key
having count(*) <> (select expected.controls from expected)
