-- Fails for each aligned operations row whose period ends on or after the HAI window start, and for each hospital-window
-- without exactly one row per control [587] [594] [595].
with

expected as (
    select count(*) as controls
    from (
        select measure_control from {{ ref('hhs_onc_measures') }}
        union all
        select measure_control from {{ ref('ownership_measures') }}
    )
)

select
    alignment_key,
    'period not before the start' as failure
from {{ ref('int_spine_operations_measures') }}
where
    alignment_status = 'aligned'
    and period_end >= window_start
union all
select
    spine_key as alignment_key,
    'control count differs' as failure
from {{ ref('int_spine_operations_measures') }}
group by spine_key
having count(*) <> (select expected.controls from expected)
