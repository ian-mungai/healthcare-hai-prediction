-- Fails for each aligned Care Compare row whose period ends on or after the HAI window start, and for each hospital-window
-- that does not have exactly one row per control [564] [566].
select
    alignment_key,
    'period not before the start' as failure
from {{ ref('int_spine_care_compare_measures') }}
where
    alignment_status = 'aligned'
    and period_end >= window_start
union all
select
    spine_key as alignment_key,
    'control count differs' as failure
from {{ ref('int_spine_care_compare_measures') }}
group by spine_key
having count(*) <> (select count(*) from {{ ref('registry_measure_sources') }})
