-- Fails for each aligned Care Compare row whose period ends on or after the HAI window start [564]. The control set is checked by
-- assert_care_compare_control_identity.
select
    alignment_key,
    'period not before the start' as failure
from {{ ref('int_spine_care_compare_measures') }}
where
    alignment_status = 'aligned'
    and period_end >= window_start
