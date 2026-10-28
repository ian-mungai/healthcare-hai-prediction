-- Fails for each aligned operations row whose period ends on or after the HAI window start [587]. The control set is checked by
-- assert_operations_control_identity.
select
    alignment_key,
    'period not before the start' as failure
from {{ ref('int_spine_operations_measures') }}
where
    alignment_status = 'aligned'
    and period_end >= window_start
