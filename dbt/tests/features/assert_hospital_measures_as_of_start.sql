-- Fails for each aligned hospital finance or operations row whose period ends on or after the HAI window start [576]. The control set is
-- checked by assert_hospital_control_identity.
select
    alignment_key,
    'period not before the start' as failure
from {{ ref('int_spine_hospital_measures') }}
where
    alignment_status = 'aligned'
    and period_end >= window_start
