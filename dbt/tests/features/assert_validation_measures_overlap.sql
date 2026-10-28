-- Fails for each aligned validation row whose period does not overlap its HAI window [597]. The control and measure set is checked by
-- assert_validation_control_identity.
select
    alignment_key,
    'period does not overlap the window' as failure
from {{ ref('int_spine_validation_measures') }}
where
    alignment_status = 'aligned'
    and (period_end < window_start or period_start > make_date(window_year, 12, 31))
