-- Non-PSI aligned validation periods must overlap their HAI window [597]; PSI-90 and PSI-13 must end strictly before it starts.
-- The control and measure set is checked by assert_validation_control_identity.
select
    alignment_key,
    'validation period violates its alignment rule' as failure
from {{ ref('int_spine_validation_measures') }}
where
    alignment_status = 'aligned'
    and (
        period_start is null
        or period_end is null
        or (
            measure in ('PSI_90', 'PSI_13')
            and period_end >= window_start
        )
        or (
            measure not in ('PSI_90', 'PSI_13')
            and (period_end < window_start or period_start > make_date(window_year, 12, 31))
        )
    )
