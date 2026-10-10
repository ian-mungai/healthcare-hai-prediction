-- Fails for each aligned AL4b row whose period ends on or after the HAI window start [604] [634].
select
    alignment_key,
    period_end,
    window_start
from {{ ref('int_spine_county_context') }}
where
    alignment_status = 'aligned'
    and period_end >= window_start
