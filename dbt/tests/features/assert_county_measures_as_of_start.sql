-- Fails for each aligned county row whose data year ends on or after the HAI window start [604].
select
    alignment_key,
    period_end,
    window_start
from {{ ref('int_spine_county_measures') }}
where
    alignment_status = 'aligned'
    and period_end >= window_start
