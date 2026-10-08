-- Fails for each published SIR outside its published bounds, or more than 0.0015 from observed / predicted when both
-- counts are published (every real calendar-year SIR agrees, Oct 8 2026) [553].
select
    outcome_key,
    sir,
    ci_lower,
    ci_upper,
    observed,
    predicted
from {{ ref('int_spine_hai_outcomes') }}
where
    sir is not null
    and (
        ci_lower > sir
        or ci_upper < sir
        or (observed is not null and predicted > 0 and abs(observed / predicted - sir) > 0.0015)
    )
