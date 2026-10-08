-- Fails for each outcome value that is neither a plain number nor a published mark (Not Available, --, N/A) [551].
select
    outcome_key,
    'sir' as field
from {{ ref('int_spine_hai_outcomes') }}
where
    nullif(trim(sir_text), '') is not null
    and trim(sir_text) not in {{ hai_outcome_tokens() }}
    and sir is null
union all
select
    outcome_key,
    'ci_lower' as field
from {{ ref('int_spine_hai_outcomes') }}
where
    nullif(trim(ci_lower_text), '') is not null
    and trim(ci_lower_text) not in {{ hai_outcome_tokens() }}
    and ci_lower is null
union all
select
    outcome_key,
    'ci_upper' as field
from {{ ref('int_spine_hai_outcomes') }}
where
    nullif(trim(ci_upper_text), '') is not null
    and trim(ci_upper_text) not in {{ hai_outcome_tokens() }}
    and ci_upper is null
union all
select
    outcome_key,
    'observed' as field
from {{ ref('int_spine_hai_outcomes') }}
where
    nullif(trim(observed_text), '') is not null
    and trim(observed_text) not in {{ hai_outcome_tokens() }}
    and observed is null
union all
select
    outcome_key,
    'predicted' as field
from {{ ref('int_spine_hai_outcomes') }}
where
    nullif(trim(predicted_text), '') is not null
    and trim(predicted_text) not in {{ hai_outcome_tokens() }}
    and predicted is null
union all
select
    outcome_key,
    'exposure' as field
from {{ ref('int_spine_hai_outcomes') }}
where
    nullif(trim(exposure_text), '') is not null
    and trim(exposure_text) not in {{ hai_outcome_tokens() }}
    and exposure is null
