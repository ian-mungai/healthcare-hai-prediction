-- Fails for each group D window value that is neither a number nor a published mark (Not Available, Not Applicable, N/A,
-- Too Few to Report) [510].
select
    'int_cc_unplanned_visits_windows' as checked_model,
    window_key,
    'score' as field
from {{ ref('int_cc_unplanned_visits_windows') }}
where
    nullif(trim(score), '') is not null
    and trim(score) not in {{ validation_tokens() }}
    and {{ strict_number('score') }} is null
union all
select
    'int_cc_unplanned_visits_windows' as checked_model,
    window_key,
    'denominator' as field
from {{ ref('int_cc_unplanned_visits_windows') }}
where
    nullif(trim(denominator), '') is not null
    and trim(denominator) not in {{ validation_tokens() }}
    and {{ strict_number('denominator') }} is null
union all
select
    'int_cc_unplanned_visits_windows' as checked_model,
    window_key,
    'lower_estimate' as field
from {{ ref('int_cc_unplanned_visits_windows') }}
where
    nullif(trim(lower_estimate), '') is not null
    and trim(lower_estimate) not in {{ validation_tokens() }}
    and {{ strict_number('lower_estimate') }} is null
union all
select
    'int_cc_unplanned_visits_windows' as checked_model,
    window_key,
    'higher_estimate' as field
from {{ ref('int_cc_unplanned_visits_windows') }}
where
    nullif(trim(higher_estimate), '') is not null
    and trim(higher_estimate) not in {{ validation_tokens() }}
    and {{ strict_number('higher_estimate') }} is null
union all
select
    'int_cc_complications_deaths_windows' as checked_model,
    window_key,
    'score' as field
from {{ ref('int_cc_complications_deaths_windows') }}
where
    nullif(trim(score), '') is not null
    and trim(score) not in {{ validation_tokens() }}
    and {{ strict_number('score') }} is null
union all
select
    'int_cc_complications_deaths_windows' as checked_model,
    window_key,
    'denominator' as field
from {{ ref('int_cc_complications_deaths_windows') }}
where
    nullif(trim(denominator), '') is not null
    and trim(denominator) not in {{ validation_tokens() }}
    and {{ strict_number('denominator') }} is null
union all
select
    'int_cc_complications_deaths_windows' as checked_model,
    window_key,
    'lower_estimate' as field
from {{ ref('int_cc_complications_deaths_windows') }}
where
    nullif(trim(lower_estimate), '') is not null
    and trim(lower_estimate) not in {{ validation_tokens() }}
    and {{ strict_number('lower_estimate') }} is null
union all
select
    'int_cc_complications_deaths_windows' as checked_model,
    window_key,
    'higher_estimate' as field
from {{ ref('int_cc_complications_deaths_windows') }}
where
    nullif(trim(higher_estimate), '') is not null
    and trim(higher_estimate) not in {{ validation_tokens() }}
    and {{ strict_number('higher_estimate') }} is null
union all
select
    'int_cc_hrrp_windows' as checked_model,
    window_key,
    'excess_readmission_ratio' as field
from {{ ref('int_cc_hrrp_windows') }}
where
    nullif(trim(excess_readmission_ratio), '') is not null
    and trim(excess_readmission_ratio) not in {{ validation_tokens() }}
    and {{ strict_number('excess_readmission_ratio') }} is null
union all
select
    'int_cc_hrrp_windows' as checked_model,
    window_key,
    'predicted_readmission_rate' as field
from {{ ref('int_cc_hrrp_windows') }}
where
    nullif(trim(predicted_readmission_rate), '') is not null
    and trim(predicted_readmission_rate) not in {{ validation_tokens() }}
    and {{ strict_number('predicted_readmission_rate') }} is null
union all
select
    'int_cc_hrrp_windows' as checked_model,
    window_key,
    'expected_readmission_rate' as field
from {{ ref('int_cc_hrrp_windows') }}
where
    nullif(trim(expected_readmission_rate), '') is not null
    and trim(expected_readmission_rate) not in {{ validation_tokens() }}
    and {{ strict_number('expected_readmission_rate') }} is null
union all
select
    'int_cc_hrrp_windows' as checked_model,
    window_key,
    'number_of_discharges' as field
from {{ ref('int_cc_hrrp_windows') }}
where
    nullif(trim(number_of_discharges), '') is not null
    and trim(number_of_discharges) not in {{ validation_tokens() }}
    and {{ strict_number('number_of_discharges') }} is null
union all
select
    'int_cc_hrrp_windows' as checked_model,
    window_key,
    'number_of_readmissions' as field
from {{ ref('int_cc_hrrp_windows') }}
where
    nullif(trim(number_of_readmissions), '') is not null
    and trim(number_of_readmissions) not in {{ validation_tokens() }}
    and {{ strict_number('number_of_readmissions') }} is null
