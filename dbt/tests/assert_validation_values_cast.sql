-- Fails for each group D window or program-year value that is neither a number, a published mark (Not Available, Not
-- Applicable, N/A, Too Few to Report) nor a reviewed value [510] [519].
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
union all
select
    'int_hac_program_years' as checked_model,
    program_key as window_key,
    'total_hac_score' as field
from {{ ref('int_hac_program_years') }}
where
    nullif(trim(total_hac_score), '') is not null
    and trim(total_hac_score) not in {{ validation_tokens() }}
    and trim(total_hac_score) not in {{ validation_reviewed_values() }}
    and {{ strict_number('total_hac_score') }} is null
union all
select
    'int_hac_program_years' as checked_model,
    program_key as window_key,
    'psi_90_value' as field
from {{ ref('int_hac_program_years') }}
where
    nullif(trim(psi_90_value), '') is not null
    and trim(psi_90_value) not in {{ validation_tokens() }}
    and trim(psi_90_value) not in {{ validation_reviewed_values() }}
    and {{ strict_number('psi_90_value') }} is null
union all
select
    'int_hac_program_years' as checked_model,
    program_key as window_key,
    'psi_90_w_z_score' as field
from {{ ref('int_hac_program_years') }}
where
    nullif(trim(psi_90_w_z_score), '') is not null
    and trim(psi_90_w_z_score) not in {{ validation_tokens() }}
    and trim(psi_90_w_z_score) not in {{ validation_reviewed_values() }}
    and {{ strict_number('psi_90_w_z_score') }} is null
union all
select
    'int_hac_program_years' as checked_model,
    program_key as window_key,
    'clabsi_w_z_score' as field
from {{ ref('int_hac_program_years') }}
where
    nullif(trim(clabsi_w_z_score), '') is not null
    and trim(clabsi_w_z_score) not in {{ validation_tokens() }}
    and trim(clabsi_w_z_score) not in {{ validation_reviewed_values() }}
    and {{ strict_number('clabsi_w_z_score') }} is null
union all
select
    'int_hac_program_years' as checked_model,
    program_key as window_key,
    'cauti_w_z_score' as field
from {{ ref('int_hac_program_years') }}
where
    nullif(trim(cauti_w_z_score), '') is not null
    and trim(cauti_w_z_score) not in {{ validation_tokens() }}
    and trim(cauti_w_z_score) not in {{ validation_reviewed_values() }}
    and {{ strict_number('cauti_w_z_score') }} is null
union all
select
    'int_hac_program_years' as checked_model,
    program_key as window_key,
    'ssi_w_z_score' as field
from {{ ref('int_hac_program_years') }}
where
    nullif(trim(ssi_w_z_score), '') is not null
    and trim(ssi_w_z_score) not in {{ validation_tokens() }}
    and trim(ssi_w_z_score) not in {{ validation_reviewed_values() }}
    and {{ strict_number('ssi_w_z_score') }} is null
union all
select
    'int_hac_program_years' as checked_model,
    program_key as window_key,
    'cdi_w_z_score' as field
from {{ ref('int_hac_program_years') }}
where
    nullif(trim(cdi_w_z_score), '') is not null
    and trim(cdi_w_z_score) not in {{ validation_tokens() }}
    and trim(cdi_w_z_score) not in {{ validation_reviewed_values() }}
    and {{ strict_number('cdi_w_z_score') }} is null
union all
select
    'int_hac_program_years' as checked_model,
    program_key as window_key,
    'mrsa_w_z_score' as field
from {{ ref('int_hac_program_years') }}
where
    nullif(trim(mrsa_w_z_score), '') is not null
    and trim(mrsa_w_z_score) not in {{ validation_tokens() }}
    and trim(mrsa_w_z_score) not in {{ validation_reviewed_values() }}
    and {{ strict_number('mrsa_w_z_score') }} is null
union all
select
    'int_vbp_program_years' as checked_model,
    program_key as window_key,
    'total_performance_score' as field
from {{ ref('int_vbp_program_years') }}
where
    nullif(trim(total_performance_score), '') is not null
    and trim(total_performance_score) not in {{ validation_tokens() }}
    and trim(total_performance_score) not in {{ validation_reviewed_values() }}
    and {{ strict_number('total_performance_score') }} is null
union all
select
    'int_vbp_program_years' as checked_model,
    program_key as window_key,
    'unweighted_normalized_safety_domain_score' as field
from {{ ref('int_vbp_program_years') }}
where
    nullif(trim(unweighted_normalized_safety_domain_score), '') is not null
    and trim(unweighted_normalized_safety_domain_score) not in {{ validation_tokens() }}
    and trim(unweighted_normalized_safety_domain_score) not in {{ validation_reviewed_values() }}
    and {{ strict_number('unweighted_normalized_safety_domain_score') }} is null
union all
select
    'int_vbp_program_years' as checked_model,
    program_key as window_key,
    'weighted_safety_domain_score' as field
from {{ ref('int_vbp_program_years') }}
where
    nullif(trim(weighted_safety_domain_score), '') is not null
    and trim(weighted_safety_domain_score) not in {{ validation_tokens() }}
    and trim(weighted_safety_domain_score) not in {{ validation_reviewed_values() }}
    and {{ strict_number('weighted_safety_domain_score') }} is null
