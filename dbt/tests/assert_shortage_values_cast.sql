-- Fails for each MMD value, HPSA score or date and MUA score or date that is published but does not cast [469] [478]
-- [479].
select
    'mmd' as checked_source,
    mmd_row_key as row_id,
    'value' as field
from {{ ref('int_mmd_prevalence') }}
where nullif(trim(value_published), '') is not null and value_number is null
union all
select
    'hpsa' as checked_source,
    hpsa_row_key as row_id,
    'score' as field
from {{ ref('int_hpsa_components') }}
where nullif(trim(hpsa_score_published), '') is not null and hpsa_score is null
union all
select
    'hpsa' as checked_source,
    hpsa_row_key as row_id,
    'designation_date' as field
from {{ ref('int_hpsa_components') }}
where nullif(trim(designation_date_published), '') is not null and designation_date is null
union all
select
    'hpsa' as checked_source,
    hpsa_row_key as row_id,
    'last_update_date' as field
from {{ ref('int_hpsa_components') }}
where nullif(trim(last_update_date_published), '') is not null and last_update_date is null
union all
select
    'hpsa' as checked_source,
    hpsa_row_key as row_id,
    'withdrawn_date' as field
from {{ ref('int_hpsa_components') }}
where nullif(trim(withdrawn_date_published), '') is not null and withdrawn_date is null
union all
select
    'mua' as checked_source,
    mua_row_key as row_id,
    'imu_score' as field
from {{ ref('int_mua_components') }}
where nullif(trim(imu_score_published), '') is not null and imu_score is null
union all
select
    'mua' as checked_source,
    mua_row_key as row_id,
    'designation_date' as field
from {{ ref('int_mua_components') }}
where nullif(trim(designation_date_published), '') is not null and designation_date is null
union all
select
    'mua' as checked_source,
    mua_row_key as row_id,
    'update_date' as field
from {{ ref('int_mua_components') }}
where nullif(trim(update_date_published), '') is not null and update_date is null
union all
select
    'mua' as checked_source,
    mua_row_key as row_id,
    'withdrawal_date' as field
from {{ ref('int_mua_components') }}
where nullif(trim(withdrawal_date_published), '') is not null and withdrawal_date is null
