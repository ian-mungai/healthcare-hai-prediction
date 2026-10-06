-- Fails for each Medicare inpatient file whose name carries no data year, so a row is never placed in a guessed year [355].
select distinct
    'provider' as file_kind,
    member_sha256,
    member_path
from {{ ref('int_mup_providers') }}
where data_year is null
union all
select distinct
    'drg' as file_kind,
    member_sha256,
    member_path
from {{ ref('int_mup_drg_discharges') }}
where data_year is null
