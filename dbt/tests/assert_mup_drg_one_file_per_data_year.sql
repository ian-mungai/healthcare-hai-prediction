-- Fails for each data year whose Medicare inpatient DRG cells come from more than one file, which the sepsis share would
-- sum [356].
select
    data_year,
    count(distinct member_sha256) as drg_files
from {{ ref('int_mup_drg_discharges') }}
group by data_year
having count(distinct member_sha256) > 1
