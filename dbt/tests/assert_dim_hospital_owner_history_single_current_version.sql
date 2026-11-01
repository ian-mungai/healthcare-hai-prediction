-- Fails for a hospital, owner and role with more than one current period, and for a period that is current but has an end date.
select
    ccn,
    associate_id_owner,
    role_code
from {{ ref('dim_hospital_owner_history') }}
where is_current
group by ccn, associate_id_owner, role_code
having count(*) > 1
union all
select
    ccn,
    associate_id_owner,
    role_code
from {{ ref('dim_hospital_owner_history') }}
where is_current and valid_to is not null
