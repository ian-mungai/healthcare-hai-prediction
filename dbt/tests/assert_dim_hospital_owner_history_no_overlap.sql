-- Fails for two periods of one hospital, owner and role whose windows overlap, and for a period that ends on or before it starts.
with

ordered as (
    select
        ccn,
        associate_id_owner,
        role_code,
        valid_from,
        valid_to,
        lag(valid_to) over by_key as previous_valid_to,
        lag(is_current) over by_key as previous_is_current
    from {{ ref('dim_hospital_owner_history') }}
    where ccn <> '<unknown>'
    window by_key as (partition by ccn, associate_id_owner, role_code order by valid_from)
)

select
    ccn,
    associate_id_owner,
    role_code,
    valid_from
from ordered
where
    previous_is_current
    or valid_from < previous_valid_to
    or valid_to <= valid_from
