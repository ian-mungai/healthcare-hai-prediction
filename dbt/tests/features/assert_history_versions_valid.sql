-- SCD2 history (silver step 7.3, failure modes 671 and 673): per key, at most one current version, versions in order
-- with valid_from before valid_to, and no overlapping windows. Returns one row per violation.
with

versions as (
    select
        'pos' as history,
        ccn as entity_key,
        valid_from,
        valid_to,
        is_current
    from {{ ref('int_hospital_pos_history') }}
    union all
    select
        'hgi' as history,
        ccn as entity_key,
        valid_from,
        valid_to,
        is_current
    from {{ ref('int_hospital_hgi_history') }}
    union all
    select
        'ownership' as history,
        enrollment_id || ':' || associate_id_owner || ':' || role_code as entity_key,
        valid_from,
        valid_to,
        is_current
    from {{ ref('int_hospital_ownership_history') }}
),

ordered as (
    select
        *,
        lead(valid_from) over (partition by history, entity_key order by valid_from) as next_from
    from versions
)

select
    history,
    entity_key,
    valid_from,
    'version ends before it starts' as problem
from ordered
where valid_to is not null and valid_to <= valid_from
union all
select
    history,
    entity_key,
    valid_from,
    'overlaps the next version' as problem
from ordered
where next_from is not null and (valid_to is null or valid_to > next_from)
union all
select
    history,
    entity_key,
    min(valid_from) as valid_from,
    'more than one current version' as problem
from versions
where is_current
group by history, entity_key
having count(*) > 1
