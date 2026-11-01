-- One row per hospital, organisation owner and role per continuous period of the monthly owner releases, plus the unknown member. A type 2 history:
-- valid_from is the period end of the first release that lists the owner, valid_to (exclusive) the period end of the release after the last one,
-- and is_current marks the open period. The periods come from int_hospital_owner_periods and are recomputed from every release, so a corrected
-- release can change or remove a period. ccn is the natural key of dim_hospital; the owner and the role stay as published codes
-- because dim_owner_organization holds the owned organisation's ID, not the owner's. The unknown member has a null valid_from, as dim_date's has a null date.
-- Names, addresses, flags and shares stay on the release rows.

with

periods as (
    select
        ccn,
        associate_id_owner,
        role_code,
        valid_from,
        valid_to,
        is_current,
        bridged_releases
    from {{ ref('int_hospital_owner_periods') }}
),

final as (
    select
        md5(concat_ws('|', ccn, associate_id_owner, role_code, cast(valid_from as varchar))) as hospital_owner_history_sk,
        ccn,
        associate_id_owner,
        role_code,
        valid_from,
        valid_to,
        is_current,
        bridged_releases
    from periods
    union all
    select
        md5('<unknown>') as hospital_owner_history_sk,
        '<unknown>' as ccn,
        '<unknown>' as associate_id_owner,
        '<unknown>' as role_code,
        cast(null as date) as valid_from,
        cast(null as date) as valid_to,
        false as is_current,
        cast(null as bigint) as bridged_releases
)

select * from final
