-- One row per hospital CCN, organisation owner associate ID and role code, per continuous period of the monthly owner releases. A period starts
-- at the period end of the first release that lists the owner in the role and ends, exclusive, at the period end of the release after the last
-- one that lists it; valid_to is null and is_current true while the latest release still lists it. One missing release is bridged and counted in
-- bridged_releases (the same rule as int_hospital_ownership_history); two or more missing releases end the period and the next listing starts
-- a new one. A repeated listing inside one release counts once. The hospital is the CCN of the enrollment in the latest enrollment release
-- that has one; an enrollment with no CCN gets no period. History is recomputed from every owner release on each build, so a corrected release
-- can change or remove a period. Organisation owners only: no individual's name, contact or share enters this model.

with

releases as (
    select
        period_end as release_date,
        row_number() over (order by period_end) as release_number
    from (
        select distinct period_end
        from {{ ref('int_hospital_owner_rows') }}
        where period_end is not null
    )
),

hospitals as (
    select
        enrollment_id,
        arg_max(ccn, period_end) as ccn
    from {{ ref('int_hospital_enrollment_rows') }}
    where
        enrollment_id is not null
        and ccn is not null
        and length(ccn) = 6
    group by enrollment_id
),

listed as (
    select distinct
        hospitals.ccn,
        owners.associate_id_owner,
        owners.role_code,
        releases.release_number
    from {{ ref('int_hospital_owner_rows') }} as owners
    inner join releases on owners.period_end = releases.release_date
    inner join hospitals on owners.enrollment_id = hospitals.enrollment_id
    where
        owners.type_owner = 'O'
        and regexp_matches(owners.associate_id_owner, '^[0-9]{10}$')
        and owners.role_code is not null
),

gaps as (
    select
        *,
        release_number - lag(release_number) over by_key - 1 as gap_before
    from listed
    window by_key as (partition by ccn, associate_id_owner, role_code order by release_number)
),

numbered as (
    select
        *,
        -- sum() gives HUGEINT, which Iceberg cannot store, so the counter is cast to BIGINT.
        sum(case when gap_before is null or gap_before > 1 then 1 else 0 end) over (
            partition by ccn, associate_id_owner, role_code order by release_number
        )::bigint as period_number
    from gaps
),

periods as (
    select
        ccn,
        associate_id_owner,
        role_code,
        min(release_number) as first_release_number,
        max(release_number) as last_release_number,
        sum(case when gap_before = 1 then 1 else 0 end)::bigint as bridged_releases
    from numbered
    group by ccn, associate_id_owner, role_code, period_number
),

final as (
    select
        periods.ccn,
        periods.associate_id_owner,
        periods.role_code,
        first_release.release_date as valid_from,
        next_release.release_date as valid_to,
        periods.bridged_releases,
        next_release.release_date is null as is_current
    from periods
    inner join releases as first_release on periods.first_release_number = first_release.release_number
    left join releases as next_release on periods.last_release_number + 1 = next_release.release_number
)

select * from final
