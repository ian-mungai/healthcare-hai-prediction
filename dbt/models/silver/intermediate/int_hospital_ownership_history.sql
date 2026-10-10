-- One row per hospital enrollment, organization owner and role, per version of the ownership terms, dated by release
-- period ends only (SCD2 for the ownership group of RQ2, silver step 7.3, failure modes 670 to 677). Organization owners
-- only (type O): no individual's name enters history [676]. A release that lists the owner twice with different terms
-- is held for that release [674]; a release without the owner closes its version [673], except that one missing release
-- is bridged and counted in bridged_releases (owner decision Oct 10 2026 [715]). Owner IDs come padded to 10 digits [714].
{{ config(materialized='table') }}

with

releases as (
    select
        period_end as release_date,
        row_number() over (order by period_end) as release_number
    from (
        select distinct period_end from {{ ref('int_hospital_owner_rows') }}
        where period_end is not null
    )
),

per_release as (
    select
        owners.enrollment_id,
        owners.associate_id_owner,
        owners.role_code,
        releases.release_date,
        releases.release_number,
        min(owners.member_sha256) as member_sha256,
        count(distinct row(
            owners.percentage_ownership, owners.organization_name_owner, owners.private_equity_company_owner,
            owners.reit_owner
        )) as distinct_sets,
        any_value(owners.percentage_ownership) as percentage_ownership,
        any_value(owners.organization_name_owner) as organization_name_owner,
        any_value(owners.private_equity_company_owner) as private_equity_company_owner,
        any_value(owners.reit_owner) as reit_owner
    from {{ ref('int_hospital_owner_rows') }} as owners
    inner join releases on owners.period_end = releases.release_date
    where
        owners.type_owner = 'O'
        and owners.enrollment_id is not null
        and owners.associate_id_owner is not null
        and owners.role_code is not null
    group by owners.enrollment_id, owners.associate_id_owner, owners.role_code, releases.release_date, releases.release_number
),

agreeing as (
    select * exclude (distinct_sets)
    from per_release
    where distinct_sets = 1
),

held as (
    select
        enrollment_id,
        associate_id_owner,
        role_code,
        release_number
    from per_release
    where distinct_sets > 1
),

{{ scd2_versions('agreeing', 'releases', ['enrollment_id', 'associate_id_owner', 'role_code'],
    ['percentage_ownership', 'organization_name_owner', 'private_equity_company_owner', 'reit_owner'],
    bridge=1, held='held') }}

select
    *,
    enrollment_id || ':' || associate_id_owner || ':' || role_code || ':' || valid_from as history_key
from dated_versions
