-- One row per hospital enrollment and stored enrollment file (release), with the release label and catalog period from the
-- ownership_release_periods seed. The CCN is the published 6-character CCN or a 5-digit CCN padded to 6. A location
-- suffix, a lost leading zero or a unit letter maps to the parent CCN when POS lists exactly one such parent in the row's
-- state; any other value gives a null CCN. The published value is kept and ccn_source names the route. A flag is true
-- for Y and false for N; dates are typed (failure modes 365, 366, 369 to 371 and 619 to 622).
{{ config(materialized='table') }}

with

periods as (
    select
        member_sha256,
        release_id,
        release_label,
        period_start,
        period_end
    from {{ ref('ownership_release_periods') }}
    where bronze_table = 'cms_hospital_enrollments'
),

enrollments as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        nullif(trim(enrollment_id), '') as enrollment_id,
        upper(nullif(trim(enrollment_state), '')) as enrollment_state,
        nullif(trim(provider_type_code), '') as provider_type_code,
        nullif(trim(provider_type_text), '') as provider_type_text,
        nullif(trim(npi), '') as npi,
        {{ published_ccn('ccn') }} as ccn,
        upper(nullif(trim(ccn), '')) as ccn_published,
        nullif(trim(associate_id), '') as associate_id,
        nullif(trim(organization_name), '') as organization_name,
        nullif(trim(doing_business_as_name), '') as doing_business_as_name,
        {{ month_day_year('incorporation_date') }} as incorporation_date,
        upper(nullif(trim(incorporation_state), '')) as incorporation_state,
        nullif(trim(organization_type_structure), '') as organization_type_structure,
        nullif(trim(organization_other_type_text), '') as organization_other_type_text,
        upper(nullif(trim(proprietary_nonprofit), '')) as proprietary_nonprofit,
        nullif(trim(address_line_1), '') as address_line_1,
        nullif(trim(address_line_2), '') as address_line_2,
        nullif(trim(city), '') as city,
        upper(nullif(trim(state), '')) as state,
        nullif(trim(zip_code), '') as zip_code,
        nullif(trim(practice_location_type), '') as practice_location_type,
        nullif(trim(location_other_type_text), '') as location_other_type_text,
        {%- for column in enrollment_flag_columns() %}
        {{ yes_no(column) }} as {{ column }},
        {%- endfor %}
        nullif(trim(subgroup_other_text), '') as subgroup_other_text,
        {{ month_day_year('reh_conversion_date') }} as reh_conversion_date,
        nullif(trim(cah_or_hospital_ccn), '') as cah_or_hospital_ccn
    from {{ ref('stg_cms_hospital_enrollments') }}
),

pos_states as (
    select distinct
        ccn,
        state_code
    from {{ ref('int_pos_hospital_snapshots') }}
    where ccn is not null
),

candidates as (
    select
        member_sha256,
        source_row_number,
        state,
        unnest({{ ccn_parent_candidates('ccn_published') }}) as candidate
    from enrollments
    where ccn is null
),

candidate_ccns as (
    select
        member_sha256,
        source_row_number,
        state,
        struct_extract(candidate, 'ccn') as ccn,
        struct_extract(candidate, 'route') as route
    from candidates
),

parents as (
    -- Exactly one parent that POS lists in the row's own state [619].
    select
        candidate_ccns.member_sha256,
        candidate_ccns.source_row_number,
        min(candidate_ccns.ccn) as ccn,
        min(candidate_ccns.route) as route
    from candidate_ccns
    inner join pos_states
        on
            candidate_ccns.ccn = pos_states.ccn
            and candidate_ccns.state = pos_states.state_code
    group by
        candidate_ccns.member_sha256,
        candidate_ccns.source_row_number
    having count(distinct candidate_ccns.ccn) = 1
)

select
    enrollments.* exclude (ccn),
    periods.release_id,
    periods.release_label,
    periods.period_start,
    periods.period_end,
    coalesce(enrollments.ccn, parents.ccn) as ccn,
    case
        when enrollments.ccn is not null then 'published'
        else parents.route
    end as ccn_source,
    enrollments.member_sha256 || ':' || enrollments.enrollment_id as enrollment_key
from enrollments
left join periods on enrollments.member_sha256 = periods.member_sha256
left join parents
    on
        enrollments.member_sha256 = parents.member_sha256
        and enrollments.source_row_number = parents.source_row_number
