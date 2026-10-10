-- One row per organisation owner row and stored owner file (release), with the release label and catalog period from the
-- ownership_release_periods seed. A flag is true for Y and false for N; a blank flag, and every flag of a file in the
-- layout before April 2025, is null; flags_published says whether the file has the private-equity column. Dates and
-- shares are typed (failure modes 365 to 371). Associate IDs that lost leading zeros are padded to 10 digits [714].
-- Choosing a release for a hospital-year waits for the alignment step.
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
    where bronze_table = 'cms_hospital_owners'
),

owners as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        nullif(trim(enrollment_id), '') as enrollment_id,
        {{ associate_id('associate_id') }} as associate_id,
        nullif(trim(organization_name), '') as organization_name,
        {{ associate_id('associate_id_owner') }} as associate_id_owner,
        {{ associate_id_padded('associate_id') }} or {{ associate_id_padded('associate_id_owner') }} as associate_ids_padded,
        nullif(trim(type_owner), '') as type_owner,
        nullif(trim(role_code_owner), '') as role_code,
        nullif(trim(role_text_owner), '') as role_text,
        {{ month_day_year('association_date_owner') }} as association_date,
        nullif(trim(organization_name_owner), '') as organization_name_owner,
        nullif(trim(doing_business_as_name_owner), '') as doing_business_as_name_owner,
        upper(nullif(trim(state_owner), '')) as state_owner,
        {{ strict_number('percentage_ownership') }} as percentage_ownership,
        bool_or(private_equity_company_owner is not null) over (partition by _member_sha256) as flags_published,
        {%- for column in owner_flag_columns() %}
        {{ yes_no(column) }} as {{ column }},
        {%- endfor %}
        nullif(trim(other_type_text_owner), '') as other_type_text_owner
    from {{ ref('stg_cms_hospital_owners') }}
)

select
    owners.*,
    periods.release_id,
    periods.release_label,
    periods.period_start,
    periods.period_end,
    owners.member_sha256 || ':' || owners.source_row_number as owner_row_key
from owners
left join periods on owners.member_sha256 = periods.member_sha256
