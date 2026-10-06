-- One row per change of ownership and stored change-of-ownership file (release), with the release label and catalog
-- period from the ownership_release_periods seed. Each release lists events back to 2016, so the same event appears in
-- several files under one event_key (buyer enrollment, seller enrollment, effective date, type); the alignment step chooses
-- the release. CCNs follow the enrollment rule, the published values kept (failure modes 365 and 369 to 372).
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
    where bronze_table = 'cms_change_of_ownership'
),

changes as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        nullif(trim(enrollment_id_buyer), '') as enrollment_id_buyer,
        upper(nullif(trim(enrollment_state_buyer), '')) as enrollment_state_buyer,
        nullif(trim(provider_type_code_buyer), '') as provider_type_code_buyer,
        nullif(trim(provider_type_text_buyer), '') as provider_type_text_buyer,
        nullif(trim(npi_buyer), '') as npi_buyer,
        {{ yes_no('multiple_npi_flag_buyer') }} as multiple_npi_flag_buyer,
        {{ published_ccn('ccn_buyer') }} as ccn_buyer,
        upper(nullif(trim(ccn_buyer), '')) as ccn_buyer_published,
        nullif(trim(associate_id_buyer), '') as associate_id_buyer,
        nullif(trim(organization_name_buyer), '') as organization_name_buyer,
        nullif(trim(doing_business_as_name_buyer), '') as doing_business_as_name_buyer,
        upper(nullif(trim(chow_type_code), '')) as chow_type_code,
        nullif(trim(chow_type_text), '') as chow_type_text,
        {{ month_day_year('effective_date') }} as effective_date,
        nullif(trim(enrollment_id_seller), '') as enrollment_id_seller,
        upper(nullif(trim(enrollment_state_seller), '')) as enrollment_state_seller,
        nullif(trim(provider_type_code_seller), '') as provider_type_code_seller,
        nullif(trim(provider_type_text_seller), '') as provider_type_text_seller,
        nullif(trim(npi_seller), '') as npi_seller,
        {{ yes_no('multiple_npi_flag_seller') }} as multiple_npi_flag_seller,
        {{ published_ccn('ccn_seller') }} as ccn_seller,
        upper(nullif(trim(ccn_seller), '')) as ccn_seller_published,
        nullif(trim(associate_id_seller), '') as associate_id_seller,
        nullif(trim(organization_name_seller), '') as organization_name_seller,
        nullif(trim(doing_business_as_name_seller), '') as doing_business_as_name_seller
    from {{ ref('stg_cms_change_of_ownership') }}
)

select
    changes.*,
    periods.release_id,
    periods.release_label,
    periods.period_start,
    periods.period_end,
    concat_ws(
        '|',
        coalesce(changes.enrollment_id_buyer, ''),
        coalesce(changes.enrollment_id_seller, ''),
        coalesce(changes.effective_date::varchar, ''),
        coalesce(changes.chow_type_code, '')
    ) as event_key,
    changes.member_sha256 || ':' || changes.source_row_number as chow_row_key
from changes
left join periods on changes.member_sha256 = periods.member_sha256
