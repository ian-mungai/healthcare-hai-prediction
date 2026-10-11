-- One row per hospital Medicare and Medicaid EHR incentive attestation, program years 2011 to 2017: a series apart from the
-- 2023 and 2024 CHPL linkage, with no registry control (failure mode 383). Years and months are typed; the CCN follows the
-- B5a rule. payment_year is the program's payment-year number (1, 2, 3 and so on), the integer of any digits-only published
-- text in the INTEGER range, kept in payment_year_text (failure mode 795).
{{ config(materialized='table') }}

with

attestations as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        nullif(trim(npi), '') as npi,
        {{ published_ccn('ccn') }} as ccn,
        upper(nullif(trim(ccn), '')) as ccn_published,
        nullif(trim(provider_type), '') as provider_type,
        upper(nullif(trim(business_state_territory), '')) as business_state_territory,
        nullif(trim(zip), '') as zip,
        nullif(trim(hospital_type), '') as hospital_type,
        nullif(trim(program_type), '') as program_type,
        {{ year_number('program_year') }} as program_year,
        nullif(trim(provider_stage_number), '') as provider_stage_number,
        -- Any digits-only text, leading zeros included (00003 is 3); a value outside the INTEGER range is null by try_cast,
        -- never an error, and fails assert_onc_payment_year_cast [795].
        case when regexp_full_match(trim(payment_year), '[0-9]+') then try_cast(trim(payment_year) as integer) end as payment_year,
        {{ month_number('attestation_month') }} as attestation_month,
        {{ year_number('attestation_year') }} as attestation_year,
        nullif(trim(mu_definition_year), '') as mu_definition_year,
        nullif(trim(stage_2_scheduled_2014), '') as stage_2_scheduled_2014,
        nullif(trim(ehr_certification_number), '') as ehr_certification_number,
        nullif(trim(ehr_product_chp_id), '') as ehr_product_chp_id,
        nullif(trim(vendor_name), '') as vendor_name,
        nullif(trim(ehr_product_name), '') as ehr_product_name,
        nullif(trim(ehr_product_version), '') as ehr_product_version,
        nullif(trim(product_classification), '') as product_classification,
        nullif(trim(product_setting), '') as product_setting,
        nullif(trim(product_certification_edition_yr), '') as product_certification_edition_yr,
        nullif(trim(payment_year), '') as payment_year_text
    from {{ ref('stg_onc_pi_attestations_csv') }}
),

keyed as (
    select
        *,
        member_sha256 || ':' || source_row_number as attestation_key
    from attestations
)

select
    * exclude (payment_year_text),
    payment_year_text
from keyed
