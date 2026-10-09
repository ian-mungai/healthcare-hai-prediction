-- One row per MUA or MUP component as published: designation type, status, IMU score and dates typed (YYYY-MM-DD), the
-- county code with XXXXX kept as a token; a tract component's number is its component name (failure modes 477 to 479). A
-- row identical in every published column to an earlier row of the same file is held as exact_repeat (475).
-- A reused ID keeps each designation apart by its type and date (476). Status and dates are as published at
-- the capture (480).
{{ config(materialized='table') }}

with

periods as (
    select
        member_sha256,
        vintage
    from {{ ref('geography_file_periods') }}
    where bronze_table = 'hrsa_mua_detail'
),

published as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        imu_score as imu_score_published,
        designation_date as designation_date_published,
        mua_p_update_date as update_date_published,
        medically_underserved_area_population_mua_p_withdrawal_date as withdrawal_date_published,
        state_and_county_federal_information_processing_standard_code as county_code_published,
        trim(mua_p_id) as mua_id,
        trim(designation_type_code) as designation_type_code,
        nullif(trim(designation_type), '') as designation_type,
        nullif(trim(mua_p_status_description), '') as mua_status,
        nullif(trim(population_type), '') as population_type,
        nullif(trim(medically_underserved_area_population_mua_p_component_geographic_type_description), '') as component_type,
        nullif(trim(medically_underserved_area_population_mua_p_component_geographic_name), '') as component_name,
        nullif(trim(county_subdivision_fips_code), '') as county_subdivision_code,
        nullif(trim(state_fips_code), '') as state_fips_published,
        nullif(trim(rural_status_description), '') as rural_status,
        md5(to_json(row(*columns(lambda c: not starts_with(c, '_') and c not in ('is_label_held', 'label_hold_issue'))))::varchar)
            as published_hash
    from {{ ref('stg_hrsa_mua_detail') }}
),

typed as (
    select
        *,
        {{ strict_number('imu_score_published') }} as imu_score,
        {{ hrsa_iso_date('designation_date_published') }} as designation_date,
        {{ hrsa_iso_date('update_date_published') }} as update_date,
        {{ hrsa_iso_date('withdrawal_date_published') }} as withdrawal_date,
        {{ county_fips('county_code_published') }} as county_fips,
        case when regexp_full_match(trim(county_code_published), 'X+') then trim(county_code_published) end as county_token,
        row_number() over (partition by member_sha256, published_hash order by source_row_number) as repeat_rank
    from published
)

select
    typed.member_sha256,
    typed.source_row_number,
    periods.vintage::date as capture_date,
    typed.mua_id,
    typed.designation_type_code,
    typed.designation_type,
    typed.mua_status,
    typed.population_type,
    typed.imu_score_published,
    typed.imu_score,
    typed.designation_date_published,
    typed.designation_date,
    typed.update_date_published,
    typed.update_date,
    typed.withdrawal_date_published,
    typed.withdrawal_date,
    typed.component_type,
    typed.component_name,
    typed.county_code_published,
    typed.county_fips,
    typed.county_token,
    typed.county_subdivision_code,
    typed.rural_status,
    case when typed.repeat_rank > 1 then 'exact_repeat' end as hold_reason,
    {{ county_scope('typed.county_fips') }} as county_scope,
    coalesce(left(typed.county_fips, 2), lpad(typed.state_fips_published, 2, '0')) = '09' as is_connecticut,
    typed.member_sha256 || ':' || typed.source_row_number as mua_row_key
from typed
left join periods on typed.member_sha256 = periods.member_sha256
