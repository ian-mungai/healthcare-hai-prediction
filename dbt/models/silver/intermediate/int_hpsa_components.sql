-- One row per primary-care HPSA component as published: designation type, status, score and dates typed (MM/DD/YYYY), the
-- county code with XXXXX and XXX kept as a token (failure modes 477 to 479). A row identical in every published column to
-- an earlier row of the same file is held as exact_repeat (475). A reused ID keeps each
-- designation apart by its date (476). Status and dates are as published at the capture; nothing builds history here (480).
-- Where the county is a token, a tract, subdivision or single-county ID in the published state gives its county, and a
-- facility's postal code gives one through HUD; county_source names the route (626 to 628).
{{ config(materialized='table') }}

with

periods as (
    select
        member_sha256,
        vintage
    from {{ ref('geography_file_periods') }}
    where bronze_table = 'hrsa_hpsa_detail'
),

published as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        hpsa_score as hpsa_score_published,
        hpsa_designation_date as designation_date_published,
        hpsa_designation_last_update_date as last_update_date_published,
        withdrawn_date as withdrawn_date_published,
        state_and_county_federal_information_processing_standard_code as county_code_published,
        trim(hpsa_id) as hpsa_id,
        nullif(trim(hpsa_name), '') as hpsa_name,
        nullif(trim(designation_type), '') as designation_type,
        nullif(trim(hpsa_discipline_class), '') as discipline,
        nullif(trim(hpsa_status), '') as hpsa_status,
        nullif(trim(hpsa_component_type_description), '') as component_type,
        nullif(trim(hpsa_geography_identification_number), '') as geography_id,
        nullif(trim(state_fips_code), '') as state_fips_published,
        nullif(trim(rural_status), '') as rural_status,
        nullif(left(trim(hpsa_postal_code), 5), '') as postal_code,
        upper(nullif(trim(primary_state_abbreviation), '')) as state_abbreviation,
        md5(to_json(row(*columns(lambda c: not starts_with(c, '_') and c not in ('is_label_held', 'label_hold_issue'))))::varchar)
            as published_hash
    from {{ ref('stg_hrsa_hpsa_detail') }}
),

typed as (
    select
        *,
        case when regexp_full_match(trim(hpsa_score_published), '[0-9]+') then trim(hpsa_score_published)::integer end as hpsa_score,
        {{ hrsa_us_date('designation_date_published') }} as designation_date,
        {{ hrsa_us_date('last_update_date_published') }} as last_update_date,
        {{ hrsa_us_date('withdrawn_date_published') }} as withdrawn_date,
        {{ county_fips('county_code_published') }} as county_fips,
        case when regexp_full_match(trim(county_code_published), 'X+') then trim(county_code_published) end as county_token,
        row_number() over (partition by member_sha256, published_hash order by source_row_number) as repeat_rank
    from published
),

derived as (
    -- A geography ID names its county only for these types, in the published state [626].
    select
        typed.*,
        periods.vintage::date as capture_date,
        case
            when
                typed.county_fips is null
                and typed.county_token is not null
                and (
                    (typed.component_type = 'Census Tract' and regexp_full_match(typed.geography_id, '[0-9]{11}'))
                    or (typed.component_type = 'County Subdivision' and regexp_full_match(typed.geography_id, '[0-9]{10}'))
                    or (typed.component_type = 'Single County' and regexp_full_match(typed.geography_id, '[0-9]{5}'))
                )
                and left(typed.geography_id, 2) = lpad(typed.state_fips_published, 2, '0')
                then left(typed.geography_id, 5)
        end as id_county_fips
    from typed
    left join periods on typed.member_sha256 = periods.member_sha256
),

hud as (
    select
        zip_code,
        county_fips,
        usps_state,
        quarter_start_date,
        -- Old Connecticut counties and planning regions are counted apart, so a quarter that publishes both is not split [698].
        count(*) over (partition by zip_code, quarter_start_date, county_fips like '091%') as counties_in_zip
    from {{ ref('int_hud_zip_county_quarters') }}
),

zip_counties as (
    -- The quarter that starts on or before the capture, else the ZIP's earliest; the ZIP's state must be the row's. A ZIP
    -- split across counties gives none: a majority share is not an exact derivation [627] [698].
    select
        derived.member_sha256,
        derived.source_row_number,
        hud.county_fips,
        hud.counties_in_zip
    from derived
    inner join hud
        on
            derived.postal_code = hud.zip_code
            and derived.state_abbreviation = hud.usps_state
    where
        derived.county_fips is null
        and derived.id_county_fips is null
    qualify row_number() over (
        partition by derived.member_sha256, derived.source_row_number
        order by
            hud.quarter_start_date > derived.capture_date asc,
            abs(datediff('day', hud.quarter_start_date, derived.capture_date)) asc,
            hud.county_fips like '091%' asc,
            hud.county_fips asc
    ) = 1
)

select
    typed.member_sha256,
    typed.source_row_number,
    typed.capture_date,
    typed.hpsa_id,
    typed.hpsa_name,
    typed.designation_type,
    typed.discipline,
    typed.hpsa_status,
    typed.hpsa_score_published,
    typed.hpsa_score,
    typed.designation_date_published,
    typed.designation_date,
    typed.last_update_date_published,
    typed.last_update_date,
    typed.withdrawn_date_published,
    typed.withdrawn_date,
    typed.component_type,
    typed.geography_id,
    typed.county_code_published,
    coalesce(typed.county_fips, typed.id_county_fips, zip_counties.county_fips) as county_fips,
    case
        when typed.county_fips is not null then 'published'
        when typed.id_county_fips is not null then 'geography_id'
        when zip_counties.county_fips is not null then 'hud_zip_single_county'
    end as county_source,
    typed.county_token,
    typed.rural_status,
    case when typed.repeat_rank > 1 then 'exact_repeat' end as hold_reason,
    {{ county_scope('coalesce(typed.county_fips, typed.id_county_fips, zip_counties.county_fips)') }} as county_scope,
    coalesce(left(typed.county_fips, 2), lpad(typed.state_fips_published, 2, '0')) = '09' as is_connecticut,
    typed.member_sha256 || ':' || typed.source_row_number as hpsa_row_key
from derived as typed
left join zip_counties
    on
        typed.member_sha256 = zip_counties.member_sha256
        and typed.source_row_number = zip_counties.source_row_number
        and zip_counties.counties_in_zip = 1
