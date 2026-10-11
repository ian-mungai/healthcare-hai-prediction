-- One row per MMD file row, by the published condition label; the control in the file name stays as file_control (it is
-- null for C258.01's earlier captures, whose names carry no control). County codes are left-padded and state rows keep a
-- 2-character state code (469); county rows without a name (codes ending 990) are unknown counties (470); every zero keeps
-- a possible-suppression flag (471); the unit comes from the reviewed mmd_condition_units seed by the published label
-- (472), and a label without a unit is left null for assert_mmd_labels_have_unit to fail; the denominator band stays as
-- published (473). The control map is a gold bridge.
{{ config(materialized='table') }}

with

units as (
    select
        condition_label,
        value_unit
    from {{ ref('mmd_condition_units') }}
),

typed as (
    select
        stg._member_sha256 as member_sha256,
        stg._row_number as source_row_number,
        trim(stg.condition) as condition_label,
        nullif('C258.' || regexp_extract(stg._member_path, 'c258_([0-9]+)', 1), 'C258.') as file_control,
        try_cast(trim(stg.year) as integer) as data_year,
        case trim(stg.geography) when 'County' then 'county' when 'State/Territory' then 'state' end as geography_level,
        nullif(trim(stg.county), '') as county_name,
        nullif(trim(stg.urban), '') as urban,
        trim(stg.primary_denominator) as denominator_band,
        stg.analysis_value as value_published,
        stg.fips as fips_published,
        {{ strict_number('stg.analysis_value') }} as value_number
    from {{ ref('stg_cms_mmd_csv') }} as stg
),

coded as (
    select
        *,
        case when geography_level = 'county' then {{ county_fips('fips_published') }} end as county_fips
    from typed
)

select
    coded.member_sha256,
    coded.source_row_number,
    coded.file_control,
    coded.condition_label,
    coded.data_year,
    coded.geography_level,
    coded.county_fips,
    case
        when coded.geography_level = 'county' then left(coded.county_fips, 2)
        when regexp_full_match(trim(coded.fips_published), '[0-9]{1,2}') then lpad(trim(coded.fips_published), 2, '0')
    end as state_fips,
    coded.county_name,
    coded.urban,
    coded.denominator_band,
    coded.value_published,
    coded.value_number,
    units.value_unit,
    coalesce(coded.value_number = 0, false) as is_possible_suppression,
    coded.geography_level = 'county' and coded.county_name is null as is_unknown_county,
    {{ county_scope('coded.county_fips') }} as county_scope,
    coalesce(left(coded.county_fips, 2), lpad(trim(coded.fips_published), 2, '0')) = '09' as is_connecticut,
    coded.member_sha256 || ':' || coded.source_row_number as mmd_row_key
from coded
left join units on coded.condition_label = units.condition_label
