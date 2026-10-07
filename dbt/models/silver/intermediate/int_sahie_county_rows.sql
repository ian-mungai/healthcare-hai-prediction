-- One row per county, SAHIE year and published group (age, race, sex and income codes as published): counts, percents
-- and margins of error typed, '.' null with its field listed in missing_fields. Only county rows (geocat 50). Groups are
-- never combined; is_all_groups marks code 0 of every category, which each file's preamble must define as under 65, all
-- races, both sexes and all incomes (failure modes 438 to 442).
{{ config(materialized='table') }}

with

periods as (
    select
        member_sha256,
        vintage
    from {{ ref('geography_file_periods') }}
    where bronze_table = 'sahie'
),

sahie_rows as (
    select
        stg.*,
        periods.vintage
    from {{ ref('stg_sahie') }} as stg
    left join periods on stg._member_sha256 = periods.member_sha256
    where trim(stg.geocat) = '50'
)

select
    _member_sha256 as member_sha256,
    _row_number as source_row_number,
    vintage::integer as file_year,
    try_cast(trim(year) as integer) as estimate_year,
    nullif(trim(version), '') as release_version,
    lpad(trim(statefips), 2, '0') || lpad(trim(countyfips), 3, '0') as county_fips,
    trim(agecat) as agecat,
    trim(racecat) as racecat,
    trim(sexcat) as sexcat,
    trim(iprcat) as iprcat,
    {%- for column in sahie_numeric_columns() %}
    {{ dot_number(column) }} as {{ column }},
    {%- endfor %}
    list_filter([
        {%- for column in sahie_numeric_columns() %}
        case when trim({{ column }}) = '.' then '{{ column }}' end{% if not loop.last %},{% endif %}
        {%- endfor %}
    ], field -> field is not null) as missing_fields,
    _member_sha256 || ':' || _row_number as sahie_row_key,
    coalesce(trim(agecat) = '0' and trim(racecat) = '0' and trim(sexcat) = '0' and trim(iprcat) = '0', false) as is_all_groups,
    trim(statefips) = '09' as is_connecticut
from sahie_rows
