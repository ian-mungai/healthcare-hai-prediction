-- One row per county and SAIPE year: all-ages, age 0-17 and related age 5-17 poverty counts and percents with their 90%
-- bounds, and median household income with its bounds, cut at the documented positions. Only the all-geography files
-- are read; US and state rows (county code 0) are not county estimates. '.' is null and its field is listed in
-- missing_fields. Dollars are nominal for the reference year (failure modes 435 to 438, 442).
{{ config(materialized='table') }}

with

periods as (
    select
        member_sha256,
        file_name,
        vintage
    from {{ ref('geography_file_periods') }}
    where bronze_table = 'saipe_text_lines'
),

saipe_lines as (
    select
        stg._member_sha256 as member_sha256,
        stg._row_number as source_row_number,
        periods.vintage::integer as estimate_year,
        stg.line_text,
        substr(stg.line_text, 1, 2) as state_published,
        trim(substr(stg.line_text, 4, 3)) as county_published
    from {{ ref('stg_saipe_text_lines') }} as stg
    inner join periods on stg._member_sha256 = periods.member_sha256
    where regexp_full_match(periods.file_name, 'est[0-9]{2}all\.(txt|dat)')
),

typed as (
    select
        member_sha256,
        source_row_number,
        estimate_year,
        state_published || lpad(county_published, 3, '0') as county_fips,
        trim(substr(line_text, 194, 45)) as county_name,
        {%- for name, start, finish in saipe_fields() %}
        {{ dot_number('substr(line_text, ' ~ start ~ ', ' ~ (finish - start + 1) ~ ')') }} as {{ name }},
        {%- endfor %}
        list_filter([
            {%- for name, start, finish in saipe_fields() %}
            case when trim(substr(line_text, {{ start }}, {{ finish - start + 1 }})) = '.' then '{{ name }}' end{% if not loop.last %},{% endif %}
            {%- endfor %}
        ], field -> field is not null) as missing_fields
    from saipe_lines
    where regexp_full_match(state_published, '[0-9]{2}') and regexp_full_match(county_published, '[0-9]{1,3}') and county_published::integer > 0
)

select
    *,
    {{ county_scope('county_fips') }} as county_scope,
    member_sha256 || ':' || source_row_number as saipe_row_key,
    left(county_fips, 2) = '09' as is_connecticut
from typed
