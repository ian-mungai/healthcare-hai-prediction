-- One row per BLS LAUS county series value as captured: the measure named from its code, year and month (null for the
-- annual average, M13), seasonal adjustment from the series ID, '-' null with the token kept and the footnote codes kept.
-- Each value keeps its capture date, the revision vintage; no capture replaces another (failure modes 442 to 445).
{{ config(materialized='table') }}

with

periods as (
    select
        member_sha256,
        vintage
    from {{ ref('geography_file_periods') }}
    where bronze_table = 'bls_laus'
),

bls_rows as (
    select
        stg._member_sha256 as member_sha256,
        stg._row_number as source_row_number,
        periods.vintage as capture_date,
        stg.value as value_published,
        stg.footnotes,
        trim(stg.seriesid) as series_id,
        trim(stg.county_fips) as county_published,
        trim(stg.measure_code) as measure_code,
        try_cast(trim(stg.year) as integer) as data_year,
        trim(stg.period) as period_code
    from {{ ref('stg_bls_laus') }} as stg
    left join periods on stg._member_sha256 = periods.member_sha256
)

select
    member_sha256,
    source_row_number,
    capture_date,
    series_id,
    {{ county_fips('county_published') }} as county_fips,
    measure_code,
    case measure_code
        {%- for code, name in bls_measures() %}
        when '{{ code }}' then '{{ name }}'
        {%- endfor %}
    end as measure,
    data_year,
    period_code,
    case when regexp_full_match(period_code, 'M(0[1-9]|1[0-2])') then right(period_code, 2)::integer end as month_number,
    period_code = 'M13' as is_annual_average,
    substr(series_id, 3, 1) = 'S' as is_seasonally_adjusted,
    value_published,
    {{ dot_number('value_published') }} as value_number,
    case when trim(value_published) = '-' then '-' end as missing_token,
    list_filter(coalesce(json_extract_string(footnotes, '$[*].code'), []), code -> code is not null) as footnote_codes,
    member_sha256 || ':' || source_row_number as bls_row_key,
    left(county_published, 2) = '09' as is_connecticut
from bls_rows
