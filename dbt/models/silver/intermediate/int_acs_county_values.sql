-- One row per county, ACS vintage and mapped concept: the column the reviewed map names for that vintage, never a code
-- assumed stable. Exports hold counties only; summary files hold every geography and only 0500000US rows are kept.
-- Missing tokens and sentinels are null with the token kept; a top- or bottom-coded median keeps its cap and a flag.
-- Values stay in each vintage's own units and dollars (failure modes 420 to 426, 431).
{{ config(materialized='table') }}
{%- for table in acs_tables() %}
-- depends_on: {{ ref('stg_' ~ table) }}
{%- endfor %}

with

periods as (
    select
        member_sha256,
        bronze_table,
        vintage
    from {{ ref('geography_file_periods') }}
),

variable_map as (
    select
        concept_id,
        bronze_table,
        vintage,
        column_name
    from {{ ref('acs_variable_map') }}
    where status = 'mapped'
),

{%- set unpivoted = [] %}
{%- for table in acs_tables() %}
{%- set columns = [] %}
{%- if execute %}
{%- set mapped = "bronze_table = '" ~ table ~ "' and status = 'mapped'" %}
{%- set columns = dbt_utils.get_column_values(ref('acs_variable_map'), 'column_name', where=mapped, default=[]) or [] %}
{%- endif %}
{%- if columns %}
{%- do unpivoted.append(table) %}

source_{{ table }} as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        '{{ table }}' as bronze_table,
        geo_id,
        {{ columns | sort | join(', ') }}
    from {{ ref('stg_' ~ table) }}
    where regexp_full_match(trim(geo_id), '0500000US[0-9]{5}')
),

cells_{{ table }} as (
    select *
    from source_{{ table }}
    unpivot include nulls (value_published for column_name in ({{ columns | sort | join(', ') }}))
),
{%- endif %}
{%- endfor %}

cells as (
    {%- for table in unpivoted %}
    select * from cells_{{ table }}
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- else %}
    select
        null::varchar as member_sha256,
        null::bigint as source_row_number,
        null::varchar as bronze_table,
        null::varchar as geo_id,
        null::varchar as column_name,
        null::varchar as value_published
    where false
    {%- endfor %}
),

typed as (
    select
        cells.member_sha256,
        cells.source_row_number,
        cells.bronze_table,
        periods.vintage,
        variable_map.concept_id,
        cells.column_name,
        right(trim(cells.geo_id), 5) as county_fips,
        cells.value_published,
        {{ acs_number('cells.value_published') }} as value_number,
        case when trim(cells.value_published) in {{ acs_missing_tokens() }} then trim(cells.value_published) end as missing_token,
        coalesce(regexp_full_match(trim(cells.value_published), '[0-9,]+[+]'), false) as is_top_coded,
        coalesce(regexp_full_match(trim(cells.value_published), '[0-9,]+-'), false) as is_bottom_coded
    from cells
    inner join periods on cells.member_sha256 = periods.member_sha256
    inner join variable_map
        on
            cells.bronze_table = variable_map.bronze_table
            and periods.vintage = variable_map.vintage
            and cells.column_name = variable_map.column_name
)

select
    member_sha256,
    source_row_number,
    bronze_table,
    vintage,
    concept_id,
    column_name,
    county_fips,
    value_published,
    value_number,
    missing_token,
    is_top_coded,
    is_bottom_coded,
    {{ county_scope('county_fips') }} as county_scope,
    member_sha256 || ':' || source_row_number || ':' || column_name as acs_value_key,
    left(county_fips, 2) = '09' as is_connecticut
from typed
