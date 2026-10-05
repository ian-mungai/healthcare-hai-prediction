-- Fails for each POS hospital column holding a value that is not blank and does not fit its kind: a code with a letter or
-- too many digits, a count with a fraction or a letter, a switch other than Y, N, true or false, or a date not in
-- YYYYMMDD [284] [287] [288] [292].
with

hospitals as (
    select *
    from {{ ref('stg_cms_provider_of_services') }}
    where {{ pos_typed_value('prvdr_ctgry_cd', 'code', 2) }} = '01'
),

counts as (
    select
        {%- for column, name, kind, width in pos_typed_columns() %}
        count(*) filter (where trim({{ column }}) <> '' and ({{ pos_typed_value(column, kind, width) }}) is null) as {{ column }},
        {%- endfor %}
        count(*) as hospital_rows
    from hospitals
),

unpivoted as (
    unpivot counts on columns(* exclude (hospital_rows)) into name bronze_column value uncast_values
)

select
    bronze_column,
    uncast_values
from unpivoted
where uncast_values > 0
