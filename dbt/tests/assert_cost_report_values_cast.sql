-- Fails for each cost-report column holding a value that is not blank and does not cast to a number [336].
with

reports as (
    select *
    from {{ ref('stg_cms_hospital_cost_reports') }}
),

counts as (
    select
        {%- for column in cost_report_amount_columns() %}
        count(*) filter (where trim({{ column }}) <> '' and ({{ cost_report_amount(column) }}) is null) as {{ column }},
        {%- endfor %}
        count(*) as report_rows
    from reports
),

unpivoted as (
    unpivot counts on columns(* exclude (report_rows)) into name bronze_column value uncast_values
)

select
    bronze_column,
    uncast_values
from unpivoted
where uncast_values > 0
