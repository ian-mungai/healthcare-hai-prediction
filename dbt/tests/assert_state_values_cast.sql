-- Fails for each state-source number column with a non-blank published value that neither types as a number nor is a reviewed missing token:
-- an HCAI utilization number, or an Illinois value, numerator or denominator.
with

util_counts as (
    select
        {%- for column in hcai_util_numeric_columns() %}
        {{ state_uncast_count(column) }} as {{ column }},
        {%- endfor %}
        0 as util_placeholder
    from (
        select * from {{ ref('stg_hcai_util_pages_csv') }}
        union all by name
        select * from {{ ref('stg_hcai_util_nonresponse_csv') }}
    ) as util
    where {{ hcai_util_hold_reason('util') }} is null
),

illinois_counts as (
    select
        {{ state_uncast_count('value') }} as value_column,
        {{ state_uncast_count('numerator') }} as numerator_column,
        {{ state_uncast_count('denominator') }} as denominator_column
    from {{ ref('stg_illinois_report_card_csv') }} as il
    where {{ illinois_hold_reason('il') }} is null
),

uncast as (
    select
        'hcai_util' as source_name,
        bronze_column,
        uncast_values
    from (unpivot util_counts on columns(* exclude (util_placeholder)) into name bronze_column value uncast_values)
    union all
    select
        'illinois' as source_name,
        bronze_column,
        uncast_values
    from (unpivot illinois_counts on columns(*) into name bronze_column value uncast_values)
)

select * from uncast
where uncast_values > 0
