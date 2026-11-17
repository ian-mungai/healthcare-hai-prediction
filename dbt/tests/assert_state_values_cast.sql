-- Fails for each kept state-source row that still carries a number text that did not type (a number field in unsafe_value_fields), and for each kept row
-- whose typed number is null although a published number was staged (a silent loss). Every failed cast must be a held row, never a typed row with a
-- null.
with

util_stg as (
    {{ hcai_util_sources() }}
),

util_kept as (
    select util_stg.*
    from util_stg
    inner join {{ ref('int_hcai_util_facility_periods') }} as typed
        on
            util_stg.bronze_table = typed.bronze_table
            and util_stg._member_sha256 = typed.member_sha256
            and util_stg._row_number = typed.source_row_number
),

il_kept as (
    select stg.*
    from {{ ref('stg_illinois_report_card_csv') }} as stg
    inner join {{ ref('int_illinois_report_card_values') }} as typed
        on
            stg._member_sha256 = typed.member_sha256
            and stg._row_number = typed.source_row_number
),

util_counts as (
    select
        count(*) filter (where unsafe_value_fields is not null) as unsafe_rows,
        {%- for column in hcai_util_coordinate_columns() + hcai_util_numeric_columns() %}
        {{ state_uncast_count(column) }} as {{ column }},
        {%- endfor %}
        0 as util_placeholder
    from util_kept
),

illinois_counts as (
    select
        count(*) filter (where unsafe_value_fields is not null) as unsafe_rows,
        {{ state_uncast_count('value') }} as value_column,
        {{ state_uncast_count('numerator') }} as numerator_column,
        {{ state_uncast_count('denominator') }} as denominator_column
    from il_kept
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
