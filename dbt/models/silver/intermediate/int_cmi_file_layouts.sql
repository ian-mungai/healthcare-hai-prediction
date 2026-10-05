-- One row per case-mix index file or sheet: whether its first row is a header and the position of each field, found by
-- exact header name from the reviewed layout seed; a headerless file has the fixed order CCN, cases, CMI, relative
-- weights (failure modes 294, 295 and 297).
{{ config(materialized='table') }}

with

first_rows as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        min(line_number) as header_row,
        arg_min(field_values, line_number) as first_fields
    from {{ ref('int_cmi_file_rows') }}
    group by
        bronze_table,
        member_sha256,
        sheet_name
),

layouts as (
    -- A first row whose first field starts with a digit is data: the file has no header [295].
    select
        bronze_table,
        member_sha256,
        sheet_name,
        header_row,
        first_fields,
        not regexp_matches(coalesce(trim(replace(first_fields[1], '"', '')), ''), '^[0-9]') as has_header
    from first_rows
),

positions as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        first_fields,
        unnest(range(1, len(first_fields) + 1)) as field_position
    from layouts
    where has_header
),

header_names as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        field_position,
        lower(regexp_replace(trim(replace(first_fields[field_position], '"', '')), '\s+', ' ', 'g')) as header_name
    from positions
),

found as (
    select
        header_names.bronze_table,
        header_names.member_sha256,
        header_names.sheet_name,
        header_names.field_position,
        layout_columns.field
    from header_names
    inner join {{ ref('cmi_layout_columns') }} as layout_columns on header_names.header_name = layout_columns.header_name
),

counted as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        max(field_position) filter (where field = 'ccn') as ccn_position,
        max(field_position) filter (where field = 'cases') as cases_position,
        max(field_position) filter (where field = 'cmi') as cmi_position,
        max(field_position) filter (where field = 'relative_weights') as relative_weights_position,
        max(field_position) filter (where field = 'transfer_adjusted_cmi') as transfer_adjusted_cmi_position,
        max(field_position) filter (where field = 'transfer_adjusted_cases') as transfer_adjusted_cases_position,
        count(*) filter (where field = 'ccn') as ccn_columns,
        count(*) filter (where field = 'cmi') as cmi_columns,
        count(*) - count(distinct field) as repeated_fields
    from found
    group by
        bronze_table,
        member_sha256,
        sheet_name
)

select
    layouts.bronze_table,
    layouts.member_sha256,
    layouts.sheet_name,
    layouts.has_header,
    layouts.header_row,
    counted.transfer_adjusted_cmi_position,
    counted.transfer_adjusted_cases_position,
    layouts.bronze_table || ':' || layouts.member_sha256 || ':' || layouts.sheet_name as layout_key,
    case when layouts.has_header then counted.ccn_position else 1 end as ccn_position,
    case when layouts.has_header then counted.cases_position else 2 end as cases_position,
    case when layouts.has_header then counted.cmi_position else 3 end as cmi_position,
    case when layouts.has_header then counted.relative_weights_position else 4 end as relative_weights_position,
    coalesce(counted.ccn_columns, 0) as ccn_columns,
    coalesce(counted.cmi_columns, 0) as cmi_columns,
    coalesce(counted.repeated_fields, 0) as repeated_fields
from layouts
left join counted
    on
        layouts.bronze_table = counted.bronze_table
        and layouts.member_sha256 = counted.member_sha256
        and layouts.sheet_name = counted.sheet_name
