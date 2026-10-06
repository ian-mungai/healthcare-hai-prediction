-- One row per read impact file or sheet: its header row (the first of the first 5 rows that names a CCN column) and the
-- position of each field, found by exact header name from the reviewed seed; a name tied to a rule year maps only in that
-- year (failure modes 343 to 347 and 352).
{{ config(materialized='table') }}

with

file_rows as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        line_number,
        field_values,
        rule_fiscal_year
    from {{ ref('int_impact_file_rows') }}
),

files as (
    select distinct
        bronze_table,
        member_sha256,
        sheet_name
    from file_rows
),

first_rows as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        line_number,
        field_values,
        rule_fiscal_year
    from file_rows
    qualify row_number() over (partition by bronze_table, member_sha256, sheet_name order by line_number) <= 5
),

cells as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        line_number,
        rule_fiscal_year,
        field_values,
        unnest(range(1, len(field_values) + 1)) as field_position
    from first_rows
),

header_names as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        line_number,
        rule_fiscal_year,
        field_position,
        lower(regexp_replace(trim(replace(field_values[field_position], '"', '')), '\s+', ' ', 'g')) as header_name
    from cells
),

mapped as (
    select
        header_names.bronze_table,
        header_names.member_sha256,
        header_names.sheet_name,
        header_names.line_number,
        header_names.field_position,
        layout_columns.field
    from header_names
    inner join {{ ref('impact_layout_columns') }} as layout_columns
        on
            header_names.header_name = layout_columns.header_name
            and (
                layout_columns.rule_fiscal_year is null
                or header_names.rule_fiscal_year = layout_columns.rule_fiscal_year
            )
),

header_rows as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        min(line_number) as header_row
    from mapped
    where field = 'ccn'
    group by
        bronze_table,
        member_sha256,
        sheet_name
),

header_fields as (
    select
        mapped.bronze_table,
        mapped.member_sha256,
        mapped.sheet_name,
        mapped.field,
        mapped.field_position
    from mapped
    inner join header_rows
        on
            mapped.bronze_table = header_rows.bronze_table
            and mapped.member_sha256 = header_rows.member_sha256
            and mapped.sheet_name = header_rows.sheet_name
            and mapped.line_number = header_rows.header_row
),

summaries as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        count(*) filter (where field = 'ccn') as ccn_columns,
        max(field_position) filter (where field = 'ccn') as ccn_position,
        count(*) - count(distinct field) as repeated_fields,
        list({ 'field': field, 'field_position': field_position } order by field_position) as field_positions
    from header_fields
    group by
        bronze_table,
        member_sha256,
        sheet_name
)

select
    files.bronze_table,
    files.member_sha256,
    files.sheet_name,
    header_rows.header_row,
    summaries.field_positions,
    summaries.ccn_position,
    coalesce(summaries.ccn_columns, 0) as ccn_columns,
    coalesce(summaries.repeated_fields, 0) as repeated_fields,
    files.bronze_table || ':' || files.member_sha256 || ':' || files.sheet_name as layout_key
from files
left join header_rows
    on
        files.bronze_table = header_rows.bronze_table
        and files.member_sha256 = header_rows.member_sha256
        and files.sheet_name = header_rows.sheet_name
left join summaries
    on
        files.bronze_table = summaries.bronze_table
        and files.member_sha256 = summaries.member_sha256
        and files.sheet_name = summaries.sheet_name
