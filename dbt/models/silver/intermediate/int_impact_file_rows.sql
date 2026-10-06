-- Every row of the read IPPS impact files and sheets, split into fields: a tab-separated text line on tabs, any other text
-- line as comma-separated values with quoted fields, a sheet row by cell; description sheets are not read (failure modes
-- 342, 344 and 348).
{{ config(materialized='table') }}

with

files as (
    -- One label per selected file of a reviewed impact family [342]; held and twin-excluded files are not selected.
    select distinct
        labels.bronze_table,
        labels.member_sha256,
        labels.family,
        labels.rule_fiscal_year,
        labels.rule_stage
    from {{ ref('stg_bronze__file_labels') }} as labels
    inner join {{ ref('stg_bronze__file_selection') }} as selection
        on
            labels.bronze_table = selection.bronze_table
            and labels.member_sha256 = selection.member_sha256
    where
        labels.role = 'data'
        and selection.is_selected
        and labels.family in {{ quoted_list('impact_families') }}
),

text_rows as (
    select
        text_lines._member_sha256 as member_sha256,
        text_lines._row_number as line_number,
        'cms_ipps_text_lines' as bronze_table,
        '' as sheet_name,
        case
            when contains(text_lines.line_text, chr(9)) then string_split(text_lines.line_text, chr(9))
            -- A quoted field may hold a comma [348].
            else regexp_extract_all(text_lines.line_text, '(?:^|,)("(?:[^"]|"")*"|[^,]*)', 1)
        end as field_values
    from {{ ref('stg_cms_ipps_text_lines') }} as text_lines
    inner join files
        on
            text_lines._member_sha256 = files.member_sha256
            and files.bronze_table = 'cms_ipps_text_lines'
),

sheets as (
    -- Only the sheets staging selects, without the variable descriptions and layouts [344].
    select
        member_sha256,
        sheet_name
    from {{ ref('stg_bronze__sheet_selection') }}
    where
        is_selected
        {%- for pattern in var('impact_excluded_sheet_patterns') %}
        and lower(sheet_name) not like '{{ pattern }}'
        {%- endfor %}
),

sheet_rows as (
    select
        sheet_cells._member_sha256 as member_sha256,
        sheet_cells.sheet_row as line_number,
        sheet_cells.sheet_name,
        sheet_cells.cells as field_values,
        'cms_ipps_sheet_rows' as bronze_table
    from {{ ref('stg_cms_ipps_sheet_rows') }} as sheet_cells
    inner join files
        on
            sheet_cells._member_sha256 = files.member_sha256
            and files.bronze_table = 'cms_ipps_sheet_rows'
    inner join sheets
        on
            sheet_cells._member_sha256 = sheets.member_sha256
            and sheet_cells.sheet_name = sheets.sheet_name
),

all_rows as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        line_number,
        field_values
    from text_rows
    union all
    select
        bronze_table,
        member_sha256,
        sheet_name,
        line_number,
        field_values
    from sheet_rows
)

select
    all_rows.bronze_table,
    all_rows.member_sha256,
    all_rows.sheet_name,
    all_rows.line_number,
    all_rows.field_values,
    files.family,
    files.rule_fiscal_year,
    files.rule_stage
from all_rows
inner join files
    on
        all_rows.bronze_table = files.bronze_table
        and all_rows.member_sha256 = files.member_sha256
