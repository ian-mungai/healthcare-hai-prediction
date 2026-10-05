-- Every row of the selected case-mix index files and sheets, split into fields: a tab-separated text line on tabs, any
-- other text line on runs of spaces, a sheet row by cell (failure modes 293, 295 and 299).
{{ config(materialized='table') }}

with

files as (
    select distinct
        bronze_table,
        member_sha256
    from ({{ cmi_snapshot_files() }}) as snapshot_files
    where
        is_selected
        and family in {{ quoted_list('cmi_families') }}
),

text_rows as (
    select
        text_lines._member_sha256 as member_sha256,
        text_lines._row_number as line_number,
        'cms_ipps_text_lines' as bronze_table,
        '' as sheet_name,
        case
            when contains(text_lines.line_text, chr(9)) then string_split(text_lines.line_text, chr(9))
            else regexp_split_to_array(trim(text_lines.line_text), '\s+')
        end as field_values
    from {{ ref('stg_cms_ipps_text_lines') }} as text_lines
    inner join files
        on
            text_lines._member_sha256 = files.member_sha256
            and files.bronze_table = 'cms_ipps_text_lines'
),

sheets as (
    -- Only the sheets staging selects: a workbook's twin-covered sheets are read from their text file [299].
    select
        member_sha256,
        sheet_name
    from {{ ref('stg_bronze__sheet_selection') }}
    where is_selected
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
)

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
