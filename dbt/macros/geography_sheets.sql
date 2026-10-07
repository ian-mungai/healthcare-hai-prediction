{% macro geography_sheet_rows(table) %}
{#- The data rows of the declared workbook sheets of one table: rows after the sheet's exact header row, with the file's
    vintage from the period seed. A sheet without exactly one header row gives no rows and fails
    assert_geography_layouts_known [412]. -#}
with

declared as (
    {%- for entry in var('geography_sheets') if entry.table == table %}
    {%- if not loop.first %}
    union all
    {%- endif %}
    select
        '{{ entry.file_name }}' as file_name,
        '{{ entry.sheet }}' as sheet_name,
        {{ entry.header }} as header_cells
    {%- endfor %}
),

files as (
    select
        periods.member_sha256,
        periods.vintage,
        declared.sheet_name,
        declared.header_cells
    from {{ ref('geography_file_periods') }} as periods
    inner join declared on periods.file_name = declared.file_name
    where periods.bronze_table = '{{ table }}'
),

sheet_rows as (
    select
        rows._member_sha256 as member_sha256,
        rows._row_number as source_row_number,
        rows.sheet_name,
        rows.sheet_row,
        rows.cells,
        files.vintage,
        list_transform(rows.cells, cell -> trim(coalesce(cell, ''))) = files.header_cells as is_header_row
    from {{ ref('stg_' ~ table) }} as rows
    inner join files
        on
            rows._member_sha256 = files.member_sha256
            and rows.sheet_name = files.sheet_name
),

headers as (
    select
        member_sha256,
        sheet_name,
        min(sheet_row) as header_row
    from sheet_rows
    where is_header_row
    group by
        member_sha256,
        sheet_name
    having count(*) = 1
)

select
    sheet_rows.member_sha256,
    sheet_rows.source_row_number,
    sheet_rows.sheet_name,
    sheet_rows.cells,
    sheet_rows.vintage
from sheet_rows
inner join headers
    on
        sheet_rows.member_sha256 = headers.member_sha256
        and sheet_rows.sheet_name = headers.sheet_name
where sheet_rows.sheet_row > headers.header_row
{% endmacro %}
