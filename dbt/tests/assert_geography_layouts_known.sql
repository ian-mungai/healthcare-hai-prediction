-- Fails for a declared workbook sheet that is not loaded or has no single exact header row, and for a long RUCC file
-- without the code attribute of its reviewed vintage: a changed layout or a wrong vintage never yields codes [410] [412].
{%- set declared = var('geography_sheets') %}
with

declared as (
    {%- for entry in declared %}
    {%- if not loop.first %}
    union all
    {%- endif %}
    select
        '{{ entry.table }}' as bronze_table,
        '{{ entry.file_name }}' as file_name,
        '{{ entry.sheet }}' as sheet_name,
        {{ entry.header }} as header_cells
    {%- endfor %}
),

periods as (
    select
        bronze_table,
        member_sha256,
        file_name,
        vintage
    from {{ ref('geography_file_periods') }}
),

sheet_rows as (
    select
        'rucc_sheet_rows' as bronze_table,
        _member_sha256 as member_sha256,
        sheet_name,
        cells
    from {{ ref('stg_rucc_sheet_rows') }}
    union all
    select
        'ruca_sheet_rows' as bronze_table,
        _member_sha256 as member_sha256,
        sheet_name,
        cells
    from {{ ref('stg_ruca_sheet_rows') }}
),

header_counts as (
    select
        declared.bronze_table,
        declared.file_name,
        count(distinct periods.member_sha256) as loaded_files,
        count(sheet_rows.member_sha256) as header_rows
    from declared
    left join periods
        on
            declared.bronze_table = periods.bronze_table
            and declared.file_name = periods.file_name
    left join sheet_rows
        on
            periods.bronze_table = sheet_rows.bronze_table
            and periods.member_sha256 = sheet_rows.member_sha256
            and declared.sheet_name = sheet_rows.sheet_name
            and list_transform(sheet_rows.cells, cell -> trim(coalesce(cell, ''))) = declared.header_cells
    group by
        declared.bronze_table,
        declared.file_name
),

long_rucc as (
    select
        periods.member_sha256,
        periods.file_name,
        count(long_rows._member_sha256) filter (where trim(long_rows.attribute) = 'RUCC_' || periods.vintage) as code_rows
    from periods
    left join {{ ref('stg_rucc') }} as long_rows on periods.member_sha256 = long_rows._member_sha256
    where periods.bronze_table = 'rucc'
    group by
        periods.member_sha256,
        periods.file_name
)

select
    bronze_table,
    file_name,
    'declared sheet: ' || loaded_files || ' files, ' || header_rows || ' header rows' as problem
from header_counts
where loaded_files <> 1 or header_rows <> 1
union all
select
    'rucc' as bronze_table,
    file_name,
    'no code rows for its vintage' as problem
from long_rucc
where code_rows = 0
