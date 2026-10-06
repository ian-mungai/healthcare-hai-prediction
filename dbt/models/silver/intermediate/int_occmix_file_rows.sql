-- Selected occupational-mix source rows, including headers. File and sheet selection own twin and label holds.
{{ config(materialized='table') }}

with

labels as (
    select
        bronze_table,
        member_sha256,
        list_sort(list_distinct(list(family))) as families,
        bool_or(contains(lower(file_name), 'delet')) as is_deleted_file,
        list_sort(list_distinct(list(rule_fiscal_year))) as rule_fiscal_years,
        list_sort(list_distinct(list(rule_stage))) as rule_stages
    from {{ ref('stg_bronze__file_labels') }}
    where starts_with(bronze_table, 'cms_occupational_mix')
    group by bronze_table, member_sha256
),

source_rows as (
    {% for table in ['cms_occupational_mix_text_lines', 'cms_occupational_mix_text_lines_utf16'] %}
    select
        '{{ table }}' as bronze_table,
        source_lines._member_sha256 as member_sha256,
        '' as sheet_name,
        source_lines._row_number as source_row_number,
        string_split(source_lines.line_text, chr(9)) as field_values
    from {{ ref('stg_' ~ table) }} as source_lines
    inner join {{ ref('stg_bronze__file_selection') }} as selection
        on
            source_lines._member_sha256 = selection.member_sha256
            and selection.bronze_table = '{{ table }}'
    where selection.is_selected
    union all
    {% endfor %}
    select
        'cms_occupational_mix_sheet_rows' as bronze_table,
        cells._member_sha256 as member_sha256,
        cells.sheet_name,
        cells.sheet_row as source_row_number,
        cells.cells as field_values
    from {{ ref('stg_cms_occupational_mix_sheet_rows') }} as cells
    inner join {{ ref('stg_bronze__sheet_selection') }} as selection
        on
            cells._member_sha256 = selection.member_sha256
            and cells.sheet_name = selection.sheet_name
            and selection.bronze_table = 'cms_occupational_mix_sheet_rows'
    where selection.is_selected
)

select
    source_rows.*,
    labels.families,
    labels.rule_fiscal_years,
    labels.rule_stages,
    labels.is_deleted_file or contains(lower(source_rows.sheet_name), 'delet') as is_deleted,
    source_rows.bronze_table || ':' || source_rows.member_sha256 || ':' || source_rows.sheet_name || ':'
    || source_rows.source_row_number as source_row_key
from source_rows
inner join labels
    on
        source_rows.bronze_table = labels.bronze_table
        and source_rows.member_sha256 = labels.member_sha256
