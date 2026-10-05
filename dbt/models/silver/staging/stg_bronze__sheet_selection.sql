-- One row per workbook sheet in the IPPS and occupational-mix sheet tables: whether intermediate models may read it.
-- A twin-excluded workbook keeps every sheet that no selected text file of the same release covers row for row; every
-- sheet of a selected workbook is selected and no sheet of a held workbook (failure modes 267 to 278).
{{ config(materialized='table') }}

with

selection as (
    select
        file_key,
        bronze_table,
        member_sha256,
        is_label_held,
        is_twin_excluded,
        is_selected
    from {{ ref('stg_bronze__file_selection') }}
),

workbooks as (
    select
        file_key,
        bronze_table,
        member_sha256,
        is_label_held,
        is_twin_excluded,
        is_selected
    from selection
    where bronze_table in ('cms_ipps_sheet_rows', 'cms_occupational_mix_sheet_rows')
),

copies as (
    select distinct
        bronze_table,
        member_sha256,
        release_id
    from {{ ref('stg_bronze__file_copies') }}
),

-- Only a selected text file that shares a release with a twin-excluded, unheld workbook can cover its sheets [268] [270].
candidates as (
    select distinct
        workbooks.member_sha256 as workbook_sha256,
        texts.member_sha256 as text_sha256
    from workbooks
    inner join copies as workbook_copies
        on
            workbooks.bronze_table = workbook_copies.bronze_table
            and workbooks.member_sha256 = workbook_copies.member_sha256
    inner join copies as text_copies on workbook_copies.release_id = text_copies.release_id
    inner join selection as texts
        on
            text_copies.bronze_table = texts.bronze_table
            and text_copies.member_sha256 = texts.member_sha256
    where
        workbooks.is_twin_excluded
        and not workbooks.is_label_held
        and texts.is_selected
        and texts.bronze_table in ('cms_ipps_text_lines', 'cms_occupational_mix_text_lines', 'cms_occupational_mix_text_lines_utf16')
),

{{ twin_parsed_rows('select candidates.text_sha256 from candidates', 'select workbooks.member_sha256 from workbooks') }},

sheets as (
    select
        _member_sha256,
        sheet_name,
        count(*) as sheet_rows
    from sheet_rows
    group by
        _member_sha256,
        sheet_name
),

sheet_counts as (
    select
        _member_sha256,
        sheet_name,
        count(*) as data_rows
    from sheet_data
    group by
        _member_sha256,
        sheet_name
),

text_counts as (
    select
        _member_sha256,
        count(*) as data_rows
    from text_data
    group by _member_sha256
),

-- Only a text with as many data rows as the sheet, and more than none, can cover it [269] [277].
pairs as (
    select
        candidates.workbook_sha256,
        sheet_counts.sheet_name,
        candidates.text_sha256,
        sheet_counts.data_rows
    from candidates
    inner join sheet_counts on candidates.workbook_sha256 = sheet_counts._member_sha256
    inner join text_counts on candidates.text_sha256 = text_counts._member_sha256
    where
        sheet_counts.data_rows = text_counts.data_rows
        and sheet_counts.data_rows > 0
),

matches as (
    select
        pairs.workbook_sha256,
        pairs.sheet_name,
        pairs.text_sha256,
        pairs.data_rows,
        count(*) filter (where {{ fields_match('text_data.row_fields', 'sheet_data.cells') }}) as matched_rows
    from pairs
    inner join text_data on pairs.text_sha256 = text_data._member_sha256
    inner join sheet_data
        on
            pairs.workbook_sha256 = sheet_data._member_sha256
            and pairs.sheet_name = sheet_data.sheet_name
            and text_data.data_row = sheet_data.data_row
    group by
        pairs.workbook_sha256,
        pairs.sheet_name,
        pairs.text_sha256,
        pairs.data_rows
),

-- The smallest covering checksum, so a rebuild gives the same answer [276].
coverage as (
    select
        workbook_sha256,
        sheet_name,
        min(text_sha256) as covering_sha256
    from matches
    where matched_rows = data_rows
    group by
        workbook_sha256,
        sheet_name
),

-- The compared sheet of a twin pair that differs stays held, as the owner decided [271].
differing as (
    select distinct
        workbook_sha256,
        compared_sheet as sheet_name
    from {{ ref('stg_bronze__twin_comparison') }}
    where
        twin_status = 'differ'
        and compared_sheet is not null
),

final as (
    select
        workbooks.file_key,
        workbooks.bronze_table,
        workbooks.member_sha256,
        sheets.sheet_name,
        sheets.sheet_rows,
        workbooks.file_key || ':' || sheets.sheet_name as sheet_key,
        coalesce(sheet_counts.data_rows, 0) as data_rows,
        case
            when workbooks.is_label_held then 'label_held'
            when workbooks.is_selected then 'file_selected'
            when differing.sheet_name is not null then 'twin_differs'
            when coverage.covering_sha256 is not null then 'covered'
            else 'kept'
        end as sheet_status,
        case
            when not workbooks.is_label_held and not workbooks.is_selected and differing.sheet_name is null then coverage.covering_sha256
        end as covering_sha256
    from workbooks
    inner join sheets on workbooks.member_sha256 = sheets._member_sha256
    left join sheet_counts
        on
            sheets._member_sha256 = sheet_counts._member_sha256
            and sheets.sheet_name = sheet_counts.sheet_name
    left join differing
        on
            sheets._member_sha256 = differing.workbook_sha256
            and sheets.sheet_name = differing.sheet_name
    left join coverage
        on
            sheets._member_sha256 = coverage.workbook_sha256
            and sheets.sheet_name = coverage.sheet_name
)

select
    final.*,
    final.sheet_status in ('file_selected', 'kept') as is_selected
from final
