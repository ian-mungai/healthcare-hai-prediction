-- One row per text/workbook twin pair: the text layout, the workbook sheet that matches best, data rows on each side,
-- matched rows, and the pair's status and preferred file (failure modes 183 to 190). Data rows (rows with a number)
-- align by position; fields are compared one by one, a text number within half a unit of its displayed decimals.
{{ config(materialized='table') }}

with

twins as (
    select
        text_table,
        text_sha256,
        workbook_table,
        workbook_sha256,
        text_sha256 || ':' || workbook_sha256 as twin_key
    from {{ ref('ipps_occmix_twins') }}
),

{{ twin_parsed_rows('select twins.text_sha256 from twins', 'select twins.workbook_sha256 from twins') }},

text_counts as (
    select
        _member_sha256,
        count(*) as data_rows
    from text_data
    group by _member_sha256
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

aligned as (
    select
        twins.twin_key,
        sheet_data.sheet_name,
        {{ fields_match('text_data.row_fields', 'sheet_data.cells') }} as is_match
    from twins
    inner join text_data on twins.text_sha256 = text_data._member_sha256
    inner join sheet_data
        on
            twins.workbook_sha256 = sheet_data._member_sha256
            and text_data.data_row = sheet_data.data_row
),

sheet_matches as (
    select
        twins.twin_key,
        sheet_counts.sheet_name,
        sheet_counts.data_rows as workbook_data_rows,
        count(aligned.is_match) filter (where aligned.is_match) as matched_rows
    from twins
    inner join sheet_counts on twins.workbook_sha256 = sheet_counts._member_sha256
    left join aligned
        on
            twins.twin_key = aligned.twin_key
            and sheet_counts.sheet_name = aligned.sheet_name
    group by
        twins.twin_key,
        sheet_counts.sheet_name,
        sheet_counts.data_rows
),

best_sheets as (
    select
        twin_key,
        sheet_name,
        workbook_data_rows,
        matched_rows
    from (
        select
            *,
            row_number() over (partition by twin_key order by matched_rows desc, sheet_name asc) as sheet_rank
        from sheet_matches
    ) as ranked
    where sheet_rank = 1
),

final as (
    select
        twins.twin_key,
        twins.text_table,
        twins.text_sha256,
        twins.workbook_table,
        twins.workbook_sha256,
        best_sheets.sheet_name as compared_sheet,
        coalesce(text_layouts.text_layout, 'empty') as text_layout,
        coalesce(text_counts.data_rows, 0) as text_data_rows,
        coalesce(best_sheets.workbook_data_rows, 0) as workbook_data_rows,
        coalesce(best_sheets.matched_rows, 0) as matched_rows,
        case
            when coalesce(text_layouts.text_layout, 'empty') in ('fixed_width', 'empty') then 'not_compared'
            when
                coalesce(best_sheets.matched_rows, 0) > 0
                and best_sheets.matched_rows = text_counts.data_rows
                and best_sheets.matched_rows = best_sheets.workbook_data_rows then 'agree'
            else 'differ'
        end as twin_status
    from twins
    left join text_layouts on twins.text_sha256 = text_layouts._member_sha256
    left join text_counts on twins.text_sha256 = text_counts._member_sha256
    left join best_sheets on twins.twin_key = best_sheets.twin_key
)

select
    final.*,
    -- Owner decision: the text file where its layout parses, the workbook where only it parses; none when they differ.
    case
        when final.twin_status = 'agree' then final.text_sha256
        when final.twin_status = 'not_compared' then final.workbook_sha256
    end as preferred_sha256
from final
