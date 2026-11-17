-- One row per California HCAI financial-disclosure value: facility (OSHPD number), report period end and publisher code (page.column.line, or the four-
-- component form), from each stored release. Staging passes a value only in a reviewed form (a number, a number in parentheses, the redaction mark, a
-- missing token, a date or one letter), so no free text that could name a person reaches silver; a row with any other value, or whose facility, period
-- or code is not in its published shape, is held in int_state_source_holds without its content. The code keeps its full published form; page, column and
-- line are parsed only for the three-component layout and the page for both layouts. is_latest_release marks the row of the newest release of each key
-- (publication date, then download run). The meaning and unit of a code come from the HCAI data dictionary in bronze. The stored copy is the privacy-
-- filtered derivative.
{{ config(materialized='table') }}

with

files as (
    select
        member_sha256,
        latest_publication_date,
        list_max(download_run_ids) as latest_download_run
    from {{ ref('stg_bronze__files') }}
    where bronze_table = 'hcai_finance_csv'
),

typed as (
    select
        stg._member_sha256 as member_sha256,
        stg._row_number as source_row_number,
        stg.oshpd_id as facility_id,
        {{ state_date('stg.rpe') }} as report_period_end,
        stg.pagecolumnline as page_column_line,
        stg.value_form,
        stg.value_text as value_published,
        files.latest_publication_date as publication_date,
        files.latest_download_run,
        list_transform(string_split(stg.pagecolumnline, '.'), part -> part::integer) as line_code_parts
    from {{ ref('stg_hcai_finance_csv') }} as stg
    left join files on stg._member_sha256 = files.member_sha256
    where {{ hcai_finance_hold_reason('stg') }} is null
)

select
    * exclude (latest_download_run),
    member_sha256 || ':' || source_row_number as finance_row_key,
    case when len(line_code_parts) = 3 then 'page_column_line' else 'four_part_code' end as code_layout,
    line_code_parts[1] as page_number,
    case when len(line_code_parts) = 3 then line_code_parts[2] end as column_number,
    case when len(line_code_parts) = 3 then line_code_parts[3] end as line_number,
    {{ state_number('value_published') }} as value_number,
    value_form = 'redacted' as is_redacted,
    dense_rank() over (
        partition by facility_id, report_period_end, page_column_line
        order by publication_date desc nulls last, latest_download_run desc nulls last, member_sha256 desc
    ) = 1 as is_latest_release
from typed
