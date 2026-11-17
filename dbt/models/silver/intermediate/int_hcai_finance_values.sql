-- One row per California HCAI financial-disclosure value: facility (OSHPD number), report period end and page.column.line, from each stored release.
-- The value keeps its published text and types as a number when it is a plain number once thousands separators are removed; a value in parentheses,
-- a name or a code stays text only. A redacted cell is flagged and has no number. Rows whose facility, period or line cannot be keyed are held in
-- int_state_source_holds, never dropped. is_latest_release marks the row of the newest release of each key (publication date, then download run).
-- The meaning and unit of a line come from the HCAI data dictionary in bronze, not from this model. The stored copy is the privacy-filtered derivative.
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
        trim(stg.oshpd_id) as facility_id,
        {{ state_date('stg.rpe') }} as report_period_end,
        trim(stg.pagecolumnline) as page_column_line,
        nullif(trim(stg.values), '') as value_published,
        files.latest_publication_date as publication_date,
        files.latest_download_run
    from {{ ref('stg_hcai_finance_csv') }} as stg
    left join files on stg._member_sha256 = files.member_sha256
    where {{ hcai_finance_hold_reason('stg') }} is null
),

split as (
    select
        *,
        string_split(page_column_line, '.') as line_parts
    from typed
)

select
    member_sha256 || ':' || source_row_number as finance_row_key,
    member_sha256,
    source_row_number,
    facility_id,
    report_period_end,
    page_column_line,
    line_parts[1]::integer as page_number,
    line_parts[2]::integer as column_number,
    line_parts[3]::integer as line_number,
    value_published,
    {{ state_number('value_published') }} as value_number,
    coalesce(value_published = '[REDACTED]', false) as is_redacted,
    publication_date,
    dense_rank() over (
        partition by facility_id, report_period_end, page_column_line
        order by publication_date desc nulls last, latest_download_run desc nulls last, member_sha256 desc
    ) = 1 as is_latest_release
from split
