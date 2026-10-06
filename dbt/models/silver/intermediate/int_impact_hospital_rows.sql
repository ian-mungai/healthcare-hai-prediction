-- One row per hospital row of a readable impact file or sheet (one CCN column, no repeated field): a row after the header
-- whose first field is a CCN; a footer or note row is not data, and a 5-digit CCN lost its leading zero in a workbook cell
-- (failure modes 350 and 351). rows_for_ccn counts the CCN's rows in the release; a repeated CCN is held.
{{ config(materialized='table') }}

with

layouts as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        header_row,
        ccn_position
    from {{ ref('int_impact_file_layouts') }}
    where
        ccn_columns = 1
        and repeated_fields = 0
),

data_rows as (
    select
        file_rows.bronze_table,
        file_rows.member_sha256,
        file_rows.sheet_name,
        file_rows.line_number,
        file_rows.field_values,
        file_rows.family,
        file_rows.rule_fiscal_year,
        file_rows.rule_stage,
        trim(replace(file_rows.field_values[layouts.ccn_position], '"', '')) as ccn_text
    from {{ ref('int_impact_file_rows') }} as file_rows
    inner join layouts
        on
            file_rows.bronze_table = layouts.bronze_table
            and file_rows.member_sha256 = layouts.member_sha256
            and file_rows.sheet_name = layouts.sheet_name
    where file_rows.line_number > layouts.header_row
),

hospital_rows as (
    select
        *,
        case
            when regexp_full_match(ccn_text, '[0-9]{5}(\.0+)?') then lpad(split_part(ccn_text, '.', 1), 6, '0')
            when regexp_full_match(ccn_text, '[0-9]{6}\.0+') then split_part(ccn_text, '.', 1)
            when regexp_full_match(ccn_text, '[0-9A-Z]{6}') then ccn_text
        end as ccn
    from data_rows
)

select
    bronze_table,
    member_sha256,
    sheet_name,
    line_number,
    field_values,
    family,
    rule_fiscal_year,
    rule_stage,
    ccn,
    count(*) over (partition by bronze_table, member_sha256, sheet_name, ccn) as rows_for_ccn,
    bronze_table || ':' || member_sha256 || ':' || sheet_name || ':' || line_number as hospital_row_key
from hospital_rows
where ccn is not null
