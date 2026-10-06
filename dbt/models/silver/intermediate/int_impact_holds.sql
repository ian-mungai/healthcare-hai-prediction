-- One row per impact-file release and CCN held: the CCN appears in more than one row of the release, so none of its rows
-- is read (failure mode 351).
{{ config(materialized='table') }}

select
    bronze_table,
    member_sha256,
    sheet_name,
    family,
    rule_fiscal_year,
    rule_stage,
    ccn,
    'repeated_in_file' as hold_reason,
    count(*) as hold_rows,
    member_sha256 || ':' || sheet_name || ':' || ccn as hold_key
from {{ ref('int_impact_hospital_rows') }}
where rows_for_ccn > 1
group by
    bronze_table,
    member_sha256,
    sheet_name,
    family,
    rule_fiscal_year,
    rule_stage,
    ccn
