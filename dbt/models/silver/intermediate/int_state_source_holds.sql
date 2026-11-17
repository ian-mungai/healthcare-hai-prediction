-- The state-source rows that cannot be keyed, one row each, with the reason: a facility or entity that is no number, a period that is no date, or a
-- financial-disclosure line that is no page.column.line. The utilization description row is one. A held row stays here and is never dropped
-- silently; the typed models leave it out.
{{ config(materialized='table') }}

with

held as (
    select
        'hcai_finance_csv' as bronze_table,
        stg._member_sha256 as member_sha256,
        stg._row_number as source_row_number,
        {{ hcai_finance_hold_reason('stg') }} as hold_reason,
        nullif(trim(stg.oshpd_id), '') as facility_published
    from {{ ref('stg_hcai_finance_csv') }} as stg
    union all
    select
        'hcai_util_pages_csv' as bronze_table,
        stg._member_sha256 as member_sha256,
        stg._row_number as source_row_number,
        {{ hcai_util_hold_reason('stg') }} as hold_reason,
        nullif(trim(stg.fac_no), '') as facility_published
    from {{ ref('stg_hcai_util_pages_csv') }} as stg
    union all
    select
        'hcai_util_nonresponse_csv' as bronze_table,
        stg._member_sha256 as member_sha256,
        stg._row_number as source_row_number,
        {{ hcai_util_hold_reason('stg') }} as hold_reason,
        nullif(trim(stg.fac_no), '') as facility_published
    from {{ ref('stg_hcai_util_nonresponse_csv') }} as stg
    union all
    select
        'illinois_report_card_csv' as bronze_table,
        stg._member_sha256 as member_sha256,
        stg._row_number as source_row_number,
        {{ illinois_hold_reason('stg') }} as hold_reason,
        nullif(trim(stg.entity_id), '') as facility_published
    from {{ ref('stg_illinois_report_card_csv') }} as stg
)

select
    *,
    md5(concat_ws('|', bronze_table, member_sha256, source_row_number)) as hold_key
from held
where hold_reason is not null
