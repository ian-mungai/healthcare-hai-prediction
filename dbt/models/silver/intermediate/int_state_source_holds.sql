-- The state-source rows that cannot be kept, one row each, with the reason: a facility, entity, measure, period or publisher code that is not in its
-- published shape, a period that runs backwards, a number or financial value that is not in a reviewed form, or a county name that does not identify
-- exactly one county of the state. A held row is identified by its file checksum and row number only: the rejected text, which can name a person, is
-- never copied here. hold_detail names the number fields that failed and nothing else. The typed models leave these rows out, and
-- assert_state_rows_reconcile makes typed rows plus held rows equal the staged rows of each file.
{{ config(materialized='table') }}

with

util_county_keys as (
    {{ state_county_keys('CA') }}
),

il_county_keys as (
    {{ state_county_keys('IL') }}
),

util_sources as (
    {{ hcai_util_sources() }}
),

held as (
    select
        'hcai_finance_csv' as bronze_table,
        stg._member_sha256 as member_sha256,
        stg._row_number as source_row_number,
        {{ hcai_finance_hold_reason('stg') }} as hold_reason,
        null::varchar as hold_detail
    from {{ ref('stg_hcai_finance_csv') }} as stg
    union all
    select
        sources.bronze_table,
        sources._member_sha256 as member_sha256,
        sources._row_number as source_row_number,
        {{ hcai_util_hold_reason('sources', 'util_county_keys') }} as hold_reason,
        sources.unsafe_value_fields as hold_detail
    from util_sources as sources
    left join util_county_keys on {{ state_county_key('sources.county') }} = util_county_keys.county_key
    union all
    select
        'illinois_report_card_csv' as bronze_table,
        stg._member_sha256 as member_sha256,
        stg._row_number as source_row_number,
        {{ illinois_hold_reason('stg', 'il_county_keys') }} as hold_reason,
        stg.unsafe_value_fields as hold_detail
    from {{ ref('stg_illinois_report_card_csv') }} as stg
    left join il_county_keys on {{ state_county_key('stg.county_name') }} = il_county_keys.county_key
)

select
    *,
    md5(concat_ws('|', bronze_table, member_sha256, source_row_number)) as hold_key
from held
where hold_reason is not null
