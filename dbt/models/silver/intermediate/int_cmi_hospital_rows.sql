-- One row per hospital (CCN) in each selected case-mix index file, for each rule year and stage the file's copies are
-- labelled with: the unadjusted CMI, cases and relative weights, and the transfer-adjusted CMI and cases in their own
-- columns, never substituted (failure modes 293 to 304).
{{ config(materialized='table') }}

with

labels as (
    -- A file labelled for several rule years or stages gives one row set per label [300].
    select distinct
        bronze_table,
        member_sha256,
        rule_fiscal_year,
        data_fiscal_year,
        rule_stage
    from ({{ cmi_snapshot_files() }}) as snapshot_files
    where
        is_selected
        and family in {{ quoted_list('cmi_families') }}
),

file_rows as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        line_number,
        field_values
    from {{ ref('int_cmi_file_rows') }}
),

layouts as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        has_header,
        header_row,
        ccn_position,
        cases_position,
        cmi_position,
        relative_weights_position,
        transfer_adjusted_cmi_position,
        transfer_adjusted_cases_position
    from {{ ref('int_cmi_file_layouts') }}
),

values_read as (
    -- Quotes and thousands separators go before any cast [296].
    select
        file_rows.bronze_table,
        file_rows.member_sha256,
        file_rows.sheet_name,
        file_rows.line_number,
        layouts.has_header,
        nullif(replace(replace(trim(file_rows.field_values[layouts.ccn_position]), '"', ''), ',', ''), '') as ccn_text,
        nullif(replace(replace(trim(file_rows.field_values[layouts.cmi_position]), '"', ''), ',', ''), '') as cmi_source_value,
        nullif(replace(replace(trim(file_rows.field_values[layouts.cases_position]), '"', ''), ',', ''), '') as cases_text,
        nullif(replace(replace(trim(file_rows.field_values[layouts.relative_weights_position]), '"', ''), ',', ''), '') as weights_text,
        nullif(replace(replace(trim(file_rows.field_values[layouts.transfer_adjusted_cmi_position]), '"', ''), ',', ''), '') as ta_cmi_text,
        nullif(replace(replace(trim(file_rows.field_values[layouts.transfer_adjusted_cases_position]), '"', ''), ',', ''), '') as ta_cases_text
    from file_rows
    inner join layouts
        on
            file_rows.bronze_table = layouts.bronze_table
            and file_rows.member_sha256 = layouts.member_sha256
            and file_rows.sheet_name = layouts.sheet_name
    where not (layouts.has_header and file_rows.line_number = layouts.header_row)
),

typed as (
    -- A 5-digit CCN lost its leading zero in a workbook cell [304]; a row without a CCN is a blank or footer line [298].
    select
        *,
        case
            when regexp_full_match(ccn_text, '[0-9]{5}(\.0+)?') then lpad(split_part(ccn_text, '.', 1), 6, '0')
            when regexp_full_match(ccn_text, '[0-9]{6}\.0+') then split_part(ccn_text, '.', 1)
            when regexp_full_match(ccn_text, '[0-9A-Z]{6}') then ccn_text
        end as ccn,
        try_cast(cmi_source_value as double) as cmi,
        try_cast(cases_text as double) as cases,
        try_cast(weights_text as double) as relative_weights,
        try_cast(ta_cmi_text as double) as transfer_adjusted_cmi,
        try_cast(ta_cases_text as double) as transfer_adjusted_cases
    from values_read
)

select
    typed.bronze_table,
    typed.member_sha256,
    typed.sheet_name,
    typed.line_number,
    labels.rule_fiscal_year,
    labels.data_fiscal_year,
    labels.rule_stage,
    typed.ccn,
    typed.cmi,
    typed.cmi_source_value,
    typed.cases,
    typed.relative_weights,
    typed.transfer_adjusted_cmi,
    typed.transfer_adjusted_cases,
    typed.has_header,
    typed.bronze_table || ':' || typed.member_sha256 || ':' || typed.sheet_name || ':' || typed.line_number || ':'
    || labels.rule_fiscal_year || ':' || labels.rule_stage as cmi_row_key
from typed
inner join labels
    on
        typed.bronze_table = labels.bronze_table
        and typed.member_sha256 = labels.member_sha256
where typed.ccn is not null
