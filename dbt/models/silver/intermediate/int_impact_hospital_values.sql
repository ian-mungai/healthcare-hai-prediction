-- One row per impact-file release (file and sheet), hospital (CCN) and mapped field, from the hospital rows whose CCN
-- appears once in the release (failure modes 345 and 349 to 352). The published text is kept, with a SAS missing dot as
-- text and no number; a number is set only for a plain number in a numeric field, or a plain number with a percent sign
-- (divided by 100).
{{ config(materialized='table') }}

with

position_lists as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        unnest(field_positions) as position_map
    from {{ ref('int_impact_file_layouts') }}
    where
        ccn_columns = 1
        and repeated_fields = 0
),

positions as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        position_map['field'] as field,
        position_map['field_position'] as field_position
    from position_lists
),

mapped_values as (
    select
        hospital_rows.bronze_table,
        hospital_rows.member_sha256,
        hospital_rows.sheet_name,
        hospital_rows.family,
        hospital_rows.rule_fiscal_year,
        hospital_rows.rule_stage,
        hospital_rows.ccn,
        positions.field,
        positions.field not in ('urgeo', 'geographic_labor_market_area') as is_numeric_field,
        nullif(trim(trim(trim(hospital_rows.field_values[positions.field_position]), '"')), '') as value_text
    from {{ ref('int_impact_hospital_rows') }} as hospital_rows
    inner join positions
        on
            hospital_rows.bronze_table = positions.bronze_table
            and hospital_rows.member_sha256 = positions.member_sha256
            and hospital_rows.sheet_name = positions.sheet_name
            and positions.field <> 'ccn'
    where hospital_rows.rows_for_ccn = 1
)

select
    *,
    case
        when not is_numeric_field then null
        when regexp_full_match(value_text, '-?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][-+]?[0-9]+)?') then value_text::double
        when regexp_full_match(value_text, '-?([0-9]+(\.[0-9]*)?|\.[0-9]+)%') then rtrim(value_text, '%')::double / 100
    end as value_number,
    member_sha256 || ':' || sheet_name || ':' || ccn || ':' || field as value_key
from mapped_values
