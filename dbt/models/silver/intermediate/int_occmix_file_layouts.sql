-- One layout per selected file and sheet. Exact normalized field names distinguish survey rows from CBSA and S-3 data.
{{ config(materialized='table') }}

with

header_candidates as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        families,
        source_row_number,
        list_transform(field_values, field -> lower(trim(trim(field), '"'))) as header_fields,
        len(list_filter(header_fields, field -> field in ('prov', 'from', 'to', 'rnsal', 'rnhr', 'lpnsthr', 'naorathr', 'mahr', 'nursehr'))) >= 4
            as has_survey_fields
    from {{ ref('int_occmix_file_rows') }}
    where source_row_number <= 5
),

first_rows as (
    select
        bronze_table,
        member_sha256,
        sheet_name,
        first(families) as families,
        coalesce(min(source_row_number) filter (where has_survey_fields), min(source_row_number)) as header_row_number,
        arg_min(header_fields, case when has_survey_fields then source_row_number else source_row_number + 5 end) as header_fields,
        bool_or(has_survey_fields) as has_survey_fields
    from header_candidates
    group by bronze_table, member_sha256, sheet_name
),

expected as (
    select
        *,
        list_has_any(families, [
            'occ_mix', 'occmix', 'nprm26_occmix', 'occmix_deleted', 'occmix_deleted_from', 'deleted_om',
            's3_occmix_occ_mix', 's3_occmix_occ_mix_deleted', 's3_occmix_occmix_deleted', 'nprm26_deleted_om'
        ])
        or (sheet_name = '' and list_has_any(families, ['s3_occ_mix', 's3_occmix']))
        or (
            regexp_matches(lower(sheet_name), 'occ\s*mix|occ_mix|occupational|deleted om data')
            and not regexp_matches(lower(sheet_name), 'descr|cbsa|factor')
        ) as is_expected_survey
    from first_rows
)

select
    *,
    bronze_table || ':' || member_sha256 || ':' || sheet_name as layout_key,
    has_survey_fields or is_expected_survey as is_survey_candidate,
    list_has_any(header_fields, [
        'is prov on this occupational mix delete list because its s-3 wage data is also deleted? (y/n)',
        'is prov on this occupational mix delete list because its occupational mix data is aberrant? (y/n)'
    ]) as is_deleted_layout,
    list_has_all(header_fields, ['prov', 'from', 'to', 'rnsal', 'rnhr', 'lpnsthr', 'naorathr', 'mahr', 'nursehr']) as is_survey_layout,
    len(list_filter(header_fields, field -> field in ('prov', 'from', 'to', 'rnsal', 'rnhr', 'lpnsthr', 'naorathr', 'mahr', 'nursehr')))
    - len(list_distinct(list_filter(header_fields, field -> field in ('prov', 'from', 'to', 'rnsal', 'rnhr', 'lpnsthr', 'naorathr', 'mahr', 'nursehr'))))
        as repeated_fields
from expected
