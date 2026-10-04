-- One row per distinct stored file: whether intermediate models may read it. A file is not selected while its labels
-- are held, or when a twin pair excludes it: the other twin is preferred, or the pair differs (failure modes 172, 187).
{{ config(materialized='table') }}

with

files as (
    select
        file_key,
        bronze_table,
        member_sha256,
        is_label_held,
        has_label_conflict
    from {{ ref('stg_bronze__files') }}
),

comparisons as (
    select
        twin_key,
        text_table,
        text_sha256,
        workbook_table,
        workbook_sha256,
        twin_status,
        preferred_sha256
    from {{ ref('stg_bronze__twin_comparison') }}
),

twin_sides as (
    select
        text_table as bronze_table,
        text_sha256 as member_sha256,
        twin_key,
        twin_status,
        preferred_sha256
    from comparisons
    union all
    select
        workbook_table as bronze_table,
        workbook_sha256 as member_sha256,
        twin_key,
        twin_status,
        preferred_sha256
    from comparisons
),

twin_flags as (
    -- A file in several pairs is excluded when any of its pairs excludes it.
    select
        bronze_table,
        member_sha256,
        count(*) as twin_pair_count,
        bool_or(twin_status = 'differ' or preferred_sha256 is distinct from member_sha256) as is_twin_excluded
    from twin_sides
    group by
        bronze_table,
        member_sha256
),

final as (
    select
        files.file_key,
        files.bronze_table,
        files.member_sha256,
        files.is_label_held,
        files.has_label_conflict,
        coalesce(twin_flags.twin_pair_count, 0) as twin_pair_count,
        coalesce(twin_flags.is_twin_excluded, false) as is_twin_excluded,
        not files.is_label_held and not coalesce(twin_flags.is_twin_excluded, false) as is_selected
    from files
    left join twin_flags
        on
            files.bronze_table = twin_flags.bronze_table
            and files.member_sha256 = twin_flags.member_sha256
)

select * from final
