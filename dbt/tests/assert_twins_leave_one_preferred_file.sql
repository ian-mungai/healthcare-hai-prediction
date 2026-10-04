-- Fails for a twin pair that leaves the wrong number of files selected by the twin rule: one for an agreeing or
-- not-compared pair, none for a differing pair [187].
with

comparisons as (
    select
        twin_key,
        text_table,
        text_sha256,
        workbook_table,
        workbook_sha256,
        twin_status
    from {{ ref('stg_bronze__twin_comparison') }}
),

selection as (
    select
        bronze_table,
        member_sha256,
        is_twin_excluded
    from {{ ref('stg_bronze__file_selection') }}
),

kept as (
    select
        comparisons.twin_key,
        comparisons.twin_status,
        count(*) filter (where not selection.is_twin_excluded) as kept_files
    from comparisons
    inner join selection
        on
            (comparisons.text_table = selection.bronze_table and comparisons.text_sha256 = selection.member_sha256)
            or (comparisons.workbook_table = selection.bronze_table and comparisons.workbook_sha256 = selection.member_sha256)
    group by
        comparisons.twin_key,
        comparisons.twin_status
)

select
    twin_key,
    twin_status,
    kept_files
from kept
where kept_files <> case when twin_status = 'differ' then 0 else 1 end
