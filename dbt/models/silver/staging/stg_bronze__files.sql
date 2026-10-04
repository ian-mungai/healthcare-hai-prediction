-- One row per distinct stored file (bronze table and member SHA-256): its canonical copy, every release that published
-- it, its row count, whether its copies' labels conflict and any owner label hold (failure modes 166 to 172, 180, 181).
{{ config(materialized='table') }}

with

copies as (
    select
        bronze_table,
        source_family,
        object_key,
        member_sha256,
        file_name,
        publication_release,
        row_count,
        is_canonical
    from {{ ref('stg_bronze__file_copies') }}
),

conflicts as (
    -- Copies conflict when they disagree on role or identity year; differing rule stages are republications [180].
    select
        bronze_table,
        member_sha256,
        count(distinct role || ':' || identity_year) > 1 as has_label_conflict
    from {{ ref('stg_bronze__file_labels') }}
    group by
        bronze_table,
        member_sha256
),

holds as (
    select
        bronze_table,
        member_sha256,
        issue,
        reason
    from {{ ref('staging_label_holds') }}
),

files as (
    select
        copies.bronze_table,
        copies.source_family,
        copies.member_sha256,
        max(case when copies.is_canonical then copies.object_key end) as canonical_object_key,
        count(*) as copy_count,
        count(*) filter (where copies.is_canonical) as canonical_count,
        max(copies.row_count) as row_count,
        list_sort(list_distinct(list(copies.publication_release))) as publication_releases,
        max(try_cast(copies.publication_release as date)) as latest_publication_date,
        list_sort(list_distinct(list(copies.file_name))) as file_names
    from copies
    group by
        copies.bronze_table,
        copies.source_family,
        copies.member_sha256
),

final as (
    select
        files.bronze_table,
        files.source_family,
        files.member_sha256,
        files.canonical_object_key,
        files.copy_count,
        files.canonical_count,
        files.row_count,
        files.publication_releases,
        files.latest_publication_date,
        files.file_names,
        holds.issue as label_hold_issue,
        holds.reason as label_hold_reason,
        files.bronze_table || ':' || files.member_sha256 as file_key,
        coalesce(conflicts.has_label_conflict, false) as has_label_conflict,
        holds.issue is not null as is_label_held
    from files
    left join conflicts
        on
            files.bronze_table = conflicts.bronze_table
            and files.member_sha256 = conflicts.member_sha256
    left join holds
        on
            files.bronze_table = holds.bronze_table
            and files.member_sha256 = holds.member_sha256
)

select * from final
