{{ config(materialized='table') }}

with

copies as (
    select
        bronze_table,
        source_family,
        object_key,
        member_sha256,
        row_count,
        is_canonical
    from {{ ref('stg_bronze__file_copies') }}
),

conflicts as (
    -- Copies conflict when they disagree on role or identity year; differing rule stages are republications.
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

releases as (
    select
        bronze_table,
        member_sha256,
        list_sort(list_distinct(list(publication_release) filter (where not is_failed_attempt))) as publication_releases,
        list_sort(list_distinct(list(publication_date) filter (where not is_failed_attempt))) as publication_dates,
        list_sort(list_distinct(list(download_run_id))) as download_run_ids,
        max(publication_date) filter (where not is_failed_attempt) as latest_publication_date,
        list_sort(list_distinct(list(file_name))) as file_names
    from {{ ref('stg_bronze__release_observations') }}
    group by bronze_table, member_sha256
),

files as (
    select
        copies.bronze_table,
        copies.source_family,
        copies.member_sha256,
        max(case when copies.is_canonical then copies.object_key end) as canonical_object_key,
        count(*) as copy_count,
        count(*) filter (where copies.is_canonical) as canonical_count,
        max(copies.row_count) as row_count
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
        releases.publication_releases,
        releases.publication_dates,
        releases.download_run_ids,
        releases.latest_publication_date,
        releases.file_names,
        holds.issue as label_hold_issue,
        holds.reason as label_hold_reason,
        files.bronze_table || ':' || files.member_sha256 as file_key,
        coalesce(conflicts.has_label_conflict, false) as has_label_conflict,
        holds.issue is not null as is_label_held
    from files
    inner join releases on files.bronze_table = releases.bronze_table and files.member_sha256 = releases.member_sha256
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
