-- One row per stored copy in the tables whose copies staging resolves, loaded or not: each copy's file, capture and
-- publication release, and whether it is the copy bronze loads (failure modes 167 to 171, 204 to 209).
{{ config(materialized='table') }}

with

{% for table in var('copy_tables') %}
loaded_{{ table }} as (
    select
        '{{ table }}' as bronze_table,
        _object_key as object_key,
        count(distinct _member_sha256) as checksum_count,
        count(*) as row_count
    from {{ source('bronze', table) }}
    group by _object_key
),

{% endfor %}
loaded as (
    {{ union_copy_tables('loaded_') }}
),

families as (
    {%- for table, family in var('copy_tables').items() %}
    select
        '{{ table }}' as bronze_table,
        '{{ family }}' as source_family
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
),

copies as (
    select
        stored_copies.table_name as bronze_table,
        families.source_family,
        stored_copies._object_key as object_key,
        stored_copies.sha256 as member_sha256,
        stored_copies.s3_key,
        stored_copies.s3_version_id,
        stored_copies.member_path,
        stored_copies.file_name,
        stored_copies.snapshot_id,
        stored_copies.dataset_id,
        stored_copies.release_id,
        stored_copies.loaded as is_loaded,
        stored_copies.retired as is_retired
    from {{ source('bronze', 'stored_copies') }} as stored_copies
    inner join families on stored_copies.table_name = families.bronze_table
),

releases as (
    select
        copies.*,
        -- A nested archive member is dated by its nested archive's name; otherwise it keeps its container's release [170].
        case
            when not contains(copies.member_path, '!') then 'release_partition'
            when regexp_matches(copies.member_path, '\d{4}-\d{2}-\d{2}\.zip!') then 'nested_archive'
            else 'container'
        end as release_source
    from copies
),

final as (
    select
        releases.bronze_table,
        releases.source_family,
        releases.object_key,
        releases.member_sha256,
        loaded.checksum_count,
        releases.s3_key,
        releases.s3_version_id,
        releases.member_path,
        releases.file_name,
        releases.snapshot_id,
        releases.dataset_id,
        releases.release_id,
        releases.release_source,
        -- Only the loaded copy has rows in bronze; its copies hold the same bytes [209].
        loaded.row_count,
        releases.is_retired,
        -- The canonical copy is the one bronze loads: the smallest object key, never tied to capture time [167] [205].
        releases.is_loaded as is_canonical,
        releases.bronze_table || ':' || releases.object_key as copy_key,
        case
            when releases.release_source = 'nested_archive' then regexp_extract(releases.member_path, '.*(\d{4}-\d{2}-\d{2})\.zip!', 1)
            else releases.release_id
        end as publication_release,
        releases.release_source = 'container' as is_release_from_container
    from releases
    left join loaded
        on
            releases.bronze_table = loaded.bronze_table
            and releases.object_key = loaded.object_key
)

select * from final
