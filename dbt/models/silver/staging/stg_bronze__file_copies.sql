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

copies as (
    select
        observations.*,
        physical_index.canonical_object_key as indexed_object_key,
        physical_index.canonical_observation_id,
        physical_index.bucket as indexed_bucket,
        physical_index.s3_key as indexed_s3_key,
        physical_index.s3_version_id as indexed_s3_version_id,
        row_number() over (
            partition by observations.bronze_table, observations.bucket, observations.s3_key, observations.s3_version_id
            order by observations.is_canonical desc, observations.object_key asc, observations.observation_id asc
        ) as physical_rank
    from {{ ref('stg_bronze__release_observations') }} as observations
    inner join {{ ref('stg_bronze__physical_object_index') }} as physical_index
        on observations.member_sha256 = physical_index.member_sha256
    where not observations.is_retired and not observations.is_failed_attempt
),

final as (
    select
        copies.bronze_table,
        copies.source_family,
        copies.member_sha256,
        loaded.checksum_count,
        copies.bucket,
        copies.s3_key,
        copies.s3_version_id,
        copies.byte_count,
        copies.member_path,
        copies.file_name,
        copies.snapshot_id,
        copies.dataset_id,
        copies.release_id,
        copies.release_source,
        loaded.row_count,
        copies.is_retired,
        copies.publication_release,
        copies.publication_date,
        copies.is_release_from_container,
        copies.indexed_object_key as object_key,
        copies.observation_id = copies.canonical_observation_id
        and copies.bucket = copies.indexed_bucket
        and copies.s3_key = copies.indexed_s3_key
        and copies.s3_version_id = copies.indexed_s3_version_id as is_canonical,
        copies.bronze_table || ':' || md5(to_json([copies.bucket, copies.s3_key, copies.s3_version_id])) as copy_key
    from copies
    left join loaded
        on copies.bronze_table = loaded.bronze_table and copies.indexed_object_key = loaded.object_key
    where copies.physical_rank = 1
)

select * from final
