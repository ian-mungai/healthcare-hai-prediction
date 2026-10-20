{{ config(materialized='table') }}

with metadata as (
    select *
    from {{ ref('stg_bronze__metadata') }}
),

families as (
    {%- for table, family in var('copy_tables').items() %}
    select '{{ table }}' as bronze_table, '{{ family }}' as source_family
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
)

select
    metadata.observation_id,
    metadata.generation,
    metadata.table_name as bronze_table,
    families.source_family,
    metadata._object_key as object_key,
    metadata.canonical_object_key,
    metadata.sha256 as member_sha256,
    metadata.bucket,
    metadata.s3_key,
    metadata.s3_version_id,
    metadata.source_id,
    metadata.snapshot_id,
    metadata.dataset_id,
    metadata.release_id,
    metadata.release_partition,
    metadata.manifest_key,
    metadata.manifest_version_id,
    metadata.manifest_sha256,
    metadata.member_path,
    metadata.file_name,
    metadata.byte_count,
    metadata.loaded as is_canonical,
    metadata.retired as is_retired,
    metadata.failed_attempt as is_failed_attempt,
    metadata.superseded_by_manifest_key,
    metadata.superseded_by_manifest_version_id,
    metadata.manifest_entry_json,
    metadata.manifest_evidence_json,
    metadata.retirement_record_json,
    metadata.publication_release,
    cast(metadata.publication_date as date) as publication_date,
    metadata.download_run_id,
    metadata.release_type,
    metadata.release_source,
    metadata.release_source = 'container' as is_release_from_container,
    metadata.table_name || ':' || metadata._object_key as copy_key
from metadata
inner join families on metadata.table_name = families.bronze_table
where metadata.kind = 'observation'
