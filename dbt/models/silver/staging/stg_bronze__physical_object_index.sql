{{ config(materialized='table') }}

select
    generation,
    table_name as bronze_table,
    sha256 as member_sha256,
    _object_key as canonical_object_key,
    observation_id as canonical_observation_id,
    bucket,
    s3_key,
    s3_version_id,
    byte_count,
    canonical_previous_observation_id,
    canonical_superseding_manifest_key,
    canonical_superseding_manifest_version_id
from {{ ref('stg_bronze__metadata') }}
where kind = 'object'
