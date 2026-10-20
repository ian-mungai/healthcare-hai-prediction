{{ config(materialized='table', pre_hook='{{ assert_file_metadata_generation() }}') }}

select
    *,
    kind || ':' || observation_id as metadata_key
from {{ file_metadata_source() }}
