-- Fails for each file without exactly one canonical copy.
select
    file_key,
    canonical_count
from {{ ref('stg_bronze__files') }}
where canonical_count <> 1 or canonical_object_key is null
