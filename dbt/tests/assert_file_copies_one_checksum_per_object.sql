-- Fails for each loaded object whose rows carry more than one file checksum.
select
    copy_key,
    checksum_count
from {{ ref('stg_bronze__file_copies') }}
where checksum_count <> 1
