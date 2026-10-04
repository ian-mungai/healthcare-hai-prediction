-- Fails for each loaded object whose rows carry more than one file checksum [168].
select
    copy_key,
    checksum_count
from {{ ref('stg_bronze__file_copies') }}
where checksum_count <> 1
