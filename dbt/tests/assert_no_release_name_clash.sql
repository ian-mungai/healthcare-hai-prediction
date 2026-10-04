-- Fails where one publication release holds different files under one name: never collapsed, left for reconciliation [170] [173].
select
    bronze_table,
    publication_release,
    file_name,
    count(distinct member_sha256) as file_versions
from {{ ref('stg_bronze__file_copies') }}
group by
    bronze_table,
    publication_release,
    file_name
having count(distinct member_sha256) > 1
