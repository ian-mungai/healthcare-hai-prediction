-- Fails for each label hold that names no stored file, so a stale hold is never kept silently [172].
with

holds as (
    select
        bronze_table,
        member_sha256,
        issue
    from {{ ref('staging_label_holds') }}
),

files as (
    select
        bronze_table,
        member_sha256
    from {{ ref('stg_bronze__files') }}
)

select
    holds.bronze_table,
    holds.member_sha256,
    holds.issue
from holds
left join files
    on
        holds.bronze_table = files.bronze_table
        and holds.member_sha256 = files.member_sha256
where files.member_sha256 is null
