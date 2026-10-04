-- Fails for an IPPS or occupational-mix copy without a label, a label of another checksum, or a label row naming no
-- loaded copy: the generated map must match storage [182].
with

labelled as (
    select
        copy_key,
        bronze_table,
        object_key
    from {{ ref('stg_bronze__file_labels') }}
    where label_source is null or not is_label_checksum_match
),

copies as (
    select
        bronze_table,
        object_key
    from {{ ref('stg_bronze__file_copies') }}
),

orphans as (
    select
        labels.bronze_table || ':' || labels.object_key as copy_key,
        labels.bronze_table,
        labels.object_key
    from {{ ref('ipps_occmix_copy_labels') }} as labels
    left join copies
        on
            labels.bronze_table = copies.bronze_table
            and labels.object_key = copies.object_key
    where copies.object_key is null
)

select * from labelled
union all
select * from orphans
