-- One row per stored dataset, plus the unknown member. The publisher and label are deferred (amendment A36). The bronze table and the source
-- family are not attributes of a dataset: a dataset can feed several bronze tables, so they stay on the stored-copy model [824].
with

observed as (
    select distinct cast(dataset_id as varchar) as dataset_id
    from {{ ref('stg_bronze__file_copies') }}
    where dataset_id is not null
),

final as (
    select
        md5(dataset_id) as source_sk,
        dataset_id
    from observed
    union all
    select
        md5('<unknown>') as source_sk,
        '<unknown>' as dataset_id
)

select * from final
