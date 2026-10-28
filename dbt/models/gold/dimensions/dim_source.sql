-- One row per stored dataset, plus the unknown member. The publisher and label of a dataset come from the source_labels seed, which
-- scripts/lakehouse/source_labels.py generates from the source registry: a placed dataset takes its source's publisher and title, and a dataset the
-- registry lists as unplaced keeps a null publisher and label. A label is never guessed, and a dataset is never moved to the unknown member. The
-- unknown member has no seed row, so its publisher and label are null. The bronze table and the source family are not attributes of a dataset: a
-- dataset can feed several bronze tables, so they stay on the stored-copy model.
with

observed as (
    select distinct cast(dataset_id as varchar) as dataset_id
    from {{ ref('stg_bronze__release_observations') }}
    where dataset_id is not null
),

keyed as (
    select
        md5(dataset_id) as source_sk,
        dataset_id
    from observed
    union all
    select
        md5('<unknown>') as source_sk,
        '<unknown>' as dataset_id
),

labels as (
    select
        dataset_id,
        publisher,
        label
    from {{ ref('source_labels') }}
),

final as (
    select
        keyed.source_sk,
        keyed.dataset_id,
        labels.publisher,
        labels.label
    from keyed
    left join labels
        on keyed.dataset_id = labels.dataset_id
)

select * from final
