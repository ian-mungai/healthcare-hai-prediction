-- dim_source takes the publisher and label of a placed dataset from the source_labels seed and leaves both null for a dataset the seed lists as unplaced
-- and for a dataset with no seed row. It never guesses a label and never gives the unknown member one [942] [945].
select
    'placed dataset differs from the seed' as problem,
    dim_source.dataset_id as detail
from {{ ref('dim_source') }} as dim_source
inner join {{ ref('source_labels') }} as source_labels
    on dim_source.dataset_id = source_labels.dataset_id
where
    source_labels.placement = 'placed'
    and (dim_source.publisher is distinct from source_labels.publisher or dim_source.label is distinct from source_labels.label)
union all
select
    'unplaced dataset has a label' as problem,
    dim_source.dataset_id as detail
from {{ ref('dim_source') }} as dim_source
inner join {{ ref('source_labels') }} as source_labels
    on dim_source.dataset_id = source_labels.dataset_id
where source_labels.placement = 'unplaced' and (dim_source.publisher is not null or dim_source.label is not null)
union all
select
    'dataset without a seed row has a label' as problem,
    dim_source.dataset_id as detail
from {{ ref('dim_source') }} as dim_source
left join {{ ref('source_labels') }} as source_labels
    on dim_source.dataset_id = source_labels.dataset_id
where source_labels.dataset_id is null and (dim_source.publisher is not null or dim_source.label is not null)
union all
select
    'seed row of a placed dataset lacks a publisher or label' as problem,
    dataset_id as detail
from {{ ref('source_labels') }}
where placement = 'placed' and (publisher is null or label is null or source_id is null)
union all
select
    'seed row of an unplaced dataset has a value' as problem,
    dataset_id as detail
from {{ ref('source_labels') }}
where placement = 'unplaced' and (publisher is not null or label is not null or source_id is not null)
