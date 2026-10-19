-- A code or measure with more than one distinct published label needs a row in identity_splits: no multi-label key gets a default rule [940].
with

label_counts as (
    select
        dimension,
        code_system,
        fact,
        code,
        count(distinct published_label) as label_count
    from (
        {{ gold_silver_label_rows() }}
    ) as silver_labels
    group by dimension, code_system, fact, code
)

select
    label_counts.dimension,
    label_counts.code_system,
    label_counts.fact,
    label_counts.code,
    label_counts.label_count
from label_counts
left join {{ ref('identity_splits') }} as identity_splits
    on
        label_counts.dimension = identity_splits.dimension
        and label_counts.code_system = identity_splits.code_system
        and label_counts.fact = coalesce(identity_splits.fact, '')
        and label_counts.code = identity_splits.code
where label_counts.label_count > 1 and identity_splits.code is null
