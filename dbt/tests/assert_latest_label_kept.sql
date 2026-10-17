-- A renamed or held key keeps the label of its latest release: the latest ordering value, then the second ordering value, then the first label in order.
-- The latest label is found with a window function, not the model's distinct on, and must be the label of a row of the key [940].
with

kept_keys as (
    select
        dimension,
        code_system,
        code,
        coalesce(fact, '') as fact
    from {{ ref('identity_splits') }}
    where identity_rule in ('single_latest', 'hold_single_latest')
),

ranked as (
    select
        silver_labels.dimension,
        silver_labels.code_system,
        silver_labels.code,
        silver_labels.fact,
        silver_labels.published_label,
        row_number() over (
            partition by silver_labels.dimension, silver_labels.code_system, silver_labels.fact, silver_labels.code
            order by silver_labels.axis desc nulls last, silver_labels.axis_2 desc nulls last, silver_labels.published_label asc
        ) as label_rank
    from (
        {{ gold_silver_label_rows() }}
    ) as silver_labels
    inner join kept_keys
        on
            silver_labels.dimension = kept_keys.dimension
            and silver_labels.code_system = kept_keys.code_system
            and silver_labels.fact = kept_keys.fact
            and silver_labels.code = kept_keys.code
),

latest as (
    select
        dimension,
        code_system,
        code,
        fact,
        published_label
    from ranked
    where label_rank = 1
),

kept as (
    select
        'dim_drg' as dimension,
        code_system,
        code,
        '' as fact,
        code_label as kept_label
    from {{ ref('dim_drg') }}
    union all
    select
        'dim_rurality_code' as dimension,
        code_system,
        code,
        '' as fact,
        code_label as kept_label
    from {{ ref('dim_rurality_code') }}
    union all
    select
        'dim_measure' as dimension,
        '' as code_system,
        measure_key as code,
        fact,
        measure_label as kept_label
    from {{ ref('dim_measure') }}
)

select
    latest.dimension,
    latest.code,
    latest.published_label as latest_label
from latest
left join kept
    on
        latest.dimension = kept.dimension
        and latest.fact = kept.fact
        and latest.code = kept.code
        and (latest.dimension = 'dim_measure' or latest.code_system = kept.code_system)
        and latest.published_label = kept.kept_label
group by latest.dimension, latest.code, latest.published_label
having count(kept.kept_label) = 0
