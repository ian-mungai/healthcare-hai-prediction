-- Each split identity's editions carry the (published label, first ordering value, last ordering value) tuples that silver and the approved seed give, and
-- nothing else. The tuple of a label is derived here from the silver rows of the key (the minimum and maximum ordering value that carry the label),
-- independently of the models, and compared with the model rows in both directions at each measure's value-column grain: every value column of a measure
-- has one row per tuple. A label that moves to the dates of another label, or a bound that moves inside a gap between editions, fails here although the
-- partition test still passes, because that test ignores the label [940].
with

split_keys as (
    select
        dimension,
        code_system,
        code,
        coalesce(fact, '') as fact
    from {{ ref('identity_splits') }}
    where identity_rule = 'split'
),

silver_bounds as (
    select
        silver_labels.dimension,
        silver_labels.code_system,
        silver_labels.fact,
        silver_labels.code,
        silver_labels.published_label,
        min(silver_labels.axis) as edition_start,
        max(silver_labels.axis) as edition_end
    from (
        {{ gold_silver_label_rows() }}
    ) as silver_labels
    inner join split_keys
        on
            silver_labels.dimension = split_keys.dimension
            and silver_labels.code_system = split_keys.code_system
            and silver_labels.fact = split_keys.fact
            and silver_labels.code = split_keys.code
    where silver_labels.axis is not null
    group by silver_labels.dimension, silver_labels.code_system, silver_labels.fact, silver_labels.code, silver_labels.published_label
),

value_columns as (
    select distinct
        fact,
        measure_key,
        value_column
    from {{ ref('dim_measure') }}
    where measure_sk <> md5('<unknown>')
),

expected as (
    select
        silver_bounds.dimension,
        silver_bounds.fact,
        silver_bounds.code,
        silver_bounds.published_label,
        silver_bounds.edition_start,
        silver_bounds.edition_end,
        coalesce(value_columns.value_column, '') as value_column
    from silver_bounds
    left join value_columns
        on
            silver_bounds.dimension = 'dim_measure'
            and silver_bounds.fact = value_columns.fact
            and silver_bounds.code = value_columns.measure_key
    where silver_bounds.dimension <> 'dim_measure' or value_columns.fact is not null
),

actual as (
    select
        'dim_rurality_code' as dimension,
        '' as fact,
        code,
        code_label as published_label,
        edition_start,
        edition_end,
        '' as value_column
    from {{ ref('dim_rurality_code') }}
    where edition_start is not null or edition_end is not null
    union all
    select
        'dim_measure' as dimension,
        fact,
        measure_key as code,
        measure_label as published_label,
        edition_start,
        edition_end,
        value_column
    from {{ ref('dim_measure') }}
    where edition_start is not null or edition_end is not null
),

unexpected as (
    select * from actual
    except all
    select * from expected
),

missing as (
    select * from expected
    except all
    select * from actual
)

select
    'edition row is not a silver tuple' as problem,
    dimension,
    code,
    concat_ws(' | ', value_column, published_label, edition_start, edition_end) as detail
from unexpected
union all
select
    'silver tuple has no edition row' as problem,
    dimension,
    code,
    concat_ws(' | ', value_column, published_label, edition_start, edition_end) as detail
from missing
