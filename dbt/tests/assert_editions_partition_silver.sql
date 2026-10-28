-- The editions of a split key never overlap, and every silver value of the key (its label at an ordering value) falls in exactly one edition. This is the
-- check that the range join of a fact finds exactly one edition for each of its rows.
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

pairs as (
    select distinct
        silver_labels.dimension,
        silver_labels.code_system,
        silver_labels.code,
        silver_labels.fact,
        silver_labels.axis
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
),

editions as (
    select
        'dim_rurality_code' as dimension,
        code_system,
        code,
        '' as fact,
        '' as value_column,
        edition_start,
        edition_end
    from {{ ref('dim_rurality_code') }}
    where edition_start is not null
    union all
    select
        'dim_measure' as dimension,
        '' as code_system,
        measure_key as code,
        fact,
        value_column,
        edition_start,
        edition_end
    from {{ ref('dim_measure') }}
    where edition_start is not null
),

covered as (
    select
        pairs.dimension,
        pairs.code,
        pairs.axis,
        editions.value_column,
        count(distinct editions.edition_start) as edition_count
    from pairs
    left join editions
        on
            pairs.dimension = editions.dimension
            and pairs.fact = editions.fact
            and pairs.code = editions.code
            and (pairs.dimension = 'dim_measure' or pairs.code_system = editions.code_system)
            and pairs.axis >= editions.edition_start
            and pairs.axis <= editions.edition_end
    group by pairs.dimension, pairs.code, pairs.axis, editions.value_column
)

select
    'silver value is in no edition or in several editions' as problem,
    dimension,
    code,
    axis as detail
from covered
where edition_count <> 1
union all
select
    'editions of one key overlap' as problem,
    first_edition.dimension,
    first_edition.code,
    first_edition.edition_start || ' and ' || second_edition.edition_start as detail
from editions as first_edition
inner join editions as second_edition
    on
        first_edition.dimension = second_edition.dimension
        and first_edition.code_system = second_edition.code_system
        and first_edition.fact = second_edition.fact
        and first_edition.code = second_edition.code
        and first_edition.value_column = second_edition.value_column
        and first_edition.edition_start < second_edition.edition_start
        and first_edition.edition_end >= second_edition.edition_start
