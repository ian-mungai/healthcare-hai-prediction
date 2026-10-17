-- Each key of identity_splits has the identity count its rule gives: one row per distinct label for a split key and one row for a renamed or held key
-- (one per value column for a measure). Which label and dates each split row carries is checked by assert_edition_bounds_match_silver, and the rule each row
-- records by assert_identity_rule_matches_seed. A seed key that the loaded silver rows do not carry is not checked: the multi-label test finds a mistyped
-- key through the key it should have named [940] [941].
with

silver_labels as (
    {{ gold_silver_label_rows() }}
),

silver_keys as (
    select
        dimension,
        code_system,
        fact,
        code,
        count(distinct published_label) as label_count
    from silver_labels
    group by dimension, code_system, fact, code
),

seeded as (
    select
        identity_splits.dimension,
        identity_splits.code_system,
        identity_splits.code,
        identity_splits.identity_rule,
        silver_keys.label_count,
        coalesce(identity_splits.fact, '') as fact,
        case when identity_splits.identity_rule = 'split' then silver_keys.label_count else 1 end as expected_rows
    from {{ ref('identity_splits') }} as identity_splits
    left join silver_keys
        on
            identity_splits.dimension = silver_keys.dimension
            and identity_splits.code_system = silver_keys.code_system
            and coalesce(identity_splits.fact, '') = silver_keys.fact
            and identity_splits.code = silver_keys.code
),

code_rows as (
    select
        'dim_drg' as dimension,
        code_system,
        code,
        count(*) as actual_rows
    from {{ ref('dim_drg') }}
    where drg_sk <> md5('<unknown>')
    group by code_system, code
    union all
    select
        'dim_rurality_code' as dimension,
        code_system,
        code,
        count(*) as actual_rows
    from {{ ref('dim_rurality_code') }}
    where rurality_code_sk <> md5('<unknown>')
    group by code_system, code
),

measure_rows as (
    select
        fact,
        measure_key,
        value_column,
        count(*) as actual_rows
    from {{ ref('dim_measure') }}
    where measure_sk <> md5('<unknown>')
    group by fact, measure_key, value_column
)

select
    'row count differs from the rule' as problem,
    seeded.dimension,
    seeded.code,
    seeded.identity_rule || ': expected ' || seeded.expected_rows || ', found ' || coalesce(code_rows.actual_rows, 0) as detail
from seeded
left join code_rows
    on
        seeded.dimension = code_rows.dimension
        and seeded.code_system = code_rows.code_system
        and seeded.code = code_rows.code
where seeded.dimension <> 'dim_measure' and seeded.label_count is not null and seeded.expected_rows is distinct from coalesce(code_rows.actual_rows, 0)
union all
select
    'measure has no identity' as problem,
    seeded.dimension,
    seeded.code,
    seeded.identity_rule as detail
from seeded
left join measure_rows
    on seeded.fact = measure_rows.fact and seeded.code = measure_rows.measure_key
where seeded.dimension = 'dim_measure' and seeded.label_count is not null and measure_rows.fact is null
union all
select
    'row count differs from the rule' as problem,
    seeded.dimension,
    seeded.code,
    seeded.identity_rule || ' ' || measure_rows.value_column || ': expected ' || seeded.expected_rows || ', found ' || measure_rows.actual_rows as detail
from seeded
inner join measure_rows
    on seeded.fact = measure_rows.fact and seeded.code = measure_rows.measure_key
where seeded.dimension = 'dim_measure' and seeded.label_count is not null and seeded.expected_rows <> measure_rows.actual_rows
