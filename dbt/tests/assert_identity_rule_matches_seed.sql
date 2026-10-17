-- Every row of dim_drg, dim_rurality_code and dim_measure carries exactly the identity rule that the approved identity_splits seed gives its key: a held
-- key stays held, a renamed key stays single_latest, a split key stays split, and a key the seed does not list has no rule. A null, substituted or invented
-- rule fails, whatever the row count is: a held DRG presented as renamed keeps one row and the latest label, so the count test cannot see it [940].
with

seed_rules as (
    select
        dimension,
        code,
        identity_rule,
        case when dimension = 'dim_measure' then '' else code_system end as code_system,
        coalesce(fact, '') as fact
    from {{ ref('identity_splits') }}
),

model_rules as (
    select
        'dim_drg' as dimension,
        code_system,
        '' as fact,
        code,
        identity_rule
    from {{ ref('dim_drg') }}
    where drg_sk <> md5('<unknown>')
    union all
    select
        'dim_rurality_code' as dimension,
        code_system,
        '' as fact,
        code,
        identity_rule
    from {{ ref('dim_rurality_code') }}
    where rurality_code_sk <> md5('<unknown>')
    union all
    select
        'dim_measure' as dimension,
        '' as code_system,
        fact,
        measure_key as code,
        identity_rule
    from {{ ref('dim_measure') }}
    where measure_sk <> md5('<unknown>')
)

select distinct
    model_rules.dimension,
    model_rules.code,
    seed_rules.identity_rule as approved_rule,
    model_rules.identity_rule as model_rule
from model_rules
left join seed_rules
    on
        model_rules.dimension = seed_rules.dimension
        and model_rules.code_system = seed_rules.code_system
        and model_rules.fact = seed_rules.fact
        and model_rules.code = seed_rules.code
where model_rules.identity_rule is distinct from seed_rules.identity_rule
