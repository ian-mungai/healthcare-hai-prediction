-- One row per HAI registry control, outcome row (spine hospital-window and type) and field with a published value: the
-- part the gold control bridge names, as text and as a number where it is a plain number. A control and its earlier
-- -outcome twin (C269 and C275) carry the same SIR; staging approves no use (AL1 gap review, failure mode 559).

with

bridge_rows as (
    -- One row per HAI control and measure of the gold control bridge: the measure key is the type and the published part
    -- (HAI_1_SIR); the SIR's national comparison is the SIR key with the value column compared_to_national. The review decision
    -- comes from the registry control dimension.
    select
        bridge.measure_control,
        control.review_decision,
        measure.value_column,
        regexp_extract(measure.measure_key, '^(HAI_[0-9]+)_', 1) as hai_type,
        regexp_extract(measure.measure_key, '^HAI_[0-9]+_(.+)$', 1) as part
    from {{ ref('feature_bridge_hai_outcome_measures') }} as bridge
    inner join {{ ref('dim_measure') }} as measure on bridge.measure_sk = measure.measure_sk
    inner join {{ ref('dim_registry_control') }} as control on bridge.registry_control_sk = control.registry_control_sk
),

seed as (
    -- Each part maps to its outcome field.
    select
        measure_control,
        hai_type,
        review_decision,
        case
            when value_column = 'compared_to_national' then 'sir_compared_to_national'
            when part = 'SIR' then 'sir'
            when part = 'CILOWER' then 'ci_lower'
            when part = 'CIUPPER' then 'ci_upper'
            when part = 'NUMERATOR' then 'observed'
            when part = 'ELIGCASES' then 'predicted'
        end as field
    from bridge_rows
),

outcome_values as (
    {%- for field in ['sir', 'observed', 'predicted', 'ci_lower', 'ci_upper'] %}
    select
        outcome_key,
        hai_type,
        '{{ field }}' as field,
        {{ field }}_text as value_text,
        {{ field }} as value_number
    from {{ ref('int_spine_hai_outcomes') }}
    union all
    {%- endfor %}
    select
        outcome_key,
        hai_type,
        'sir_compared_to_national' as field,
        sir_compared_to_national as value_text,
        cast(null as double) as value_number
    from {{ ref('int_spine_hai_outcomes') }}
)

select
    seed.measure_control,
    seed.review_decision,
    outcome_values.outcome_key,
    outcome_values.field,
    outcome_values.value_text,
    outcome_values.value_number,
    seed.measure_control || ':' || outcome_values.field || ':' || outcome_values.outcome_key as outcome_value_key
from seed
inner join outcome_values
    on
        seed.hai_type = outcome_values.hai_type
        and seed.field = outcome_values.field
where nullif(trim(outcome_values.value_text), '') is not null
