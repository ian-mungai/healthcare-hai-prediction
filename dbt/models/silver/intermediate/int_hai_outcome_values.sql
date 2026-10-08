-- One row per HAI registry control, outcome row (spine hospital-window and type) and field with a published value: the
-- part the seed hai_outcome_measures names, as text and as a number where it is a plain number. A control and its earlier
-- -outcome twin (C269 and C275) carry the same SIR; staging approves no use (AL1 gap review, failure mode 559).
{{ config(materialized='table') }}

with

seed as (
    select
        measure_control,
        hai_type,
        field,
        review_decision,
        original_decision
    from {{ ref('hai_outcome_measures') }}
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
    seed.original_decision,
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
