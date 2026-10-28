-- One row per HAC Reduction or VBP registry control, field, hospital and program fiscal year: the published value of
-- exactly the field validation_measures names, and a number only when it is a plain number; tokens and the reviewed odd
-- value stay text. C288.payment_adjustment has no field and no rows (failure modes 518 to 520).

with

seed as (
    -- The HAC Reduction and VBP controls of the gold control bridge, by fact and measure key (the gold column the registry field is
    -- published in); the review decision comes from the registry control dimension.
    select
        bridge.measure_control,
        measure.fact,
        measure.measure_key,
        control.review_decision
    from {{ ref('feature_bridge_validation_measures') }} as bridge
    inner join {{ ref('dim_measure') }} as measure on bridge.measure_sk = measure.measure_sk
    inner join {{ ref('dim_registry_control') }} as control on bridge.registry_control_sk = control.registry_control_sk
    where measure.fact in ('fct_hac_program_year', 'fct_vbp_program_year')
),

program_values as (
    select
        ccn,
        fiscal_year,
        release_date,
        member_sha256,
        'fct_hac_program_year' as fact,
        'payment_reduction' as field,
        'payment_reduction_published' as measure_key,
        payment_reduction_published as value_text
    from {{ ref('fct_hac_program_year') }}
    union all
    select
        ccn,
        fiscal_year,
        release_date,
        member_sha256,
        'fct_hac_program_year' as fact,
        'total_hac_score' as field,
        'total_hac_score_number' as measure_key,
        total_hac_score as value_text
    from {{ ref('fct_hac_program_year') }}
    {%- for field in ['unweighted_normalized_safety_domain_score', 'weighted_safety_domain_score', 'total_performance_score'] %}
    union all
    select
        ccn,
        fiscal_year,
        release_date,
        member_sha256,
        'fct_vbp_program_year' as fact,
        '{{ field }}' as field,
        '{{ field }}_number' as measure_key,
        {{ field }} as value_text
    from {{ ref('fct_vbp_program_year') }}
    {%- endfor %}
)

select
    seed.measure_control,
    seed.review_decision,
    program_values.ccn,
    program_values.fiscal_year,
    program_values.field,
    program_values.value_text,
    program_values.release_date,
    program_values.member_sha256,
    {{ strict_number('program_values.value_text') }} as value_number,
    seed.measure_control || ':' || program_values.field || ':' || program_values.ccn || ':' || program_values.fiscal_year
        as program_value_key
from seed
inner join program_values
    on
        seed.fact = program_values.fact
        and seed.measure_key = program_values.measure_key
where nullif(trim(program_values.value_text), '') is not null
