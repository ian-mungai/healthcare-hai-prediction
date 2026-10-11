-- One row per HAC Reduction or VBP registry control, field, hospital and program fiscal year: the published value of
-- exactly the field validation_measures names, and a number only when it is a plain number; tokens and the reviewed odd
-- value stay text. C288.payment_adjustment has no field and no rows (failure modes 518 to 520).
{{ config(materialized='table') }}

with

seed as (
    select
        measure_control,
        source_model,
        source_measure_id,
        review_decision
    from {{ ref('validation_measures') }}
    where
        source_model in ('int_hac_program_years', 'int_vbp_program_years')
        and nullif(source_measure_id, '') is not null
),

program_values as (
    select
        ccn,
        fiscal_year,
        release_date,
        member_sha256,
        'int_hac_program_years' as source_model,
        'payment_reduction' as field,
        payment_reduction_published as value_text
    from {{ ref('int_hac_program_years') }}
    union all
    select
        ccn,
        fiscal_year,
        release_date,
        member_sha256,
        'int_hac_program_years' as source_model,
        'total_hac_score' as field,
        total_hac_score as value_text
    from {{ ref('int_hac_program_years') }}
    {%- for field in ['unweighted_normalized_safety_domain_score', 'weighted_safety_domain_score', 'total_performance_score'] %}
    union all
    select
        ccn,
        fiscal_year,
        release_date,
        member_sha256,
        'int_vbp_program_years' as source_model,
        '{{ field }}' as field,
        {{ field }} as value_text
    from {{ ref('int_vbp_program_years') }}
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
        seed.source_model = program_values.source_model
        and seed.source_measure_id = program_values.field
where nullif(trim(program_values.value_text), '') is not null
