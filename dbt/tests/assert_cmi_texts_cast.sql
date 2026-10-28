-- Fails for each CMI row where one of the four operand numbers does not match the published text it is cast from: a
-- non-blank text that gives no number, a number without its text, or a number that differs from the text's value.
with

pairs as (
    select
        cmi_row_key,
        'cases' as field,
        cases_text as published_text,
        cases as typed_value
    from {{ ref('int_cmi_hospital_rows') }}
    union all
    select
        cmi_row_key,
        'relative_weights' as field,
        relative_weights_text as published_text,
        relative_weights as typed_value
    from {{ ref('int_cmi_hospital_rows') }}
    union all
    select
        cmi_row_key,
        'transfer_adjusted_cmi' as field,
        transfer_adjusted_cmi_text as published_text,
        transfer_adjusted_cmi as typed_value
    from {{ ref('int_cmi_hospital_rows') }}
    union all
    select
        cmi_row_key,
        'transfer_adjusted_cases' as field,
        transfer_adjusted_cases_text as published_text,
        transfer_adjusted_cases as typed_value
    from {{ ref('int_cmi_hospital_rows') }}
)

select
    cmi_row_key,
    field,
    published_text,
    typed_value
from pairs
where
    (published_text is null) <> (typed_value is null)
    or typed_value is distinct from try_cast(published_text as double)
