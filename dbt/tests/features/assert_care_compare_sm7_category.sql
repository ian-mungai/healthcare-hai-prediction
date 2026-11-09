-- Fails when a category does not follow the reviewed mapping of the published SM_7 text (C167): Yes, No and Not Applicable are categories and Not Available
-- stays missing. The mapping is written out again here, not read from the macro, so a changed macro fails. Another control (C141 apart) has no category.
with

reviewed as (
    select
        fixed.value_text,
        fixed.expected_category
    from (
        values
        ('Yes', 'yes'),
        ('No', 'no'),
        ('Not Applicable (our hospital does not provide inpatient labor/delivery care)', 'not applicable'),
        ('Not Available', cast(null as varchar))
    ) as fixed (value_text, expected_category)
)

select
    measures.alignment_key,
    measures.value_text,
    measures.value_category,
    reviewed.expected_category
from {{ ref('int_spine_care_compare_measures') }} as measures
left join reviewed on measures.value_text = reviewed.value_text
where
    (measures.measure_control = 'C167' and measures.value_category is distinct from reviewed.expected_category)
    or (measures.measure_control not in ('C141', 'C167') and measures.value_category is not null)
