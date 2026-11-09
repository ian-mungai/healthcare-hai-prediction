-- Fails for each Care Compare derived row that is missing or has the wrong value (design 2.11). The log of C143 (the minutes of OP_18b) is the natural log
-- of the score and null when the score is not above 0. The ordinal of C141 (emergency department volume) maps the category low, medium, high and very high to
-- 1, 2, 3 and 4 and is null for any other value. The expected values come from the parent rows of the same spine key and literal rules.

with

parents as (
    select
        spine_key,
        measure_control,
        value_number,
        value_category,
        case measure_control
            when 'C143' then 'value_log'
            else 'value_ordinal'
        end as derived_field,
        case measure_control
            when 'C143' then case when value_number > 0 then ln(value_number) end
            else case value_category when 'low' then 1 when 'medium' then 2 when 'high' then 3 when 'very high' then 4 end
        end as expected_value
    from {{ ref('int_spine_care_compare_measures') }}
    where
        field = 'value'
        and measure_control in ('C143', 'C141')
)

select
    parents.spine_key || ':' || parents.measure_control || ':' || parents.derived_field as row_key,
    case
        when derived.alignment_key is null then 'derived row missing'
        else 'derived value differs from the arithmetic of the parent'
    end as failure
from parents
left join {{ ref('int_spine_care_compare_measures') }} as derived
    on
        parents.spine_key = derived.spine_key
        and parents.measure_control = derived.measure_control
        and parents.derived_field = derived.field
where
    derived.alignment_key is null
    or (derived.value_number is null) <> (parents.expected_value is null)
    or abs(derived.value_number - parents.expected_value) > 0.000000001
