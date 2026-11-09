-- Fails for each C167 text outside the accepted values, so a new SM_7 text is mapped on purpose, never folded.
select
    alignment_key,
    value_text
from {{ ref('int_spine_care_compare_measures') }}
where
    measure_control = 'C167'
    and value_text is not null
    and value_text not in {{ sm7_published_texts() }}
