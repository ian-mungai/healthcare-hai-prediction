-- Fails for each C141 text outside the accepted spellings, so a new spelling is mapped on purpose, never folded [569].
select
    alignment_key,
    value_text
from {{ ref('int_spine_care_compare_measures') }}
where
    measure_control = 'C141'
    and value_text is not null
    and value_text not in {{ edv_published_texts() }}
