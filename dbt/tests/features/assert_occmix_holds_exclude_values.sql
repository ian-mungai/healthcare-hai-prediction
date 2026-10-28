-- Fails for each occupational-mix measure row that has a value although its survey row is held or its recipe denominator is not positive.
select measures.measure_key
from {{ ref('int_occmix_measures') }} as measures
inner join {{ ref('fct_occupational_mix_survey') }} as survey on measures.survey_row_key = survey.survey_row_key
where
    measures.value_number is not null
    and (
        measures.hold_reason is not null
        or (measures.measure_control = 'C034' and coalesce(survey.rnhr, 0) <= 0)
        or (measures.measure_control in ('C031', 'C032', 'C033') and coalesce(survey.nursehr, 0) <= 0)
    )
