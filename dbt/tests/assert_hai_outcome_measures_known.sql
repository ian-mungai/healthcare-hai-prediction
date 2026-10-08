-- Fails for each calendar-year HAI hospital measure ID that is not one of the six parts of the six types, so no part is
-- lost or read from the wrong row [550].
select
    entity_id,
    measure_id,
    window_start
from {{ ref('int_hai_hospital_windows') }}
where
    month(window_start) = 1
    and day(window_start) = 1
    and window_end = make_date(year(window_start), 12, 31)
    and measure_id not in (
        {%- for hai_type in hai_outcome_types() %}
        {%- set outer_last = loop.last %}
        {%- for part, column in hai_outcome_parts() %}
        '{{ hai_type }}_{{ part }}'{% if not (outer_last and loop.last) %},{% endif %}
        {%- endfor %}
        {%- endfor %}
    )
