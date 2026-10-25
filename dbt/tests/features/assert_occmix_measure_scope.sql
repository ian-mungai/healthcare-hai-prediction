select measure_key
from {{ ref('int_occmix_measures') }}
where
    (measure_control = 'C043' and value_number is not null)
    or (hold_reason is not null and value_number is not null)
    or (source_status = 'unavailable_source_definition' and value_number is not null)
