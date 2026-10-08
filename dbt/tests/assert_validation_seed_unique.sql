-- Fails for a validation seed row repeated for one control and published measure ID, so no control is mapped twice [511].
select
    measure_control,
    source_measure_id
from {{ ref('validation_measures') }}
group by
    measure_control,
    source_measure_id
having count(*) > 1
