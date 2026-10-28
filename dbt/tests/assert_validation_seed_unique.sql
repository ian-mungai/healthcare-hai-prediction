-- Fails for a validation bridge row repeated for one control, fact and published measure ID, so no control is mapped twice.
select
    measure_control,
    fact,
    measure_key
from {{ ref('validation_measures_bridge') }}
group by
    measure_control,
    fact,
    measure_key
having count(*) > 1
