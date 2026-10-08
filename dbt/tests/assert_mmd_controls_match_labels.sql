-- Fails for an MMD row whose condition label the reviewed condition map does not name, and for a file named for one
-- control that holds another control's label [468].
select
    mmd_row_key,
    condition_label,
    file_control,
    measure_control
from {{ ref('int_mmd_prevalence') }}
where
    measure_control is null
    or (file_control is not null and file_control <> measure_control)
