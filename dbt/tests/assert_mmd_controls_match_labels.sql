-- Fails for an MMD condition label that the MMD bridge binds to more than one control, for an MMD control that the bridge binds to more than one
-- label, for an MMD row whose condition label the bridge does not name, and for a file named for one control that holds another control's label.
with

labels as (
    select
        measure_key as condition_label,
        measure_control
    from {{ ref('mmd_conditions_bridge') }}
    where
        fact = 'fct_mmd_prevalence'
        and nullif(measure_key, '') is not null
),

repeated_labels as (
    select
        condition_label,
        cast(null as varchar) as measure_control
    from labels
    group by condition_label
    having count(distinct measure_control) > 1
),

repeated_controls as (
    select
        cast(null as varchar) as condition_label,
        measure_control
    from labels
    group by measure_control
    having count(distinct condition_label) > 1
)

select
    cast(null as varchar) as mmd_row_key,
    condition_label,
    cast(null as varchar) as file_control,
    measure_control
from repeated_labels
union all
select
    cast(null as varchar) as mmd_row_key,
    condition_label,
    cast(null as varchar) as file_control,
    measure_control
from repeated_controls
union all
select
    prevalence.mmd_row_key,
    prevalence.condition_label,
    prevalence.file_control,
    labels.measure_control
from {{ ref('int_mmd_prevalence') }} as prevalence
left join labels
    on prevalence.condition_label = labels.condition_label
where
    labels.measure_control is null
    or (prevalence.file_control is not null and prevalence.file_control <> labels.measure_control)
