-- Fails for two rows of one registry control whose vintage ranges overlap. A control that the registry bridge maps to two published IDs reads one
-- measure under two names (IMM_3_OP_27_FAC_ADHPCT and IMM_3 for C154), so each vintage belongs to one ID: the later ID starts after the earlier ends.
-- An empty start is open to the past and an empty end is open to the future, so two open ranges overlap.
select
    first_row.measure_control,
    first_row.measure_key as first_key,
    first_row.vintage_start as first_start,
    first_row.vintage_end as first_end,
    second_row.measure_key as second_key,
    second_row.vintage_start as second_start,
    second_row.vintage_end as second_end
from {{ ref('registry_measure_sources_bridge') }} as first_row
inner join {{ ref('registry_measure_sources_bridge') }} as second_row
    on
        first_row.measure_control = second_row.measure_control
        and first_row.source_row < second_row.source_row
where
    coalesce(nullif(first_row.vintage_start, ''), '0000') <= coalesce(nullif(second_row.vintage_end, ''), '9999')
    and coalesce(nullif(second_row.vintage_start, ''), '0000') <= coalesce(nullif(first_row.vintage_end, ''), '9999')
