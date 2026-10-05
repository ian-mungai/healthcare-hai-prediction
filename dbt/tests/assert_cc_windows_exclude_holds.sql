-- Fails for each Care Compare window that is both selected and held: a held conflict never feeds the window models [322].
with

selected as (
    select
        'cms_cc_timely_and_effective_care_hospital' as bronze_table,
        entity_id,
        measure_id,
        window_start,
        window_end
    from {{ ref('int_cc_timely_effective_windows') }}
    union all
    select
        'cms_cc_maternal_health_hospital' as bronze_table,
        entity_id,
        measure_id,
        window_start,
        window_end
    from {{ ref('int_cc_maternal_windows') }}
    union all
    select
        'cms_cc_hcahps_hospital' as bronze_table,
        entity_id,
        measure_id,
        window_start,
        window_end
    from {{ ref('int_cc_hcahps_windows') }}
)

select
    selected.bronze_table,
    selected.entity_id,
    selected.measure_id,
    selected.window_start,
    selected.window_end
from selected
inner join {{ ref('int_cc_window_holds') }} as holds
    on
        selected.bronze_table = holds.bronze_table
        and selected.entity_id = holds.entity_id
        and selected.measure_id = holds.measure_id
        and selected.window_start = holds.window_start
        and selected.window_end = holds.window_end
