-- Fails for each HAI window that is both selected and held: a held conflict never feeds the window models [232].
with

selected as (
    select
        'cms_hai_hospital' as bronze_table,
        entity_id,
        measure_id,
        window_start,
        window_end
    from {{ ref('int_hai_hospital_windows') }}
    union all
    select
        'cms_hai_state' as bronze_table,
        entity_id,
        measure_id,
        window_start,
        window_end
    from {{ ref('int_hai_state_windows') }}
    union all
    select
        'cms_hai_national' as bronze_table,
        entity_id,
        measure_id,
        window_start,
        window_end
    from {{ ref('int_hai_national_windows') }}
)

select
    selected.bronze_table,
    selected.entity_id,
    selected.measure_id,
    selected.window_start,
    selected.window_end
from selected
inner join {{ ref('int_hai_window_holds') }} as holds
    on
        selected.bronze_table = holds.bronze_table
        and selected.entity_id = holds.entity_id
        and selected.measure_id = holds.measure_id
        and selected.window_start = holds.window_start
        and selected.window_end = holds.window_end
