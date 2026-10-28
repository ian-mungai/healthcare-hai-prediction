-- Fails for each gold bridge row whose registry control is not in the registry controls: the base registry of revision 3 or its
-- append-only additions (C258.80 and C258.81 are additions).
{%- set bridge_seeds = [
    'registry_measure_sources_bridge',
    'cost_report_measures_bridge',
    'impact_measures_bridge',
    'mup_measures_bridge',
    'occmix_measures_bridge',
    'validation_measures_bridge',
    'hai_outcome_measures_bridge',
    'hhs_onc_measures_bridge',
    'ownership_measures_bridge',
    'county_health_measures_bridge',
    'acs_svi_measures_bridge',
    'income_labor_measures_bridge',
    'geography_measures_bridge',
    'mmd_conditions_bridge',
    'shortage_measures_bridge',
] %}

with

bridge as (
    {%- for bridge_seed in bridge_seeds %}
    select
        fact,
        measure_key,
        value_column,
        vintage_start,
        vintage_end,
        measure_control,
        source_seed,
        source_row
    from {{ ref(bridge_seed) }}
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
)

select
    bridge.measure_control,
    bridge.fact,
    bridge.measure_key,
    bridge.source_seed,
    bridge.source_row
from bridge
left join {{ ref('registry_controls') }} as registry
    on bridge.measure_control = registry.measure_control
where registry.measure_control is null
