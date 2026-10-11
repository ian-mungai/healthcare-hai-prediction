-- Fails for a gold bridge identity that appears more than once across the bridge seeds: the same fact, measure key, value column and
-- vintage start for one registry control [802].
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
    fact,
    measure_key,
    value_column,
    vintage_start,
    measure_control,
    count(*) as bridge_rows
from bridge
group by
    fact,
    measure_key,
    value_column,
    vintage_start,
    measure_control
having count(*) > 1
