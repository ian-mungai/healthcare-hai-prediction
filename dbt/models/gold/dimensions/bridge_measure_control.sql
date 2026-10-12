-- One row per bridge seed row with a non-empty measure key: the many-to-many of a measure identity (with its value column), a vintage range and a
-- registry control. It is the only gold model where registry controls meet the data. A closed control with an empty measure key stays in
-- dim_registry_control only [827] [744].
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

seed_rows as (
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
    where measure_key is not null and measure_key <> ''
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
),

keyed as (
    select
        measure_control,
        vintage_start,
        vintage_end,
        source_seed,
        source_row,
        md5(concat_ws('|', fact, measure_key, value_column)) as measure_sk,
        md5(measure_control) as registry_control_sk
    from seed_rows
)

select
    measure_sk,
    registry_control_sk,
    measure_control,
    vintage_start,
    vintage_end,
    source_seed,
    source_row
from keyed
