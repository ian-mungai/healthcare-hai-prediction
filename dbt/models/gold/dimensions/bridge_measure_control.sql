-- One row per bridge seed row with a non-empty measure key: the many-to-many of a measure identity (with its value column), a vintage range and a
-- registry control. A seed row of a measure that the identity_splits seed lists as split maps to every edition of the measure when its vintage range is
-- empty and to the editions whose years overlap the range otherwise. It is the only gold model where registry controls meet the data. A closed control
-- with an empty measure key stays in dim_registry_control only.
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

editions as (
    select
        fact,
        measure_key,
        value_column,
        edition_start,
        edition_end
    from {{ ref('dim_measure') }}
    where edition_start is not null
),

keyed as (
    {{ gold_measure_control_rows('seed_rows', 'editions') }}
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
