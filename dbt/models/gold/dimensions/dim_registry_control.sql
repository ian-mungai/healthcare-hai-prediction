-- One row per registry control of revision 3 (the base registry and its append-only additions), plus the unknown member. The review decision is
-- read from the control bridge seeds and is null for a control with no bridge row. The family is the analytic domain of the control (Structure,
-- Process, Outcome, Context or Linkage) and the closeout decision is one of dropped, retired, redefined, narrowed or id_assigned (null for a control with
-- no closeout decision); both are generated into the registry_controls seed, and the registry keeps the raw closeout text [824] [827].
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

reviews as (
    select distinct
        measure_control,
        review_decision
    from (
        {%- for bridge_seed in bridge_seeds %}
        select
            measure_control,
            review_decision
        from {{ ref(bridge_seed) }}
        where review_decision is not null
        {%- if not loop.last %}
        union all
        {%- endif %}
        {%- endfor %}
    ) as bridge_rows
),

final as (
    select
        md5(registry_controls.measure_control) as registry_control_sk,
        registry_controls.measure_control,
        registry_controls.parent_control as parent,
        registry_controls.family,
        registry_controls.closeout_decision,
        reviews.review_decision
    from {{ ref('registry_controls') }} as registry_controls
    left join reviews
        on registry_controls.measure_control = reviews.measure_control
    union all
    select
        md5('<unknown>') as registry_control_sk,
        '<unknown>' as measure_control,
        cast(null as varchar) as parent,
        cast(null as varchar) as family,
        cast(null as varchar) as closeout_decision,
        cast(null as varchar) as review_decision
)

select * from final
