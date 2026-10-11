-- Fails for a gold bridge row with no fact, control, source seed or source row, for a row with no review decision (the registry holds none
-- for the HAI outcome twins, so those rows may stay empty), for a measure key without a value column or the other way round, for a vintage that
-- starts after it ends, for an empty mapping (no key and no column) that is not on the allowed list, and for an allowed entry with no empty row [801].
-- Allowed empty mappings, each a control whose registry decision leaves no data column:
-- C001: the HHS part is closed in registry revision 3 (STG-008); no columns.
-- C040: the HHS part is closed in registry revision 3 (STG-008); no columns.
-- C043: closed as unavailable from public sources (STG-007).
-- C114: held in the registry; no value.
-- C115: held in the registry; no value.
-- C116: held in the registry; no value.
-- C118: closed as not a predictor; no value.
-- C258: parent of the MMD condition controls; each condition control carries its label. The exception is bound to one row: origin
-- shortage_measures row 4, decision hold_definition, no vintage; a keyless C258 row of any other origin, decision or vintage fails, and so does a
-- second keyless C258 row (exactly one across all vintages).
-- C288.payment_adjustment: retired; no field.
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
        review_decision,
        source_seed,
        source_row
    from {{ ref(bridge_seed) }}
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
),

allowed_empty as (
    select
        allowed.measure_control,
        allowed.fact
    from (
        values
        ('C001', 'fct_hhs_capacity_week'),
        ('C040', 'fct_hhs_capacity_week'),
        ('C043', 'fct_occupational_mix_survey'),
        ('C114', 'fct_medicare_inpatient_provider'),
        ('C115', 'fct_medicare_inpatient_provider'),
        ('C116', 'fct_medicare_inpatient_provider'),
        ('C118', 'fct_medicare_inpatient_provider'),
        ('C258', 'fct_mmd_prevalence'),
        ('C288.payment_adjustment', 'fct_vbp_program_year')
    ) as allowed (measure_control, fact)
),

keyless_parent as (
    select count(*) as row_count
    from bridge
    where
        bridge.measure_control = 'C258'
        and bridge.fact = 'fct_mmd_prevalence'
        and nullif(bridge.measure_key, '') is null
        and nullif(bridge.value_column, '') is null
),

bad_rows as (
    select
        bridge.fact,
        bridge.measure_key,
        bridge.value_column,
        bridge.vintage_start,
        bridge.vintage_end,
        bridge.measure_control,
        bridge.review_decision,
        bridge.source_seed,
        bridge.source_row
    from bridge
    left join allowed_empty
        on
            bridge.measure_control = allowed_empty.measure_control
            and bridge.fact = allowed_empty.fact
    cross join keyless_parent
    where
        nullif(bridge.fact, '') is null
        or nullif(bridge.measure_control, '') is null
        or nullif(bridge.source_seed, '') is null
        or bridge.source_row is null
        or (nullif(bridge.review_decision, '') is null and bridge.source_seed <> 'hai_outcome_measures')
        or (nullif(bridge.measure_key, '') is null) <> (nullif(bridge.value_column, '') is null)
        or (
            nullif(bridge.vintage_start, '') is not null
            and nullif(bridge.vintage_end, '') is not null
            and bridge.vintage_start > bridge.vintage_end
        )
        or (nullif(bridge.measure_key, '') is null and nullif(bridge.value_column, '') is null and allowed_empty.measure_control is null)
        or (
            nullif(bridge.measure_key, '') is null
            and nullif(bridge.value_column, '') is null
            and bridge.measure_control = 'C258'
            and bridge.fact = 'fct_mmd_prevalence'
            and (
                keyless_parent.row_count <> 1
                or bridge.source_seed <> 'shortage_measures'
                or bridge.source_row <> 4
                or bridge.review_decision <> 'hold_definition'
                or nullif(bridge.vintage_start, '') is not null
                or nullif(bridge.vintage_end, '') is not null
            )
        )
),

stale_entries as (
    select
        allowed_empty.fact,
        cast(null as varchar) as measure_key,
        cast(null as varchar) as value_column,
        cast(null as varchar) as vintage_start,
        cast(null as varchar) as vintage_end,
        allowed_empty.measure_control,
        cast(null as varchar) as review_decision,
        cast(null as varchar) as source_seed,
        cast(null as integer) as source_row
    from allowed_empty
    left join bridge
        on
            allowed_empty.measure_control = bridge.measure_control
            and allowed_empty.fact = bridge.fact
            and nullif(bridge.measure_key, '') is null
            and nullif(bridge.value_column, '') is null
    where bridge.measure_control is null
)

select * from bad_rows
union all
select * from stale_entries
