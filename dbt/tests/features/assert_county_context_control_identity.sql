-- Fails when int_spine_county_context would read other ACS and SVI, income and labor, or shortage controls, measures or value columns than the retired seeds
-- acs_svi_measures, income_labor_measures and shortage_measures gave at e918ed7 (frozen_county_controls), also when a control is redirected to another
-- measure of the same fact and the count stays the same. The tuples are retired seed, control, fact, measure key and value column.
with

frozen as (
    select
        retired_seed,
        measure_control,
        fact,
        measure_key,
        value_column
    from {{ ref('frozen_county_controls') }}
    where retired_seed in ('acs_svi_measures', 'income_labor_measures', 'shortage_measures')
),

lookup as (
    -- What the model reads: the controls of the gold control bridge by fact, measure key and typed value column.
    select distinct
        bridge.source_seed as retired_seed,
        bridge.measure_control,
        measure.fact,
        measure.measure_key,
        measure.value_column
    from (
        select * from {{ ref('feature_bridge_acs_svi_measures') }}
        union all
        select * from {{ ref('feature_bridge_income_labor_measures') }}
        union all
        select * from {{ ref('feature_bridge_shortage_measures') }}
    ) as bridge
    inner join {{ ref('dim_measure') }} as measure on bridge.measure_sk = measure.measure_sk
)

select
    'county lookup differs from the frozen identity' as failure,
    retired_seed || ' ' || measure_control || ' ' || fact || ' ' || measure_key || ' ' || value_column as detail
from (
    select * from lookup
    except
    select * from frozen
) as lookup_without_frozen
union all
select
    'frozen county lookup row missing from the bridge' as failure,
    retired_seed || ' ' || measure_control || ' ' || fact || ' ' || measure_key || ' ' || value_column as detail
from (
    select * from frozen
    except
    select * from lookup
) as frozen_without_lookup
