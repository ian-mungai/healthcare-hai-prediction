-- Fails when int_spine_county_measures would read other county health or geography controls, measures or value columns than the retired seeds
-- county_health_measures and geography_measures gave at e918ed7, or other mmd conditions than the retired seed mmd_conditions (frozen_county_controls),
-- also when a control is redirected to another measure of the same fact and the count stays the same. The tuples are retired seed, control, fact, measure
-- key and value column; for mmd_conditions they are control, fact, condition label, value column, condition code and geography level.
with

frozen as (
    select
        retired_seed,
        measure_control,
        fact,
        measure_key,
        value_column
    from {{ ref('frozen_county_controls') }}
    where retired_seed in ('county_health_measures', 'geography_measures')
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
        select * from {{ ref('feature_bridge_county_health_measures') }}
        union all
        select * from {{ ref('feature_bridge_geography_measures') }}
    ) as bridge
    inner join {{ ref('dim_measure') }} as measure on bridge.measure_sk = measure.measure_sk
),

frozen_conditions as (
    select
        measure_control,
        fact,
        measure_key,
        value_column,
        condition_code,
        geography_level
    from {{ ref('frozen_county_controls') }}
    where retired_seed = 'mmd_conditions'
),

conditions as (
    -- What the model reads: the value input of each condition of the recipe, joined to the prevalence fact by its label.
    select
        measure_control,
        fact,
        key_value as measure_key,
        value_column,
        condition_code,
        geography_level
    from {{ ref('mmd_conditions_recipe') }}
    where key_column = 'condition_label'
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
union all
select
    'mmd condition in the recipe differs from the frozen identity' as failure,
    measure_control || ' ' || fact || ' ' || measure_key || ' ' || value_column || ' ' || condition_code || ' ' || geography_level as detail
from (
    select * from conditions
    except
    select * from frozen_conditions
) as conditions_without_frozen
union all
select
    'frozen mmd condition missing from the recipe' as failure,
    measure_control || ' ' || fact || ' ' || measure_key || ' ' || value_column || ' ' || condition_code || ' ' || geography_level as detail
from (
    select * from frozen_conditions
    except
    select * from conditions
) as frozen_without_conditions
