-- Fails when the validation alignment does not carry exactly the controls and measures the retired seed validation_measures gave at e918ed7
-- (frozen_validation_controls; the retired C288.payment_adjustment has no measure and is not carried), also when a control is swapped for
-- another and the count stays the same, and for each hospital-window that does not have exactly one row per control and measure [597] [600].
-- The row checks wait for a model with rows; the lookup check always runs.
with

frozen as (
    select
        measure_control,
        fact,
        measure,
        measure_key,
        value_column
    from {{ ref('frozen_validation_controls') }}
    where is_carried
),

lookup as (
    -- What the model reads: the controls of the gold control bridge by fact, measure key and typed value column.
    select distinct
        bridge.measure_control,
        measure.fact,
        measure.measure_key,
        measure.value_column
    from {{ ref('feature_bridge_validation_measures') }} as bridge
    inner join {{ ref('dim_measure') }} as measure on bridge.measure_sk = measure.measure_sk
),

frozen_lookup as (
    select distinct
        measure_control,
        fact,
        measure_key,
        value_column
    from frozen
),

carried as (
    select distinct
        measure_control,
        measure,
        fact
    from {{ ref('int_spine_validation_measures') }}
),

expected as (
    select distinct
        measure_control,
        measure,
        fact
    from frozen
)

select
    'bridge control or measure differs from the frozen identity' as failure,
    measure_control || ' ' || fact || ' ' || measure_key || ' ' || value_column as detail
from (
    select * from lookup
    except
    select * from frozen_lookup
) as lookup_without_frozen_lookup
union all
select
    'frozen control or measure missing from the bridge' as failure,
    measure_control || ' ' || fact || ' ' || measure_key || ' ' || value_column as detail
from (
    select * from frozen_lookup
    except
    select * from lookup
) as frozen_lookup_without_lookup
union all
select
    'control carried by the model is not frozen' as failure,
    measure_control || ' ' || measure || ' ' || fact as detail
from (
    select * from carried
    except
    select * from expected
) as carried_without_expected
union all
select
    'frozen control missing from the model' as failure,
    measure_control || ' ' || measure || ' ' || fact as detail
from (
    select * from expected
    except
    select * from carried
) as expected_without_carried
where exists (select 1 from {{ ref('int_spine_validation_measures') }})
union all
select
    'control count differs' as failure,
    spine_key as detail
from {{ ref('int_spine_validation_measures') }}
group by spine_key
having count(*) <> (select count(*) from expected) or count(distinct measure_control || ':' || measure) <> count(*)
