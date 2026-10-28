-- Fails when int_hai_outcome_values would read other HAI controls, infection types or outcome fields than the retired seed hai_outcome_measures gave
-- at e918ed7 (frozen_hai_outcome_controls), also when a control is swapped for another and the count stays the same [559]. The tuples are derived from
-- the gold control bridge exactly as the model derives them: the measure key is the infection type and the published part, and the national
-- comparison of the SIR is the SIR key with the value column compared_to_national.
with

frozen as (
    select
        measure_control,
        hai_type,
        field
    from {{ ref('frozen_hai_outcome_controls') }}
),

bridge_rows as (
    select
        bridge.measure_control,
        measure.value_column,
        regexp_extract(measure.measure_key, '^(HAI_[0-9]+)_', 1) as hai_type,
        regexp_extract(measure.measure_key, '^HAI_[0-9]+_(.+)$', 1) as part
    from {{ ref('feature_bridge_hai_outcome_measures') }} as bridge
    inner join {{ ref('dim_measure') }} as measure on bridge.measure_sk = measure.measure_sk
),

lookup as (
    select distinct
        measure_control,
        hai_type,
        case
            when value_column = 'compared_to_national' then 'sir_compared_to_national'
            when part = 'SIR' then 'sir'
            when part = 'CILOWER' then 'ci_lower'
            when part = 'CIUPPER' then 'ci_upper'
            when part = 'NUMERATOR' then 'observed'
            when part = 'ELIGCASES' then 'predicted'
        end as field
    from bridge_rows
)

select
    'bridge control, type or field differs from the frozen identity' as failure,
    measure_control || ' ' || hai_type || ' ' || coalesce(field, 'null') as detail
from (
    select * from lookup
    except
    select * from frozen
) as lookup_without_frozen
union all
select
    'frozen control, type or field missing from the bridge' as failure,
    measure_control || ' ' || hai_type || ' ' || coalesce(field, 'null') as detail
from (
    select * from frozen
    except
    select * from lookup
) as frozen_without_lookup
