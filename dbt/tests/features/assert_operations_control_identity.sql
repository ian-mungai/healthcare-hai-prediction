-- Fails when the operations alignment does not carry exactly the HHS capacity, ONC and ownership controls the retired seeds hhs_onc_measures and
-- ownership_measures gave at e918ed7 (frozen_operations_controls; the two closed HHS mappings C001 and C040 have no field and are not carried),
-- also when a control is swapped for another and the count stays the same; when an HHS field list or the ownership fields differ from the frozen
-- ones; and for each hospital-window that does not have exactly one row per control [587] [595]. The row checks wait for a model with rows; the
-- recipe and bridge check always runs.
with

frozen as (
    select
        measure_source,
        measure_control,
        fact,
        -- ONC fields are read by the recipe contexts, not compared; ownership fields compare as a sorted list.
        case
            when measure_source = 'hhs_capacity' then coalesce(fields, '')
            when measure_source = 'ownership' then array_to_string(list_sort(string_split(fields, ' ')), ' ')
            else ''
        end as field_list
    from {{ ref('frozen_operations_controls') }}
    where is_carried
),

hhs as (
    -- What the model reads: the HHS capacity controls of the recipe, the value or numerator field and then the denominator field.
    select
        'hhs_capacity' as measure_source,
        measure_control,
        'fct_hhs_capacity_week' as fact,
        array_to_string(
            list_filter(
                [
                    max(case when role in ('value', 'numerator') then value_column end),
                    max(case when role = 'denominator' then value_column end),
                ],
                item -> item is not null
            ),
            ' '
        ) as field_list
    from {{ ref('hhs_onc_measures_recipe') }}
    where fact = 'fct_hhs_capacity_week'
    group by
        measure_control,
        source_row
),

onc as (
    select distinct
        'onc' as measure_source,
        measure_control,
        fact,
        '' as field_list
    from {{ ref('hhs_onc_measures_recipe') }}
    where fact = 'fct_onc_ehr_product'
),

ownership as (
    -- The ownership controls of the gold control bridge with the fields they read.
    select
        'ownership' as measure_source,
        bridge.measure_control,
        measure.fact,
        array_to_string(list_sort(list_distinct(list(measure.measure_key))), ' ') as field_list
    from {{ ref('feature_bridge_ownership_measures') }} as bridge
    inner join {{ ref('dim_measure') }} as measure on bridge.measure_sk = measure.measure_sk
    group by
        bridge.measure_control,
        measure.fact
),

lookup as (
    select * from hhs
    union all
    select * from onc
    union all
    select * from ownership
),

carried as (
    select distinct
        measure_source,
        measure_control
    from {{ ref('int_spine_operations_measures') }}
),

expected as (
    select distinct
        measure_source,
        measure_control
    from frozen
)

select
    'recipe or bridge differs from the frozen identity' as failure,
    measure_source || ' ' || measure_control || ' ' || fact || ' ' || field_list as detail
from (
    select * from lookup
    except
    select * from frozen
) as lookup_without_frozen
union all
select
    'frozen identity missing from the recipe or bridge' as failure,
    measure_source || ' ' || measure_control || ' ' || fact || ' ' || field_list as detail
from (
    select * from frozen
    except
    select * from lookup
) as frozen_without_lookup
union all
select
    'control carried by the model is not frozen' as failure,
    measure_source || ' ' || measure_control as detail
from (
    select * from carried
    except
    select * from expected
) as carried_without_expected
union all
select
    'frozen control missing from the model' as failure,
    measure_source || ' ' || measure_control as detail
from (
    select * from expected
    except
    select * from carried
) as expected_without_carried
where exists (select 1 from {{ ref('int_spine_operations_measures') }})
union all
select
    'control count differs' as failure,
    spine_key as detail
from {{ ref('int_spine_operations_measures') }}
group by spine_key
having count(*) <> (select count(*) from expected) or count(distinct measure_source || ':' || measure_control) <> count(*)
