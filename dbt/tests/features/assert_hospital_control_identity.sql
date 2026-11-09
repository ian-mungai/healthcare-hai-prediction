-- Fails when the hospital finance and operations alignment does not carry exactly the source, control and field rows the retired seeds
-- cost_report_measures, impact_measures, mup_measures and occmix_measures gave at e918ed7 (frozen_hospital_controls, 98 rows), also when a control is
-- swapped for another and the count stays the same; when a formula recipe differs from the frozen rule and columns; and for each hospital-window
-- that does not have exactly one row per source, control and field [576] [582]. The row checks wait for a model with rows; the recipe check always runs.
with

frozen as (
    select
        measure_source,
        measure_control,
        field,
        coalesce(rule, '') as recipe_rule,
        coalesce(value_column, '') as value_column,
        coalesce(denominator_column, '') as denominator_column,
        coalesce(drg_codes, '') as drg_codes
    from {{ ref('frozen_hospital_controls') }}
),

recipes as (
    -- What the model reads: one row per recipe input group, with the value or numerator column, the denominator column and, for a DRG share,
    -- the DRG codes as a space-separated list.
    select
        'cost_report' as measure_source,
        measure_control,
        'value' as field,
        rule as recipe_rule,
        coalesce(max(case when role in ('value', 'numerator') then value_column end), '') as value_column,
        coalesce(max(case when role = 'denominator' then value_column end), '') as denominator_column,
        '' as drg_codes
    from {{ ref('cost_report_measures_recipe') }}
    group by
        measure_control,
        source_row,
        rule
    union all
    select
        'ipps_impact' as measure_source,
        measure_control,
        coalesce(max(case when role in ('value', 'numerator') then key_value end), 'none') as field,
        rule as recipe_rule,
        coalesce(max(case when role in ('value', 'numerator') then key_value end), '') as value_column,
        coalesce(max(case when role = 'denominator' then key_value end), '') as denominator_column,
        '' as drg_codes
    from {{ ref('impact_measures_recipe') }}
    group by
        measure_control,
        source_row,
        rule
    union all
    select
        'medicare_inpatient' as measure_source,
        measure_control,
        coalesce(max(case when role in ('value', 'numerator') then value_column end), 'none') as field,
        rule as recipe_rule,
        coalesce(max(case when role in ('value', 'numerator') then value_column end), '') as value_column,
        coalesce(max(case when role = 'denominator' then value_column end), '') as denominator_column,
        coalesce(max(case when rule = 'drg_share' and role = 'numerator' then replace(key_value, '|', ' ') end), '') as drg_codes
    from {{ ref('mup_measures_recipe') }}
    group by
        measure_control,
        source_row,
        rule
    union all
    select
        'occupational_mix' as measure_source,
        measure_control,
        coalesce(max(output_name), 'none') as field,
        '' as recipe_rule,
        coalesce(max(output_name), '') as value_column,
        '' as denominator_column,
        '' as drg_codes
    from {{ ref('occmix_measures_recipe') }}
    group by measure_control
),

frozen_recipes as (
    -- The occupational-mix recipe has no rule column in the retired seed.
    select
        measure_source,
        measure_control,
        field,
        case when measure_source = 'occupational_mix' then '' else recipe_rule end as recipe_rule,
        value_column,
        denominator_column,
        drg_codes
    from frozen
),

carried as (
    select distinct
        measure_source,
        measure_control,
        field
    from {{ ref('int_spine_hospital_measures') }}
),

expected as (
    select distinct
        measure_source,
        measure_control,
        field
    from frozen
)

select
    'recipe differs from the frozen identity' as failure,
    measure_source || ' ' || measure_control || ' ' || field || ' ' || recipe_rule
    || ' ' || value_column || ' ' || denominator_column || ' ' || drg_codes as detail
from (
    select * from recipes
    except
    select * from frozen_recipes
) as recipes_without_frozen_recipes
union all
select
    'frozen identity missing from the recipes' as failure,
    measure_source || ' ' || measure_control || ' ' || field || ' ' || recipe_rule
    || ' ' || value_column || ' ' || denominator_column || ' ' || drg_codes as detail
from (
    select * from frozen_recipes
    except
    select * from recipes
) as frozen_recipes_without_recipes
union all
select
    'control carried by the model is not frozen' as failure,
    measure_source || ' ' || measure_control || ' ' || field as detail
from (
    select * from carried
    except
    select * from expected
) as carried_without_expected
union all
select
    'frozen control missing from the model' as failure,
    measure_source || ' ' || measure_control || ' ' || field as detail
from (
    select * from expected
    except
    select * from carried
) as expected_without_carried
where exists (select 1 from {{ ref('int_spine_hospital_measures') }})
union all
select
    'control count differs' as failure,
    spine_key as detail
from {{ ref('int_spine_hospital_measures') }}
group by spine_key
having count(*) <> (select count(*) from expected) or count(distinct measure_source || ':' || measure_control || ':' || field) <> count(*)
