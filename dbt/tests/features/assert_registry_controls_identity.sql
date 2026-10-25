-- Fails when the gold control bridge (read through its 15 filter models, one per retired seed) and the formula recipes together hold other registry
-- controls than the 15 retired seeds listed at e918ed7 (frozen_registry_controls), also when a control is swapped for another and the count stays the
-- same. The registry controls the feature models read all come from these two places, so a control lost from, or added to, every lookup of the feature
-- phase fails here.
with

frozen as (
    select measure_control
    from {{ ref('frozen_registry_controls') }}
    where is_carried
),

lookup as (
    select measure_control from {{ ref('feature_bridge_acs_svi_measures') }}
    union
    select measure_control from {{ ref('feature_bridge_cost_report_measures') }}
    union
    select measure_control from {{ ref('feature_bridge_county_health_measures') }}
    union
    select measure_control from {{ ref('feature_bridge_geography_measures') }}
    union
    select measure_control from {{ ref('feature_bridge_hai_outcome_measures') }}
    union
    select measure_control from {{ ref('feature_bridge_hhs_onc_measures') }}
    union
    select measure_control from {{ ref('feature_bridge_impact_measures') }}
    union
    select measure_control from {{ ref('feature_bridge_income_labor_measures') }}
    union
    select measure_control from {{ ref('feature_bridge_mmd_conditions') }}
    union
    select measure_control from {{ ref('feature_bridge_mup_measures') }}
    union
    select measure_control from {{ ref('feature_bridge_occmix_measures') }}
    union
    select measure_control from {{ ref('feature_bridge_ownership_measures') }}
    union
    select measure_control from {{ ref('feature_bridge_registry_measure_sources') }}
    union
    select measure_control from {{ ref('feature_bridge_shortage_measures') }}
    union
    select measure_control from {{ ref('feature_bridge_validation_measures') }}
    union
    select measure_control from {{ ref('cost_report_measures_recipe') }}
    union
    select measure_control from {{ ref('hhs_onc_measures_recipe') }}
    union
    select measure_control from {{ ref('impact_measures_recipe') }}
    union
    select measure_control from {{ ref('mmd_conditions_recipe') }}
    union
    select measure_control from {{ ref('mup_measures_recipe') }}
    union
    select measure_control from {{ ref('occmix_measures_recipe') }}
    union
    select measure_control from {{ ref('shortage_measures_recipe') }}
)

select
    'control in the bridge or a recipe is not frozen' as failure,
    measure_control as detail
from (
    select * from lookup
    except
    select * from frozen
) as lookup_without_frozen
union all
select
    'frozen control missing from the bridge and the recipes' as failure,
    measure_control as detail
from (
    select * from frozen
    except
    select * from lookup
) as frozen_without_lookup
