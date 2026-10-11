-- Fails for each field the shortage_measures bridge names that is not a column of its gold model, so a registry control never points at a field gold does
-- not carry; a control that names no value column passes [482].
select
    bridge.measure_control,
    bridge.fact,
    bridge.value_column
from {{ ref('shortage_measures_bridge') }} as bridge
left join {{ ref('gold_contract_columns') }} as contract
    on
        bridge.fact = contract.gold_model
        and bridge.value_column = contract.column_name
where
    nullif(bridge.value_column, '') is not null
    and contract.column_name is null
