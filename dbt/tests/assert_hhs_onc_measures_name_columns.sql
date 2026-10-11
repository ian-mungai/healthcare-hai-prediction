-- Fails for each field the hhs_onc_measures bridge names that is not a column of its gold model, so a registry control never points at a field gold does
-- not carry; a control the registry closes names no value column and passes [378] [384].
select
    bridge.measure_control,
    bridge.fact,
    bridge.value_column
from {{ ref('hhs_onc_measures_bridge') }} as bridge
left join {{ ref('gold_contract_columns') }} as contract
    on
        bridge.fact = contract.gold_model
        and bridge.value_column = contract.column_name
where
    nullif(bridge.value_column, '') is not null
    and contract.column_name is null
