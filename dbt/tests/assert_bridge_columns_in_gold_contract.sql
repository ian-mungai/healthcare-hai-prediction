-- Fails for each gold bridge row that does not match the gold contract:
-- a fact that is not in the contract, a value column that is not a column of that fact, a value column that is the fact's measure-key column,
-- a wide fact (no measure-key column) whose measure key differs from its value column, a long fact whose measure key is the name of one of
-- its columns (a long fact holds the published ID, field or concept in its measure-key column), and a long-fact measure key that its rule in
-- gold_measure_key_rules does not accept:
-- a domain rule needs an exact, case-sensitive member of that domain in gold_measure_id_domains (no narrative fallback);
-- an identifier rule rejects a wide-source column that identifies a row (svi_identifier_columns, gv_identifier_columns);
-- an evidence rule needs a row in gold_measure_key_evidence (the reviewed ACS, impact, HAI and BLS files);
-- a structural_only rule has no authoritative domain, so only the column checks apply; a long fact with no rule fails.
-- A closed control has no key and no column.
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

models as (
    select
        gold_model,
        coalesce(bool_or(is_measure_key = 'true'), false) as is_long
    from {{ ref('gold_contract_columns') }}
    group by gold_model
),

fact_rules as (
    select
        fact,
        max(case when rule = 'domain' then value end) as domain_name,
        coalesce(bool_or(rule = 'evidence'), false) as needs_evidence
    from {{ ref('gold_measure_key_rules') }}
    group by fact
)

select
    bridge.fact,
    bridge.measure_key,
    bridge.value_column,
    bridge.measure_control,
    bridge.source_seed,
    bridge.source_row
from bridge
left join models
    on bridge.fact = models.gold_model
left join {{ ref('gold_contract_columns') }} as value_contract
    on
        bridge.fact = value_contract.gold_model
        and bridge.value_column = value_contract.column_name
left join {{ ref('gold_contract_columns') }} as key_contract
    on
        bridge.fact = key_contract.gold_model
        and bridge.measure_key = key_contract.column_name
left join {{ ref('gold_measure_key_evidence') }} as evidence
    on
        bridge.fact = evidence.fact
        and bridge.measure_key = evidence.measure_key
        and bridge.measure_control = evidence.measure_control
left join fact_rules
    on bridge.fact = fact_rules.fact
left join {{ ref('gold_measure_id_domains') }} as domain_key
    on
        fact_rules.domain_name = domain_key.domain
        and bridge.measure_key = domain_key.published_key
left join {{ ref('gold_measure_key_rules') }} as identifier_rule
    on
        bridge.fact = identifier_rule.fact
        and identifier_rule.rule = 'identifier'
        and bridge.measure_key = identifier_rule.value
where
    models.gold_model is null
    or (nullif(bridge.value_column, '') is not null and value_contract.column_name is null)
    or value_contract.is_measure_key = 'true'
    or (nullif(bridge.measure_key, '') is not null and not models.is_long and bridge.measure_key <> bridge.value_column)
    or (nullif(bridge.measure_key, '') is not null and models.is_long and key_contract.column_name is not null)
    or (nullif(bridge.measure_key, '') is not null and models.is_long and fact_rules.fact is null)
    or (nullif(bridge.measure_key, '') is not null and models.is_long and fact_rules.domain_name is not null and domain_key.domain is null)
    or (nullif(bridge.measure_key, '') is not null and models.is_long and identifier_rule.fact is not null)
    or (nullif(bridge.measure_key, '') is not null and models.is_long and fact_rules.needs_evidence and evidence.fact is null)
