{% macro hai_outcome_types() %}
{#- The six published infection types, each a model outcome kept apart; the target is chosen before training. -#}
{{ return(['HAI_1', 'HAI_2', 'HAI_3', 'HAI_4', 'HAI_5', 'HAI_6']) }}
{% endmacro %}

{% macro hai_outcome_parts() %}
{#- The parts of each type, by the suffix of the published measure ID, and the column each becomes. -#}
{{ return([
    ['SIR', 'sir'],
    ['CILOWER', 'ci_lower'],
    ['CIUPPER', 'ci_upper'],
    ['NUMERATOR', 'observed'],
    ['ELIGCASES', 'predicted'],
    ['DOPC', 'exposure']
]) }}
{% endmacro %}
