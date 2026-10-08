{% macro hai_outcome_types() %}
{#- The six published infection types, each a model outcome kept apart; the target is chosen before training [550] [558]. -#}
{{ return(['HAI_1', 'HAI_2', 'HAI_3', 'HAI_4', 'HAI_5', 'HAI_6']) }}
{% endmacro %}

{% macro hai_outcome_parts() %}
{#- The parts of each type, by the suffix of the published measure ID, and the column each becomes [550]. -#}
{{ return([
    ['SIR', 'sir'],
    ['CILOWER', 'ci_lower'],
    ['CIUPPER', 'ci_upper'],
    ['NUMERATOR', 'observed'],
    ['ELIGCASES', 'predicted'],
    ['DOPC', 'exposure']
]) }}
{% endmacro %}

{% macro hai_outcome_tokens() %}
{#- The published marks for an HAI value that is not given (real calendar-year windows, Oct 8 2026) [551]. -#}
('Not Available', '--', 'N/A')
{% endmacro %}

{% macro hai_baseline_reviewed_through() %}
{#- The last release reviewed as 2015-baseline (schema review, Oct 1 2026; latest stored release Aug 13 2026). A later
    release is held until it is reviewed, so 2022-baseline SIRs never join the series [554]. -#}
date '2026-08-13'
{% endmacro %}

{% macro hai_footnote_codes(column) %}
{#- The leading footnote codes in every published format (13; 3, 13; 13 - text), sorted as numbers [555]. -#}
nullif(array_to_string(list_sort(list_transform(list_filter(
    list_transform(string_split(split_part(trim({{ column }}), ' - ', 1), ','), code -> trim(code)),
    code -> regexp_full_match(code, '[0-9]+')
), code -> code::integer)), ','), '')
{% endmacro %}
