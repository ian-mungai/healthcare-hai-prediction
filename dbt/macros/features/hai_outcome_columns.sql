{% macro hai_outcome_tokens() %}
{#- The published marks for an HAI value that is not given (all marks in the real calendar-year windows) [551]. -#}
('Not Available', '--', 'N/A')
{% endmacro %}

{% macro hai_baseline_reviewed_through() %}
{#- The last release reviewed as 2015-baseline (schema review; the latest stored release is Aug 13 2026). A later
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
