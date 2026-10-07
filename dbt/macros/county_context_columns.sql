{% macro acs_tables() %}
{#- The ACS export and summary tables C2 types (owner decision Oct 7 2026: vintages 2010 to 2024) [420]. -#}
{{ return(['acs_dp02', 'acs_dp03', 'acs_dp04', 'acs_dp05', 'acs_s0101', 'acs_s0601', 'acs_s1701', 'acs_s2503', 'acs_s2701',
    'acs_b16005', 'acs_b19013', 'acs_b25070', 'acs_b25091', 'acs_b26001', 'acs_c16001', 'acs_summary_b16005',
    'acs_summary_b19013', 'acs_summary_b25070', 'acs_summary_b25091', 'acs_summary_b26001', 'acs_summary_c16001']) }}
{% endmacro %}

{% macro acs_missing_tokens() %}
{#- Published ACS marks for a value that is not given, and the negative sentinels of the summary files [426]. -#}
('(X)', 'null', '-', 'N', '**', '***', '*****', '-999999999', '-888888888', '-666666666', '-555555555', '-333333333', '-222222222')
{% endmacro %}

{% macro acs_number(column) %}
{#- A published ACS cell as a number: a plain number, or a top- or bottom-coded value without its comma and mark ('250,000+'
    is 250000); missing tokens and anything else are null [426]. -#}
{%- set value = "trim(" ~ column ~ ")" -%}
case
    when {{ value }} in {{ acs_missing_tokens() }} then null
    when regexp_full_match({{ value }}, '-?[0-9]+(\.[0-9]+)?') then {{ value }}::double
    when regexp_full_match({{ value }}, '[0-9]{1,3}(,[0-9]{3})*[+-]') then replace(left({{ value }}, -1), ',', '')::double
end
{%- endmacro %}

{% macro svi_identifier_columns() %}
{#- SVI columns that name a place or shape rather than carry a value [428]. -#}
{{ return(['st', 'state', 'st_abbr', 'stcnty', 'county', 'fips', 'location', 'state_fips', 'cnty_fips', 'stcofips', 'state_name',
    'state_abbr', 'shape', 'shape_starea', 'shape_stlength', 'affgeoid']) }}
{% endmacro %}
