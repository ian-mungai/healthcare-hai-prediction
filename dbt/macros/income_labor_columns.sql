{% macro saipe_fields() %}
{#- SAIPE estimate fields and their documented positions (start, end), identical in every year's layout 1989 to 2024 [435]. -#}
{{ return([
    ('poverty_all_count', 8, 15), ('poverty_all_count_lb90', 17, 24), ('poverty_all_count_ub90', 26, 33),
    ('poverty_all_pct', 35, 38), ('poverty_all_pct_lb90', 40, 43), ('poverty_all_pct_ub90', 45, 48),
    ('poverty_0_17_count', 50, 57), ('poverty_0_17_count_lb90', 59, 66), ('poverty_0_17_count_ub90', 68, 75),
    ('poverty_0_17_pct', 77, 80), ('poverty_0_17_pct_lb90', 82, 85), ('poverty_0_17_pct_ub90', 87, 90),
    ('poverty_5_17_related_count', 92, 99), ('poverty_5_17_related_count_lb90', 101, 108), ('poverty_5_17_related_count_ub90', 110, 117),
    ('poverty_5_17_related_pct', 119, 122), ('poverty_5_17_related_pct_lb90', 124, 127), ('poverty_5_17_related_pct_ub90', 129, 132),
    ('median_household_income', 134, 139), ('median_household_income_lb90', 141, 146), ('median_household_income_ub90', 148, 153),
]) }}
{% endmacro %}

{% macro sahie_numeric_columns() %}
{#- SAHIE counts, percents and their margins of error [442]. -#}
{{ return(['nipr', 'nipr_moe', 'nui', 'nui_moe', 'nic', 'nic_moe', 'pctui', 'pctui_moe', 'pctic', 'pctic_moe', 'pctelig',
    'pctelig_moe', 'pctliic', 'pctliic_moe']) }}
{% endmacro %}

{% macro bls_measures() %}
{#- The four LAUS county measure codes and their names [445]. -#}
{{ return([('03', 'unemployment_rate'), ('04', 'unemployed'), ('05', 'employed'), ('06', 'labor_force')]) }}
{% endmacro %}

{% macro dot_number(column) %}
{#- A trimmed plain number as a double; '.' and blank are null (the caller keeps '.' as missing) [442]. -#}
{%- set value = "nullif(trim(" ~ column ~ "), '')" -%}
case when regexp_full_match({{ value }}, '-?[0-9]+(\.[0-9]+)?') then {{ value }}::double end
{%- endmacro %}
