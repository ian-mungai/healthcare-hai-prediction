{% macro gv_identifier_columns() %}
{#- Geographic variation columns that identify a row rather than carry a value [456]. -#}
{{ return(['year', 'bene_geo_lvl', 'bene_geo_desc', 'bene_geo_cd', 'bene_age_lvl']) }}
{% endmacro %}

{% macro wonder_tokens() %}
{#- WONDER's published marks for a count or rate that is not given [459]. -#}
('Suppressed', 'Unreliable', 'Missing', 'Not Available')
{% endmacro %}
