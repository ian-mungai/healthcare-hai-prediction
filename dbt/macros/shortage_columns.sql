{% macro hrsa_us_date(column) %}
{#- An HRSA HPSA date published as MM/DD/YYYY; blank is null and anything else is null for the cast test [478]. -#}
try_strptime(nullif(trim({{ column }}), ''), '%m/%d/%Y')::date
{%- endmacro %}

{% macro hrsa_iso_date(column) %}
{#- An HRSA MUA date published as YYYY-MM-DD; blank is null and anything else is null for the cast test [478]. -#}
case when regexp_full_match(trim({{ column }}), '[0-9]{4}-[0-9]{2}-[0-9]{2}') then try_strptime(trim({{ column }}), '%Y-%m-%d')::date end
{%- endmacro %}
