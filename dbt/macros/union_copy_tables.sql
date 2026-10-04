{% macro union_copy_tables(cte_prefix) %}
{#- Union the per-table CTEs named <cte_prefix><table> for every table in the copy_tables variable. -#}
{%- for table in var('copy_tables') %}
    select * from {{ cte_prefix }}{{ table }}
    {%- if not loop.last %}
    union all
    {%- endif %}
{%- endfor %}
{% endmacro %}
