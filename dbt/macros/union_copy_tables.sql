{% macro union_copy_tables(cte_prefix, tables=none) %}
{#- Union the per-table CTEs named <cte_prefix><table> for the given tables, by default every table in the copy_tables variable. -#}
{%- for table in (tables if tables is not none else var('copy_tables')) %}
    select * from {{ cte_prefix }}{{ table }}
    {%- if not loop.last %}
    union all
    {%- endif %}
{%- endfor %}
{% endmacro %}

{% macro staged_copy_tables() %}
{#- The copy_tables that have a staging view: a table whose view is deferred (vars.deferred_models) keeps its stored copies in stg_bronze__file_copies but has no
    view to count. A ref to a missing view would make dbt disable the global staging tests for every table, so they iterate this list. -#}
{%- set deferred = [] -%}
{%- for entry in var('deferred_models', []) -%}
    {%- for name in entry.models -%}
        {%- do deferred.append(name) -%}
    {%- endfor -%}
{%- endfor -%}
{%- set tables = [] -%}
{%- for table in var('copy_tables') -%}
    {%- if ('stg_' ~ table) not in deferred -%}
        {%- do tables.append(table) -%}
    {%- endif -%}
{%- endfor -%}
{{ return(tables) }}
{% endmacro %}
