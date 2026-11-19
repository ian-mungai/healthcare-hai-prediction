{#- What a staging view needs of its bronze table before it reads named columns from it. A table without rows reads as a typed empty result, whatever its
    columns (a table pruned to nothing keeps the columns of its old files); a table that is absent, or that has rows and columns that differ from the ones the
    view reads, stops the build and names the table and the columns. -#}

{% macro bronze_projection_columns(projection, flags='') %}
{#- The bronze columns a projection reads: every name written as bronze.<name>, without the flag columns that the bronze CTE adds itself. Read from the
    projection text, so the list cannot drift from the columns the view selects. -#}
{%- set derived = modules.re.findall('(?i)\\bas\\s+([a-z_][a-z0-9_]*)\\s*(?:,|\\Z)', flags) -%}
{%- set names = [] -%}
{%- for name in modules.re.findall('\\bbronze\\.([a-z_][a-z0-9_]*)', projection) -%}
    {%- if name not in derived and name not in names -%}
        {%- do names.append(name) -%}
    {%- endif -%}
{%- endfor -%}
{{ return(names) }}
{%- endmacro %}

{% macro bronze_shape(table, required) %}
{#- 'present' when the bronze table has every column in required (a table without rows is present and reads as empty); 'empty' when it lacks a column and has no
    row; otherwise a refusal that names the table and the columns. Nothing is read when the project is only parsed or compiled for linting (target lint has no
    catalog). -#}
{%- if not execute or target.name == 'lint' -%}
    {{ return('present') }}
{%- endif -%}
{%- set relation = source('bronze', table) -%}
{%- set found = adapter.get_columns_in_relation(relation) | map(attribute='name') | list -%}
{%- if found | length == 0 -%}
    {{ exceptions.raise_compiler_error('bronze table ' ~ table ~ ' is absent from the catalog or has no columns, and the silver view reads it') }}
{%- endif -%}
{%- set missing = [] -%}
{%- for name in required -%}
    {%- if name not in found -%}
        {%- do missing.append(name) -%}
    {%- endif -%}
{%- endfor -%}
{%- if missing | length == 0 -%}
    {{ return('present') }}
{%- endif -%}
{%- set counted = run_query('select count(*) from ' ~ relation) -%}
{%- set count = counted.rows[0][0] -%}
{%- if count == 0 -%}
    {{ return('empty') }}
{%- endif -%}
{{ exceptions.raise_compiler_error('bronze table ' ~ table ~ ' does not have the columns its silver view reads: missing ' ~ bronze_names_text(missing) ~ '; the table has ' ~ bronze_names_text(found)) }}
{%- endmacro %}

{% macro bronze_names_text(names) %}
{#- The first twelve names, then the count of the rest, so a refusal for a 354-column table stays readable. -#}
{{ names[:12] | join(', ') }}{% if names | length > 12 %} and {{ names | length - 12 }} more{% endif %}
{%- endmacro %}

{% macro bronze_source_rows(table, required, shape) %}
{#- The select that stands for the bronze table: the table itself, or for an empty one no rows of the typed columns the view reads (a lineage row number is a BIGINT, every other bronze column a VARCHAR). -#}
{%- if shape == 'empty' -%}
select
    {%- for name in required %}
    cast(null as {{ 'bigint' if name == '_row_number' else 'varchar' }}) as {{ name }}{% if not loop.last %},{% endif %}
    {%- endfor %}
where false
{%- else -%}
select * from {{ source('bronze', table) }}
{%- endif -%}
{%- endmacro %}
