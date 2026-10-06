{% macro occmix_date(column) %}
{#- Text PUFs use slash dates. Bronze workbook cells serialize dates as ISO midnight timestamps. -#}
coalesce(
    {{ month_day_year(column) }},
    case
        when regexp_full_match({{ column }}, '[0-9]{4}-[0-9]{2}-[0-9]{2}T00:00:00')
            then try_strptime({{ column }}, '%Y-%m-%dT%H:%M:%S')::date
    end
)
{%- endmacro %}

{% macro occmix_number(column) %}
{#- Accept plain numbers or correctly grouped publisher thousands. A dash, dot or blank is missing. -#}
coalesce(
    {{ strict_number(column) }},
    case
        when regexp_full_match(trim({{ column }}), '-?[0-9]{1,3}(,[0-9]{3})+(\.[0-9]+)?')
            then replace(trim({{ column }}), ',', '')::double
    end
)
{%- endmacro %}
