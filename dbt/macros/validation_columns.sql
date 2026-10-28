{% macro validation_tokens() %}
{#- The published marks for a Care Compare validation value that is not given. -#}
('Not Available', 'Not Applicable', 'N/A', 'Too Few to Report')
{% endmacro %}

{% macro validation_reviewed_values() %}
{#- Published values reviewed by the owner that stay text with no number; the last two are hospital 330201's fiscal
    year 2020 clinical outcomes domain scores (owner, Oct 11 2026). -#}
('24.083333333333(23)', '63.333333333333(23)', '15.833333333333(23)')
{% endmacro %}

{% macro validation_ccn(value) %}
{#- The hospital ID as a 6-character CCN: a 5-digit ID is padded; any other ID that is not 6 digits or capital letters is
    null, so the row is held as no_key and the published ID stays in the staging view (group B rule 370). -#}
case
    when regexp_full_match(trim({{ value }}), '[0-9]{5}') then lpad(trim({{ value }}), 6, '0')
    when regexp_full_match(trim({{ value }}), '[0-9A-Z]{6}') then trim({{ value }})
end
{% endmacro %}

{% macro validation_date(column) %}
{#- A trimmed MM/DD/YYYY date text as a date; blank text and anything else give null, so a cast test can tell them apart. -#}
try_strptime(nullif(trim({{ column }}), ''), '%m/%d/%Y')::date
{%- endmacro %}

{% macro validation_aliased_windows(source_model, windows, numbers) %}
{#- The window rows of a CTE with the reviewed measure ID: the exact published ID wins; a reviewed alias of measure_id_aliases
    fills only a hospital and window the exact ID leaves empty. A renamed row keeps the ID as published in
    published_measure_id and its window_key carries the reviewed ID, so a second row for one reviewed ID fails the unique
    test. numbers is a list of [number column, text column] pairs cast with strict_number. -#}
select
    published.* replace (
        coalesce(aliases.measure_id, published.measure_id) as measure_id,
        published.entity_id || ':' || coalesce(aliases.measure_id, published.measure_id) || ':' || published.window_start || ':'
        || published.window_end as window_key
    ),
    published.measure_id as published_measure_id,
    {%- for number, text in numbers %}
    {{ strict_number('published.' ~ text) }} as {{ number }}{{ ',' if not loop.last }}
    {%- endfor %}
from {{ windows }} as published
left join {{ ref('measure_id_aliases') }} as aliases
    on
        aliases.source_model = '{{ source_model }}'
        and published.measure_id = aliases.published_measure_id
where
    aliases.measure_id is null
    or not exists (
        select 1
        from {{ windows }} as exact
        where
            exact.measure_id = aliases.measure_id
            and exact.entity_id = published.entity_id
            and exact.window_start = published.window_start
            and exact.window_end = published.window_end
    )
{% endmacro %}
