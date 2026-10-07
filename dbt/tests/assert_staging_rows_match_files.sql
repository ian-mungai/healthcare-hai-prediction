-- Fails for each staging view whose rows differ from the summed rows of its distinct files: no copy counted twice [166].
with

{% for table in var('copy_tables') %}
staged_{{ table }} as (
    select
        '{{ table }}' as bronze_table,
        count(*) as staged_rows
    from {{ ref('stg_' ~ table) }}
),

{% endfor %}
staged as (
    {{ union_copy_tables('staged_') }}
),

files as (
    select
        bronze_table,
        sum(row_count) as file_rows
    from {{ ref('stg_bronze__files') }}
    group by bronze_table
)

select
    staged.bronze_table,
    staged.staged_rows,
    files.file_rows
from staged
left join files on staged.bronze_table = files.bronze_table
where coalesce(files.file_rows, 0) <> staged.staged_rows
