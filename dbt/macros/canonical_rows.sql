{% macro canonical_rows(table) %}
-- One copy of each stored file: the rows of its canonical object, with the file's label hold [166] [172].
with

bronze as (
    select * from {{ source('bronze', table) }}
),

files as (
    select
        canonical_object_key,
        is_label_held,
        label_hold_issue
    from {{ ref('stg_bronze__files') }}
    where bronze_table = '{{ table }}'
)

select
    bronze.*,
    files.is_label_held,
    files.label_hold_issue
from bronze
inner join files on bronze._object_key = files.canonical_object_key
{% endmacro %}
