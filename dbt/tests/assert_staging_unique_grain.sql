-- Fails for each repeated file checksum and row number in a staging view: one row per row of each distinct file [166].
with

{% for table in var('copy_tables') %}
grain_{{ table }} as (
    select
        '{{ table }}' as bronze_table,
        _member_sha256,
        _row_number,
        count(*) as row_copies
    from {{ ref('stg_' ~ table) }}
    group by
        _member_sha256,
        _row_number
    having count(*) > 1
),

{% endfor %}
repeated as (
    {{ union_copy_tables('grain_') }}
)

select * from repeated
