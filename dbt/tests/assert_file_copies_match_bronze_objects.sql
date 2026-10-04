-- Fails for each object bronze holds that the copies table does not list as loaded, and each loaded copy bronze does not
-- hold: bronze loads exactly the listed copies [168] [207].
with

{% for table in var('copy_tables') %}
bronze_{{ table }} as (
    select distinct
        '{{ table }}' as bronze_table,
        _object_key as object_key
    from {{ source('bronze', table) }}
),

{% endfor %}
bronze as (
    {{ union_copy_tables('bronze_') }}
),

loaded as (
    select
        bronze_table,
        object_key
    from {{ ref('stg_bronze__file_copies') }}
    where is_canonical
)

select
    coalesce(bronze.bronze_table, loaded.bronze_table) as bronze_table,
    coalesce(bronze.object_key, loaded.object_key) as object_key,
    bronze.object_key is not null as in_bronze,
    loaded.object_key is not null as listed_as_loaded
from bronze
full outer join loaded
    on
        bronze.bronze_table = loaded.bronze_table
        and bronze.object_key = loaded.object_key
where bronze.object_key is null or loaded.object_key is null
