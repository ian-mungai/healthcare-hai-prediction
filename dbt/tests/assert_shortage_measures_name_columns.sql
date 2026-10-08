-- Fails for each field the shortage_measures seed names that is not a column of its model, so a registry control never
-- points at a field staging does not carry [482].
-- depends_on: {{ ref('int_mmd_prevalence') }}
-- depends_on: {{ ref('int_hpsa_components') }}
-- depends_on: {{ ref('int_mua_components') }}
with

named as (
    select
        measure_control,
        source_model,
        unnest(string_split(components, ' ')) as field
    from {{ ref('shortage_measures') }}
),

columns as (
    select
        table_name,
        column_name
    from information_schema.columns
    where table_name in (select distinct named.source_model from named)
)

select
    named.measure_control,
    named.source_model,
    named.field
from named
left join columns
    on
        named.source_model = columns.table_name
        and named.field = columns.column_name
where columns.column_name is null
