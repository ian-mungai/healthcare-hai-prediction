-- Fails for each field the hhs_onc_measures seed names that is not a column of its model, so a registry control never
-- points at a field staging does not carry; controls the registry names no field for have none [378] [384].
-- depends_on: {{ ref('int_hhs_capacity_weeks') }}
-- depends_on: {{ ref('int_onc_chpl_linkage_rows') }}
with

named as (
    select
        measure_control,
        source_model,
        unnest(string_split(fields, ' ')) as field
    from {{ ref('hhs_onc_measures') }}
    where coalesce(fields, '') <> ''
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
