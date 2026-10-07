-- Fails for each field the geography_measures seed names that is not a column of its model, so a registry control never
-- points at a field staging does not carry [417].
-- depends_on: {{ ref('int_rucc_county_codes') }}
-- depends_on: {{ ref('int_ruca_codes') }}
-- depends_on: {{ ref('int_hsa_zip_cases') }}
-- depends_on: {{ ref('int_hud_zip_county_quarters') }}
-- depends_on: {{ ref('int_county_adjacency_edges') }}
with

named as (
    select
        measure_control,
        source_model,
        unnest(string_split(fields, ' ')) as field
    from {{ ref('geography_measures') }}
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
