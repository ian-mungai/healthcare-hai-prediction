-- Fails for each field the income_labor_measures seed names that is not a column of its model, so a registry control never
-- points at a field staging does not carry [446].
-- depends_on: {{ ref('int_saipe_county_estimates') }}
-- depends_on: {{ ref('int_sahie_county_rows') }}
-- depends_on: {{ ref('int_bls_county_series') }}
with

named as (
    select
        measure_control,
        source_model,
        unnest(string_split(fields, ' ')) as field
    from {{ ref('income_labor_measures') }}
    -- C177.01 names only ACS and SVI fields, carried by acs_svi_measures.
    where nullif(trim(fields), '') is not null
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
