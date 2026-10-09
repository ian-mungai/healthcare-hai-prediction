-- One row per county, SVI edition and published field, long: every field the edition publishes except the place and
-- shape identifiers, under its published name. -999 is null with the token kept. The edition comes
-- from the capture receipt. The 2000 edition's second header row, which names each field, has no county code and is left
-- out (failure modes 427 to 431).
{{ config(materialized='table') }}

with

periods as (
    select
        member_sha256,
        vintage
    from {{ ref('geography_file_periods') }}
    where bronze_table = 'svi'
),

rows as (
    select
        *,
        coalesce(nullif(trim(fips), ''), nullif(trim(state_fips), '') || nullif(trim(cnty_fips), '')) as county_published
    from {{ ref('stg_svi') }}
),

fields as (
    unpivot (
        select
            _member_sha256 as member_sha256,
            _row_number as source_row_number,
            county_published,
            columns(
                c -> not starts_with(c, '_')
                and c not in (
                    'county_published', 'is_label_held', 'label_hold_issue',
                    {%- for column in svi_identifier_columns() %}
                    '{{ column }}'{% if not loop.last %},{% endif %}
                    {%- endfor %}
                )
            )
        from rows
        where regexp_full_match(county_published, '[0-9]{4,5}')
    )
    on columns(* exclude (member_sha256, source_row_number, county_published))
    into name field value value_published
),

published as (
    select
        fields.member_sha256,
        fields.source_row_number,
        periods.vintage as edition,
        {{ county_fips('fields.county_published') }} as county_fips,
        fields.field,
        fields.value_published,
        {{ strict_number('fields.value_published') }} as published_number
    from fields
    left join periods on fields.member_sha256 = periods.member_sha256
)

select
    member_sha256,
    source_row_number,
    edition,
    county_fips,
    field,
    value_published,
    case when published_number <> -999 then published_number end as value_number,
    case when published_number = -999 then trim(value_published) end as missing_token,
    {{ county_scope('county_fips') }} as county_scope,
    member_sha256 || ':' || source_row_number || ':' || field as svi_value_key,
    left(county_fips, 2) = '09' as is_connecticut
from published
