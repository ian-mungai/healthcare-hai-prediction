-- One row per county, year and published field of the Medicare Geographic Variation file, long: every field except the
-- row identifiers, under its published name, for county rows at the All age level only. '*' (suppressed) and 'NA' (not
-- applicable) are null with the token kept (failure modes 454 to 456).
{{ config(materialized='table') }}

with

counties as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        bene_geo_cd,
        columns(
            c -> not starts_with(c, '_')
            and c not in (
                'is_label_held', 'label_hold_issue',
                {%- for column in gv_identifier_columns() if column != 'year' %}
                '{{ column }}'{% if not loop.last %},{% endif %}
                {%- endfor %}
            )
        )
    from {{ ref('stg_cms_geographic_variation_csv') }}
    where trim(bene_geo_lvl) = 'County' and trim(bene_age_lvl) = 'All'
),

fields as (
    select *
    from counties
    unpivot (value_published for field in (columns(* exclude (member_sha256, source_row_number, bene_geo_cd, year))))
),

typed as (
    select
        member_sha256,
        source_row_number,
        field,
        value_published,
        try_cast(trim(year) as integer) as data_year,
        {{ county_fips('bene_geo_cd') }} as county_fips,
        {{ strict_number('value_published') }} as value_number,
        case when trim(value_published) in ('*', 'NA') then trim(value_published) end as missing_token
    from fields
)

select
    *,
    {{ county_scope('county_fips') }} as county_scope,
    member_sha256 || ':' || source_row_number || ':' || field as gv_value_key,
    left(county_fips, 2) = '09' as is_connecticut
from typed
