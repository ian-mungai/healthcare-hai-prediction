-- One row per county, year, age level and published field of the Medicare Geographic Variation file, long: every field
-- except the row identifiers, under its published name, for county rows at every age level; the age level is kept as
-- bene_age_lvl. '*' (suppressed) and 'NA' (not applicable) are null with the token kept.
{{ config(materialized='table') }}

with

counties as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        bene_geo_cd,
        bene_age_lvl,
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
    where trim(bene_geo_lvl) = 'County'
),

fields as (
    select *
    from counties
    unpivot (value_published for field in (columns(* exclude (member_sha256, source_row_number, bene_geo_cd, bene_age_lvl, year))))
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
        case when trim(value_published) in ('*', 'NA') then trim(value_published) end as missing_token,
        nullif(trim(bene_age_lvl), '') as bene_age_lvl
    from fields
)

select
    * exclude (bene_age_lvl),
    {{ county_scope('county_fips') }} as county_scope,
    member_sha256 || ':' || source_row_number || ':' || field || ':' || bene_age_lvl as gv_value_key,
    left(county_fips, 2) = '09' as is_connecticut,
    bene_age_lvl
from typed
