-- Fails for each SAIPE county field, SAHIE number or BLS value that is neither a number nor its published missing mark,
-- and each BLS measure code outside the four LAUS measures [435] [442] [445].
with

saipe_lines as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        line_text
    from {{ ref('stg_saipe_text_lines') }}
    where
        regexp_full_match(substr(line_text, 1, 2), '[0-9]{2}')
        and regexp_full_match(trim(substr(line_text, 4, 3)), '[0-9]{1,3}')
        and trim(substr(line_text, 4, 3))::integer > 0
),

saipe_fields as (
    {%- for name, start, finish in saipe_fields() %}
    select
        member_sha256,
        source_row_number,
        '{{ name }}' as field,
        trim(substr(line_text, {{ start }}, {{ finish - start + 1 }})) as value_published
    from saipe_lines
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
),

sahie_fields as (
    {%- for column in sahie_numeric_columns() %}
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        '{{ column }}' as field,
        trim({{ column }}) as value_published
    from {{ ref('stg_sahie') }}
    where trim(geocat) = '50'
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
),

problems as (
    select
        'saipe' as checked_source,
        member_sha256,
        source_row_number,
        field
    from saipe_fields
    where nullif(value_published, '') is not null and value_published <> '.' and {{ dot_number('value_published') }} is null
    union all
    select
        'sahie' as checked_source,
        member_sha256,
        source_row_number,
        field
    from sahie_fields
    where nullif(value_published, '') is not null and value_published <> '.' and {{ dot_number('value_published') }} is null
    union all
    select
        'bls' as checked_source,
        member_sha256,
        source_row_number,
        'value or measure' as field
    from {{ ref('int_bls_county_series') }}
    where
        measure is null
        or (nullif(trim(value_published), '') is not null and missing_token is null and value_number is null)
)

select * from problems
