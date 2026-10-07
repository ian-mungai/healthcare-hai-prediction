-- Fails for each published C1 value that does not type: a HUD ZIP code or ratio (scientific notation is a number; a ratio
-- must lie in 0 to 1), an adjacency county code, border length or 2010 line shape, a RUCC or RUCA code outside its
-- published categories, a tract or ZIP identifier, and a service-area ZIP code or count that is neither a number nor the
-- suppression mark [405] [407] [411] [413].
with

hud as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        zip,
        list_value(res_ratio, bus_ratio, oth_ratio, tot_ratio) as ratios
    from {{ ref('stg_hud_zip_county') }}
),

hud_ratios as (
    select
        member_sha256,
        source_row_number,
        unnest(ratios) as ratio
    from hud
),

adjacency_lines as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        string_split(line_text, chr(9)) as line_fields
    from {{ ref('stg_county_adjacency_2010_text_lines') }}
),

hsa as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        zip_cd_of_residence,
        list_value(total_cases, total_days_of_care, total_charges) as counts
    from {{ ref('stg_cms_hsa_csv') }}
),

hsa_counts as (
    select
        member_sha256,
        source_row_number,
        unnest(counts) as count_published
    from hsa
),

problems as (
    select
        'hud_zip_county' as checked_model,
        member_sha256,
        source_row_number::varchar as row_id,
        'zip' as checked_field
    from hud
    where nullif(trim(zip), '') is not null and {{ zip_code('zip') }} is null
    union all
    select
        'hud_zip_county' as checked_model,
        member_sha256,
        source_row_number::varchar as row_id,
        'ratio' as checked_field
    from hud_ratios
    where
        nullif(trim(ratio), '') is not null
        and coalesce(({{ strict_number('ratio') }}) not between 0 and 1, true)
    union all
    select
        'int_county_adjacency_edges' as checked_model,
        member_sha256,
        source_row_number::varchar as row_id,
        'county or neighbor' as checked_field
    from {{ ref('int_county_adjacency_edges') }}
    where county_fips is null or (nullif(trim(neighbor_published), '') is not null and neighbor_fips is null)
    union all
    select
        'int_county_adjacency_edges' as checked_model,
        member_sha256,
        source_row_number::varchar as row_id,
        'length' as checked_field
    from {{ ref('int_county_adjacency_edges') }}
    where nullif(trim(length_published), '') is not null and coalesce(shared_border_length_m < 0, true)
    union all
    select
        'county_adjacency_2010_text_lines' as checked_model,
        member_sha256,
        source_row_number::varchar as row_id,
        'line shape' as checked_field
    from adjacency_lines
    where len(line_fields) <> 4
    union all
    select
        'int_rucc_county_codes' as checked_model,
        member_sha256,
        county_published as row_id,
        'county, code or population' as checked_field
    from {{ ref('int_rucc_county_codes') }}
    where
        county_fips is null
        or (rucc_published is not null and rucc_code is null)
        or (population_published is not null and population is null)
    union all
    select
        'int_ruca_codes' as checked_model,
        member_sha256,
        ruca_row_key as row_id,
        'geography or code' as checked_field
    from {{ ref('int_ruca_codes') }}
    where
        geography_id is null
        or (primary_published is not null and primary_ruca is null and primary_published <> '99')
        or (secondary_published is not null and secondary_ruca is null and secondary_published <> '99')
    union all
    select
        'cms_hsa_csv' as checked_model,
        member_sha256,
        source_row_number::varchar as row_id,
        'zip' as checked_field
    from hsa
    where nullif(trim(zip_cd_of_residence), '') is not null and trim(zip_cd_of_residence) <> '*' and {{ zip_code('zip_cd_of_residence') }} is null
    union all
    select
        'cms_hsa_csv' as checked_model,
        member_sha256,
        source_row_number::varchar as row_id,
        'count' as checked_field
    from hsa_counts
    where
        nullif(trim(count_published), '') is not null
        and trim(count_published) <> '*'
        and coalesce(({{ strict_number('count_published') }}) < 0, true)
)

select * from problems
