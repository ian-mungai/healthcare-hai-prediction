-- Fails for each PLACES value or limit, geographic variation field or WONDER count or rate that is neither a number nor its
-- published mark [449] [455] [459].
with

places as (
    select
        _member_sha256 as member_sha256,
        _row_number as source_row_number,
        list_value(data_value, low_confidence_limit, high_confidence_limit) as published
    from {{ ref('stg_places') }}
    where regexp_full_match(trim(locationid), '[0-9]{5}')
),

places_values as (
    select
        member_sha256,
        source_row_number,
        unnest(published) as value_published
    from places
)

select
    'places' as checked_source,
    member_sha256 || ':' || source_row_number as row_id
from places_values
where nullif(trim(value_published), '') is not null and {{ strict_number('value_published') }} is null
union all
select
    'geographic_variation' as checked_source,
    gv_value_key as row_id
from {{ ref('int_gv_county_values') }}
where nullif(trim(value_published), '') is not null and value_number is null and missing_token is null
union all
select
    'wonder' as checked_source,
    wonder_row_key as row_id
from {{ ref('int_wonder_county_deaths') }}
where
    (nullif(trim(deaths_published), '') is not null and deaths is null and deaths_token is null)
    or (nullif(trim(crude_rate_published), '') is not null and crude_rate is null and crude_rate_token is null)
