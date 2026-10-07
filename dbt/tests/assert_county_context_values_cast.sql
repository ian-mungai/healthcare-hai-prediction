-- Fails for each ACS cell that is neither a number, a coded median nor a published missing token, and each SVI field that
-- is neither a number nor -999 [426] [428].
select
    'int_acs_county_values' as checked_model,
    acs_value_key as row_id,
    value_published
from {{ ref('int_acs_county_values') }}
where nullif(trim(value_published), '') is not null and value_number is null and missing_token is null
union all
select
    'int_svi_county_values' as checked_model,
    svi_value_key as row_id,
    value_published
from {{ ref('int_svi_county_values') }}
where nullif(trim(value_published), '') is not null and value_number is null and missing_token is null
