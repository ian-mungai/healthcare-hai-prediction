-- Fails for each state_county_names row that the USDA continuum code list does not confirm: the county code is missing from the list, or the list names
-- it differently. The fixture target skips the check because its continuum code rows hold other counties; the real build runs it.
{%- if target.name == 'fixture' %}
select
    null as county_fips,
    null as county_name
where false
{%- else %}
with

rucc as (
    select distinct
        county_fips,
        {{ state_county_key('county_name') }} as county_key
    from {{ ref('int_rucc_county_codes') }}
)

select
    seed.county_fips,
    seed.county_name
from {{ ref('state_county_names') }} as seed
left join rucc
    on
        seed.county_fips = rucc.county_fips
        and {{ state_county_key('seed.county_name') }} = rucc.county_key
where rucc.county_fips is null
{%- endif %}
