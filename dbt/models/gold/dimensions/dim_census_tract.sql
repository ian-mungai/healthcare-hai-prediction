-- One row per 11-digit census tract GEOID, plus the unknown member: the RUCA rows of tract geography and the HPSA geography IDs that are 11
-- digits. Any other HPSA ID stays in the fact and takes the unknown member.
with

observed as (
    select cast(geography_id as varchar) as census_tract_geoid
    from {{ ref('int_ruca_codes') }}
    where geography_type = 'tract'
    union
    select cast(geography_id as varchar) as census_tract_geoid
    from {{ ref('int_hpsa_components') }}
),

valid as (
    select census_tract_geoid
    from observed
    where regexp_matches(census_tract_geoid, '^[0-9]{11}$')
),

final as (
    select
        md5(census_tract_geoid) as census_tract_sk,
        census_tract_geoid
    from valid
    union all
    select
        md5('<unknown>') as census_tract_sk,
        '<unknown>' as census_tract_geoid
)

select * from final
