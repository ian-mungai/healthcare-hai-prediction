-- state_fips is the first two characters of the county FIPS in the RUCC rows of the state, one to one. A state with no RUCC row keeps a null
-- state_fips, and the unknown member has none.
with

pairs as (
    select distinct
        state as state_code,
        substring(county_fips, 1, 2) as state_fips
    from {{ ref('int_rucc_county_codes') }}
    where state is not null and county_fips is not null and length(county_fips) = 5
)

select
    'postal code has two FIPS codes' as problem,
    state_code as detail
from pairs
group by state_code
having count(*) > 1
union all
select
    'FIPS code has two postal codes' as problem,
    state_fips as detail
from pairs
group by state_fips
having count(*) > 1
union all
select
    'state_fips differs from the RUCC pair' as problem,
    dim_state.state_code as detail
from {{ ref('dim_state') }} as dim_state
left join pairs
    on dim_state.state_code = pairs.state_code
where dim_state.state_sk <> md5('<unknown>') and dim_state.state_fips is distinct from pairs.state_fips
union all
select
    'RUCC state is not in dim_state' as problem,
    pairs.state_code as detail
from pairs
left join {{ ref('dim_state') }} as dim_state
    on pairs.state_code = dim_state.state_code
where dim_state.state_code is null
union all
select
    'unknown member has a state_fips' as problem,
    state_fips as detail
from {{ ref('dim_state') }}
where state_sk = md5('<unknown>') and state_fips is not null
