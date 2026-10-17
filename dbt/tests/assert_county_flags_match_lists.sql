-- The retired-code attributes of dim_county are recomputed from the county lists per vintage (adjacency counties and neighbors, RUCC, ACS). A code is in
-- the latest list when the adjacency list of the latest adjacency vintage names it; its last listed vintage is the latest vintage of any list that names
-- it; its first unlisted vintage is the first vintage of any list after that, for a code outside the latest list. A code in no list has null vintages.
-- There is no successor column, so nothing is shown as a published successor [943].
with

lists as (
    select
        'adjacency' as list_name,
        vintage,
        county_fips
    from {{ ref('int_county_adjacency_edges') }}
    union all
    select
        'adjacency' as list_name,
        vintage,
        neighbor_fips as county_fips
    from {{ ref('int_county_adjacency_edges') }}
    where neighbor_fips is not null
    union all
    select
        'rucc' as list_name,
        vintage,
        county_fips
    from {{ ref('int_rucc_county_codes') }}
    union all
    select
        'acs' as list_name,
        vintage,
        county_fips
    from {{ ref('int_acs_county_values') }}
),

named as (
    select distinct
        vintage,
        county_fips
    from lists
    where vintage is not null and county_fips is not null
),

all_vintages as (
    select distinct vintage
    from named
),

adjacency_vintage as (
    select max(vintage) as vintage
    from lists
    where list_name = 'adjacency'
),

latest_codes as (
    select distinct lists.county_fips
    from lists
    inner join adjacency_vintage
        on lists.vintage = adjacency_vintage.vintage
    where lists.list_name = 'adjacency'
),

last_listed as (
    select
        county_fips,
        max(vintage) as last_listed_vintage
    from named
    group by county_fips
),

after_last as (
    select
        last_listed.county_fips,
        min(all_vintages.vintage) as first_vintage_after
    from last_listed
    inner join all_vintages
        on last_listed.last_listed_vintage < all_vintages.vintage
    group by last_listed.county_fips
),

expected as (
    select
        dim_county.county_fips,
        last_listed.last_listed_vintage,
        latest_codes.county_fips is not null as in_latest_county_list,
        case when latest_codes.county_fips is null then after_last.first_vintage_after end as first_unlisted_vintage
    from {{ ref('dim_county') }} as dim_county
    left join latest_codes
        on dim_county.county_fips = latest_codes.county_fips
    left join last_listed
        on dim_county.county_fips = last_listed.county_fips
    left join after_last
        on dim_county.county_fips = after_last.county_fips
    where dim_county.county_sk <> md5('<unknown>')
)

select
    'flag or vintage differs from the county lists' as problem,
    dim_county.county_fips as detail
from {{ ref('dim_county') }} as dim_county
inner join expected
    on dim_county.county_fips = expected.county_fips
where
    dim_county.in_latest_county_list is distinct from expected.in_latest_county_list
    or dim_county.last_listed_vintage is distinct from expected.last_listed_vintage
    or dim_county.first_unlisted_vintage is distinct from expected.first_unlisted_vintage
union all
select
    'unknown member has a county-list attribute' as problem,
    county_fips as detail
from {{ ref('dim_county') }}
where
    county_sk = md5('<unknown>')
    and (in_latest_county_list is not null or last_listed_vintage is not null or first_unlisted_vintage is not null)
