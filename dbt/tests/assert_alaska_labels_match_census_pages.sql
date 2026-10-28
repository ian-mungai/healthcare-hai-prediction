with

labels as (
    select
        county_fips,
        historical_name,
        census_page_name,
        census_page,
        retirement_basis
    from {{ ref('alaska_county_code_labels') }}
),

events as (
    select
        county_fips,
        county_name,
        page
    from {{ ref('census_county_change_events') }}
    where event_type = 'deleted'
),

retirements as (
    select
        county_fips,
        historical_county_name,
        retirement_basis
    from {{ ref('int_county_retirements') }}
)

select
    labels.county_fips,
    'census page does not delete the entity' as problem
from labels
where
    labels.retirement_basis = 'census_change_page'
    and not exists (
        select 1
        from events
        where
            events.county_fips = labels.county_fips
            and cast(events.page as varchar) = labels.census_page
            and starts_with(events.county_name, labels.census_page_name)
    )
union all
select
    labels.county_fips,
    'a page retires a code labelled absent from later lists' as problem
from labels
inner join events
    on labels.county_fips = events.county_fips
where labels.retirement_basis = 'absent_from_later_lists'
union all
select
    labels.county_fips,
    'retirement row lacks the reviewed name or basis' as problem
from labels
left join retirements
    on labels.county_fips = retirements.county_fips
where retirements.county_fips is null or retirements.historical_county_name <> labels.historical_name or retirements.retirement_basis <> labels.retirement_basis
