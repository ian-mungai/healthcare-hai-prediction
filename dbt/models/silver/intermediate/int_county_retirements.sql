{{ config(materialized='table') }}

with

events as (
    select
        county_fips,
        county_name,
        event_type,
        page,
        effective_date,
        coalesce(successor_fips, '') as successor_fips
    from {{ ref('census_county_change_events') }}
    where event_type in ('deleted', 'renamed_to', 'created', 'renamed_from')
),

ranked as (
    select
        *,
        row_number() over (
            partition by county_fips order by effective_date desc nulls last, page desc, event_type asc, county_name asc, successor_fips asc
        ) as event_rank
    from events
    where event_type in ('deleted', 'renamed_to')
),

latest as (
    select
        county_fips,
        county_name,
        event_type,
        page,
        effective_date,
        successor_fips
    from ranked
    where event_rank = 1
),

assignments as (
    select
        latest.county_fips,
        events.county_name,
        events.page,
        events.effective_date,
        coalesce(events.page > latest.page or (events.page = latest.page and events.effective_date > latest.effective_date), false) as follows_retirement
    from latest
    inner join events
        on latest.county_fips = events.county_fips and events.event_type in ('created', 'renamed_from')
),

reassigned as (
    select distinct county_fips
    from assignments
    where follows_retirement
),

assigned_names as (
    select
        county_fips,
        county_name,
        row_number() over (partition by county_fips order by page desc, effective_date desc nulls last, county_name asc) as assignment_rank
    from assignments
    where not follows_retirement
),

retired as (
    select
        latest.county_fips,
        latest.event_type,
        latest.page,
        latest.effective_date,
        latest.successor_fips,
        coalesce(assigned_names.county_name, latest.county_name) as county_name
    from latest
    left join assigned_names
        on latest.county_fips = assigned_names.county_fips and assigned_names.assignment_rank = 1
    where latest.county_fips not in (select reassigned.county_fips from reassigned)
),

labels as (
    select
        county_fips,
        historical_name,
        retirement_basis
    from {{ ref('alaska_county_code_labels') }}
),

final as (
    select
        true as is_retired,
        retired.effective_date as retired_effective_date,
        retired.page as change_page,
        retired.event_type as change_event,
        coalesce(retired.county_fips, labels.county_fips) as county_fips,
        nullif(retired.successor_fips, '') as successor_county_fips,
        coalesce(labels.historical_name, retired.county_name) as historical_county_name,
        case when retired.county_fips is not null then 'census_change_page' else labels.retirement_basis end as retirement_basis
    from retired
    full outer join labels
        on retired.county_fips = labels.county_fips
)

select * from final
