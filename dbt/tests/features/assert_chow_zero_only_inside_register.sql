-- Fails for each spine row whose C069 (change of ownership in the prior 12 months) or C069 any-event flag differs from the events of fct_ownership_change
-- (design 2.6). The count is the distinct events of the CCN, as buyer or seller, with an effective date in [start - 1 year, start); it is 0, not missing,
-- when there is none, but only when the 12 months lie inside the register, which starts on the earliest effective date of the gold events. An earlier
-- window stays missing. An event 366 days before the start and an event on the start are not counted. The flag is 1 when the count is above 0, else 0, and
-- missing when the count is missing.

with

register as (
    select min(effective_date) as register_start
    from {{ ref('fct_ownership_change') }}
),

events as (
    select distinct
        event_key,
        effective_date,
        ccn_buyer as ccn
    from {{ ref('fct_ownership_change') }}
    where ccn_buyer is not null
    union
    select distinct
        event_key,
        effective_date,
        ccn_seller as ccn
    from {{ ref('fct_ownership_change') }}
    where ccn_seller is not null
),

counted as (
    select
        spine.spine_key,
        (spine.window_start - interval 1 year)::date >= register.register_start as is_inside_register,
        count(distinct case
            when events.effective_date >= (spine.window_start - interval 1 year)::date and events.effective_date < spine.window_start then events.event_key
        end) as event_count
    from {{ ref('int_hospital_spine') }} as spine
    cross join register
    left join events on spine.ccn = events.ccn
    group by
        spine.spine_key,
        spine.window_start,
        register.register_start
),

expected as (
    select
        spine_key,
        case when is_inside_register then event_count::double end as expected_count,
        case when is_inside_register then case when event_count > 0 then 1.0 else 0.0 end end as expected_flag
    from counted
),

published as (
    select
        spine_key,
        max(value_number) filter (where measure_control = 'C069' and field = 'value') as event_count,
        max(value_number) filter (where measure_control = 'C069' and field = 'any_event') as any_event
    from {{ ref('int_spine_operations_measures') }}
    group by spine_key
)

select
    expected.spine_key,
    case
        when (published.event_count is null) <> (expected.expected_count is null) then 'count missing or filled against the register start'
        when published.event_count <> expected.expected_count then 'wrong count of events'
        else 'wrong any-event flag'
    end as failure
from expected
left join published on expected.spine_key = published.spine_key
where
    (published.event_count is null) <> (expected.expected_count is null)
    or published.event_count <> expected.expected_count
    or (published.any_event is null) <> (expected.expected_flag is null)
    or published.any_event <> expected.expected_flag
