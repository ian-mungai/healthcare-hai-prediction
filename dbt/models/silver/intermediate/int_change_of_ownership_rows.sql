-- One row per change of ownership (event_key: buyer enrollment, seller enrollment, effective date and type) from the
-- latest release that lists it: the latest catalog period end, then the highest release ID, then the lower source row and
-- checksum (decision 4, gold decision C, amendment A6; failure mode 798). releases_listed counts the releases that list the
-- event. Values that differ between releases, and another row of the event in the kept row's release, are held in
-- int_change_of_ownership_holds. The typing, release periods and CCN routes are those of change_of_ownership_release_rows()
-- (failure modes 365, 369 to 372, 619 to 622 and 714; associate IDs that lost leading zeros are padded to 10 digits).
{{ config(materialized='table') }}

with

release_rows as (
    {{ change_of_ownership_release_rows() }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by event_key
            order by period_end desc nulls last, release_id desc nulls last, source_row_number asc, member_sha256 asc
        ) as event_rank,
        count(distinct release_id) over (partition by event_key) as releases_listed
    from release_rows
)

select * exclude (event_rank)
from ranked
where event_rank = 1
