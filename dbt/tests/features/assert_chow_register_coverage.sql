-- Fails when the reviewed coverage start of the change-of-ownership register is not the one row that the CMS Hospital Change of Ownership dataset page
-- states: "hospital ownership changes that occurred on or after January 1, 2016" (read 9 Nov 2026 on the page
-- https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/hospital-change-of-ownership). The true zero of C069 starts from this date
-- and never from the earliest event of the data. A new date needs a new citation in the seed and here.

with

expected as (
    select
        'cms_change_of_ownership' as register,
        date '2016-01-01' as coverage_start
),

stored as (
    select
        register,
        coverage_start,
        source_url,
        source_wording
    from {{ ref('register_coverage') }}
)

select
    coalesce(expected.register, stored.register) as register,
    case
        when stored.register is null then 'coverage row missing'
        when expected.register is null then 'coverage row not reviewed'
        when coalesce(stored.coverage_start, date '1900-01-01') <> expected.coverage_start then 'coverage start differs from the cited date'
        when coalesce(stored.source_url, '') not like 'https://data.cms.gov/%' or coalesce(stored.source_wording, '') = '' then 'citation missing'
    end as failure
from expected
full outer join stored on expected.register = stored.register
where
    stored.register is null
    or expected.register is null
    or coalesce(stored.coverage_start, date '1900-01-01') <> expected.coverage_start
    or coalesce(stored.source_url, '') not like 'https://data.cms.gov/%'
    or coalesce(stored.source_wording, '') = ''
