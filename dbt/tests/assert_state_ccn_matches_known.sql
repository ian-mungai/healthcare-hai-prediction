-- Fails for each reviewed state-facility CCN match whose facility number no state model carries (a typing mistake), or whose CCN is not six characters of
-- digits and capital letters.
with

known as (
    select
        'HCAI_UTIL' as source_id,
        fac_no as facility_key
    from {{ ref('int_hcai_util_facility_periods') }}
    union
    select
        'IL' as source_id,
        entity_id as facility_key
    from {{ ref('int_illinois_report_card_values') }}
)

select
    matches.source_id,
    matches.facility_key,
    matches.ccn
from {{ ref('state_facility_ccn_matches') }} as matches
left join known
    on
        matches.source_id = known.source_id
        and matches.facility_key = known.facility_key
where
    known.facility_key is null
    or not regexp_full_match(matches.ccn, '[0-9A-Z]{6}')
