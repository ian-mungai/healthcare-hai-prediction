-- Fails for each reviewed HHS match whose CCN also reports under its own published CCN, which would double-count weeks [683].
select distinct matches.ccn
from {{ ref('hhs_reviewed_ccn_matches') }} as matches
inner join {{ ref('int_hhs_capacity_weeks') }} as weeks
    on
        matches.ccn = weeks.ccn
        and weeks.ccn_source = 'published'
