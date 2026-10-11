-- Fails for each aligned validation row whose period does not overlap its HAI window, and for each hospital-window without
-- exactly one row per seed control and measure [597] [600].
select
    alignment_key,
    'period does not overlap the window' as failure
from {{ ref('int_spine_validation_measures') }}
where
    alignment_status = 'aligned'
    and (period_end < window_start or period_start > make_date(window_year, 12, 31))
union all
select
    spine_key as alignment_key,
    'control count differs' as failure
from {{ ref('int_spine_validation_measures') }}
group by spine_key
having count(*) <> (select count(*) from {{ ref('validation_measures') }})
