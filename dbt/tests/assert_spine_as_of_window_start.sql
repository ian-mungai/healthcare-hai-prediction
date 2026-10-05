-- Fails for each spine row whose POS snapshot does not end in the 12 months before the window or whose CMI is not for the
-- fiscal year before the window year: every predictor is as of the window start (owner decision, Oct 5 2026) [308] to [310].
select
    ccn,
    window_year,
    window_start,
    pos_period_end,
    cmi_data_fiscal_year
from {{ ref('int_hospital_spine') }}
where
    pos_period_end >= window_start
    or pos_period_end < window_start - interval 1 year
    or cmi_data_fiscal_year <> window_year - 1
