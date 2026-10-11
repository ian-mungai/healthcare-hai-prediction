select survey_row_key
from {{ ref('int_occmix_survey_rows') }}
where
    (hold_reason is not null and coalesce(rn_paid_hour_wage, rn_paid_hour_share, lpnst_paid_hour_share, naorat_paid_hour_share) is not null)
    or (coalesce(rnhr, 0) <= 0 and rn_paid_hour_wage is not null)
    or (coalesce(nursehr, 0) <= 0 and coalesce(rn_paid_hour_share, lpnst_paid_hour_share, naorat_paid_hour_share) is not null)
