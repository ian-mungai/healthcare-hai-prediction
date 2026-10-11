-- One Care Compare timely and effective care row per hospital, measure and measurement window, from the latest stored
-- release; windows whose latest release holds two rows are held in int_cc_window_holds (failure modes 318 to 322).
-- A value that is a plain number (an optional minus sign and digits, with an optional decimal part) is also given as a number;
-- a category, token or other text stays text with a null number [327].

with

windows as (
    {{ care_windows(
        'cms_cc_timely_and_effective_care_hospital',
        "coalesce(facility_id, provider_id)",
        columns=[['measure_name', 'measure_name'], ['condition', 'condition'], ['score', 'score'], ['sample', 'sample'], ['footnote', 'footnote']]
    ) }}
)

select
    *,
    case when regexp_full_match(trim(score), '-?[0-9]+(\.[0-9]+)?') then trim(score)::double end as score_number
from windows
