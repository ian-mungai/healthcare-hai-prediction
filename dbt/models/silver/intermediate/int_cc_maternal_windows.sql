-- One Care Compare maternal health row per hospital, measure and measurement window, from the latest stored release; the
-- table has only facility_id, start_date and end_date.
-- A value that is a plain number (an optional minus sign and digits, with an optional decimal part) is also given as a number;
-- a category, token or other text stays text with a null number.

with

windows as (
    {{ care_windows(
        'cms_cc_maternal_health_hospital',
        "facility_id",
        start_column='start_date',
        end_column='end_date',
        columns=[['measure_name', 'measure_name'], ['score', 'score'], ['sample', 'sample'], ['footnote', 'footnote']]
    ) }}
)

select
    *,
    case when regexp_full_match(trim(score), '-?[0-9]+(\.[0-9]+)?') then trim(score)::double end as score_number
from windows
