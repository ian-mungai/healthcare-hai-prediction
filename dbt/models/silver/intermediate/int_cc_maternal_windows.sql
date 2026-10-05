-- One Care Compare maternal health row per hospital, measure and measurement window, from the latest stored release; the
-- table has only facility_id, start_date and end_date (failure modes 318 to 322).
{{ care_windows(
    'cms_cc_maternal_health_hospital',
    "facility_id",
    start_column='start_date',
    end_column='end_date',
    columns=[['measure_name', 'measure_name'], ['score', 'score'], ['sample', 'sample'], ['footnote', 'footnote']]
) }}
