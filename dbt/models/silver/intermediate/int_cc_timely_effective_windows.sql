-- One Care Compare timely and effective care row per hospital, measure and measurement window, from the latest stored
-- release; windows whose latest release holds two rows are held in int_cc_window_holds (failure modes 318 to 322).
{{ care_windows(
    'cms_cc_timely_and_effective_care_hospital',
    "coalesce(facility_id, provider_id)",
    columns=[['measure_name', 'measure_name'], ['condition', 'condition'], ['score', 'score'], ['sample', 'sample'], ['footnote', 'footnote']]
) }}
