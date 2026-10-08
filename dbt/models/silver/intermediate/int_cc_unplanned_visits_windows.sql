-- One Care Compare unplanned hospital visits row per hospital, measure and window, from the latest stored release; windows
-- whose latest release holds two rows are held in int_validation_window_holds. Every measure ID is kept, including renamed
-- ones such as OP-32 (failure modes 504 to 508).
{{ care_windows(
    'cms_cc_unplanned_hospital_visits_hospital',
    "coalesce(facility_id, provider_id)",
    columns=[
        ['measure_name', 'measure_name'],
        ['compared_to_national', 'compared_to_national'],
        ['denominator', 'denominator'],
        ['score', 'score'],
        ['lower_estimate', 'lower_estimate'],
        ['higher_estimate', 'higher_estimate'],
        ['number_of_patients', 'number_of_patients'],
        ['number_of_patients_returned', 'number_of_patients_returned'],
        ['footnote', 'footnote']
    ]
) }}
