-- One Care Compare complications and deaths row per hospital, measure and window, from the latest stored release; windows
-- whose latest release holds two rows are held in int_validation_window_holds. Mortality, PSI and other IDs are all kept;
-- none is a predictor (failure modes 504 to 508 and 524).
{{ care_windows(
    'cms_cc_complications_and_deaths_hospital',
    validation_ccn('coalesce(facility_id, provider_id)'),
    columns=[
        ['measure_name', 'measure_name'],
        ['compared_to_national', 'compared_to_national'],
        ['denominator', 'denominator'],
        ['score', 'score'],
        ['lower_estimate', 'lower_estimate'],
        ['higher_estimate', 'higher_estimate'],
        ['footnote', 'footnote']
    ]
) }}
