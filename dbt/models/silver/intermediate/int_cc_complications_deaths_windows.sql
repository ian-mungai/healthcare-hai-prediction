-- One Care Compare complications and deaths row per hospital, measure and window, from the latest stored release; windows
-- whose latest release holds two rows are held in int_validation_window_holds. Mortality, PSI and other IDs are all kept;
-- none is a predictor. A reviewed renamed ID follows the same rule as in int_cc_unplanned_visits_windows; score_number is the
-- score when it is a plain number and tokens stay text (failure modes 504 to 508, 524, 629 and 630).
with

windows as (
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
)

{{ validation_aliased_windows('int_cc_complications_deaths_windows', 'windows', [['score_number', 'score']]) }}
