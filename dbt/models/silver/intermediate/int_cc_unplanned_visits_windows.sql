-- One Care Compare unplanned hospital visits row per hospital, measure and window, from the latest stored release; windows
-- whose latest release holds two rows are held in int_validation_window_holds. Every measure ID is kept, including renamed
-- ones such as OP-32. A reviewed renamed ID enters under the exact ID only where the hospital and window have no exact-ID
-- row, and keeps the ID as published in published_measure_id; score_number is the score when it is a plain number (failure
-- modes 504 to 508, 524, 629 and 630).
with

windows as (
    {{ care_windows(
        'cms_cc_unplanned_hospital_visits_hospital',
        validation_ccn('coalesce(facility_id, provider_id)'),
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
)

{{ validation_aliased_windows('int_cc_unplanned_visits_windows', 'windows', [['score_number', 'score']]) }}
