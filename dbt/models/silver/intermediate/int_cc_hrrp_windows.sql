-- One Hospital Readmissions Reduction Program row per hospital, condition (measure_name) and performance window, from the
-- latest stored release; windows whose latest release holds two rows are held in int_validation_window_holds (failure
-- modes 504 to 507).
{{ care_windows(
    'cms_cc_hospital_readmissions_reduction_program_hospital',
    "facility_id",
    measure='measure_name',
    start_column='start_date',
    end_column='end_date',
    columns=[
        ['number_of_discharges', 'number_of_discharges'],
        ['excess_readmission_ratio', 'excess_readmission_ratio'],
        ['predicted_readmission_rate', 'predicted_readmission_rate'],
        ['expected_readmission_rate', 'expected_readmission_rate'],
        ['number_of_readmissions', 'number_of_readmissions'],
        ['footnote', 'footnote']
    ]
) }}
