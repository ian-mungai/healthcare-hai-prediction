-- One Hospital Readmissions Reduction Program row per hospital, condition (measure_name) and performance window, from the
-- latest stored release; windows whose latest release holds two rows are held in int_validation_window_holds (failure
-- modes 504 to 507 and 524). excess_readmission_ratio_number is the ratio when it is a plain number and tokens stay text;
-- reviewed renamed conditions follow the same rule as in int_cc_unplanned_visits_windows.
with

windows as (
    {{ care_windows(
        'cms_cc_hospital_readmissions_reduction_program_hospital',
        validation_ccn('facility_id'),
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
)

{{ validation_aliased_windows('int_cc_hrrp_windows', 'windows', [['excess_readmission_ratio_number', 'excess_readmission_ratio']]) }}
