-- Group D measure rows the window models leave out, with the reason and row count: no key, an unparsed date, a held label,
-- an undated release, or a conflict at the latest release date (failure modes 507 and 524). Kept apart from int_cc_window_holds so
-- the group B holds stay unchanged.
select * from ({{ care_window_holds('cms_cc_unplanned_hospital_visits_hospital', validation_ccn('coalesce(facility_id, provider_id)')) }})
union all by name
select * from ({{ care_window_holds('cms_cc_complications_and_deaths_hospital', validation_ccn('coalesce(facility_id, provider_id)')) }})
union all by name
select * from ({{ care_window_holds(
    'cms_cc_hospital_readmissions_reduction_program_hospital',
    validation_ccn('facility_id'),
    measure='measure_name',
    start_column='start_date',
    end_column='end_date',
    columns=[['footnote', 'footnote']]
) }})
