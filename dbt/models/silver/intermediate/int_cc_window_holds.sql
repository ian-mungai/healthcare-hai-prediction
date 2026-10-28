-- Care Compare measure rows the window models leave out, with the reason and row count: no key, an unparsed date, a held
-- label, an undated release, or a conflict at the latest release date.
-- Each hold has a null-safe hold_key and the sorted checksums of its files, member_sha256s.
select * from ({{ care_window_holds('cms_cc_timely_and_effective_care_hospital', "coalesce(facility_id, provider_id)") }})
union all by name
select * from ({{ care_window_holds('cms_cc_maternal_health_hospital', "facility_id", start_column='start_date', end_column='end_date') }})
union all by name
select * from ({{ care_window_holds(
    'cms_cc_hcahps_hospital',
    "coalesce(facility_id, provider_id)",
    measure='hcahps_measure_id',
    columns=[['measure_name', 'hcahps_question']]
) }})
