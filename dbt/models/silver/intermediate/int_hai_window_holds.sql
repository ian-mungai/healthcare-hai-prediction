-- HAI rows the window models leave out, with the reason and row count: no key, an unparsed date, a held label, an
-- undated release, or a conflict at the latest release date (BRZ-002; failure modes 232 to 235).
-- Each hold has a null-safe hold_key and the sorted checksums of its files, member_sha256s (failure modes 791 and 792).
select * from ({{ care_window_holds('cms_hai_hospital', "coalesce(facility_id, provider_id)") }})
union all by name
select * from ({{ care_window_holds('cms_hai_state', "state") }})
union all by name
select * from ({{ care_window_holds('cms_hai_national', "'US'") }})
