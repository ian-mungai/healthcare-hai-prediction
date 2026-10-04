-- HAI rows the window models leave out, with the reason and row count: no key, an unparsed date, a held label, an
-- undated release, or a conflict at the latest release date (BRZ-002; failure modes 232 to 235).
select * from ({{ hai_window_holds('cms_hai_hospital', "coalesce(facility_id, provider_id)") }})
union all by name
select * from ({{ hai_window_holds('cms_hai_state', "state") }})
union all by name
select * from ({{ hai_window_holds('cms_hai_national', "'US'") }})
