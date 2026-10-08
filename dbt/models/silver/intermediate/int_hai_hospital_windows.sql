-- One HAI hospital row per entity, measure and measurement window, from the latest stored release (BRZ-002; failure modes
-- 230 to 236, plans/hai_windows_20261004/failure_modes.md).
{{ care_windows('cms_hai_hospital', "coalesce(facility_id, provider_id)") }}
