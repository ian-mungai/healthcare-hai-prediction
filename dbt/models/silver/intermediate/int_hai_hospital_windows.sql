-- One HAI hospital row per entity, measure and measurement window, from the latest stored release (BRZ-002; failure modes
-- 230 to 236, plans/hai_windows_20261004/failure_modes.md). The national benchmark category is kept as published (AL1 gap
-- review, failure mode 563).
{{ care_windows(
    'cms_hai_hospital',
    "coalesce(facility_id, provider_id)",
    columns=[
        ['measure_name', 'measure_name'],
        ['score', 'score'],
        ['footnote', 'footnote'],
        ['compared_to_national', 'compared_to_national']
    ]
) }}
