-- One HAI hospital row per entity, measure and measurement window, from the latest stored release. The national benchmark category is kept as published.
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
