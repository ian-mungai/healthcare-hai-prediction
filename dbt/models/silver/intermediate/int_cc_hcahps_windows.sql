-- One Care Compare HCAHPS row per hospital, HCAHPS measure and measurement window, from the latest stored release; each
-- value keeps its own column and footnote (failure modes 318 to 322).
{{ care_windows(
    'cms_cc_hcahps_hospital',
    "coalesce(facility_id, provider_id)",
    measure='hcahps_measure_id',
    columns=[
        ['measure_name', 'hcahps_question'],
        ['hcahps_answer_description', 'hcahps_answer_description'],
        ['hcahps_answer_percent', 'hcahps_answer_percent'],
        ['hcahps_answer_percent_footnote', 'hcahps_answer_percent_footnote'],
        ['hcahps_linear_mean_value', 'hcahps_linear_mean_value'],
        ['patient_survey_star_rating', 'patient_survey_star_rating'],
        ['patient_survey_star_rating_footnote', 'patient_survey_star_rating_footnote'],
        ['number_of_completed_surveys', 'number_of_completed_surveys'],
        ['number_of_completed_surveys_footnote', 'number_of_completed_surveys_footnote'],
        ['survey_response_rate_percent', 'survey_response_rate_percent'],
        ['survey_response_rate_percent_footnote', 'survey_response_rate_percent_footnote']
    ]
) }}
