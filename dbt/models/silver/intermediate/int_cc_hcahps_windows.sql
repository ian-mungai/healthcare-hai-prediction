-- One Care Compare HCAHPS row per hospital, HCAHPS measure and measurement window, from the latest stored release; each
-- value keeps its own column and footnote (failure modes 318 to 322).
-- A value that is a plain number (an optional minus sign and digits, with an optional decimal part) is also given as a number;
-- a category, token or other text stays text with a null number [327].

with

windows as (
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
)

select
    *,
    case
        when regexp_full_match(trim(hcahps_answer_percent), '-?[0-9]+(\.[0-9]+)?') then trim(hcahps_answer_percent)::double
    end as hcahps_answer_percent_number,
    case
        when regexp_full_match(trim(number_of_completed_surveys), '-?[0-9]+(\.[0-9]+)?') then trim(number_of_completed_surveys)::double
    end as number_of_completed_surveys_number,
    case
        when regexp_full_match(trim(survey_response_rate_percent), '-?[0-9]+(\.[0-9]+)?') then trim(survey_response_rate_percent)::double
    end as survey_response_rate_percent_number
from windows
