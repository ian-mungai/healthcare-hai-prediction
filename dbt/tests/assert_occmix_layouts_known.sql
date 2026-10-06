select layout_key
from {{ ref('int_occmix_file_layouts') }}
where is_survey_candidate and (not is_survey_layout or repeated_fields <> 0)
