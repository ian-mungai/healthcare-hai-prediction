select survey_row_key
from {{ ref('int_occmix_survey_rows') }}
where
{% for field in ['rnsal', 'rnhr', 'lpnsthr', 'naorathr', 'mahr', 'nursehr', 'rnahw'] %}
    ({{ field }}_text is not null and {{ field }}_text not in ('.', '-') and {{ field }} is null)
    {% if not loop.last %}or{% endif %}
{% endfor %}
